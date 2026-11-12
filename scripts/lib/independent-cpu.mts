/** Independent VM pairs. Named boundaries own their operation; CPU includes every process
 * thread and natural GC. Setup, verification and IPC are outside that clock. No forced GC.
 */
import { fork } from "node:child_process";
import { loadavg } from "node:os";
import { getHeapStatistics, getHeapSpaceStatistics } from "node:v8";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { pairedCPU } from "./paired-cpu.mts";

export type Arm = "control" | "candidate";
export interface CPUWorkerContext {
  readonly arm: Arm;
  readonly revision: string;
  readonly cohort: number;
  readonly workload: string;
  readonly boundary: string;
  readonly data: unknown;
  readonly signal: AbortSignal;
}
export interface Verification {
  readonly fingerprint: string;
  readonly rows?: number;
}
export interface BoundaryValue {
  readonly rows?: number;
  readonly detail?: unknown;
}
export interface CPUWorkerHarness {
  readonly revision: string;
  readonly identity: unknown;
  readonly workloads: readonly string[];
  readonly boundaries: readonly string[];
  verify(workload: string): Promise<Verification>;
  run(workload: string, boundary: string): Promise<BoundaryValue>;
  close(): Promise<void>;
  cancel?(reason: unknown): void;
  command?(name: string, data: unknown): Promise<unknown>;
}
export interface CPUArm {
  /** Module default export: async (context: CPUWorkerContext) => CPUWorkerHarness. */
  readonly module: string;
  readonly cwd: string;
  readonly revision: string;
  readonly data?: unknown;
  readonly env?: NodeJS.ProcessEnv;
  readonly execArgv?: readonly string[];
}
interface Request {
  id: number;
  op: "run" | "verify" | "close" | "abort" | "command";
  workload?: string;
  boundary?: string;
  name?: string;
  data?: unknown;
}
interface Response {
  id: number;
  ok: boolean;
  value?: unknown;
  error?: string;
}
interface Ready {
  kind: "ready";
  revision: string;
  pid: number;
  node: string;
  identity: unknown;
  workloads: readonly string[];
  boundaries: readonly string[];
  memory: unknown;
}
export interface CPUValue extends BoundaryValue {
  readonly cpuMs: number;
  readonly userMs: number;
  readonly systemMs: number;
  readonly wallMs: number;
  readonly load: number[];
}
const errorText = (error: unknown): string =>
  error instanceof Error ? (error.stack ?? error.message) : String(error);
const memory = () => ({
  process: process.memoryUsage(),
  heap: getHeapStatistics(),
  spaces: getHeapSpaceStatistics(),
});
const send = (message: unknown): void => {
  process.send?.(message);
};

/** Adapters may expose any named boundary: frames/rows, roots, sizing, native component, etc.
 * A boundary callback must await its own delivery/cleanup boundary before returning.
 */
export function serveCPUWorker(harness: CPUWorkerHarness, abort = new AbortController()): void {
  let pending = Promise.resolve();
  process.on("message", (raw: Request) => {
    if (raw.op === "abort") {
      const error = new Error(String(raw.data ?? "CPU comparison cancelled"));
      abort.abort(error);
      harness.cancel?.(error);
      return;
    }
    pending = pending.then(async () => {
      try {
        let value: unknown;
        if (raw.op === "close") {
          await harness.close();
          send({ id: raw.id, ok: true, value: null });
          process.disconnect?.();
          return;
        } else if (raw.op === "command") {
          if (harness.command === undefined || raw.name === undefined)
            throw new Error("Unknown CPU worker command");
          value = await harness.command(raw.name, raw.data);
        } else {
          const workload = raw.workload;
          if (workload === undefined || !harness.workloads.includes(workload))
            throw new Error("Unknown CPU workload");
          if (raw.op === "verify") value = await harness.verify(workload);
          else {
            const boundary = raw.boundary;
            if (boundary === undefined || !harness.boundaries.includes(boundary))
              throw new Error("Unknown CPU boundary");
            send({ kind: "run-start", workload, boundary });
            const load = loadavg(),
              cpu = process.cpuUsage(),
              wall = performance.now();
            const result = await harness.run(workload, boundary);
            const used = process.cpuUsage(cpu),
              elapsed = performance.now() - wall;
            value = {
              ...result,
              cpuMs: (used.user + used.system) / 1000,
              userMs: used.user / 1000,
              systemMs: used.system / 1000,
              wallMs: elapsed,
              load,
              memory: memory(),
            };
          }
        }
        send({ id: raw.id, ok: true, value });
      } catch (error) {
        send({ id: raw.id, ok: false, error: errorText(error) });
      }
    });
  });
  send({
    kind: "ready",
    revision: harness.revision,
    pid: process.pid,
    node: process.version,
    identity: harness.identity,
    workloads: harness.workloads,
    boundaries: harness.boundaries,
    memory: memory(),
  });
}

/** Each call starts a real new process. Only PIDs started here are ever terminated. */
export async function startCPUWorker(
  spec: CPUArm,
  context: Omit<CPUWorkerContext, "revision" | "data" | "signal">,
  onEvent?: (event: unknown) => void,
  signal?: AbortSignal,
) {
  signal?.throwIfAborted();
  const child = fork(
    fileURLToPath(import.meta.url),
    [
      resolve(spec.module),
      JSON.stringify({ ...context, revision: spec.revision, data: spec.data }),
    ],
    {
      cwd: spec.cwd,
      env: { ...process.env, ...spec.env },
      execArgv: [...(spec.execArgv ?? [])],
      serialization: "advanced",
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    },
  );
  let serial = 0,
    ended = false;
  const requests = new Map<
    number,
    { resolve(value: unknown): void; reject(error: unknown): void }
  >();
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((yes) =>
    child.once("exit", (code, exitSignal) => {
      ended = true;
      for (const request of requests.values())
        request.reject(new Error(`CPU worker exited ${String(code)}/${String(exitSignal)}`));
      requests.clear();
      yes({ code, signal: exitSignal });
    }),
  );
  child.on("message", (raw: Response & { kind?: string }) => {
    if (raw.kind !== undefined) {
      onEvent?.({ arm: context.arm, pid: child.pid, ...raw });
      return;
    }
    const request = requests.get(raw.id);
    if (request === undefined) return;
    requests.delete(raw.id);
    if (raw.ok) request.resolve(raw.value);
    else request.reject(new Error(raw.error));
  });
  const ready = new Promise<Ready>((yes, no) => {
    const receive = (raw: Ready & { error?: string }) => {
      if (raw.kind === "ready") {
        child.off("message", receive);
        yes(raw);
      } else if (raw.kind === "fatal") {
        child.off("message", receive);
        no(new Error(raw.error ?? "CPU worker did not become ready"));
      }
    };
    child.on("message", receive);
    child.once("error", no);
    child.once("exit", (code, exitSignal) =>
      no(new Error(`CPU worker startup exited ${String(code)}/${String(exitSignal)}`)),
    );
  });
  const ask = <T,>(request: Omit<Request, "id">): Promise<T> =>
    new Promise((yes, no) => {
      const id = ++serial;
      requests.set(id, { resolve: (value) => yes(value as T), reject: no });
      child.send({ id, ...request }, (error) => {
        if (error) {
          requests.delete(id);
          no(error);
        }
      });
    });
  const onAbort = () => {
    if (child.connected) child.send({ id: 0, op: "abort", data: errorText(signal?.reason) });
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const hello = await ready;
    if (
      hello.revision !== spec.revision ||
      hello.node !== process.version ||
      hello.pid !== child.pid
    )
      throw new Error("CPU worker revision/Node mismatch");
    return {
      hello,
      ask,
      async close() {
        signal?.removeEventListener("abort", onAbort);
        try {
          if (!ended) await ask({ op: "close" });
        } catch (error) {
          if (!ended) child.kill("SIGTERM");
          await exited;
          throw error;
        }
        const status = await exited;
        if (status.code !== 0 || status.signal !== null)
          throw new Error(`CPU worker close failed: ${JSON.stringify(status)}`);
      },
    };
  } catch (error) {
    signal?.removeEventListener("abort", onAbort);
    if (!ended) {
      try {
        await ask({ op: "close" });
      } catch {
        if (!ended) child.kill("SIGTERM");
      }
    }
    await exited;
    throw error;
  }
}

export interface IndependentCPUOptions {
  readonly control: CPUArm;
  readonly candidate: CPUArm;
  readonly workloads: readonly string[];
  readonly boundaries: readonly string[];
  readonly primaryBoundary: string;
  readonly pairs?: number;
  readonly settleMs?: number;
  readonly purpose?: "true-parent" | "identical-build-control";
  readonly signal?: AbortSignal;
  readonly onEvent?: (event: unknown) => void;
  readonly onSample?: (sample: unknown) => void;
}

/** One fresh pair per workload/boundary/round; startup and execution both alternate AB/BA.
 * No previous catalogue cell can leave heap/tiering state in the next cell's observation.
 */
export async function runIndependentCPU(options: IndependentCPUOptions) {
  const pairs = options.pairs ?? 9,
    settleMs = options.settleMs ?? 1000;
  if (!Number.isSafeInteger(pairs) || pairs < 9 || settleMs < 0 || !Number.isFinite(settleMs))
    throw new RangeError("Invalid independent CPU pair policy");
  const purpose = options.purpose ?? "true-parent";
  if (
    options.workloads.length === 0 ||
    options.boundaries.length === 0 ||
    options.workloads.some((name) => name.length === 0) ||
    options.boundaries.some((name) => name.length === 0) ||
    new Set(options.workloads).size !== options.workloads.length ||
    new Set(options.boundaries).size !== options.boundaries.length
  )
    throw new Error("CPU workload/boundary names must be nonempty and unique");
  if (
    (options.control.revision === options.candidate.revision) !==
    (purpose === "identical-build-control")
  )
    throw new Error("True-parent and identical-build controls must be labelled correctly");
  if (!options.boundaries.includes(options.primaryBoundary))
    throw new Error("Missing primary CPU boundary");
  const records = [];
  for (const workload of options.workloads)
    for (const boundary of options.boundaries) {
      const before: number[] = [],
        after: number[] = [],
        samples = [];
      for (let cohort = 0; cohort < pairs; cohort++) {
        options.signal?.throwIfAborted();
        const order: Arm[] = cohort % 2 === 0 ? ["control", "candidate"] : ["candidate", "control"];
        const verification = new Map<Arm, Verification>();
        for (const arm of order) {
          const worker = await startCPUWorker(
            options[arm],
            { arm, cohort, workload, boundary },
            options.onEvent,
            options.signal,
          );
          try {
            const checked = await worker.ask<Verification>({ op: "verify", workload });
            if (
              typeof checked.fingerprint !== "string" ||
              checked.fingerprint.length === 0 ||
              (checked.rows !== undefined &&
                (!Number.isSafeInteger(checked.rows) || checked.rows < 0))
            )
              throw new Error("Invalid CPU output verification");
            verification.set(arm, checked);
            const other = verification.get(arm === "control" ? "candidate" : "control");
            if (
              other !== undefined &&
              (other.fingerprint !== checked.fingerprint || other.rows !== checked.rows)
            )
              throw new Error(`CPU workload differs: ${workload}`);
            if (settleMs > 0) await new Promise((yes) => setTimeout(yes, settleMs));
            options.signal?.throwIfAborted();
            const value = await worker.ask<CPUValue>({ op: "run", workload, boundary });
            if (
              !Number.isFinite(value.cpuMs) ||
              value.cpuMs < 0 ||
              Math.abs(value.cpuMs - value.userMs - value.systemMs) > 1e-6
            )
              throw new Error("Invalid process CPU sample");
            if (checked.rows !== value.rows) throw new Error("CPU output row count changed");
            const sample = {
              arm,
              cohort,
              workload,
              boundary,
              order,
              identity: worker.hello,
              ...value,
            };
            samples.push(sample);
            (arm === "control" ? before : after).push(value.cpuMs);
            options.onSample?.(sample);
            const last = await worker.ask<Verification>({ op: "verify", workload });
            if (last.fingerprint !== checked.fingerprint || last.rows !== checked.rows)
              throw new Error("CPU output changed after measurement");
          } finally {
            await worker.close();
          }
        }
      }
      records.push({
        workload,
        boundary,
        primary: purpose === "true-parent" && boundary === options.primaryBoundary,
        samples,
        acceptance: pairedCPU(before, after),
      });
    }
  return {
    purpose,
    pairs,
    settleMs,
    lifecycle: "fresh VM per arm/workload/boundary/round; serial arms, alternating AB/BA",
    clock:
      "process user+system CPU, all threads and natural GC; no forced GC; setup/verification/IPC excluded",
    records,
  };
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const modulePath = process.argv[2],
      encoded = process.argv[3];
    if (modulePath === undefined || encoded === undefined)
      throw new Error("Missing CPU worker adapter/context");
    const abort = new AbortController(),
      context = JSON.parse(encoded) as Omit<CPUWorkerContext, "signal">;
    const earlyAbort = (request: Request) => {
      if (request.op === "abort")
        abort.abort(new Error(String(request.data ?? "CPU comparison cancelled")));
    };
    process.on("message", earlyAbort);
    const module = (await import(pathToFileURL(resolve(modulePath)).href)) as {
      default(context: CPUWorkerContext): Promise<CPUWorkerHarness>;
    };
    const harness = await module.default({ ...context, signal: abort.signal });
    process.off("message", earlyAbort);
    if (abort.signal.aborted) {
      await harness.close();
      abort.signal.throwIfAborted();
    }
    serveCPUWorker(harness, abort);
  } catch (error) {
    send({ kind: "fatal", error: errorText(error) });
    process.exitCode = 1;
    if (process.connected) process.disconnect?.();
  }
}
