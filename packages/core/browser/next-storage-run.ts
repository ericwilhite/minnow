/**
 * Page side of the rebuild core's storage browser suite (browser-tests/next-storage.spec.ts):
 * spawns next-storage-worker.ts workers, kills them mid-publish, and verifies from fresh ones.
 */
import { column, schema, table } from "@minnowdb/core";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import type {
  ChaosExpect,
  ChaosInjection,
  ChaosMode,
  ChaosIntent,
  ChaosMessage,
  ChaosRecovery,
  ChaosState,
  ChaosStep,
} from "./next-storage-chaos.js";
import type { ReopenBuild, ReopenLayout, ReopenTiming } from "./next-storage-reopen.js";
import type { NextStorageRequest, PublishPhase, StoreKind } from "./next-storage-worker.js";

/** The backend every following scenario runs on (OPFS unless a test picks IndexedDB). */
let store: StoreKind = "opfs";
export function useStore(kind: StoreKind): void {
  store = kind;
}

interface StorageWorker {
  call<T>(request: NextStorageRequest): Promise<T>;
  /** The publisher's last reported phase, or null before its first round. */
  readonly progress: { readonly round: number; readonly phase: PublishPhase } | null;
  /** The highest round this worker reported as published (its publish resolved). */
  readonly acked: number;
  /** Rounds this worker published. */
  readonly published: number;
  /** Starts a publish loop; a failure other than this page's own `terminate` is kept. */
  publish(dir: string, rounds: number): void;
  /**
   * Resolves when `ready()` holds; rejects naming `what`, the last phase and any publisher
   * failure when it does not within `ms` (a stalled or failed publisher fails the test).
   */
  until(ready: () => boolean, what: string, ms?: number): Promise<void>;
  post(message: unknown, transfer?: Transferable[]): void;
  terminate(): void;
}

async function spawn(): Promise<StorageWorker> {
  const worker = new Worker(new URL("./next-storage-worker.ts", import.meta.url), {
    type: "module",
  });
  const pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();
  let next = 1;
  let progress: StorageWorker["progress"] = null;
  let acked = 0;
  let published = 0;
  let failure: Error | null = null;
  let terminated = false;
  /** Waiters checked on every progress message, so no phase can slip by between polls. */
  const watchers = new Set<() => void>();
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error("storage worker did not start"));
    }, 20_000);
    worker.addEventListener("message", (event: MessageEvent<unknown>) => {
      if (event.data === "next-storage-ready") {
        clearTimeout(timer);
        resolve();
        return;
      }
      const data = event.data as {
        id?: number;
        result?: unknown;
        error?: string;
        progress?: { round: number; phase: PublishPhase };
      };
      if (data.progress !== undefined) {
        progress = data.progress;
        if (data.progress.phase === "published") {
          acked = Math.max(acked, data.progress.round);
          published += 1;
        }
        for (const watch of [...watchers]) watch();
      }
      if (data.id === undefined) return;
      const waiter = pending.get(data.id);
      pending.delete(data.id);
      if (data.error !== undefined) waiter?.reject(new Error(data.error));
      else waiter?.resolve(data.result);
    });
    worker.addEventListener("error", (event) => {
      clearTimeout(timer);
      // A plain Event (no message) is a module that failed to load or evaluate.
      failure ??= new Error(
        typeof event.message === "string"
          ? `storage worker error: ${event.message} (${event.filename}:${String(event.lineno)})`
          : "storage worker failed to load or evaluate its module (an error event without a message)",
      );
      reject(failure);
    });
  });
  const call = <T>(request: NextStorageRequest) => {
    const id = next++;
    return new Promise<T>((resolve, reject) => {
      pending.set(id, { resolve: resolve, reject });
      worker.postMessage({ id, request: { store, ...request } });
    });
  };
  return {
    call,
    get progress() {
      return progress;
    },
    get acked() {
      return acked;
    },
    get published() {
      return published;
    },
    publish(dir, rounds) {
      call({ op: "publish-loop", dir, rounds }).then(
        () => {
          failure ??= new Error("the publisher stopped before it was killed");
        },
        (error: unknown) => {
          if (!terminated) failure ??= error instanceof Error ? error : new Error(String(error));
        },
      );
    },
    until(ready, what, ms = 20_000) {
      return new Promise<void>((resolve, reject) => {
        const finish = (error: Error | null) => {
          watchers.delete(check);
          clearInterval(timer);
          if (error === null) resolve();
          else reject(error);
        };
        const check = () => {
          if (failure !== null)
            finish(new Error(`publisher failed before ${what}: ${failure.message}`));
          else if (ready()) finish(null);
          else if (performance.now() > deadline)
            finish(
              new Error(
                `publisher stalled before ${what}: last phase ${JSON.stringify(progress)}, ${String(published)} rounds published`,
              ),
            );
        };
        const deadline = performance.now() + ms;
        const timer = setInterval(check, 10);
        watchers.add(check);
        check();
      });
    },
    post(message, transfer = []) {
      worker.postMessage(message, transfer);
    },
    terminate() {
      terminated = true;
      worker.terminate();
      for (const waiter of pending.values())
        waiter.reject(Object.assign(new Error("terminated"), { name: "WorkerTerminated" }));
      pending.clear();
    },
  };
}

const unique = (label: string) =>
  `${label}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

async function withWorker<T>(work: (worker: StorageWorker) => Promise<T>): Promise<T> {
  const worker = await spawn();
  try {
    return await work(worker);
  } finally {
    worker.terminate();
  }
}

export async function runConformance(): Promise<Array<{ name: string; error: string | null }>> {
  return withWorker((worker) => worker.call({ op: "conformance", prefix: unique("conformance") }));
}

export const PUBLISH_PHASES: readonly PublishPhase[] = ["writing", "publishing", "published"];

export interface KillResult {
  readonly iterations: Array<{
    /** The phase this iteration aimed its kill at, and where the publisher was when killed. */
    readonly target: PublishPhase;
    readonly killedRound: number;
    readonly acked: number;
    readonly round: number;
    readonly previous: number | null;
    readonly openedMs: number;
  }>;
}

/**
 * Kills a publishing worker in each phase of a round in turn (writing its extents, inside
 * `publish`, just after `publish` resolved), after a random 2–5 rounds of its own so an idle or
 * failed publisher stalls the test instead of passing it, then verifies the files from a fresh
 * worker: the root is at least the last acknowledged round, its previous root is the round
 * before, every blob of both verifies, the free map is exact (`audit`), and the next owner keeps
 * writing. Each phase is the target of every third iteration.
 */
export async function runKillDuringPublish(iterations: number, seed: number): Promise<KillResult> {
  let state = seed >>> 0;
  const random = () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state / 2 ** 32;
  };
  const dir = unique("kill");
  const results: Array<KillResult["iterations"][number]> = [];
  let acked = 0;
  for (let iteration = 0; iteration < iterations; iteration++) {
    const target = PUBLISH_PHASES[iteration % PUBLISH_PHASES.length] ?? "publishing";
    const warmup = 2 + Math.floor(random() * 4);
    const writer = await spawn();
    try {
      writer.publish(dir, 1_000_000);
      await writer.until(
        () => writer.published >= warmup && writer.progress?.phase === target,
        `the ${target} phase after ${String(warmup)} rounds`,
      );
    } finally {
      writer.terminate();
    }
    const killedRound = writer.progress?.round ?? 0;
    acked = Math.max(acked, writer.acked);
    const verified = await withWorker((worker) =>
      worker.call<{ round: number; previous: number | null; openedMs: number }>({
        op: "verify",
        dir,
        minAcked: acked,
      }),
    );
    results.push({ target, killedRound, acked, ...verified });
    // The verifier published one more round.
    acked = verified.round + 1;
  }
  await withWorker((worker) => worker.call({ op: "remove", dir }));
  return { iterations: results };
}

/** Starts a publish loop in this page's worker and returns the directory (the tab is then closed). */
export async function startPublishLoop(): Promise<string> {
  const dir = unique("tab");
  const writer = await spawn();
  writer.publish(dir, 1_000_000);
  await writer.until(() => writer.published >= 3, "three published rounds");
  return dir;
}

export async function verifyDirectory(
  dir: string,
): Promise<{ round: number; previous: number | null; openedMs: number }> {
  return withWorker((worker) => worker.call({ op: "verify", dir, minAcked: 3 }));
}

/**
 * The fence across workers: a second owner without `steal` times out; with `steal` it takes
 * over once the first worker's lease has closed its handles, and the first worker's next mutation
 * is refused.
 */
export async function runSteal(): Promise<{
  waited: { ok: boolean; error?: string };
  stolen: { ok: boolean; ms?: number; error?: string };
  loser: { lost: boolean; error: string | null };
}> {
  const dir = unique("steal");
  const first = await spawn();
  try {
    await first.call({ op: "hold", dir });
    const waited = await withWorker((worker) =>
      worker.call<{ ok: boolean; error?: string }>({
        op: "acquire",
        dir,
        steal: false,
        timeoutMs: 300,
      }),
    );
    const second = await spawn();
    try {
      const stolen = await second.call<{ ok: boolean; ms?: number; error?: string }>({
        op: "acquire",
        dir,
        steal: true,
        timeoutMs: 5_000,
      });
      const loser = await first.call<{ lost: boolean; error: string | null }>({
        op: "write-after-loss",
      });
      return { waited, stolen, loser };
    } finally {
      second.terminate();
    }
  } finally {
    first.terminate();
    await withWorker((worker) => worker.call({ op: "remove", dir }));
  }
}

export async function runSpill(): Promise<{ pages: number; inUse: number; after: number }> {
  const dir = unique("spill");
  try {
    return await withWorker((worker) => worker.call({ op: "spill", dir }));
  } finally {
    await withWorker((worker) => worker.call({ op: "remove", dir }));
  }
}

export interface BuilderBench {
  readonly appendMs: number;
  readonly sealMs: number;
  readonly publishMs: number;
  readonly bytes: number;
}

/** The page format's group-writer pattern: many column builders, page by page (or one by one). */
export async function runBuilderBench(
  alternating: boolean,
  buffered: boolean,
  columns: number,
  pagesPerColumn: number,
  pageBytes: number,
): Promise<BuilderBench> {
  const dir = unique("bench");
  try {
    return await withWorker((worker) =>
      worker.call<BuilderBench>({
        op: "bench-builders",
        dir,
        columns,
        pagesPerColumn,
        pageBytes,
        alternating,
        buffered,
      }),
    );
  } finally {
    await withWorker((worker) => worker.call({ op: "remove", dir }));
  }
}

export interface ChooseResult {
  readonly kind: string | null;
  readonly opfsRefused: string | null;
  readonly error: { name: string; store?: string; location?: string } | null;
}

/** What `chooseBackend` picks in a dedicated worker of this page (for `name`, or a fresh one). */
export async function runChoose(name?: string): Promise<ChooseResult> {
  return withWorker((worker) =>
    worker.call({ op: "choose", ...(name === undefined ? {} : { name }) }),
  );
}

/** The IndexedDB scenarios on the platform's IndexedDB and on the simulator. */
export async function runIdbDifferential(): Promise<{ real: unknown; simulated: unknown }> {
  return withWorker((worker) => worker.call({ op: "idb-scenarios" }));
}

/**
 * The pre-reset engine creates a database (in OPFS or IndexedDB) and writes a row; then
 * `chooseBackend` for the same name must refuse with `LegacyDataError` and create nothing.
 */
export async function runLegacyDetection(kind: "opfs" | "indexeddb"): Promise<ChooseResult> {
  const name = `legacy-${kind}-${crypto.randomUUID()}`;
  const store =
    kind === "indexeddb" ? { kind: "indexeddb" as const, name } : { kind: "opfs" as const, name };
  const client = new MinnowDatabaseClient(
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
    { store },
  );
  await client.migrate(schema([table("notes", { id: column.integer().unique() })]));
  await client.execute("INSERT INTO notes VALUES (1)");
  await client.close({ terminateWorker: true });
  return runChoose(name);
}

/** The IndexedDB backend's bulk write and small-commit costs, with and without batching. */
export async function runIdbBench(
  batching: boolean,
): Promise<{ bulkMs: number; commitMs: number }> {
  const dir = unique("idb-bench");
  try {
    return await withWorker((worker) =>
      worker.call({ op: "bench-idb", dir, batching, store: "indexeddb" }),
    );
  } finally {
    await withWorker((worker) => worker.call({ op: "remove", dir, store: "indexeddb" }));
  }
}

/** The multi-GB run's worker: kept across steps, restarted to reopen as a new process would. */
let heavyWorker: StorageWorker | null = null;

export async function heavyStep(request: NextStorageRequest): Promise<unknown> {
  heavyWorker ??= await spawn();
  return heavyWorker.call(request);
}

export function heavyRestart(): void {
  heavyWorker?.terminate();
  heavyWorker = null;
}

export async function storageEstimate(): Promise<{ usage: number; quota: number }> {
  const estimate = await navigator.storage.estimate();
  return { usage: estimate.usage ?? 0, quota: estimate.quota ?? 0 };
}

/** The storage microbench on OPFS and IndexedDB (each in a fresh directory or database). */
export async function runMicrobench(): Promise<Record<string, Record<string, number>>> {
  const out: Record<string, Record<string, number>> = {};
  for (const kind of ["opfs", "indexeddb"] as const) {
    const dir = unique(`microbench-${kind}`);
    try {
      out[kind] = await withWorker((worker) =>
        worker.call<Record<string, number>>({ op: "microbench", dir, store: kind }),
      );
    } finally {
      await withWorker((worker) => worker.call({ op: "remove", dir, store: kind }));
    }
  }
  return out;
}

// ── The storage kill campaign (Gate A; next-storage-chaos.ts) ────────────────────────────────

/** How a kill lands: terminate the worker after `delayMs`, or hand back for a page close/crash. */
export interface KillPlan {
  /** The step whose `occurrence`-th intent (after this kill is armed) starts the countdown. */
  readonly target: ChaosStep;
  readonly occurrence: number;
  readonly delayMs: number;
  readonly mode: "terminate" | "page";
}

/** Workers that failed before reporting ready (module load), retried by `chaosBegin`. */
let startRetries = 0;

/** How many chaos workers failed to start and were started again (none ran storage code). */
export function chaosStartRetries(): number {
  return startRetries;
}

/**
 * A worker that failed before it reported ready ran no storage code: it is started again, up to
 * twice, and counted (`chaosStartRetries`). Any later failure is the test's.
 */
async function spawnForChaos(): Promise<StorageWorker> {
  for (let attempt = 0; ; attempt++)
    try {
      return await spawn();
    } catch (error) {
      if (attempt >= 2) throw error;
      startRetries += 1;
    }
}

/**
 * Where a kill landed, from the messages the worker posted before it died (drained after the
 * kill): `between` (no step in flight), or for the step in flight `before-<step>` (it had its go
 * but had not begun its storage work), `in-<step>` (it had begun and not finished: the kill
 * landed inside storage) or `after-<step>` (its storage work had returned; its end was not yet
 * posted).
 */
export interface KillLanding {
  readonly landed: string;
  /** What the next worker is told: every acknowledgement the page received, the step in flight. */
  readonly expect: ChaosExpect;
}

interface ChaosRun {
  readonly worker: StorageWorker;
  /** The campaign's own channel: what the worker posted before it died still arrives. */
  readonly port: MessagePort;
  state: ChaosState | null;
  /** Steps announced and not yet done, by sequence. */
  readonly inFlight: Map<number, ChaosIntent>;
  /** Steps whose storage work began, and those whose storage work returned. */
  readonly began: Set<number>;
  readonly settled: Set<number>;
  /** Answers no more intents (a page kill is armed). */
  hold: boolean;
  /** The first failure the workload reported (a corruption, an error), whenever it came. */
  failure: Error | null;
  recovered: ChaosRecovery | null;
  armed: { target: ChaosStep; remaining: number; fire: (intent: ChaosIntent) => void } | null;
  /** When the last message arrived (draining after a kill). */
  lastMessage: number;
}

let chaos: ChaosRun | null = null;

/**
 * Starts a worker on store `dir` that recovers against `expect` and then runs the seeded workload;
 * resolves with what recovery found once it is verified. A corruption rejects with its message.
 */
export async function chaosBegin(
  kind: StoreKind,
  dir: string,
  seed: number,
  expect: ChaosExpect | null,
  inject: ChaosInjection | null = null,
  mode: ChaosMode = "serial",
): Promise<ChaosRecovery> {
  useStore(kind);
  const worker = await spawnForChaos();
  const channel = new MessageChannel();
  const run: ChaosRun = {
    worker,
    port: channel.port1,
    state: expect?.state ?? null,
    inFlight: new Map(),
    began: new Set(),
    settled: new Set(),
    hold: false,
    failure: null,
    recovered: null,
    armed: null,
    lastMessage: performance.now(),
  };
  chaos = run;
  channel.port1.onmessage = (event: MessageEvent<ChaosMessage>) => {
    const message = event.data;
    run.lastMessage = performance.now();
    if (message.chaos === "recovered") {
      run.recovered = message.recovery;
      run.state = message.recovery.state;
    } else if (message.chaos === "ack") run.state = message.state;
    else if (message.chaos === "failure") run.failure ??= new Error(message.message);
    else if (message.chaos === "began") run.began.add(message.sequence);
    else if (message.chaos === "settled") run.settled.add(message.sequence);
    else if (message.chaos === "done") run.inFlight.delete(message.sequence);
    else {
      if (run.hold) return; // armed for a page close: nothing new starts
      run.inFlight.set(message.intent.sequence, message.intent);
      if (mode === "concurrent") {
        run.port.postMessage({ go: message.intent.sequence }); // at once: lanes stay concurrent
        return;
      }
      const armed = run.armed;
      if (armed !== null && armed.target === message.intent.step) {
        armed.remaining -= 1;
        if (armed.remaining === 0) {
          run.armed = null;
          armed.fire(message.intent);
          return;
        }
      }
      run.port.postMessage({ go: message.intent.sequence });
    }
  };
  worker.post({ chaosPort: channel.port2 }, [channel.port2]);
  worker
    .call({ op: "chaos", dir, seed, expect, mode, ...(inject === null ? {} : { inject }) })
    .catch((error: unknown) => {
      // Only this page's own terminate() is not a failure.
      if (error instanceof Error && error.name === "WorkerTerminated") return;
      run.failure ??= error instanceof Error ? error : new Error(String(error));
    });
  await worker.until(() => run.failure !== null || run.recovered !== null, "recovery", 60_000);
  if (run.failure !== null) {
    worker.terminate();
    throw run.failure;
  }
  const recovered = run.recovered;
  if (recovered === null) throw new Error("recovery reported nothing");
  return recovered;
}

const expectOf = (run: ChaosRun): ChaosExpect => {
  if (run.state === null) throw new Error("no acknowledged state");
  return { state: run.state, inFlight: [...run.inFlight.values()] };
};

function landingOf(run: ChaosRun): string {
  const [current] = run.inFlight.values();
  if (current === undefined) return "between";
  if (!run.began.has(current.sequence)) return `before-${current.step}`;
  if (!run.settled.has(current.sequence)) return `in-${current.step}`;
  return `after-${current.step}`;
}

/** Waits until 30 ms pass without a message from the (dead) worker. */
async function drain(run: ChaosRun): Promise<void> {
  for (;;) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 10);
    });
    if (performance.now() - run.lastMessage > 30) return;
  }
}

/** A workload failure the page observed (review B1-p2f finding 1): the campaign fails on it. */
function failed(run: ChaosRun): Error | null {
  return run.failure === null
    ? null
    : new Error(`the workload reported a failure: ${run.failure.message}`, { cause: run.failure });
}

/**
 * Kills the running chaos worker as `plan` says. Terminate: after `delayMs`, the worker is
 * terminated, the messages it posted before dying are drained, and the landing and expectation
 * are taken from all of them. Page: the page stops answering intents and returns; the caller then
 * calls `chaosPageBoundary` and closes the page. Any workload failure the page observed rejects.
 */
export function chaosKill(plan: KillPlan): Promise<KillLanding | null> {
  const run = chaos;
  if (run === null) return Promise.reject(new Error("chaosBegin first"));
  return new Promise<KillLanding | null>((resolve, reject) => {
    const stall = setTimeout(() => {
      run.armed = null;
      reject(
        failed(run) ??
          new Error(`the workload never reached ${plan.target} #${String(plan.occurrence)}`),
      );
    }, 60_000);
    run.armed = {
      target: plan.target,
      remaining: plan.occurrence,
      fire: (intent) => {
        clearTimeout(stall);
        if (plan.mode === "page") {
          run.hold = true;
          run.port.postMessage({ go: intent.sequence });
          resolve(null);
          return;
        }
        run.port.postMessage({ go: intent.sequence });
        setTimeout(() => {
          run.worker.terminate();
          chaos = null;
          void drain(run).then(() => {
            run.port.close();
            const failure = failed(run);
            if (failure !== null) reject(failure);
            else resolve({ landed: landingOf(run), expect: expectOf(run) });
          });
        }, plan.delayMs);
      },
    };
  });
}

/** Where a concurrent kill landed: the operations whose storage work it interrupted. */
export interface ConcurrentLanding {
  /** Steps that had begun their storage work and not returned from it, sorted. */
  readonly inOperation: readonly ChaosStep[];
  /** Steps announced and not done (in operation, or returned and not yet reported done). */
  readonly inFlight: readonly ChaosStep[];
  readonly expect: ChaosExpect;
}

/**
 * Kills the running concurrent chaos worker `delayMs` after it was armed (a wall-clock moment, not
 * a step boundary), drains what it posted before it died, and returns every acknowledgement and
 * the operations in flight. A workload failure observed at any time rejects.
 */
export async function chaosKillAfter(delayMs: number): Promise<ConcurrentLanding> {
  const run = chaos;
  if (run === null) throw new Error("chaosBegin first");
  await new Promise<void>((resolve) => {
    setTimeout(resolve, delayMs);
  });
  // No operation starts after this: everything the worker does until the kill takes effect was
  // announced (terminate() is asynchronous; what the worker posts meanwhile may be dropped).
  run.hold = true;
  run.worker.terminate();
  chaos = null;
  await drain(run);
  run.port.close();
  const failure = failed(run);
  if (failure !== null) throw failure;
  const intents = [...run.inFlight.values()];
  return {
    inOperation: intents
      .filter((intent) => run.began.has(intent.sequence) && !run.settled.has(intent.sequence))
      .map((intent) => intent.step)
      .sort(),
    inFlight: intents.map((intent) => intent.step).sort(),
    expect: expectOf(run),
  };
}

/**
 * The page-close boundary (review B1-p2f finding 2): the page stops processing the worker's
 * messages and returns what it received up to now; the caller closes the page next, and the page
 * processes nothing in between. Acknowledgements the worker posted after this point never reached
 * the page, so they are not required, while the step in flight may have completed (its root is
 * accepted as pending). A failure observed up to here rejects.
 */
export function chaosPageBoundary(): KillLanding {
  const run = chaos;
  if (run === null) throw new Error("chaosBegin first");
  run.port.onmessage = null;
  run.port.close();
  chaos = null;
  const failure = failed(run);
  if (failure !== null) throw failure;
  return { landed: landingOf(run), expect: expectOf(run) };
}

/** Stops the chaos worker (the campaign is over) and removes its store. */
export async function chaosEnd(kind: StoreKind, dir: string): Promise<void> {
  chaos?.worker.terminate();
  chaos = null;
  useStore(kind);
  await withWorker((worker) => worker.call({ op: "remove", dir }));
}

/**
 * Gate A's reopen at the storage layer (next-storage-reopen.ts): builds a space of `blobs` blobs in
 * `layout` once, then reopens it `repeats` times, each in a fresh worker, and returns the build and
 * every timing.
 */
export async function runReopenBench(
  kind: StoreKind,
  layout: ReopenLayout,
  blobs: number,
  repeats: number,
): Promise<{
  build: ReopenBuild;
  reopens: ReopenTiming[];
  /** IndexedDB only: open's requests timed alone. */
  requests: Record<string, number> | null;
}> {
  useStore(kind);
  const dir = unique(`reopen-${kind}-${layout}`);
  try {
    const build = await withWorker((worker) =>
      worker.call<ReopenBuild>({ op: "reopen-build", dir, layout, blobs }),
    );
    const reopens: ReopenTiming[] = [];
    for (let run = 0; run < repeats; run++)
      reopens.push(
        await withWorker((worker) => worker.call<ReopenTiming>({ op: "reopen-measure", dir })),
      );
    const requests =
      kind === "indexeddb"
        ? await withWorker((worker) =>
            worker.call<Record<string, number>>({ op: "reopen-idb-requests", dir }),
          )
        : null;
    return { build, reopens, requests };
  } finally {
    await withWorker((worker) => worker.call({ op: "remove", dir }));
  }
}
