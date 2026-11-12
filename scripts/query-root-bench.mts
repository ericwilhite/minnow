/** Loaded-host AB/BA comparison over two frozen complete cores. Setup/hash are outside CPU.
 * Uses next-executor-quiet's real storage/executor worker, but makes no quiet-host claim.
 * --baseline/--candidate are freeze-next-runtime outputs. --check uses seven rows, no timings.
 */
import { createHash } from "node:crypto";
import { fork } from "node:child_process";
import { mkdir, writeFile, appendFile, access, readFile } from "node:fs/promises";
import { mkdtemp } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { pairedCPU } from "./lib/paired-cpu.mts";
const option = (key: string, fallback = "") => {
  const at = process.argv.indexOf(key);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
interface Sample {
  cpuMs: number;
  wallMs: number;
  digest: string;
  rows: number;
  load: number[];
  repeats: number;
  completed: number;
  totalRows: number;
}
interface Reply {
  ready?: boolean;
  id?: number;
  ok?: boolean;
  error?: string;
  value?: Sample;
}
const checking = process.argv.includes("--check");
const queries = option(
  "--only",
  "scan_plain3,scan_proj3_agg,agg_count_distinct,win_running_sum",
).split(",");
const repeats = Number(option("--repeats", "1")),
  warmups = Number(option("--warmups", "1"));
if (
  !Number.isSafeInteger(repeats) ||
  repeats < 1 ||
  repeats > 1000 ||
  !Number.isSafeInteger(warmups) ||
  warmups < 1 ||
  warmups > 1000
)
  throw new Error("Invalid observation policy");
const rows = checking ? 7 : Number(option("--rows", "1000000"));
if (!Number.isSafeInteger(rows) || rows < 1) throw new Error("Invalid row count");
const output = option("--output") || (await mkdtemp(join(tmpdir(), "minnow-query-roots-")));
await mkdir(output, { recursive: true });
if (
  await access(join(output, "samples.jsonl")).then(
    () => true,
    () => false,
  )
)
  throw new Error("Refusing to overwrite evidence");
async function start(root: string) {
  if (!root) throw new Error("Frozen --baseline and --candidate roots required");
  const child = fork(
    fileURLToPath(new URL("./next-executor-quiet.mts", import.meta.url)),
    ["--arm", resolve(root), "--rows", String(rows), "--only", queries.join(",")],
    { execArgv: [], serialization: "advanced", stdio: ["ignore", "inherit", "inherit", "ipc"] },
  );
  let serial = 0;
  const requests = new Map<number, { resolve(value: Sample): void; reject(error: Error): void }>();
  let readyResolve: (value: Reply) => void = () => undefined;
  let readyReject: (error: Error) => void = () => undefined;
  const ready = new Promise<Reply>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const exited = new Promise<void>((resolveExit, rejectExit) => {
    child.once("exit", (code, signal) => {
      const error = new Error(`Worker exited ${String(code)}/${String(signal)}`);
      readyReject(error);
      for (const job of requests.values()) job.reject(error);
      requests.clear();
      if (code === 0 && signal === null) resolveExit();
      else rejectExit(error);
    });
  });
  // Install rejection handling immediately; close below still checks this exact fence.
  void exited.catch(() => undefined);
  child.on("message", (message: Reply) => {
    if (message.ready) {
      readyResolve(message);
      return;
    }
    const job = requests.get(message.id ?? -1);
    if (!job) return;
    requests.delete(message.id ?? -1);
    if (message.ok && message.value) job.resolve(message.value);
    else job.reject(new Error(message.error ?? "Invalid worker reply"));
  });
  const ask = (message: Record<string, unknown>) =>
    new Promise<Sample>((resolve, reject) => {
      const id = ++serial;
      requests.set(id, { resolve, reject });
      child.send({ ...message, id }, (error) => {
        if (error) {
          requests.delete(id);
          reject(error);
        }
      });
    });
  try {
    return { ask, exited, identity: await ready };
  } catch (error) {
    await exited.catch(() => undefined);
    throw error;
  }
}
const workers: Array<Awaited<ReturnType<typeof start>>> = [];
let cleanupError: Error | undefined;
try {
  // Serialize fixture creation; no concurrent builds/browser lanes or two ingests.
  workers.push(await start(option("--baseline")));
  workers.push(await start(option("--candidate")));
  await writeFile(
    join(output, "identity.json"),
    JSON.stringify(
      {
        rows,
        queries,
        checking,
        repeats,
        warmups,
        sources: Object.fromEntries(
          await Promise.all(
            [
              "query-root-bench.mts",
              "next-executor-quiet.mts",
              "lib/paired-cpu.mts",
              "next-executor-workload/gen.mjs",
              "next-executor-workload/queries.mjs",
            ].map(async (file) => [
              file,
              createHash("sha256")
                .update(await readFile(new URL(file, import.meta.url)))
                .digest("hex"),
            ]),
          ),
        ),
        workers: workers.map((w) => w.identity),
      },
      null,
      2,
    ),
  );
  if (process.argv.includes("--await-start")) {
    console.log("Fixtures ready; send a newline to start measurements.");
    process.stdin.resume();
    await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
    process.stdin.pause();
  }
  const records = [];
  for (const query of queries) {
    const run = async (
      arm: number,
      timed: boolean,
      boundary = "frames",
      count = timed ? repeats : 1,
    ) => {
      const worker = workers[arm];
      if (!worker) throw new Error("Missing arm");
      const sample = await worker.ask({ op: "run", query, timed, boundary, repeats: count });
      if (
        sample.repeats !== count ||
        sample.completed !== count ||
        sample.totalRows !== sample.rows * count
      )
        throw new Error("Observation executed the wrong amount of work");
      return sample;
    };
    const verify = async () => {
      const a = await run(0, false, "verify"),
        b = await run(1, false, "verify");
      if (a.digest !== b.digest || a.rows !== b.rows) throw new Error(`Output mismatch ${query}`);
      return { rows: a.rows, digest: a.digest };
    };
    const before = await verify();
    const a: Sample[] = [],
      b: Sample[] = [];
    if (!checking) {
      for (let warmup = 0; warmup < warmups; warmup++) {
        for (const arm of warmup % 2 === 0 ? [0, 1] : [1, 0]) {
          const sample = await run(arm, true, "frames", 1);
          await appendFile(
            join(output, "warmups.jsonl"),
            JSON.stringify({ query, warmup, arm, sample }) + "\n",
          );
        }
      }
      for (let pair = 0; pair < 9; pair++) {
        for (const arm of pair % 2 === 0 ? [0, 1] : [1, 0]) {
          const sample = await run(arm, true);
          (arm === 0 ? a : b).push(sample);
          await appendFile(
            join(output, "samples.jsonl"),
            JSON.stringify({ query, pair, arm, sample }) + "\n",
          );
        }
      }
    }
    const after = await verify();
    if (before.digest !== after.digest) throw new Error("Fixture/output drift");
    const record = {
      query,
      before,
      after,
      a,
      b,
      cpu: checking
        ? null
        : pairedCPU(
            a.map((x) => x.cpuMs),
            b.map((x) => x.cpuMs),
          ),
      wall: checking
        ? null
        : pairedCPU(
            a.map((x) => x.wallMs),
            b.map((x) => x.wallMs),
          ),
    };
    records.push(record);
    await writeFile(join(output, "results.json"), JSON.stringify(records, null, 2));
    console.log(JSON.stringify({ query, cpu: record.cpu, wall: record.wall }));
  }
} finally {
  const closed = await Promise.allSettled(
    workers.map(async (worker) => {
      await worker.ask({ op: "close" });
      await worker.exited;
    }),
  );
  await writeFile(join(output, "cleanup.json"), JSON.stringify(closed, null, 2));
  if (closed.some((result) => result.status === "rejected"))
    cleanupError = new Error("Worker cleanup failed");
}
if (cleanupError) throw cleanupError;
