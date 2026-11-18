/** One independent process of the B8 native-retirement point-read microbenchmark.
 * Bundle this once before and once after the fence change; run balanced AB/BA pairs.
 * PK reads retain their existing completion state; the scalar control must stay synchronous.
 * Neither workload may request a resource-fence promise.
 */
import { StorageCore } from "../packages/core/src/next/testing/storage-core.js";
import { QueryExecution } from "../packages/core/src/next/exec/run.js";
import { createExecutor } from "../packages/core/src/next/exec/create.js";
import { beginTask, taskClock } from "../packages/core/src/next/task.js";

const mode = process.argv[4] ?? "point";
if (mode !== "point" && mode !== "scalar") throw new Error("Expected point or scalar workload");
const iterations = Number(process.argv[2] ?? 25000);
const warmups = Number(process.argv[3] ?? 1000);
if (
  !Number.isSafeInteger(iterations) ||
  iterations < 1 ||
  !Number.isSafeInteger(warmups) ||
  warmups < 1
)
  throw new RangeError("Positive iteration/warmup counts required");
let fenceCalls = 0;
const fence = Reflect.get(QueryExecution.prototype, "settled") as
  ((this: QueryExecution) => Promise<void>) | undefined;
if (fence)
  Object.defineProperty(QueryExecution.prototype, "settled", {
    value: function (this: QueryExecution) {
      fenceCalls++;
      return fence.call(this);
    },
  });
const core = await new StorageCore().open();
try {
  await core.execute("CREATE TABLE points (id INTEGER PRIMARY KEY, v INTEGER)");
  await core.execute("INSERT INTO points VALUES (1, 101)");
  const lowered = core.lowerer.lower(
    mode === "point" ? "SELECT v FROM points WHERE id = $1" : "SELECT CAST($1 AS INTEGER) AS v",
    core.context(),
  );
  if (lowered.kind !== "query") throw new Error("Point read did not lower as SELECT");
  // Existing native key access is a diagnostic opt-in until its separate cutover.
  const prepared = createExecutor({ keyOrder: true }).prepare(lowered.plan);
  for (let i = 0; i < warmups; i++) {
    beginTask();
    const { execution, release } = core.start(prepared, [mode === "point" ? 1 : 101]);
    try {
      for (;;) {
        const frame = await execution.results.next();
        if (!frame) break;
        frame.release();
      }
      await execution.executed;
      await execution.delivered;
    } finally {
      release();
    }
  }
  let digest = 0,
    synchronous = 0;
  const now = taskClock.now;
  taskClock.now = () => 0;
  const before = process.cpuUsage(),
    started = performance.now();
  for (let i = 0; i < iterations; i++) {
    beginTask();
    const { execution, release } = core.start(prepared, [mode === "point" ? 1 : 101]);
    try {
      if (mode === "scalar" && execution.state !== "executed")
        throw new Error("Scalar left the synchronous path");
      for (;;) {
        const frame = mode === "scalar" ? execution.results.poll() : await execution.results.next();
        if (frame === undefined) throw new Error("Point read needed an asynchronous frame");
        if (frame === null) break;
        const column = frame.payload.columns[0];
        const flat = column?.form === "constant" ? column.value : column;
        if (flat?.form !== "flat" || flat.data.layout !== "i32" || frame.payload.rowCount !== 1)
          throw new Error("Point output shape changed");
        digest += flat.data.values[0] ?? 0;
        frame.release();
      }
      const summary = await execution.executed;
      if (summary.profile.synchronous) synchronous++;
      await execution.delivered;
    } finally {
      release();
    }
  }
  taskClock.now = now;
  const cpu = process.cpuUsage(before),
    wallMs = performance.now() - started;
  if (
    (mode === "scalar" && synchronous !== iterations) ||
    digest !== iterations * 101 ||
    fenceCalls !== 0
  )
    throw new Error("Point-read correctness/lazy-fence contract changed");
  await core.close();
  if (core.harness.governor.usedBytes !== 0) throw new Error("Point benchmark retained memory");
  process.stdout.write(
    JSON.stringify({
      pid: process.pid,
      mode,
      iterations,
      warmups,
      digest,
      synchronous,
      fenceCalls,
      cpuMs: (cpu.user + cpu.system) / 1000,
      wallMs,
      peakBytes: core.harness.governor.peakBytes,
    }) + "\n",
  );
} finally {
  await core.close();
}
