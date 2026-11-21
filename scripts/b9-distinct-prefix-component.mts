/** Actual W h01/h05 inputs. Setup and exact models stay outside CPU. State
 * construction, growth-certificate reads, filtering and close are inside the
 * state clock; ordinary SQL through released frames is a separate component.
 */
import { strict as assert } from "node:assert";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { pairedCPU } from "./lib/paired-cpu.mts";
const args = process.argv.slice(2),
  option = (name: string, fallback: string) => {
    const i = args.indexOf(name);
    return i < 0 ? fallback : (args[i + 1] ?? fallback);
  },
  parent = option("--parent-core", ""),
  candidate = option("--candidate-core", "packages/core/dist"),
  total = Number(option("--rows", "65536")),
  executionsPerArm = Number(option("--executions-per-arm", "16"));
if (!parent) throw new Error("--parent-core required");
if (!Number.isSafeInteger(executionsPerArm) || executionsPerArm < 4 || executionsPerArm > 16)
  throw new Error("Bounded executions per arm required");
if (!Number.isSafeInteger(total) || total < 2048 || total > 65536)
  throw new Error("Bounded local rows required");
const gen = (await import(new URL("./next-executor-workload/gen.mjs", import.meta.url).href)) as {
    value(spec: { fam: string; k: number }, i: number, n: number): string;
  },
  values = [1, 5].map((k) =>
    Array.from({ length: total }, (_, i) => gen.value({ fam: "h", k }, i, total)),
  ),
  expected = values.map((v) => new Set(v).size);
async function load(root: string) {
  const url = (f: string) => pathToFileURL(resolve(root, f)).href,
    { ScalarByteDistinct } = (await import(url("next/exec/agg/scalar-byte-distinct.js"))) as {
      ScalarByteDistinct: new (
        ...args: ConstructorParameters<
          typeof import("../packages/core/src/next/exec/agg/scalar-byte-distinct.js").ScalarByteDistinct
        >
      ) => InstanceType<
        typeof import("../packages/core/src/next/exec/agg/scalar-byte-distinct.js").ScalarByteDistinct
      > & {
        readonly bitmapCount?: number;
        readonly bitmapPages?: number;
      };
    },
    { Governor } = (await import(
      url("next/memory/governor.js")
    )) as typeof import("../packages/core/dist/next/memory/governor.js"),
    { texts, TEXT } = (await import(
      url("next/exec/kernels/test-support.js")
    )) as typeof import("../packages/core/dist/next/exec/kernels/test-support.js"),
    { StorageCore } = (await import(
      url("next/testing/storage-core.js")
    )) as typeof import("../packages/core/dist/next/testing/storage-core.js"),
    { oracleRows } = (await import(
      url("next/testing/differential-adapter.js")
    )) as typeof import("../packages/core/dist/next/testing/differential-adapter.js");
  return {
    ScalarByteDistinct,
    Governor,
    TEXT,
    StorageCore,
    oracleRows,
    inputs: values.map((v) =>
      Array.from({ length: Math.ceil(total / 2048) }, (_, b) =>
        texts(v.slice(b * 2048, (b + 1) * 2048)),
      ),
    ),
  };
}
const before = await load(parent),
  after = await load(candidate);
type Engine = typeof before;
async function state(engine: Engine, columns: readonly number[]) {
  const governor = new engine.Governor(32 << 20),
    grant = await governor.admit({
      label: "B9 DISTINCT state",
      minimumBytes: 8 << 20,
      spillable: true,
    });
  assert.ok(grant.allocate(6 * 2048 + 4 * 64));
  let peakAllocated = grant.allocated;
  const charge = {
      allocate(bytes: number) {
        const ok = grant.allocate(bytes);
        peakAllocated = Math.max(peakAllocated, grant.allocated);
        return ok;
      },
      free(bytes: number) {
        grant.free(bytes);
      },
    },
    gids = new Int32Array(2048),
    out = { rows: new Uint16Array(2048), count: 0 },
    start = process.cpuUsage(),
    sets = columns.map(() => new engine.ScalarByteDistinct(engine.TEXT, charge));
  let certificateBytes = 0;
  try {
    for (const [i, c] of columns.entries()) {
      const set = sets[i];
      if (!set) throw new Error("Missing set");
      for (const vector of engine.inputs[c] ?? []) {
        certificateBytes += set.growthBytes(vector, vector.length);
        set.filter(vector, gids, vector.length, null, true, out);
      }
    }
    const counts = sets.map((s) => s.count),
      bitmap = sets.map((s) => ({ count: s.bitmapCount ?? 0, pages: s.bitmapPages ?? 0 }));
    for (const set of sets) set.close();
    const used = process.cpuUsage(start);
    assert.deepEqual(
      counts,
      columns.map((c) => expected[c]),
    );
    assert.ok(Number.isFinite(certificateBytes));
    return { cpuMs: (used.user + used.system) / 1000, peakAllocated, bitmap };
  } finally {
    for (const set of sets) set.close();
    grant.free(grant.allocated);
    grant.close();
    assert.equal(governor.usedBytes, 0);
  }
}
async function pairs<T extends { cpuMs: number }>(run: (candidate: boolean) => Promise<T>) {
  for (let i = 0; i < 8; i++) {
    await run(false);
    await run(true);
  }
  const block = async (candidate: boolean) => {
      const runs = [];
      for (let i = 0; i < executionsPerArm; i++) runs.push(await run(candidate));
      return { cpuMs: runs.reduce((n, r) => n + r.cpuMs, 0) / runs.length, runs };
    },
    samples = [];
  for (let round = 0; round < 9; round++) {
    let control, candidate;
    if (round % 2 === 0) {
      control = await block(false);
      candidate = await block(true);
    } else {
      candidate = await block(true);
      control = await block(false);
    }
    samples.push({ round, order: round % 2 === 0 ? "AB" : "BA", control, candidate });
  }
  return {
    samples,
    comparison: pairedCPU(
      samples.map((s) => s.control.cpuMs),
      samples.map((s) => s.candidate.cpuMs),
    ),
  };
}
for (const columns of [[0], [1], [0, 1]])
  console.log(
    JSON.stringify({
      phase: "state",
      rows: total,
      fields: columns.map((c) => (c === 0 ? "h01" : "h05")),
      clock: "local process user+system CPU",
      warmups: 8,
      executionsPerArm,
      ...(await pairs((c) => state(c ? after : before, columns))),
    }),
  );
if (!args.includes("--state-only")) {
  async function database(engine: Engine) {
    const core = await new engine.StorageCore(65536).open();
    try {
      await core.execute("CREATE TABLE events_wide(id INT PRIMARY KEY,h01 TEXT,h05 TEXT)");
      await core.writeBatch(
        "insertBatch",
        "events_wide",
        Array.from({ length: total }, (_, i) => {
          const h01 = values[0]?.[i],
            h05 = values[1]?.[i];
          assert.equal(typeof h01, "string");
          assert.equal(typeof h05, "string");
          assert(h01 !== undefined && h05 !== undefined);
          return { id: i + 1, h01, h05 };
        }),
      );
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      return { core, engine };
    } catch (error) {
      await core.close();
      throw error;
    }
  }
  const dbBefore = await database(before);
  let dbAfter: Awaited<ReturnType<typeof database>>;
  try {
    dbAfter = await database(after);
  } catch (error) {
    await dbBefore.core.close();
    throw error;
  }
  try {
    const run = async (candidate: boolean) => {
      const { core, engine } = candidate ? dbAfter : dbBefore,
        start = process.cpuUsage(),
        result = await core.execute(
          "SELECT COUNT(DISTINCT h01) AS u_email,COUNT(DISTINCT h05) AS u_acct FROM events_wide",
        ),
        used = process.cpuUsage(start);
      if (!("batches" in result)) throw new Error("Missing result");
      assert.deepEqual(engine.oracleRows(result), [{ u_email: expected[0], u_acct: expected[1] }]);
      assert.equal(core.spill.bytesInUse, 0);
      return { cpuMs: (used.user + used.system) / 1000, peakHeld: core.lastProfile?.peakHeldBytes };
    };
    console.log(
      JSON.stringify({
        phase: "SQL",
        cell: "agg_count_distinct",
        rows: total,
        clock: "local process user+system CPU through released frames",
        warmups: 8,
        executionsPerArm,
        ...(await pairs(run)),
      }),
    );
  } finally {
    await dbBefore.core.close();
    await dbAfter.core.close();
    assert.equal(dbBefore.core.harness.governor.usedBytes, 0);
    assert.equal(dbAfter.core.harness.governor.usedBytes, 0);
  }
}
