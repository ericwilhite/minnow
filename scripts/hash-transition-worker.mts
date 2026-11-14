/** Native hybrid hash and full SQL; decoded input and storage setup are outside the clock. */
import { strict as assert } from "node:assert";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import type { Rel } from "../packages/core/dist/next/ir/ir.js";

export default async function create(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const config = context.data as { runtimeRoot: string };
  const load = (name: string) =>
    import(pathToFileURL(join(config.runtimeRoot, "packages/core/dist/next", name + ".js")).href);
  const [
    { HybridAggregateOperator },
    { Governor },
    { TEXT, I64, texts, valueAt },
    { OutputArena },
    { MemorySpillService },
    { drive },
    { ArraySource },
    { StorageCore },
    { oracleRows },
  ] = (await Promise.all([
    load("exec/agg/hybrid"),
    load("memory/governor"),
    load("exec/kernels/test-support"),
    load("exec/kernels/vectors"),
    load("spill/memory-spill"),
    load("exec/pipeline"),
    load("exec/join-fixtures"),
    load("testing/storage-core"),
    load("testing/differential-adapter"),
  ])) as [
    typeof import("../packages/core/dist/next/exec/agg/hybrid.js"),
    typeof import("../packages/core/dist/next/memory/governor.js"),
    typeof import("../packages/core/dist/next/exec/kernels/test-support.js"),
    typeof import("../packages/core/dist/next/exec/kernels/vectors.js"),
    typeof import("../packages/core/dist/next/spill/memory-spill.js"),
    typeof import("../packages/core/dist/next/exec/pipeline.js"),
    typeof import("../packages/core/dist/next/exec/join-fixtures.js"),
    typeof import("../packages/core/dist/next/testing/storage-core.js"),
    typeof import("../packages/core/dist/next/testing/differential-adapter.js"),
  ];
  const rows = 32768,
    groups = 8192,
    batchRows = 2048;
  const key = (i: number) => "account-" + String(i % groups).padStart(8, "0");
  const batches = Array.from({ length: rows / batchRows }, (_, b) => ({
    length: batchRows,
    selection: null,
    columns: [texts(Array.from({ length: batchRows }, (_, i) => key(b * batchRows + i)))],
  }));
  const rel: Extract<Rel, { op: "aggregate" }> = {
    op: "aggregate",
    id: 2,
    input: { op: "values", id: 1, rows: [], output: [] },
    output: [],
    groupBy: [0],
    groupingSets: null,
    aggregates: [
      { fn: "count_star", args: [], type: I64, filter: null, distinct: false, orderBy: [] },
    ],
  };
  const core = new StorageCore();
  await core.open();
  await core.execute("CREATE TABLE hash_step(k TEXT)");
  for (let i = 0; i < rows; i += batchRows)
    await core.writeBatch(
      "insertBatch",
      "hash_step",
      Array.from({ length: batchRows }, (_, j) => ({ k: key(i + j) })),
    );
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
  const sql = "SELECT k,count(*) n FROM hash_step GROUP BY k";
  async function component(check: boolean): Promise<void> {
    const gov = new Governor(256 << 20),
      hashMinimum = 512 << 10,
      ioMinimum = 2 << 20;
    const parts = await gov.admitGroup({
      label: "native hash",
      grants: [
        { label: "fixed", minimumBytes: hashMinimum + ioMinimum, spillable: true },
        { label: "hash growth", minimumBytes: 0, spillable: true },
        { label: "io growth", minimumBytes: 0, spillable: true },
      ],
    });
    const grant = parts[0];
    if (!grant) throw new Error("Missing native grant");
    const spill = new MemorySpillService(),
      arena = new OutputArena(grant, 2048);
    let seen = 0;
    const operator = new HybridAggregateOperator(
      rel,
      { arena, input: [TEXT], params: { slots: [] } },
      {
        grant,
        extras: parts.slice(1),
        hashMinimum,
        ioMinimum,
        spill,
        queryId: 1,
        signal: context.signal,
      },
      {
        push(batch) {
          const count = batch.selection?.count ?? batch.length;
          if (check) {
            const keys = batch.columns[0],
              counts = batch.columns[1];
            if (!keys || !counts) throw new Error("Missing output");
            for (let i = 0; i < count; i++) {
              const row = batch.selection?.rows[i] ?? i;
              assert.equal(valueAt(keys, row), key(seen + i));
              assert.equal(Number(valueAt(counts, row)), 4);
            }
          }
          seen += count;
          return "ready";
        },
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      },
    );
    try {
      await drive(new ArraySource(batches), operator);
      assert.equal(seen, groups);
      assert.equal(operator.profile.spilledBytes, 0);
    } finally {
      operator.abort(new Error("benchmark cleanup"));
      await operator.closed;
      arena.close();
      for (const part of parts) part.close();
      assert.equal(gov.usedBytes, 0);
      assert.equal(spill.bytesInUse, 0);
    }
  }
  async function query(check: boolean): Promise<void> {
    const result = await core.execute(sql);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error("Missing grouped rows");
    const values = oracleRows(result);
    assert.equal(values.length, groups);
    if (check) {
      const seen = new Set(values.map((r) => r.k));
      assert.equal(seen.size, groups);
      for (let i = 0; i < groups; i++) assert.equal(seen.has(key(i)), true);
      for (const row of values) assert.equal(row.n, 4);
    }
    assert.equal(core.spill.bytesInUse, 0);
  }
  let warm = false;
  return {
    revision: context.revision,
    identity: {
      rows,
      groups,
      batchRows,
      key: "account-NNNNNNNN",
      component: "Hybrid COUNT, admission through emission/cleanup",
      componentIterations: 50,
      queryLifetimes: 10,
      warmups: 10,
    },
    workloads: ["short-text-hash"],
    boundaries: ["operator", "query"],
    async verify() {
      await component(true);
      await query(true);
      if (!warm) {
        for (let i = 0; i < 10; i++) {
          await component(false);
          await query(false);
        }
        warm = true;
      }
      return {
        fingerprint: "8192keys,count4,first-seen order",
        rows: rows * (context.boundary === "operator" ? 50 : 10),
      };
    },
    async run(_id, boundary) {
      const iterations = boundary === "operator" ? 50 : 10;
      for (let i = 0; i < iterations; i++) {
        context.signal.throwIfAborted();
        if (boundary === "operator") await component(false);
        else await query(false);
      }
      return { rows: rows * iterations, detail: { iterations } };
    },
    async close() {
      await core.close();
    },
  };
}
