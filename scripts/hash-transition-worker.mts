/** Already-decoded hash keys and full SQL; each observation owns a fresh VM. */
import { strict as assert } from "node:assert";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";

export default async function create(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const config = context.data as { runtimeRoot: string };
  const load = (name: string) =>
    import(pathToFileURL(join(config.runtimeRoot, "packages/core/dist/next", name + ".js")).href);
  const [
    { GroupTable },
    { HashWorkspace },
    { Governor },
    { TEXT, texts },
    { StorageCore },
    { oracleRows },
  ] = (await Promise.all([
    load("exec/agg/groups"),
    load("exec/agg/hash-workspace"),
    load("memory/governor"),
    load("exec/kernels/test-support"),
    load("testing/storage-core"),
    load("testing/differential-adapter"),
  ])) as [
    typeof import("../packages/core/dist/next/exec/agg/groups.js"),
    typeof import("../packages/core/dist/next/exec/agg/hash-workspace.js"),
    typeof import("../packages/core/dist/next/memory/governor.js"),
    typeof import("../packages/core/dist/next/exec/kernels/test-support.js"),
    typeof import("../packages/core/dist/next/testing/storage-core.js"),
    typeof import("../packages/core/dist/next/testing/differential-adapter.js"),
  ];
  const rows = 32768,
    groups = 8192,
    batchRows = 2048;
  const key = (i: number) => "account-" + String(i % groups).padStart(8, "0");
  const batches = Array.from({ length: rows / batchRows }, (_, b) =>
    texts(Array.from({ length: batchRows }, (_, i) => key(b * batchRows + i))),
  );
  const ids = new Int32Array(batchRows);
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
    const governor = new Governor(256 << 20);
    const grant = await governor.admit({
      label: "hash component",
      minimumBytes: 1 << 20,
      spillable: true,
    });
    const workspace = new HashWorkspace(grant, grant.held);
    const table = new GroupTable([TEXT], workspace);
    try {
      for (let b = 0; b < batches.length; b++) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion -- fixed dense fixture
        const vector = batches[b]!;
        assert.equal(
          await workspace.prepare(workspace.used + table.growthBytes([vector], batchRows)),
          true,
        );
        table.assign([vector], batchRows, null, ids);
        if (check)
          for (let i = 0; i < batchRows; i++) assert.equal(ids[i], (b * batchRows + i) % groups);
      }
    } finally {
      table.close();
      workspace.close();
      grant.close();
      assert.equal(governor.usedBytes, 0);
    }
  }
  async function query(check: boolean): Promise<void> {
    const result = await core.execute(sql);
    const values = oracleRows(result);
    assert.equal(values.length, groups);
    if (check) {
      const seen = new Set(values.map((r) => r.k));
      assert.equal(seen.size, groups);
      for (let i = 0; i < groups; i++) assert.equal(seen.has(key(i)), true);
      for (const r of values) assert.equal(r.n, 4);
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
        fingerprint: "8192 keys, count4, dense first-seen ids",
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
