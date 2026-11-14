/** The scalar demand contract applies even when tokenization suspends before evaluation. */
import { expect, it } from "vitest";
import { OutputArena } from "./kernels/vectors.js";
import { FtsExpressions } from "./fts-expressions.js";
import { CollectSink, TEXT, vectorOf, batchesOf } from "./join-fixtures.js";
import type { ScalarExpr } from "../ir/ir.js";
import { Governor } from "../memory/governor.js";
import { FTS_WORKSPACE_BYTES } from "./fts-value.js";
import { FtsScanOperator, ftsScanMinimum } from "./fts-scan.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { TaskClock } from "./pipeline.js";
import type { FlatVector } from "../vector/vector.js";
const query: ScalarExpr = {
  op: "literal",
  type: TEXT,
  value: vectorOf(TEXT, ["alpha"]) as FlatVector,
};
const match: ScalarExpr = {
  op: "call",
  fn: "fts_match",
  type: { kind: "bool" },
  args: [query, { op: "column", index: 0, type: TEXT }],
};
it("defers inactive CASE errors and raises demanded document errors", async () => {
  const governor = new Governor(32 << 20);
  for (const demanded of [false, true]) {
    const grant = await governor.admit({
        label: "case",
        minimumBytes: FTS_WORKSPACE_BYTES + 65536,
        spillable: false,
      }),
      arena = new OutputArena(grant, 2048),
      sink = new CollectSink();
    const predicate: ScalarExpr = {
      op: "literal",
      type: { kind: "bool" },
      value: vectorOf({ kind: "bool" }, [demanded]) as FlatVector,
    };
    const expression: ScalarExpr = {
      op: "case",
      type: { kind: "bool" },
      branches: [{ when: predicate, then: match }],
      otherwise: {
        op: "literal",
        type: { kind: "bool" },
        value: vectorOf({ kind: "bool" }, [false]) as FlatVector,
      },
    };
    const op = new FtsExpressions(
      [expression],
      { arena, input: [TEXT], params: { slots: [] } },
      sink,
    );
    try {
      const [batch] = [...batchesOf([TEXT], [["a".repeat(16385)]])];
      if (!batch) throw Error("missing CASE batch");
      op.push(batch);
      if (demanded) await expect(op.ready()).rejects.toThrow("16384");
      else {
        await op.ready();
        await op.finish();
        expect(sink.rows).toEqual([[false]]);
      }
    } finally {
      op.abort(new Error("cleanup"));
      await op.closed;
      arena.close();
      grant.close();
    }
  }
  expect(governor.usedBytes).toBe(0);
});
it("yields within the shared task slice and cancels an unfinished corpus borrow", async () => {
  const governor = new Governor(32 << 20),
    types = [TEXT],
    spill = new MemorySpillService();
  const grant = await governor.admit({
      label: "clock",
      minimumBytes: ftsScanMinimum(types, 1),
      spillable: true,
    }),
    arena = new OutputArena(grant, 2048);
  let reads = 0,
    released = 0;
  const clock = new TaskClock(() => reads++ * 0.001);
  const op = new FtsScanOperator(
    types,
    [{ query, columns: [0] }],
    { arena, input: types, params: { slots: [] } },
    { grant, spill, queryId: 9, clock },
    new CollectSink(),
  );
  try {
    const batch = [
      ...batchesOf(
        types,
        Array.from({ length: 256 }, () => ["alpha ".repeat(2500)]),
      ),
    ][0];
    if (!batch) throw Error("missing clock batch");
    op.push(batch, {
      release: () => {
        released++;
      },
    });
    expect(reads).toBeLessThan(5000);
    expect(released).toBe(0);
    const error = new Error("cancel suspended corpus");
    op.abort(error);
    await expect(op.ready()).rejects.toBe(error);
    await op.closed;
    expect(released).toBe(1);
    expect(grant.allocated).toBeGreaterThan(0);
  } finally {
    op.abort(new Error("cleanup"));
    await op.closed;
    arena.close();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
});
