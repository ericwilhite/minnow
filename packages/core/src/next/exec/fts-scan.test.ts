/** Corpus storage spills before token/stat workspace can escape its admitted step. */
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, FTS_SCAN_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { FtsStatsAccumulator, ftsBm25Row, tokenizeQuery, tokenize } from "../full-text.js";
import { FtsScanOperator, ftsScanMinimum } from "./fts-scan.js";
import { FTS_WORKSPACE_BYTES } from "./fts-value.js";
import { OutputArena } from "./kernels/vectors.js";
import { INT, TEXT, batchesOf, CollectSink, vectorOf, type Row } from "./join-fixtures.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { FlatVector } from "../vector/vector.js";

for (const seed of seedsFor("next-fts-scan-pressure", [0xb7f5, 0xb7f6]))
  it(`spills with exact corpus counts and competing grants (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      types = [INT, TEXT, TEXT];
    const minimum = ftsScanMinimum(types, 1),
      governor = new Governor(48 << 20),
      spill = new MemorySpillService();
    const reach = createReach(`FTS pressure ${String(seed)}`, FTS_SCAN_PRESSURE_REACH);
    reach.beginHistory();
    const grant = await governor.admit({ label: "fts", minimumBytes: minimum, spillable: true });
    const competitor = await governor.admit({
      label: "competitor",
      minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes - 128 * 1024,
      spillable: false,
    });
    expect(competitor.allocate(competitor.held)).toBe(true);
    reach.reach("competing grant");
    const arena = new OutputArena(grant, 2048),
      sink = new CollectSink(3);
    const query: ScalarExpr = {
      op: "literal",
      type: TEXT,
      value: vectorOf(TEXT, ["al* alpha"]) as FlatVector,
    };
    const operator = new FtsScanOperator(
      types,
      [{ columns: [1, 2, 1], query }],
      { arena, input: types, params: { slots: [] } },
      { grant, spill, queryId: seed },
      sink,
    );
    const rows: Row[] = Array.from({ length: 3072 }, (_, id) => [
      id,
      id % 13 === 0 ? null : (random() < 0.5 ? "alpha " : "alpine ") + "x".repeat(2048),
      id % 7 === 0 ? "alpha" : null,
    ]);
    const terms = tokenizeQuery("al* alpha"),
      stats = new FtsStatsAccumulator(terms);
    for (const row of rows)
      stats.addDocument(
        [row[1], row[2], row[1]].flatMap((value) =>
          typeof value === "string" ? tokenize(value) : [],
        ),
      );
    try {
      for (const batch of batchesOf(types, rows, { batchRows: 64, dictionary: true })) {
        expect(operator.push(batch)).toBe("blocked");
        await operator.ready();
      }
      await operator.finish();
      await operator.closed;
      reach.reach("spill");
      expect(operator.profile.spilledBytes).toBeGreaterThan(0);
      reach.reach("df union");
      reach.reach("backpressure");
      expect(sink.rows.map((row) => row[0])).toEqual(rows.map((row) => row[0]));
      expect(sink.rows).toEqual(
        rows.map((row) => [...row, ftsBm25Row(terms, [row[1], row[2], row[1]], stats.stats)]),
      );
      reach.reach("exact scores");
    } finally {
      operator.abort(new Error("cleanup"));
      await operator.closed;
      arena.close();
      grant.close();
      competitor.close();
    }
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    reach.reach("drained");
    reach.assertReached();
  }, 60000);

it("charges tokenizer workspace before constructing corpus state", async () => {
  const types = [INT, TEXT],
    governor = new Governor(32 << 20),
    spill = new MemorySpillService();
  const grant = await governor.admit({
    label: "refusing",
    minimumBytes: ftsScanMinimum(types, 1),
    spillable: false,
  });
  const attempts: number[] = [];
  const charge = {
    allocate: (bytes: number) => {
      attempts.push(bytes);
      return false;
    },
    free: () => undefined,
  };
  const arena = new OutputArena(charge, 2048),
    query: ScalarExpr = {
      op: "literal",
      type: TEXT,
      value: vectorOf(TEXT, ["alpha"]) as FlatVector,
    };
  try {
    expect(
      () =>
        new FtsScanOperator(
          types,
          [{ columns: [1], query }],
          { arena, input: types, params: { slots: [] } },
          { grant, spill, queryId: 1 },
          new CollectSink(),
        ),
    ).toThrow("refused");
    expect(attempts).toEqual([FTS_WORKSPACE_BYTES]);
    expect(grant.allocated).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  } finally {
    arena.close();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("keeps scratch charged and closed pending through abandoned output readiness", async () => {
  const types = [INT, TEXT],
    governor = new Governor(32 << 20),
    spill = new MemorySpillService();
  const grant = await governor.admit({
      label: "cancel-output",
      minimumBytes: ftsScanMinimum(types, 1),
      spillable: true,
    }),
    arena = new OutputArena(grant, 2048);
  let release!: () => void, entered!: () => void;
  const wait = new Promise<void>((resolve) => {
      release = resolve;
    }),
    seen = new Promise<void>((resolve) => {
      entered = resolve;
    });
  const sink = {
    push: () => {
      entered();
      return "blocked" as const;
    },
    ready: () => wait,
    finish: () => undefined,
    abort: () => undefined,
  };
  const query: ScalarExpr = {
    op: "literal",
    type: TEXT,
    value: vectorOf(TEXT, ["alpha"]) as FlatVector,
  };
  const op = new FtsScanOperator(
    types,
    [{ columns: [1], query }],
    { arena, input: types, params: { slots: [] } },
    { grant, spill, queryId: 2 },
    sink,
  );
  const error = new Error("cancel output");
  try {
    for (const batch of batchesOf(types, [
      [1, "alpha"],
      [2, null],
    ])) {
      op.push(batch);
      await op.ready();
    }
    const finish = op.finish();
    void finish.catch(() => undefined);
    await seen;
    op.abort(error);
    let closed = false;
    const closing = op.closed.then(() => {
      closed = true;
    });
    await Promise.resolve();
    await Promise.resolve();
    expect(closed).toBe(false);
    expect(grant.allocated).toBeGreaterThanOrEqual(8 << 20);
    release();
    await expect(finish).rejects.toThrow("cancel output");
    await closing;
  } finally {
    release();
    op.abort(error);
    await op.closed;
    arena.close();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
});
