/**
 * The bounded paths of the streamed mutation replay. A delta too large for its patches to fit a
 * query's fair share of its budget is replayed per range of slots as the scan reaches it, and a
 * delta whose touched keys do not fit the replay's scratch is replayed in key-hash partitions.
 * Both must return exactly what the in-memory replay returns, in the same order, for every scan
 * feature: projections, aggregates, ordering, index-selected rows, layered partial updates,
 * deletes, upserts of new keys deleted again, and reads of older versions.
 *
 * Small tables force the paths through test hooks, and one test reaches both through a real
 * small budget.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { overlayReplayTestHooks } from "./overlay-replay.js";
import { pointReadTestHooks } from "./point-read.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const REGIONS = ["west", "east", "north", "south"] as const;

interface Row {
  // Batch inputs take records with an index signature.
  [column: string]: string | number | null;
  id: number;
  region: string;
  amount: number;
  note: string | null;
}

/** Applies a partial update to one modeled row. */
function patchModel(model: Map<number, Row>, id: number, changes: Partial<Row>): void {
  const row = model.get(id);
  if (row === undefined) throw new Error(`Modeled row is missing: ${String(id)}`);
  model.set(id, { ...row, ...changes } as Row);
}

/** A keyed table and a model of it, changed together. */
async function historyTable(database: MinnowDatabase, rows: number) {
  await database.createTable({
    name: "t",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
      { name: "amount", type: "number" },
      { name: "note", type: "string", nullable: true },
    ],
  });
  const model = new Map<number, Row>();
  const initial: Row[] = Array.from({ length: rows }, (_, id) => ({
    id,
    region: REGIONS[id % 4] ?? "west",
    amount: id,
    note: `n${String(id % 13)}`,
  }));
  // Two inserts, so the scan spans segments before any delta lands.
  const half = Math.floor(rows / 2);
  await database.insertBatch("t", initial.slice(0, half));
  await database.insertBatch("t", initial.slice(half));
  for (const row of initial) model.set(row.id, { ...row });
  const versions: Array<{ version: number | null; model: Map<number, Row> }> = [];
  const snapshot = (version: number | null) =>
    versions.push({ version, model: new Map([...model].map(([id, row]) => [id, { ...row }])) });

  // Partial updates of different columns, layered on the same rows.
  const amountKeys = initial.filter((row) => row.id % 3 === 0).map((row) => row.id);
  await database.updateBatch("t", {
    keys: amountKeys,
    changes: { amount: amountKeys.map((id) => id * 10) },
  });
  for (const id of amountKeys) patchModel(model, id, { amount: id * 10 });
  const noteKeys = initial.filter((row) => row.id % 5 === 0).map((row) => row.id);
  const noted = await database.updateBatch("t", {
    keys: noteKeys,
    changes: { note: noteKeys.map((id) => (id % 2 === 0 ? null : `u${String(id)}`)) },
  });
  for (const id of noteKeys) {
    patchModel(model, id, { note: id % 2 === 0 ? null : `u${String(id)}` });
  }
  snapshot(noted.version);

  // Full upserts over some of them, written in an order unrelated to the table's.
  const upserted = initial
    .filter((row) => row.id % 7 === 0)
    .map((row) => ({
      id: row.id,
      region: REGIONS[(row.id + 1) % 4] ?? "west",
      amount: -row.id,
      note: `v${String(row.id % 11)}`,
    }))
    .reverse();
  await database.upsertBatch("t", upserted);
  for (const row of upserted) model.set(row.id, { ...row });

  // A partial update after the upsert, then deletes.
  const lateKeys = initial.filter((row) => row.id % 11 === 0).map((row) => row.id);
  await database.updateBatch("t", { keys: lateKeys, changes: { amount: lateKeys.map(() => 7) } });
  for (const id of lateKeys) patchModel(model, id, { amount: 7 });
  const deleted = initial.filter((row) => row.id % 17 === 0).map((row) => row.id);
  await database.deleteBatch("t", { keys: deleted });
  for (const id of deleted) model.delete(id);

  // New keys upserted, half of them deleted again, a few deleted keys written back.
  const fresh = Array.from({ length: Math.floor(rows / 8) }, (_, index) => ({
    id: rows + index,
    region: "north",
    amount: index,
    note: null,
  }));
  await database.upsertBatch("t", fresh);
  for (const row of fresh) model.set(row.id, { ...row });
  const dropped = fresh.filter((row) => row.id % 2 === 0).map((row) => row.id);
  await database.deleteBatch("t", { keys: dropped });
  for (const id of dropped) model.delete(id);
  const back = deleted
    .filter((id) => id % 34 === 0)
    .map((id) => ({ id, region: "south", amount: 1_000 + id, note: "back" }));
  const restored = await database.upsertBatch("t", back);
  for (const row of back) model.set(row.id, { ...row });
  snapshot(restored.version);
  return { model, versions };
}

const QUERIES = [
  "SELECT * FROM t",
  "SELECT id, note FROM t",
  "SELECT COUNT(*) AS n, SUM(amount) AS total, COUNT(note) AS notes FROM t",
  "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP BY region ORDER BY region",
  "SELECT id, amount FROM t ORDER BY amount DESC, id LIMIT 40",
  "SELECT id, region FROM t WHERE amount > 500 ORDER BY id",
  "SELECT id, amount FROM t WHERE id IN (0, 21, 33, 34, 35, 70, 77, 3000, 3002, 3003) ORDER BY id",
  "SELECT id, note FROM t WHERE note IS NULL ORDER BY id",
] as const;

function expectedRows(model: ReadonlyMap<number, Row>, sql: (typeof QUERIES)[number]): unknown[] {
  const rows = [...model.values()].sort((left, right) => left.id - right.id);
  switch (sql) {
    case "SELECT * FROM t":
      return rows;
    case "SELECT id, note FROM t":
      return rows.map(({ id, note }) => ({ id, note }));
    case "SELECT COUNT(*) AS n, SUM(amount) AS total, COUNT(note) AS notes FROM t":
      return [
        {
          n: rows.length,
          total: rows.reduce((sum, row) => sum + row.amount, 0),
          notes: rows.filter((row) => row.note !== null).length,
        },
      ];
    case "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP BY region ORDER BY region":
      return [...REGIONS].sort().flatMap((region) => {
        const members = rows.filter((row) => row.region === region);
        return members.length === 0
          ? []
          : [
              {
                region,
                n: members.length,
                total: members.reduce((sum, row) => sum + row.amount, 0),
              },
            ];
      });
    case "SELECT id, amount FROM t ORDER BY amount DESC, id LIMIT 40":
      return [...rows]
        .sort((left, right) => right.amount - left.amount || left.id - right.id)
        .slice(0, 40)
        .map(({ id, amount }) => ({ id, amount }));
    case "SELECT id, region FROM t WHERE amount > 500 ORDER BY id":
      return rows.filter((row) => row.amount > 500).map(({ id, region }) => ({ id, region }));
    case "SELECT id, amount FROM t WHERE id IN (0, 21, 33, 34, 35, 70, 77, 3000, 3002, 3003) ORDER BY id":
      return rows
        .filter((row) => [0, 21, 33, 34, 35, 70, 77, 3000, 3002, 3003].includes(row.id))
        .map(({ id, amount }) => ({ id, amount }));
    case "SELECT id, note FROM t WHERE note IS NULL ORDER BY id":
      return rows.filter((row) => row.note === null).map(({ id, note }) => ({ id, note }));
  }
}

/** Rows of an unordered scan, by key, for comparison with the model. */
function byId(rows: ReadonlyArray<Record<string, unknown>>): unknown[] {
  return [...rows].sort((left, right) => Number(left.id) - Number(right.id));
}

const BUDGET = 8 * 1024 * 1024;

async function runAll(
  database: MinnowDatabase,
  version?: number | null,
): Promise<Map<string, Array<Record<string, unknown>>>> {
  const results = new Map<string, Array<Record<string, unknown>>>();
  for (const sql of QUERIES) {
    const result = await database.query(sql, {
      memoize: false,
      executionMemoryBudgetBytes: BUDGET,
      ...(version === undefined ? {} : { version }),
    });
    results.set(sql, result.rows);
  }
  return results;
}

function expectModel(
  results: ReadonlyMap<string, Array<Record<string, unknown>>>,
  model: ReadonlyMap<number, Row>,
  label: string,
): void {
  for (const sql of QUERIES) {
    const rows = results.get(sql) ?? [];
    const actual =
      sql === "SELECT * FROM t" || sql === "SELECT id, note FROM t" ? byId(rows) : rows;
    expect(actual, `${label} :: ${sql}`).toEqual(expectedRows(model, sql));
  }
}

afterEach(() => {
  overlayReplayTestHooks.fairShareBytes = undefined;
  overlayReplayTestHooks.scratchBytes = undefined;
  pointReadTestHooks.disabled = false;
});

describe("bounded streamed mutation replay", () => {
  it("replays patches per slot range when they exceed the fair share, matching the in-memory replay", async () => {
    const store = new MemoryBlockStore();
    const options = { autoCompact: false, autoCollect: false, rowsPerBlock: 256 } as const;
    const database = new MinnowDatabase(store, options);
    try {
      const { model, versions } = await historyTable(database, 3_000);
      const inMemory = await runAll(database);
      expectModel(inMemory, model, "in memory");
      // Keep nothing: every patched window replays its range, ranges one block long.
      overlayReplayTestHooks.fairShareBytes = 0;
      const ranges = overlayReplayTestHooks.rangeReplays;
      const bounded = await runAll(database);
      expect(overlayReplayTestHooks.rangeReplays).toBeGreaterThan(ranges + 10);
      for (const sql of QUERIES) {
        // Same rows in the same order: the bounded path is the same scan, not a re-sort.
        expect(bounded.get(sql), sql).toEqual(inMemory.get(sql));
      }
      for (const { version, model: then } of versions) {
        expectModel(await runAll(database, version), then, `version ${String(version)}`);
      }
    } finally {
      await database.close();
    }
  });

  it("replays touched keys in hash partitions when they exceed the scratch, matching a single pass", async () => {
    const store = new MemoryBlockStore();
    const options = { autoCompact: false, autoCollect: false, rowsPerBlock: 256 } as const;
    const writer = new MinnowDatabase(store, options);
    const { model, versions } = await historyTable(writer, 3_000);
    const inMemory = await runAll(writer);
    await writer.close();
    for (const keep of [undefined, 0]) {
      // A fresh connection builds the replay again; a few kilobytes of scratch is a few
      // hundred keys a pass.
      const database = new MinnowDatabase(store, options);
      try {
        overlayReplayTestHooks.scratchBytes = 8 * 1024;
        overlayReplayTestHooks.fairShareBytes = keep;
        const partitioned = overlayReplayTestHooks.partitionedBuilds;
        const results = await runAll(database);
        expect(overlayReplayTestHooks.partitionedBuilds).toBeGreaterThan(partitioned);
        for (const sql of QUERIES) {
          expect(results.get(sql), `keep ${String(keep)} :: ${sql}`).toEqual(inMemory.get(sql));
        }
        expectModel(results, model, `partitioned, keep ${String(keep)}`);
        for (const { version, model: then } of versions) {
          expectModel(await runAll(database, version), then, `version ${String(version)}`);
        }
      } finally {
        await database.close();
      }
    }
  });

  it("serves index-selected rows through range replay", async () => {
    pointReadTestHooks.disabled = true;
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
      rowsPerBlock: 128,
    });
    try {
      await database.createTable({
        name: "u",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "region", type: "string" },
          { name: "amount", type: "number" },
        ],
      });
      const rows = Array.from({ length: 4_000 }, (_, id) => ({
        id,
        region: REGIONS[id % 4] ?? "west",
        amount: id,
      }));
      for (let start = 0; start < rows.length; start += 1_000) {
        await database.insertBatch("u", rows.slice(start, start + 1_000));
      }
      await database.createIndex("u_region", "u", "region");
      const updated = rows.filter((row) => row.id % 3 === 0).map((row) => row.id);
      await database.updateBatch("u", {
        keys: updated,
        changes: { amount: updated.map((id) => -id) },
      });
      const deleted = rows.filter((row) => row.id % 10 === 1).map((row) => row.id);
      await database.deleteBatch("u", { keys: deleted });
      const expected = rows
        .filter((row) => row.region === "east" && row.id % 10 !== 1)
        .map((row) => ({ id: row.id, amount: row.id % 3 === 0 ? -row.id : row.id }));
      const sql = "SELECT id, amount FROM u WHERE region = 'east'";
      const inMemory = await database.query(sql, {
        memoize: false,
        executionMemoryBudgetBytes: BUDGET,
      });
      expect(byId(inMemory.rows)).toEqual(expected);
      overlayReplayTestHooks.fairShareBytes = 0;
      const ranges = overlayReplayTestHooks.rangeReplays;
      const bounded = await database.query(sql, {
        memoize: false,
        executionMemoryBudgetBytes: BUDGET,
      });
      expect(overlayReplayTestHooks.rangeReplays).toBeGreaterThan(ranges);
      expect(bounded.rows).toEqual(inMemory.rows);
    } finally {
      await database.close();
    }
  });

  it("replays a pruned scan by the keys it reads when the deltas hold more", async () => {
    pointReadTestHooks.disabled = true;
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
      rowsPerBlock: 64,
    });
    try {
      await database.createTable({
        name: "w",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "amount", type: "number" },
        ],
      });
      const rows = 4_000;
      const keys = Array.from({ length: rows }, (_, id) => id);
      await database.insertBatch(
        "w",
        keys.map((id) => ({ id, amount: id })),
      );
      // Every row updated twice and a few deleted: the deltas hold far more rows than the few
      // blocks a narrow key range keeps.
      await database.updateBatch("w", { keys, changes: { amount: keys.map((id) => id * 2) } });
      const odd = keys.filter((id) => id % 2 === 1);
      await database.updateBatch("w", { keys: odd, changes: { amount: odd.map((id) => -id) } });
      await database.deleteBatch("w", { keys: [1_000, 1_003, 1_010] });
      const sql = "SELECT id, amount FROM w WHERE id BETWEEN 990 AND 1020 ORDER BY id";
      const expected = keys
        .filter((id) => id >= 990 && id <= 1_020 && ![1_000, 1_003, 1_010].includes(id))
        .map((id) => ({ id, amount: id % 2 === 1 ? -id : id * 2 }));
      const built = overlayReplayTestHooks.scanKeyedBuilds;
      for (const budget of [BUDGET, 128 * 1024]) {
        const result = await database.query(sql, {
          memoize: false,
          executionMemoryBudgetBytes: budget,
        });
        expect(result.rows).toEqual(expected);
      }
      expect(overlayReplayTestHooks.scanKeyedBuilds).toBeGreaterThan(built);
      // The full scan replays by the deltas' keys and agrees.
      const total = await database.query("SELECT COUNT(*) AS n, SUM(amount) AS s FROM w", {
        memoize: false,
        executionMemoryBudgetBytes: BUDGET,
      });
      const live = keys.filter((id) => ![1_000, 1_003, 1_010].includes(id));
      expect(total.rows).toEqual([
        {
          n: live.length,
          s: live.reduce((sum, id) => sum + (id % 2 === 1 ? -id : id * 2), 0),
        },
      ]);
    } finally {
      await database.close();
    }
  });

  it("answers under a small real budget through both bounded paths", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
      rowsPerBlock: 1_024,
    });
    try {
      await database.createTable({
        name: "v",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "amount", type: "number" },
        ],
      });
      const rows = 24_000;
      await database.insertBatch(
        "v",
        Array.from({ length: rows }, (_, id) => ({ id, amount: id })),
      );
      // Every key patched, in an order unrelated to the table's.
      await database.upsertBatch(
        "v",
        Array.from({ length: rows }, (_, index) => {
          const id = (index * 7_919) % rows;
          return { id, amount: id * 2 };
        }),
      );
      const partitioned = overlayReplayTestHooks.partitionedBuilds;
      const ranges = overlayReplayTestHooks.rangeReplays;
      // 192 KB keeps 48 KB of patches (24,000 need 192 KB) and leaves about 100 KB of scratch.
      const result = await database.query(
        "SELECT COUNT(*) AS n, SUM(amount) AS total, MIN(amount) AS low, MAX(amount) AS high FROM v",
        { memoize: false, executionMemoryBudgetBytes: 192 * 1024 },
      );
      expect(result.rows).toEqual([
        { n: rows, total: rows * (rows - 1), low: 0, high: (rows - 1) * 2 },
      ]);
      expect(overlayReplayTestHooks.partitionedBuilds).toBeGreaterThan(partitioned);
      expect(overlayReplayTestHooks.rangeReplays).toBeGreaterThan(ranges);
      // The same connection with room to keep the patches builds them and keeps them.
      const roomy = await database.query("SELECT SUM(amount) AS total FROM v", {
        memoize: false,
        executionMemoryBudgetBytes: 64 * 1024 * 1024,
      });
      expect(roomy.rows).toEqual([{ total: rows * (rows - 1) }]);
    } finally {
      await database.close();
    }
  });
});
