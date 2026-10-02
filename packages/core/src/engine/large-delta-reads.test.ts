/**
 * A table refreshed by one full-table upsert, read with default settings. The upsert leaves a
 * single delta as large as the table: every read replays it until automatic compaction folds it
 * away, which it must do without waiting for another write. Reads in that window once failed
 * with a memory budget error from 316,179 rows up, on every store, and never recovered, because
 * one delta was never due for a fold. Queries must succeed at once, during the fold and after
 * it, with exact results.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(240_000) });

const REGIONS = ["west", "east", "north", "south"] as const;

type StoreKind = "memory" | "opfs" | "indexeddb";

async function openStore(kind: StoreKind) {
  if (kind === "opfs") return OpfsBlockStore.open({ name: "large", root: new MemoryOpfs().root });
  if (kind === "indexeddb") {
    return IndexedDbBlockStore.open({ name: "large", indexedDB: new IDBFactory() });
  }
  return new MemoryBlockStore();
}

/** The table after the upsert: every row's amount is its id plus one, its label renamed. */
function expectedRegionTotals(rows: number): Array<{ region: string; n: number; total: number }> {
  const totals = new Map<string, { n: number; total: number }>();
  for (let id = 0; id < rows; id += 1) {
    const region = REGIONS[id % 4] ?? "west";
    const entry = totals.get(region) ?? { n: 0, total: 0 };
    entry.n += 1;
    entry.total += id + 1;
    totals.set(region, entry);
  }
  return [...totals]
    .sort(([left], [right]) => (left < right ? -1 : 1))
    .map(([region, { n, total }]) => ({ region, n, total }));
}

async function expectRefreshed(database: MinnowDatabase, rows: number, when: string) {
  const counted = await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM t");
  expect(counted.rows, when).toEqual([{ n: rows, total: (rows * (rows + 1)) / 2 }]);
  const one = await database.query("SELECT COUNT(*) AS n FROM t WHERE id = 5");
  expect(one.rows, when).toEqual([{ n: 1 }]);
  const probe = rows - 3;
  const point = await database.query(`SELECT * FROM t WHERE id = ${String(probe)}`);
  expect(point.rows, when).toEqual([
    {
      id: probe,
      region: REGIONS[probe % 4],
      amount: probe + 1,
      label: `m${String(probe % 97)}`,
    },
  ]);
  const grouped = await database.query(
    "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP BY region ORDER BY region",
  );
  expect(grouped.rows, when).toEqual(expectedRegionTotals(rows));
  const top = await database.query("SELECT id, amount FROM t ORDER BY amount DESC LIMIT 3");
  expect(top.rows, when).toEqual([
    { id: rows - 1, amount: rows },
    { id: rows - 2, amount: rows - 1 },
    { id: rows - 3, amount: rows - 2 },
  ]);
  const all = await database.query("SELECT * FROM t");
  expect(all.rows.length, when).toBe(rows);
  const seen = new Uint8Array(rows);
  let wrong = 0;
  for (const row of all.rows) {
    const id = row.id as number;
    if (seen[id] === 1) wrong += 1;
    seen[id] = 1;
    if (row.amount !== id + 1 || row.label !== `m${String(id % 97)}`) wrong += 1;
    if (row.region !== REGIONS[id % 4]) wrong += 1;
  }
  expect(wrong, when).toBe(0);
}

/** Waits until compaction has folded the delta: the visible rows are the table's rows again. */
async function folded(database: MinnowDatabase, rows: number): Promise<void> {
  const deadline = Date.now() + 180_000;
  for (;;) {
    const segments = await allVisibleSegments(database, "t");
    if (segments.reduce((total, segment) => total + segment.rowCount, 0) === rows) return;
    if (Date.now() > deadline) throw new Error("The full-table upsert was never folded");
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

const CASES: ReadonlyArray<{ store: StoreKind; rows: number }> = [
  { store: "memory", rows: 400_000 },
  { store: "opfs", rows: 400_000 },
  { store: "indexeddb", rows: 400_000 },
  { store: "memory", rows: 1_000_000 },
  { store: "opfs", rows: 1_000_000 },
];

describe("compactTable without a memory budget", () => {
  it("fits a fold larger than the default budget, as automatic compaction does", async () => {
    const rows = 400_000;
    const database = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false });
    try {
      await database.createTable({
        name: "t",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "amount", type: "number" },
        ],
      });
      await database.insertBatch(
        "t",
        Array.from({ length: rows }, (_, id) => ({ id, amount: id })),
      );
      await database.upsertBatch(
        "t",
        Array.from({ length: rows }, (_, id) => ({ id, amount: id + 1 })),
      );
      // The smallest fold, the base and its one delta, needs more than the default 32 MiB.
      const result = await database.compactTable("t");
      expect(result.compacted).toBe(true);
      const segments = await allVisibleSegments(database, "t");
      expect(segments.reduce((total, segment) => total + segment.rowCount, 0)).toBe(rows);
      expect(
        (await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM t")).rows,
      ).toEqual([{ n: rows, total: (rows * (rows + 1)) / 2 }]);
    } finally {
      await database.close();
    }
  });
});

describe("reads after a full-table upsert, with default settings", () => {
  for (const { store: kind, rows } of CASES) {
    it(`answers exactly at once and after the fold: ${kind}, ${String(rows)} rows`, async () => {
      const database = new MinnowDatabase(await openStore(kind));
      try {
        await database.createTable({
          name: "t",
          uniqueKey: "id",
          columns: [
            { name: "id", type: "number" },
            { name: "region", type: "string" },
            { name: "amount", type: "number" },
            { name: "label", type: "string" },
          ],
        });
        await database.insertBatch(
          "t",
          Array.from({ length: rows }, (_, id) => ({
            id,
            region: REGIONS[id % 4] ?? "west",
            amount: id,
            label: `l${String(id % 97)}`,
          })),
        );
        await database.upsertBatch(
          "t",
          Array.from({ length: rows }, (_, id) => ({
            id,
            region: REGIONS[id % 4] ?? "west",
            amount: id + 1,
            label: `m${String(id % 97)}`,
          })),
        );
        // At once: the fold has at most just started, and every read replays the delta.
        await expectRefreshed(database, rows, "before the fold");
        // One delta is due for a fold on its own, with no further write to prompt it.
        await folded(database, rows);
        await expectRefreshed(database, rows, "after the fold");
      } finally {
        await database.close();
      }
    });
  }
});
