/**
 * Differential scan conformance over mutation histories. A table that has been deleted from or
 * updated no longer reads as a plain append, and the paths that serve it — the streamed replay,
 * its zone-map elimination, and the materialized delta overlay — each rebuild the visible rows
 * a different way. This harness runs a seeded script of deletes, updates, re-inserts, and
 * upserts against MinnowDatabase and SQLite (node:sqlite, the reference oracle), and after every
 * step diffs a battery of scans: full projections, aggregates, key equality and ranges, IN
 * lists, predicates on non-key columns, and NULL-bearing ones.
 *
 * Every query runs twice, once with a memory budget small enough to force the streamed scan and
 * once without, so a divergence cannot hide in whichever path this table size happens to pick.
 */
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { type QueryValue } from "./query.js";
import { mulberry32 } from "../testing/seeds.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const REGIONS = ["west", "east", "north", null] as const;
const LABELS = ["alpha", "bravo", "charlie", "delta"] as const;
const ROWS = 240;

/** Deterministic ORDER BY everywhere: an unordered scan's row order is not a contract. */
const SCANS = [
  "SELECT id, region, amount, active, label FROM items ORDER BY id",
  "SELECT COUNT(*) AS n FROM items",
  "SELECT id, amount FROM items WHERE id = 137",
  "SELECT id, amount FROM items WHERE id BETWEEN 60 AND 95 ORDER BY id",
  "SELECT id, label FROM items WHERE id IN (3, 77, 141, 199, 240, 301) ORDER BY id",
  "SELECT id, region FROM items WHERE amount > 60 ORDER BY id",
  "SELECT COUNT(*) AS n, SUM(amount) AS s FROM items WHERE active = TRUE",
  "SELECT id, label FROM items WHERE region IS NULL ORDER BY id",
  "SELECT id, amount FROM items WHERE label = 'charlie' ORDER BY id",
  "SELECT id FROM items WHERE id > 200 ORDER BY id",
] as const;

function normalize(rows: ReadonlyArray<Record<string, unknown>>): unknown[] {
  return rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        typeof value === "bigint"
          ? Number(value)
          : // SQLite has no boolean type; both sides compare as 0/1.
            typeof value === "boolean"
            ? Number(value)
            : value,
      ]),
    ),
  );
}

describe("scans over mutation histories", () => {
  it("matches SQLite after every delete, update, re-insert, and upsert", async () => {
    // Sixteen rows per block makes the table many blocks wide, so zone-map elimination and the
    // streamed loader's window boundaries are both live at this size.
    const minnow = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 16 });
    await minnow.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "region", type: "string", nullable: true },
        { name: "amount", type: "number" },
        { name: "active", type: "boolean" },
        { name: "label", type: "string" },
      ],
    });
    const sqlite = new DatabaseSync(":memory:");
    sqlite.exec(
      `CREATE TABLE items ("id" INTEGER PRIMARY KEY, "region" TEXT, "amount" REAL, "active" INTEGER, "label" TEXT)`,
    );

    const rng = mulberry32(0x0de17a);
    const rows = Array.from({ length: ROWS }, (_, index) => ({
      id: index + 1,
      region: REGIONS[Math.floor(rng() * REGIONS.length)] ?? null,
      amount: Math.floor(rng() * 400) / 4,
      active: rng() < 0.5,
      label: LABELS[Math.floor(rng() * LABELS.length)] ?? "alpha",
    }));
    // Several batches, so the history is several segments before any delta lands on it.
    for (let start = 0; start < rows.length; start += 60) {
      await minnow.insertBatch("items", rows.slice(start, start + 60));
    }
    const insert = sqlite.prepare("INSERT INTO items VALUES (?, ?, ?, ?, ?)");
    for (const row of rows) {
      insert.run(row.id, row.region, row.amount, row.active ? 1 : 0, row.label);
    }

    const compare = async (step: string): Promise<void> => {
      for (const sql of SCANS) {
        const expected = normalize(sqlite.prepare(sql).all());
        for (const budget of [undefined, 64 * 1024]) {
          const actual = await minnow.query(sql, {
            memoize: false,
            ...(budget === undefined ? {} : { executionMemoryBudgetBytes: budget }),
          });
          expect(
            normalize(actual.rows),
            `${step} :: ${sql} :: ${budget === undefined ? "materialized" : "streamed"}`,
          ).toEqual(expected);
        }
      }
    };

    await compare("seeded");

    const live = new Set(rows.map((row) => row.id));
    let nextId = ROWS + 1;
    for (let step = 0; step < 40; step += 1) {
      const choice = rng();
      const liveIds = [...live];
      const victim = liveIds[Math.floor(rng() * liveIds.length)] ?? 1;
      if (choice < 0.3) {
        // Delete, sometimes naming a key that is already gone alongside one that is not.
        const keys = rng() < 0.3 ? [victim, ROWS + 5_000] : [victim];
        await minnow.deleteBatch("items", { keys });
        for (const key of keys) sqlite.prepare("DELETE FROM items WHERE id = ?").run(key);
        live.delete(victim);
      } else if (choice < 0.55) {
        // Update a predicate column: a row can move into or out of any scan's range.
        const amount = Math.floor(rng() * 400) / 4;
        await minnow.updateBatch("items", { keys: [victim], changes: { amount: [amount] } });
        sqlite.prepare("UPDATE items SET amount = ? WHERE id = ?").run(amount, victim);
      } else if (choice < 0.7) {
        const region = REGIONS[Math.floor(rng() * REGIONS.length)] ?? null;
        await minnow.updateBatch("items", { keys: [victim], changes: { region: [region] } });
        sqlite.prepare("UPDATE items SET region = ? WHERE id = ?").run(region, victim);
      } else if (choice < 0.85) {
        // Re-insert a key the script deleted earlier: the row is newer than the delete that
        // removed the old one, which is the case a key-set mask alone would get wrong.
        const missing = [...Array.from({ length: ROWS }, (_, index) => index + 1)].filter(
          (id) => !live.has(id),
        );
        const id = missing[Math.floor(rng() * missing.length)] ?? nextId++;
        const row = {
          id,
          region: REGIONS[Math.floor(rng() * REGIONS.length)] ?? null,
          amount: Math.floor(rng() * 400) / 4,
          active: rng() < 0.5,
          label: LABELS[Math.floor(rng() * LABELS.length)] ?? "alpha",
        };
        if (live.has(id)) continue;
        await minnow.insertBatch("items", [row]);
        insert.run(row.id, row.region, row.amount, row.active ? 1 : 0, row.label);
        live.add(id);
      } else {
        const row = {
          id: victim,
          region: REGIONS[Math.floor(rng() * REGIONS.length)] ?? null,
          amount: Math.floor(rng() * 400) / 4,
          active: rng() < 0.5,
          label: LABELS[Math.floor(rng() * LABELS.length)] ?? "alpha",
        };
        await minnow.upsertBatch("items", [row]);
        sqlite
          .prepare(
            "INSERT INTO items VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET " +
              "region = excluded.region, amount = excluded.amount, active = excluded.active, label = excluded.label",
          )
          .run(row.id, row.region, row.amount, row.active ? 1 : 0, row.label);
        live.add(row.id);
      }
      await compare(`step ${String(step)}`);
    }
    sqlite.close();
  });
  it("keeps a deleted row out of a range its zone map still covers", async () => {
    // The deleted row sits inside the queried key range, so elimination keeps its row group and
    // the mask has to remove exactly one row from it — the case a pruned scan could double-count.
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "n", type: "number" },
      ],
    });
    await database.insertBatch(
      "t",
      Array.from({ length: 200 }, (_, index) => ({ id: index + 1, n: index + 1 })),
    );
    await database.deleteBatch("t", { keys: [42] });
    const params: QueryValue[] = [40, 45];
    expect(
      (
        await database.query("SELECT id FROM t WHERE id BETWEEN ? AND ? ORDER BY id", {
          memoize: false,
          params,
        })
      ).rows,
    ).toEqual([{ id: 40 }, { id: 41 }, { id: 43 }, { id: 44 }, { id: 45 }]);
    expect((await database.query("SELECT COUNT(*) AS n FROM t", { memoize: false })).rows).toEqual([
      { n: 199 },
    ]);
    // A key range that excludes the deleted row must not pay for it, and must not lose a row.
    expect(
      (
        await database.query("SELECT COUNT(*) AS n FROM t WHERE id BETWEEN 100 AND 109", {
          memoize: false,
        })
      ).rows,
    ).toEqual([{ n: 10 }]);
  });

  it("reads a key re-inserted after its delete, inside a write scope", async () => {
    // A write scope reads through the materialized path rather than the streamed one, and a key
    // that was deleted and then written again has two base rows: the delete only removes the one
    // that came before it. A mask keyed on the value alone would drop both.
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "n", type: "number" },
      ],
    });
    await database.insertBatch(
      "t",
      Array.from({ length: 200 }, (_, index) => ({ id: index + 1, n: index + 1 })),
    );
    await database.deleteBatch("t", { keys: [42] });
    await database.insertBatch("t", [{ id: 42, n: 4_242 }]);
    await database.deleteBatch("t", { keys: [7] });
    await database.updateBatch("t", { keys: [9], changes: { n: [909] } });

    const inScope = await database.write(async (session) => ({
      pair: (await session.query("SELECT id, n FROM t WHERE id IN (7, 9, 42) ORDER BY id")).rows,
      total: (await session.query("SELECT COUNT(*) AS n FROM t")).rows,
      // A predicate keeps the re-inserted row's row group, so elimination is live here too: it
      // can only compose with deltas that are newer than every base segment, and this delete
      // is not — the insert that re-added the key came after it.
      ranged: (await session.query("SELECT id, n FROM t WHERE id BETWEEN 40 AND 44 ORDER BY id"))
        .rows,
      staged: await session.insertBatch("t", [{ id: 500, n: 500 }]),
    }));
    expect(inScope.result.pair).toEqual([
      { id: 9, n: 909 },
      { id: 42, n: 4_242 },
    ]);
    expect(inScope.result.total).toEqual([{ n: 199 }]);
    expect(inScope.result.ranged).toEqual([
      { id: 40, n: 40 },
      { id: 41, n: 41 },
      { id: 42, n: 4_242 },
      { id: 43, n: 43 },
      { id: 44, n: 44 },
    ]);
    // And the same history reads identically outside the scope.
    expect(
      (
        await database.query("SELECT id, n FROM t WHERE id IN (7, 9, 42, 500) ORDER BY id", {
          memoize: false,
        })
      ).rows,
    ).toEqual([
      { id: 9, n: 909 },
      { id: 42, n: 4_242 },
      { id: 500, n: 500 },
    ]);
  });

  it("keeps elimination away from a delete an insert came after", async () => {
    // Deletes compose with zone-map elimination only while every one of them is newer than
    // every base segment. Here the re-insert of key 42 is newer than the delete that removed
    // it, so which row the delete takes depends on the segment a row came from — and a pruned
    // scan no longer reports that.
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "n", type: "number" },
      ],
    });
    await database.insertBatch(
      "t",
      Array.from({ length: 200 }, (_, index) => ({ id: index + 1, n: index + 1 })),
    );
    await database.deleteBatch("t", { keys: [42] });
    await database.insertBatch("t", [{ id: 42, n: 4_242 }]);
    const ranged = await database.write(async (session) => ({
      rows: (await session.query("SELECT id, n FROM t WHERE id BETWEEN 40 AND 44 ORDER BY id"))
        .rows,
      staged: await session.insertBatch("t", [{ id: 501, n: 501 }]),
    }));
    expect(ranged.result.rows).toEqual([
      { id: 40, n: 40 },
      { id: 41, n: 41 },
      { id: 42, n: 4_242 },
      { id: 43, n: 43 },
      { id: 44, n: 44 },
    ]);
  });

  it("returns a row an update moves into a range whose row group was eliminated", async () => {
    // id 3 lives in the first block; the query's predicate is on the updated column itself, so
    // elimination must not run at all — the update is what makes the row match.
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "n", type: "number" },
      ],
    });
    await database.insertBatch(
      "t",
      Array.from({ length: 200 }, (_, index) => ({ id: index + 1, n: index + 1 })),
    );
    await database.updateBatch("t", { keys: [3], changes: { n: [1_000] } });
    expect(
      (await database.query("SELECT id, n FROM t WHERE n > 900 ORDER BY id", { memoize: false }))
        .rows,
    ).toEqual([{ id: 3, n: 1_000 }]);
    expect(
      (
        await database.query("SELECT id FROM t WHERE n BETWEEN 3 AND 4 ORDER BY id", {
          memoize: false,
        })
      ).rows,
    ).toEqual([{ id: 4 }]);
  });
});
