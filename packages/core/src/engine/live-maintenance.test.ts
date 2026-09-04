import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { mulberry32 } from "../testing/seeds.js";
import { MinnowDatabase } from "./database.js";
import { type LiveQuerySet } from "./live.js";
import { type QueryResult, type QueryValue } from "./query.js";

/**
 * Incremental maintenance against the executor as the oracle. Every statement here is
 * subscribed once; a seeded script of inserts, updates, deletes, and upserts then commits one
 * change set at a time, and after each commit the retained result — patched from the commit's
 * keys rather than re-executed — must equal what running the statement afresh returns. The
 * maintainable shapes must actually be maintained (the stat proves the path was taken), and the
 * shapes the planner declines must fall back cleanly to full execution.
 */

const REGIONS = ["west", "east", "north", null] as const;

interface Item {
  id: number;
  region: string | null;
  amount: number;
  active: boolean;
  label: string;
  seen: Date;
}

function item(random: () => number, id: number): Item {
  return {
    id,
    region: REGIONS[Math.floor(random() * REGIONS.length)] ?? null,
    amount: Math.floor(random() * 100),
    active: random() < 0.5,
    label: `L${String(Math.floor(random() * 12))}`,
    seen: new Date(Date.UTC(2026, 0, 1 + Math.floor(random() * 60))),
  };
}

async function seededDatabase(
  seed: number,
  rows: number,
): Promise<{
  database: MinnowDatabase;
  random: () => number;
  nextId: () => number;
}> {
  const random = mulberry32(seed);
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 16,
    compression: "raw",
  });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number" },
      { name: "active", type: "boolean" },
      { name: "label", type: "string" },
      { name: "seen", type: "datetime" },
    ],
  });
  await database.createTable({
    name: "other",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "amount", type: "number" },
    ],
  });
  const items = Array.from({ length: rows }, (_, index) => item(random, index + 1));
  await database.insertBatch("items", {
    columns: {
      id: items.map((row) => row.id),
      region: items.map((row) => row.region),
      amount: items.map((row) => row.amount),
      active: items.map((row) => row.active),
      label: items.map((row) => row.label),
      seen: items.map((row) => row.seen),
    },
  });
  await database.insertBatch("other", { columns: { id: [1, 2], amount: [5, 7] } });
  await database.createView(
    "recent_items",
    "SELECT id, region, amount FROM items WHERE amount >= 50",
  );
  let id = rows;
  return {
    database,
    random,
    nextId: () => {
      id += 1;
      return id;
    },
  };
}

/** One random commit: a batch insert, an update, a delete, an upsert, or a mixed write scope. */
async function randomCommit(
  database: MinnowDatabase,
  random: () => number,
  nextId: () => number,
  live: Set<number>,
): Promise<string> {
  const pick = random();
  const alive = [...live];
  const existing = (): number => alive[Math.floor(random() * alive.length)] ?? 1;
  const created = (): number => {
    const id = nextId();
    live.add(id);
    return id;
  };
  if (pick < 0.3) {
    const count = 1 + Math.floor(random() * 4);
    const rows = Array.from({ length: count }, () => item(random, created()));
    await database.insertBatch("items", {
      columns: {
        id: rows.map((row) => row.id),
        region: rows.map((row) => row.region),
        amount: rows.map((row) => row.amount),
        active: rows.map((row) => row.active),
        label: rows.map((row) => row.label),
        seen: rows.map((row) => row.seen),
      },
    });
    return `insert ${String(count)}`;
  }
  if (pick < 0.55) {
    const keys = [existing(), existing()];
    await database.updateBatch("items", {
      keys,
      changes: {
        amount: keys.map(() => Math.floor(random() * 100)),
        region: keys.map(() => REGIONS[Math.floor(random() * REGIONS.length)] ?? null),
      },
    });
    return `update ${keys.join(",")}`;
  }
  if (pick < 0.75) {
    const key = existing();
    live.delete(key);
    await database.deleteBatch("items", { keys: [key] });
    return `delete ${String(key)}`;
  }
  if (pick < 0.9) {
    const rows = [item(random, existing()), item(random, created())];
    await database.upsertBatch("items", {
      columns: {
        id: rows.map((row) => row.id),
        region: rows.map((row) => row.region),
        amount: rows.map((row) => row.amount),
        active: rows.map((row) => row.active),
        label: rows.map((row) => row.label),
        seen: rows.map((row) => row.seen),
      },
    });
    return `upsert ${rows.map((row) => row.id).join(",")}`;
  }
  // The other table alone: nothing on items may move.
  await database.updateBatch("other", {
    keys: [1],
    changes: { amount: [Math.floor(random() * 9)] },
  });
  return "other";
}

function last(results: readonly QueryResult[]): QueryResult {
  const result = results.at(-1);
  if (result === undefined) throw new Error("no delivery yet");
  return result;
}

/** Rows as plain data; a statement without ORDER BY makes no promise about order, so sort those. */
function plain(result: QueryResult, sql = "ORDER BY"): Array<Record<string, QueryValue>> {
  const rows = result.rows.map((row) => {
    const copy: Record<string, QueryValue> = {};
    for (const column of result.columns) copy[column] = row[column] ?? null;
    return copy;
  });
  if (sql.includes("ORDER BY")) return rows;
  return rows.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
}

const MAINTAINABLE = [
  "SELECT * FROM items WHERE amount < 30 ORDER BY id",
  "SELECT id, amount FROM items ORDER BY id OFFSET 3",
  "SELECT id, region FROM recent_items ORDER BY id",
  "SELECT id, region, amount FROM items WHERE amount >= 40 ORDER BY id",
  "SELECT region, amount FROM items WHERE active = TRUE ORDER BY amount DESC, id",
  "SELECT id, label FROM items WHERE region IS NULL ORDER BY label, id DESC",
  "SELECT id, amount FROM items ORDER BY region NULLS FIRST, amount, id",
  "SELECT id, amount FROM items ORDER BY region DESC, id",
  "SELECT id, amount * 2 AS doubled FROM items WHERE label IN ('L1', 'L2', 'L3') ORDER BY doubled DESC, id",
  "SELECT id, seen FROM items WHERE seen >= '2026-02-01' ORDER BY seen DESC, id",
  "SELECT id FROM items WHERE amount BETWEEN 20 AND 60 AND active = FALSE ORDER BY LOWER(label), id",
  "SELECT id, amount FROM items ORDER BY amount DESC, id LIMIT 12",
  "SELECT id, amount FROM items WHERE amount < 90 ORDER BY id LIMIT 8 OFFSET 4",
  "SELECT id, region FROM items WHERE amount > 70",
  "SELECT i.id, i.amount FROM items AS i WHERE i.active = TRUE ORDER BY i.amount, i.id",
  "SELECT id, amount FROM items ORDER BY 2 DESC, 1",
] as const;

const NOT_MAINTAINABLE = [
  "SELECT COUNT(*) AS n, SUM(amount) AS total FROM items WHERE active = TRUE",
  "SELECT region, COUNT(*) AS n FROM items GROUP BY region ORDER BY region",
  "SELECT DISTINCT label FROM items ORDER BY label",
  "SELECT i.id, o.amount FROM items AS i JOIN other AS o ON o.id = i.id ORDER BY i.id",
  "SELECT id FROM items WHERE amount > (SELECT AVG(amount) FROM other) ORDER BY id",
  "SELECT id, ROW_NUMBER() OVER (ORDER BY amount) AS rank FROM items ORDER BY id",
] as const;

async function subscribeAll(
  live: LiveQuerySet,
  statements: readonly string[],
): Promise<Map<string, QueryResult>> {
  const latest = new Map<string, QueryResult>();
  for (const sql of statements) {
    await live.subscribe(sql, {
      onChange: (result) => {
        latest.set(sql, result);
      },
    });
  }
  return latest;
}

describe("live query incremental maintenance", () => {
  it.each([7, 19, 23])(
    "keeps every maintainable statement equal to a fresh execution across mutation script %i",
    async (seed) => {
      const { database, random, nextId } = await seededDatabase(seed, 120);
      const live = database.liveQueries({ maxGroups: 64 });
      const latest = await subscribeAll(live, [...MAINTAINABLE, ...NOT_MAINTAINABLE]);
      const log: string[] = [];
      const alive = new Set(Array.from({ length: 120 }, (_, index) => index + 1));
      for (let step = 0; step < 100; step += 1) {
        log.push(await randomCommit(database, random, nextId, alive));
        await live.refresh();
        for (const sql of [...MAINTAINABLE, ...NOT_MAINTAINABLE]) {
          const delivered = latest.get(sql);
          if (delivered === undefined) throw new Error(`no delivery for ${sql}`);
          const fresh = await database.query(sql, { memoize: false });
          expect(plain(delivered, sql), `${sql}\nafter: ${log.join(" | ")}`).toEqual(
            plain(fresh, sql),
          );
        }
      }
      const stats = live.stats;
      // Most commits touch items, so the maintainable statements were patched far more often
      // than they were executed; the unmaintainable ones only ever executed.
      expect(stats.maintained).toBeGreaterThan(MAINTAINABLE.length * 30);
      expect(stats.reruns).toBeGreaterThan(0);
      live.close();
      await database.close();
    },
  );

  it("declines every unmaintainable shape rather than patching it", async () => {
    const { database } = await seededDatabase(13, 50);
    for (const sql of NOT_MAINTAINABLE) {
      const live = database.liveQueries();
      await live.subscribe(sql, { onChange: () => undefined });
      await database.updateBatch("items", { keys: [3], changes: { amount: [77] } });
      await live.refresh();
      expect(live.stats.maintained, sql).toBe(0);
      expect(live.stats.reruns, sql).toBe(1);
      live.close();
    }
    await database.close();
  });

  it("maintains parameterized statements and string-keyed tables", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { compression: "raw" });
    await database.createTable({
      name: "skus",
      uniqueKey: "sku",
      columns: [
        { name: "sku", type: "string" },
        { name: "stock", type: "number" },
        { name: "name", type: "string" },
      ],
    });
    await database.insertBatch("skus", {
      columns: {
        sku: ["a-1", "b-2", "c-3", "d-4"],
        stock: [5, 0, 12, 3],
        name: ["ant", "bee", "cat", "dog"],
      },
    });
    const live = database.liveQueries();
    const results: QueryResult[] = [];
    const query = {
      kind: "sql-query" as const,
      sql: "SELECT sku, name FROM skus WHERE stock > $1 ORDER BY name DESC LIMIT $2",
      params: [2, 3],
    };
    await live.subscribe(query, { onChange: (result) => results.push(result) });
    expect(last(results).rows.map((row) => row.sku)).toEqual(["d-4", "c-3", "a-1"]);
    await database.upsertBatch("skus", {
      columns: { sku: ["b-2", "e-5"], stock: [9, 1], name: ["bee", "eel"] },
    });
    await live.refresh();
    expect(last(results).rows.map((row) => row.sku)).toEqual(["d-4", "c-3", "b-2"]);
    await database.deleteBatch("skus", { keys: ["d-4"] });
    await live.refresh();
    expect(last(results).rows.map((row) => row.sku)).toEqual(["c-3", "b-2", "a-1"]);
    expect(live.stats.maintained).toBe(2);
    expect(live.stats.reruns).toBe(0);
    live.close();
    await database.close();
  });

  it("refills a window from its margin, and executes only past it or for wide commits", async () => {
    const { database } = await seededDatabase(11, 200);
    const live = database.liveQueries();
    const sql = "SELECT id, amount FROM items ORDER BY amount DESC, id LIMIT 5";
    const results: QueryResult[] = [];
    await live.subscribe(sql, { onChange: (result) => results.push(result) });
    const top = results[0]?.rows[0]?.id;
    if (typeof top !== "number") throw new Error("expected a numeric key");

    // Deleting the window's first row: the row after the visible edge was already held.
    await database.deleteBatch("items", { keys: [top] });
    await live.refresh();
    expect(live.stats.maintained).toBe(1);
    expect(live.stats.reruns).toBe(0);
    expect(results).toHaveLength(2);
    expect(plain(last(results))).toEqual(plain(await database.query(sql, { memoize: false })));

    // Deleting more rows than the margin holds: the full statement refills it.
    const held = (await database.query(sql.replace("LIMIT 5", "LIMIT 40"), { memoize: false })).rows
      .map((row) => row.id)
      .filter((id): id is number => typeof id === "number");
    await database.deleteBatch("items", { keys: held.slice(0, 30) });
    await live.refresh();
    expect(live.stats.reruns).toBe(1);
    expect(plain(last(results))).toEqual(plain(await database.query(sql, { memoize: false })));

    // A row landing beyond the window's edge is patched and changes nothing visible.
    await database.insertBatch("items", {
      columns: {
        id: [9_001],
        region: ["west"],
        amount: [-1],
        active: [true],
        label: ["L0"],
        seen: [new Date(0)],
      },
    });
    const deliveries = results.length;
    await live.refresh();
    expect(live.stats.maintained).toBe(2);
    expect(results).toHaveLength(deliveries);

    // A row landing inside the window is patched into place.
    await database.insertBatch("items", {
      columns: {
        id: [9_002],
        region: ["west"],
        amount: [1_000],
        active: [true],
        label: ["L0"],
        seen: [new Date(0)],
      },
    });
    await live.refresh();
    expect(live.stats.maintained).toBe(3);
    expect(results.at(-1)?.rows[0]).toMatchObject({ id: 9_002, amount: 1_000 });
    expect(plain(last(results))).toEqual(plain(await database.query(sql, { memoize: false })));

    // A commit wider than the delta ceiling runs the statement instead.
    const wide = Array.from({ length: 3_000 }, (_, index) => 20_000 + index);
    await database.insertBatch("items", {
      columns: {
        id: wide,
        region: wide.map(() => "east"),
        amount: wide.map((id) => id % 50),
        active: wide.map(() => false),
        label: wide.map(() => "L9"),
        seen: wide.map(() => new Date(0)),
      },
    });
    await live.refresh();
    expect(live.stats.maintained).toBe(3);
    expect(live.stats.reruns).toBe(2);
    expect(plain(last(results))).toEqual(plain(await database.query(sql, { memoize: false })));
    live.close();
    await database.close();
  });

  it("retains no more than its windows show, however many commits it sees", async () => {
    const { database, random, nextId } = await seededDatabase(29, 400);
    const live = database.liveQueries({ maxGroups: 64 });
    const windows = Array.from(
      { length: 20 },
      (_, index) =>
        `SELECT id, amount, label FROM items WHERE amount > ${String(index)} ORDER BY amount DESC, id LIMIT 25`,
    );
    await subscribeAll(live, windows);
    const alive = new Set(Array.from({ length: 400 }, (_, index) => index + 1));
    for (let step = 0; step < 200; step += 1) {
      await randomCommit(database, random, nextId, alive);
      await live.refresh();
      expect(live.stats.retainedRows).toBeLessThanOrEqual(windows.length * 25);
    }
    // Two hundred commits, and the set's footprint is still twenty windows of twenty-five.
    expect(live.stats.maintained).toBeGreaterThan(windows.length * 100);
    expect(live.stats.retainedRows).toBeLessThanOrEqual(windows.length * 25);
    live.close();
    await database.close();
  });

  it("tells consecutive subscribers where each kept row was", async () => {
    const { database } = await seededDatabase(5, 60);
    const live = database.liveQueries({ sharedResults: true });
    const sql = "SELECT id, amount FROM items ORDER BY amount DESC, id LIMIT 6";
    const deliveries: Array<{ rows: QueryResult["rows"]; retained: Int32Array | undefined }> = [];
    await live.subscribe(sql, {
      onChange: (result, delivery) =>
        deliveries.push({ rows: result.rows, retained: delivery.retained }),
    });
    expect(deliveries[0]?.retained).toBeUndefined();
    const before = last(
      deliveries.map((delivery) => ({ ...delivery, columns: [], columnDomains: [] })),
    );

    // A row entering at the top: every previous row moved down by one, and says so.
    await database.insertBatch("items", {
      columns: {
        id: [8_001],
        region: ["west"],
        amount: [10_000],
        active: [true],
        label: ["L1"],
        seen: [new Date(0)],
      },
    });
    await live.refresh();
    const shifted = deliveries.at(-1);
    expect(shifted?.retained === undefined ? undefined : [...shifted.retained]).toEqual([
      -1, 0, 1, 2, 3, 4,
    ]);
    for (let index = 1; index < 6; index += 1) {
      expect(shifted?.rows[index]).toBe(before.rows[index - 1]);
    }

    // A late subscriber's first delivery is not relative to anything it saw.
    const late: Array<Int32Array | undefined> = [];
    await live.subscribe(sql, { onChange: (_result, delivery) => late.push(delivery.retained) });
    expect(late).toEqual([undefined]);
    // The next change reaches both, and both can map it.
    await database.updateBatch("items", { keys: [8_001], changes: { amount: [9_999] } });
    await live.refresh();
    expect(late.at(-1)).toBeInstanceOf(Int32Array);
    expect(deliveries.at(-1)?.retained).toBeInstanceOf(Int32Array);
    expect([...(late.at(-1) ?? [])]).toEqual([-1, 1, 2, 3, 4, 5]);
    live.close();
    await database.close();
  });

  it("re-plans after a catalog change and keeps unchanged rows by identity", async () => {
    const { database } = await seededDatabase(3, 40);
    const live = database.liveQueries({ sharedResults: true });
    const sql = "SELECT id, amount FROM items WHERE amount >= 10 ORDER BY id";
    const results: QueryResult[] = [];
    await live.subscribe(sql, { onChange: (result) => results.push(result) });
    const before = last(results);
    await database.updateBatch("items", { keys: [5], changes: { amount: [99] } });
    await live.refresh();
    const after = last(results);
    expect(live.stats.maintained).toBe(1);
    // Rows the commit did not touch are the same objects; the patched row is new.
    const untouched = before.rows.find((row) => row.id === 6);
    expect(after.rows.find((row) => row.id === 6)).toBe(untouched);
    expect(after.rows.find((row) => row.id === 5)).toMatchObject({ amount: 99 });

    // A view replacement moves the schema epoch: the plan is remade and the statement re-run.
    await database.createView("recent", "SELECT id FROM items WHERE amount > 50");
    await database.insertBatch("items", {
      columns: {
        id: [777],
        region: ["west"],
        amount: [11],
        active: [true],
        label: ["L1"],
        seen: [new Date(0)],
      },
    });
    await live.refresh();
    expect(plain(last(results))).toEqual(plain(await database.query(sql, { memoize: false })));
    await database.updateBatch("items", { keys: [777], changes: { amount: [12] } });
    await live.refresh();
    expect(live.stats.maintained).toBe(2);
    expect(plain(last(results))).toEqual(plain(await database.query(sql, { memoize: false })));
    live.close();
    await database.close();
  });
});
