import { MemoryBlockStore } from "../../storage/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../database.js";
import { compileQuery, type CompiledQuery } from "../query.js";
import { column, schema, table } from "../schema.js";
import { Minnow } from "./db.js";
import { sql } from "./sql-tag.js";
import { type InferDatabase } from "./types.js";

/**
 * Correctness audit for the Kysely-style DSL: plan parity on the long tail of shapes, result
 * equivalence against raw SQL on a seeded database, builder immutability and compile
 * determinism, three-valued logic, live-query selectivity, and a compile-cost guardrail.
 */

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  city: column.string().nullable(),
  joined: column.datetime().nullable(),
});
const orders = table("orders", {
  order_id: column.number().unique(),
  person: column.string().references("people", "name"),
  total: column.number(),
});
const appSchema = schema([people, orders]);
type DB = InferDatabase<typeof appSchema>;

function stripSql(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stripSql);
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([key]) => key !== "sql")
        .map(([key, entry]) => [key, stripSql(entry)]),
    );
  }
  return value;
}

async function seeded(): Promise<{ db: Minnow<DB>; database: MinnowDatabase }> {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 4,
    compression: "raw",
  });
  await database.migrate(appSchema);
  const db = new Minnow<DB>(database, { schema: appSchema });
  await db
    .insertInto("people")
    .values([
      { name: "Ada", score: 10, city: "London", joined: new Date("2024-01-02T00:00:00Z") },
      { name: "Grace", score: 20, city: "DC" },
      { name: "Katherine", score: 30 },
      { name: "Barbara", score: 20, city: "London" },
      { name: "Edsger", score: 5, city: null },
    ])
    .execute();
  await db
    .insertInto("orders")
    .values([
      { order_id: 1, person: "Ada", total: 12.5 },
      { order_id: 2, person: "Ada", total: 7.5 },
      { order_id: 3, person: "Grace", total: 40 },
      { order_id: 4, person: "Barbara", total: 12.5 },
    ])
    .execute();
  return { db, database };
}

interface Equivalence {
  label: string;
  builder: (db: Minnow<DB>) => {
    compile: () => { plan: CompiledQuery };
    execute: () => Promise<unknown[]>;
  };
  sql: string;
}

/** Every case asserts plan identity AND result identity AND compile determinism. */
const equivalences: Equivalence[] = [
  {
    label: "bare references on an aliased table",
    builder: (db) =>
      db.selectFrom("people as p").where("score", ">=", 20).select(["name"]).orderBy("name"),
    sql: "SELECT name FROM people p WHERE score >= 20 ORDER BY name",
  },
  {
    label: "order by a selected qualified column, descending",
    builder: (db) =>
      db.selectFrom("people as p").select(["p.name", "p.score"]).orderBy("score", "desc").limit(2),
    sql: "SELECT p.name AS name, p.score AS score FROM people p ORDER BY score DESC LIMIT 2",
  },
  {
    label: "order by a selected expression alias",
    builder: (db) =>
      db
        .selectFrom("people as p")
        .select((eb) => [eb.ref("p.name").as("name"), eb("p.score", "*", -1).as("inverted")])
        .orderBy("inverted"),
    sql: "SELECT p.name AS name, p.score * -1 AS inverted FROM people p ORDER BY inverted",
  },
  {
    label: "distinct with order by",
    builder: (db) => db.selectFrom("people").select(["city"]).distinct().orderBy("city"),
    sql: "SELECT DISTINCT city FROM people ORDER BY city",
  },
  {
    label: "IS NOT NULL with NOT IN",
    builder: (db) =>
      db
        .selectFrom("people")
        .where("city", "is not", null)
        .where("city", "not in", ["DC"])
        .select(["name"])
        .orderBy("name"),
    sql: "SELECT name FROM people WHERE city IS NOT NULL AND city NOT IN ('DC') ORDER BY name",
  },
  {
    label: "three-valued OR over nullable column",
    builder: (db) =>
      db
        .selectFrom("people")
        .where((eb) => eb.or([eb("city", "=", "London"), eb("score", "<", 8)]))
        .select(["name"])
        .orderBy("name"),
    sql: "SELECT name FROM people WHERE city = 'London' OR score < 8 ORDER BY name",
  },
  {
    label: "NOT LIKE and negation",
    builder: (db) =>
      db
        .selectFrom("people")
        .where("name", "not like", "%a%")
        .select((eb) => [eb.ref("name").as("name"), eb.neg("score").as("negated")])
        .orderBy("name"),
    sql: "SELECT name AS name, -score AS negated FROM people WHERE name NOT LIKE '%a%' ORDER BY name",
  },
  {
    label: "CASE without ELSE yields null",
    builder: (db) =>
      db
        .selectFrom("people")
        .select((eb) => [
          eb.ref("name").as("name"),
          eb.case().when("score", ">", 15).then("high").end().as("tier"),
        ])
        .orderBy("name"),
    sql: "SELECT name AS name, CASE WHEN score > 15 THEN 'high' END AS tier FROM people ORDER BY name",
  },
  {
    label: "date literals and DATE_TRUNC",
    builder: (db) =>
      db
        .selectFrom("people")
        .where("joined", "=", new Date("2024-01-02T00:00:00Z"))
        .select((eb) => [eb.fn.dateTrunc("year", "joined").as("year")]),
    sql: "SELECT DATE_TRUNC('year', joined) AS year FROM people WHERE joined = DATE '2024-01-02'",
  },
  {
    label: "multi-condition callback join becomes a nested-loop ON",
    builder: (db) =>
      db
        .selectFrom("people as p")
        .innerJoin("orders as o", (join) =>
          join.onRef("o.person", "=", "p.name").on("o.total", ">", 10),
        )
        .select(["p.name", "o.total"])
        .orderBy("total", "desc"),
    sql: "SELECT p.name AS name, o.total AS total FROM people p JOIN orders o ON o.person = p.name AND o.total > 10 ORDER BY total DESC",
  },
  {
    label: "non-equi callback join",
    builder: (db) =>
      db
        .selectFrom("people as p")
        .innerJoin("orders as o", (join) => join.onRef("o.total", ">", "p.score"))
        .select(["p.name", "o.order_id"])
        .orderBy("order_id"),
    sql: "SELECT p.name AS name, o.order_id AS order_id FROM people p JOIN orders o ON o.total > p.score ORDER BY order_id",
  },
  {
    label: "grouped aggregates with rounded arithmetic",
    builder: (db) =>
      db
        .selectFrom("orders")
        .groupBy("person")
        .select((eb) => [
          eb.ref("person").as("person"),
          eb.fn.round(eb.fn.avg("total"), 1).as("avgTotal"),
          eb.fn.min("total").as("cheapest"),
          eb.fn.max("total").as("priciest"),
        ])
        .orderBy("person"),
    sql: "SELECT person AS person, ROUND(AVG(total), 1) AS avgTotal, MIN(total) AS cheapest, MAX(total) AS priciest FROM orders GROUP BY person ORDER BY person",
  },
  {
    label: "union deduplicates, union all keeps duplicates",
    builder: (db) =>
      db
        .selectFrom("people")
        .where("score", "=", 20)
        .select(["city"])
        .union(db.selectFrom("people").where("city", "=", "London").select(["city"]))
        .orderBy("city"),
    sql: "SELECT city FROM people WHERE score = 20 UNION SELECT city FROM people WHERE city = 'London' ORDER BY city",
  },
  {
    label: "except",
    builder: (db) =>
      db
        .selectFrom("people")
        .select(["name"])
        .except(db.selectFrom("orders").select(["person as name"]))
        .orderBy("name"),
    sql: "SELECT name FROM people EXCEPT SELECT person AS name FROM orders ORDER BY name",
  },
  {
    label: "windows over the whole partition space",
    builder: (db) =>
      db
        .selectFrom("people")
        .select((eb) => [
          eb.ref("name").as("name"),
          eb
            .rank()
            .over((over) => over.orderBy("score", "desc"))
            .as("standing"),
          eb.fn
            .countAll()
            .over((over) => over.partitionBy("city"))
            .as("cityPeers"),
        ])
        .orderBy("name"),
    sql: "SELECT name AS name, RANK() OVER (ORDER BY score DESC) AS standing, COUNT(*) OVER (PARTITION BY city) AS cityPeers FROM people ORDER BY name",
  },
  {
    label: "count distinct executes through the shared desugar",
    builder: (db) =>
      db
        .selectFrom("orders")
        .select((eb) => [eb.fn.count("total").distinct().as("distinctTotals")]),
    sql: "SELECT COUNT(DISTINCT total) AS distinctTotals FROM orders",
  },
  {
    label: "offset pagination",
    builder: (db) => db.selectFrom("people").select(["name"]).orderBy("name").limit(2).offset(2),
    sql: "SELECT name FROM people ORDER BY name LIMIT 2 OFFSET 2",
  },
  {
    label: "CTE joined by name",
    builder: (db) =>
      db
        .with("spend", (creator) =>
          creator
            .selectFrom("orders")
            .groupBy("person")
            .select((eb) => [eb.ref("person").as("person"), eb.fn.sum("total").as("total")]),
        )
        .selectFrom("people as p")
        .innerJoin("spend", "spend.person", "p.name")
        .select(["p.name", "spend.total"])
        .orderBy("name"),
    sql: "WITH spend AS (SELECT person AS person, SUM(total) AS total FROM orders GROUP BY person) SELECT p.name AS name, spend.total AS total FROM people p JOIN spend ON spend.person = p.name ORDER BY name",
  },
  {
    label: "scalar subquery in a comparison",
    builder: (db) =>
      db
        .selectFrom("people")
        .where((eb) =>
          eb(
            "score",
            ">=",
            db.selectFrom("people").select((inner) => [inner.fn.max("score").as("m")]),
          ),
        )
        .select(["name"]),
    sql: "SELECT name FROM people WHERE score >= (SELECT MAX(score) AS m FROM people)",
  },
];

describe("plan and result equivalence", () => {
  for (const { label, builder, sql: sqlText } of equivalences) {
    it(label, async () => {
      const { db, database } = await seeded();
      const query = builder(db);
      const plan = query.compile().plan;
      expect(stripSql(plan)).toEqual(stripSql(compileQuery(sqlText)));
      // Compiling twice is deterministic — no state leaks between compiles.
      expect(stripSql(query.compile().plan)).toEqual(stripSql(plan));
      const rows = await query.execute();
      const reference = await database.query(sqlText);
      expect(rows).toEqual(reference.rows);
      // Executing again returns the same rows — shared expression subtrees stay unmutated.
      expect(await query.execute()).toEqual(reference.rows);
    });
  }
});

describe("builder semantics", () => {
  it("keeps branched builders independent", async () => {
    const { db, database } = await seeded();
    const base = db.selectFrom("people").select(["name"]);
    const high = base.where("score", ">", 15);
    const low = base.where("score", "<", 15);
    const highRows = (await high.orderBy("name").execute()).map((row) => row.name);
    const lowRows = (await low.orderBy("name").execute()).map((row) => row.name);
    expect(highRows).toEqual(["Barbara", "Grace", "Katherine"]);
    expect(lowRows).toEqual(["Ada", "Edsger"]);
    const all = (await base.orderBy("name").execute()).map((row) => row.name);
    expect(all).toHaveLength(5);
    void database;
  });

  it("returns no rows for an empty IN list, like SQL's vacuous membership", async () => {
    const { db } = await seeded();
    const rows = await db.selectFrom("people").where("name", "in", []).select(["name"]).execute();
    expect(rows).toEqual([]);
    const negated = await db
      .selectFrom("people")
      .where("name", "not in", [])
      .select(["name"])
      .execute();
    expect(negated).toHaveLength(5);
  });

  it("shares one subquery builder across two outer queries without interference", async () => {
    const { db } = await seeded();
    const buyers = db.selectFrom("orders").select(["person"]);
    const inBuyers = await db
      .selectFrom("people")
      .where("name", "in", buyers)
      .select(["name"])
      .orderBy("name")
      .execute();
    const notInBuyers = await db
      .selectFrom("people")
      .where("name", "not in", buyers)
      .select(["name"])
      .orderBy("name")
      .execute();
    expect(inBuyers.map((row) => row.name)).toEqual(["Ada", "Barbara", "Grace"]);
    expect(notInBuyers.map((row) => row.name)).toEqual(["Edsger", "Katherine"]);
  });

  it("orders by a non-selected column on both paths, and rejects unknown ones alike", async () => {
    const { db, database } = await seeded();
    // The standard lets a query sort by a source column it does not return; the value travels
    // as a hidden select item and is projected away, so the output columns are unchanged.
    const ordered = ["Edsger", "Ada", "Grace", "Barbara", "Katherine"];
    const built = await db
      .selectFrom("people as p")
      .select(["p.name"])
      .orderBy("p.score")
      .execute();
    expect(built.map((row) => row.name)).toEqual(ordered);
    expect(built.every((row) => Object.keys(row).length === 1)).toBe(true);
    const queried = await database.query("SELECT p.name AS name FROM people p ORDER BY p.score");
    expect(queried.rows).toEqual(built);
    expect(queried.columns).toEqual(["name"]);
    // A column no source has still fails, on both paths, with the same message.
    await expect(database.query("SELECT name FROM people ORDER BY missing")).rejects.toThrow(
      "Ambiguous or missing column: missing",
    );
  });

  it("null comparisons follow three-valued logic exactly as SQL", async () => {
    const { db, database } = await seeded();
    const viaEquals = await db
      .selectFrom("people")
      .where("city", "=", null)
      .select(["name"])
      .execute();
    const referenceEquals = await database.query("SELECT name FROM people WHERE city = NULL");
    expect(viaEquals).toEqual(referenceEquals.rows);
    expect(viaEquals).toEqual([]);
    const viaIs = await db
      .selectFrom("people")
      .where("city", "is", null)
      .select(["name"])
      .orderBy("name")
      .execute();
    expect(viaIs.map((row) => row.name)).toEqual(["Edsger", "Katherine"]);
  });
});

describe("mutation semantics", () => {
  it("updates with expressions over other columns of the same row", async () => {
    const { db, database } = await seeded();
    const result = await db
      .updateTable("orders")
      .set((eb) => ({ total: eb(eb("total", "*", 2), "+", 1) }))
      .where("person", "=", "Ada")
      .executeTakeFirstOrThrow();
    expect(result.numUpdatedRows).toBe(2);
    const rows = await database.query(
      "SELECT total FROM orders WHERE person = 'Ada' ORDER BY total",
    );
    expect(rows.rows.map((row) => row.total)).toEqual([16, 26]);
  });

  it("reports zero-row mutations without error", async () => {
    const { db } = await seeded();
    expect(await db.deleteFrom("people").where("name", "=", "Nobody").executeTakeFirst()).toEqual({
      numDeletedRows: 0,
    });
    expect(
      await db
        .updateTable("people")
        .set({ score: 1 })
        .where("name", "=", "Nobody")
        .executeTakeFirst(),
    ).toEqual({ numUpdatedRows: 0 });
  });

  it("round-trips datetime values through insert and select", async () => {
    const { db } = await seeded();
    const joined = new Date("2025-06-07T00:00:00Z");
    await db.insertInto("people").values({ name: "Annie", score: 1, joined }).execute();
    const row = await db
      .selectFrom("people")
      .where("name", "=", "Annie")
      .select(["joined", "city"])
      .executeTakeFirstOrThrow();
    expect(row.joined).toEqual(joined);
    expect(row.city).toBeNull(); // schema padding filled the omitted nullable column
  });

  it("matches SQL mutation results for the same statements", async () => {
    const first = await seeded();
    const second = await seeded();
    await first.db
      .updateTable("people")
      .set((eb) => ({ score: eb("score", "+", 5) }))
      .where("city", "=", "London")
      .execute();
    await second.database.execute("UPDATE people SET score = score + 5 WHERE city = 'London'");
    const checkSql = "SELECT name, score FROM people ORDER BY name";
    expect((await first.database.query(checkSql)).rows).toEqual(
      (await second.database.query(checkSql)).rows,
    );
  });
});

describe("sql tag hardening", () => {
  it("survives hostile string content", async () => {
    const { db } = await seeded();
    const hostile = "x'; DELETE FROM people; --";
    await db.insertInto("people").values({ name: hostile, score: 1 }).execute();
    const rows = await sql<{ name: string }>`
      SELECT name FROM people WHERE name = ${hostile}
    `.execute(db);
    expect(rows).toEqual([{ name: hostile }]);
    const stillThere = await db.selectFrom("people").select(["name"]).execute();
    expect(stillThere.length).toBe(6);
  });

  it("splices nested fragments with renumbered placeholders", () => {
    const clause = sql`score > ${10}`;
    const spliced = sql`SELECT name FROM people WHERE ${clause} AND city = ${"DC"}`;
    expect(spliced.sql).toBe("SELECT name FROM people WHERE score > $1 AND city = $2");
    expect(spliced.params).toEqual([10, "DC"]);
    // The nested fragment still renders standalone with its own numbering.
    expect(clause.sql).toBe("score > $1");
    expect(clause.params).toEqual([10]);
  });
});

describe("live query selectivity", () => {
  it("skips re-running subscriptions whose tables did not change", async () => {
    const { db, database } = await seeded();
    const set = database.liveQueries();
    const peopleResults: unknown[] = [];
    const subscription = await set.subscribe(
      db.selectFrom("people").select(["name"]).orderBy("name").compile(),
      {
        onChange: (result) => peopleResults.push(result.rows.length),
      },
    );
    expect(peopleResults).toEqual([5]);
    await db.insertInto("orders").values({ order_id: 9, person: "Ada", total: 1 }).execute();
    await set.refresh();
    // The orders commit is visible in stats as an avoided re-run, not a notification.
    expect(peopleResults).toEqual([5]);
    expect(set.stats.rerunsAvoided).toBeGreaterThan(0);
    await db.insertInto("people").values({ name: "Annie", score: 1 }).execute();
    await set.refresh();
    expect(peopleResults).toEqual([5, 6]);
    subscription.close();
    set.close();
  });
});

describe("compile cost guardrail", () => {
  it("keeps builder compilation within a small constant factor of SQL parsing", () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const db = new Minnow<DB>(database, { schema: appSchema });
    const builder = db
      .selectFrom("people as p")
      .innerJoin("orders as o", "o.person", "p.name")
      .where("p.score", ">", 10)
      .where((eb) => eb.or([eb("p.city", "=", "London"), eb("p.city", "is", null)]))
      .groupBy("p.name")
      .having((eb) => eb(eb.fn.countAll(), ">", 1))
      .select(["p.name"])
      .select((eb) => [eb.fn.sum("o.total").as("revenue")])
      .orderBy("revenue", "desc")
      .limit(10);
    const sqlText =
      "SELECT p.name AS name, SUM(o.total) AS revenue FROM people p JOIN orders o ON o.person = p.name WHERE p.score > 10 AND (p.city = 'London' OR p.city IS NULL) GROUP BY p.name HAVING COUNT(*) > 1 ORDER BY revenue DESC LIMIT 10";
    const iterations = 300;
    // Warm up both paths so JIT states are comparable.
    for (let index = 0; index < 20; index += 1) {
      builder.compile();
      compileQuery(sqlText);
    }
    const builderStart = performance.now();
    for (let index = 0; index < iterations; index += 1) builder.compile();
    const builderMs = performance.now() - builderStart;
    const parseStart = performance.now();
    for (let index = 0; index < iterations; index += 1) compileQuery(sqlText);
    const parseMs = performance.now() - parseStart;
    // Generous CI-safe ceilings: catch quadratic blowups, not microsecond noise.
    expect(builderMs).toBeLessThan(1_000);
    expect(builderMs).toBeLessThan(Math.max(parseMs * 5, 250));
  });
});

describe("returning", () => {
  it("echoes inserted rows, including padded nullable columns, in insertion order", async () => {
    const { db } = await seeded();
    const rows = await db
      .insertInto("people")
      .values([
        { name: "Annie", score: 1 },
        { name: "Frances", score: 2, city: "Philadelphia" },
      ])
      .returningAll()
      .execute();
    expect(rows).toEqual([
      { name: "Annie", score: 1, city: null, joined: null },
      { name: "Frances", score: 2, city: "Philadelphia", joined: null },
    ]);
    const projected = await db
      .insertInto("people")
      .values({ name: "Jean", score: 3 })
      .returning(["name", "city"])
      .executeTakeFirstOrThrow();
    expect(projected).toEqual({ name: "Jean", city: null });
  });

  it("returns post-update values for updates and the read rows for deletes", async () => {
    const { db } = await seeded();
    const updated = await db
      .updateTable("people")
      .set((eb) => ({ score: eb("score", "+", 100) }))
      .where("city", "=", "London")
      .returning(["name", "score", "city"])
      .execute();
    expect(updated.sort((a, b) => a.name.localeCompare(b.name))).toEqual([
      { name: "Ada", score: 110, city: "London" },
      { name: "Barbara", score: 120, city: "London" },
    ]);

    const deleted = await db.deleteFrom("people").where("score", ">", 100).returningAll().execute();
    expect(deleted.map((row) => row.name).sort()).toEqual(["Ada", "Barbara"]);
    expect(deleted[0]?.joined instanceof Date || deleted[0]?.joined === null).toBe(true);

    const none = await db
      .deleteFrom("people")
      .where("name", "=", "Nobody")
      .returningAll()
      .execute();
    expect(none).toEqual([]);
    await expect(
      db.deleteFrom("people").where("name", "=", "Nobody").returningAll().executeTakeFirstOrThrow(),
    ).rejects.toThrow("The query returned no result");
  });
});

describe("update set hygiene", () => {
  it("skips undefined entries so spread-patches leave columns untouched", async () => {
    const { db, database } = await seeded();
    const patch: { score?: number; city?: string } = { score: 77 };
    const result = await db
      .updateTable("people")
      .set({ ...patch, city: undefined })
      .where("name", "=", "Ada")
      .executeTakeFirstOrThrow();
    expect(result.numUpdatedRows).toBe(1);
    const ada = await database.query("SELECT score, city FROM people WHERE name = 'Ada'");
    expect(ada.rows).toEqual([{ score: 77, city: "London" }]); // city untouched
    expect(() => db.updateTable("people").set({ city: undefined }).compile()).toThrow(
      "updateTable() requires set()",
    );
  });

  it("supports the two-argument set(column, value) form and validates literals eagerly", async () => {
    const { db, database } = await seeded();
    await db.updateTable("people").set("score", 55).where("name", "=", "Grace").execute();
    const grace = await database.query("SELECT score FROM people WHERE name = 'Grace'");
    expect(grace.rows).toEqual([{ score: 55 }]);
    expect(() => db.updateTable("people").set({ score: Number.NaN })).toThrow(
      "Query literals must be finite numbers",
    );
  });
});

describe("live query lifecycle", () => {
  it("resolves a cancelled iterator promptly instead of waiting for the next commit", async () => {
    const { db } = await seeded();
    const iterator = db.selectFrom("people").select(["name"]).live()[Symbol.asyncIterator]();
    const first = await iterator.next();
    expect(first.done).toBe(false);
    // Park a next() with no pending change, then cancel: return() must win the race.
    const parked = iterator.next();
    const winner = await Promise.race([
      (async () => {
        await iterator.return?.();
        return "returned";
      })(),
      new Promise<string>((resolve) => setTimeout(() => resolve("timeout"), 500)),
    ]);
    expect(winner).toBe("returned");
    expect((await parked).done).toBe(true);
    await db.close();
  });

  it("ends iteration when the database's live set closes", async () => {
    const { db } = await seeded();
    const iterator = db.selectFrom("people").select(["name"]).live()[Symbol.asyncIterator]();
    expect((await iterator.next()).done).toBe(false);
    const parked = iterator.next();
    await db.close();
    const result = await Promise.race([
      parked,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("hung")), 500)),
    ]);
    expect(result.done).toBe(true);
  });

  it("carries cross-tab commit hints through a shared channel name", async () => {
    const store = new MemoryBlockStore();
    const writerDatabase = new MinnowDatabase(store, { rowsPerBlock: 4, compression: "raw" });
    await writerDatabase.migrate(appSchema);
    const writer = new Minnow<DB>(writerDatabase, { schema: appSchema });
    await writer.insertInto("people").values({ name: "Ada", score: 1 }).execute();

    const readerDatabase = new MinnowDatabase(store);
    const channelName = `audit-hints-${String(Date.now())}`;
    const reader = new Minnow<DB>(readerDatabase, { live: { channelName } });
    const counts: number[] = [];
    let notify = (): void => undefined;
    const next = (): Promise<void> =>
      new Promise((resolve) => {
        notify = resolve;
      });
    const initial = next();
    const subscription = await reader
      .selectFrom("people")
      .select(["name"])
      .live()
      .subscribe({
        onChange: (rows) => {
          counts.push(rows.length);
          notify();
        },
      });
    await initial;
    expect(counts).toEqual([1]);

    // The writer's own channel (same name) hints the reader across the "tab" boundary. The
    // writer's live set must exist before the commit — commits hint through live sets.
    const writerHinted = new Minnow<DB>(writerDatabase, {
      schema: appSchema,
      live: { channelName },
    });
    const writerSubscription = await writerHinted
      .selectFrom("people")
      .select(["name"])
      .live()
      .subscribe({ onChange: () => undefined });
    const changed = next();
    await writerHinted.insertInto("people").values({ name: "Grace", score: 2 }).execute();
    await changed;
    expect(counts).toEqual([1, 2]);
    await writerSubscription.close();

    await subscription.close();
    await reader.close();
    await writerHinted.close();
    await writer.close();
  });
});

describe("sql tag lists", () => {
  it("splices fragments inside IN lists and rejects nested arrays", () => {
    const listed = sql`SELECT 1 WHERE x IN ${[1, sql`score + 1`, "a"]}`;
    expect(listed.sql).toBe("SELECT 1 WHERE x IN ($1, score + 1, $2)");
    expect(listed.params).toEqual([1, "a"]);
    expect(() => sql`SELECT ${[[1]] as never}`).toThrow("sql lists cannot nest arrays");
  });
});
