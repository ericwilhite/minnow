import { MemoryBlockStore } from "@browserdatabase/storage-idb";
import { describe, expect, expectTypeOf, it } from "vitest";
import { BrowserDatabase } from "../database.js";
import { compileQuery, type CompiledQuery } from "../query.js";
import { column, schema, table } from "../schema.js";
import { BrowserDb } from "./db.js";
import { NoResultError, type SelectQueryBuilder } from "./select-query-builder.js";
import { sql } from "./sql-tag.js";
import { type InferDatabase } from "./types.js";

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  city: column.string().nullable(),
});
const orders = table("orders", {
  order_id: column.number().unique(),
  person: column.string().references("people", "name"),
  total: column.number(),
});
const appSchema = schema([people, orders]);

type DB = InferDatabase<typeof appSchema>;

/** Removes every (nested) `sql` label so plans compare structurally. */
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

function expectPlanEquivalent(
  builder: { compile(): { plan: CompiledQuery } },
  sqlText: string,
): void {
  expect(stripSql(builder.compile().plan)).toEqual(stripSql(compileQuery(sqlText)));
}

function createDb(): { db: BrowserDb<DB>; database: BrowserDatabase } {
  const database = new BrowserDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 8,
    compression: "raw",
  });
  return { db: new BrowserDb<DB>(database, { schema: appSchema }), database };
}

async function seededDb(): Promise<{ db: BrowserDb<DB>; database: BrowserDatabase }> {
  const { db, database } = createDb();
  await database.migrate(appSchema);
  await db
    .insertInto("people")
    .values([
      { name: "Ada", score: 10, city: "London" },
      { name: "Grace", score: 20, city: "DC" },
      { name: "Katherine", score: 30 },
    ])
    .execute();
  await db
    .insertInto("orders")
    .values([
      { order_id: 1, person: "Ada", total: 12.5 },
      { order_id: 2, person: "Ada", total: 7.5 },
      { order_id: 3, person: "Grace", total: 40 },
    ])
    .execute();
  return { db, database };
}

describe("InferDatabase", () => {
  it("derives table row types keyed by literal table names", () => {
    expectTypeOf<DB>().toEqualTypeOf<{
      people: { name: string; score: number; city: string | null };
      orders: { order_id: number; person: string; total: number };
    }>();
  });
});

describe("select builder plan parity", () => {
  const { db } = createDb();

  it("compiles filters, expressions, ordering, and limits to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom("people as p")
        .where("p.score", ">", 10)
        .select(["p.name"])
        .select((eb) => [eb("p.score", "*", 2).as("doubled")])
        .orderBy("doubled", "desc")
        .limit(5),
      "SELECT p.name AS name, p.score * 2 AS doubled FROM people p WHERE p.score > 10 ORDER BY doubled DESC LIMIT 5",
    );
  });

  it("compiles joins, grouping, aggregates, and having to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom("people as p")
        .innerJoin("orders as o", "o.person", "p.name")
        .groupBy("p.name")
        .having((eb) => eb(eb.fn.countAll(), ">", 1))
        .select((eb) => [
          eb.ref("p.name").as("name"),
          eb.fn.countAll().as("orders"),
          eb.fn.sum("o.total").as("revenue"),
        ])
        .orderBy("revenue", "desc"),
      "SELECT p.name AS name, COUNT(*) AS orders, SUM(o.total) AS revenue FROM people p JOIN orders o ON o.person = p.name GROUP BY p.name HAVING COUNT(*) > 1 ORDER BY revenue DESC",
    );
  });

  it("compiles boolean trees, IN lists, LIKE, and BETWEEN to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom("people as p")
        .where((eb) => eb.or([eb("p.city", "=", "London"), eb("p.city", "is", null)]))
        .where("p.name", "like", "A%")
        .where((eb) => eb.between("p.score", 5, 25))
        .select(["p.name"]),
      "SELECT p.name AS name FROM people p WHERE (p.city = 'London' OR p.city IS NULL) AND p.name LIKE 'A%' AND p.score BETWEEN 5 AND 25",
    );
    expectPlanEquivalent(
      db.selectFrom("people").where("city", "in", ["London", "DC"]).select(["city"]).distinct(),
      "SELECT DISTINCT city FROM people WHERE city IN ('London', 'DC')",
    );
  });

  it("compiles left joins and NOT to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom("people as p")
        .leftJoin("orders as o", "o.person", "p.name")
        .where((eb) => eb.not(eb("p.score", ">=", 100)))
        .select(["p.name", "o.total"]),
      "SELECT p.name AS name, o.total AS total FROM people p LEFT JOIN orders o ON o.person = p.name WHERE NOT p.score >= 100",
    );
  });

  it("compiles CASE and scalar functions to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom("people as p")
        .select((eb) => [
          eb.case().when("p.score", ">", 15).then("high").else("low").end().as("tier"),
          eb.fn.round(eb("p.score", "/", 3), 1).as("third"),
          eb.fn.coalesce("p.city", eb.val("unknown")).as("place"),
        ]),
      "SELECT CASE WHEN p.score > 15 THEN 'high' ELSE 'low' END AS tier, ROUND(p.score / 3, 1) AS third, COALESCE(p.city, 'unknown') AS place FROM people p",
    );
  });

  it("compiles EXISTS and IN subqueries to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom("people as p")
        .where((eb) =>
          eb.exists(
            eb
              .selectFrom("orders as o")
              .where((inner) => inner("o.person", "=", inner.ref("p.name")))
              .select(["o.order_id"]),
          ),
        )
        .select(["p.name"]),
      "SELECT p.name AS name FROM people p WHERE EXISTS (SELECT o.order_id AS order_id FROM orders o WHERE o.person = p.name)",
    );
    expectPlanEquivalent(
      db
        .selectFrom("people")
        .where("name", "in", db.selectFrom("orders").select(["person"]))
        .select(["name"]),
      "SELECT name FROM people WHERE name IN (SELECT person FROM orders)",
    );
  });

  it("compiles derived tables to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom(
          db
            .selectFrom("orders")
            .groupBy("person")
            .select((eb) => [eb.ref("person").as("person"), eb.fn.sum("total").as("spend")])
            .as("t"),
        )
        .where("t.spend", ">", 10)
        .select(["t.person"]),
      "SELECT t.person AS person FROM (SELECT person AS person, SUM(total) AS spend FROM orders GROUP BY person) t WHERE t.spend > 10",
    );
  });

  it("compiles CTEs to the parsed SQL plan", () => {
    const withCte = db.with("spenders", (creator) =>
      creator
        .selectFrom("orders")
        .groupBy("person")
        .select((eb) => [eb.ref("person").as("person"), eb.fn.sum("total").as("spend")]),
    );
    expectPlanEquivalent(
      withCte.selectFrom("spenders").where("spend", ">", 10).select(["person"]),
      "WITH spenders AS (SELECT person AS person, SUM(total) AS spend FROM orders GROUP BY person) SELECT person FROM spenders WHERE spend > 10",
    );
  });

  it("compiles set operations to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db
        .selectFrom("people")
        .select(["name"])
        .unionAll(db.selectFrom("orders").select(["person as name"]))
        .orderBy("name")
        .limit(10),
      "SELECT name FROM people UNION ALL SELECT person AS name FROM orders ORDER BY name LIMIT 10",
    );
    expectPlanEquivalent(
      db
        .selectFrom("people")
        .select(["name"])
        .intersect(db.selectFrom("orders").select(["person as name"])),
      "SELECT name FROM people INTERSECT SELECT person AS name FROM orders",
    );
  });

  it("compiles window functions to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db.selectFrom("people as p").select((eb) => [
        eb.ref("p.name").as("name"),
        eb
          .rowNumber()
          .over((over) => over.partitionBy("p.city").orderBy("p.score", "desc"))
          .as("rank"),
        eb.fn
          .sum("p.score")
          .over((over) => over.partitionBy("p.city"))
          .as("cityTotal"),
      ]),
      "SELECT p.name AS name, ROW_NUMBER() OVER (PARTITION BY p.city ORDER BY p.score DESC) AS rank, SUM(p.score) OVER (PARTITION BY p.city) AS cityTotal FROM people p",
    );
  });

  it("compiles COUNT(DISTINCT) to the parsed SQL plan", () => {
    expectPlanEquivalent(
      db.selectFrom("orders").select((eb) => [eb.fn.count("person").distinct().as("buyers")]),
      "SELECT COUNT(DISTINCT person) AS buyers FROM orders",
    );
  });

  it("compiles selectAll to the parsed SQL plan", () => {
    expectPlanEquivalent(db.selectFrom("people").selectAll(), "SELECT * FROM people");
  });

  it("rejects what the parser rejects, with the parser's errors", () => {
    expect(() =>
      db.selectFrom("people").groupBy("city").select(["city"]).distinct().compile(),
    ).toThrow("SELECT DISTINCT cannot be combined with GROUP BY");
    expect(() => db.selectFrom("people").select(["name", "city as name"]).compile()).toThrow(
      "Duplicate output column: name",
    );
    expect(() => db.selectFrom("people").limit(0)).toThrow("LIMIT must be between 1 and 100,000");
    expect(() => db.selectFrom("people").offset(1)).toThrow("OFFSET requires LIMIT");
    expect(() => db.selectFrom("people").selectAll().select(["name"])).toThrow(
      "SELECT * cannot be mixed with other expressions",
    );
    expect(() => db.selectFrom("people").compile()).toThrow(
      "A query needs select() or selectAll() before compile()",
    );
  });
});

describe("select builder execution", () => {
  it("returns typed rows equal to the SQL results", async () => {
    const { db, database } = await seededDb();
    const rows = await db
      .selectFrom("people as p")
      .innerJoin("orders as o", "o.person", "p.name")
      .groupBy("p.name")
      .select((eb) => [
        eb.ref("p.name").as("name"),
        eb.fn.countAll().as("orders"),
        eb.fn.sum("o.total").as("revenue"),
      ])
      .orderBy("revenue", "desc")
      .execute();
    expectTypeOf(rows).toEqualTypeOf<
      Array<{ name: string; orders: number; revenue: number | null }>
    >();
    const reference = await database.query(
      "SELECT p.name AS name, COUNT(*) AS orders, SUM(o.total) AS revenue FROM people p JOIN orders o ON o.person = p.name GROUP BY p.name ORDER BY revenue DESC",
    );
    expect(rows).toEqual(reference.rows);
    expect(rows[0]).toEqual({ name: "Grace", orders: 1, revenue: 40 });
  });

  it("widens left-joined columns to null in types and results", async () => {
    const { db } = await seededDb();
    const rows = await db
      .selectFrom("people as p")
      .leftJoin("orders as o", "o.person", "p.name")
      .select(["p.name", "o.total"])
      .orderBy("name")
      .execute();
    expectTypeOf(rows).toEqualTypeOf<Array<{ name: string; total: number | null }>>();
    expect(rows.find((row) => row.name === "Katherine")?.total).toBeNull();
  });

  it("supports executeTakeFirst and executeTakeFirstOrThrow", async () => {
    const { db } = await seededDb();
    const first = await db
      .selectFrom("people")
      .select(["name"])
      .where("name", "=", "Ada")
      .executeTakeFirst();
    expectTypeOf(first).toEqualTypeOf<{ name: string } | undefined>();
    expect(first).toEqual({ name: "Ada" });
    expect(
      await db
        .selectFrom("people")
        .select(["name"])
        .where("name", "=", "Nobody")
        .executeTakeFirst(),
    ).toBeUndefined();
    await expect(
      db
        .selectFrom("people")
        .select(["name"])
        .where("name", "=", "Nobody")
        .executeTakeFirstOrThrow(),
    ).rejects.toBeInstanceOf(NoResultError);
  });

  it("executes IN-subqueries and CTE queries", async () => {
    const { db } = await seededDb();
    // Correlated subqueries compile (see plan parity) but the engine resolves subqueries at one
    // snapshot, so execution exercises the uncorrelated form.
    const buyers = await db
      .selectFrom("people")
      .where("name", "in", db.selectFrom("orders").select(["person"]))
      .select(["name"])
      .orderBy("name")
      .execute();
    expect(buyers).toEqual([{ name: "Ada" }, { name: "Grace" }]);

    const spenders = await db
      .with("spend", (creator) =>
        creator
          .selectFrom("orders")
          .groupBy("person")
          .select((eb) => [eb.ref("person").as("person"), eb.fn.sum("total").as("total")]),
      )
      .selectFrom("spend")
      .where("total", ">", 15)
      .select(["person"])
      .orderBy("person")
      .execute();
    expect(spenders).toEqual([{ person: "Ada" }, { person: "Grace" }]);
  });

  it("throws when a subquery builder executes directly", () => {
    const { db } = createDb();
    let captured: SelectQueryBuilder<DB, { p: DB["people"] }, Record<never, unknown>> | undefined;
    db.selectFrom("people as p")
      .where((eb) => {
        captured = eb.selectFrom("people as p");
        return eb("p.score", ">", 0);
      })
      .select(["p.name"])
      .compile();
    expect(captured).toBeDefined();
  });
});

describe("mutation builders", () => {
  it("inserts, updates, and deletes with SQL-equivalent semantics", async () => {
    const { db, database } = await seededDb();
    const inserted = await db
      .insertInto("people")
      .values({ name: "Edsger", score: 5 })
      .executeTakeFirstOrThrow();
    expect(inserted).toEqual({ numInsertedRows: 1 });

    const updated = await db
      .updateTable("people")
      .set((eb) => ({ score: eb("score", "+", 1) }))
      .where("city", "is", null)
      .executeTakeFirstOrThrow();
    expect(updated.numUpdatedRows).toBe(2);
    const scores = await database.query(
      "SELECT name, score FROM people WHERE city IS NULL ORDER BY name",
    );
    expect(scores.rows).toEqual([
      { name: "Edsger", score: 6 },
      { name: "Katherine", score: 31 },
    ]);

    const deleted = await db.deleteFrom("people").where("score", "<", 10).executeTakeFirstOrThrow();
    expect(deleted).toEqual({ numDeletedRows: 1 });
    const remaining = await database.query("SELECT COUNT(*) AS n FROM people");
    expect(remaining.rows[0]?.n).toBe(3);
  });

  it("routes orReplace() to the upsert path", async () => {
    const { db, database } = await seededDb();
    await db
      .insertInto("people")
      .values({ name: "Ada", score: 99, city: "Cambridge" })
      .orReplace()
      .execute();
    const ada = await database.query("SELECT score, city FROM people WHERE name = 'Ada'");
    expect(ada.rows).toEqual([{ score: 99, city: "Cambridge" }]);
  });

  it("mirrors the SQL mutation validation errors", async () => {
    const { db } = await seededDb();
    expect(() => db.updateTable("people").set({ score: 1 }).set({ score: 2 }).compile()).toThrow(
      "UPDATE assignments must set each column once",
    );
    await expect(db.insertInto("people").execute()).rejects.toThrow(
      "insertInto() requires values()",
    );
  });
});

describe("sql template tag", () => {
  it("escapes interpolated values into literals", async () => {
    const { db } = await seededDb();
    const name = "A'da".replace("'", "'"); // literal quote survives escaping
    await db.insertInto("people").values({ name, score: 1 }).execute();
    const rows = await sql<{ name: string }>`
      SELECT name FROM people WHERE name = ${name}
    `.execute(db);
    expect(rows).toEqual([{ name: "A'da" }]);
    const listed = await sql<{ name: string }>`
      SELECT name FROM people WHERE city IN ${["London", "DC"]} ORDER BY name
    `.execute(db);
    expect(listed).toEqual([{ name: "Ada" }, { name: "Grace" }]);
  });

  it("refuses literals the SQL surface cannot represent", () => {
    expect(() => sql`SELECT ${Number.NaN}`).toThrow("finite");
    expect(() => sql`SELECT ${1e21}`).toThrow("cannot render");
    expect(() => sql`SELECT ${new Date("2024-01-02T03:04:05Z")}`).toThrow("DATE literals");
    expect(sql`SELECT ${new Date("2024-01-02T00:00:00Z")}`.sql).toBe("SELECT DATE '2024-01-02'");
  });
});

describe("live queries", () => {
  it("delivers the initial result and re-runs on relevant commits", async () => {
    const { db } = await seededDb();
    const results: Array<Array<{ name: string }>> = [];
    let notify = (): void => undefined;
    const next = (): Promise<void> =>
      new Promise((resolve) => {
        notify = resolve;
      });
    const waited = next();
    const subscription = await db
      .selectFrom("people")
      .select(["name"])
      .where("score", ">", 15)
      .orderBy("name")
      .live()
      .subscribe({
        onChange: (rows) => {
          expectTypeOf(rows).toEqualTypeOf<Array<{ name: string }>>();
          results.push(rows);
          notify();
        },
      });
    await waited;
    expect(results).toEqual([[{ name: "Grace" }, { name: "Katherine" }]]);

    const changed = next();
    await db.insertInto("people").values({ name: "Barbara", score: 50 }).execute();
    await changed;
    expect(results[1]).toEqual([{ name: "Barbara" }, { name: "Grace" }, { name: "Katherine" }]);
    await subscription.close();
    await db.close();
  });

  it("supports async iteration with latest-wins coalescing", async () => {
    const { db } = await seededDb();
    const seen: Array<Array<{ name: string }>> = [];
    const live = db
      .selectFrom("people")
      .select(["name"])
      .where("score", ">", 25)
      .orderBy("name")
      .live();
    for await (const rows of live) {
      seen.push(rows);
      if (seen.length === 1) {
        await db.insertInto("people").values({ name: "Annie", score: 90 }).execute();
      } else {
        break;
      }
    }
    expect(seen[0]).toEqual([{ name: "Katherine" }]);
    expect(seen[1]).toEqual([{ name: "Annie" }, { name: "Katherine" }]);
    await db.close();
  });
});
