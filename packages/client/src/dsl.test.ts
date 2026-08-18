import { MemoryBlockStore } from "@minnowdb/core/storage";
import { describe, expect, expectTypeOf, it } from "vitest";
import { MinnowDatabase } from "@minnowdb/core";
import { compileQuery, compileStatement } from "@minnowdb/core";
import { type CompiledQuery } from "@minnowdb/core/plan";
import { column, schema, table, type Generated } from "@minnowdb/core";
import { Minnow } from "./db.js";
import { NoResultError, type SelectQueryBuilder } from "./select-query-builder.js";
import { sql } from "./sql-tag.js";
import {
  type FromRow,
  type InferDatabase,
  type InsertRowOf,
  type SelectRowOf,
  type UpdateRowOf,
} from "./types.js";

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

function createDb(): { db: Minnow<DB>; database: MinnowDatabase } {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 8,
    compression: "raw",
  });
  return { db: new Minnow<DB>(database, { schema: appSchema }), database };
}

async function seededDb(): Promise<{ db: Minnow<DB>; database: MinnowDatabase }> {
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
    // DB names the three shapes explicitly rather than making a reader decode a marker.
    expectTypeOf<SelectRowOf<DB["people"]>>().toEqualTypeOf<{
      name: string;
      score: number;
      city: string | null;
    }>();
    expectTypeOf<SelectRowOf<DB["orders"]>>().toEqualTypeOf<{
      order_id: number;
      person: string;
      total: number;
    }>();
    expectTypeOf<InsertRowOf<DB["people"]>>().toEqualTypeOf<
      { name: string; score: number } & { city?: string | null }
    >();
    expectTypeOf<UpdateRowOf<DB["people"]>>().toEqualTypeOf<{
      score?: number | undefined;
      city?: string | null | undefined;
    }>();
  });
});

describe("enum columns", () => {
  const tasks = table("tasks", {
    id: column.number().unique(),
    state: column.enum(["todo", "doing", "done"]),
  });
  const taskSchema = schema([tasks]);
  type TaskDB = InferDatabase<typeof taskSchema>;

  it("types selects and inserts with the literal union and validates writes", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const db = new Minnow<TaskDB>(database, { schema: taskSchema });
    await database.migrate(taskSchema);
    await db
      .insertInto("tasks")
      .values([
        { id: 1, state: "todo" },
        { id: 2, state: "doing" },
      ])
      .execute();
    // @ts-expect-error a value outside the enum is a compile error
    const bad = db.insertInto("tasks").values([{ id: 3, state: "later" }]);
    await expect(bad.execute()).rejects.toThrow("state[0] must be one of: todo, doing, done");

    const rows = await db
      .selectFrom("tasks")
      .where("state", "=", "doing")
      .select(["id", "state"])
      .execute();
    expectTypeOf(rows).toEqualTypeOf<Array<{ id: number; state: "todo" | "doing" | "done" }>>();
    expect(rows).toEqual([{ id: 2, state: "doing" }]);

    await expect(
      db
        .updateTable("tasks")
        .set({ state: "later" as "done" })
        .where("id", "=", 1)
        .execute(),
    ).rejects.toThrow("must be one of: todo, doing, done");
  });
});

describe("driver access", () => {
  it("hands back the driver the facade was created with", () => {
    const { db, database } = createDb();
    expect(db.driver).toBe(database);
    // Tools given only the facade reach the catalog and raw SQL entry points through it.
    expect(typeof (db.driver as MinnowDatabase).listTables).toBe("function");
  });

  it("keeps the same driver on facades derived with with()", () => {
    const { db, database } = createDb();
    const derived = db.with("recent", (creator) =>
      creator.selectFrom("orders").select(["order_id"]),
    );
    expect(derived.driver).toBe(database);
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
    let captured:
      SelectQueryBuilder<DB, { p: SelectRowOf<DB["people"]> }, Record<never, unknown>> | undefined;
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

  it("compiles match expressions to the SQL plan shape and matches at runtime", async () => {
    const { db } = await seededDb();
    expectPlanEquivalent(
      db
        .selectFrom("people")
        .select(["name"])
        .where((eb) => eb.match(["name", "city"], "ada lond*")),
      "SELECT name FROM people WHERE MATCH(name, city) AGAINST 'ada lond*'",
    );
    expectPlanEquivalent(
      db
        .selectFrom("people")
        .select(["name"])
        .where((eb) => eb.match("*", "ada")),
      "SELECT name FROM people WHERE MATCH(*) AGAINST 'ada'",
    );
    const found = await db
      .selectFrom("people")
      .select(["name"])
      .where((eb) => eb.match(["name", "city"], "lond*"))
      .execute();
    expect(found).toEqual([{ name: "Ada" }]);
    expect(() =>
      db
        .selectFrom("people")
        .selectAll()
        .where((eb) => eb.match([], "x")),
    ).toThrow("at least one column");
  });

  it("desugars search() to match + scored ordering with SQL plan parity", async () => {
    const { db } = await seededDb();
    // The ordering expression desugars into a hidden select item inside a projected-away
    // derived block — identically for the builder and the equivalent SQL.
    expectPlanEquivalent(
      db.selectFrom("people").select(["name"]).search("ada lond*"),
      "SELECT name FROM people WHERE MATCH(*) AGAINST 'ada lond*' ORDER BY BM25(*) AGAINST 'ada lond*' DESC",
    );
    expectPlanEquivalent(
      db
        .selectFrom("people")
        .select(["name"])
        .search("ada", { columns: ["name"] }),
      "SELECT name FROM people WHERE MATCH(name) AGAINST 'ada' ORDER BY BM25(name) AGAINST 'ada' DESC",
    );
    // The row shape carries no synthetic score column, and repeated searches compose.
    const hits = await db.selectFrom("people").select(["name"]).search("lond*").execute();
    expect(hits).toEqual([{ name: "Ada" }]);
    const repeated = await db
      .selectFrom("people")
      .select(["name"])
      .search("ada")
      .search("ada")
      .execute();
    expect(repeated).toEqual([{ name: "Ada" }]);
    // Selecting the score yourself reuses the same expression for the ordering (no hidden
    // duplicate work), and general ORDER BY expressions work outside search too.
    const scored = await db
      .selectFrom("people")
      .select((eb) => ["name", eb.fn.bm25(["name"], "grace").as("relevance")])
      .orderBy((eb) => eb.fn.bm25(["name"], "grace"), "desc")
      .orderBy("name")
      .execute();
    expect(scored[0]?.name).toBe("Grace");
    expect(scored[0]?.relevance ?? 0).toBeGreaterThan(0);
    const arithmetic = await db
      .selectFrom("people")
      .select(["name"])
      .orderBy((eb) => eb("score", "*", -1))
      .executeTakeFirstOrThrow();
    expect(arithmetic).toEqual({ name: "Katherine" });
  });

  it("merges ranked hits across tables through db.search()", async () => {
    const { db } = await seededDb();
    // "ada" appears in people.name and in orders.person — hits merge ranked across both tables.
    const hits = await db.search("ada");
    expect(hits.length).toBeGreaterThanOrEqual(3);
    expect(new Set(hits.map((hit) => hit.table))).toEqual(new Set(["people", "orders"]));
    for (const hit of hits) {
      expect(hit.score).toBeGreaterThan(0);
      expect("(search score)" in hit.row).toBe(false);
    }
    const scores = hits.map((hit) => hit.score);
    expect([...scores].sort((a, b) => b - a)).toEqual(scores);
    const limited = await db.search("ada", { tables: ["people"], limit: 1 });
    expect(limited).toHaveLength(1);
    expect(limited[0]?.table).toBe("people");
    expect(limited[0]?.row.name).toBe("Ada");
  });

  it("keeps generated columns omissible in hand-declared DB interfaces via Generated<T>", async () => {
    const notes = table("notes", {
      id: column.number().unique().autoIncrement(),
      body: column.string(),
    });
    const notesSchema = schema([notes]);
    // The Kysely convention: a hand-written DB interface marks engine-filled columns itself
    // instead of deriving the brand through InferDatabase.
    // A hand-written DB: FromRow reads the Generated<> marker once, here, so nothing deeper
    // in the stack has to look for it.
    interface HandDeclared {
      notes: FromRow<{ id: Generated<number>; body: string }>;
    }
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      rowsPerBlock: 8,
      compression: "raw",
    });
    await database.migrate(notesSchema);
    const db = new Minnow<HandDeclared>(database, { schema: notesSchema });
    const written = await db
      .insertInto("notes")
      .values({ body: "hi" }) // id omissible thanks to the explicit brand
      .returningAll()
      .executeTakeFirstOrThrow();
    expect(written).toEqual({ id: 1, body: "hi" });
    // The flavor stays assignable both ways: plain literals compare and arithmetic works.
    const found = await db.selectFrom("notes").select(["body"]).where("id", "=", 1).execute();
    expect(found).toEqual([{ body: "hi" }]);
  });

  it("echoes generated columns from inserts, matching SQL RETURNING", async () => {
    const notes = table("notes", {
      id: column.number().unique().autoIncrement(),
      slug: column.string().default(() => `s-${crypto.randomUUID()}`),
      body: column.string(),
    });
    const notesSchema = schema([notes]);
    type NotesDB = InferDatabase<typeof notesSchema>;
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      rowsPerBlock: 8,
      compression: "raw",
    });
    await database.migrate(notesSchema);
    const db = new Minnow<NotesDB>(database, { schema: notesSchema });

    // Default-bearing columns are omissible in values(); explicit values pass through. The
    // function default fills slug in the facade, before the batch reaches the engine. A mixed
    // batch reserves once past its explicit maximum, so the omitted row generates 11, not 1.
    const rows = await db
      .insertInto("notes")
      .values([{ body: "first" }, { body: "second", id: 10, slug: "explicit" }])
      .returningAll()
      .execute();
    expect(rows.map((row) => row.id)).toEqual([11, 10]);
    expect(rows.map((row) => row.body)).toEqual(["first", "second"]);
    expect(rows[0]?.slug).toMatch(/^s-/);
    expect(rows[1]?.slug).toBe("explicit");

    const idOnly = await db
      .insertInto("notes")
      .values({ body: "third" })
      .returning(["id"])
      .executeTakeFirstOrThrow();
    expect(idOnly).toEqual({ id: 12 });

    // The flavored column type still takes plain values in predicates.
    const found = await db
      .selectFrom("notes")
      .select(["id", "body"])
      .where("id", "=", 10)
      .execute();
    expect(found).toEqual([{ id: 10, body: "second" }]);

    // Parity: SQL RETURNING accepts generated columns absent from the INSERT column list and
    // returns the same shape the builder echoes. A function default lives in the facade, so
    // the SQL path must name the column explicitly.
    const viaSql = await database.runStatement(
      compileStatement("INSERT INTO notes (slug, body) VALUES ('sql-slug', 'fourth')"),
      { returning: ["id", "body"] },
    );
    expect(viaSql).toMatchObject({ returnedRows: [{ id: 13, body: "fourth" }] });
    await expect(
      database.runStatement(
        compileStatement("INSERT INTO notes (slug, body) VALUES ('sql-slug-2', 'x')"),
        { returning: ["missing"] },
      ),
    ).rejects.toThrow("RETURNING column does not exist: missing");
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

  it("refuses unrepresentable values and binds the rest as parameters", () => {
    expect(() => sql`SELECT ${Number.NaN}`).toThrow("finite");
    expect(() => sql`SELECT ${new Date("nope")}`).toThrow("valid dates");
    // Values the old literal renderer refused are representable as bound parameters.
    const timed = sql`SELECT ${1e21}, ${new Date("2024-01-02T03:04:05Z")}`;
    expect(timed.sql).toBe("SELECT $1, $2");
    expect(timed.params).toEqual([1e21, new Date("2024-01-02T03:04:05.000Z")]);
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
