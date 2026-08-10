import { MemoryBlockStore } from "@browserdatabase/storage-idb";
import { describe, expect, expectTypeOf, it } from "vitest";
import { BrowserDatabase } from "./database.js";
import {
  add,
  avg,
  count,
  eq,
  from,
  gt,
  inList,
  max,
  mul,
  nullableRefs,
  refs,
  round,
  sum,
  type TypedQuery,
} from "./orm.js";
import { compileQuery, type CompiledQuery } from "./query.js";
import { column, schema, table, typedTable } from "./schema.js";

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

/** Structural plan equality modulo the original SQL text. */
function normalize(plan: CompiledQuery): unknown {
  const { sql, ...rest } = plan;
  void sql;
  return JSON.parse(JSON.stringify(rest)) as unknown;
}

function expectPlanEquivalent(query: TypedQuery<unknown>, sql: string): void {
  expect(normalize(query.plan)).toEqual(normalize(compileQuery(sql)));
}

async function seededDatabase(): Promise<BrowserDatabase> {
  const database = new BrowserDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 8,
    compression: "raw",
  });
  await database.migrate(schema([people, orders]));
  await typedTable(database, people).insert([
    { name: "Ada", score: 10, city: "London" },
    { name: "Grace", score: 20, city: "DC" },
    { name: "Katherine", score: 30 },
  ]);
  await typedTable(database, orders).insert([
    { order_id: 1, person: "Ada", total: 12.5 },
    { order_id: 2, person: "Ada", total: 7.5 },
    { order_id: 3, person: "Grace", total: 40 },
  ]);
  return database;
}

describe("ORM query builder", () => {
  it("builds plans equivalent to compiled SQL across representative shapes", () => {
    const p = refs(people, "p");
    const o = refs(orders, "o");

    expectPlanEquivalent(
      from(people, "p")
        .where(gt(p.score, 10))
        .select({ name: p.name, doubled: mul(p.score, 2) })
        .orderBy("doubled", "desc")
        .limit(5)
        .build(),
      "SELECT p.name AS name, p.score * 2 AS doubled FROM people p WHERE p.score > 10 ORDER BY doubled DESC LIMIT 5",
    );

    expectPlanEquivalent(
      from(people, "p")
        .innerJoin(orders, "o", eq(o.person, p.name))
        .groupBy(p.name)
        .having(gt(count(), 1))
        .select({ name: p.name, orders: count(), revenue: sum(o.total) })
        .orderBy("revenue", "desc")
        .build(),
      "SELECT p.name AS name, COUNT(*) AS orders, SUM(o.total) AS revenue FROM people p JOIN orders o ON o.person = p.name GROUP BY p.name HAVING COUNT(*) > 1 ORDER BY revenue DESC",
    );

    expectPlanEquivalent(
      from(people, "p")
        .where(inList(p.city, ["London", "DC"]))
        .select({ city: p.city })
        .distinct()
        .build(),
      "SELECT DISTINCT p.city AS city FROM people p WHERE p.city IN ('London', 'DC')",
    );

    expectPlanEquivalent(
      from(people, "p")
        .leftJoin(orders, "o", eq(o.person, p.name))
        .select({ name: p.name, total: nullableRefs(orders, "o").total })
        .build(),
      "SELECT p.name AS name, o.total AS total FROM people p LEFT JOIN orders o ON o.person = p.name",
    );

    expectPlanEquivalent(
      from(people, "p")
        .select({
          bumped: round(add(p.score, 1), 1),
          top: max(p.score),
          mean: avg(p.score),
        })
        .build(),
      "SELECT ROUND(p.score + 1, 1) AS bumped, MAX(p.score) AS top, AVG(p.score) AS mean FROM people p",
    );
  });

  it("returns typed rows equal to the SQL results", async () => {
    const database = await seededDatabase();
    const p = refs(people, "p");
    const o = refs(orders, "o");

    const query = from(people, "p")
      .innerJoin(orders, "o", eq(o.person, p.name))
      .groupBy(p.name)
      .select({ name: p.name, orders: count(), revenue: sum(o.total) })
      .orderBy("revenue", "desc")
      .build();
    const rows = await database.run(query);
    expectTypeOf(rows).toEqualTypeOf<
      Array<{ name: string; orders: number; revenue: number | null }>
    >();
    const sql = await database.query(
      "SELECT p.name AS name, COUNT(*) AS orders, SUM(o.total) AS revenue FROM people p JOIN orders o ON o.person = p.name GROUP BY p.name ORDER BY revenue DESC",
    );
    expect(rows).toEqual(sql.rows);
    expect(rows[0]).toEqual({ name: "Grace", orders: 1, revenue: 40 });

    const leftRows = await database.run(
      from(people, "p")
        .leftJoin(orders, "o", eq(o.person, p.name))
        .select({ name: p.name, total: nullableRefs(orders, "o").total })
        .orderBy("name")
        .build(),
    );
    expectTypeOf(leftRows).toEqualTypeOf<Array<{ name: string; total: number | null }>>();
    expect(leftRows.find((row) => row.name === "Katherine")?.total).toBeNull();
  });

  it("rejects invalid builder states explicitly", () => {
    expect(() => from(people).build()).toThrow("A query needs a select() shape before build()");
    const p = refs(people, "p");
    expect(() =>
      from(people, "p").groupBy(p.city).select({ city: p.city }).distinct().build(),
    ).toThrow("distinct() cannot be combined with groupBy()");
  });
});
