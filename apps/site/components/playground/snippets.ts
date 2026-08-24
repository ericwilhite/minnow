/**
 * The snippets the TypeScript console offers as a starting point.
 *
 * They deliberately shadow the SQL console's chips — the same questions, asked through the
 * builder — so switching tabs compares two ways of writing one query rather than two demos. Each
 * one also puts a different part of the type system on screen: a row type that follows the select
 * list, an aggregate that only accepts numeric columns, a join that widens a column to null.
 *
 * Every snippet here is executed by `snippets.test.ts` against the same dataset the console
 * builds, and typechecked by the repository's own `tsc`. A chip that throws is the worst thing
 * this page can do.
 */
export interface Snippet {
  id: string;
  label: string;
  /** What the snippet is for, and what it shows about the types. */
  note: string;
  code: string;
}

export const snippets: readonly Snippet[] = [
  {
    id: "revenue-by-month",
    label: "Revenue by month",
    note: "Aggregates and grouping. The row type follows the select list, not the table.",
    code: `import { sql } from "kysely";

// Every completed order, bucketed by month. Hover \`rows\` to see the type the
// select list produced — three keys, not the fourteen columns \`orders\` has.
const month = sql<Date>\`DATE_TRUNC('month', placed_at)\`;
const rows = await db
  .selectFrom("orders")
  .select((eb) => [
    month.as("month"),
    eb.fn.countAll().as("orders"),
    sql<number>\`ROUND(SUM(total), 2)\`.as("revenue"),
  ])
  .where("status", "=", "completed")
  .groupBy(month)
  .orderBy("month", "desc")
  .execute();

console.log(rows);`,
  },
  {
    id: "typed-errors",
    label: "What the types catch",
    note: "Three mistakes the compiler refuses, each with the line to uncomment.",
    code: `// This one runs. The three below it do not — uncomment any of them and the
// editor marks it before anything reaches the database.
const rows = await db
  .selectFrom("customers")
  .select(["name", "city", "loyalty_tier"])
  .where("country", "=", "United Kingdom")
  .limit(5)
  .execute();

// db.selectFrom("custommers");                  // no such table
// db.selectFrom("customers").select(["nmae"]);  // no such column
// db.selectFrom("customers").selectAll().where("signed_up_on", "=", 42); // wrong value type

console.log(rows);`,
  },
  {
    id: "store-performance",
    label: "Store performance",
    note: "A join, and an alias that carries through every later column reference.",
    code: `import { sql } from "kysely";

// Aliasing a table renames it for the whole query: after \`stores as s\`, only
// \`s.\` resolves, and a stale \`stores.name\` is a compile error.
const rows = await db
  .selectFrom("stores as s")
  .innerJoin("orders as o", "o.store_id", "s.store_id")
  .select((eb) => [
    "s.name",
    "s.city",
    eb.fn.countAll().as("orders"),
    sql<number>\`ROUND(SUM(o.total), 2)\`.as("revenue"),
  ])
  .where("o.status", "=", "completed")
  .groupBy(["s.store_id", "s.name", "s.city"])
  .orderBy("revenue", "desc")
  .limit(10)
  .execute();

console.log(rows);`,
  },
  {
    id: "left-join-null",
    label: "A left join widens the type",
    note: "The joined table's columns become nullable, because that is what the join returns.",
    code: `// \`returns\` may have no matching row, so every column it contributes is typed
// \`| null\`. Hover \`refund_amount\` below: the type says what the data does.
const rows = await db
  .selectFrom("order_items as i")
  .leftJoin("returns as r", "r.order_item_id", "i.order_item_id")
  .select(["i.order_item_id", "i.line_total", "r.reason", "r.refund_amount"])
  .where("i.quantity", ">", 2)
  .limit(10)
  .execute();

const refunded = rows.filter((row) => row.refund_amount !== null);
console.log(\`\${refunded.length} of \${rows.length} lines came back\`);
console.log(rows);`,
  },
  {
    id: "customer-value",
    label: "Who spends the most",
    note: "A grouped aggregate with HAVING, ordered by an alias from the select list.",
    code: `import { sql } from "kysely";

const rows = await db
  .selectFrom("customers as c")
  .innerJoin("orders as o", "o.customer_id", "c.customer_id")
  .select((eb) => [
    "c.loyalty_tier",
    eb.fn.count("c.customer_id").distinct().as("customers"),
    sql<number>\`ROUND(SUM(o.total), 2)\`.as("revenue"),
  ])
  .groupBy("c.loyalty_tier")
  .having((eb) => eb(eb.fn.countAll(), ">", 100))
  .orderBy("revenue", "desc")
  .execute();

console.log(rows);`,
  },
  {
    id: "search",
    label: "Full-text search",
    note: "Ranked by BM25, with no index to declare and no schema change to make.",
    code: `import { sql } from "kysely";

const query = "espresso grinder";
const hits = await db
  .selectFrom("products")
  .select([
    "name",
    "category",
    "brand",
    "list_price",
    sql<number>\`BM25(name) AGAINST \${query}\`.as("rank"),
  ])
  .where(sql<boolean>\`MATCH(name) AGAINST \${query}\`)
  .orderBy("rank", "desc")
  .limit(10)
  .execute();

console.log(hits);`,
  },
  {
    id: "raw-sql",
    label: "Dropping to SQL",
    note: "The escape hatch, with values bound as parameters rather than pasted into the text.",
    code: `import { sql } from "kysely";

// \`threshold\` is bound, not interpolated — the statement the engine parses is the
// same one every time, whatever the value is.
const threshold = 400;
const result = await sql<{ status: string; orders: number; revenue: number }>\`
  SELECT status, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue
  FROM orders
  WHERE total > \${threshold}
  GROUP BY status
  ORDER BY revenue DESC\`.execute(db);

console.log(result.rows);`,
  },
  {
    id: "write",
    label: "Writing, and reading it back",
    note: "A parameterized Kysely upsert, then the row the engine wrote.",
    code: `// This writes to the database in your browser. Rebuild above to undo it.
// The conflict clause makes the snippet safe to run twice.
const created = await db
  .insertInto("products")
  .values({
    product_id: 999_001,
    sku: "MIN-999001",
    name: "Minnow House Blend",
    category: "Coffee",
    subcategory: "Whole bean",
    brand: "Minnow",
    unit_cost: 6.4,
    list_price: 15.5,
    launched_on: new Date(),
    discontinued: false,
  })
  .onConflict((conflict) =>
    conflict.column("product_id").doUpdateSet({
      name: "Minnow House Blend",
      unit_cost: 6.4,
      list_price: 15.5,
      launched_on: new Date(),
    }),
  )
  .returningAll()
  .executeTakeFirstOrThrow();

console.log(created);

const found = await db
  .selectFrom("products")
  .select(["product_id", "name", "list_price"])
  .where("brand", "=", "Minnow")
  .execute();

console.log(found);`,
  },
];

export const defaultSnippet = snippets[0]?.code ?? "";
