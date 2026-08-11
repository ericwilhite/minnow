# @browserdatabase/engine

Columnar SQL engine for the browser, with a Kysely-style typed query API, raw SQL, and live
queries. Runs in-worker (`BrowserDatabase`) or on the main thread through an RPC proxy
(`BrowserDatabaseClient`); the typed API works identically over both.

## The typed API

One schema declaration drives everything — migration planning, insert validation, and the
builder's autocomplete:

```ts
import { BrowserDb, column, schema, table, type InferDatabase } from "@browserdatabase/engine";

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

// In a worker: new BrowserDb<DB>(database, { schema: appSchema })
// On the main thread: new BrowserDb<DB>(client, { schema: appSchema })
await driver.migrate(appSchema);
const db = new BrowserDb<DB>(driver, { schema: appSchema });
```

### Selects

```ts
const rows = await db
  .selectFrom("people as p")
  .innerJoin("orders as o", "o.person", "p.name")
  .where("p.score", ">", 10)
  .where((eb) => eb.or([eb("p.city", "=", "London"), eb("p.city", "is", null)]))
  .groupBy("p.name")
  .having((eb) => eb(eb.fn.countAll(), ">", 1))
  .select(["p.name"])
  .select((eb) => [eb.fn.sum("o.total").as("revenue")])
  .orderBy("revenue", "desc")
  .limit(10)
  .execute(); // Array<{ name: string; revenue: number | null }>
```

- String left-hand sides are **column references**; right-hand sides are **values**. Compare two
  columns with `eb.ref("a.x")` or a join's `onRef`.
- `select()` accepts strings (`"p.name"`, `"p.name as owner"`), a callback returning aliased
  expressions, or both across repeated calls; `selectAll()` is `SELECT *`.
- Left-joined columns widen to `| null` in the row type, matching runtime behavior.
- Also available: `distinct()`, `offset()` (after `limit()`), `union`/`unionAll`/`intersect`/
  `except`, `eb.case()`, `eb.between`, `eb.exists`, `eb.fn.{count,countAll,sum,avg,min,max,round,
coalesce,dateTrunc}`, `fn.count(x).distinct()`, window functions (`eb.rowNumber().over(...)`,
  `eb.fn.sum(x).over((o) => o.partitionBy(...).orderBy(...))`), derived tables
  (`selectFrom(subquery.as("t"))`), and CTEs (`db.with("t", (c) => c.selectFrom(...)...)`).
- `execute()`, `executeTakeFirst()`, `executeTakeFirstOrThrow()` (throws `NoResultError`),
  `compile()`.

Every builder query compiles through the **same assembly pipeline as the SQL parser**
(`assembleSelectBlock` and friends), so a builder query and its equivalent SQL produce identical
optimized plans: same validation errors, same DISTINCT/COUNT(DISTINCT)/window desugars, same
execution strategy. `packages/engine/src/dsl/dsl.test.ts` asserts this plan-for-plan.

### Writes

Kysely conventions throughout: `execute()` resolves to an **array** (one result object, or the
`returning(...)` rows), and the idiomatic single-result call is `executeTakeFirst()` /
`executeTakeFirstOrThrow()`:

```ts
const { numInsertedRows } = await db
  .insertInto("people")
  .values([{ name: "Ada", score: 10 }])
  .executeTakeFirstOrThrow();

// returning() rewrites the result type to the projected rows.
const written = await db
  .insertInto("people")
  .values({ name: "Grace", score: 20 }) // omitted nullable columns pad with null
  .returningAll()
  .executeTakeFirstOrThrow(); // { name: "Grace", score: 20, city: null, joined: null }

await db.insertInto("people").values({ name: "Ada", score: 99 }).orReplace().execute(); // upsert

const bumped = await db
  .updateTable("people")
  .set((eb) => ({ score: eb("score", "+", 1) })) // or set("score", 5)
  .where("city", "is", null)
  .returning(["name", "score"]) // post-update values
  .execute();

const removed = await db
  .deleteFrom("people")
  .where("score", "<", 10)
  .returningAll() // the deleted rows
  .execute();
```

- `set({ ...patch })` skips `undefined` entries (Kysely convention) — spread-patches never
  accidentally null a column; explicit `null` still writes NULL. Literals validate eagerly.
- Insert `returning` echoes the written rows — exact here, since the engine has no defaults or
  generated columns. Update/delete `returning` rows come from the statement's own snapshot:
  post-update values for updates, the rows as read for deletes.
- Inserts pivot into the columnar batch APIs (pass `schema` in the options so omitted nullable
  columns pad with null). Updates and deletes compile to the same mutation statements SQL
  parses into and run through the engine's read-keys-then-apply pipeline.

### Live queries

Any select becomes a live query. Subscribers get the current result immediately, then a fresh
result after any commit that may have changed it; unchanged results are digest-suppressed, and
the dependency tables come from the compiled plan (no SQL re-parse):

```ts
const live = db.selectFrom("people").select(["name"]).where("score", ">", 15).live();

const sub = await live.subscribe({
  onChange: (rows) => render(rows),
  onComplete: () => console.log("live set closed"), // optional
});
// or, latest-wins async iteration:
for await (const rows of live) render(rows);

await sub.close();
await db.close(); // closes the shared live set; open iterators end cleanly
```

Iteration ends promptly on `break`, `iterator.return()` (so `Promise.race` timeouts work), or
when the live set closes — a cancelled iterator never waits for the next commit. Cross-tab
invalidation and polling configure once and work over both drivers:
`new BrowserDb(driver, { live: { channelName: "app-commits", pollIntervalMs: 5000 } })`.

### Raw SQL

```ts
import { sql } from "@browserdatabase/engine";

const rows = await sql<{ n: number }>`
  SELECT COUNT(*) AS n FROM people WHERE city IN ${["London", "DC"]}
`.execute(db);
```

Interpolations render as SQL literals (strings escape their quotes, arrays become IN lists,
nested `sql` fragments splice). Values the SQL surface cannot represent — non-finite numbers,
datetimes with a time component — throw instead of silently corrupting.

### Limits (by design)

- Correlated subqueries compile but the engine resolves subqueries at one snapshot, so they fail
  at execution; use joins or uncorrelated forms.
- Set operations chain **left to right** (`a.union(b).intersect(c)` is `(a ∪ b) ∩ c`), unlike
  SQL's INTERSECT-binds-tighter precedence.
- No interactive multi-statement transactions; each batch write commits atomically. A mutation's
  read-then-apply steps run at one snapshot, and a competing writer fails the statement
  explicitly rather than interleaving — `returning` rows reflect exactly what the statement
  computed.
- Result counts are plain numbers (`numInsertedRows: number`), not Kysely's bigints.
