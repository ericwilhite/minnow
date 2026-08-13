# Minnow `><(((('>`

**A relational database that runs entirely in the browser.** Typed queries, real SQL, full-text
search, live results, and durable storage on IndexedDB — no server, no WASM file, no build step.

```bash
npm install @minnowdb/core
```

> **Experimental.** The block format carries no compatibility promise yet, and the SQL surface is a
> correctness-first subset, not full SQL-92.

---

## Highlights

- **One schema, everything typed** — a single declaration drives migrations, autocomplete, row
  types, insert validation, and live subscriptions.
- **SQL and a typed builder** — both compile to the same plans, so a builder query and its SQL
  equivalent run identically. Tests enforce it.
- **Full-text search with no index DDL** — `MATCH` / BM25 on any column, with a persisted index
  that builds itself in the background on large tables.
- **Safe across tabs** — writes publish atomically. Another tab sees the old version or the new
  one, never half a write.
- **Reads never block writes** — queries run against an immutable snapshot, so a long read can't be
  torn by a concurrent commit.
- **Built for real data** — compressed columnar blocks, vectorized execution, a memory budget you
  set, and spill to storage when a query outgrows it.
- **Stays fast over time** — compaction and GC are durable, resumable, and cancellable, so they fit
  in idle time instead of blocking the page.
- **Our own engine** — parser, planner, optimizer, and executor are implemented here. No SQLite or
  DuckDB underneath.

## Quick start

```ts
import {
  MinnowDatabase,
  createMinnow,
  column,
  schema,
  table,
  type InferDatabase,
} from "@minnowdb/core";
import { IndexedDbBlockStore } from "@minnowdb/core/storage";

const people = table("people", {
  id: column.number().unique().autoIncrement(),
  name: column.string(),
  score: column.number().default(0),
  joined: column.datetime().default("now"),
  bio: column.string().nullable(),
});

const appSchema = schema([people]);
interface DB extends InferDatabase<typeof appSchema> {}

const database = new MinnowDatabase(await IndexedDbBlockStore.open({ name: "app-db" }));
await database.migrate(appSchema);

const db = createMinnow<DB>(database, { schema: appSchema });

await db
  .insertInto("people")
  .values([{ name: "Ada" }, { name: "Grace", score: 20 }])
  .execute();

const rows = await db
  .selectFrom("people")
  .where("score", ">=", 10)
  .select(["name", "joined"])
  .orderBy("name")
  .execute(); // Array<{ name: string; joined: Date }>
```

There are **four column types on purpose**: `boolean`, `number`, `string`, `datetime`. Numeric
widths, encodings, and row IDs are the engine's job, not schema choices. `column.enum([...])`
is a string column restricted to a closed value set — typed as the literal union and validated
on every write.

---

## API overview

### Schema and migrations

`table()`, `column`, and `schema()` define tables with compile-time row types.

| Modifier                  | Effect                                                             |
| ------------------------- | ------------------------------------------------------------------ |
| `.unique()`               | The table's unique key — one non-nullable column                   |
| `.nullable()`             | Permits NULL and widens the inferred type                          |
| `.autoIncrement()`        | Cross-tab atomic counter for number unique keys                    |
| `.default(value \| fn)`   | A literal or `"now"` filled engine-side; a function (`() => ulid()`) filled by the typed facade |
| `.renamedFrom(name)`      | Rename via stable column ID — metadata only, not a drop-and-add    |
| `.references(table, col)` | Declared relation, stored as catalog metadata                      |

`migrate()` diffs the live catalog and applies metadata-only steps: create a table, add a nullable
column, rename, widen nullability, change a default. Each step is one atomic swap, so an
interrupted migration finishes by re-running. Changes that would rewrite stored data — type
changes, drops, unique-key changes — are **rejected instead of guessed at**.

### Typed queries

`createMinnow(database, { schema })` returns a Kysely-style builder: `selectFrom`, joins, `where`,
`groupBy`, `having`, `orderBy`, `limit`, expression helpers, CTEs, subqueries, set operations, and
aggregate and window functions. Left-joined columns type as `| null`, and `sum` over an empty group
types as `number | null` — result types don't lie.

### SQL

```ts
const result = await database.query("SELECT name FROM people WHERE score >= 10");
const prepared = await database.prepareQuery("SELECT ..."); // pins one immutable version
console.log(await database.explain("SELECT ...")); // optimized plan
```

`prepareQuery()` pins a snapshot and decodes only the columns it needs, so repeated `execute()`
calls reuse it. The supported and rejected surface is checked in at
[`sql-feature-matrix.json`](packages/core/sql-feature-matrix.json), with an executable example per
entry and a conformance test that keeps it honest. **Unsupported syntax fails explicitly** rather
than being silently reinterpreted.

### Full-text search

Any column is searchable — no index DDL, no schema marking. A search treats the row as one
document; matching is Unicode-aware and deterministic, and prefix terms end in `*`.

```ts
// Filter by match, rank by BM25, expose the score as _score
const hits = await db
  .selectFrom("articles")
  .select(["title"])
  .search("quick fo*")
  .limit(10)
  .execute();

// The pieces, separately
await db
  .selectFrom("articles")
  .select((eb) => ["title", eb.fn.bm25(["title", "body"], "quick fox").as("score")])
  .where((eb) => eb.match(["title", "body"], "quick fox"))
  .orderBy("score", "desc")
  .execute();
// SELECT title, BM25(title, body) AGAINST 'quick fox' AS score FROM articles
// WHERE MATCH(title, body) AGAINST 'quick fox' ORDER BY score DESC

// Every table at once, merged into one ranked list
await db.search("quick fox", { limit: 20 }); // Array<{ table, row, score }>
```

Once a searched table crosses ~4096 rows, a **persisted index builds itself in the background** and
inserts maintain it atomically. It is a pruning accelerator, never ground truth: every candidate is
re-verified, and any invalidating mutation silently falls back to a scan and rebuilds.

### Writes

Typed `insertInto`, `updateTable`, and `deleteFrom` cover most work, with exact `returning` —
including engine-generated defaults and auto-increment keys. Underneath, `insertBatch`,
`upsertBatch`, `updateBatch`, and `deleteBatch` take arrays of rows or whole columns for bulk loads,
and `bufferedWriter()` groups single rows into batches that flush on a row, byte, or age limit.

Every write validates column names, row counts, nulls, and types before storing anything, and
reports rows, bytes, timing, retries, and write amplification. A competing writer **fails your
statement with a typed error** instead of interleaving.

### Live queries

```ts
const live = db.selectFrom("people").where("score", ">=", 10).select(["name"]).live();
```

Subscriptions re-run when their tables change, **across tabs**. Every hint path — a local commit, a
cross-tab message, or a poll tick — converges on the durable manifest version, so a missed message
delays a refresh but cannot leave a stale result. A subscription keeps only its SQL, its table
dependencies, and a result digest — never rows — and stays quiet when a rerun produces the same
digest.

### Workers

The engine runs wherever you construct it. For real datasets, put it in a worker. The whole API is
async, so your code is the same either way.

```ts
import { MinnowDatabaseClient } from "@minnowdb/core/client";

const client = new MinnowDatabaseClient(
  new Worker(new URL("@minnowdb/core/worker", import.meta.url), { type: "module" }),
  { store: { kind: "indexeddb", name: "app-db" } },
);
const db = createMinnow<DB>(client, { schema: appSchema });
```

### Testing and maintenance

`MemoryBlockStore` runs the real engine — tables, queries, compaction — with no browser storage.
`compactTable()` rewrites older segments into fewer, larger ones; `collectGarbage()` reclaims
unreachable data. Both are durable, resumable, and cancellable, with `...Step()` variants you can
advance one step at a time.

### Devtools

An embeddable SQL console for the database your app is already using. It is a separate package, so
it never reaches a production bundle unless you put it there.

```ts
import { mountMinnowDevtools } from "@minnowdb/devtools";

if (import.meta.env.DEV) mountMinnowDevtools(db);
```

A launcher appears in the corner (`Cmd/Ctrl + Shift + D`). Browse tables in a windowed grid with
sortable headers and typed filters, edit records inline, or run SQL in the console. The panel
floats over your page without blocking it, every change is described and confirmed first, and
`permissions: { write: false }` refuses those statements outright. It also ships as
`<minnow-devtools>`, a custom element that works in any framework. See
[the devtools guide](apps/site/src/content/docs/devtools.mdx), or run the docs site and open
`/docs/playground/` to try the panel against a database running in the page.

### Modules

| Import                        | Contents                                            |
| ----------------------------- | --------------------------------------------------- |
| `@minnowdb/core`              | `MinnowDatabase`, `createMinnow`, schema DSL, types |
| `@minnowdb/core/storage`      | `IndexedDbBlockStore`, `MemoryBlockStore`           |
| `@minnowdb/core/client`       | `MinnowDatabaseClient` main-thread proxy            |
| `@minnowdb/core/worker`       | The shipped worker entry                            |
| `@minnowdb/core/testing`      | Test helpers                                        |
| `@minnowdb/core/transactions` | Snapshots, leases, transaction manager              |
| `@minnowdb/core/block-format` | Block encoding and decoding                         |
| `@minnowdb/devtools`          | Embeddable SQL console panel                        |

---

## Performance

Latest capture (2026-08-12) on a 956,160-row, 50-table commerce dataset, 15 oracle-verified queries
against SQLite Wasm, DuckDB-Wasm, and PGlite in real browsers:

- **~516k rows/s** insert through the public batch API against IndexedDB (Chromium)
- **~6× faster** than SQLite Wasm on repeated-execution medians in Chromium, **~31×** in Firefox
- **~18.9 MB** stored for that dataset

Single-host, single-run methodology — treat as observations, not a leaderboard. Full records and
raw JSON are in [`benchmarks/`](benchmarks/).

## Documentation

The full docs cover the schema DSL, the query builder, writes and transactions, live queries,
workers, best practices, architecture, and the complete API reference. They aren't published
publicly yet — run the site locally:

```bash
npm run site:dev
```

[`packages/core/README.md`](packages/core/README.md) is the typed API guide.
[`ARCHITECTURE.md`](ARCHITECTURE.md) and [`ROADMAP.md`](ROADMAP.md) cover the design and the
milestone gates.

## Development

```bash
npm install
npm run check
```

`npm run check` is the local format, lint, type, build, and unit-test gate. For the full release
gate — real IndexedDB plus the browser dashboard suite:

```bash
npx playwright install chromium firefox webkit
npm run check:release
```

`@minnowdb/bench` is the internal benchmark and test harness: a browser lab that generates the
commerce dataset, verifies every value it stores, checks results against an independent JavaScript
oracle, and compares Minnow against SQLite Wasm, DuckDB-Wasm, and PGlite. `@minnowdb/site` is the
public docs site.

```bash
npm run dev --workspace @minnowdb/bench
```
