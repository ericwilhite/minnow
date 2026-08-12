# Minnow `><(((('>`

Minnow is a relational database that runs entirely in the browser. You get typed queries, real SQL,
live results, and durable storage on top of IndexedDB — no server, no WASM file, no build step.

> **Experimental.** The storage format carries no compatibility promise yet, and the SQL surface is
> a correctness-first subset, not full SQL-92.

## Why Minnow

- **One schema, everything typed.** A single declaration drives migrations, autocomplete, row
  types, insert validation, and live subscriptions.
- **Real SQL and a typed builder.** Both compile to the same plans, so a builder query and its SQL
  equivalent run identically. Tests enforce that.
- **Safe across tabs.** Writes publish atomically. Another tab sees the old version or the new one,
  never half a write.
- **Reads never block writes.** Queries run against an immutable snapshot, so a long read cannot be
  torn by a concurrent commit.
- **Built for large data in a browser.** Compressed columnar blocks, typed vectors, a memory budget
  you set, and spill to storage when a query outgrows it.
- **Stays fast over time.** Background compaction and garbage collection are restart-safe and
  resumable, so they fit in idle time instead of blocking the page.
- **Easy to test.** Swap in an in-memory store and your tests run the real engine without touching
  browser storage.
- **Our own engine.** Parser, planner, optimizer, and executor are implemented here — no SQLite or
  DuckDB underneath.

## Install

```bash
npm install @minnowdb/core
```

## Quick start

Declare a schema, open a store, migrate, and query.

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
  name: column.string().unique(),
  score: column.number(),
  joined: column.datetime().nullable(),
});
const appSchema = schema([people]);
interface DB extends InferDatabase<typeof appSchema> {}

const database = new MinnowDatabase(await IndexedDbBlockStore.open({ name: "app-db" }));
await database.migrate(appSchema);

const db = createMinnow<DB>(database, { schema: appSchema });

await db
  .insertInto("people")
  .values([
    { name: "Ada", score: 10, joined: new Date() },
    { name: "Grace", score: 20 }, // joined reads as null
  ])
  .execute();

const rows = await db
  .selectFrom("people")
  .where("score", ">=", 10)
  .select(["name", "joined"])
  .orderBy("name")
  .execute(); // Array<{ name: string; joined: Date | null }>
```

There are four column types on purpose: `boolean`, `number`, `string`, and `datetime`. Numeric
widths, encodings, and row IDs are the engine's job, not schema choices.

## API overview

### Schema and migrations

`table()`, `column`, and `schema()` define tables with compile-time row types. `migrate()` diffs
the live catalog and applies metadata-only steps: create a table, add a nullable column, rename a
column, widen nullability. Each step is one atomic swap, so an interrupted migration finishes by
re-running. Changes that would rewrite stored data — type changes, drops, unique-key changes — are
rejected instead of guessed at.

### Typed queries

`createMinnow(database, { schema })` returns a Kysely-style builder: `selectFrom`, joins, `where`,
`groupBy`, `having`, `orderBy`, `limit`, expression helpers, and aggregate and window functions.
Left-joined columns type as `| null`.

### SQL

`database.query(sql)` runs a statement. `database.prepareQuery(sql)` pins one immutable version and
decodes only the columns it needs, so repeated `execute()` calls reuse that snapshot. `explain()`
prints the optimized plan. The supported and rejected SQL surface is checked in at
[`packages/core/sql-feature-matrix.json`](packages/core/sql-feature-matrix.json), with an executable
example for every entry and a conformance test that keeps it honest. Unsupported syntax fails
explicitly rather than being silently reinterpreted.

### Writes

Typed `insertInto`, `updateTable`, and `deleteFrom` cover most work. Underneath, `insertBatch`,
`upsertBatch`, `updateBatch`, and `deleteBatch` take arrays of rows or whole columns for bulk
loads, and `bufferedWriter()` groups single rows into batches that flush on a row, byte, or age
limit. Every write validates column names, row counts, nulls, and types before storing anything,
and reports rows, bytes, timing, retries, and write amplification.

### Live queries

`database.liveQueries()` subscribes to a statement and re-runs it when its tables change, across
tabs. Every hint path — a local commit, a cross-tab message, or a poll tick — converges on the
durable manifest version, so a missed message delays a refresh but cannot leave a stale result. A
subscription keeps only its SQL, its table dependencies, and a result digest — never rows — and
stays quiet when a rerun produces the same digest.

### Workers

The engine runs wherever you construct it. For real datasets, put it in a worker: import the
shipped worker entry and drive it from `MinnowDatabaseClient` on the main thread. The whole API is
async, so your code is the same either way.

```ts
import { MinnowDatabaseClient } from "@minnowdb/core/client";

const client = new MinnowDatabaseClient(
  new Worker(new URL("@minnowdb/core/worker", import.meta.url), { type: "module" }),
  { store: { kind: "indexeddb", name: "app-db" } },
);
const db = createMinnow<DB>(client, { schema: appSchema });
```

### Testing

`MemoryBlockStore` from `@minnowdb/core/storage` runs the real engine — tables, queries, compaction
— with no browser storage.

### Maintenance

`compactTable()` rewrites older segments into fewer, larger ones, and `collectGarbage()` reclaims
data nothing can reach anymore. Both are durable, resumable, and cancellable: you can advance them
a step at a time and stop whenever the page needs the time back.

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

## Documentation

The full docs cover the schema DSL, the query builder, writes and transactions, live queries,
workers, best practices, architecture, and the complete API reference. They are not published
publicly yet. Until they are, run the site locally:

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
npx playwright install chromium firefox webkit
npm run check:release
```

`npm run check` is the local format, lint, type, build, and unit-test gate. `npm run check:release`
adds the real IndexedDB library suite and the full browser dashboard suite.

`@minnowdb/bench` is the internal benchmark and test harness — a browser lab that generates a
50-table commerce dataset, verifies every value it stores, checks results against an independent
JavaScript oracle, and compares Minnow against SQLite Wasm, DuckDB-Wasm, and PGlite.
`@minnowdb/site` is the public docs site.

```bash
npm run dev --workspace @minnowdb/bench
npm run dev --workspace @minnowdb/site
```
