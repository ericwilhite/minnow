# Design: The Public Client as a Connection, Not a Mirror

Status: proposed, August 2026. Companion to [ARCHITECTURE.md](ARCHITECTURE.md). Supersedes the
hand-mirrored `MinnowDatabaseClient` and the ad-hoc `DslDriver` seam in
`packages/core/src/engine/dsl/db.ts`.

## Summary

Replace the 36-method main-thread mirror of `MinnowDatabase` with one small contract —
`MinnowConnection` — that both the in-process engine and the worker client implement. Everything
above it (the `Minnow<DB>` builder, a Kysely dialect, a Drizzle driver, migration tooling) becomes
an in-process consumer of that contract rather than another layer welded to the engine.

The architectural claim is one line: **engine-first means the public client is a driver, not a
mirror.** A mirror has to grow every time the engine does. A driver has one currency and a fixed
set of frames.

There is no usage to migrate. This is the moment the shape is free to change.

## The problem

`MinnowDatabaseClient` (`packages/core/src/engine/client.ts`) restates ~36 engine methods plus
three proxy classes. Adding one engine method costs four edits: engine, worker-host dispatch
whitelist, client method, type re-export.

The drift this predicts has already happened: `dropTable`, `createView`, and `dropView` exist on
`MinnowDatabase` and were never added to the client.

Three defects block the adapter goal specifically:

| Defect                                                 | Evidence                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The driver seam speaks Minnow internals                | `DslDriver.run()` takes `CompiledQuery` (`dsl/db.ts:61`). Kysely and Drizzle emit SQL strings; they can neither satisfy nor consume this contract.                                                                                                                                                                                          |
| Two transaction models, neither adapter-shaped         | `write()` scopes stage batches and expose no SQL DML from the main thread (`ClientWriteSession`, `client.ts:642`); SQL `BEGIN`/`COMMIT` runs through `execute()`. Adapters model a transaction as _a connection you run the same queries against_. Neither gives them one.                                                                  |
| Introspection is thinner than migration planning needs | `listTables()` returns `TableDefinition` — name, type, nullable, default, enum (`database.ts:630`). `planMigration` takes storage's `TableRecord`, which also carries the stable column `id` and `uniqueKeyColumnId` (`schema.ts:433`). Renames key off those IDs, so migration planning is reachable only through Minnow's own schema DSL. |

## Principles

1. **One currency crosses the boundary.** A statement in, a result out. Not `query` and `execute`
   and `run` and `runStatement` and `insertBatch`.
2. **A transaction is a connection.** `transaction(fn)` hands the callback something with the same
   type as the outer connection. This is what every SQL builder ecosystem already assumes, and it
   collapses the two current transaction models into one.
3. **Adapters run in-process, on whichever thread they are on.** A Kysely dialect built on
   `MinnowConnection` never knows whether the engine is in this thread or a worker, and never adds
   a frame to the wire protocol.
4. **The catalog is public, and planning over it is pure.**
   `planMigration(catalog, desired) → Statement[]` with no connection in scope is what makes
   userland schema tooling possible at all.
5. **Operator surface is a different audience.** Compaction, garbage collection, spill cleanup,
   segment inspection, and snapshot import/export belong to devtools, not to applications.

## Layering

```text
L0  MinnowDatabase                 the engine. implementation, not public API.
L1  MinnowConnection               the narrow waist. ~7 methods.
      +- in-process implementation   wraps MinnowDatabase directly
      +- worker implementation       ~7 RPC frames, replacing 36 methods
L2  adapters                       all in-process, either thread
      +- inbound:  Kysely dialect, Drizzle driver
      +- outbound: Minnow<DB> builder
L3  schema management              pure planMigration(Catalog, desired) -> Statement[]
L4  connection.admin               compaction, GC, snapshots, buffer pool, segments
```

Today the chain is `DSL -> driver -> client -> RPC -> engine`, and every layer knows every layer.
Under this design, everything above L1 consumes `MinnowConnection` and never crosses the wire
itself. The worker RPC surface collapses from 36 methods to roughly seven frames.

## The contract

```ts
type Statement = { sql: string; params?: readonly QueryValue[] } | { bulk: BulkWrite };

interface BulkWrite {
  table: string;
  mode: "insert" | "upsert" | "update" | "delete";
  columns: Readonly<Record<string, readonly QueryValue[]>>;
}

interface MinnowConnection {
  execute(statement: Statement, options?: ExecuteOptions): Promise<Result>;
  transaction<T>(fn: (tx: MinnowConnection) => Promise<T>): Promise<T>;
  snapshot<T>(fn: (tx: MinnowConnection) => Promise<T>): Promise<T>;
  subscribe(statement: Statement, handlers: LiveHandlers): Promise<Subscription>;
  introspect(): Promise<Catalog>;
  readonly admin: MinnowAdmin;
  close(): Promise<void>;
}
```

`Result` is the existing `ExecuteResult` union (`database.ts:749`), which already discriminates
rows, DDL outcomes, and per-kind row counts. It does not need reinventing.

### Why the statement is a union and not a string

A Postgres driver speaks only SQL because rows cross a socket regardless. In-process in a browser,
the parser is a measurable cost and the columnar form is free. `{ bulk }` preserves the documented
fast path — `insertBatch` with columnar input skips the parser entirely and sustains roughly
650k rows/s — which rendering a 500k-row `INSERT ... VALUES` as text and parsing it back does not.
Its shape is `table` plus `columns`; no internal type is exposed.

### Why `{ plan }` is deferred

The `Minnow<DB>` builder currently produces a `CompiledQuery` directly and calls `run(plan)`; no
SQL text is ever rendered. Admitting `{ plan }` to the union would preserve that.

It is deferred because:

- the 512-statement LRU plan cache already absorbs repeat parses, so the win is first-call and
  cache-miss only;
- it makes `CompiledQuery` a structured-cloned, version-locked public wire type;
- rendering SQL from the builder converts the builder/SQL plan-parity invariant into
  "builder renders SQL that parses to the right plan", which the existing SQLite/PGlite
  conformance harness can diff more directly than it diffs two plan trees.

It is additive. If measurement shows the parse cost matters, `{ plan }` joins the union later
without breaking a single consumer.

**This is the decision most likely to be wrong.** It should be settled by measuring builder-issued
query latency with and without a render-then-parse round trip, before the builder is re-targeted.

## The catalog

Migration planning needs identity, not just shape. The public catalog gains what `TableRecord`
already carries internally:

```ts
interface Catalog {
  tables: readonly CatalogTable[];
  views: readonly CatalogView[];
}

interface CatalogTable {
  name: string;
  columns: readonly CatalogColumn[];
  uniqueKeyColumnId?: string;
}

interface CatalogColumn {
  id: string; // stable across renames; this is what makes rename planning possible
  name: string;
  type: SimpleDataType;
  nullable: boolean;
  defaultValue?: ColumnDefault;
  enumValues?: readonly string[];
}
```

`planMigration` then takes `Catalog` instead of `readonly TableRecord[]`, and emits `Statement[]`
instead of `MigrationStep[]` interpreted by the engine. Applying a migration becomes running
statements in a transaction — which any userland tool can do.

This single change is what moves schema management out of the engine and into userland. It is the
highest-leverage item in this document.

## What each adapter needs

| Adapter              | Needs from L1                                     | Remaining work                                                                            |
| -------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Drizzle driver       | `execute`, `transaction`                          | Thin session shim. Nearly free.                                                           |
| Kysely dialect       | `execute`, `transaction`, `introspect`            | A Minnow-flavored SQL query compiler. The real work; verified by the conformance harness. |
| Migration tooling    | `introspect`, `transaction`, pure `planMigration` | None beyond the catalog change.                                                           |
| `Minnow<DB>` builder | `execute`, `transaction`, `subscribe`             | Re-target from `DslDriver`; render SQL unless `{ plan }` is admitted.                     |

## Sequencing

Each step is independently landable and independently testable.

1. **Publish the catalog.** Add stable column IDs and unique-key identity to the public shape;
   change `planMigration` to take `Catalog` and emit `Statement[]`. No client changes yet.
2. **Unify transactions** on `transaction(fn) -> MinnowConnection`, with SQL DML permitted inside.
   The engine already supports this via `runStatement(statement, { writer })` (`database.ts:790`);
   it is simply not exposed through the client.
3. **Collapse the entry points.** `query`, `execute`, `run`, `runStatement`, and the batch writers
   become `execute(Statement)`.
4. **Split `admin`.** Move compaction, GC, spill cleanup, `listVisibleSegments`, and snapshot
   import/export off the main contract. Devtools is the consumer; retarget it in the same change.
5. **Rewrite the worker client** against the reduced frame set, and re-target `Minnow<DB>` at
   `MinnowConnection`.
6. **Write the Kysely dialect** as the proof the contract is genuinely adapter-shaped. A contract
   that has never had a foreign adapter built on it is a guess.

Steps 1–2 are worth landing before committing to the rest: if the catalog change does not make a
standalone migration tool writable without engine access, the premise of this document is wrong
and the remaining steps should be reconsidered.

## Exit gates

1. A Kysely dialect and a Drizzle driver both run the shipped SQL conformance suite through
   `MinnowConnection` with no engine imports.
2. A migration tool built only on `introspect()` + `planMigration()` + `transaction()` performs
   an add-column, a rename, and an enum widening, with no access to `MinnowDatabase`.
3. The worker RPC surface is under ten frames, and adding an engine method requires no client edit
   unless it changes the contract.
4. Bulk load throughput through `execute({ bulk })` matches today's `insertBatch` within noise.
5. The in-process and worker connections pass one shared contract test suite.

## Open questions

- **Does `{ plan }` earn its place?** Settle by measurement before step 5, per above.
- **Does `subscribe` belong on the contract or beside it?** Live queries are a Minnow feature with
  no equivalent in the Kysely or Drizzle contracts. Keeping it on `MinnowConnection` is simpler;
  moving it beside keeps the waist closer to what adapters actually use.
- **Streaming results.** An `AsyncIterable<Batch>` across a worker needs real backpressure or it is
  a memory hazard. Deliberately excluded here rather than half-specified.
- **Should `MinnowDatabase` stay exported?** Treating it as non-public simplifies the story but
  removes the zero-ceremony in-process path the current docs open with.
