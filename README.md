# Minnow `><(((('>`

**A columnar SQL database for the browser.** Minnow runs PostgreSQL-style SQL over durable data in
IndexedDB or OPFS. It needs no server and has no WebAssembly module to download or compile.

```bash
npm install @minnowdb/core
```

[minnowdb.com](https://minnowdb.com) has the full documentation, a live SQL and Kysely console,
and benchmarks that run in your browser.

> **Experimental.** Minnow is in 0.x. Minor releases can include breaking changes, and the stored
> block format may change. Pin exact package versions in applications. See
> [Versioning](https://minnowdb.com/docs/reference/versioning/).

## Why Minnow

- **PostgreSQL-style SQL.** Use familiar joins, CTEs, window functions, grouping sets, upserts,
  `RETURNING`, triggers, exact decimals, JSON/JSONB, arrays, enums, sequences, savepoints, and
  more. The [PostgreSQL compatibility guide](https://minnowdb.com/docs/sql/feature-matrix/) lists
  exact matches, differences, extensions, and exclusions.
- **Fast analytical reads over application data.** Compressed column blocks let a query read only
  the columns it needs and skip blocks that cannot match. Secondary indexes speed up selective
  lookups and ordered reads.
- **Durable browser storage.** IndexedDB and OPFS adapters publish writes atomically. Every query
  reads one stable snapshot, even when another tab commits at the same time.
- **Plain JavaScript.** The complete engine is about 220 KB gzipped, with no Wasm download,
  compile step, special headers, or server process.
- **Direct SQL or Kysely.** Run PostgreSQL-style SQL through the engine API or use Kysely through
  `@minnowdb/kysely`. Kysely's `DB` type derives from the same schema used for migration, so tables
  and columns are declared once. Both paths use the same SQL engine.
- **Built for responsive applications.** A ready-made worker client keeps query work off the UI
  thread. Batch execution and spillable sorts and aggregates keep large working sets under a
  configurable budget.
- **Search and reactive views.** Full-text search uses `MATCH` and BM25 without index DDL. Live
  queries refresh after relevant commits, including commits from other tabs.
- **Portable and extensible.** Snapshots move a committed database between stores. The public
  storage interface includes a shared test kit for custom adapters.

Minnow is designed for browser-local application data that needs both keyed reads and analytical
queries. It does not provide a server, replication, or multi-device sync.

## Packages

| Package              | Purpose                                                                   |
| -------------------- | ------------------------------------------------------------------------- |
| `@minnowdb/core`     | SQL engine, schema management, workers, storage adapters, and snapshots.  |
| `@minnowdb/kysely`   | Optional Kysely dialect using Kysely's PostgreSQL compiler.               |
| `@minnowdb/devtools` | Optional SQL console and data browser for development and embedded demos. |

Start with [Installation](https://minnowdb.com/docs/installation/) and
[Your first query](https://minnowdb.com/docs/first-query/). The
[Comparison](https://minnowdb.com/docs/comparison/) page explains when IndexedDB, Dexie, SQLite
Wasm, PGlite, or DuckDB-Wasm is a better fit.

For coding agents, use [llms.txt](https://minnowdb.com/llms.txt) or add the concise
[Minnow agent rules](https://minnowdb.com/agent-rules.md) to your project.

## Development

```bash
npm install
npm test               # unit tests and the standard SQLLogicTest profile
npm run check          # format, types, lint, build, coverage, and SQL tests
npm run test:sql:full  # full supported SQLLogicTest profile
npm run test:browser   # library and site tests in real browsers
npm run site:dev       # docs, live console, and browser benchmarks
```

See [Testing and benchmarks](https://minnowdb.com/docs/reference/testing/) for the complete test
map and [Versioning](https://minnowdb.com/docs/reference/versioning/) for release commands.

## License

MIT — see [LICENSE](LICENSE).
