# @minnowdb/core

Columnar SQL engine for the browser: real SQL over immutable snapshots, durable on IndexedDB,
with no WebAssembly to download. Runs in a worker (`MinnowDatabase`) or on the main thread through
an RPC proxy (`MinnowDatabaseClient`); the API is identical over both.

```bash
npm install @minnowdb/core
```

- **One schema, everything typed** — `table()`, `column`, and `schema()` drive migrations,
  autocomplete, row types, insert validation, and live subscriptions.
- **SQL is the contract** — every statement compiles through the same plan assembly, and the
  supported surface ships as a checked-in feature matrix
  (`@minnowdb/core/sql-feature-matrix.json`). The optional `@minnowdb/client` adds a typed builder
  over the same published primitives.
- **Full-text search** — `MATCH` / BM25 on any column, no index DDL.
- **Live queries** — `.live()` subscriptions that update across tabs.
- **Durable browser storage** — compressed columnar blocks on IndexedDB, atomic multi-tab
  commits, snapshot reads, compaction and GC.
- **Subpath exports** — `/storage`, `/client`, `/worker`, `/transactions`, `/block-format`,
  `/testing`.

Quick start, guides, benchmark results, contributor workflows, and the complete API reference live
in the [Minnow docs](https://minnowdb.com/docs/) — the single source of truth.

## License

MIT
