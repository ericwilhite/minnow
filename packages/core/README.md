# @minnowdb/core

Columnar SQL engine for the browser, with a Kysely-style typed query API, raw SQL, and live
queries. Runs in-worker (`MinnowDatabase`) or on the main thread through an RPC proxy
(`MinnowDatabaseClient`); the typed API works identically over both.

```bash
npm install @minnowdb/core
```

- **One schema, everything typed** — `table()`, `column`, and `schema()` drive migrations,
  autocomplete, row types, insert validation, and live subscriptions.
- **SQL and a typed builder, one engine** — both compile through the same plan assembly; the
  SQL surface ships as a checked-in feature matrix (`@minnowdb/core/sql-feature-matrix.json`).
- **Full-text search** — `MATCH` / BM25 on any column, no index DDL.
- **Live queries** — `.live()` subscriptions that update across tabs.
- **Durable browser storage** — compressed columnar blocks on IndexedDB, atomic multi-tab
  commits, snapshot reads, compaction and GC.
- **Subpath exports** — `/storage`, `/client`, `/worker`, `/transactions`, `/block-format`,
  `/testing`.

Quick start, guides, benchmark results, contributor workflows, and the complete API reference live
in the [Minnow docs](https://minnowdb.dev/docs/) — the single source of truth.
