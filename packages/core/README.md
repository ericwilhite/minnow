# @minnowdb/core

Columnar SQL engine for the browser: real SQL over immutable snapshots, durable on IndexedDB, with
no WebAssembly to download. It runs in the thread that constructs it (`MinnowDatabase`), or in a
worker with a main-thread client that mirrors it call for call (`MinnowDatabaseClient`).

**[minnowdb.com](https://minnowdb.com)** — documentation, a live console, and benchmarks you
run yourself.

```bash
npm install @minnowdb/core
```

- **SQL is the contract** — parser, planner, optimizer, and a vectorized executor written here,
  with no SQLite or DuckDB underneath. The supported surface ships as a checked-in feature matrix
  (`@minnowdb/core/sql-feature-matrix.json`) that the engine is tested against.
- **One schema, in TypeScript** — `table()`, `column`, and `schema()` declare tables once and
  `migrate()` evolves them without rewriting stored data.
- **Full-text search** — `MATCH` / BM25 on any column, with no index DDL to write.
- **Live queries** — `liveQueries()` hands back a set whose subscribers get a fresh result after
  any commit that could have changed it, across tabs.
- **Durable browser storage** — compressed columnar blocks on IndexedDB, atomic multi-tab commits,
  snapshot reads, compaction and GC.
- **Bounded memory** — execution works in batches under a budget you set and spills to storage
  rather than failing.
- **Subpath exports** — `/storage`, `/client`, `/worker`, `/worker-protocol`, `/transactions`,
  `/plan`, `/block-format`, `/testing`.

Guides, the API reference, and the SQL feature matrix live in the
[docs](https://minnowdb.com/docs/), which are the single source of truth. Building with an agent?
[minnowdb.com/agent-rules.md](https://minnowdb.com/agent-rules.md) is a short rules file to drop
into an `AGENTS.md`, and [llms.txt](https://minnowdb.com/llms.txt) indexes the documentation in
markdown.

Every `@minnowdb` package shares a major version and moves independently inside it, so install
them on the same major. See [Versioning](https://minnowdb.com/docs/reference/versioning/).

## License

MIT
