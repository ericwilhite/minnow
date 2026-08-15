# Minnow `><(((('>`

**A relational database that runs entirely in the browser.** Typed queries, real SQL, full-text
search, live results, and durable storage on IndexedDB — no server, no WASM file, no build step.

```bash
npm install @minnowdb/core
```

> **Experimental.** The block format carries no compatibility promise yet.

## Features

- **One schema, everything typed** — a single declaration drives migrations, autocomplete, row
  types, insert validation, and live subscriptions.
- **SQL and a Kysely-style typed builder** — both compile to the same plans and run identically;
  parameters bind per execution against a cached plan.
- **SQL:2016 surface** — joins, CTEs, set operations, window functions, grouping sets, quantified
  comparisons, upserts with `RETURNING`; the supported and rejected forms ship as a checked-in
  feature matrix, and deliberate omissions are documented with reasons.
- **Full-text search with no index DDL** — `MATCH` / BM25 on any column, with a persisted index
  that builds itself in the background on large tables.
- **Fast** — columnar blocks, vectorized execution, and a perf gate that keeps every query class
  ahead of native SQLite; in-browser, it leads SQLite Wasm and PGlite on queries and bulk ingest
  at every published dataset scale.
- **Safe across tabs** — writes publish atomically; readers see the old version or the new one,
  never half a write. Reads never block writes.
- **Always fresh** — every query observes the latest committed state, even commits from another
  tab; stale reads are unrepresentable. Multi-statement consistency is an explicit `snapshot()`
  scope that releases itself.
- **Built for real data** — compression, a memory budget you set, spill to storage, and durable,
  resumable compaction and GC that fit in idle time.
- **Workers first** — a shipped worker entry and main-thread client; the API is identical on
  either side of the boundary.
- **Live queries** — subscriptions re-run when their tables change, across tabs, and stay quiet
  when results are unchanged.
- **Devtools** — an embeddable SQL console and data browser for the database your app is already
  using, shipped as a separate package.
- **Our own engine** — parser, planner, optimizer, and executor implemented here; no SQLite or
  DuckDB underneath.
- **Differentially tested** — a seeded query corpus runs through the engine's two executors and
  three independent oracles (native SQLite, PGlite, DuckDB) on every test run; results must
  agree.

## Documentation

Everything else — quick start, the schema DSL, queries, writes, live queries, workers, devtools,
the full SQL feature matrix, benchmark results, and the API reference — lives in the docs site,
which is the single source of truth. It isn't published publicly yet; run it locally:

```bash
npm run site:dev
```

[`ARCHITECTURE.md`](ARCHITECTURE.md) and [`ROADMAP.md`](ROADMAP.md) are internal engineering
records of the design and milestone gates.

## Development

```bash
npm install
npm run check
```

`npm run check` is the local format, lint, type, build, and unit-test gate;
`npm run check:release` adds real-browser suites (run `npx playwright install chromium firefox
webkit` once first). `@minnowdb/bench` is the internal measurement harness; `@minnowdb/site` is
the docs site.
