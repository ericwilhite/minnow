# Minnow `><(((('>`

**A columnar SQL engine that runs entirely in the browser.** Typed queries, real SQL, full-text
search, live results, and durable storage on IndexedDB — no server, no WASM file, no build step.

Minnow is the engine. Use its typed builder directly, or treat it as the layer a framework
binding or ORM-style adapter is written against — the driver contract, the storage contract, and
the worker RPC are all public seams.

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
- **Fast** — columnar blocks, vectorized execution, and a checked-in perf gate that pins every
  query class against native SQLite and PGlite so a regression can't land unnoticed. In-browser
  it leads on analytical reads and on bulk writes at every published scale; SQLite Wasm still
  wins single-key lookups and small writes. The
  [benchmarks](https://minnowdb.dev/benchmarks/) publish the raw captures, including the shapes
  where it loses.
- **Small** — 143 KB gzipped, and no WebAssembly to fetch: about a third of SQLite Wasm and a
  fortieth of PGlite, both of which download a Wasm build before answering anything.
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
  two independent oracles (native SQLite and PGlite) on every test run; results must agree.

## Documentation

Everything else — quick start, the schema DSL, queries, writes, live queries, workers, devtools,
the full SQL feature matrix, benchmark results, contributor test runners, and the API reference —
lives in the [docs site](https://minnowdb.dev/docs/), the single source of truth.

[`ARCHITECTURE.md`](ARCHITECTURE.md) and [`ROADMAP.md`](ROADMAP.md) are internal engineering
records of the design and milestone gates.

## Development

See [Testing & benchmarks](https://minnowdb.dev/docs/testing/) for the contributor workflow,
runner map, release gate, and benchmark capture process.
