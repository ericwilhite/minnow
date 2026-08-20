# Minnow `><(((('>`

**A columnar SQL engine that runs entirely in the browser.** Real SQL over immutable snapshots,
durable on IndexedDB or OPFS — no server, no WebAssembly to download, no build step.

**[minnowdb.com](https://minnowdb.com)** — documentation, a live console, and benchmarks you
run yourself.

```bash
npm install @minnowdb/core
```

> **Experimental.** Minnow is in 0.x, so breaking changes can land in a minor release and the
> block format carries no compatibility promise yet. Every package shares a major version and
> moves independently inside it — see
> [Versioning](https://minnowdb.com/docs/reference/versioning/).

## Features

- **Our own engine** — parser, planner, optimizer, and vectorized executor implemented here. No
  SQLite or DuckDB underneath, and nothing to compile before the first query answers.
- **SQL:2023 surface** — joins (including `NATURAL` and `USING`), CTEs including recursive,
  set operations, window functions with `GROUPS` frames and `EXCLUDE`, grouping sets with
  `GROUPING`, row constructors, quantified comparisons, the standard string and datetime
  functions, SQL/JSON (`JSON_VALUE`, `JSON_QUERY`, `IS JSON`), upserts with `RETURNING`, and
  triggers. Every supported and rejected form ships as a checked-in feature matrix keyed to the
  standard's own feature identifiers, executed on every test run and diffed against SQLite and
  PostgreSQL; deliberate omissions are documented with reasons.
- **Full-text search with no index DDL** — `MATCH` / BM25 on any column, with a persisted index
  that builds itself in the background once a table is large enough to want one.
- **Columnar and durable** — immutable compressed column blocks on IndexedDB or OPFS. Writes publish
  atomically; another tab sees the old version or the new one, never half of one.
- **Pluggable storage** — the storage contract is a public, documented interface with a
  [conformance kit](https://minnowdb.com/docs/storage/custom/) and an adapter toolkit, so a
  store for another substrate — the Node filesystem, React Native, an object store — is an
  adapter away, with no engine changes.
- **Snapshot reads** — every query executes against one version, reads never block writes, and
  stale reads are unrepresentable. Multi-statement consistency is an explicit scope that releases
  itself.
- **Bounded memory** — execution works in batches under a budget you set and spills to storage
  rather than failing. A whole table is never required to be resident.
- **Small** — about 172 KB gzipped with every storage adapter included, and no Wasm blob: just
  over a third of SQLite's WebAssembly build and a thirty-second of PGlite's, both of which
  download and compile a module before answering anything. The adapters tree-shake, so an app
  that uses one store ships less.
- **Workers first** — a shipped worker entry and a main-thread client with an identical API.
- **Snapshots** — copy one committed version out as a portable file and load it into any store.
- **[Typed client](https://minnowdb.com/docs/client/), optional** — `@minnowdb/client` adds a schema-aware query builder with inferred
  row types, declared foreign keys and CHECK constraints, views, and live queries. It ships
  separately and is built only from the engine's published primitives, so anyone can build their
  own layer the same way.
- **Extensible** — a published catalog with stable column IDs, plan-construction primitives, and a
  machine-readable SQL feature matrix. See [Extending Minnow](https://minnowdb.com/docs/reference/extending/).
- **Devtools** — an embeddable SQL console and data browser, shipped as a separate package.
- **Differentially tested** — a seeded query corpus runs through both executors and two
  independent oracles (native SQLite and PGlite) on every test run; results must agree.

## Documentation

Everything else — installation, running SQL, the language surface, the client API, transactions,
workers, storage adapters, and the API reference — lives on the
[docs site](https://minnowdb.com/docs/).

[Comparison](https://minnowdb.com/docs/comparison/) is the page to read first if you are choosing
between Minnow, IndexedDB, Dexie, SQLite Wasm, PGlite, and DuckDB-Wasm; it says where each of the
others is the better answer.

Two things there are worth knowing about specifically:

- **[The console](https://minnowdb.com/#console)** — a real database of around 590,000 rows,
  generated in your browser and stored in IndexedDB, on the home page. Write whatever SQL you
  like against it, or switch to the TypeScript tab and query the same database through the typed
  client, in an editor holding the declarations the packages publish.
- **[Benchmarks](https://minnowdb.com/benchmarks/)** — Minnow against SQLite Wasm and PGlite,
  run live on your machine at a dataset size you choose. There are no published numbers to take
  on trust; every result is checked against an independent oracle before its timing counts.

For agents and language models, the site publishes itself in machine-readable form:
[`llms.txt`](https://minnowdb.com/llms.txt), [`llms-full.txt`](https://minnowdb.com/llms-full.txt),
a `.md` twin of every page, the SQL surface as
[JSON](https://minnowdb.com/sql-feature-matrix.json), and
[`agent-rules.md`](https://minnowdb.com/agent-rules.md) — a short rules file to drop into an
`AGENTS.md`. See [AI agents & LLMs](https://minnowdb.com/docs/reference/agents/).

[`ARCHITECTURE.md`](ARCHITECTURE.md) and [`ROADMAP.md`](ROADMAP.md) are internal engineering
records of the design and milestone gates.

## Development

```bash
npm install
npm run check          # format, typecheck, lint, build, unit tests with coverage floors
npm run site:dev       # the docs site, the console and the benchmarks
npm run test:browser   # library and site tests in real browsers
npm run soak           # generative suites on fresh random seeds, to find new failures
npm run version:set -- minor @minnowdb/core   # one package; `major` moves them all together
npm run release:publish -- --dry-run          # what a release would send to npm
```

`npm install` points git at `.githooks`, where a pre-push hook runs `npm run check` — the same
suite, in the same order, as CI's first job. A red build is then something you see before the
push rather than after it. The hook checks the working tree rather than the commits being pushed,
so a file another session left broken stops you too; `git push --no-verify` is the way past it
when that is what you want.

See [Testing](https://minnowdb.com/docs/reference/testing/) for the full runner map and the
release gate, and [Versioning](https://minnowdb.com/docs/reference/versioning/) for how a release
is cut.

## Deployment

The docs site is a static export deployed to Vercel, whose project root directory is `apps/site`
with "Include source files outside of the Root Directory" enabled — the build compiles the
workspace packages the site imports, so it needs the rest of the repository present.

[`apps/site/vercel.json`](apps/site/vercel.json) carries the cross-origin isolation headers the
benchmarks page needs to reach SQLite's OPFS backend. They cannot live in `next.config.mjs`,
because a static export serves plain files and Next.js drops its `headers()` config there.

Documentation for an older release is the same site built from that release's tag with
`SITE_BASE_PATH=/v0.1`, deployed at that prefix and listed in `apps/site/public/versions.json`.
Every path it emits is versioned with it, and the picker in the docs sidebar moves between them.
See [Versioning](https://minnowdb.com/docs/reference/versioning/).

The benchmarks rule is written twice, and the trailing slash is the reason. A `source` matches a
request path ending in `/` neither as `/benchmarks` nor as `/benchmarks/:path*`, and
`trailingSlash` means the document is only ever served at `/benchmarks/` -- so the literal form is
what covers the page itself, and the pattern covers anything below it. Check a change to these by
reading `crossOriginIsolated` in the console on the deployed page; when it is false, SQLite has
silently fallen back to the handle pool and the benchmarks measure a different browser.

## License

MIT — see [LICENSE](LICENSE).
