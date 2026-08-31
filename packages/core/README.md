# @minnowdb/core

A columnar SQL database for the browser. Run PostgreSQL-style SQL over durable IndexedDB or OPFS
storage with no server and no WebAssembly module.

```bash
npm install @minnowdb/core
```

- Direct SQL through `MinnowDatabase.query()` and `execute()`.
- Joins, CTEs, window functions, grouping sets, nested correlated subqueries, subquery-backed
  mutations, upserts, `RETURNING`, triggers, exact decimals, nested JSON/JSONB, stored generated
  columns, zoneless DATE, arrays, enums, sequences, and savepoints.
- Compressed column storage, secondary indexes, full-text search, and snapshot reads.
- Atomic writes across tabs through IndexedDB or OPFS, strict durability by default, and explicit
  origin-eviction persistence policy.
- A ready-made worker client with the same everyday database API.
- TypeScript schema declarations and metadata-only migrations, including SQL domains,
  composite primary/foreign keys, and informational relationships.
- Batch and upsert write APIs, typed catalog errors, and per-column result type metadata.
- Pull-driven query cursors, live queries, snapshots, compaction, and configurable query memory.

Use [the PostgreSQL compatibility page](https://minnowdb.com/docs/sql/feature-matrix/) for the
exact SQL surface. Use [the documentation](https://minnowdb.com/docs/) for installation, storage,
workers, transactions, and API details.

The optional [Kysely dialect](https://minnowdb.com/docs/adapters/kysely/) connects Kysely's
PostgreSQL compiler to the engine and derives its `DB` types from the same schema declaration.

Minnow is 0.x: breaking changes land in minor releases, so pin exact versions and upgrade
`@minnowdb` packages together. See
[Versioning](https://minnowdb.com/docs/reference/versioning/).

## License

MIT
