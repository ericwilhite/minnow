# @minnowdb/core

A columnar SQL database for the browser. Run PostgreSQL-style SQL over durable IndexedDB or OPFS
storage with no server and no WebAssembly module.

```bash
npm install @minnowdb/core
```

- Direct SQL through `MinnowDatabase.query()` and `execute()`.
- Joins, CTEs, window functions, grouping sets, upserts, `RETURNING`, triggers, exact decimals,
  JSON/JSONB, arrays, enums, sequences, and savepoints.
- Compressed column storage, secondary indexes, full-text search, and snapshot reads.
- Atomic writes across tabs through IndexedDB or OPFS.
- A ready-made worker client with the same everyday database API.
- TypeScript schema declarations and metadata-only migrations, including SQL domains and
  composite primary/foreign keys.
- Batch writes, live queries, snapshots, compaction, and configurable query memory.

Use [the PostgreSQL compatibility page](https://minnowdb.com/docs/sql/feature-matrix/) for the
exact SQL surface. Use [the documentation](https://minnowdb.com/docs/) for installation, storage,
workers, transactions, and API details.

The optional [Kysely dialect](https://minnowdb.com/docs/adapters/kysely/) connects Kysely's
PostgreSQL compiler to the engine and derives its `DB` types from the same schema declaration.

Every `@minnowdb` package in one application must use the same major version. See
[Versioning](https://minnowdb.com/docs/reference/versioning/).

## License

MIT
