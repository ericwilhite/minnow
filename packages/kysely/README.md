# @minnowdb/kysely

Run Kysely queries against Minnow's browser-local SQL engine. The dialect uses Kysely's PostgreSQL
compiler and works with an in-thread `MinnowDatabase` or a worker-backed
`MinnowDatabaseClient`.

```bash
npm install @minnowdb/core @minnowdb/kysely kysely
```

```ts
import { createKysely } from "@minnowdb/kysely";
import { column, schema, table } from "@minnowdb/core";

const appSchema = schema([
  table("orders", {
    id: column.integer().unique().autoIncrement(),
    total: column.numeric({ precision: 12, scale: 2 }).default("0"),
  }),
]);

await minnowDatabase.migrate(appSchema);
const db = createKysely({
  driver: minnowDatabase,
  schema: appSchema,
});
```

`InferKyselyDatabase<typeof appSchema>` is also exported for applications that construct
`Kysely` themselves. It preserves nullable/default insert optionality, enum literals, logical SQL
domain boundary types, primary-key update safety, composite keys, and read-only view columns.
Exact `NUMERIC` results are lossless strings, while inserts, updates, and predicates accept native
numbers or strings.

Pass the schema to `createKysely` (or `MinnowDialect`) for exact types and multi-row empty-object
INSERT normalization. Literal and SQL-expression defaults remain catalog-owned and run inside
Minnow for Kysely, raw SQL, batches, workers, and other tabs. Compiled queries contain ordinary
PostgreSQL SQL with visible `DEFAULT` slots and no hidden generated parameters.

The dialect supports reads, inserts, updates, deletes, `RETURNING`, transactions, schema DDL, and
catalog introspection, including Minnow's exact numeric, JSON, UUID, time, interval, array, and enum
types. Minnow is an embedded database, so it does not provide PostgreSQL schemas, streaming
results, configurable isolation levels, roles, or grants. The adapter supports Kysely 0.29.x.

Full guide: [minnowdb.com/docs/adapters/kysely](https://minnowdb.com/docs/adapters/kysely/).

## License

MIT
