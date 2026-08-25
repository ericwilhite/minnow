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
numbers or strings. Schema-derived databases also infer `count()` and `countAll()` results as
Minnow's runtime `number`, infer `sum()` and `avg()` from the selected numeric domain, and infer
the fixed return types of Minnow built-ins such as `round` and `date_trunc` without explicit
generics. Value-returning functions such as `coalesce`, `nullif`, `greatest`, and `least` infer from
their arguments, and `cast()` infers from its supported SQL target. Exact-numeric aggregates
remain lossless strings; ordinary numeric aggregates are numbers, and every aggregate except
`count` includes `null` for an empty input. Use Kysely's inferred `coalesce()` helper when a
fallback removes that case. Arbitrary custom functions and raw SQL still need an output type
because TypeScript cannot derive one from a function or SQL string it does not understand.

Pass the schema to `createKysely` (or `MinnowDialect`) for exact types and multi-row empty-object
INSERT normalization. Literal and SQL-expression defaults remain catalog-owned and run inside
Minnow for Kysely, raw SQL, batches, workers, and other tabs. Compiled queries contain ordinary
PostgreSQL SQL with visible `DEFAULT` slots and no hidden generated parameters.

The dialect supports reads, inserts, updates, deletes, `RETURNING`, transactions, schema DDL,
streaming, typed live queries, and catalog introspection, including Minnow's exact numeric, JSON,
UUID, time, interval, array, and enum types. Minnow is an embedded database, so it does not provide
PostgreSQL schemas, configurable isolation levels, roles, or grants. The adapter supports Kysely
0.29.x.

`search.match(eb, columns, query)` and `search.rank(eb, columns, query)` expose Minnow's `MATCH`
and BM25 SQL with checked, non-empty column lists, a bound query parameter, and an inferred numeric
rank.

Full guide: [minnowdb.com/docs/adapters/kysely](https://minnowdb.com/docs/adapters/kysely/).

## License

MIT
