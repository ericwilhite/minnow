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
  resultDecoding: { numeric: "number", json: "parse" },
});
```

- **Types from your schema.** `createKysely` — and the exported `InferKyselyDatabase` for
  applications that construct `Kysely` themselves — derives the full `DB` type from the same
  schema declaration Minnow migrates: insert optionality, enum literals, composite keys,
  read-only view columns, and the results of Minnow aggregates and built-ins such as `count`,
  `sum`, `avg`, `round`, and `date_trunc`, without output generics. Raw SQL and custom functions
  still need an explicit output type.
- **Faithful values.** Exact `NUMERIC` results are lossless strings and JSON/JSONB is text by
  default; `resultDecoding` converts them to numbers and parsed objects, and the inferred select
  types follow. Zoneless `DATE` values are canonical `YYYY-MM-DD` strings; timestamps are `Date`.
- **Catalog-owned defaults.** Literal and SQL-expression defaults and `.generatedSql()` stored
  columns run inside Minnow, so they hold for Kysely, raw SQL, batches, workers, and other tabs.
- **Full surface, named refusals.** Reads, writes, `RETURNING`, transactions with nested
  savepoints, schema DDL, streaming, typed live queries, and catalog introspection are supported.
  Forms Kysely can build that Minnow does not run — PostgreSQL features outside its profile, and
  MySQL, SQLite, and T-SQL spellings such as `replaceInto()` or update `LIMIT` — are refused when
  the query compiles, with the feature named and an alternative offered.
- **Search and JSON helpers.** `search.match(eb, columns, query)` and `search.rank(...)` expose
  Minnow's `MATCH` and BM25 SQL. `jsonBuildObject`, `jsonArrayFrom`, and `jsonObjectFrom` from
  `@minnowdb/kysely/helpers` build fully typed nested JSON projections.

Minnow is an embedded database, so there are no PostgreSQL schemas, configurable isolation
levels, roles, or grants. The adapter supports Kysely 0.29.5 and later 0.29.x releases.

Full guide: [minnowdb.com/docs/adapters/kysely](https://minnowdb.com/docs/adapters/kysely/).

## License

MIT
