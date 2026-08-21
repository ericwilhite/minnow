# @minnowdb/client

The typed query client for [Minnow](https://minnowdb.com): a schema-aware builder over
`@minnowdb/core`, with inferred row types, typed mutations, and live queries.

**[minnowdb.com](https://minnowdb.com)** — documentation, a live console, and benchmarks you
run yourself.

```bash
npm install @minnowdb/core @minnowdb/client
```

```ts
import { MinnowDatabase, column, schema, table } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { createMinnow, type InferDatabase } from "@minnowdb/client";

const appSchema = schema([
  table("orders", { order_id: column.number().unique(), total: column.number() }),
]);

interface DB extends InferDatabase<typeof appSchema> {}

const database = new MinnowDatabase(new MemoryBlockStore());
await database.migrate(appSchema);
const db = createMinnow<DB>(database, { schema: appSchema });

const rows = await db.selectFrom("orders").select(["order_id", "total"]).execute();
```

This package is optional. SQL remains Minnow's full language; the client adds a friendly typed
builder plus parameter-safe `db.query(...)` and `db.execute(...)` entry points. Builders can also
expose their parameterized form with `toSQL()`. `db.transaction(...)` groups typed builders and SQL
into one atomic write, and cross-table search uses the live catalog when no schema option is
present.

Full documentation: [minnowdb.com/docs/client](https://minnowdb.com/docs/client/).

Every `@minnowdb` package shares a major version and moves independently inside it. Install the
engine version required by the client's package range; npm chooses a compatible version and
refuses a mixed-major install. See
[Versioning](https://minnowdb.com/docs/reference/versioning/).

## License

MIT
