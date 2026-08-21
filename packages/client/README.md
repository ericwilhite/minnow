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

This package is optional. `@minnowdb/core` runs SQL on its own; the client adds a friendly, typed
way to build those same queries. It uses only the public tools in `@minnowdb/core/plan`, which are
also available for anyone building a client of their own.

Full documentation: [minnowdb.com/docs/client](https://minnowdb.com/docs/client/).

Every `@minnowdb` package shares a major version and moves independently inside it. Install the
engine version required by the client's package range; npm chooses a compatible version and
refuses a mixed-major install. See
[Versioning](https://minnowdb.com/docs/reference/versioning/).

## License

MIT
