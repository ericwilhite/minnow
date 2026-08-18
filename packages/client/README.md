# @minnowdb/client

The typed query client for [Minnow](https://minnowdb.dev): a schema-aware builder over
`@minnowdb/core`, with inferred row types, typed mutations, and live queries.

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

This package is optional. SQL is Minnow's contract — `@minnowdb/core` runs every statement on its
own, and this client is one consumer of the plan primitives published at `@minnowdb/core/plan`.
It ships separately so it can move at its own pace, and because building it only from those
published primitives is what proves they are complete enough for anyone else to build on.

Full documentation lives at [minnowdb.dev/docs/client](https://minnowdb.dev/docs/client/).
