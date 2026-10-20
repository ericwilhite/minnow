import { column, schema, table } from "@minnowdb/core";
import type { KeyChange } from "@minnowdb/core/live";
import { MinnowDatabaseClient } from "@minnowdb/core/client";

/** The complete offline-identity flow over the published worker and durable adapters. */
export async function runOfflineIdentity(kind: "indexeddb" | "opfs") {
  const name = `offline-identity-${crypto.randomUUID()}`;
  const store =
    kind === "indexeddb" ? { kind: "indexeddb" as const, name } : { kind: "opfs" as const, name };
  const definition = schema([
    table("customers", {
      id: column.integer().unique(),
      defaultOrder: column.integer().nullable().references("orders", "id"),
    }),
    table("orders", {
      id: column.integer().unique(),
      customer: column.integer().references("customers", "id"),
      note: column.string(),
    }),
    table("items", {
      id: column.integer().unique(),
      orderId: column.integer().references("orders", "id", { enforced: false }),
    }),
  ]);
  const open = () =>
    new MinnowDatabaseClient(
      new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
      { store },
    );
  const client = open();
  let reopened: MinnowDatabaseClient | undefined;
  try {
    await client.migrate(definition);
    await client.execute("INSERT INTO customers VALUES (1, NULL)");
    await client.execute("INSERT INTO orders VALUES (-1, 1, 'offline'), (900, 1, 'server')");
    await client.execute("INSERT INTO items VALUES (-7, -1)");
    const changes: KeyChange[] = [],
      order: string[] = [];
    const unsubscribe = await client.onKeyChanges((batch) => {
      changes.push(...batch);
      order.push("keys");
    });
    const live = client.liveQueries();
    await live.subscribe("SELECT * FROM orders ORDER BY id", {
      onChange: (_result, delivery) => {
        if (!delivery.initial) order.push("rows");
      },
    });
    await live.subscribe("SELECT COUNT(*) AS n FROM items", { onChange: () => undefined });
    await client.mergeKey("orders", -1, 900);
    await live.refresh();
    const mergeOrder = [...order];
    await client.execute("UPDATE items SET id = 500 WHERE id = -7");
    await live.refresh();
    const resolved = await client.resolveKeys("orders", [-1, 900, -1, 123]);
    await unsubscribe();
    await live.close();
    await client.close({ terminateWorker: true });
    reopened = open();
    const migration = await reopened.migrate(definition);
    await reopened.write(async (session) => {
      await session.mergeKey("orders", -1, 900);
    });
    return {
      changes,
      mergeOrder,
      resolved,
      remainingSteps: migration.steps.length,
      orders: (await reopened.query("SELECT * FROM orders")).rows,
      items: (await reopened.query("SELECT * FROM items")).rows,
      reopenedKeys: await reopened.resolveKeys("orders", [-1, 900]),
    };
  } finally {
    await reopened?.close({ terminateWorker: true });
    await client.close({ terminateWorker: true });
  }
}
