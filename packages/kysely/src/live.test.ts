import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { MinnowDatabase, column, schema, table } from "@minnowdb/core";
import { MinnowDatabaseClient, type ClientTransport } from "@minnowdb/core/client";
import { attachDatabaseWorker, type RpcScope } from "@minnowdb/core/worker-host";
import { CamelCasePlugin, Kysely, type KyselyPlugin, type OrderByNode } from "kysely";
import { describe, expect, expectTypeOf, it } from "vitest";
import { MinnowDialect } from "./dialect.js";
import { createKysely } from "./create-kysely.js";
import { createKyselyLiveQueries } from "./live.js";

async function until(assertion: () => void): Promise<void> {
  let failure: unknown;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      assertion();
      return;
    } catch (error) {
      failure = error;
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
  }
  throw failure;
}

function createBoundary(): { clientSide: ClientTransport; workerSide: RpcScope } {
  const clientListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  const workerListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  let chain = Promise.resolve();
  const deliver = (
    listeners: Array<(event: MessageEvent<unknown>) => void>,
    message: unknown,
  ): void => {
    const data = structuredClone(message);
    chain = chain.then(() => {
      for (const listener of listeners) listener({ data } as MessageEvent<unknown>);
    });
  };
  return {
    clientSide: {
      postMessage: (message) => deliver(workerListeners, message),
      addEventListener: (type, listener) => {
        if (type === "message") clientListeners.push(listener);
      },
    },
    workerSide: {
      postMessage: (message) => deliver(clientListeners, message),
      addEventListener: (_type, listener) => workerListeners.push(listener),
    },
  };
}

/** Adds or removes the final top-level ORDER BY without changing the rest of the Kysely query. */
function transformOrderBy(orderBy: OrderByNode | undefined): KyselyPlugin {
  return {
    transformQuery: ({ node }) => {
      if (node.kind !== "SelectQueryNode") return node;
      if (orderBy !== undefined) return Object.freeze({ ...node, orderBy });
      const { orderBy: _orderBy, ...unordered } = node;
      void _orderBy;
      return Object.freeze(unordered);
    },
    transformResult: async ({ result }) => result,
  };
}

const shopSchema = schema([
  table("orders", {
    id: column.integer().unique(),
    status: column.enum(["pending", "complete"]),
    total: column.numeric({ precision: 12, scale: 2 }),
  }),
  table("other", { id: column.integer().unique() }),
]);

describe("Kysely live queries", () => {
  it("infers rows through both the wrapper and Kysely $call", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(shopSchema);
    const db = createKysely({ driver: database, schema: shopSchema });
    const live = createKyselyLiveQueries({ driver: database });
    const builder = db
      .selectFrom("orders")
      .select(["id", "status", "total"])
      .where("status", "=", "pending")
      .orderBy("id");
    const wrapped = live(builder);
    const piped = builder.$call(live);
    expectTypeOf<typeof wrapped.$inferRow>().toEqualTypeOf<{
      id: number;
      status: "pending" | "complete";
      total: string;
    }>();
    expectTypeOf(piped).toEqualTypeOf(wrapped);

    let notifications = 0;
    const unsubscribe = wrapped.subscribe(() => {
      notifications += 1;
    });
    await until(() => expect(wrapped.getSnapshot().status).toBe("ready"));
    expect(wrapped.getSnapshot().rows).toEqual([]);

    await db.insertInto("orders").values({ id: 1, status: "pending", total: 12.5 }).execute();
    await live.refresh();
    await until(() =>
      expect(wrapped.getSnapshot().rows).toEqual([{ id: 1, status: "pending", total: "12.5" }]),
    );

    // An unrelated commit advances the version but never executes the SELECT again.
    const ready = wrapped.getSnapshot();
    await db.insertInto("other").values({ id: 1 }).execute();
    await live.refresh();
    expect(wrapped.getSnapshot()).toBe(ready);
    expect(notifications).toBe(2);

    unsubscribe();
    wrapped.close();
    piped.close();
    await live.close();
    await db.destroy();
    store.close();
  });

  it("executes through Kysely so result plugins remain part of live semantics", async () => {
    const runtimeSchema = schema([
      table("person_record", {
        id: column.integer().unique(),
        first_name: column.string(),
      }),
    ]);
    interface CamelDatabase {
      personRecord: { id: number; firstName: string };
    }
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(runtimeSchema);
    const db = new Kysely<CamelDatabase>({
      dialect: new MinnowDialect({ driver: database, schema: runtimeSchema }),
    }).withPlugin(new CamelCasePlugin());
    const live = createKyselyLiveQueries({ driver: database });
    const people = live(db.selectFrom("personRecord").select(["id", "firstName"]).orderBy("id"));
    expectTypeOf<typeof people.$inferRow>().toEqualTypeOf<{
      id: number;
      firstName: string;
    }>();
    const unsubscribe = people.subscribe(() => undefined);
    await until(() => expect(people.getSnapshot().status).toBe("ready"));

    await db.insertInto("personRecord").values({ id: 1, firstName: "Ada" }).execute();
    await live.refresh();
    await until(() => expect(people.getSnapshot().rows).toEqual([{ id: 1, firstName: "Ada" }]));

    unsubscribe();
    await live.close();
    await db.destroy();
    store.close();
  });

  it("produces typed keyed changes and bounded ordered windows", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(shopSchema);
    const db = createKysely({ driver: database, schema: shopSchema });
    const live = createKyselyLiveQueries({ driver: database });
    await db
      .insertInto("orders")
      .values([
        { id: 1, status: "pending", total: 1 },
        { id: 2, status: "pending", total: 2 },
        { id: 3, status: "pending", total: 3 },
      ])
      .execute();

    const builder = db.selectFrom("orders").select(["id", "status", "total"]).orderBy("id");
    const changes = live.changes(builder, { key: "id" });
    const windowed = live.window(builder, { key: "id", limit: 2 });
    expectTypeOf<typeof changes.$inferKey>().toEqualTypeOf<number>();
    expectTypeOf<typeof windowed.$inferRow>().toEqualTypeOf<{
      id: number;
      status: "pending" | "complete";
      total: string;
    }>();
    const unsubscribeChanges = changes.subscribe(() => undefined);
    const unsubscribeWindow = windowed.subscribe(() => undefined);
    await until(() => expect(changes.getSnapshot().status).toBe("ready"));
    await until(() => expect(windowed.getSnapshot().status).toBe("ready"));
    expect(windowed.getSnapshot().rows.map(({ id }) => id)).toEqual([1, 2]);

    await db.updateTable("orders").set({ total: 10 }).where("id", "=", 1).execute();
    await live.refresh();
    await until(() => {
      const snapshot = changes.getSnapshot();
      expect(
        snapshot.status === "ready" && snapshot.changes.some(({ type }) => type === "update"),
      ).toBe(true);
    });
    const changed = changes.getSnapshot();
    if (changed.status !== "ready") throw new Error("Expected changes to be ready");
    expect(changed.changes).toEqual([
      {
        type: "update",
        row: { id: 1, status: "pending", total: "10" },
        previous: { id: 1, status: "pending", total: "1" },
        index: 0,
      },
    ]);

    await db.insertInto("orders").values({ id: 0, status: "pending", total: 0 }).execute();
    await live.refresh();
    await until(() => expect(windowed.getSnapshot().rows[0]?.id).toBe(0));
    expect(windowed.getSnapshot().rows.map(({ id }) => id)).toEqual([0, 1]);

    expect(() =>
      live.window(db.selectFrom("orders").select(["id", "status"]), {
        key: "id",
        limit: 2,
      }),
    ).toThrow(/ORDER BY/);

    unsubscribeChanges();
    unsubscribeWindow();
    changes.close();
    windowed.close();
    await live.close();
    await db.destroy();
    store.close();
  });

  it("validates window ordering after Kysely plugins transform the final query", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(shopSchema);
    const db = createKysely({ driver: database, schema: shopSchema });
    const live = createKyselyLiveQueries({ driver: database });
    await db
      .insertInto("orders")
      .values([
        { id: 3, status: "pending", total: 3 },
        { id: 1, status: "pending", total: 1 },
        { id: 2, status: "pending", total: 2 },
      ])
      .execute();

    const orderBy = db.selectFrom("orders").select("id").orderBy("id").toOperationNode().orderBy;
    if (orderBy === undefined) throw new Error("Expected the Kysely query to carry ORDER BY");
    const pluginOrdered = db
      .selectFrom("orders")
      .select(["id", "status"])
      .withPlugin(transformOrderBy(orderBy));
    const windowed = live.window(pluginOrdered, { key: "id", limit: 2 });
    const unsubscribe = windowed.subscribe(() => undefined);
    await until(() => expect(windowed.getSnapshot().status).toBe("ready"));
    expect(windowed.getSnapshot().rows.map(({ id }) => id)).toEqual([1, 2]);

    const pluginUnordered = db
      .selectFrom("orders")
      .select(["id", "status"])
      .orderBy("id")
      .withPlugin(transformOrderBy(undefined));
    expect(() => live.window(pluginUnordered, { key: "id", limit: 2 })).toThrow(/ORDER BY/);

    unsubscribe();
    windowed.close();
    await live.close();
    await db.destroy();
    store.close();
  });

  it("preserves typed invalidation and cleanup across the worker boundary", async () => {
    const { clientSide, workerSide } = createBoundary();
    attachDatabaseWorker(workerSide);
    const client = new MinnowDatabaseClient(clientSide, { store: { kind: "memory" } });
    await client.migrate(shopSchema);
    const db = createKysely({ driver: client, schema: shopSchema });
    const live = createKyselyLiveQueries({ driver: client });
    const orders = db
      .selectFrom("orders")
      .select(["id", "status"])
      .where("status", "=", "pending")
      .$call(live);
    expectTypeOf<typeof orders.$inferRow>().toEqualTypeOf<{
      id: number;
      status: "pending" | "complete";
    }>();
    const unsubscribe = orders.subscribe(() => undefined);
    await until(() => expect(orders.getSnapshot().status).toBe("ready"));

    await db.insertInto("orders").values({ id: 1, status: "pending", total: 3 }).execute();
    await live.refresh();
    await until(() => expect(orders.getSnapshot().rows).toEqual([{ id: 1, status: "pending" }]));

    unsubscribe();
    await live.close();
    await db.destroy();
    await client.close();
  });
});
