import { IDBFactory } from "fake-indexeddb";
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { faultyStore } from "./client-audit-harness.js";
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { column, schema, table } from "./schema.js";

const cyclic = (enforced: boolean) =>
  schema([
    table("customers", {
      id: column.integer().unique(),
      defaultOrderId: column.integer().nullable().references("orders", "id", { enforced }),
    }),
    table("orders", {
      id: column.integer().unique(),
      customerId: column.integer().references("customers", "id", { enforced }),
    }),
  ]);

describe("cyclic declarative migrations", () => {
  it("persists cyclic references across an IndexedDB close and reopen", async () => {
    const indexedDB = new IDBFactory(),
      name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ indexedDB, name });
    const db = new MinnowDatabase(store);
    await db.migrate(cyclic(true));
    await db.execute("INSERT INTO customers VALUES (1, NULL)");
    await db.execute("INSERT INTO orders VALUES (2, 1)");
    await db.close();
    store.close();
    const nextStore = await IndexedDbBlockStore.open({ indexedDB, name });
    const next = new MinnowDatabase(nextStore);
    expect((await next.migrate(cyclic(true))).steps).toEqual([]);
    await next.execute("UPDATE customers SET id = 10 WHERE id = 1");
    expect((await next.query("SELECT customerId FROM orders")).rows).toEqual([{ customerId: 10 }]);
    await expect(next.execute("INSERT INTO orders VALUES (3, 99)")).rejects.toThrow(/FOREIGN KEY/);
    await next.close();
    nextStore.close();
  });

  it("refuses a new enforced reference until existing rows satisfy it", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await db.migrate(
      schema([
        table("p", { id: column.integer().unique() }),
        table("c", { id: column.integer().unique(), parent: column.integer() }),
      ]),
    );
    await db.execute("INSERT INTO c VALUES (1, 99)");
    const target = schema([
      table("p", { id: column.integer().unique() }),
      table("c", { id: column.integer().unique(), parent: column.integer().references("p", "id") }),
    ]);
    await expect(db.migrate(target)).rejects.toThrow(/FOREIGN KEY/);
    expect((await db.introspect()).tables.find((t) => t.name === "c")?.foreignKeys).toEqual([]);
    expect((await db.query("SELECT * FROM c")).rows).toEqual([{ id: 1, parent: 99 }]);
    await db.execute("INSERT INTO p VALUES (99)");
    await db.migrate(target);
    await expect(db.execute("INSERT INTO c VALUES (2, 100)")).rejects.toThrow(/FOREIGN KEY/);
    await db.close();
  });

  it("validates the backfill when adding a referencing column", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    const p = table("p", { id: column.integer().unique() });
    await db.migrate(schema([p, table("c", { id: column.integer().unique() })]));
    await db.execute("INSERT INTO c VALUES (1)");
    const target = schema([
      p,
      table("c", {
        id: column.integer().unique(),
        parent: column.integer().backfill(99).references("p", "id"),
      }),
    ]);
    await expect(db.migrate(target)).rejects.toThrow(/FOREIGN KEY/);
    await db.execute("INSERT INTO p VALUES (99)");
    await db.migrate(target);
    expect((await db.query("SELECT parent FROM c")).rows).toEqual([{ parent: 99 }]);
    await db.close();
  });

  it.each([true, false])("creates and reopens a cycle (enforced: %s)", async (enforced) => {
    const store = new MemoryBlockStore();
    const db = new MinnowDatabase(store);
    await db.migrate(cyclic(enforced));
    await db.execute("INSERT INTO customers VALUES (1, NULL)");
    await db.execute("INSERT INTO orders VALUES (2, 1)");
    await db.execute("UPDATE customers SET defaultOrderId = 2 WHERE id = 1");
    await db.execute("UPDATE orders SET id = 3 WHERE id = 2");
    const reopened = new MinnowDatabase(store);
    expect((await reopened.migrate(cyclic(enforced))).steps).toEqual([]);
    expect((await reopened.query("SELECT * FROM customers")).rows).toEqual([
      { id: 1, defaultOrderId: 3 },
    ]);
    if (enforced)
      await expect(reopened.execute("INSERT INTO orders VALUES (4, 999)")).rejects.toThrow(
        /FOREIGN KEY/,
      );
    await reopened.close();
    await db.close();
  });

  it.each(["before", "after"])(
    "resumes after failure %s either table publication",
    async (timing) => {
      for (const failAt of [1, 2, 3, 4]) {
        let publications = 0;
        const { store } = faultyStore(async (method, _args, run) => {
          const fail =
            (method === "addTable" || method === "updateTable") && ++publications === failAt;
          if (fail && timing === "before") throw new Error("interrupted publication");
          const result = await run();
          if (fail && timing === "after") throw new Error("interrupted publication");
          return result;
        });
        const interrupted = new MinnowDatabase(store);
        await expect(interrupted.migrate(cyclic(true))).rejects.toThrow("interrupted publication");
        const reopened = new MinnowDatabase(store);
        await reopened.migrate(cyclic(true));
        expect((await reopened.introspect()).tables.map((t) => t.foreignKeys.length)).toEqual([
          1, 1,
        ]);
        await expect(reopened.execute("INSERT INTO orders VALUES (4, 999)")).rejects.toThrow(
          /FOREIGN KEY/,
        );
        await reopened.close();
        await interrupted.close();
      }
    },
  );

  it("serializes concurrent migrations of the same cycle", async () => {
    const store = new MemoryBlockStore();
    const a = new MinnowDatabase(store),
      b = new MinnowDatabase(store);
    await Promise.all([a.migrate(cyclic(true)), b.migrate(cyclic(true))]);
    expect((await a.migrate(cyclic(true))).steps).toEqual([]);
    await a.close();
    await b.close();
  });

  it.each([true, false])(
    "rejects invalid final references before publication (enforced: %s)",
    async (enforced) => {
      const db = new MinnowDatabase(new MemoryBlockStore());
      await expect(async () =>
        db.migrate(
          schema([
            table("valid", { id: column.integer().unique() }),
            table("invalid", {
              id: column.integer().unique(),
              parent: column.integer().references("missing", "id", { enforced }),
            }),
          ]),
        ),
      ).rejects.toThrow(/Relation target does not exist/);
      expect((await db.introspect()).tables).toEqual([]);
      await expect(async () =>
        db.migrate(
          schema([
            table("parent", { id: column.string().unique() }),
            table("invalid", {
              id: column.integer().unique(),
              parent: column.integer().references("parent", "id", { enforced }),
            }),
          ]),
        ),
      ).rejects.toThrow(/Relation types must match/);
      expect((await db.introspect()).tables).toEqual([]);
      await db.close();
    },
  );
});
