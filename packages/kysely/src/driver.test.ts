import { MinnowDatabase, type MinnowSqlDriver } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { Kysely } from "kysely";
import { beforeEach, describe, expect, it } from "vitest";
import { MinnowDialect } from "./dialect.js";

interface PersonTable {
  id: number;
  name: string;
}

interface TestDatabase {
  person: PersonTable;
  person_source: PersonTable;
}

describe("MinnowKyselyDriver", () => {
  let database: MinnowDatabase;
  let db: Kysely<TestDatabase>;

  beforeEach(async () => {
    database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
    db = new Kysely<TestDatabase>({ dialect: new MinnowDialect({ driver: database }) });
    for (const name of ["person", "person_source"] as const) {
      await db.schema
        .createTable(name)
        .addColumn("id", "integer", (column) => column.primaryKey())
        .addColumn("name", "text", (column) => column.notNull())
        .execute();
    }
  });

  describe("connection mutex", () => {
    it("keeps a concurrent db-level query out of an open transaction", async () => {
      await db.insertInto("person").values({ id: 1, name: "before" }).execute();

      const order: string[] = [];
      let concurrentRead: Promise<Array<{ id: number; name: string }>> | undefined;
      let concurrentWrite: Promise<unknown> | undefined;
      await expect(
        db.transaction().execute(async (trx) => {
          await trx.insertInto("person").values({ id: 2, name: "inside" }).execute();
          // Issued while the transaction holds the connection, deliberately not awaited here:
          // awaiting them inside the callback would deadlock, exactly as with Kysely's other
          // single-connection dialects.
          concurrentRead = db
            .selectFrom("person")
            .selectAll()
            .orderBy("id")
            .execute()
            .then((rows) => {
              order.push("concurrent-read");
              return rows;
            });
          concurrentWrite = db
            .insertInto("person")
            .values({ id: 3, name: "concurrent" })
            .execute()
            .then(() => order.push("concurrent-write"));
          // Give the concurrent statements every chance to run inside the open transaction.
          await new Promise((resolve) => setTimeout(resolve, 25));
          order.push("transaction-end");
          throw new Error("force rollback");
        }),
      ).rejects.toThrow("force rollback");

      if (concurrentRead === undefined || concurrentWrite === undefined) {
        throw new Error("the transaction callback never issued the concurrent statements");
      }
      const rows = await concurrentRead;
      await concurrentWrite;
      // The concurrent statements waited for ROLLBACK instead of joining the transaction.
      expect(order).toEqual(["transaction-end", "concurrent-read", "concurrent-write"]);
      // The read never observed the uncommitted row.
      expect(rows.map((row) => row.name)).not.toContain("inside");
      // The concurrent write was not rolled back with the transaction.
      expect(await db.selectFrom("person").select("id").orderBy("id").execute()).toEqual([
        { id: 1 },
        { id: 3 },
      ]);
    });

    it("serializes two concurrent transactions instead of interleaving BEGIN", async () => {
      // Without serialization the second BEGIN would join or reject against the first open
      // transaction; with it, both commit.
      const first = db.transaction().execute(async (trx) => {
        await trx.insertInto("person").values({ id: 1, name: "first" }).execute();
        await new Promise((resolve) => setTimeout(resolve, 20));
        return "first";
      });
      const second = db.transaction().execute(async (trx) => {
        await trx.insertInto("person").values({ id: 2, name: "second" }).execute();
        return "second";
      });
      expect(await Promise.all([first, second])).toEqual(["first", "second"]);
      expect(await db.selectFrom("person").select("id").orderBy("id").execute()).toEqual([
        { id: 1 },
        { id: 2 },
      ]);
    });

    it("releases the connection after a fully consumed stream", async () => {
      await db
        .insertInto("person")
        .values([
          { id: 1, name: "a" },
          { id: 2, name: "b" },
          { id: 3, name: "c" },
        ])
        .execute();
      const streamed: number[] = [];
      for await (const row of db.selectFrom("person").select("id").orderBy("id").stream(2)) {
        streamed.push(row.id);
      }
      expect(streamed).toEqual([1, 2, 3]);
      expect(await db.selectFrom("person").select("id").orderBy("id").execute()).toEqual([
        { id: 1 },
        { id: 2 },
        { id: 3 },
      ]);
    });

    it("releases the connection when a stream is abandoned before it finishes", async () => {
      await db
        .insertInto("person")
        .values([
          { id: 1, name: "a" },
          { id: 2, name: "b" },
          { id: 3, name: "c" },
        ])
        .execute();
      for await (const row of db.selectFrom("person").select("id").orderBy("id").stream(1)) {
        expect(row.id).toBe(1);
        break;
      }
      // A held connection would deadlock this query.
      expect(await db.selectFrom("person").select("id").orderBy("id").executeTakeFirst()).toEqual({
        id: 1,
      });
    });

    it("releases the connection when a stream is aborted mid-flight", async () => {
      await db
        .insertInto("person")
        .values([
          { id: 1, name: "a" },
          { id: 2, name: "b" },
          { id: 3, name: "c" },
        ])
        .execute();
      const controller = new AbortController();
      await expect(
        (async () => {
          for await (const row of db
            .selectFrom("person")
            .select("id")
            .orderBy("id")
            .stream({ chunkSize: 1, signal: controller.signal })) {
            expect(row.id).toBe(1);
            controller.abort(new Error("stop streaming"));
          }
        })(),
      ).rejects.toThrow("stop streaming");
      // A held connection would deadlock this query.
      expect(await db.selectFrom("person").select("id").orderBy("id").executeTakeFirst()).toEqual({
        id: 1,
      });
    });
  });

  describe("savepoints", () => {
    it("creates, rolls back to, and releases nested savepoints", async () => {
      const trx = await db.startTransaction().execute();
      try {
        await trx.insertInto("person").values({ id: 1, name: "one" }).execute();
        const afterOne = await trx.savepoint("after_one").execute();
        await afterOne.insertInto("person").values({ id: 2, name: "two" }).execute();
        const afterTwo = await afterOne.savepoint("after_two").execute();
        await afterTwo.insertInto("person").values({ id: 3, name: "three" }).execute();
        expect(await afterTwo.selectFrom("person").select("id").orderBy("id").execute()).toEqual([
          { id: 1 },
          { id: 2 },
          { id: 3 },
        ]);

        // Rolling back to the inner savepoint removes row 3 while the transaction stays open.
        const rolledBack = await afterTwo.rollbackToSavepoint("after_two").execute();
        expect(await rolledBack.selectFrom("person").select("id").orderBy("id").execute()).toEqual([
          { id: 1 },
          { id: 2 },
        ]);

        // Rolling back to the outer savepoint removes row 2 as well.
        const outer = await rolledBack.rollbackToSavepoint("after_one").execute();
        expect(await outer.selectFrom("person").select("id").orderBy("id").execute()).toEqual([
          { id: 1 },
        ]);

        await outer.insertInto("person").values({ id: 4, name: "four" }).execute();
        await outer.releaseSavepoint("after_one").execute();
        await trx.commit().execute();
      } catch (error) {
        await trx.rollback().execute();
        throw error;
      }
      expect(await db.selectFrom("person").select("id").orderBy("id").execute()).toEqual([
        { id: 1 },
        { id: 4 },
      ]);
    });

    it("discards work after a released savepoint when the transaction rolls back", async () => {
      await db.insertInto("person").values({ id: 1, name: "committed" }).execute();
      const trx = await db.startTransaction().execute();
      await trx.insertInto("person").values({ id: 2, name: "staged" }).execute();
      const savepoint = await trx.savepoint("staged").execute();
      await savepoint.insertInto("person").values({ id: 3, name: "nested" }).execute();
      await savepoint.releaseSavepoint("staged").execute();
      await trx.rollback().execute();
      expect(await db.selectFrom("person").select("id").execute()).toEqual([{ id: 1 }]);
    });
  });

  describe("MERGE", () => {
    const merge = (kysely: Kysely<TestDatabase>) =>
      kysely
        .mergeInto("person")
        .using("person_source", "person_source.id", "person.id")
        .whenMatched()
        .thenUpdateSet((eb) => ({ name: eb.ref("person_source.name") }))
        .whenNotMatched()
        .thenInsertValues((eb) => ({
          id: eb.ref("person_source.id"),
          name: eb.ref("person_source.name"),
        }));

    it("rejects RETURNING on MERGE loudly instead of returning no rows", async () => {
      const withReturning = merge(db).returning("person.id");
      expect(() => withReturning.compile()).toThrow(
        "Minnow does not support RETURNING on MERGE statements",
      );
      await expect(withReturning.execute()).rejects.toThrow(
        "Minnow does not support RETURNING on MERGE statements",
      );
      const withReturningAll = merge(db).returningAll("person");
      expect(() => withReturningAll.compile()).toThrow(
        "Minnow does not support RETURNING on MERGE statements",
      );
    });

    it("still executes MERGE without RETURNING and reports affected rows", async () => {
      await db.insertInto("person").values({ id: 1, name: "old" }).execute();
      await db
        .insertInto("person_source")
        .values([
          { id: 1, name: "updated" },
          { id: 2, name: "inserted" },
        ])
        .execute();
      const result = await merge(db).executeTakeFirstOrThrow();
      expect(result.numChangedRows).toBe(2n);
      expect(await db.selectFrom("person").selectAll().orderBy("id").execute()).toEqual([
        { id: 1, name: "updated" },
        { id: 2, name: "inserted" },
      ]);
    });
  });
});

describe("buffered query cancellation", () => {
  class AbortingStore extends MemoryBlockStore {
    blockReads = 0;
    abortAtRead = Number.POSITIVE_INFINITY;
    readonly controller = new AbortController();

    override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
      this.blockReads += ids.length;
      if (this.blockReads >= this.abortAtRead) {
        this.controller.abort(new Error("stop buffered query"));
      }
      return super.getBlocks(ids);
    }
  }

  interface EventsDatabase {
    events: { id: number; label: string };
  }

  async function populatedStore(): Promise<AbortingStore> {
    const store = new AbortingStore();
    const writer = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 32 });
    await writer.createTable({
      name: "events",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number", integer: true },
        { name: "label", type: "string" },
      ],
    });
    await writer.insertBatch("events", {
      columns: {
        id: Array.from({ length: 5_000 }, (_, index) => index),
        label: Array.from({ length: 5_000 }, (_, index) => `event-${String(index)}`),
      },
    });
    await writer.close();
    return store;
  }

  it("forwards the AbortSignal to a buffered execute and the engine stops work", async () => {
    const store = await populatedStore();

    // A cold database over the same store measures what a complete buffered scan reads.
    store.blockReads = 0;
    const complete = new MinnowDatabase(store);
    const completeDb = new Kysely<EventsDatabase>({
      dialect: new MinnowDialect({ driver: complete }),
    });
    expect(await completeDb.selectFrom("events").selectAll().execute()).toHaveLength(5_000);
    const fullReads = store.blockReads;
    await completeDb.destroy();
    await complete.close();

    // The engine's own promise is captured so the rejection is provably the engine stopping,
    // not just Kysely racing ahead of a query that keeps running in the background.
    store.blockReads = 0;
    store.abortAtRead = 1;
    const database = new MinnowDatabase(store);
    let enginePromise: ReturnType<MinnowDatabase["execute"]> | undefined;
    const spyingDriver: MinnowSqlDriver = {
      query: database.query.bind(database),
      queryCursor: database.queryCursor.bind(database),
      introspect: database.introspect.bind(database),
      execute: (sql, params, options) => {
        const pending = database.execute(sql, params, options);
        enginePromise = pending;
        return pending;
      },
    };
    const db = new Kysely<EventsDatabase>({
      dialect: new MinnowDialect({ driver: spyingDriver }),
    });
    await expect(
      db.selectFrom("events").selectAll().execute({ signal: store.controller.signal }),
    ).rejects.toThrow("stop buffered query");
    if (enginePromise === undefined) throw new Error("the driver never received the query");
    await expect(enginePromise).rejects.toThrow("stop buffered query");
    const readsAtRejection = store.blockReads;
    expect(readsAtRejection).toBeGreaterThan(0);
    expect(readsAtRejection).toBeLessThan(fullReads);
    await new Promise((resolve) => setTimeout(resolve, 10));
    // The engine stopped reading instead of finishing the scan in the background.
    expect(store.blockReads).toBe(readsAtRejection);

    // The database remains usable after the cancelled statement.
    store.abortAtRead = Number.POSITIVE_INFINITY;
    expect(
      await db.selectFrom("events").select("id").where("id", "=", 7).executeTakeFirstOrThrow(),
    ).toEqual({ id: 7 });
    await db.destroy();
    await database.close();
  });

  it("rejects an already-aborted buffered execute without touching storage", async () => {
    const store = await populatedStore();
    const database = new MinnowDatabase(store);
    const db = new Kysely<EventsDatabase>({ dialect: new MinnowDialect({ driver: database }) });
    const controller = new AbortController();
    const reason = new Error("aborted before execution");
    controller.abort(reason);
    store.blockReads = 0;
    await expect(
      db.selectFrom("events").selectAll().execute({ signal: controller.signal }),
    ).rejects.toThrow("aborted before execution");
    expect(store.blockReads).toBe(0);
    await db.destroy();
    await database.close();
  });
});
