import { describe, expect, expectTypeOf, it } from "vitest";
import { MinnowDatabaseClient, type ClientTransport } from "@minnowdb/core";
import { column, schema, table } from "@minnowdb/core";
import { attachDatabaseWorker, type RpcScope } from "@minnowdb/core";
import { Minnow } from "./db.js";
import { type InferDatabase } from "./types.js";

/**
 * The full Kysely-style API through the worker boundary: the same Minnow facade over
 * MinnowDatabaseClient, with plans and statements crossing the RPC channel by structured clone
 * and live-query events routed back as rpc-event frames.
 */

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  city: column.string().nullable(),
});
const appSchema = schema([people]);
type DB = InferDatabase<typeof appSchema>;

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
      postMessage: (message) => {
        deliver(workerListeners, message);
      },
      addEventListener: (type: string, listener: (event: MessageEvent<unknown>) => void) => {
        if (type === "message") clientListeners.push(listener);
      },
    },
    workerSide: {
      postMessage: (message) => {
        deliver(clientListeners, message);
      },
      addEventListener: (_type, listener) => {
        workerListeners.push(listener);
      },
    },
  };
}

async function connect(): Promise<{ db: Minnow<DB>; client: MinnowDatabaseClient }> {
  const { clientSide, workerSide } = createBoundary();
  attachDatabaseWorker(workerSide);
  const client = new MinnowDatabaseClient(clientSide, { store: { kind: "memory" } });
  await client.migrate(appSchema);
  return { db: new Minnow<DB>(client, { schema: appSchema }), client };
}

describe("Minnow over the worker client", () => {
  it("runs selects, mutations, and reads through the channel", async () => {
    const { db, client } = await connect();
    await db
      .insertInto("people")
      .values([
        { name: "Ada", score: 10, city: "London" },
        { name: "Grace", score: 20, city: "DC" },
        { name: "Katherine", score: 30 },
      ])
      .execute();

    const rows = await db
      .selectFrom("people as p")
      .where("p.score", ">", 15)
      .select(["p.name"])
      .select((eb) => [eb("p.score", "*", 2).as("doubled")])
      .orderBy("doubled", "desc")
      .execute();
    expectTypeOf(rows).toEqualTypeOf<Array<{ name: string; doubled: number }>>();
    expect(rows).toEqual([
      { name: "Katherine", doubled: 60 },
      { name: "Grace", doubled: 40 },
    ]);

    const updated = await db
      .updateTable("people")
      .set((eb) => ({ score: eb("score", "+", 100) }))
      .where("city", "is", null)
      .executeTakeFirstOrThrow();
    expect(updated.numUpdatedRows).toBe(1);

    const deleted = await db.deleteFrom("people").where("score", "<", 15).executeTakeFirstOrThrow();
    expect(deleted.numDeletedRows).toBe(1);

    const remaining = await client.query("SELECT name, score FROM people ORDER BY score");
    expect(remaining.rows).toEqual([
      { name: "Grace", score: 20 },
      { name: "Katherine", score: 130 },
    ]);
    await db.close();
    await client.close();
  });

  it("carries returning rows across the channel", async () => {
    const { db, client } = await connect();
    const inserted = await db
      .insertInto("people")
      .values([{ name: "Ada", score: 10 }])
      .returningAll()
      .executeTakeFirstOrThrow();
    expect(inserted).toEqual({ name: "Ada", score: 10, city: null });

    const updated = await db
      .updateTable("people")
      .set((eb) => ({ score: eb("score", "+", 1) }))
      .where("name", "=", "Ada")
      .returning(["name", "score"])
      .execute();
    expect(updated).toEqual([{ name: "Ada", score: 11 }]);

    const deleted = await db
      .deleteFrom("people")
      .where("name", "=", "Ada")
      .returning(["score", "city"])
      .executeTakeFirstOrThrow();
    expect(deleted).toEqual({ score: 11, city: null });
    await db.close();
    await client.close();
  });

  it("runs typed transactions across the worker boundary", async () => {
    const { db, client } = await connect();
    const row = await db.transaction(async (tx) => {
      await tx.insertInto("people").values({ name: "Ada", score: 10 }).execute();
      await tx.updateTable("people").set({ score: 11 }).where("name", "=", "Ada").execute();
      return tx
        .selectFrom("people")
        .where("name", "=", "Ada")
        .select(["name", "score"])
        .executeTakeFirstOrThrow();
    });
    expect(row).toEqual({ name: "Ada", score: 11 });
    await db.close();
    await client.close();
  });

  it("rolls back a partial conflict update across the worker boundary", async () => {
    const { db, client } = await connect();
    await db.insertInto("people").values({ name: "Ada", score: 10, city: "London" }).execute();

    await expect(
      db.transaction(async (tx) => {
        await tx.execute(
          "INSERT INTO people (name, score, city) VALUES (?, ?, ?) " +
            "ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score",
          ["Ada", 99, "ignored"],
        );
        throw new Error("roll back");
      }),
    ).rejects.toThrow("roll back");

    await expect(
      db.selectFrom("people").where("name", "=", "Ada").selectAll().executeTakeFirstOrThrow(),
    ).resolves.toEqual({ name: "Ada", score: 10, city: "London" });
    await db.close();
    await client.close();
  });

  it("lets INSERT SELECT read rows staged earlier across the worker boundary", async () => {
    const { db, client } = await connect();

    await db.transaction(async (tx) => {
      await tx.insertInto("people").values({ name: "Ada", score: 10, city: "London" }).execute();
      await tx.execute(
        "INSERT INTO people (name, score, city) " +
          "SELECT ?, score, city FROM people WHERE name = ?",
        ["Grace", "Ada"],
      );
    });

    await expect(db.selectFrom("people").selectAll().orderBy("name").execute()).resolves.toEqual([
      { name: "Ada", score: 10, city: "London" },
      { name: "Grace", score: 10, city: "London" },
    ]);
    await db.close();
    await client.close();
  });

  it("ends a live iterator when the facade closes, across the channel", async () => {
    const { db, client } = await connect();
    await db.insertInto("people").values({ name: "Ada", score: 10 }).execute();
    const iterator = db.selectFrom("people").select(["name"]).live()[Symbol.asyncIterator]();
    expect((await iterator.next()).done).toBe(false);
    const parked = iterator.next();
    await db.close();
    const result = await Promise.race([
      parked,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("hung")), 500)),
    ]);
    expect(result.done).toBe(true);
    await client.close();
  });

  it("delivers live query updates across the channel from plan-based subscriptions", async () => {
    const { db, client } = await connect();
    await db.insertInto("people").values({ name: "Ada", score: 10, city: "London" }).execute();

    const results: Array<Array<{ name: string }>> = [];
    let notify = (): void => undefined;
    const next = (): Promise<void> =>
      new Promise((resolve) => {
        notify = resolve;
      });
    const initial = next();
    const subscription = await db
      .selectFrom("people")
      .select(["name"])
      .where("score", ">", 5)
      .orderBy("name")
      .live()
      .subscribe({
        onChange: (rows) => {
          results.push(rows);
          notify();
        },
      });
    await initial;
    expect(results).toEqual([[{ name: "Ada" }]]);

    const changed = next();
    await db.insertInto("people").values({ name: "Grace", score: 20 }).execute();
    // The worker's live set hears local commits directly; the client write lands in the same
    // worker-side database, so the commit hint fires without any cross-tab channel.
    await changed;
    expect(results[1]).toEqual([{ name: "Ada" }, { name: "Grace" }]);

    await subscription.close();
    await db.close();
    await client.close();
  });
});
