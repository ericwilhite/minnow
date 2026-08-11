import { describe, expect, expectTypeOf, it } from "vitest";
import { BrowserDatabaseClient, type ClientTransport } from "../client.js";
import { column, schema, table } from "../schema.js";
import { attachDatabaseWorker, type RpcScope } from "../worker-host.js";
import { BrowserDb } from "./db.js";
import { type InferDatabase } from "./types.js";

/**
 * The full Kysely-style API through the worker boundary: the same BrowserDb facade over
 * BrowserDatabaseClient, with plans and statements crossing the RPC channel by structured clone
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

async function connect(): Promise<{ db: BrowserDb<DB>; client: BrowserDatabaseClient }> {
  const { clientSide, workerSide } = createBoundary();
  attachDatabaseWorker(workerSide);
  const client = new BrowserDatabaseClient(clientSide, { store: { kind: "memory" } });
  await client.migrate(appSchema);
  return { db: new BrowserDb<DB>(client, { schema: appSchema }), client };
}

describe("BrowserDb over the worker client", () => {
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
