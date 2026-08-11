import { MemoryBlockStore } from "@browserdatabase/storage-idb";
import { describe, expect, it } from "vitest";
import { BrowserDatabaseClient, type ClientTransport } from "./client.js";
import { BrowserDatabase } from "./database.js";
import { UniqueConstraintError } from "./errors.js";
import { compileQuery, type QueryResult } from "./query.js";
import { column, schema, table, typedTable } from "./schema.js";
import { attachDatabaseWorker, exposeDatabase, type RpcScope } from "./worker-host.js";

/**
 * An in-process stand-in for the worker boundary: two endpoints whose messages are
 * structured-cloned and delivered asynchronously in order, exactly like postMessage.
 */
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

function connect(): BrowserDatabaseClient {
  const { clientSide, workerSide } = createBoundary();
  attachDatabaseWorker(workerSide);
  return new BrowserDatabaseClient(clientSide, { store: { kind: "memory" } });
}

async function createPeopleTable(client: BrowserDatabaseClient): Promise<void> {
  await client.createTable({
    name: "people",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "joined", type: "datetime" },
    ],
  });
}

describe("BrowserDatabaseClient", () => {
  it("initializes, writes, and queries through the boundary", async () => {
    const client = connect();
    await client.ready();
    await createPeopleTable(client);
    const inserted = await client.insertBatch("people", {
      columns: {
        id: [1, 2],
        name: ["Ada", "Grace"],
        joined: [new Date("2024-01-02T03:04:05Z"), new Date("2024-06-07T08:09:10Z")],
      },
    });
    expect(inserted.rowCount).toBe(2);
    const result = await client.query("SELECT name, joined FROM people ORDER BY name");
    expect(result.rows.map((row) => row.name)).toEqual(["Ada", "Grace"]);
    expect(result.rows[0]?.joined).toBeInstanceOf(Date);
    expect((result.rows[0]?.joined as Date).toISOString()).toBe("2024-01-02T03:04:05.000Z");
    const tables = await client.listTables();
    expect(tables.map(({ name }) => name)).toEqual(["people"]);
    await client.close();
  });

  it("issues calls without awaiting ready because the channel is ordered", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const rows = await client.readTable("people", { columns: ["name"] });
    expect(rows).toEqual([{ name: "Ada" }]);
  });

  it("rehydrates typed engine errors with their fields", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const failure = await client
      .insert("people", { id: 1, name: "Twin", joined: new Date() })
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(UniqueConstraintError);
    const typed = failure as UniqueConstraintError;
    expect(typed.name).toBe("UniqueConstraintError");
    expect(typed.tableName).toBe("people");
    expect(typed.columnName).toBe("id");
    expect(typed.value).toBe(1);
  });

  it("migrates a schema DSL definition and reports wire-format steps", async () => {
    const client = connect();
    const people = table("people", {
      id: column.number().unique(),
      name: column.string(),
      nickname: column.string().nullable(),
    });
    const first = await client.migrate(schema([people]));
    expect(first.createdTables).toEqual(["people"]);
    expect(first.steps).toEqual([
      {
        kind: "create-table",
        table: {
          name: "people",
          columns: {
            id: { type: "number", isNullable: false, isUnique: true },
            name: { type: "string", isNullable: false, isUnique: false },
            nickname: { type: "string", isNullable: true, isUnique: false },
          },
        },
      },
    ]);
    const second = await client.migrate(schema([people]));
    expect(second.steps).toEqual([]);
    const handle = typedTable(client, people);
    await handle.insert([{ id: 1, name: "Ada" }]);
    expect(await handle.rows()).toEqual([{ id: 1, name: "Ada", nickname: null }]);
  });

  it("runs compiled typed queries", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insertBatch("people", {
      columns: { id: [1, 2], name: ["Ada", "Grace"], joined: [new Date(), new Date()] },
    });
    const rows = await client.run<{ name: string }>({
      kind: "typed-query",
      plan: compileQuery("SELECT name FROM people ORDER BY name DESC"),
    });
    expect(rows).toEqual([{ name: "Grace" }, { name: "Ada" }]);
  });

  it("proxies prepared queries as worker-side handles", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const prepared = await client.prepareQuery("SELECT name FROM people");
    expect(prepared.sql).toBe("SELECT name FROM people");
    expect(prepared.tables).toEqual(["people"]);
    const first = await prepared.execute();
    const second = await prepared.execute();
    expect(first.rows).toEqual([{ name: "Ada" }]);
    expect(second.rows).toEqual(first.rows);
    const usage = await prepared.memoryUsage();
    expect(usage.peakBytes).toBeGreaterThanOrEqual(0);
    await prepared.close();
    await expect(prepared.execute()).rejects.toThrow(/Unknown handle/);
  });

  it("proxies buffered writers, including flush results and stats", async () => {
    const client = connect();
    await createPeopleTable(client);
    const writer = client.bufferedWriter("people", { maxRows: 100 });
    await writer.add({ id: 1, name: "Ada", joined: new Date() });
    await writer.add({ id: 2, name: "Grace", joined: new Date() });
    expect(await writer.stats()).toEqual(
      expect.objectContaining({ pendingRowCount: 2 }) as unknown,
    );
    const flushed = await writer.flush();
    expect(flushed?.rowCount).toBe(2);
    expect(await writer.close()).toBeUndefined();
    expect((await client.readTable("people")).length).toBe(2);
  });

  it("streams live query changes and stops after close", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const live = client.liveQueries();
    const changes: QueryResult[] = [];
    const subscription = await live.subscribe("SELECT name FROM people ORDER BY name", {
      onChange: (result) => changes.push(result),
    });
    expect(subscription.dependencyTableIds.length).toBe(1);
    expect(changes.length).toBe(1);
    expect(changes[0]?.rows).toEqual([{ name: "Ada" }]);
    await client.insert("people", { id: 2, name: "Grace", joined: new Date() });
    await live.refresh();
    expect(changes.length).toBe(2);
    expect(changes[1]?.rows).toEqual([{ name: "Ada" }, { name: "Grace" }]);
    await subscription.close();
    await client.insert("people", { id: 3, name: "Edsger", joined: new Date() });
    await live.refresh();
    expect(changes.length).toBe(2);
    const stats = await live.stats();
    expect(stats.sweeps).toBeGreaterThanOrEqual(1);
    await live.close();
  });

  it("serves a caller-constructed database through exposeDatabase", async () => {
    const { clientSide, workerSide } = createBoundary();
    const database = new BrowserDatabase(new MemoryBlockStore());
    let disposed = false;
    exposeDatabase(database, workerSide, {
      onDispose: () => {
        disposed = true;
      },
    });
    const client = new BrowserDatabaseClient(clientSide);
    await client.ready();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    expect((await client.readTable("people")).length).toBe(1);
    await client.close();
    expect(disposed).toBe(true);
    await expect(client.listTables()).rejects.toThrow(/closed/);
  });

  it("rejects calls after dispose and unknown methods cleanly", async () => {
    const client = connect();
    await client.ready();
    await expect(
      (
        client as unknown as { _invoke(h: string, m: string, a: unknown[]): Promise<unknown> }
      )._invoke("missing-handle", "close", []),
    ).rejects.toThrow(/Unknown handle/);
    await client.close();
    await expect(client.listTables()).rejects.toThrow(/closed/);
  });
});
