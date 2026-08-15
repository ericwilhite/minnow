import { MemoryBlockStore } from "../storage/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabaseClient, type ClientTransport } from "./client.js";
import { MinnowDatabase } from "./database.js";
import { SqlCompileError, UniqueConstraintError } from "./errors.js";
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

function connect(): MinnowDatabaseClient {
  const { clientSide, workerSide } = createBoundary();
  attachDatabaseWorker(workerSide);
  return new MinnowDatabaseClient(clientSide, { store: { kind: "memory" } });
}

async function createPeopleTable(client: MinnowDatabaseClient): Promise<void> {
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

describe("MinnowDatabaseClient", () => {
  it("initializes, writes, and queries through the boundary", async () => {
    const client = connect();
    await client.ready();
    await createPeopleTable(client);
    // Rows here, columns in the typed-query test below: both forms cross the worker boundary.
    const inserted = await client.insertBatch("people", [
      { id: 1, name: "Ada", joined: new Date("2024-01-02T03:04:05Z") },
      { id: 2, name: "Grace", joined: new Date("2024-06-07T08:09:10Z") },
    ]);
    expect(inserted.rowCount).toBe(2);
    const result = await client.query("SELECT name, joined FROM people ORDER BY name");
    expect(result.rows.map((row) => row.name)).toEqual(["Ada", "Grace"]);
    expect(result.rows[0]?.joined).toBeInstanceOf(Date);
    expect((result.rows[0]?.joined as Date).toISOString()).toBe("2024-01-02T03:04:05.000Z");
    const tables = await client.listTables();
    expect(tables.map(({ name }) => name)).toEqual(["people"]);
    await client.close();
  });

  it("binds statement parameters across the boundary", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.execute("INSERT INTO people (id, name, joined) VALUES (?, ?, ?)", [
      1,
      "Ada",
      new Date("2024-01-02T03:04:05Z"),
    ]);
    const result = await client.query("SELECT name FROM people WHERE joined = $1", {
      params: [new Date("2024-01-02T03:04:05Z")],
    });
    expect(result.rows).toEqual([{ name: "Ada" }]);
    const updated = await client.execute("UPDATE people SET name = $2 WHERE id = $1", [
      1,
      "Countess",
    ]);
    expect(updated.kind).toBe("update");
    const requeried = await client.query("SELECT name FROM people WHERE id = ?", {
      params: [1],
    });
    expect(requeried.rows).toEqual([{ name: "Countess" }]);
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

  it("carries compile-error positions across the worker boundary", async () => {
    // A devtools editor compiles in the page, but a query typed against the client fails in the
    // worker; the position has to survive serialization or the squiggle lands nowhere.
    const client = connect();
    await createPeopleTable(client);
    const sql = "SELECT * FROM people WHERE name = 'unclosed";
    const failure = await client.query(sql).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(SqlCompileError);
    const typed = failure as SqlCompileError;
    expect(typed.message).toBe("Unterminated string literal");
    expect(sql.slice(typed.offset, typed.offset + typed.length)).toBe("'unclosed");
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

  it("returns generated columns across the worker boundary", async () => {
    const client = connect();
    // Defaults declared through the schema DSL must survive serialization to the worker, and
    // the generated values (including Dates) must survive the structured clone back.
    const notes = table("notes", {
      id: column.number().unique().autoIncrement(),
      created: column.datetime().default("now"),
      body: column.string(),
    });
    await client.migrate(schema([notes]));
    const result = await client.insertBatch("notes", [{ body: "hello" }, { body: "there" }]);
    expect(result.generatedColumns?.id).toEqual([1, 2]);
    expect(result.generatedColumns?.created?.[0]).toBeInstanceOf(Date);
    const rows = await client.readTable("notes");
    expect(rows.map((row) => row.id).sort()).toEqual([1, 2]);

    // A batch whose every column is generated still carries its row count across the boundary
    // (the columnar pivot of empty rows would otherwise lose it).
    const stamps = table("stamps", {
      id: column.number().unique().autoIncrement(),
      created: column.datetime().default("now"),
    });
    await client.migrate(schema([stamps]));
    const empty = await client.insertBatch("stamps", [{}, {}]);
    expect(empty.rowCount).toBe(2);
    expect(empty.generatedColumns?.id).toEqual([1, 2]);
    await client.close();
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

  it("runs an atomic write scope across the channel", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const { result, version } = await client.write(async (tx) => {
      const staged = await tx.insertBatch("people", {
        columns: { id: [2], name: ["Grace"], joined: [new Date()] },
      });
      await tx.updateBatch("people", { keys: [1], changes: { name: ["Countess"] } });
      // Read-your-writes across the channel: the staged rows are visible in-scope only.
      expect((await tx.query("SELECT name FROM people ORDER BY name")).rows).toEqual([
        { name: "Countess" },
        { name: "Grace" },
      ]);
      expect((await client.query("SELECT COUNT(*) AS n FROM people")).rows).toEqual([{ n: 1 }]);
      return staged.rowCount;
    });
    expect(result).toBe(1);
    expect(version).not.toBeNull();
    expect((await client.query("SELECT name FROM people ORDER BY name")).rows).toEqual([
      { name: "Countess" },
      { name: "Grace" },
    ]);
    // A failing scope publishes nothing.
    await expect(
      client.write(async (tx) => {
        await tx.deleteBatch("people", { keys: [2] });
        throw new Error("abort it");
      }),
    ).rejects.toThrow("abort it");
    expect((await client.query("SELECT COUNT(*) AS n FROM people")).rows).toEqual([{ n: 2 }]);
  });

  it("pins a snapshot scope across the channel while writes continue", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const observed = await client.snapshot(async (session) => {
      const before = await session.query("SELECT COUNT(*) AS people FROM people");
      // A commit lands mid-scope; the session must keep observing the pinned version while
      // fresh queries outside the scope see the new row immediately.
      await client.insert("people", { id: 2, name: "Grace", joined: new Date() });
      const still = await session.query("SELECT COUNT(*) AS people FROM people");
      const fresh = await client.query("SELECT COUNT(*) AS people FROM people");
      return { before: before.rows, still: still.rows, fresh: fresh.rows };
    });
    expect(observed.before).toEqual([{ people: 1 }]);
    expect(observed.still).toEqual([{ people: 1 }]);
    expect(observed.fresh).toEqual([{ people: 2 }]);
    // After the scope, queries are fresh by construction.
    expect((await client.query("SELECT COUNT(*) AS people FROM people")).rows).toEqual([
      { people: 2 },
    ]);
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
    const database = new MinnowDatabase(new MemoryBlockStore());
    let disposed = false;
    exposeDatabase(database, workerSide, {
      onDispose: () => {
        disposed = true;
      },
    });
    const client = new MinnowDatabaseClient(clientSide);
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
