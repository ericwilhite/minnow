/**
 * The schema-typed batch API: a database constructed with `{ schema }` types every batch method
 * by table name. Positive cases run against the engine and the worker client; negative cases
 * live in a never-called function so `@ts-expect-error` proves each one is refused at compile time.
 */
import { describe, expect, expectTypeOf, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabaseClient, type ClientTransport } from "./client.js";
import { MinnowDatabase, type DatabaseRow, type WriteSession } from "./database.js";
import {
  column,
  schema,
  table,
  type BatchConflictWhere,
  type InferInsertRow,
  type InferRow,
  type JsonShape,
} from "./schema.js";
import { exposeDatabase, type RpcScope } from "./worker-server.js";

const orders = table("orders", {
  order_id: column.integer().unique(),
  customer: column.string(),
  total: column.numeric(),
  note: column.string().nullable(),
  placed_at: column.datetime().default(new Date("2026-01-01T00:00:00.000Z")),
  payload: column.jsonb<{ gift: boolean }>(),
});
const customers = table("customers", {
  id: column.integer().unique(),
  name: column.string(),
  tier: column.integer().default(1),
});
const retail = schema([orders, customers]);

type OrderRow = InferRow<typeof orders>;
type OrderInsert = InferInsertRow<typeof orders>;

function boundary(): { clientSide: ClientTransport; workerSide: RpcScope } {
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
      addEventListener: (type: string, listener: (event: MessageEvent<unknown>) => void) => {
        if (type === "message") clientListeners.push(listener);
      },
    },
    workerSide: {
      postMessage: (message) => deliver(clientListeners, message),
      addEventListener: (_type, listener) => {
        workerListeners.push(listener);
      },
    },
  };
}

/**
 * Never called: every line here must fail to compile. A schema-typed database refuses unknown
 * tables, missing or mistyped columns, foreign keys of the wrong type, and guards that name a
 * column the table does not have.
 */
async function rejectedAtCompileTime(
  database: MinnowDatabase<typeof retail>,
  client: MinnowDatabaseClient<typeof retail>,
): Promise<void> {
  // @ts-expect-error no such table
  await database.insertBatch("orderz", [{ order_id: 1 }]);
  // @ts-expect-error `customer` and `total` are required
  await database.insertBatch("orders", [{ order_id: 1 }]);
  // @ts-expect-error the key is an integer
  await database.insertBatch("orders", [{ order_id: "1", customer: "a", total: 1 }]);
  await database.insertBatch("orders", [
    // @ts-expect-error unknown column
    { order_id: 1, customer: "a", total: 1, bogus: true },
  ]);
  // @ts-expect-error a columnar batch is checked column by column too
  await database.insertBatch("orders", { columns: { order_id: [1], customer: ["a"], bogus: [1] } });
  // @ts-expect-error `total` cannot be omitted from a columnar batch either
  await database.insertBatch("orders", { columns: { order_id: [1], customer: ["a"] } });
  await database.upsertBatch("orders", [{ order_id: 1, customer: "a", total: 1, payload: "{}" }], {
    // @ts-expect-error the guard's column must exist
    conflictWhere: { column: "nope", operator: "=", value: 1 },
  });
  await database.upsertBatch("orders", [{ order_id: 1, customer: "a", total: 1, payload: "{}" }], {
    // @ts-expect-error the guard's value follows its column: `total` is numeric, not boolean
    conflictWhere: { column: "total", operator: "=", value: true },
  });
  await database.upsertBatch("orders", [{ order_id: 1, customer: "a", total: 1, payload: "{}" }], {
    // @ts-expect-error `customer` is not nullable, so its guard cannot compare against null
    conflictWhere: { column: "customer", operator: "=", value: null },
  });
  // @ts-expect-error the key is an integer, not a string
  await database.update("orders", "1", { note: "x" });
  // @ts-expect-error the unique key cannot be changed
  await database.update("orders", 1, { order_id: 2 });
  // @ts-expect-error a change must match its column's type
  await database.update("orders", 1, { total: true });
  // @ts-expect-error keys are typed in the batch form as well
  await database.updateBatch("orders", { keys: ["1"], changes: { note: ["x"] } });
  // @ts-expect-error the key column is not a change
  await database.updateBatch("orders", { keys: [1], changes: { order_id: [2] } });
  // @ts-expect-error delete keys are typed
  await database.delete("orders", "1");
  // @ts-expect-error and so are batch delete keys
  await database.deleteBatch("customers", { keys: ["1"] });
  // @ts-expect-error readTable narrows to declared columns
  await database.readTable("orders", { columns: ["nope"] });
  await database.write(async (tx) => {
    // @ts-expect-error the write scope is typed the same way
    await tx.insertBatch("customers", [{ id: 1 }]);
    // @ts-expect-error including its guards
    await tx.upsertBatch("customers", [{ id: 1, name: "n" }], { conflictWhere: { column: "x" } });
  });
  // @ts-expect-error a buffered writer's rows follow the table it was opened on
  await database.bufferedWriter("customers").add({ id: 1 });
  // @ts-expect-error the worker client mirrors every check
  await client.insertBatch("orders", [{ order_id: 1 }]);
  await client.upsert(
    "orders",
    { order_id: 1, customer: "a", total: 1, payload: "{}" },
    // @ts-expect-error including the guard's column
    { conflictWhere: { column: "nope", operator: "=", value: 1 } },
  );
  await client.write(async (tx) => {
    // @ts-expect-error and the scope
    await tx.deleteBatch("orders", { keys: ["1"] });
  });
  // @ts-expect-error and the buffered writer
  await client.bufferedWriter("orders").add({ order_id: 1 });
}

describe("a schema-typed database", () => {
  it("infers rows, keys, changes, guards and reads from the declaration", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { schema: retail });
    expectTypeOf(database).toEqualTypeOf<MinnowDatabase<typeof retail>>();
    await database.migrate();

    // Rows: required columns must be present; nullable and defaulted ones may be omitted.
    await database.insertBatch("orders", [
      { order_id: 1, customer: "ada", total: "9.50", payload: JSON.stringify({ gift: true }) },
      {
        order_id: 2,
        customer: "bob",
        total: 3,
        note: null,
        placed_at: new Date("2026-02-02T00:00:00.000Z"),
        payload: "{}",
      },
    ]);
    await database.insertBatch("customers", {
      columns: { id: [1, 2], name: ["ada", "bob"] },
    });
    await database.insert("customers", { id: 3, name: "cy", tier: 2 });

    // Guards: the value is typed by the column it names.
    const guard = {
      conflictWhere: { column: "customer", operator: "=", value: "bob" },
    } satisfies { conflictWhere: BatchConflictWhere<typeof orders> };
    const guarded = await database.upsertBatch(
      "orders",
      [
        {
          order_id: 2,
          customer: "bob",
          total: 4,
          placed_at: new Date("2026-02-02T00:00:00.000Z"),
          payload: "{}",
        },
      ],
      guard,
    );
    expect(guarded).toMatchObject({ updatedRowCount: 1, skippedRowCount: 0 });
    const skipped = await database.upsert(
      "orders",
      { order_id: 1, customer: "ada", total: 1, payload: "{}" },
      { conflictWhere: { column: "customer", operator: "=", value: "someone else" } },
    );
    expect(skipped).toMatchObject({ rowCount: 0, skippedRowCount: 1 });

    // Keys and changes: an explicit undefined change leaves the column alone.
    await database.update("orders", 1, { note: "gift", total: undefined });
    await database.updateBatch("customers", { keys: [1, 2], changes: { tier: [3, 3] } });
    await database.delete("customers", 3);
    await database.deleteBatch("customers", { keys: [2] });

    // Reads: the complete row shape, or the picked columns.
    const rows = await database.readTable("orders");
    expectTypeOf(rows).toEqualTypeOf<OrderRow[]>();
    expectTypeOf(rows[0]?.payload).toEqualTypeOf<JsonShape<{ gift: boolean }> | undefined>();
    expect(rows).toEqual([
      {
        order_id: 1,
        customer: "ada",
        // A bare NUMERIC renders canonically: the "9.50" that went in comes back without its
        // trailing zero, exactly as a query would show it.
        total: "9.5",
        note: "gift",
        placed_at: new Date("2026-01-01T00:00:00.000Z"),
        payload: '{"gift":true}',
      },
      {
        order_id: 2,
        customer: "bob",
        total: "4",
        note: null,
        placed_at: new Date("2026-02-02T00:00:00.000Z"),
        payload: "{}",
      },
    ]);
    const picked = await database.readTable("customers", { columns: ["id", "tier"] });
    expectTypeOf(picked).toEqualTypeOf<Array<{ id: number; tier: number }>>();
    expect(picked).toEqual([{ id: 1, tier: 3 }]);
    expectTypeOf(await database.readTable("customers", 1)).toEqualTypeOf<
      Array<InferRow<typeof customers>>
    >();

    // Write scopes and buffered writers carry the same types.
    const { result } = await database.write(async (tx) => {
      expectTypeOf(tx).toEqualTypeOf<WriteSession<typeof retail>>();
      await tx.insertBatch("customers", [{ id: 4, name: "dee" }]);
      return tx.upsertBatch("customers", [{ id: 4, name: "dee", tier: 9 }], {
        conflictWhere: { column: "tier", operator: "<", value: 5 },
      });
    });
    expect(result).toMatchObject({ rowCount: 1, skippedRowCount: 0 });
    const writer = database.bufferedWriter("customers", { maxRows: 1 });
    await writer.add({ id: 5, name: "eve" });
    await writer.close();
    expect(await database.readTable("customers", { columns: ["id"] })).toEqual([
      { id: 1 },
      { id: 4 },
      { id: 5 },
    ]);
    await database.close();
  });

  it("stays assignable to the untyped database, which still addresses tables by string", async () => {
    const typed = new MinnowDatabase(new MemoryBlockStore(), { schema: retail });
    const untyped: MinnowDatabase = typed;
    const plain = new MinnowDatabase(new MemoryBlockStore());
    expectTypeOf(plain).toEqualTypeOf<MinnowDatabase>();
    await plain.execute("CREATE TABLE ad_hoc (id INTEGER PRIMARY KEY, label TEXT)");
    await plain.insertBatch("ad_hoc", [{ id: 1, label: "x" }]);
    await plain.upsert(
      "ad_hoc",
      { id: 1, label: "y" },
      {
        conflictWhere: { column: "label", operator: "=", value: "x" },
      },
    );
    const rows = await plain.readTable("ad_hoc");
    expectTypeOf(rows).toEqualTypeOf<DatabaseRow[]>();
    expect(rows).toEqual([{ id: 1, label: "y" }]);
    expectTypeOf(await plain.readTable("ad_hoc", { columns: ["label"] })).toEqualTypeOf<
      Array<Pick<DatabaseRow, "label">>
    >();
    await expect(plain.migrate()).rejects.toThrow("migrate() needs a schema");
    await untyped.close();
    await plain.close();
  });

  it("types the worker client the same way, and migrates its declared schema", async () => {
    const { clientSide, workerSide } = boundary();
    exposeDatabase(new MinnowDatabase(new MemoryBlockStore()), workerSide);
    const client = new MinnowDatabaseClient(clientSide, { schema: retail });
    expectTypeOf(client).toEqualTypeOf<MinnowDatabaseClient<typeof retail>>();
    await client.migrate();
    await client.insertBatch("customers", [
      { id: 1, name: "ada" },
      { id: 2, name: "bob", tier: 2 },
    ]);
    await client.update("customers", 1, { tier: 5, name: undefined });
    const { result } = await client.write(async (tx) => {
      await tx.deleteBatch("customers", { keys: [2] });
      return tx.upsertBatch("customers", [{ id: 1, name: "ada!" }], {
        conflictWhere: { column: "tier", operator: ">=", value: 5 },
      });
    });
    expect(result).toMatchObject({ rowCount: 1, skippedRowCount: 0 });
    const writer = client.bufferedWriter("customers", { maxRows: 1 });
    await writer.add({ id: 3, name: "cy" });
    await writer.close();
    const rows = await client.readTable("customers");
    expectTypeOf(rows).toEqualTypeOf<Array<InferRow<typeof customers>>>();
    expect(rows).toEqual([
      { id: 1, name: "ada!", tier: 1 },
      { id: 3, name: "cy", tier: 1 },
    ]);
    const untyped: MinnowDatabaseClient = client;
    await untyped.close();
  });

  it("keeps the never-called negative cases referenced", () => {
    expect(typeof rejectedAtCompileTime).toBe("function");
    expectTypeOf<OrderInsert>().toExtend<{ order_id: number; customer: string }>();
  });
});
