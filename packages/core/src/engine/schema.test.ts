import { IDBFactory } from "fake-indexeddb";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  TableRecordConflictError,
  type BlockStore,
} from "../storage/index.js";
import { describe, expect, expectTypeOf, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import {
  column,
  planMigration,
  schema,
  table,
  typedTable,
  type InferInsertRow,
  type InferRow,
  type InferUpdateChanges,
} from "./schema.js";

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  joined: column.datetime().nullable(),
});

const orders = table("orders", {
  order_id: column.number().unique(),
  person: column.string().references("people", "name"),
  total: column.number(),
});

function implementations(): Array<{ name: string; create: () => Promise<BlockStore> }> {
  return [
    { name: "memory", create: async () => new MemoryBlockStore() },
    {
      name: "indexeddb",
      create: async () =>
        IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
    },
  ];
}

describe("schema DSL", () => {
  it("infers select, insert, and update types at compile time", () => {
    expectTypeOf<InferRow<typeof people>>().toEqualTypeOf<{
      name: string;
      score: number;
      joined: Date | null;
    }>();
    expectTypeOf<InferInsertRow<typeof people>>().toEqualTypeOf<
      { name: string; score: number } & { joined?: Date | null }
    >();
    expectTypeOf<InferUpdateChanges<typeof people>>().toEqualTypeOf<{
      score?: number;
      joined?: Date | null;
    }>();
  });

  it("rejects invalid table and schema definitions explicitly", () => {
    expect(() =>
      table("bad", { a: column.number().unique(), b: column.string().unique() }),
    ).toThrow("may name at most one unique column");
    expect(() => table("bad", { a: column.number().nullable().unique() })).toThrow(
      "unique column must not be nullable",
    );
    expect(() => schema([people, people])).toThrow("Duplicate table in schema: people");
    expect(() => schema([table("a", { x: column.number().references("missing", "y") })])).toThrow(
      "Relation target does not exist: a.x -> missing.y",
    );
  });

  it("validates rows through the Standard Schema interface", () => {
    const valid = people["~standard"].validate({ name: "Ada", score: 1, joined: null });
    expect("value" in valid).toBe(true);
    const invalid = people["~standard"].validate({ score: "high", extra: 1 });
    if ("issues" in invalid) {
      expect(invalid.issues.map(({ path }) => path[0]).sort()).toEqual(["extra", "name", "score"]);
    } else {
      expect.unreachable("expected issues");
    }
  });
});

for (const implementation of implementations()) {
  it(`${implementation.name} migrates, evolves, and reads typed rows`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });

    const first = await database.migrate(schema([people, orders]));
    expect(first.createdTables.sort()).toEqual(["orders", "people"]);
    const again = await database.migrate(schema([people, orders]));
    expect(again.steps).toEqual([]);

    const handle = typedTable(database, people);
    await handle.insert([
      { name: "Ada", score: 10, joined: new Date("2026-01-01T00:00:00.000Z") },
      { name: "Grace", score: 20 },
    ]);
    const rows = await handle.rows();
    expectTypeOf(rows).toEqualTypeOf<Array<{ name: string; score: number; joined: Date | null }>>();
    expect(rows.find((row) => row.name === "Grace")?.joined).toBeNull();

    // Evolve: add a nullable column, rename one, widen nullability.
    const evolved = table("people", {
      name: column.string().unique(),
      points: column.number().renamedFrom("score"),
      joined: column.datetime().nullable(),
      city: column.string().nullable(),
    });
    const evolution = await database.migrate(schema([evolved, orders]));
    expect(evolution.alteredTables).toEqual(["people"]);
    expect(evolution.steps.map(({ kind }) => kind).sort()).toEqual(["add-column", "rename-column"]);
    expect((await database.migrate(schema([evolved, orders]))).steps).toEqual([]);

    const evolvedHandle = typedTable(database, evolved);
    const evolvedRows = await evolvedHandle.rows();
    expect(evolvedRows.find((row) => row.name === "Ada")).toMatchObject({
      points: 10,
      city: null,
    });
    await evolvedHandle.insert([{ name: "Katherine", points: 30, city: "DC" }]);
    const sql = await database.query(
      "SELECT name, points, city FROM people WHERE city = 'DC' ORDER BY name",
    );
    expect(sql.rows).toEqual([{ name: "Katherine", points: 30, city: "DC" }]);
    const nullFilled = await database.query(
      "SELECT COUNT(city) AS present, COUNT(*) AS total FROM people",
    );
    expect(nullFilled.rows).toEqual([{ present: 1, total: 3 }]);

    // Upserts written before the column existed replay as NULL for it afterwards.
    await evolvedHandle.upsert([{ name: "Grace", points: 21 }]);
    const graceRows = await evolvedHandle.rows();
    expect(graceRows.find((row) => row.name === "Grace")).toMatchObject({
      points: 21,
      city: null,
    });

    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} completes an interrupted migration idempotently`, async () => {
    const store = await implementation.create();
    let failuresRemaining = 0;
    const failing = new Proxy(store, {
      get(target, property, receiver) {
        if (property === "updateTable" && failuresRemaining > 0) {
          return () => {
            failuresRemaining -= 1;
            return Promise.reject(new Error("Injected catalog fault"));
          };
        }
        const value = Reflect.get(target, property, receiver) as unknown;
        if (typeof value === "function") {
          return (value as (...callArguments: unknown[]) => unknown).bind(target);
        }
        return value;
      },
    });
    const database = new MinnowDatabase(failing, { rowsPerBlock: 8, compression: "raw" });
    await database.migrate(schema([people]));

    const evolved = table("people", {
      name: column.string().unique(),
      points: column.number().renamedFrom("score"),
      joined: column.datetime().nullable(),
      city: column.string().nullable(),
    });
    failuresRemaining = 1;
    await expect(database.migrate(schema([evolved]))).rejects.toThrow("Injected catalog fault");

    const resumed = await database.migrate(schema([evolved]));
    expect(resumed.steps.length).toBeGreaterThan(0);
    expect((await database.migrate(schema([evolved]))).steps).toEqual([]);
    const record = await store.getTableByName("people");
    expect(record?.columns.map(({ name }) => name).sort()).toEqual([
      "city",
      "joined",
      "name",
      "points",
    ]);
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} migrates hundreds of tables with one write per change`, async () => {
    const store = await implementation.create();
    const counters = { listTables: 0, getTableByName: 0, updateTable: 0, addTable: 0 };
    const counting = new Proxy(store, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver) as unknown;
        if (typeof value !== "function") return value;
        const method = value as (...callArguments: unknown[]) => unknown;
        if (property in counters) {
          return (...callArguments: unknown[]) => {
            counters[property as keyof typeof counters] += 1;
            return method.apply(target, callArguments);
          };
        }
        return method.bind(target);
      },
    });
    const database = new MinnowDatabase(counting, { rowsPerBlock: 8, compression: "raw" });
    const tableCount = 300;
    const wide = (version: number) =>
      schema(
        Array.from({ length: tableCount }, (_, index) =>
          table(`wide_${String(index)}`, {
            id: column.number().unique(),
            label: column.string(),
            amount: column.number().nullable(),
            flag: column.boolean().nullable(),
            created: column.datetime().nullable(),
            ...(version >= 2 ? { note: column.string().nullable() } : {}),
          }),
        ),
      );

    const started = performance.now();
    const first = await database.migrate(wide(1));
    const createMs = performance.now() - started;
    expect(first.createdTables).toHaveLength(tableCount);
    expect(counters.addTable).toBe(tableCount);
    expect(counters.getTableByName).toBe(0);
    expect(counters.updateTable).toBe(0);

    Object.assign(counters, { listTables: 0, getTableByName: 0, updateTable: 0, addTable: 0 });
    const noopStarted = performance.now();
    const noop = await database.migrate(wide(1));
    const noopMs = performance.now() - noopStarted;
    expect(noop.steps).toEqual([]);
    expect(counters).toEqual({ listTables: 1, getTableByName: 0, updateTable: 0, addTable: 0 });

    Object.assign(counters, { listTables: 0, getTableByName: 0, updateTable: 0, addTable: 0 });
    const evolveStarted = performance.now();
    const evolved = await database.migrate(wide(2));
    const evolveMs = performance.now() - evolveStarted;
    expect(evolved.alteredTables).toHaveLength(tableCount);
    // Exactly one catalog write per altered table, no per-step lookups.
    expect(counters).toEqual({
      listTables: 1,
      getTableByName: 0,
      updateTable: tableCount,
      addTable: 0,
    });
    // Guard against quadratic planning: a full pass over 300 tables stays well under a second
    // even in CI; the operation counts above are the real contract.
    expect(noopMs).toBeLessThan(1_000);
    expect(createMs + evolveMs).toBeLessThan(15_000);
    store.close();
  }, 30_000);
}

describe("migration planning rejections", () => {
  it("rejects unsupported evolution explicitly", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([people]));
    const current = await store.listTables();

    expect(() =>
      planMigration(
        current,
        schema([
          table("people", {
            name: column.string().unique(),
            score: column.string(),
            joined: column.datetime().nullable(),
          }),
        ]),
      ),
    ).toThrow("Column types cannot change");
    expect(() =>
      planMigration(
        current,
        schema([table("people", { name: column.string().unique(), score: column.number() })]),
      ),
    ).toThrow("Dropping columns is not supported");
    expect(() =>
      planMigration(
        current,
        schema([
          table("people", {
            name: column.string().unique(),
            score: column.number(),
            joined: column.datetime().nullable(),
            level: column.number(),
          }),
        ]),
      ),
    ).toThrow("Added columns must be nullable");
    expect(() =>
      planMigration(
        current,
        schema([
          table("people", {
            name: column.string(),
            score: column.number().unique(),
            joined: column.datetime().nullable(),
          }),
        ]),
      ),
    ).toThrow("Unique keys cannot change");
    expect(() =>
      planMigration(
        current,
        schema([
          table("people", {
            name: column.string().unique(),
            score: column.number(),
            joined: column.datetime(),
          }),
        ]),
      ),
    ).toThrow("Nullable columns cannot tighten");
    store.close();
  });

  it("surfaces concurrent catalog changes as conflicts", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([people]));
    const record = await store.getTableByName("people");
    if (record === undefined) throw new Error("missing record");
    await store.updateTable(record.id, record.revision ?? 0, { columns: record.columns });
    await expect(
      store.updateTable(record.id, record.revision ?? 0, { columns: record.columns }),
    ).rejects.toThrow(TableRecordConflictError);
    store.close();
  });
});
