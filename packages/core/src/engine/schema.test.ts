import { IDBFactory } from "fake-indexeddb";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  TableRecordConflictError,
  type BlockStore,
} from "../storage/index.js";
import { describe, expect, expectTypeOf, it, vi } from "vitest";
import { toCatalog, type Catalog } from "./catalog.js";
import { MinnowDatabase } from "./database.js";
import { allVisibleSegments } from "./storage-test-helpers.js";
import {
  column,
  declaredForeignKeys,
  isDestructiveStep,
  planMigration,
  schema,
  table,
  typedTable,
  view,
  type InferInsertRow,
  type InferRow,
  type InferUpdateChanges,
} from "./schema.js";
import { deserializeSchema, serializeMigrationSteps, serializeSchema } from "./schema-wire.js";

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  joined: column.datetime().nullable(),
});

const generatedRows = table("generated_rows", {
  id: column.integer().unique(),
  field_id: column.string(),
  ticket_number: column.integer(),
  version: column.integer(),
  offline_key: column
    .string()
    .generatedSql("field_id || ':' || CAST(ticket_number AS TEXT) || ':' || CAST(version AS TEXT)"),
});

const orders = table("orders", {
  order_id: column.number().unique(),
  person: column.string().references("people", "name"),
  total: column.number(),
});

function invalidColumnChainsAreRejectedByTypeScript(): void {
  // @ts-expect-error nullable unique keys are invalid by construction
  column.number().nullable().unique();
  // @ts-expect-error nullable columns do not take backfills
  column.string().nullable().backfill("x");
  // @ts-expect-error auto-increment is only available after a numeric unique key is declared
  column.number().autoIncrement();
  // @ts-expect-error SET NULL requires a nullable child column
  column.number().references("parents", "id", { onDelete: "set null" });
  // @ts-expect-error defaults are catalog values or SQL expressions, never JavaScript callbacks
  column.string().default(() => "generated");
}

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

  it("excludes generated columns from insert and update types", () => {
    expectTypeOf<InferRow<typeof generatedRows>>().toEqualTypeOf<{
      id: number;
      field_id: string;
      ticket_number: number;
      version: number;
      offline_key: string;
    }>();
    expectTypeOf<InferInsertRow<typeof generatedRows>>().toExtend<{
      id: number;
      field_id: string;
      ticket_number: number;
      version: number;
    }>();
    expectTypeOf<InferUpdateChanges<typeof generatedRows>>().toEqualTypeOf<{
      field_id?: string;
      ticket_number?: number;
      version?: number;
    }>();
    expect(generatedRows.columns.offline_key.isGenerated).toBe(true);
    const badInsert: InferInsertRow<typeof generatedRows> = {
      id: 1,
      field_id: "field",
      ticket_number: 2,
      version: 3,
      // @ts-expect-error callers cannot provide generated values
      offline_key: "wrong",
    };
    // @ts-expect-error callers cannot update generated values
    const badUpdate: InferUpdateChanges<typeof generatedRows> = { offline_key: "wrong" };
    expect([badInsert, badUpdate]).toHaveLength(2);
  });

  it("rejects invalid table and schema definitions explicitly", () => {
    expect(invalidColumnChainsAreRejectedByTypeScript).toBeTypeOf("function");
    expect(() =>
      table("bad", { a: column.number().unique(), b: column.string().unique() }),
    ).toThrow("may name at most one unique column");
    expect(() =>
      table("bad", {
        a: (
          column.number().nullable() as unknown as {
            unique(): ReturnType<typeof column.number>;
          }
        ).unique(),
      }),
    ).toThrow("unique column must not be nullable");
    expect(() => schema([people, people])).toThrow("Duplicate table in schema: people");
    expect(() => schema([table("a", { x: column.number().references("missing", "y") })])).toThrow(
      "Relation target does not exist: a.x -> missing.y",
    );
    expect(() => table(" ", { id: column.number() })).toThrow("Table name cannot start or end");
    expect(() => table("bad", { "": column.number() })).toThrow("Column name cannot be empty");
    expect(() =>
      schema([
        table("parents", { id: column.number(), label: column.string().unique() }),
        table("children", { parent: column.number().references("parents", "id") }),
      ]),
    ).toThrow("Relation target must be the unique key");
    expect(() =>
      schema([
        table("parents", { id: column.string().unique() }),
        table("children", { parent: column.number().references("parents", "id") }),
      ]),
    ).toThrow("Relation types must match");
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
    const invalidScalars = table("values", {
      id: column.number(),
      at: column.datetime(),
    })["~standard"].validate({ id: Number.NaN, at: new Date(Number.NaN) });
    expect(
      "issues" in invalidScalars ? invalidScalars.issues.map(({ path }) => path[0]) : [],
    ).toEqual(["id", "at"]);
  });
});

describe("SQL-domain and composite-key schema declarations", () => {
  const accounts = table(
    "accounts",
    {
      tenant_id: column.uuid(),
      account_no: column.integer(),
      balance: column.numeric({ precision: 12, scale: 2 }).default("0"),
      profile: column.jsonb().nullable(),
      state: column.sqlEnum("account_state", ["open", "closed"]).default("open"),
      tags: column.array("text").nullable(),
    },
    { primaryKey: ["tenant_id", "account_no"] },
  );

  it("retains exact read/write types and excludes every primary-key column from updates", () => {
    expectTypeOf<InferRow<typeof accounts>>().toEqualTypeOf<{
      tenant_id: string;
      account_no: number;
      balance: string;
      profile: string | null;
      state: "open" | "closed";
      tags: string | null;
    }>();
    expectTypeOf<InferInsertRow<typeof accounts>>().toEqualTypeOf<
      {
        tenant_id: string;
        account_no: number;
      } & {
        balance?: string | number;
        profile?: string | null;
        state?: "open" | "closed";
        tags?: string | null;
      }
    >();
    expectTypeOf<InferUpdateChanges<typeof accounts>>().toEqualTypeOf<{
      balance?: string | number;
      profile?: string | null;
      state?: "open" | "closed";
      tags?: string | null;
    }>();
  });

  it("rejects malformed domain metadata and values at declaration or validation time", () => {
    expect(() => column.numeric({ precision: 2, scale: 3 })).toThrow(
      "Invalid NUMERIC domain metadata",
    );
    expect(() => column.array("  ")).toThrow("ARRAY element type must be a trimmed non-empty name");
    expect(() =>
      table("bad_uuid", {
        id: column.integer().unique(),
        value: column.uuid().default("not-a-uuid"),
      }),
    ).toThrow("Invalid UUID value");
    const integers = table("integers", { id: column.integer().unique() });
    expect(integers["~standard"].validate({ id: 1.5 })).toEqual({
      issues: [{ message: "Expected safe integer", path: ["id"] }],
    });
  });

  it("round-trips domains, primary keys, and composite relations through the wire", () => {
    const entries = table(
      "entries",
      {
        tenant_id: column.uuid(),
        account_no: column.integer(),
        entry_no: column.integer(),
      },
      {
        primaryKey: ["tenant_id", "account_no", "entry_no"],
        foreignKeys: [
          {
            name: "entries_account_fkey",
            columns: ["tenant_id", "account_no"],
            references: { table: "accounts", columns: ["tenant_id", "account_no"] },
            onDelete: "cascade",
          },
        ],
      },
    );
    const restored = deserializeSchema(serializeSchema(schema([accounts, entries])));
    expect(restored.tables[0]?.primaryKey).toEqual(["tenant_id", "account_no"]);
    expect(restored.tables[0]?.columns.balance?.sqlDomain).toEqual({
      kind: "numeric",
      precision: 12,
      scale: 2,
    });
    expect(restored.tables[1]?.foreignKeys).toEqual(entries.foreignKeys);
  });

  it("creates composite keys and relations and supports typed composite mutations", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const entries = table(
      "entries",
      {
        tenant_id: column.uuid(),
        account_no: column.integer(),
        entry_no: column.integer(),
        memo: column.string(),
      },
      {
        primaryKey: ["tenant_id", "account_no", "entry_no"],
        foreignKeys: [
          {
            name: "entries_account_fkey",
            columns: ["tenant_id", "account_no"],
            references: { table: "accounts", columns: ["tenant_id", "account_no"] },
            onDelete: "cascade",
          },
        ],
      },
    );
    await database.migrate(schema([entries, accounts]));
    const tenant = "9b2c7c8f-6f56-4a78-a8e1-7936cc12bd83";
    await typedTable(database, accounts).insert([{ tenant_id: tenant, account_no: 7 }]);
    const handle = typedTable(database, entries);
    await handle.insert([{ tenant_id: tenant, account_no: 7, entry_no: 1, memo: "before" }]);
    await expect(
      handle.update({
        keys: [{ tenant_id: tenant, account_no: 7, entry_no: 1 }],
        changes: { memo: ["after"] },
      }),
    ).resolves.toMatchObject({ kind: "update", rowCount: 1 });
    expect(
      (
        await database.query("SELECT tenant_id, account_no, entry_no, memo FROM entries", {
          memoize: false,
        })
      ).rows,
    ).toEqual([{ tenant_id: tenant, account_no: 7, entry_no: 1, memo: "after" }]);
    expect(await handle.rows()).toEqual([
      { tenant_id: tenant, account_no: 7, entry_no: 1, memo: "after" },
    ]);
    await handle.delete({ keys: [{ tenant_id: tenant, account_no: 7, entry_no: 1 }] });
    expect(await handle.rows()).toEqual([]);
    store.close();
  });

  it("preserves and validates domain defaults and added-column backfills", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const before = table("prices", {
      id: column.integer().unique(),
      label: column.string(),
    });
    await database.migrate(schema([before]));
    await database.execute("INSERT INTO prices (id, label) VALUES (1, 'old')");
    const after = table("prices", {
      id: column.integer().unique(),
      label: column.string(),
      amount: column.numeric({ precision: 8, scale: 2 }).default("1.25").backfill("0"),
    });
    await database.migrate(schema([after]));
    await database.execute("INSERT INTO prices (id, label) VALUES (2, 'new')");
    expect((await database.query("SELECT id, amount FROM prices ORDER BY id")).rows).toEqual([
      { id: 1, amount: "0" },
      { id: 2, amount: "1.25" },
    ]);
    const amount = (await database.introspect()).tables[0]?.columns.find(
      ({ name }) => name === "amount",
    );
    expect(amount).toMatchObject({
      backfill: "0",
      sqlDomain: { kind: "numeric", precision: 8, scale: 2 },
    });
    expect((await database.migrate(schema([after]))).steps).toEqual([]);
    store.close();
  });

  it("keeps logical domains when SQL adds columns beside schema-managed ones", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.execute("CREATE TYPE mood AS ENUM ('calm', 'busy')");
    await database.execute("CREATE TABLE domain_additions (id INTEGER PRIMARY KEY)");
    await database.execute("ALTER TABLE domain_additions ADD COLUMN amount NUMERIC(8, 2)");
    await database.execute("ALTER TABLE domain_additions ADD COLUMN feeling mood");
    await database.execute(
      "INSERT INTO domain_additions (id, amount, feeling) VALUES (1, 12.50, 'calm')",
    );
    expect((await database.query("SELECT amount, feeling FROM domain_additions")).rows).toEqual([
      { amount: "12.5", feeling: "calm" },
    ]);
    expect(
      (await database.introspect()).tables[0]?.columns.map(({ name, sqlDomain }) => ({
        name,
        ...(sqlDomain === undefined ? {} : { sqlDomain }),
      })),
    ).toEqual([
      { name: "id" },
      { name: "amount", sqlDomain: { kind: "numeric", precision: 8, scale: 2 } },
      { name: "feeling", sqlDomain: { kind: "enum", name: "mood", values: ["calm", "busy"] } },
    ]);
    store.close();
  });

  it("rejects unsafe domain, primary-key, and frozen-backfill changes", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([accounts]));
    await expect(
      database.migrate(
        schema([
          table(
            "accounts",
            {
              ...accounts.columns,
              balance: column.numeric({ precision: 14, scale: 2 }).default("0"),
            },
            { primaryKey: ["tenant_id", "account_no"] },
          ),
        ]),
      ),
    ).rejects.toThrow("Column domains cannot change: accounts.balance");

    const before = table("flags", { id: column.integer().unique() });
    await database.migrate(schema([before]));
    const added = table("flags", {
      id: column.integer().unique(),
      state: column.string().backfill("old"),
    });
    await database.migrate(schema([added]));
    await expect(
      database.migrate(
        schema([
          table("flags", {
            id: column.integer().unique(),
            state: column.string().backfill("changed"),
          }),
        ]),
      ),
    ).rejects.toThrow("Backfills cannot change: flags.state");
    await expect(
      database.migrate(
        schema([table("flags", { id: column.integer().unique(), state: column.string() })]),
      ),
    ).rejects.toThrow("Backfills cannot be removed: flags.state");
    store.close();
  });
});

describe("typedTable SQL execution", () => {
  it("routes inserts, updates, deletes, and reads through SQL", async () => {
    const definition = table("typed_rows", {
      id: column.number().unique(),
      value: column.string(),
      note: column.string().nullable(),
    });
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.migrate(schema([definition]));
    const execute = vi.spyOn(database, "execute");
    const query = vi.spyOn(database, "query");
    const handle = typedTable(database, definition);

    await handle.insert([
      { id: 1, value: "a" },
      { id: 2, value: "b", note: "old" },
    ]);
    await handle.update({ keys: [1, 2], changes: { value: ["A", "B"], note: [undefined, "new"] } });
    await handle.delete({ keys: [1] });
    await expect(handle.rows()).resolves.toEqual([{ id: 2, value: "B", note: "new" }]);

    expect(execute.mock.calls.map(([sql]) => sql.split(" ")[0])).toEqual([
      "INSERT",
      "UPDATE",
      "DELETE",
    ]);
    expect(query).toHaveBeenCalledWith(expect.stringMatching(/^SELECT /));
  });
});

const notes = table("notes", {
  id: column.number().unique().autoIncrement(),
  slug: column.uuid().defaultSql("gen_random_uuid()"),
  status: column.string().default("draft"),
  created: column.datetime().defaultSql("CURRENT_TIMESTAMP"),
  body: column.string(),
  tags: column.string().nullable(),
});

describe("column defaults", () => {
  it("carries literal and SQL default specs on builders", () => {
    expect(notes.columns.id.defaultSpec).toEqual({ kind: "autoincrement" });
    expect(notes.columns.status.defaultSpec).toEqual({ kind: "literal", value: "draft" });
    expect(notes.columns.created.defaultSpec).toEqual({
      kind: "expression",
      sql: "CURRENT_TIMESTAMP",
    });
    expect(notes.columns.body.defaultSpec).toBeUndefined();
    expect(notes.columns.id.hasDefault).toBe(true);
    expect(notes.columns.body.hasDefault).toBe(false);
    expect(notes.columns.slug.defaultSpec).toEqual({
      kind: "expression",
      sql: "gen_random_uuid()",
    });
    expect(notes.columns.slug.hasDefault).toBe(true);
    const replaced = column.string().defaultSql("lower('generated')").default("literal");
    expect(replaced.defaultSpec).toEqual({ kind: "literal", value: "literal" });
  });

  it("rejects invalid default combinations explicitly", () => {
    expect(table("nullable_default", { a: column.string().default("x").nullable() })).toBeDefined();
    expect(
      table("nullable_sql", { a: column.string().defaultSql("lower('X')").nullable() }),
    ).toBeDefined();
    expect(() =>
      table("bad", {
        a: (
          column.number() as unknown as {
            autoIncrement(): ReturnType<typeof column.number>;
          }
        ).autoIncrement(),
        b: column.string(),
      }),
    ).toThrow("Auto-increment requires the unique key column");
    expect(
      table("constant_key", { a: column.string().unique().default("constant") }),
    ).toBeDefined();
    expect(() =>
      (column.string() as unknown as { autoIncrement: () => unknown }).autoIncrement(),
    ).toThrow("Auto-increment requires a number column");
    expect(() => column.datetime().default("later" as unknown as Date)).toThrow("valid Date");
    expect(() => column.number().default(Number.NaN)).toThrow("finite number");
    expect(() =>
      table("row_expression", {
        source: column.string(),
        copy: column.string().defaultSql("source"),
      }),
    ).toThrow("variable-free scalar SQL expression");
    expect(() =>
      table("parameter_expression", { value: column.string().defaultSql("$1") }),
    ).toThrow("variable-free scalar SQL expression");
    expect(() =>
      table("aggregate_expression", { value: column.number().defaultSql("COUNT(*)") }),
    ).toThrow("variable-free scalar SQL expression");
    expect(() =>
      table("wrong_expression_type", { value: column.string().defaultSql("1 + 2") }),
    ).toThrow("produces number, but the column is string");
  });

  it("makes default-bearing columns optional in insert rows", () => {
    type Insert = InferInsertRow<typeof notes>;
    const minimal: Insert = { body: "hi" };
    const explicit: Insert = { body: "hi", id: 5, status: "posted", tags: null };
    expect([minimal, explicit]).toBeDefined();
    // @ts-expect-error body has no default and stays required
    const missing: Insert = { id: 1 };
    expect(missing).toBeDefined();

    const nullableDefault = table("nullable_default_type", {
      body: column.string(),
      status: column.string().default("draft").nullable(),
    });
    type NullableDefaultInsert = InferInsertRow<typeof nullableDefault>;
    const omitted: NullableDefaultInsert = { body: "omitted" };
    const explicitNull: NullableDefaultInsert = { body: "null", status: null };
    expect([nullableDefault, omitted, explicitNull]).toBeDefined();
  });

  it("accepts omitted default-bearing columns through the Standard Schema validator", () => {
    const valid = notes["~standard"].validate({ body: "hi" });
    expect("value" in valid).toBe(true);
    const invalid = notes["~standard"].validate({});
    if ("issues" in invalid) {
      expect(invalid.issues.map(({ path }) => path[0])).toEqual(["body"]);
    } else {
      expect.unreachable("expected issues");
    }
  });

  it("round-trips every default spec through the wire format", () => {
    const rebuilt = deserializeSchema(serializeSchema(schema([notes])));
    const rebuiltColumns = rebuilt.tables[0]?.columns ?? {};
    expect(
      Object.fromEntries(
        Object.entries(rebuiltColumns).map(([name, definition]) => [
          name,
          definition.defaultSpec ?? null,
        ]),
      ),
    ).toEqual({
      id: { kind: "autoincrement" },
      slug: { kind: "expression", sql: "gen_random_uuid()" },
      status: { kind: "literal", value: "draft" },
      created: { kind: "expression", sql: "CURRENT_TIMESTAMP" },
      body: null,
      tags: null,
    });
    expect(rebuiltColumns.id?.isUnique).toBe(true);
  });

  it("plans default alterations and rejects auto-increment transitions", () => {
    const idRecord = {
      id: "c1",
      name: "id",
      type: "number" as const,
      nullable: false,
      defaultValue: { kind: "autoincrement" as const },
    };
    const statusRecord = { id: "c2", name: "status", type: "string" as const, nullable: false };
    const notesRecord = {
      id: "t",
      name: "notes",
      columns: [idRecord, statusRecord],
      uniqueKeyColumnId: "c1",
      managed: false,
      createdAt: "2026-01-01T00:00:00.000Z",
      revision: 0,
    };
    const current = [notesRecord];
    const idColumn = column.number().unique().autoIncrement();
    const added = planMigration(
      toCatalog(current),
      schema([table("notes", { id: idColumn, status: column.string().default("draft") })]),
    );
    expect(added.steps).toEqual([
      {
        kind: "alter-default",
        tableName: "notes",
        columnName: "status",
        defaultValue: { kind: "literal", value: "draft" },
      },
    ]);
    const unchanged = planMigration(
      toCatalog(current),
      schema([table("notes", { id: idColumn, status: column.string() })]),
    );
    expect(unchanged.steps).toEqual([]);
    const currentWithLiteral = [
      {
        ...notesRecord,
        columns: [
          idRecord,
          { ...statusRecord, defaultValue: { kind: "literal" as const, value: "x" } },
        ],
      },
    ];
    const removed = planMigration(
      toCatalog(currentWithLiteral),
      schema([table("notes", { id: idColumn, status: column.string() })]),
    );
    expect(removed.steps).toEqual([
      { kind: "alter-default", tableName: "notes", columnName: "status", defaultValue: null },
    ]);
    const currentWithExpression = [
      {
        ...notesRecord,
        columns: [
          idRecord,
          {
            ...statusRecord,
            defaultValue: { kind: "expression" as const, sql: "lower('QUEUED')" },
          },
        ],
      },
    ];
    expect(
      planMigration(
        toCatalog(currentWithExpression),
        schema([
          table("notes", {
            id: idColumn,
            status: column.string().defaultSql("lower('QUEUED')"),
          }),
        ]),
      ).steps,
    ).toEqual([]);
    expect(
      planMigration(
        toCatalog(currentWithExpression),
        schema([
          table("notes", {
            id: idColumn,
            status: column.string().defaultSql("upper('queued')"),
          }),
        ]),
      ).steps,
    ).toEqual([
      {
        kind: "alter-default",
        tableName: "notes",
        columnName: "status",
        defaultValue: { kind: "expression", sql: "upper('queued')" },
      },
    ]);
    // Dropping the generator is a catalog edit: nothing stored changes, writes just stop
    // being filled.
    expect(
      planMigration(
        toCatalog(current),
        schema([table("notes", { id: column.number().unique(), status: column.string() })]),
      ).steps,
    ).toEqual([
      { kind: "set-auto-increment", tableName: "notes", columnName: "id", enabled: false },
    ]);
  });

  it("evaluates SQL defaults per omitted row and preserves explicit NULL", async () => {
    const store = new MemoryBlockStore();
    const stamped = new Date("2026-08-24T12:34:56.000Z");
    const database = new MinnowDatabase(store, { now: () => stamped });
    const entries = table("default_expressions", {
      id: column.integer().unique().autoIncrement(),
      token: column.uuid().defaultSql("gen_random_uuid()"),
      created: column.datetime().defaultSql("CURRENT_TIMESTAMP"),
      amount: column.numeric({ precision: 8, scale: 2 }).defaultSql("1.25 + 2"),
      note: column.string().default("generated").nullable(),
    });
    await database.migrate(schema([entries]));
    const result = await database.insertBatch("default_expressions", [{}, { note: null }]);
    expect(result.generatedColumns?.created).toEqual([stamped, stamped]);
    const rows = await database.query(
      "SELECT id, token, created, amount, note FROM default_expressions ORDER BY id",
    );
    expect(rows.rows).toMatchObject([
      { id: 1, created: stamped, amount: "3.25", note: "generated" },
      { id: 2, created: stamped, amount: "3.25", note: null },
    ]);
    expect(rows.rows.map(({ token }) => token)).toEqual([
      expect.stringMatching(/^[0-9a-f-]{36}$/),
      expect.stringMatching(/^[0-9a-f-]{36}$/),
    ]);
    expect(rows.rows[0]?.token).not.toBe(rows.rows[1]?.token);
    store.close();
  });

  it("applies default alterations through migrate and uses them on insert", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const v1 = table("posts", {
      id: column.number().unique().autoIncrement(),
      status: column.string(),
    });
    await database.migrate(schema([v1]));
    const v2 = table("posts", {
      id: column.number().unique().autoIncrement(),
      status: column.string().default("draft"),
    });
    const altered = await database.migrate(schema([v2]));
    expect(altered.alteredTables).toEqual(["posts"]);
    expect((await database.migrate(schema([v2]))).steps).toEqual([]);
    const inserted = await database.insertBatch("posts", [{}]);
    expect(inserted.generatedColumns).toEqual({ id: [1], status: ["draft"] });

    const v3 = table("posts", {
      id: column.number().unique().autoIncrement(),
      status: column.string().defaultSql("upper('queued')"),
    });
    await database.migrate(schema([v3]));
    expect((await database.insertBatch("posts", [{}])).generatedColumns).toEqual({
      id: [2],
      status: ["QUEUED"],
    });

    await database.migrate(schema([v1]));
    await expect(database.insertBatch("posts", [{}])).rejects.toThrow("status[0] cannot be null");
    expect((await database.query("SELECT id, status FROM posts ORDER BY id")).rows).toEqual([
      { id: 1, status: "draft" },
      { id: 2, status: "QUEUED" },
    ]);
    store.close();
  });
});

describe("stored generated columns", () => {
  const ordinary = table("offline_rows", {
    id: column.integer().unique(),
    field_id: column.string(),
    version: column.integer(),
    offline_key: column.string(),
  });
  const generated = table("offline_rows", {
    id: column.integer().unique(),
    field_id: column.string(),
    version: column.integer(),
    offline_key: column.string().generatedSql("field_id || ':' || CAST(version AS TEXT)"),
  });

  it("creates, writes, updates, introspects, and wire-round-trips a generated column", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.migrate(schema([generated]));
    const inserted = await database.insertBatch("offline_rows", [
      { id: 1, field_id: "field", version: 2 },
    ]);
    expect(inserted.generatedColumns).toEqual({ offline_key: ["field:2"] });
    const updated = await database.updateBatch("offline_rows", {
      keys: [1],
      changes: { version: [3] },
    });
    expect(updated.generatedColumns).toEqual({ offline_key: ["field:3"] });
    expect((await database.query("SELECT * FROM offline_rows")).rows).toEqual([
      { id: 1, field_id: "field", version: 3, offline_key: "field:3" },
    ]);
    expect(
      (await database.listTables())[0]?.columns.find(({ name }) => name === "offline_key"),
    ).toMatchObject({
      generatedValue: { kind: "stored", sql: "field_id || ':' || CAST(version AS TEXT)" },
    });

    const restored = deserializeSchema(serializeSchema(schema([generated])));
    expect(restored.tables[0]?.columns.offline_key?.generatedSpec).toEqual({
      kind: "stored",
      sql: "field_id || ':' || CAST(version AS TEXT)",
    });
    await expect(database.dropColumn("offline_rows", "field_id")).rejects.toThrow(
      "Generated column offline_key still uses this column",
    );
  });

  it("adopts a matching application-maintained column and refuses stale rows", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.migrate(schema([ordinary]));
    await database.insertBatch("offline_rows", [
      { id: 1, field_id: "field", version: 2, offline_key: "field:2" },
    ]);
    const adopted = await database.migrate(schema([generated]));
    expect(adopted.steps).toEqual([
      {
        kind: "alter-generated",
        tableName: "offline_rows",
        columnName: "offline_key",
        generatedValue: { kind: "stored", sql: "field_id || ':' || CAST(version AS TEXT)" },
      },
    ]);
    expect((await database.migrate(schema([generated]))).steps).toEqual([]);

    const stale = new MinnowDatabase(new MemoryBlockStore());
    await stale.migrate(schema([ordinary]));
    await stale.insertBatch("offline_rows", [
      { id: 1, field_id: "field", version: 2, offline_key: "stale" },
    ]);
    await expect(stale.migrate(schema([generated]))).rejects.toThrow(
      "Generated column cannot be adopted",
    );
  });

  it("rejects adding a generated column to a populated table without a rewrite", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const before = table("offline_rows", {
      id: column.integer().unique(),
      field_id: column.string(),
      version: column.integer(),
    });
    await database.migrate(schema([before]));
    await expect(database.migrate(schema([generated]))).rejects.toThrow(
      "Generated columns cannot be added to an existing table without rewriting its rows",
    );
  });
});

const machines = table("machines", {
  name: column.string().unique(),
  state: column.enum(["off", "idle", "running"]),
  mode: column.enum(["auto", "manual"]).nullable(),
});

describe("enum columns", () => {
  it("infers literal unions for select, insert, and update types", () => {
    expectTypeOf<InferRow<typeof machines>>().toEqualTypeOf<{
      name: string;
      state: "off" | "idle" | "running";
      mode: "auto" | "manual" | null;
    }>();
    expectTypeOf<InferInsertRow<typeof machines>>().toEqualTypeOf<
      { name: string; state: "off" | "idle" | "running" } & {
        mode?: "auto" | "manual" | null;
      }
    >();
    expectTypeOf<InferUpdateChanges<typeof machines>>().toEqualTypeOf<{
      state?: "off" | "idle" | "running";
      mode?: "auto" | "manual" | null;
    }>();
    // @ts-expect-error a value outside the enum is a compile error
    const bad: InferInsertRow<typeof machines> = { name: "m", state: "paused" };
    expect(bad).toBeDefined();
  });

  it("carries the value set on the builder and keeps defaults literal", () => {
    expect(machines.columns.state.enumValues).toEqual(["off", "idle", "running"]);
    expect(machines.columns.state.type).toBe("string");
    const defaulted = column.enum(["open", "closed"]).default("open");
    expect(defaulted.defaultSpec).toEqual({ kind: "literal", value: "open" });
    expect(defaulted.hasDefault).toBe(true);
    expect(defaulted.enumValues).toEqual(["open", "closed"]);
  });

  it("rejects invalid enum declarations explicitly", () => {
    expect(() => column.enum([] as unknown as ["x"])).toThrow("at least one value");
    expect(() => column.enum(["a", "a"])).toThrow("Duplicate enum value");
    expect(() => column.enum([""])).toThrow("non-empty strings");
    expect(() => table("bad", { state: column.enum(["a", "b"]).default("c" as "a") })).toThrow(
      "Default must be one of the enum values",
    );
  });

  it("validates membership through the Standard Schema interface", () => {
    const valid = machines["~standard"].validate({ name: "m", state: "idle", mode: null });
    expect("value" in valid).toBe(true);
    const invalid = machines["~standard"].validate({ name: "m", state: "paused" });
    if ("issues" in invalid) {
      expect(invalid.issues).toEqual([
        { message: "Expected one of: off, idle, running", path: ["state"] },
      ]);
    } else {
      expect.unreachable("expected issues");
    }
  });

  it("round-trips enum values through the wire format", () => {
    const rebuilt = deserializeSchema(serializeSchema(schema([machines])));
    const rebuiltColumns = rebuilt.tables[0]?.columns ?? {};
    expect(rebuiltColumns.state?.enumValues).toEqual(["off", "idle", "running"]);
    expect(rebuiltColumns.state?.type).toBe("string");
    expect(rebuiltColumns.mode?.enumValues).toEqual(["auto", "manual"]);
    expect(rebuiltColumns.mode?.isNullable).toBe(true);
    expect(rebuiltColumns.name?.enumValues).toBeUndefined();
  });

  it("plans enum widening and rejects narrowing", () => {
    const keyRecord = { id: "c1", name: "name", type: "string" as const, nullable: false };
    const stateRecord = {
      id: "c2",
      name: "state",
      type: "string" as const,
      nullable: false,
      enumValues: ["off", "idle"],
    };
    const current = [
      {
        id: "t",
        name: "machines",
        columns: [keyRecord, stateRecord],
        uniqueKeyColumnId: "c1",
        managed: false,
        createdAt: "2026-01-01T00:00:00.000Z",
        revision: 0,
      },
    ];
    const key = column.string().unique();
    const unchanged = planMigration(
      toCatalog(current),
      schema([table("machines", { name: key, state: column.enum(["off", "idle"]) })]),
    );
    expect(unchanged.steps).toEqual([]);
    const widened = planMigration(
      toCatalog(current),
      schema([table("machines", { name: key, state: column.enum(["off", "idle", "running"]) })]),
    );
    expect(widened.steps).toEqual([
      {
        kind: "widen-enum",
        tableName: "machines",
        columnName: "state",
        enumValues: ["off", "idle", "running"],
      },
    ]);
    const relaxed = planMigration(
      toCatalog(current),
      schema([table("machines", { name: key, state: column.string() })]),
    );
    expect(relaxed.steps).toEqual([
      { kind: "widen-enum", tableName: "machines", columnName: "state", enumValues: null },
    ]);
    expect(() =>
      planMigration(
        toCatalog(current),
        schema([table("machines", { name: key, state: column.enum(["off"]) })]),
      ),
    ).toThrow("Enum values cannot be removed: machines.state drops idle");
    expect(() =>
      planMigration(
        toCatalog(current),
        schema([table("machines", { name: column.enum(["a"]).unique(), state: column.string() })]),
      ),
    ).toThrow("Plain string columns cannot tighten to an enum: machines.name");
  });

  it("migrates enum columns end to end and validates every write", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const v1 = table("tickets", {
      id: column.number().unique().autoIncrement(),
      status: column.enum(["open", "closed"]).default("open"),
    });
    await database.migrate(schema([v1]));
    const inserted = await database.insertBatch("tickets", [{}, { status: "closed" }]);
    expect(inserted.generatedColumns?.status).toEqual(["open", "closed"]);
    await expect(database.insertBatch("tickets", [{ status: null }])).rejects.toThrow(
      "status[0] cannot be null",
    );
    await expect(database.insertBatch("tickets", [{ status: "reopened" }])).rejects.toThrow(
      "status[0] must be one of: open, closed",
    );
    await expect(
      database.updateBatch("tickets", { keys: [1], changes: { status: ["reopened"] } }),
    ).rejects.toThrow("status[0] must be one of: open, closed");

    // Widening the set and adding a nullable enum column are catalog-only steps.
    const v2 = table("tickets", {
      id: column.number().unique().autoIncrement(),
      status: column.enum(["open", "closed", "reopened"]).default("open"),
      severity: column.enum(["low", "high"]).nullable(),
    });
    const widened = await database.migrate(schema([v2]));
    expect(widened.alteredTables).toEqual(["tickets"]);
    expect((await database.migrate(schema([v2]))).steps).toEqual([]);
    await database.insertBatch("tickets", [{ status: "reopened", severity: "high" }]);
    await expect(
      database.insertBatch("tickets", [{ status: "open", severity: "medium" }]),
    ).rejects.toThrow("severity[0] must be one of: low, high");
    expect(
      (await database.listTables())[0]?.columns.map(({ name, enumValues }) => ({
        name,
        ...(enumValues === undefined ? {} : { enumValues }),
      })),
    ).toEqual([
      { name: "id" },
      { name: "status", enumValues: ["open", "closed", "reopened"] },
      { name: "severity", enumValues: ["low", "high"] },
    ]);

    // Relaxing to a plain string drops the restriction.
    const v3 = table("tickets", {
      id: column.number().unique().autoIncrement(),
      status: column.string().default("open"),
      severity: column.enum(["low", "high"]).nullable(),
    });
    await database.migrate(schema([v3]));
    await database.insertBatch("tickets", [{ status: "anything", severity: null }]);
    store.close();
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

    const first = await database.migrate(wide(1));
    expect(first.createdTables).toHaveLength(tableCount);
    expect(counters.addTable).toBe(tableCount);
    expect(counters.getTableByName).toBe(0);
    expect(counters.updateTable).toBe(0);

    Object.assign(counters, { listTables: 0, getTableByName: 0, updateTable: 0, addTable: 0 });
    const noop = await database.migrate(wide(1));
    expect(noop.steps).toEqual([]);
    expect(counters).toEqual({ listTables: 1, getTableByName: 0, updateTable: 0, addTable: 0 });

    Object.assign(counters, { listTables: 0, getTableByName: 0, updateTable: 0, addTable: 0 });
    const evolved = await database.migrate(wide(2));
    expect(evolved.alteredTables).toHaveLength(tableCount);
    // Exactly one catalog write per altered table, no per-step lookups.
    expect(counters).toEqual({
      listTables: 1,
      getTableByName: 0,
      updateTable: tableCount,
      addTable: 0,
    });
    // The operation counts above are the whole contract. A wall-clock budget used to sit here
    // too, and did nothing but fail on a loaded machine: quadratic planning shows up as extra
    // catalog reads, which the counters catch exactly. Timings belong in the performance gate.
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
        toCatalog(current),
        schema([
          table("people", {
            name: column.string().unique(),
            score: column.string(),
            joined: column.datetime().nullable(),
          }),
        ]),
      ),
    ).toThrow("Column types cannot change");
    // A drop is planned, not refused — migrate() is what withholds it without an opt-in.
    expect(
      planMigration(
        toCatalog(current),
        schema([table("people", { name: column.string().unique(), score: column.number() })]),
      ).steps,
    ).toEqual([{ kind: "drop-column", tableName: "people", columnName: "joined" }]);
    expect(() =>
      planMigration(
        toCatalog(current),
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
        toCatalog(current),
        schema([
          table("people", {
            name: column.string(),
            score: column.number().unique(),
            joined: column.datetime().nullable(),
          }),
        ]),
      ),
    ).toThrow("Unique keys cannot change");
    // Tightening is planned, then earned: migrate() proves it from block headers before
    // applying, and refuses when a stored row holds NULL.
    expect(
      planMigration(
        toCatalog(current),
        schema([
          table("people", {
            name: column.string().unique(),
            score: column.number(),
            joined: column.datetime(),
          }),
        ]),
      ).steps,
    ).toEqual([{ kind: "tighten-nullable", tableName: "people", columnName: "joined" }]);
    store.close();
  });

  it("surfaces concurrent catalog changes as conflicts", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([people]));
    const record = await store.getTableByName("people");
    if (record === undefined) throw new Error("missing record");
    await store.updateTable(record.id, record.revision, { columns: record.columns });
    await expect(
      store.updateTable(record.id, record.revision, { columns: record.columns }),
    ).rejects.toThrow(TableRecordConflictError);
    store.close();
  });
});

// --- Declared constraints -------------------------------------------------------------------------

/**
 * The same intent expressed two ways. A table the schema DSL creates and a table SQL DDL creates
 * must be the same table: same catalog, same rejections. `migrate()` used to drop both constraint
 * kinds on the floor, so this pair is the regression that proves it does not.
 */
const relational = schema([
  table("parents", { id: column.number().unique(), label: column.string() }),
  table(
    "children",
    {
      id: column.number().unique(),
      parent_id: column.number().references("parents", "id"),
      qty: column.number(),
    },
    { checks: [{ name: "positive_qty", sql: "qty > 0" }] },
  ),
]);

const EQUIVALENT_DDL = [
  `CREATE TABLE parents (id INTEGER PRIMARY KEY, label VARCHAR(40) NOT NULL)`,
  `CREATE TABLE children (
     id INTEGER PRIMARY KEY,
     parent_id INTEGER NOT NULL REFERENCES parents(id),
     qty INTEGER NOT NULL,
     CONSTRAINT positive_qty CHECK (qty > 0)
   )`,
];

async function viaMigrate(): Promise<{ database: MinnowDatabase; store: MemoryBlockStore }> {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.migrate(relational);
  return { database, store };
}

async function viaSql(): Promise<{ database: MinnowDatabase; store: MemoryBlockStore }> {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  for (const statement of EQUIVALENT_DDL) await database.execute(statement);
  return { database, store };
}

const paths = [
  { name: "schema DSL migrate()", open: viaMigrate },
  { name: "SQL DDL", open: viaSql },
];

describe.each(paths)("declared constraints via $name", ({ open }) => {
  it("stores the same foreign keys and checks in the catalog", async () => {
    const { store } = await open();
    const record = await store.getTableByName("children");
    expect(record?.foreignKeys).toEqual([
      {
        name: "children_parent_id_fkey",
        columns: ["parent_id"],
        parentTable: "parents",
        parentColumns: ["id"],
        onDelete: "restrict",
      },
    ]);
    expect(record?.checks).toEqual([{ name: "positive_qty", sql: "qty > 0" }]);
    store.close();
  });

  it("rejects a child row whose parent does not exist", async () => {
    const { database, store } = await open();
    await expect(
      database.insertBatch("children", [{ id: 1, parent_id: 999, qty: 1 }]),
    ).rejects.toThrow(/FOREIGN KEY children_parent_id_fkey/);
    store.close();
  });

  it("rejects a row failing the check", async () => {
    const { database, store } = await open();
    await database.insertBatch("parents", [{ id: 1, label: "a" }]);
    await expect(
      database.insertBatch("children", [{ id: 1, parent_id: 1, qty: 0 }]),
    ).rejects.toThrow(/CHECK positive_qty/);
    store.close();
  });

  it("restricts deleting a referenced parent, and allows an unreferenced one", async () => {
    const { database, store } = await open();
    await database.insertBatch("parents", [
      { id: 1, label: "referenced" },
      { id: 2, label: "free" },
    ]);
    await database.insertBatch("children", [{ id: 1, parent_id: 1, qty: 5 }]);
    await expect(database.deleteBatch("parents", { keys: [1] })).rejects.toThrow(/FOREIGN KEY/);
    await expect(database.deleteBatch("parents", { keys: [2] })).resolves.toBeDefined();
    store.close();
  });

  it("accepts a row satisfying both constraints", async () => {
    const { database, store } = await open();
    await database.insertBatch("parents", [{ id: 1, label: "a" }]);
    await database.insertBatch("children", [{ id: 1, parent_id: 1, qty: 3 }]);
    expect(await database.readTable("children")).toEqual([{ id: 1, parent_id: 1, qty: 3 }]);
    store.close();
  });
});

describe("referential actions", () => {
  it("cascades child deletes when the relation declares it", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(
      schema([
        table("parents", { id: column.number().unique(), label: column.string() }),
        table("children", {
          id: column.number().unique(),
          parent_id: column.number().references("parents", "id", { onDelete: "cascade" }),
        }),
      ]),
    );
    await database.insertBatch("parents", [{ id: 1, label: "a" }]);
    await database.insertBatch("children", [
      { id: 1, parent_id: 1 },
      { id: 2, parent_id: 1 },
    ]);
    await database.deleteBatch("parents", { keys: [1] });
    expect(await database.readTable("children")).toEqual([]);
    store.close();
  });

  it("nulls the child column when the relation declares set null", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(
      schema([
        table("parents", { id: column.number().unique(), label: column.string() }),
        table("children", {
          id: column.number().unique(),
          parent_id: column
            .number()
            .nullable()
            .references("parents", "id", { onDelete: "set null" }),
        }),
      ]),
    );
    await database.insertBatch("parents", [{ id: 1, label: "a" }]);
    await database.insertBatch("children", [{ id: 1, parent_id: 1 }]);
    await database.deleteBatch("parents", { keys: [1] });
    expect(await database.readTable("children")).toEqual([{ id: 1, parent_id: null }]);
    store.close();
  });

  it("refuses set null on a non-nullable column at declaration time", () => {
    expect(() =>
      table("children", {
        id: column.number().unique(),
        parent_id: column.number().references("parents", "id", { onDelete: "set null" as never }),
      }),
    ).toThrow("ON DELETE SET NULL requires a nullable column: children.parent_id");
  });

  it("derives the same constraint name the SQL parser derives", () => {
    const children = relational.tables[1];
    if (children === undefined) expect.unreachable("children table missing");
    expect(declaredForeignKeys(children).map((key) => key.name)).toEqual([
      "children_parent_id_fkey",
    ]);
  });
});

describe("constraint migration safety", () => {
  async function migrated(): Promise<{ database: MinnowDatabase; store: MemoryBlockStore }> {
    return viaMigrate();
  }

  it("is idempotent when the constraints are unchanged", async () => {
    const { database, store } = await migrated();
    const again = await database.migrate(relational);
    expect(again.createdTables).toEqual([]);
    expect(again.steps).toEqual([]);
    store.close();
  });

  it("refuses to add a relation to an existing table", async () => {
    const { database, store } = await migrated();
    await expect(
      database.migrate(
        schema([
          table("parents", { id: column.number().unique(), label: column.string() }),
          table(
            "children",
            {
              id: column.number().unique(),
              parent_id: column.number().references("parents", "id"),
              qty: column.number().references("parents", "id"),
            },
            { checks: [{ name: "positive_qty", sql: "qty > 0" }] },
          ),
        ]),
      ),
    ).rejects.toThrow("FOREIGN KEY cannot be added after creation: children.qty");
    store.close();
  });

  it("refuses to drop a relation still in the catalog", async () => {
    const { database, store } = await migrated();
    await expect(
      database.migrate(
        schema([
          table("parents", { id: column.number().unique(), label: column.string() }),
          table(
            "children",
            {
              id: column.number().unique(),
              parent_id: column.number(),
              qty: column.number(),
            },
            { checks: [{ name: "positive_qty", sql: "qty > 0" }] },
          ),
        ]),
      ),
    ).rejects.toThrow("FOREIGN KEY cannot be dropped: children still has children_parent_id_fkey");
    store.close();
  });

  it("refuses to change a referential action", async () => {
    const { database, store } = await migrated();
    await expect(
      database.migrate(
        schema([
          table("parents", { id: column.number().unique(), label: column.string() }),
          table(
            "children",
            {
              id: column.number().unique(),
              parent_id: column.number().references("parents", "id", { onDelete: "cascade" }),
              qty: column.number(),
            },
            { checks: [{ name: "positive_qty", sql: "qty > 0" }] },
          ),
        ]),
      ),
    ).rejects.toThrow("FOREIGN KEY cannot change: children.parent_id");
    store.close();
  });

  it("refuses to add, change, or drop a check on an existing table", async () => {
    const children = (checks: Array<{ name: string; sql: string }>) =>
      schema([
        table("parents", { id: column.number().unique(), label: column.string() }),
        table(
          "children",
          {
            id: column.number().unique(),
            parent_id: column.number().references("parents", "id"),
            qty: column.number(),
          },
          { checks },
        ),
      ]);
    const { database, store } = await migrated();
    await expect(
      database.migrate(
        children([
          { name: "positive_qty", sql: "qty > 0" },
          { name: "small_qty", sql: "qty < 100" },
        ]),
      ),
    ).rejects.toThrow("CHECK cannot be added after creation: children.small_qty");
    await expect(
      database.migrate(children([{ name: "positive_qty", sql: "qty >= 0" }])),
    ).rejects.toThrow("CHECK cannot change: children.positive_qty");
    await expect(database.migrate(children([]))).rejects.toThrow(
      "CHECK cannot be dropped: children still has positive_qty",
    );
    store.close();
  });

  it("rejects malformed check declarations at table definition", () => {
    expect(() =>
      table("t", { id: column.number().unique() }, { checks: [{ name: "a", sql: "  " }] }),
    ).toThrow("CHECK a in table t has no expression");
    expect(() =>
      table(
        "t",
        { id: column.number().unique() },
        {
          checks: [
            { name: "a", sql: "id > 0" },
            { name: "a", sql: "id < 9" },
          ],
        },
      ),
    ).toThrow("Duplicate CHECK in table t: a");
    expect(() =>
      table("t", { id: column.number().unique() }, { checks: [{ name: "bad", sql: "nope(" }] }),
    ).toThrow();
    expect(() =>
      table(
        "t",
        { id: column.number().unique(), parent_id: column.number() },
        { checks: [{ name: "known", sql: "missing > 0" }] },
      ),
    ).toThrow("CHECK known names an unknown column: missing");
    expect(() =>
      table(
        "t",
        { id: column.number().unique(), parent_id: column.number() },
        {
          foreignKeys: [
            {
              name: "same_name",
              columns: ["parent_id"],
              references: { table: "t", columns: ["id"] },
            },
          ],
          checks: [{ name: "same_name", sql: "parent_id >= 0" }],
        },
      ),
    ).toThrow("Duplicate constraint in table t: same_name");
  });
});

describe("constraint wire round trip", () => {
  it("preserves referential actions and checks across serialization", () => {
    const original = schema([
      table("parents", { id: column.number().unique(), label: column.string() }),
      table(
        "children",
        {
          id: column.number().unique(),
          parent_id: column
            .number()
            .nullable()
            .references("parents", "id", { onDelete: "set null" }),
        },
        { checks: [{ name: "positive_id", sql: "id > 0" }] },
      ),
    ]);
    const restored = deserializeSchema(serializeSchema(original));
    const children = restored.tables[1];
    const declared = original.tables[1];
    if (children === undefined || declared === undefined) {
      expect.unreachable("children table missing");
    }
    expect(children.checks).toEqual([{ name: "positive_id", sql: "id > 0" }]);
    expect(children.columns.parent_id?.reference).toEqual({
      table: "parents",
      column: "id",
      onDelete: "set null",
      enforced: true,
    });
    expect(declaredForeignKeys(children)).toEqual(declaredForeignKeys(declared));
  });
});

// --- Views ----------------------------------------------------------------------------------------

const withView = (sql: string) =>
  schema([table("customers", { id: column.number().unique(), status: column.string() })], {
    views: [
      view("active_customers", {
        sql,
        columns: { id: column.number(), status: column.string() },
      }),
    ],
  });

const ACTIVE_SQL = `SELECT id, status FROM customers WHERE status = 'active'`;

describe("views in the schema", () => {
  it("creates the view and reads through it", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const runStatement = vi.spyOn(database, "runStatement");
    const result = await database.migrate(withView(ACTIVE_SQL));
    expect(result.replacedViews).toEqual(["active_customers"]);
    expect(runStatement.mock.calls.map(([statement]) => statement.kind)).toEqual([
      "create-table",
      "create-view",
    ]);
    await database.insertBatch("customers", [
      { id: 1, status: "active" },
      { id: 2, status: "churned" },
    ]);
    const rows = await database.query("SELECT id FROM active_customers ORDER BY id");
    expect(rows.rows).toEqual([{ id: 1 }]);
    store.close();
  });

  it("is idempotent when the body is unchanged", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(withView(ACTIVE_SQL));
    const again = await database.migrate(withView(ACTIVE_SQL));
    expect(again.steps).toEqual([]);
    expect(again.replacedViews).toEqual([]);
    store.close();
  });

  it("replaces the body when the query changes, without touching table data", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(withView(ACTIVE_SQL));
    await database.insertBatch("customers", [
      { id: 1, status: "active" },
      { id: 2, status: "churned" },
    ]);
    const changed = await database.migrate(withView(`SELECT id, status FROM customers`));
    expect(changed.replacedViews).toEqual(["active_customers"]);
    expect((await database.query("SELECT id FROM active_customers ORDER BY id")).rows).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    // The underlying table is untouched by a view replacement.
    expect((await database.readTable("customers")).length).toBe(2);
    store.close();
  });

  it("drops a view it created once the schema stops declaring it", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(withView(ACTIVE_SQL));
    const dropped = await database.migrate(
      schema([table("customers", { id: column.number().unique(), status: column.string() })], {
        views: [
          view("recent_customers", {
            sql: `SELECT id, status FROM customers WHERE id > 0`,
            columns: { id: column.number(), status: column.string() },
          }),
        ],
      }),
    );
    expect(dropped.droppedViews).toEqual(["active_customers"]);
    await expect(database.query("SELECT id FROM active_customers")).rejects.toThrow();
    expect((await database.query("SELECT id FROM recent_customers")).rows).toEqual([]);
    store.close();
  });

  it("never drops a view it did not create", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(withView(ACTIVE_SQL));
    // A CREATE VIEW record carries no ownership flag — which is also the shape of any view
    // written before the flag existed, so this covers the upgrade case too.
    await database.execute(`CREATE VIEW handmade AS SELECT id FROM customers`);
    // A schema that declares neither view: the one it made is gone, the hand-made one stays.
    const result = await database.migrate(
      schema([table("customers", { id: column.number().unique(), status: column.string() })], {
        views: [
          view("other", { sql: `SELECT id FROM customers`, columns: { id: column.number() } }),
        ],
      }),
    );
    expect(result.droppedViews).toEqual(["active_customers"]);
    expect(
      (await database.introspect()).views
        .map(({ name, managed }) => `${name}:${managed ? "managed" : "unmanaged"}`)
        .sort(),
    ).toEqual(["handmade:unmanaged", "other:managed"]);
    store.close();
  });

  it("fails when the declared columns disagree with what the query produces", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await expect(
      database.migrate(
        schema([table("customers", { id: column.number().unique(), status: column.string() })], {
          views: [
            view("wrong", {
              sql: `SELECT id FROM customers`,
              columns: { id: column.number(), status: column.string() },
            }),
          ],
        }),
      ),
    ).rejects.toThrow(/View wrong declares .* but its query produces/);
    store.close();
  });

  it("refuses to shadow a table in the same schema, at declaration time", () => {
    // schema() catches the collision before a database is ever involved, so the mistake cannot
    // reach a migration that would have to decide what to do with the real table's rows.
    expect(() =>
      schema([table("customers", { id: column.number().unique(), status: column.string() })], {
        views: [
          view("customers", { sql: `SELECT id FROM customers`, columns: { id: column.number() } }),
        ],
      }),
    ).toThrow("Duplicate name in schema: customers");
  });

  it("refuses to replace a table created outside the schema", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.execute(`CREATE TABLE legacy (id INTEGER PRIMARY KEY)`);
    await expect(
      database.migrate(
        schema([table("customers", { id: column.number().unique(), status: column.string() })], {
          views: [
            view("legacy", { sql: `SELECT id FROM customers`, columns: { id: column.number() } }),
          ],
        }),
      ),
    ).rejects.toThrow("A table already exists with this name: legacy");
    store.close();
  });

  it("rejects malformed view declarations", () => {
    expect(() => view("v", { sql: "  ", columns: { a: column.number() } })).toThrow(
      "View v has no query",
    );
    expect(() => view("v", { sql: "SELECT 1", columns: {} })).toThrow(
      "View v needs at least one column",
    );
    expect(() =>
      view("v", { sql: "SELECT 1 AS a", columns: { a: column.number().unique() } }),
    ).toThrow("A view column cannot be a unique key: v.a");
    expect(() =>
      view("v", { sql: "SELECT 1 AS a", columns: { a: column.number().default(1) } }),
    ).toThrow("A view column cannot have a default: v.a");
    expect(() =>
      schema([table("t", { a: column.number().unique() })], {
        views: [view("t", { sql: "SELECT a FROM t", columns: { a: column.number() } })],
      }),
    ).toThrow("Duplicate name in schema: t");
  });

  it("survives the wire round trip with its body and columns", () => {
    const restored = deserializeSchema(serializeSchema(withView(ACTIVE_SQL)));
    const restoredView = restored.views[0];
    if (restoredView === undefined) expect.unreachable("view missing");
    expect(restoredView.name).toBe("active_customers");
    expect(restoredView.sql).toBe(ACTIVE_SQL);
    expect(Object.keys(restoredView.columns)).toEqual(["id", "status"]);
  });
});

// --- Published catalog ------------------------------------------------------------------------

describe("catalog introspection", () => {
  it("reports identity, constraints, triggers, and views", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(
      schema(
        [
          table("parents", {
            id: column.number().unique().autoIncrement(),
            label: column.string(),
          }),
          table(
            "children",
            {
              id: column.number().unique(),
              parent_id: column.number().references("parents", "id", { onDelete: "cascade" }),
              tier: column.enum(["free", "paid"]).default("free"),
            },
            { checks: [{ name: "positive_id", sql: "id > 0" }] },
          ),
        ],
        {
          views: [
            view("paid_children", {
              sql: `SELECT id, tier FROM children WHERE tier = 'paid'`,
              columns: { id: column.number(), tier: column.string() },
            }),
          ],
        },
      ),
    );
    // A trigger body may only insert into a keyless table, so the audit sink has no unique key.
    await database.execute(`CREATE TABLE child_audit (note VARCHAR(20) NOT NULL)`);
    await database.execute(
      `CREATE TRIGGER audit AFTER INSERT ON children BEGIN INSERT INTO child_audit (note) VALUES ('added'); END`,
    );

    const catalog = await database.introspect();
    expect(catalog.tables.map(({ name }) => name)).toEqual(["child_audit", "children", "parents"]);
    expect(catalog.views.map(({ name }) => name)).toEqual(["paid_children"]);

    const children = catalog.tables.find(({ name }) => name === "children");
    const parents = catalog.tables.find(({ name }) => name === "parents");
    if (children === undefined || parents === undefined) expect.unreachable("missing table");

    // Identity: every column has a stable id, and the key is named by id rather than by name.
    expect(children.columns.every(({ id }) => id.length > 0)).toBe(true);
    const keyColumn = children.columns.find(({ id }) => id === children.uniqueKeyColumnId);
    expect(keyColumn?.name).toBe("id");

    expect(children.foreignKeys).toEqual([
      {
        name: "children_parent_id_fkey",
        columns: ["parent_id"],
        parentTable: "parents",
        parentColumns: ["id"],
        onDelete: "cascade",
        enforced: true,
      },
    ]);
    expect(children.checks).toEqual([{ name: "positive_id", sql: "id > 0" }]);
    expect(children.triggers).toHaveLength(1);
    expect(children.triggers[0]).toMatchObject({
      name: "audit",
      event: "insert",
      timing: "after",
    });
    expect(children.triggers[0]?.id.length).toBeGreaterThan(0);

    // Derived facts a planner would otherwise have to decode from a default spec.
    expect(parents.columns.find(({ name }) => name === "id")?.isAutoIncrementing).toBe(true);
    expect(children.columns.find(({ name }) => name === "id")?.isAutoIncrementing).toBe(false);
    expect(children.columns.find(({ name }) => name === "tier")?.enumValues).toEqual([
      "free",
      "paid",
    ]);

    const paidChildren = catalog.views[0];
    if (paidChildren === undefined) expect.unreachable("missing view");
    expect(paidChildren.sql).toContain("WHERE tier = 'paid'");
    expect(paidChildren.columns.map(({ name }) => name)).toEqual(["id", "tier"]);
    store.close();
  });

  it("keeps a rename planable by holding column ids stable across it", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(
      schema([table("t", { id: column.number().unique(), before: column.string().nullable() })]),
    );
    const idBefore = (await database.introspect()).tables[0]?.columns.find(
      ({ name }) => name === "before",
    )?.id;
    await database.migrate(
      schema([
        table("t", {
          id: column.number().unique(),
          after: column.string().nullable().renamedFrom("before"),
        }),
      ]),
    );
    const renamed = (await database.introspect()).tables[0]?.columns.find(
      ({ name }) => name === "after",
    );
    expect(renamed?.id).toBe(idBefore);
    store.close();
  });
});

describe("planning a migration without engine access", () => {
  /**
   * The whole point of publishing the catalog: a schema tool can hold a `Catalog` value it got
   * from `introspect()` — or built itself — and plan against it with no database, no store, and
   * no engine import. This test constructs one by hand for exactly that reason.
   */
  const handBuilt: Catalog = {
    tables: [
      {
        name: "notes",
        managed: true,
        uniqueKeyColumnId: "col-id",
        columns: [
          {
            id: "col-id",
            name: "id",
            type: "number",
            nullable: false,
            defaultValue: { kind: "autoincrement" },
            isAutoIncrementing: true,
          },
          {
            id: "col-body",
            name: "body",
            type: "string",
            nullable: false,
            isAutoIncrementing: false,
          },
        ],
        foreignKeys: [],
        checks: [],
        triggers: [],
      },
    ],
    views: [],
  };

  const notesTable = () =>
    table("notes", { id: column.number().unique().autoIncrement(), body: column.string() });

  it("plans an add-column from a hand-built catalog", () => {
    const plan = planMigration(
      handBuilt,
      schema([
        table("notes", {
          id: column.number().unique().autoIncrement(),
          body: column.string(),
          note: column.string().nullable(),
        }),
      ]),
    );
    expect(plan.steps.length).toBe(1);
    const [step] = plan.steps;
    if (step?.kind !== "add-column") expect.unreachable("expected an add-column step");
    expect(step.tableName).toBe("notes");
    expect(step.columnName).toBe("note");
    expect(step.definition.type).toBe("string");
    expect(step.definition.isNullable).toBe(true);
  });

  it("plans a rename through the stable column id, not the name", () => {
    const plan = planMigration(
      handBuilt,
      schema([
        table("notes", {
          id: column.number().unique().autoIncrement(),
          content: column.string().renamedFrom("body"),
        }),
      ]),
    );
    expect(plan.steps).toEqual([
      { kind: "rename-column", tableName: "notes", from: "body", to: "content" },
    ]);
  });

  it("rejects two rename targets claiming the same catalog column", () => {
    expect(() =>
      planMigration(
        handBuilt,
        schema([
          table("notes", {
            id: column.number().unique().autoIncrement(),
            content: column.string().renamedFrom("body"),
            duplicate: column.string().renamedFrom("body"),
          }),
        ]),
      ),
    ).toThrow("Rename source is used more than once: notes.body");
  });

  it("plans nothing when the catalog already matches", () => {
    expect(planMigration(handBuilt, schema([notesTable()])).steps).toEqual([]);
  });

  it("plans a view replacement against a catalog that already has one", () => {
    const withView: Catalog = {
      ...handBuilt,
      views: [
        {
          name: "recent",
          sql: "SELECT id FROM notes",
          columns: [
            { id: "v-id", name: "id", type: "number", nullable: true, isAutoIncrementing: false },
          ],
          managed: true,
        },
      ],
    };
    const declared = view("recent", {
      sql: "SELECT id FROM notes WHERE id > 10",
      columns: { id: column.number() },
    });
    const plan = planMigration(withView, schema([notesTable()], { views: [declared] }));
    expect(plan.steps).toEqual([{ kind: "replace-view", view: declared }]);

    // Unchanged body plans nothing; a view the schema stops declaring is dropped.
    const same = view("recent", { sql: "SELECT id FROM notes", columns: { id: column.number() } });
    expect(planMigration(withView, schema([notesTable()], { views: [same] })).steps).toEqual([]);
    const other = view("other", { sql: "SELECT id FROM notes", columns: { id: column.number() } });
    expect(planMigration(withView, schema([notesTable()], { views: [other] })).steps).toEqual([
      { kind: "replace-view", view: other },
      { kind: "drop-view", viewName: "recent" },
    ]);
    // An unmanaged view of the same name is nobody's to drop.
    const unmanaged: Catalog = {
      ...withView,
      views: withView.views.map((v) => ({ ...v, managed: false })),
    };
    expect(planMigration(unmanaged, schema([notesTable()], { views: [other] })).steps).toEqual([
      { kind: "replace-view", view: other },
    ]);
  });

  it("plans a destructive step from a hand-built catalog, and flags it as one", () => {
    const plan = planMigration(
      handBuilt,
      schema([table("notes", { id: column.number().unique().autoIncrement() })]),
    );
    expect(plan.steps).toEqual([{ kind: "drop-column", tableName: "notes", columnName: "body" }]);
    expect(plan.steps.filter(isDestructiveStep)).toHaveLength(1);
  });

  it("still refuses what cannot be planned at all", () => {
    expect(() =>
      planMigration(
        handBuilt,
        schema([
          table("notes", {
            id: column.number().unique().autoIncrement(),
            body: column.number(),
          }),
        ]),
      ),
    ).toThrow("Column types cannot change");
  });
});

describe("creation order follows declared relations", () => {
  it("creates a parent before the child that references it, whatever the declaration order", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(
      schema([
        table("child", {
          id: column.number().unique(),
          parent_id: column.number().references("parent", "id"),
        }),
        table("parent", { id: column.number().unique(), label: column.string() }),
      ]),
    );
    const catalog = await database.introspect();
    expect(catalog.tables.map(({ name }) => name)).toEqual(["child", "parent"]);
    // The constraint really exists, so the ordering did not quietly skip it.
    await expect(database.insertBatch("child", [{ id: 1, parent_id: 9 }])).rejects.toThrow(
      /FOREIGN KEY/,
    );
    store.close();
  });

  it("plans creation parents-first while keeping unrelated tables in declaration order", () => {
    const plan = planMigration(
      { tables: [], views: [] },
      schema([
        table("z", { id: column.number().unique() }),
        table("child", {
          id: column.number().unique(),
          parent_id: column.number().references("parent", "id"),
        }),
        table("parent", { id: column.number().unique() }),
        table("a", { id: column.number().unique() }),
      ]),
    );
    expect(
      plan.steps.map((step) => (step.kind === "create-table" ? step.table.name : step.kind)),
    ).toEqual(["z", "parent", "child", "a"]);
  });

  it("still creates a self-referencing table", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(
      schema([
        table("node", {
          id: column.number().unique(),
          parent: column.number().nullable().references("node", "id"),
        }),
      ]),
    );
    expect((await database.introspect()).tables[0]?.foreignKeys).toHaveLength(1);
    store.close();
  });
});

// --- Backfilled columns ---------------------------------------------------------------------

describe("adding a column with a backfill", () => {
  const before = schema([table("notes", { id: column.number().unique(), body: column.string() })]);
  const after = (backfill: string) =>
    schema([
      table("notes", {
        id: column.number().unique(),
        body: column.string(),
        status: column.string().backfill(backfill),
      }),
    ]);

  async function seeded(): Promise<{ database: MinnowDatabase; store: MemoryBlockStore }> {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(before);
    await database.insertBatch("notes", [
      { id: 1, body: "old one" },
      { id: 2, body: "old two" },
    ]);
    return { database, store };
  }

  it("adds a non-nullable column and reads the value for rows written before it", async () => {
    const { database, store } = await seeded();
    await database.migrate(after("archived"));
    // The boxed read path.
    expect(await database.readTable("notes")).toEqual([
      { id: 1, body: "old one", status: "archived" },
      { id: 2, body: "old two", status: "archived" },
    ]);
    // The vectorized read path.
    expect((await database.query("SELECT id, status FROM notes ORDER BY id")).rows).toEqual([
      { id: 1, status: "archived" },
      { id: 2, status: "archived" },
    ]);
    // And it is filterable, so the value is real to the executor, not patched into output rows.
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM notes WHERE status = 'archived'")).rows,
    ).toEqual([{ n: 2 }]);
    store.close();
  });

  it("keeps a new column's backfill separate from its future SQL default", async () => {
    const { database, store } = await seeded();
    const evolved = schema([
      table("notes", {
        id: column.number().unique(),
        body: column.string(),
        status: column.string().backfill("legacy").defaultSql("lower('NEW')"),
      }),
    ]);
    await database.migrate(evolved);
    await database.insertBatch("notes", [{ id: 3, body: "new" }]);
    expect((await database.query("SELECT id, status FROM notes ORDER BY id")).rows).toEqual([
      { id: 1, status: "legacy" },
      { id: 2, status: "legacy" },
      { id: 3, status: "new" },
    ]);
    store.close();
  });

  it("uses a new nullable column's default only for future writes", async () => {
    const { database, store } = await seeded();
    await database.migrate(
      schema([
        table("notes", {
          id: column.number().unique(),
          body: column.string(),
          status: column.string().default("new").nullable(),
        }),
      ]),
    );
    await database.insertBatch("notes", [{ id: 3, body: "new" }]);
    expect((await database.query("SELECT id, status FROM notes ORDER BY id")).rows).toEqual([
      { id: 1, status: null },
      { id: 2, status: null },
      { id: 3, status: "new" },
    ]);
    store.close();
  });

  it("does not rewrite the stored segments", async () => {
    const { database, store } = await seeded();
    const before = await allVisibleSegments(database, "notes");
    await database.migrate(after("archived"));
    const after2 = await allVisibleSegments(database, "notes");
    expect(after2.map(({ id }) => id)).toEqual(before.map(({ id }) => id));
    // The new column owns no blocks in the old segment: the value is substituted, not stored.
    expect(Object.keys(after2[0]?.columnBlockIds ?? {})).toHaveLength(2);
    store.close();
  });

  it("lets rows written afterwards carry their own value, including through mutations", async () => {
    const { database, store } = await seeded();
    await database.migrate(after("archived"));
    await database.insertBatch("notes", [{ id: 3, body: "new", status: "live" }]);
    await database.updateBatch("notes", { keys: [1], changes: { status: ["revived"] } });
    // The keyed-mutation read path: seeded slots, overwritten where a segment carried the column.
    expect(await database.readTable("notes")).toEqual([
      { id: 1, body: "old one", status: "revived" },
      { id: 2, body: "old two", status: "archived" },
      { id: 3, body: "new", status: "live" },
    ]);
    store.close();
  });

  it("freezes a generated backfill so every reader agrees", async () => {
    const { database, store } = await seeded();
    let calls = 0;
    const migrated = await database.migrate(
      schema([
        table("notes", {
          id: column.number().unique(),
          body: column.string(),
          stamp: column.number().backfill(() => {
            calls += 1;
            return 100 + calls;
          }),
        }),
      ]),
    );
    expect(calls).toBe(1);
    const reported = serializeMigrationSteps(migrated.steps);
    expect(calls).toBe(1);
    expect(reported.find((step) => step.kind === "add-column")).toMatchObject({
      definition: { backfillValue: 101 },
    });
    const rows = await database.readTable("notes");
    expect(rows.map((row) => row.stamp)).toEqual([101, 101]);
    // Re-reading does not re-run the generator.
    expect(calls).toBe(1);
    expect((await database.readTable("notes")).map((row) => row.stamp)).toEqual([101, 101]);
    store.close();
  });

  it("backfills every column type", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([table("t", { id: column.number().unique() })]));
    await database.insertBatch("t", [{ id: 1 }]);
    const stamp = new Date("2024-05-06T07:08:09.000Z");
    await database.migrate(
      schema([
        table("t", {
          id: column.number().unique(),
          flag: column.boolean().backfill(true),
          count: column.number().backfill(7),
          label: column.string().backfill("x"),
          at: column.datetime().backfill(stamp),
          tier: column.enum(["free", "paid"]).backfill("paid"),
        }),
      ]),
    );
    expect(await database.readTable("t")).toEqual([
      { id: 1, flag: true, count: 7, label: "x", at: stamp, tier: "paid" },
    ]);
    store.close();
  });

  it("still refuses a non-nullable column with no backfill", async () => {
    const { database, store } = await seeded();
    await expect(
      database.migrate(
        schema([
          table("notes", {
            id: column.number().unique(),
            body: column.string(),
            status: column.string(),
          }),
        ]),
      ),
    ).rejects.toThrow("Added columns must be nullable, or carry a backfill: notes.status");
    store.close();
  });

  it("rejects a backfill that does not fit the column", () => {
    expect(() =>
      table("t", { id: column.number().unique(), n: column.number().backfill("x" as never) }),
    ).toThrow("Backfill value must be a number: t.n");
    expect(() =>
      table("t", {
        id: column.number().unique(),
        tier: column.enum(["free", "paid"]).backfill("gold" as never),
      }),
    ).toThrow("Backfill value must be one of: free, paid");
    expect(() =>
      table("t", {
        id: column.number().unique(),
        s: (
          column.string().nullable() as unknown as {
            backfill(value: string): ReturnType<typeof column.string>;
          }
        ).backfill("x"),
      }),
    ).toThrow("A nullable column needs no backfill: t.s");
  });

  it("survives the wire round trip with the value frozen", () => {
    const restored = deserializeSchema(serializeSchema(after("archived")));
    const status = restored.tables[0]?.columns.status;
    expect(status?.backfillValue).toBe("archived");
    expect(status?.isNullable).toBe(false);
  });

  it("reports the value in the catalog", async () => {
    const { database, store } = await seeded();
    await database.migrate(after("archived"));
    const status = (await database.introspect()).tables[0]?.columns.find(
      ({ name }) => name === "status",
    );
    expect(status?.backfill).toBe("archived");
    expect(status?.nullable).toBe(false);
    store.close();
  });
});

// --- Provable evolution ----------------------------------------------------------------------

describe("tightening a column to NOT NULL", () => {
  const loose = schema([
    table("people", { id: column.number().unique(), city: column.string().nullable() }),
  ]);
  const tight = schema([table("people", { id: column.number().unique(), city: column.string() })]);

  it("applies when no stored row holds NULL", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(loose);
    await database.insertBatch("people", [
      { id: 1, city: "London" },
      { id: 2, city: "Paris" },
    ]);
    await database.migrate(tight);
    const city = (await database.introspect()).tables[0]?.columns.find(
      ({ name }) => name === "city",
    );
    expect(city?.nullable).toBe(false);
    // And it is enforced from then on.
    await expect(database.insertBatch("people", [{ id: 3, city: null }])).rejects.toThrow();
    store.close();
  });

  it("refuses when a stored row holds NULL, naming the fix", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(loose);
    await database.insertBatch("people", [
      { id: 1, city: "London" },
      { id: 2, city: null },
    ]);
    await expect(database.migrate(tight)).rejects.toThrow(/people\.city holds NULL in stored rows/);
    // The catalog is unchanged: a refused migration applies nothing.
    const city = (await database.introspect()).tables[0]?.columns.find(
      ({ name }) => name === "city",
    );
    expect(city?.nullable).toBe(true);
    store.close();
  });

  it("counts rows written before the column existed as NULL unless it is backfilled", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([table("t", { id: column.number().unique() })]));
    await database.insertBatch("t", [{ id: 1 }]);
    await database.migrate(
      schema([table("t", { id: column.number().unique(), note: column.string().nullable() })]),
    );
    // Those rows have no block for `note`, so tightening cannot be proven.
    await expect(
      database.migrate(
        schema([table("t", { id: column.number().unique(), note: column.string() })]),
      ),
    ).rejects.toThrow(/t\.note holds NULL in stored rows/);
    store.close();
  });
});

describe("adopting and dropping auto-increment", () => {
  it("adopts on a populated table, seeding past the largest stored key", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([table("t", { id: column.number().unique() })]));
    await database.insertBatch("t", [{ id: 5 }, { id: 41 }]);
    await database.migrate(schema([table("t", { id: column.number().unique().autoIncrement() })]));
    const id = (await database.introspect()).tables[0]?.columns[0];
    expect(id?.isAutoIncrementing).toBe(true);
    // A generated key must not collide with one already stored.
    const written = await database.insert("t", {});
    expect(written.rowCount).toBe(1);
    const ids = (await database.readTable("t"))
      .map((row) => row.id)
      .sort((a, b) => Number(a) - Number(b));
    expect(ids).toEqual([5, 41, 42]);
    store.close();
  });

  it("drops the generator without touching stored rows", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([table("t", { id: column.number().unique().autoIncrement() })]));
    await database.insertBatch("t", [{}, {}]);
    const before = await database.readTable("t");
    await database.migrate(schema([table("t", { id: column.number().unique() })]));
    expect((await database.introspect()).tables[0]?.columns[0]?.isAutoIncrementing).toBe(false);
    expect(await database.readTable("t")).toEqual(before);
    store.close();
  });
});

// --- Destructive migrations ---------------------------------------------------------------------

describe("dropping columns and tables", () => {
  const full = schema([
    table("keep", { id: column.number().unique(), body: column.string() }),
    table("gone", { id: column.number().unique() }),
  ]);
  const trimmed = schema([table("keep", { id: column.number().unique() })]);

  async function seeded(): Promise<{ database: MinnowDatabase; store: MemoryBlockStore }> {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(full);
    await database.insertBatch("keep", [{ id: 1, body: "text" }]);
    await database.insertBatch("gone", [{ id: 1 }]);
    return { database, store };
  }

  it("refuses to destroy anything by default, naming what it would have destroyed", async () => {
    const { database, store } = await seeded();
    await expect(database.migrate(trimmed, { schemaOwnsDatabase: true })).rejects.toThrow(
      /would destroy data: column keep\.body, table gone/,
    );
    // And without claiming the whole database, the undeclared table is simply not its business.
    await expect(database.migrate(trimmed)).rejects.toThrow(
      /would destroy data: column keep\.body\. Pass/,
    );
    // Nothing was applied.
    expect(await database.readTable("keep")).toEqual([{ id: 1, body: "text" }]);
    expect((await database.introspect()).tables.map(({ name }) => name)).toEqual(["gone", "keep"]);
    store.close();
  });

  it("applies the drops when the caller opts in", async () => {
    const { database, store } = await seeded();
    const result = await database.migrate(trimmed, {
      allowDestructive: true,
      schemaOwnsDatabase: true,
    });
    expect(result.droppedTables).toEqual(["gone"]);
    expect((await database.introspect()).tables.map(({ name }) => name)).toEqual(["keep"]);
    // The dropped column stops being readable; the surviving rows are untouched.
    expect(await database.readTable("keep")).toEqual([{ id: 1 }]);
    await expect(database.query("SELECT body FROM keep")).rejects.toThrow();
    expect((await database.query("SELECT id FROM keep")).rows).toEqual([{ id: 1 }]);
    store.close();
  });

  it("drops a managed view before the managed table it depends on", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    const owned = schema([table("source", { id: column.number().unique() })], {
      views: [
        view("source_ids", {
          sql: "SELECT id FROM source",
          columns: { id: column.number() },
        }),
      ],
    });
    await database.migrate(owned);
    const dropped = await database.migrate(schema([]), {
      allowDestructive: true,
      schemaOwnsDatabase: true,
    });
    expect(dropped.steps.map(({ kind }) => kind)).toEqual(["drop-view", "drop-table"]);
    expect(dropped.droppedViews).toEqual(["source_ids"]);
    expect(dropped.droppedTables).toEqual(["source"]);
    store.close();
  });

  it("never drops a table it did not create", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(schema([table("keep", { id: column.number().unique() })]));
    await database.execute(`CREATE TABLE handmade (id INTEGER PRIMARY KEY)`);
    const result = await database.migrate(
      schema([table("keep", { id: column.number().unique() })]),
      { allowDestructive: true, schemaOwnsDatabase: true },
    );
    expect(result.droppedTables).toEqual([]);
    expect((await database.introspect()).tables.map(({ name }) => name).sort()).toEqual([
      "handmade",
      "keep",
    ]);
    store.close();
  });

  it("refuses to drop a column the catalog still points at", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(
      schema([
        table("parents", { id: column.number().unique() }),
        table(
          "children",
          {
            id: column.number().unique(),
            parent_id: column.number().references("parents", "id"),
            qty: column.number(),
          },
          { checks: [{ name: "positive", sql: "qty > 0" }] },
        ),
      ]),
    );
    const without = (columns: Record<string, ReturnType<typeof column.number>>) =>
      schema([
        table("parents", { id: column.number().unique() }),
        table(
          "children",
          { id: column.number().unique(), ...columns },
          {
            checks: [{ name: "positive", sql: "qty > 0" }],
          },
        ),
      ]);
    // The FK still uses parent_id.
    await expect(
      database.migrate(without({ qty: column.number() }), { allowDestructive: true }),
    ).rejects.toThrow(
      "FOREIGN KEY children_parent_id_fkey still uses this column: children.parent_id",
    );
    // And the CHECK guards its own column the same way.
    await expect(database.dropColumn("children", "qty")).rejects.toThrow(
      "CHECK positive still uses this column: children.qty",
    );
    store.close();
  });

  it("refuses to drop the unique key", async () => {
    const { database, store } = await seeded();
    await expect(
      database.migrate(
        schema([
          table("keep", { body: column.string() }),
          table("gone", { id: column.number().unique() }),
        ]),
        { allowDestructive: true },
      ),
    ).rejects.toThrow("The unique key cannot be dropped: keep.id");
    store.close();
  });
});
