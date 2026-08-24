import { Kysely, sql, type Insertable, type Selectable, type Updateable } from "kysely";
import { Migrator, type MigrationProvider } from "kysely/migration";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import {
  MinnowDatabase,
  column,
  schema,
  table,
  view,
  type MinnowDatabaseClient,
  type MinnowSqlDriver,
} from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { MinnowDialect } from "./dialect.js";
import { createKysely } from "./create-kysely.js";
import type { InferKyselyDatabase } from "./schema.js";

interface PersonTable {
  id: number;
  name: string;
  score: number | null;
}

interface TestDatabase {
  person: PersonTable;
}

const declaredSchema = schema(
  [
    table("orders", {
      id: column.integer().unique().autoIncrement(),
      total: column.numeric({ precision: 12, scale: 2 }).default("0"),
      status: column.enum(["open", "closed"]).default("open"),
      note: column.string().nullable(),
    }),
    table(
      "order_lines",
      {
        order_id: column.integer(),
        line_no: column.integer(),
        description: column.string(),
      },
      { primaryKey: ["order_id", "line_no"] },
    ),
  ],
  {
    views: [
      view("open_orders", {
        sql: "SELECT id, total FROM orders WHERE status = 'open'",
        columns: { id: column.integer(), total: column.numeric({ precision: 12, scale: 2 }) },
      }),
    ],
  },
);

type DeclaredDatabase = InferKyselyDatabase<typeof declaredSchema>;

function invalidDerivedWritesAreRejected(): void {
  const lineChange: Updateable<DeclaredDatabase["order_lines"]> = {};
  // @ts-expect-error composite primary-key columns are not updateable
  lineChange.order_id = 2;
  const viewInsert: Insertable<DeclaredDatabase["open_orders"]> = {};
  // @ts-expect-error a view column accepts no insert value
  viewInsert.id = 1;
}

describe("schema-derived Kysely types", () => {
  it("derives select, insert, and update shapes without a second DB interface", () => {
    expect(invalidDerivedWritesAreRejected).toBeTypeOf("function");
    expectTypeOf<Selectable<DeclaredDatabase["orders"]>>().toEqualTypeOf<{
      id: number;
      total: string;
      status: "open" | "closed";
      note: string | null;
    }>();
    expectTypeOf<Insertable<DeclaredDatabase["orders"]>>().toExtend<{
      id?: number | undefined;
      total?: string | number | undefined;
      status?: "open" | "closed" | undefined;
      note?: string | null | undefined;
    }>();
    expectTypeOf<Updateable<DeclaredDatabase["orders"]>>().toEqualTypeOf<{
      total?: string | number | undefined;
      status?: "open" | "closed" | undefined;
      note?: string | null | undefined;
    }>();
    expectTypeOf<Selectable<DeclaredDatabase["open_orders"]>>().toEqualTypeOf<{
      id: number;
      total: string;
    }>();
  });

  it("creates an inferred Kysely instance and executes against the migrated schema", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(declaredSchema);
    const db = createKysely({ driver: database, schema: declaredSchema });
    const predicate = db.selectFrom("orders").select(["id", "total"]).where("total", ">=", 12.5);
    expect(predicate.compile()).toMatchObject({
      sql: 'select "id", "total" from "orders" where "total" >= $1',
      parameters: [12.5],
    });
    expectTypeOf<Awaited<ReturnType<typeof predicate.execute>>>().toEqualTypeOf<
      Array<{ id: number; total: string }>
    >();

    await db.insertInto("orders").values({ total: 12.5, status: "open" }).execute();
    expect(await predicate.execute()).toEqual([{ id: 1, total: "12.5" }]);
    expect(await db.selectFrom("orders").selectAll().execute()).toEqual([
      { id: 1, total: "12.5", status: "open", note: null },
    ]);

    const defaults = db
      .insertInto("orders")
      .values({})
      .returning(["id", "total", "status", "note"]);
    expect(defaults.compile()).toMatchObject({
      sql: 'insert into "orders" default values returning "id", "total", "status", "note"',
      parameters: [],
    });
    expect(await defaults.executeTakeFirstOrThrow()).toEqual({
      id: 2,
      total: "0",
      status: "open",
      note: null,
    });

    const defaultBatch = db
      .insertInto("orders")
      .values([{}, {}])
      .returning(["id", "total", "status"]);
    expect(defaultBatch.compile()).toMatchObject({
      sql: 'insert into "orders" ("id") values (default), (default) returning "id", "total", "status"',
      parameters: [],
    });
    expect(await defaultBatch.execute()).toEqual([
      { id: 3, total: "0", status: "open" },
      { id: 4, total: "0", status: "open" },
    ]);
    await db.destroy();
    store.close();
  });

  it("leaves catalog SQL defaults visible in compiled Kysely SQL", async () => {
    const notesSchema = schema([
      table("notes", {
        id: column.integer().unique(),
        slug: column.string().defaultSql("lower('GENERATED')"),
        body: column.string(),
      }),
    ]);
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(notesSchema);
    const db = createKysely({ driver: database, schema: notesSchema });

    const compiled = db
      .insertInto("notes")
      .values([
        { id: 1, body: "generated" },
        { id: 2, slug: "kept", body: "explicit" },
        { id: 3, slug: undefined, body: "undefined" },
      ])
      .compile();
    expect(compiled).toMatchObject({
      sql: 'insert into "notes" ("id", "body", "slug") values ($1, $2, default), ($3, $4, $5), ($6, $7, default)',
      parameters: [1, "generated", 2, "explicit", "kept", 3, "undefined"],
    });

    await db.executeQuery(compiled);
    expect(await db.selectFrom("notes").selectAll().orderBy("id").execute()).toEqual([
      { id: 1, slug: "generated", body: "generated" },
      { id: 2, slug: "kept", body: "explicit" },
      { id: 3, slug: "generated", body: "undefined" },
    ]);

    await db.insertInto("notes").values({ id: 4, body: "executed" }).execute();
    expect(await db.selectFrom("notes").select("slug").where("id", "=", 4).execute()).toEqual([
      { slug: "generated" },
    ]);
    await db.destroy();
    store.close();
  });

  it("uses SQL defaults for DEFAULT VALUES and INSERT SELECT omissions", async () => {
    const tokenSchema = schema([
      table("tokens", {
        id: column.integer().unique().autoIncrement(),
        value: column.string().defaultSql("lower('TOKEN')"),
      }),
      table("token_sources", {
        id: column.integer().unique(),
      }),
    ]);
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(tokenSchema);
    const db = createKysely({ driver: database, schema: tokenSchema });

    const compiled = db.insertInto("tokens").defaultValues().returningAll().compile();
    expect(compiled).toMatchObject({
      sql: 'insert into "tokens" default values returning *',
      parameters: [],
    });
    await db.executeQuery(compiled);
    expect(await db.selectFrom("tokens").selectAll().execute()).toEqual([
      { id: 1, value: "token" },
    ]);

    await database.execute("INSERT INTO token_sources (id) VALUES (10), (11)");
    await db
      .insertInto("tokens")
      .columns(["id"])
      .expression((builder) => builder.selectFrom("token_sources").select("id"))
      .execute();
    expect(await db.selectFrom("tokens").selectAll().orderBy("id").execute()).toEqual([
      { id: 1, value: "token" },
      { id: 10, value: "token" },
      { id: 11, value: "token" },
    ]);

    await db.destroy();
    store.close();
  });
});

describe("MinnowDialect", () => {
  let database: MinnowDatabase;
  let db: Kysely<TestDatabase>;

  beforeEach(async () => {
    database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
    db = new Kysely<TestDatabase>({ dialect: new MinnowDialect({ driver: database }) });
    await db.schema
      .createTable("person")
      .addColumn("id", "integer", (column) => column.primaryKey())
      .addColumn("name", "text", (column) => column.notNull())
      .addColumn("score", "double precision")
      .execute();
  });

  it("accepts both direct and worker-backed Minnow clients through one structural contract", () => {
    expectTypeOf<MinnowDatabase>().toExtend<MinnowSqlDriver>();
    expectTypeOf<MinnowDatabaseClient>().toExtend<MinnowSqlDriver>();
  });

  it("compiles PostgreSQL placeholders and executes reads and writes", async () => {
    const compiled = db
      .selectFrom("person")
      .select(["id", "name"])
      .where("id", ">=", 2)
      .orderBy("id")
      .compile();
    expect(compiled.sql).toBe('select "id", "name" from "person" where "id" >= $1 order by "id"');
    expect(compiled.parameters).toEqual([2]);

    const inserted = await db
      .insertInto("person")
      .values([
        { id: 1, name: "Ada", score: null },
        { id: 2, name: "Grace", score: 9 },
      ])
      .returning(["id", "name"])
      .execute();
    expect(inserted).toEqual([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);

    expect(await db.selectFrom("person").selectAll().orderBy("score").execute()).toEqual([
      { id: 2, name: "Grace", score: 9 },
      { id: 1, name: "Ada", score: null },
    ]);

    const updated = await db
      .updateTable("person")
      .set({ score: 10 })
      .where("id", "=", 1)
      .returning("score")
      .executeTakeFirstOrThrow();
    expect(updated).toEqual({ score: 10 });
  });

  it("maps Kysely transactions onto Minnow transactions", async () => {
    await db.transaction().execute(async (transaction) => {
      await transaction.insertInto("person").values({ id: 1, name: "Ada", score: 8 }).execute();
    });
    expect(await db.selectFrom("person").select("id").execute()).toEqual([{ id: 1 }]);

    await expect(
      db.transaction().execute(async (transaction) => {
        await transaction.insertInto("person").values({ id: 2, name: "Grace", score: 9 }).execute();
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect(await db.selectFrom("person").select("id").execute()).toEqual([{ id: 1 }]);

    await expect(
      db
        .transaction()
        .setIsolationLevel("serializable")
        .execute(async () => undefined),
    ).rejects.toThrow("Minnow has one transaction mode");
  });

  it("reads Minnow's catalog without inventing schemas", async () => {
    await database.execute(
      "CREATE VIEW scored AS SELECT id, name, score FROM person WHERE score IS NOT NULL",
    );
    await database.execute(
      "CREATE TABLE kysely_migration (name TEXT PRIMARY KEY, timestamp TEXT NOT NULL)",
    );

    expect(await db.introspection.getSchemas()).toEqual([]);
    const ordinary = await db.introspection.getTables();
    expect(ordinary.map(({ name, isView }) => ({ name, isView }))).toEqual([
      { name: "person", isView: false },
      { name: "scored", isView: true },
    ]);
    expect(ordinary[0]?.columns).toEqual([
      {
        name: "id",
        dataType: "integer",
        isAutoIncrementing: false,
        isNullable: false,
        hasDefaultValue: false,
      },
      {
        name: "name",
        dataType: "text",
        isAutoIncrementing: false,
        isNullable: false,
        hasDefaultValue: false,
      },
      {
        name: "score",
        dataType: "double precision",
        isAutoIncrementing: false,
        isNullable: true,
        hasDefaultValue: false,
      },
    ]);
    expect(
      (await db.introspection.getTables({ withInternalKyselyTables: true })).map(
        ({ name }) => name,
      ),
    ).toContain("kysely_migration");
  });

  it("reports logical SQL domains instead of their physical string storage", async () => {
    await database.execute("CREATE TYPE mood AS ENUM ('sad', 'happy')");
    await database.execute(
      "CREATE TABLE domain_metadata (" +
        "amount NUMERIC(12, 2), document JSONB, reference UUID, at TIME, span INTERVAL, " +
        "tags TEXT[], feeling mood)",
    );

    const table = (await db.introspection.getTables()).find(
      ({ name }) => name === "domain_metadata",
    );
    expect(table?.columns.map(({ name, dataType }) => ({ name, dataType }))).toEqual([
      { name: "amount", dataType: "numeric(12,2)" },
      { name: "document", dataType: "jsonb" },
      { name: "reference", dataType: "uuid" },
      { name: "at", dataType: "time" },
      { name: "span", dataType: "interval" },
      { name: "tags", dataType: "text[]" },
      { name: "feeling", dataType: "mood" },
    ]);
  });

  it("runs Kysely migrations on its reserved single connection", async () => {
    const provider: MigrationProvider = {
      async getMigrations() {
        return {
          "001_add_events": {
            async up(migrationDb) {
              await migrationDb.schema
                .createTable("events")
                .addColumn("id", "integer", (column) => column.primaryKey())
                .addColumn("name", "text", (column) => column.notNull())
                .execute();
            },
          },
        };
      },
    };
    const migrated = await new Migrator({ db, provider }).migrateToLatest();
    expect(migrated.error).toBeUndefined();
    expect(migrated.results).toEqual([
      { migrationName: "001_add_events", direction: "Up", status: "Success" },
    ]);
    expect((await database.introspect()).tables.map(({ name }) => name)).toEqual([
      "events",
      "kysely_migration",
      "kysely_migration_lock",
      "person",
    ]);
  });

  it("rejects values and execution modes Minnow cannot represent", async () => {
    const bigintQuery = sql`SELECT ${1n} AS value`.compile(db);
    await expect(db.executeQuery(bigintQuery)).rejects.toThrow("unsupported type bigint");
    await expect(db.selectFrom("person").selectAll().stream().next()).rejects.toThrow(
      "streaming is not supported",
    );
  });

  it("does not take ownership of the underlying database", async () => {
    await db.destroy();
    expect((await database.query("SELECT COUNT(*) AS count FROM person")).rows).toEqual([
      { count: 0 },
    ]);
  });
});
