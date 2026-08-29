import {
  ColumnNode,
  HandleEmptyInListsPlugin,
  Kysely,
  SelectQueryNode,
  SelectionNode,
  TableNode,
  replaceWithNoncontingentExpression,
  sql,
  type Expression,
  type Insertable,
  type Selectable,
  type Updateable,
} from "kysely";
import { Migrator, type MigrationProvider } from "kysely/migration";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import {
  MinnowDatabase,
  column,
  schema,
  table,
  view,
  type ExecuteResult,
  type MinnowSqlDriver,
} from "@minnowdb/core";
import type { MinnowDatabaseClient } from "@minnowdb/core/client";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { MinnowDialect } from "./dialect.js";
import { createKysely } from "./create-kysely.js";
import { jsonArrayFrom, jsonBuildObject, jsonObjectFrom } from "./helpers.js";
import type { InferKyselyDatabase, MinnowJsonValue } from "./schema.js";

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

const functionSchema = schema([
  table("events", {
    amount: column.number(),
    exact_amount: column.numeric({ precision: 12, scale: 2 }),
    happened_at: column.datetime(),
    calendar_day: column.date(),
    label: column.string().nullable(),
    payload: column.string(),
  }),
]);

const decodedSchema = schema([
  table("decoded_values", {
    id: column.integer().unique(),
    amount: column.numeric({ precision: 12, scale: 2 }),
    document: column.jsonb().nullable(),
  }),
]);

const generatedSchema = schema([
  table("generated_values", {
    id: column.integer().unique(),
    source: column.string(),
    derived: column.string().generatedSql("upper(source)"),
  }),
]);

const correlatedSchema = schema([
  table("aa", {
    aaKey: column.integer().unique(),
    bbKey: column.integer(),
  }),
  table("bb", {
    bbKey: column.integer().unique(),
    ok: column.boolean(),
  }),
  table("cc", {
    ccKey: column.integer().unique(),
    bbKey: column.integer(),
  }),
  table("widgets", {
    widgetKey: column.integer().unique(),
  }),
  table("keptKeys", {
    keptKey: column.integer().unique(),
  }),
  table("owners", {
    ownerKey: column.integer().unique(),
    name: column.string(),
  }),
  table("pets", {
    petKey: column.integer().unique(),
    ownerKey: column.integer(),
    petName: column.string(),
  }),
]);

type GeneratedDatabase = InferKyselyDatabase<typeof generatedSchema>;

type FunctionDatabase = InferKyselyDatabase<typeof functionSchema>;

function invalidDerivedWritesAreRejected(): void {
  const lineChange: Updateable<DeclaredDatabase["order_lines"]> = {};
  // @ts-expect-error composite primary-key columns are not updateable
  lineChange.order_id = 2;
  const viewInsert: Insertable<DeclaredDatabase["open_orders"]> = {};
  // @ts-expect-error a view column accepts no insert value
  viewInsert.id = 1;
  const generatedInsert: Insertable<GeneratedDatabase["generated_values"]> = {
    id: 1,
    source: "value",
  };
  // @ts-expect-error generated columns accept no inserted value
  generatedInsert.derived = "VALUE";
  const generatedUpdate: Updateable<GeneratedDatabase["generated_values"]> = { source: "next" };
  // @ts-expect-error generated columns accept no updated value
  generatedUpdate.derived = "NEXT";
}

function inferredDecodedResults(driver: MinnowSqlDriver): void {
  const db = createKysely({
    driver,
    schema: decodedSchema,
    resultDecoding: { numeric: "number", json: "parse" },
  });
  const query = db.selectFrom("decoded_values").selectAll();
  expectTypeOf<Awaited<ReturnType<typeof query.execute>>>().toEqualTypeOf<
    Array<{ id: number; amount: number; document: MinnowJsonValue | null }>
  >();
  void db;
  void query;
}

function portableKyselyCountRemainsPortable(db: Kysely<TestDatabase>): void {
  const counted = db.selectFrom("person").select((builder) => builder.fn.countAll().as("count"));
  expectTypeOf<Awaited<ReturnType<typeof counted.execute>>>().toEqualTypeOf<
    Array<{ count: number | string | bigint }>
  >();
  const explicit = db
    .selectFrom("person")
    .select((builder) => builder.fn.countAll<number>().as("count"));
  expectTypeOf<Awaited<ReturnType<typeof explicit.execute>>>().toEqualTypeOf<
    Array<{ count: number }>
  >();
  void counted;
  void explicit;
}

function portableKyselyFunctionsRemainPortable(db: Kysely<TestDatabase>): void {
  const query = db
    .selectFrom("person")
    .select((builder) => [
      builder.fn.sum("score").as("sum"),
      builder.fn.avg("score").as("average"),
      builder.fn("round", ["score"]).as("rounded"),
      builder.fn("coalesce", ["score", builder.val(0)]).as("coalesced"),
      builder.cast("score", "integer").as("cast_score"),
    ]);
  expectTypeOf<Awaited<ReturnType<typeof query.execute>>>().toEqualTypeOf<
    Array<{
      sum: number | string | bigint;
      average: number | string;
      rounded: unknown;
      coalesced: unknown;
      cast_score: unknown;
    }>
  >();
  void query;
}

function inferredMinnowFunctions(db: Kysely<FunctionDatabase>): void {
  const query = db
    .selectFrom("events")
    .select((builder) => [
      builder.fn.sum("amount").as("sum"),
      builder.fn.avg("amount").as("average"),
      builder.fn.min("amount").as("minimum"),
      builder.fn.max("amount").as("maximum"),
      builder.fn.sum("exact_amount").as("exact_sum"),
      builder.fn.avg("exact_amount").as("exact_average"),
      builder
        .fn("round", [
          builder.fn.coalesce(builder.fn.sum("amount"), builder.val(0)),
          builder.val(2),
        ])
        .as("rounded"),
      builder.fn("date_trunc", [builder.val("month"), "happened_at"]).as("month"),
      builder.fn("date_trunc", [builder.val("month"), "calendar_day"]).as("calendar_month"),
      builder.fn("current_date").as("today"),
      builder.fn("upper", ["label"]).as("upper_label"),
      builder.fn("json_value", ["payload", builder.val("$.name")]).as("json_name"),
      builder.fn("coalesce", ["label", builder.val("unknown")]).as("coalesced_label"),
      builder.fn("nullif", ["amount", builder.val(0)]).as("nullif_amount"),
      builder.fn("greatest", ["amount", builder.val(0)]).as("greatest_amount"),
      builder.fn("least", ["label", builder.val("zzzz")]).as("least_label"),
      builder.fn.agg("count", ["amount"]).as("aggregate_count"),
      builder.fn.agg("sum", ["exact_amount"]).as("aggregate_sum"),
      builder.cast("amount", "text").as("text_amount"),
      builder.cast("amount", "numeric(12, 2)").as("numeric_amount"),
      builder.cast("happened_at", "date").as("date_amount"),
      builder.cast("label", "text").as("nullable_text"),
    ]);
  expectTypeOf<Awaited<ReturnType<typeof query.execute>>>().toEqualTypeOf<
    Array<{
      sum: number | null;
      average: number | null;
      minimum: number | null;
      maximum: number | null;
      exact_sum: string | null;
      exact_average: string | null;
      rounded: number;
      month: Date;
      calendar_month: Date;
      today: string;
      upper_label: string | null;
      json_name: string | null;
      coalesced_label: string;
      nullif_amount: number | null;
      greatest_amount: number;
      least_label: string;
      aggregate_count: number;
      aggregate_sum: string | null;
      text_amount: string;
      numeric_amount: string;
      date_amount: string;
      nullable_text: string | null;
    }>
  >();

  const explicit = db
    .selectFrom("events")
    .select((builder) => builder.fn.sum<number>("exact_amount").as("sum"));
  expectTypeOf<Awaited<ReturnType<typeof explicit.execute>>>().toEqualTypeOf<
    Array<{ sum: number }>
  >();
  void query;
  void explicit;
}

describe("typed correlated subqueries", () => {
  it("compiles and executes nested EXISTS and DELETE subquery predicates", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(correlatedSchema);
    const db = createKysely({ driver: database, schema: correlatedSchema });

    await db
      .insertInto("aa")
      .values([
        { aaKey: 1, bbKey: 10 },
        { aaKey: 2, bbKey: 20 },
      ])
      .execute();
    await db
      .insertInto("bb")
      .values([
        { bbKey: 10, ok: true },
        { bbKey: 20, ok: false },
      ])
      .execute();
    await db.insertInto("cc").values({ ccKey: 100, bbKey: 10 }).execute();

    const nestedExists = db
      .selectFrom("aa")
      .select("aa.aaKey")
      .where((outer) =>
        outer.or([
          outer("aa.aaKey", "=", 0),
          outer.exists(
            outer
              .selectFrom("bb")
              .select("bb.bbKey")
              .whereRef("bb.bbKey", "=", "aa.bbKey")
              .where("bb.ok", "=", true),
          ),
          outer.exists(
            outer
              .selectFrom("cc")
              .select("cc.ccKey")
              .whereRef("cc.bbKey", "=", "aa.bbKey")
              .where((middle) =>
                middle.exists(
                  middle
                    .selectFrom("bb")
                    .select("bb.bbKey")
                    .whereRef("bb.bbKey", "=", "cc.bbKey")
                    .where("bb.ok", "=", true),
                ),
              ),
          ),
        ]),
      );
    expectTypeOf<Awaited<ReturnType<typeof nestedExists.execute>>>().toEqualTypeOf<
      Array<{ aaKey: number }>
    >();
    expect(nestedExists.compile()).toMatchObject({
      sql: 'select "aa"."aaKey" from "aa" where ("aa"."aaKey" = $1 or exists (select "bb"."bbKey" from "bb" where "bb"."bbKey" = "aa"."bbKey" and "bb"."ok" = $2) or exists (select "cc"."ccKey" from "cc" where "cc"."bbKey" = "aa"."bbKey" and exists (select "bb"."bbKey" from "bb" where "bb"."bbKey" = "cc"."bbKey" and "bb"."ok" = $3)))',
      parameters: [0, true, true],
    });
    expect(await nestedExists.execute()).toEqual([{ aaKey: 1 }]);

    const nestedNotIn = db
      .selectFrom("aa")
      .select("aa.aaKey")
      .where((outer) =>
        outer.or([
          outer("aa.aaKey", "=", 1),
          outer(
            "aa.bbKey",
            "not in",
            outer
              .selectFrom("bb")
              .select("bb.bbKey")
              .whereRef("bb.bbKey", "=", "aa.bbKey")
              .where("bb.ok", "=", true),
          ),
        ]),
      );
    expectTypeOf<Awaited<ReturnType<typeof nestedNotIn.execute>>>().toEqualTypeOf<
      Array<{ aaKey: number }>
    >();
    expect(await nestedNotIn.execute()).toEqual([{ aaKey: 1 }, { aaKey: 2 }]);

    const nestedAny = db
      .selectFrom("aa")
      .select("aa.aaKey")
      .where((outer) =>
        outer.or([
          outer("aa.aaKey", "=", 2),
          outer(
            "aa.bbKey",
            "=",
            outer.fn.any(
              outer
                .selectFrom("bb")
                .select("bb.bbKey")
                .whereRef("bb.bbKey", "=", "aa.bbKey")
                .where("bb.ok", "=", true),
            ),
          ),
        ]),
      );
    expectTypeOf<Awaited<ReturnType<typeof nestedAny.execute>>>().toEqualTypeOf<
      Array<{ aaKey: number }>
    >();
    expect(await nestedAny.execute()).toEqual([{ aaKey: 1 }, { aaKey: 2 }]);

    await db
      .insertInto("widgets")
      .values([{ widgetKey: 1 }, { widgetKey: 2 }])
      .execute();
    await db.insertInto("keptKeys").values({ keptKey: 1 }).execute();

    const notIn = db
      .deleteFrom("widgets")
      .where("widgets.widgetKey", "not in", db.selectFrom("keptKeys").select("keptKey"))
      .returning("widgets.widgetKey");
    expectTypeOf<Awaited<ReturnType<typeof notIn.execute>>>().toEqualTypeOf<
      Array<{ widgetKey: number }>
    >();
    expect(notIn.compile()).toMatchObject({
      sql: 'delete from "widgets" where "widgets"."widgetKey" not in (select "keptKey" from "keptKeys") returning "widgets"."widgetKey"',
      parameters: [],
    });
    expect(await notIn.execute()).toEqual([{ widgetKey: 2 }]);

    await db.insertInto("widgets").values({ widgetKey: 2 }).execute();
    const notExists = db
      .deleteFrom("widgets")
      .where((outer) =>
        outer.not(
          outer.exists(
            outer
              .selectFrom("keptKeys")
              .select("keptKeys.keptKey")
              .whereRef("keptKeys.keptKey", "=", "widgets.widgetKey"),
          ),
        ),
      )
      .returning("widgets.widgetKey");
    expectTypeOf<Awaited<ReturnType<typeof notExists.execute>>>().toEqualTypeOf<
      Array<{ widgetKey: number }>
    >();
    expect(await notExists.execute()).toEqual([{ widgetKey: 2 }]);

    await db.destroy();
    store.close();
  });
});

describe("JSON projection helpers", () => {
  it("builds fully typed objects, arrays, and nullable objects with per-parent ordering", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(correlatedSchema);
    const db = createKysely({
      driver: database,
      schema: correlatedSchema,
      resultDecoding: { json: "parse" },
    });

    await db
      .insertInto("owners")
      .values([
        { ownerKey: 1, name: "Alice" },
        { ownerKey: 2, name: "Bob" },
        { ownerKey: 3, name: "Cara" },
      ])
      .execute();
    await db
      .insertInto("pets")
      .values([
        { petKey: 10, ownerKey: 1, petName: "Rex" },
        { petKey: 11, ownerKey: 1, petName: "Milo" },
        { petKey: 12, ownerKey: 2, petName: "Zed" },
      ])
      .execute();

    const projected = db
      .selectFrom("owners")
      .select((outer) => [
        "owners.ownerKey",
        jsonBuildObject({
          id: outer.ref("owners.ownerKey"),
          name: outer.ref("owners.name"),
        }).as("owner"),
        jsonArrayFrom(
          outer
            .selectFrom("pets")
            .select((pet) => [
              "pets.petKey as pet_id",
              jsonBuildObject({ name: pet.ref("pets.petName") }).as("profile"),
            ])
            .whereRef("pets.ownerKey", "=", "owners.ownerKey")
            .orderBy("pets.petName")
            .limit(2),
        ).as("pets"),
        jsonArrayFrom(
          outer
            .selectFrom("pets")
            .select("pets.petName")
            .whereRef("pets.ownerKey", "=", "owners.ownerKey")
            .orderBy("pets.petName"),
        ).as("pet_names"),
        jsonObjectFrom(
          outer
            .selectFrom("pets")
            .select(["pets.petKey as pet_id", "pets.petName as name"])
            .whereRef("pets.ownerKey", "=", "owners.ownerKey")
            .orderBy("pets.petName")
            .limit(1),
        ).as("first_pet"),
      ])
      .orderBy("owners.ownerKey");

    expectTypeOf<Awaited<ReturnType<typeof projected.execute>>>().toEqualTypeOf<
      Array<{
        ownerKey: number;
        owner: { id: number; name: string };
        pets: Array<{ pet_id: number; profile: { name: string } }>;
        pet_names: Array<{ petName: string }>;
        first_pet: { pet_id: number; name: string } | null;
      }>
    >();
    const compiled = projected.compile();
    expect(compiled.sql).toContain("JSON_OBJECT(");
    expect(compiled.sql).toContain("JSON_ARRAYAGG(");
    expect(compiled.sql).not.toContain("json_build_object");
    expect(compiled.sql).not.toContain("json_agg");
    expect(await projected.execute()).toEqual([
      {
        ownerKey: 1,
        owner: { id: 1, name: "Alice" },
        pets: [
          { pet_id: 11, profile: { name: "Milo" } },
          { pet_id: 10, profile: { name: "Rex" } },
        ],
        pet_names: [{ petName: "Milo" }, { petName: "Rex" }],
        first_pet: { pet_id: 11, name: "Milo" },
      },
      {
        ownerKey: 2,
        owner: { id: 2, name: "Bob" },
        pets: [{ pet_id: 12, profile: { name: "Zed" } }],
        pet_names: [{ petName: "Zed" }],
        first_pet: { pet_id: 12, name: "Zed" },
      },
      {
        ownerKey: 3,
        owner: { id: 3, name: "Cara" },
        pets: [],
        pet_names: [],
        first_pet: null,
      },
    ]);

    await db.destroy();
    store.close();
  });

  it("requires explicit output names for row-to-object helpers", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(correlatedSchema);
    const db = createKysely({ driver: database, schema: correlatedSchema });

    expect(() =>
      db
        .selectFrom("owners")
        .select((outer) => jsonArrayFrom(outer.selectFrom("pets").selectAll()).as("pets")),
    ).toThrow("require explicit selections; selectAll() is not supported");
    expect(() => jsonArrayFrom(sql<number>`1`)).toThrow("require a select query");
    expect(() =>
      jsonBuildObject({ missing: undefined } as unknown as Record<string, Expression<unknown>>),
    ).toThrow("Missing JSON object expression: missing");

    const columnSelection: Expression<{ petName: string }> = {
      get expressionType() {
        return undefined;
      },
      toOperationNode: () =>
        SelectQueryNode.cloneWithSelections(
          SelectQueryNode.createFrom([TableNode.create("pets")]),
          [SelectionNode.create(ColumnNode.create("petName"))],
        ),
    };
    expect(jsonObjectFrom(columnSelection).toOperationNode().kind).toBe("RawNode");

    await db.destroy();
    store.close();
  });
});

describe("schema-derived Kysely types", () => {
  it("derives select, insert, and update shapes without a second DB interface", () => {
    expect(invalidDerivedWritesAreRejected).toBeTypeOf("function");
    expect(inferredDecodedResults).toBeTypeOf("function");
    expect(inferredMinnowFunctions).toBeTypeOf("function");
    expect(portableKyselyFunctionsRemainPortable).toBeTypeOf("function");
    expect(generatedSchema.tables[0]?.columns.derived.isGenerated).toBe(true);
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
    expect(await predicate.execute()).toEqual([{ id: 1, total: "12.50" }]);
    expect(await db.selectFrom("orders").selectAll().execute()).toEqual([
      { id: 1, total: "12.50", status: "open", note: null },
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
      total: "0.00",
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
      { id: 3, total: "0.00", status: "open" },
      { id: 4, total: "0.00", status: "open" },
    ]);
    await db.destroy();
    store.close();
  });

  it("infers Minnow COUNT results without a generic", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(declaredSchema);
    const db = createKysely({ driver: database, schema: declaredSchema });
    expect(portableKyselyCountRemainsPortable).toBeTypeOf("function");

    await db
      .insertInto("orders")
      .values([{ note: "kept" }, { note: null }])
      .execute();
    const counted = db
      .selectFrom("orders")
      .select((builder) => [
        builder.fn.countAll().as("orders"),
        builder.fn.count("note").as("notes"),
      ]);
    expectTypeOf<Awaited<ReturnType<typeof counted.execute>>>().toEqualTypeOf<
      Array<{ orders: number; notes: number }>
    >();
    expect(await counted.executeTakeFirstOrThrow()).toEqual({ orders: 2, notes: 1 });

    const derived = db
      .selectFrom(db.selectFrom("orders").select("id").as("selected_orders"))
      .select((builder) => builder.fn.countAll().as("orders"));
    expectTypeOf<Awaited<ReturnType<typeof derived.execute>>>().toEqualTypeOf<
      Array<{ orders: number }>
    >();
    expect(await derived.executeTakeFirstOrThrow()).toEqual({ orders: 2 });

    const explicit = db
      .selectFrom("orders")
      .select((builder) => builder.fn.countAll<string>().as("orders"));
    expectTypeOf<Awaited<ReturnType<typeof explicit.execute>>>().toEqualTypeOf<
      Array<{ orders: string }>
    >();
    void explicit;

    await db.destroy();
    store.close();
  });

  it("infers Minnow aggregate and scalar function results without generics", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(functionSchema);
    const db = createKysely({ driver: database, schema: functionSchema });
    const january = new Date("2026-01-19T14:23:00.000Z");

    await db
      .insertInto("events")
      .values([
        {
          amount: 2,
          exact_amount: 1.25,
          happened_at: january,
          calendar_day: "2026-01-19",
          label: "minnow",
          payload: '{"name":"Minnow"}',
        },
        {
          amount: 4,
          exact_amount: 2.75,
          happened_at: new Date("2026-02-01T00:00:00.000Z"),
          calendar_day: "2026-02-01",
          label: null,
          payload: "{}",
        },
      ])
      .execute();

    const aggregate = db
      .selectFrom("events")
      .select((builder) => [
        builder.fn.sum("amount").as("sum"),
        builder.fn.avg("amount").as("average"),
        builder.fn.min("amount").as("minimum"),
        builder.fn.max("amount").as("maximum"),
        builder.fn.sum("exact_amount").as("exact_sum"),
        builder.fn.avg("exact_amount").as("exact_average"),
        builder
          .fn("round", [
            builder.fn.coalesce(builder.fn.sum("amount"), builder.val(0)),
            builder.val(2),
          ])
          .as("rounded"),
        builder.fn.agg("count", ["amount"]).as("aggregate_count"),
      ]);
    expectTypeOf<Awaited<ReturnType<typeof aggregate.execute>>>().toEqualTypeOf<
      Array<{
        sum: number | null;
        average: number | null;
        minimum: number | null;
        maximum: number | null;
        exact_sum: string | null;
        exact_average: string | null;
        rounded: number;
        aggregate_count: number;
      }>
    >();
    expect(await aggregate.executeTakeFirstOrThrow()).toEqual({
      sum: 6,
      average: 3,
      minimum: 2,
      maximum: 4,
      exact_sum: "4.00",
      exact_average: "2.00",
      rounded: 6,
      aggregate_count: 2,
    });
    expect(await aggregate.where("amount", ">", 100).executeTakeFirstOrThrow()).toEqual({
      sum: null,
      average: null,
      minimum: null,
      maximum: null,
      exact_sum: null,
      exact_average: null,
      rounded: 0,
      aggregate_count: 0,
    });

    const derived = db
      .selectFrom(db.selectFrom("events").select("exact_amount").as("selected_events"))
      .select((builder) => builder.fn.sum("exact_amount").as("exact_sum"));
    expectTypeOf<Awaited<ReturnType<typeof derived.execute>>>().toEqualTypeOf<
      Array<{ exact_sum: string | null }>
    >();
    expect(await derived.executeTakeFirstOrThrow()).toEqual({ exact_sum: "4.00" });

    const scalar = db
      .selectFrom("events")
      .select((builder) => [
        builder.fn("date_trunc", [builder.val("month"), "happened_at"]).as("month"),
        builder.fn("date_trunc", [builder.val("month"), "calendar_day"]).as("calendar_month"),
        builder.fn("upper", ["label"]).as("upper_label"),
        builder.fn("json_value", ["payload", builder.val("$.name")]).as("json_name"),
        builder.fn("coalesce", ["label", builder.val("unknown")]).as("coalesced_label"),
        builder.fn("nullif", ["amount", builder.val(0)]).as("nullif_amount"),
        builder.fn("greatest", ["amount", builder.val(0)]).as("greatest_amount"),
        builder.fn("least", ["label", builder.val("zzzz")]).as("least_label"),
        builder.cast("amount", "text").as("text_amount"),
        builder.cast("amount", "numeric").as("numeric_amount"),
        builder.cast("happened_at", "date").as("date_amount"),
      ])
      .where("amount", "=", 2);
    expectTypeOf<Awaited<ReturnType<typeof scalar.execute>>>().toEqualTypeOf<
      Array<{
        month: Date;
        calendar_month: Date;
        upper_label: string | null;
        json_name: string | null;
        coalesced_label: string;
        nullif_amount: number | null;
        greatest_amount: number;
        least_label: string;
        text_amount: string;
        numeric_amount: string;
        date_amount: string;
      }>
    >();
    expect(await scalar.executeTakeFirstOrThrow()).toEqual({
      month: new Date("2026-01-01T00:00:00.000Z"),
      calendar_month: new Date("2026-01-01T00:00:00.000Z"),
      upper_label: "MINNOW",
      json_name: "Minnow",
      coalesced_label: "minnow",
      nullif_amount: 2,
      greatest_amount: 2,
      least_label: "minnow",
      text_amount: "2",
      numeric_amount: "2",
      date_amount: "2026-01-19",
    });

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

  it("compiles Kysely's JSON references to the -> and ->> operators and executes them", async () => {
    interface JsonDatabase {
      profiles: {
        id: number;
        document: { name: string; tags: string[]; meta: { score: number } } | null;
      };
    }
    await database.execute("CREATE TABLE profiles (id INTEGER PRIMARY KEY, document JSON)");
    await database.execute(
      "INSERT INTO profiles VALUES " +
        '(1, \'{"name":"Ada","tags":["x","y"],"meta":{"score":9}}\'), ' +
        "(2, NULL)",
    );
    const json = new Kysely<JsonDatabase>({
      dialect: new MinnowDialect({ driver: database }),
    });

    const compiled = json
      .selectFrom("profiles")
      .select((eb) => eb.ref("document", "->>").key("name").as("name"))
      .compile();
    expect(compiled.sql).toBe('select "document"->>\'name\' as "name" from "profiles"');

    // Chained access: every step but the last stays JSON; the ref's operator ends the chain.
    expect(
      await json
        .selectFrom("profiles")
        .select((eb) => [
          eb.ref("document", "->>").key("name").as("name"),
          eb.ref("document", "->").key("tags").as("tags"),
          eb.ref("document", "->>").key("tags").at(0).as("first_tag"),
          eb.ref("document", "->>").key("tags").at(-1).as("last_tag"),
          eb.ref("document", "->>").key("meta").key("score").as("score"),
        ])
        .orderBy("id")
        .execute(),
    ).toEqual([
      { name: "Ada", tags: '["x","y"]', first_tag: "x", last_tag: "y", score: "9" },
      { name: null, tags: null, first_tag: null, last_tag: null, score: null },
    ]);

    // The operators work in predicates, with parameters bound by Kysely.
    expect(
      await json
        .selectFrom("profiles")
        .select("id")
        .where((eb) => eb(eb.ref("document", "->>").key("name"), "=", "Ada"))
        .execute(),
    ).toEqual([{ id: 1 }]);
  });

  it("optionally decodes NUMERIC, JSON, and JSONB results in buffered and streamed reads", async () => {
    interface NativeDomains {
      domain_values: {
        id: number;
        amount: number | null;
        document: { name: string } | null;
        details: string[] | null;
      };
    }
    interface LosslessDomains {
      domain_values: {
        id: number;
        amount: string | null;
        document: string | null;
        details: string | null;
      };
    }
    await database.execute(
      "CREATE TABLE domain_values (id INTEGER PRIMARY KEY, amount NUMERIC(12, 2), document JSON, details JSONB)",
    );
    await database.execute(
      "INSERT INTO domain_values VALUES " +
        '(1, 12.50, \'{"name":"Ada"}\', \'["compiler"]\'), ' +
        '(2, 9.25, \'{"name":"Grace"}\', \'["cobol","navy"]\'), ' +
        "(3, NULL, NULL, NULL)",
    );

    const lossless = new Kysely<LosslessDomains>({
      dialect: new MinnowDialect({ driver: database }),
    });
    expect(await lossless.selectFrom("domain_values").selectAll().orderBy("id").execute()).toEqual([
      { id: 1, amount: "12.50", document: '{"name":"Ada"}', details: '["compiler"]' },
      {
        id: 2,
        amount: "9.25",
        document: '{"name":"Grace"}',
        details: '["cobol","navy"]',
      },
      { id: 3, amount: null, document: null, details: null },
    ]);

    const native = new Kysely<NativeDomains>({
      dialect: new MinnowDialect({
        driver: database,
        resultDecoding: { numeric: "number", json: "parse" },
      }),
    });
    expect(await native.selectFrom("domain_values").selectAll().orderBy("id").execute()).toEqual([
      { id: 1, amount: 12.5, document: { name: "Ada" }, details: ["compiler"] },
      { id: 2, amount: 9.25, document: { name: "Grace" }, details: ["cobol", "navy"] },
      { id: 3, amount: null, document: null, details: null },
    ]);
    const streamed = [];
    for await (const row of native
      .selectFrom("domain_values")
      .selectAll()
      .orderBy("id")
      .stream(1)) {
      streamed.push(row);
    }
    expect(streamed).toEqual([
      { id: 1, amount: 12.5, document: { name: "Ada" }, details: ["compiler"] },
      { id: 2, amount: 9.25, document: { name: "Grace" }, details: ["cobol", "navy"] },
      { id: 3, amount: null, document: null, details: null },
    ]);
    expect(
      await native
        .deleteFrom("domain_values")
        .where("id", "=", 1)
        .returningAll()
        .executeTakeFirstOrThrow(),
    ).toEqual({ id: 1, amount: 12.5, document: { name: "Ada" }, details: ["compiler"] });

    let includeDomain = false;
    const compatibilityDriver: MinnowSqlDriver = {
      query: database.query.bind(database),
      queryCursor: database.queryCursor.bind(database),
      introspect: database.introspect.bind(database),
      execute: async (): Promise<ExecuteResult> => ({
        kind: "insert",
        table: "domain_values",
        rowCount: 1,
        returnedRows: [{ amount: includeDomain ? "1e400" : "2" }],
        returnedColumns: ["amount"],
        ...(includeDomain ? { returnedColumnDomains: [{ kind: "numeric" }] } : {}),
      }),
    };
    const compatibility = new Kysely<NativeDomains>({
      dialect: new MinnowDialect({
        driver: compatibilityDriver,
        resultDecoding: { numeric: "number" },
      }),
    });
    expect((await sql<{ amount: unknown }>`SELECT 1`.execute(compatibility)).rows).toEqual([
      { amount: "2" },
    ]);
    includeDomain = true;
    await expect(sql<{ amount: number }>`SELECT 1`.execute(compatibility)).rejects.toThrow(
      "NUMERIC result cannot be represented as a finite number: 1e400",
    );
    await compatibility.destroy();
    await native.destroy();
    await lossless.destroy();
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

  it("rejects values Minnow cannot represent and streams typed rows", async () => {
    const bigintQuery = sql`SELECT ${1n} AS value`.compile(db);
    await expect(db.executeQuery(bigintQuery)).rejects.toThrow("unsupported type bigint");
    await db
      .insertInto("person")
      .values([
        { id: 1, name: "Ada", score: 8 },
        { id: 2, name: "Grace", score: 9 },
        { id: 3, name: "Katherine", score: 10 },
      ])
      .execute();
    const rows: Array<{ id: number; name: string }> = [];
    for await (const row of db.selectFrom("person").select(["id", "name"]).stream(2)) {
      rows.push(row);
    }
    expect(rows).toEqual([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
      { id: 3, name: "Katherine" },
    ]);
  });

  it("does not take ownership of the underlying database", async () => {
    await db.destroy();
    expect((await database.query("SELECT COUNT(*) AS count FROM person")).rows).toEqual([
      { count: 0 },
    ]);
  });
});

describe("compile-time refusals for unsupported PostgreSQL forms", () => {
  // Kysely can build forms Minnow's engine refuses with a bare parse error ("Expected eof,
  // found from"). The compiler knows which builder produced each node, so it refuses these
  // before execution with the feature named and an alternative offered.
  interface ProbeDatabase {
    person: { id: number; name: string; doc: { a: number } | null };
  }
  const db = new Kysely<ProbeDatabase>({
    dialect: new MinnowDialect({ driver: new MinnowDatabase(new MemoryBlockStore()) }),
  });

  it("names each unsupported query form instead of the engine's parse error", () => {
    expect(() => db.selectFrom("person").distinctOn("name").selectAll().compile()).toThrow(
      "Minnow does not support DISTINCT ON",
    );
    expect(() => db.selectFrom("person").selectAll().forUpdate().compile()).toThrow(
      "row-locking clauses",
    );
    expect(() =>
      db.updateTable("person").from("person as source").set({ name: "renamed" }).compile(),
    ).toThrow("Minnow does not support UPDATE ... FROM");
    expect(() => db.deleteFrom("person").using("person as source").compile()).toThrow(
      "Minnow does not support DELETE ... USING",
    );
    expect(() =>
      db
        .insertInto("person")
        .values({ id: 1, name: "n", doc: null })
        .onConflict((conflict) => conflict.constraint("person_pkey").doNothing())
        .compile(),
    ).toThrow("Name the unique key's columns");
    expect(() => db.selectFrom("person").selectAll().where("id", "in", []).compile()).toThrow(
      "HandleEmptyInListsPlugin",
    );
    expect(() => db.withSchema("main").selectFrom("person").selectAll().compile()).toThrow(
      "Minnow has no schemas",
    );
    expect(() => db.schema.alterTable("person").renameTo("people").compile()).toThrow("RENAME TO");
    expect(() => db.schema.alterTable("person").renameColumn("name", "title").compile()).toThrow(
      "RENAME COLUMN",
    );
  });

  it("keeps the supported neighbors compiling", () => {
    expect(() =>
      db
        .insertInto("person")
        .values({ id: 1, name: "n", doc: null })
        .onConflict((conflict) => conflict.column("id").doNothing())
        .compile(),
    ).not.toThrow();
    expect(() => db.selectFrom("person").distinct().selectAll().compile()).not.toThrow();
    expect(() =>
      db.selectFrom("person").selectAll().where("id", "in", [1, 2]).compile(),
    ).not.toThrow();
    expect(() => db.schema.alterTable("person").addColumn("extra", "text").compile()).not.toThrow();
    // Kysely's own remedy for empty lists resolves them before this compiler runs.
    expect(() =>
      db
        .withPlugin(new HandleEmptyInListsPlugin({ strategy: replaceWithNoncontingentExpression }))
        .selectFrom("person")
        .selectAll()
        .where("id", "in", [])
        .compile(),
    ).not.toThrow();
  });
});

describe("arithmetic scalar functions over exact NUMERIC columns", () => {
  it("rejects them at the type level instead of promising a number the engine refuses", () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const db = createKysely({ driver: database, schema: functionSchema });
    const query = db.selectFrom("events").select((eb) => [
      // A Float64 column stays a number.
      eb.fn("round", ["amount"]).as("rounded"),
      // An exact NUMERIC crosses the SQL boundary as a string; the engine refuses ROUND on it,
      // so the inferred result is the refusal, not an unreachable number.
      eb.fn("round", ["exact_amount"]).as("rejected"),
      eb.fn("abs", ["exact_amount"]).as("rejected_abs"),
      // A CAST to a float target is the documented path back to arithmetic.
      eb.fn("round", [eb.cast<number>("exact_amount", "double precision")]).as("cast_first"),
    ]);
    type Row = Awaited<ReturnType<typeof query.execute>>[number];
    expectTypeOf<Row["rounded"]>().toEqualTypeOf<number>();
    expectTypeOf<Row["cast_first"]>().toEqualTypeOf<number>();
    expectTypeOf<Row["rejected"]>().not.toExtend<number>();
    expectTypeOf<Row["rejected_abs"]>().not.toExtend<number>();

    // The refusal keys off the operand boundary, so numeric result decoding — which turns the
    // select type into number while the engine still sees the string boundary — cannot
    // reintroduce the unsound number.
    const decoded = createKysely({
      driver: database,
      schema: functionSchema,
      resultDecoding: { numeric: "number" },
    });
    const decodedQuery = decoded
      .selectFrom("events")
      .select((eb) => [eb.fn("round", ["exact_amount"]).as("rejected")]);
    type DecodedRow = Awaited<ReturnType<typeof decodedQuery.execute>>[number];
    expectTypeOf<DecodedRow["rejected"]>().not.toExtend<number>();
    void query;
    void decodedQuery;
  });
});
