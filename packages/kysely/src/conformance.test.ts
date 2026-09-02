import { MinnowDatabase, column, schema, table } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { Kysely, sql, type ColumnType, type Generated } from "kysely";
import { beforeEach, afterEach, describe, expect, expectTypeOf, it } from "vitest";
import { createKysely } from "./create-kysely.js";
import type { InferKyselyDatabase } from "./schema.js";

/**
 * Full-surface conformance: every Kysely builder form the engine supports executes here with
 * asserted rows and inferred types, and every form the engine refuses fails at compile time
 * with the feature named. dialect.test.ts owns the driver/transaction/decoding behaviors.
 */
const conformanceSchema = schema([
  table("users", {
    userId: column.integer().unique(),
    name: column.string(),
    age: column.number().nullable(),
    teamId: column.integer().nullable(),
  }),
  table("teams", {
    teamId: column.integer().unique(),
    teamName: column.string(),
  }),
  table("scores", {
    scoreId: column.integer().unique(),
    userId: column.integer(),
    points: column.number(),
  }),
  table("user_changes", {
    userId: column.integer().unique(),
    newName: column.string(),
    dropped: column.boolean(),
  }),
]);

type ConformanceDatabase = InferKyselyDatabase<typeof conformanceSchema>;

let store: MemoryBlockStore;
let database: MinnowDatabase;
let db: Kysely<ConformanceDatabase>;

beforeEach(async () => {
  store = new MemoryBlockStore();
  database = new MinnowDatabase(store);
  await database.migrate(conformanceSchema);
  db = createKysely({ driver: database, schema: conformanceSchema });
  await db
    .insertInto("teams")
    .values([
      { teamId: 1, teamName: "Red" },
      { teamId: 2, teamName: "Blue" },
      { teamId: 3, teamName: "Empty" },
    ])
    .execute();
  await db
    .insertInto("users")
    .values([
      { userId: 1, name: "Ada", age: 36, teamId: 1 },
      { userId: 2, name: "Grace", age: null, teamId: 1 },
      { userId: 3, name: "Katherine", age: 55, teamId: 2 },
      { userId: 4, name: "Solo", age: 20, teamId: null },
    ])
    .execute();
  await db
    .insertInto("scores")
    .values([
      { scoreId: 1, userId: 1, points: 10 },
      { scoreId: 2, userId: 1, points: 20 },
      { scoreId: 3, userId: 2, points: 30 },
      { scoreId: 4, userId: 3, points: 5 },
    ])
    .execute();
});

afterEach(async () => {
  await db.destroy();
  await database.close();
  store.close();
});

describe("joins", () => {
  it("names selectAll() outputs by their bare column names on joins", async () => {
    const aliased = db
      .selectFrom("users as u")
      .innerJoin("teams as t", "t.teamId", "u.teamId")
      .selectAll("u")
      .orderBy("u.userId");
    expectTypeOf<Awaited<ReturnType<typeof aliased.execute>>>().toEqualTypeOf<
      Array<{ userId: number; name: string; age: number | null; teamId: number | null }>
    >();
    expect(await aliased.execute()).toEqual([
      { userId: 1, name: "Ada", age: 36, teamId: 1 },
      { userId: 2, name: "Grace", age: null, teamId: 1 },
      { userId: 3, name: "Katherine", age: 55, teamId: 2 },
    ]);
    // A bare select * over the join keeps every name bare except the one both sides carry.
    const rows = await db
      .selectFrom("users")
      .innerJoin("teams", "teams.teamId", "users.teamId")
      .selectAll()
      .orderBy("users.userId")
      .execute();
    expect(rows[0]).toEqual({
      userId: 1,
      name: "Ada",
      age: 36,
      "users.teamId": 1,
      "teams.teamId": 1,
      teamName: "Red",
    });
  });

  it("executes inner, left, right, full, and cross joins with exact nullability", async () => {
    const inner = db
      .selectFrom("users")
      .innerJoin("teams", "users.teamId", "teams.teamId")
      .select(["users.name", "teams.teamName"])
      .orderBy("users.userId");
    expectTypeOf<Awaited<ReturnType<typeof inner.execute>>>().toEqualTypeOf<
      Array<{ name: string; teamName: string }>
    >();
    expect(await inner.execute()).toEqual([
      { name: "Ada", teamName: "Red" },
      { name: "Grace", teamName: "Red" },
      { name: "Katherine", teamName: "Blue" },
    ]);

    const left = db
      .selectFrom("users")
      .leftJoin("teams", "users.teamId", "teams.teamId")
      .select(["users.name", "teams.teamName"])
      .orderBy("users.userId");
    expectTypeOf<Awaited<ReturnType<typeof left.execute>>>().toEqualTypeOf<
      Array<{ name: string; teamName: string | null }>
    >();
    expect(await left.execute()).toEqual([
      { name: "Ada", teamName: "Red" },
      { name: "Grace", teamName: "Red" },
      { name: "Katherine", teamName: "Blue" },
      { name: "Solo", teamName: null },
    ]);

    const right = db
      .selectFrom("users")
      .rightJoin("teams", "users.teamId", "teams.teamId")
      .select(["users.name", "teams.teamName"])
      .orderBy("teams.teamId")
      .orderBy("users.userId");
    expectTypeOf<Awaited<ReturnType<typeof right.execute>>>().toEqualTypeOf<
      Array<{ name: string | null; teamName: string }>
    >();
    expect(await right.execute()).toEqual([
      { name: "Ada", teamName: "Red" },
      { name: "Grace", teamName: "Red" },
      { name: "Katherine", teamName: "Blue" },
      { name: null, teamName: "Empty" },
    ]);

    const full = db
      .selectFrom("users")
      .fullJoin("teams", "users.teamId", "teams.teamId")
      .select(["users.name", "teams.teamName"])
      .orderBy(sql`"users"."userId" IS NULL`)
      .orderBy("users.userId");
    expectTypeOf<Awaited<ReturnType<typeof full.execute>>>().toEqualTypeOf<
      Array<{ name: string | null; teamName: string | null }>
    >();
    expect(await full.execute()).toEqual([
      { name: "Ada", teamName: "Red" },
      { name: "Grace", teamName: "Red" },
      { name: "Katherine", teamName: "Blue" },
      { name: "Solo", teamName: null },
      { name: null, teamName: "Empty" },
    ]);

    const crossed = await db
      .selectFrom("users")
      .crossJoin("teams")
      .select(({ fn }) => fn.countAll().as("combinations"))
      .executeTakeFirstOrThrow();
    expect(crossed).toEqual({ combinations: 12 });
  });

  it("joins with multi-condition ON callbacks and expression conditions", async () => {
    const rows = await db
      .selectFrom("users")
      .innerJoin("scores", (join) =>
        join
          .onRef("scores.userId", "=", "users.userId")
          .on("scores.points", ">=", 10)
          .on((eb) => eb("users.name", "!=", "Katherine")),
      )
      .select(["users.name", "scores.points"])
      .orderBy("scores.scoreId")
      .execute();
    expect(rows).toEqual([
      { name: "Ada", points: 10 },
      { name: "Ada", points: 20 },
      { name: "Grace", points: 30 },
    ]);
  });

  it("executes inner, left, and cross lateral joins", async () => {
    const big = db
      .selectFrom("users")
      .innerJoinLateral(
        (eb) =>
          eb
            .selectFrom("scores")
            .select("scores.points")
            .whereRef("scores.userId", "=", "users.userId")
            .where("scores.points", ">=", 10)
            .as("big"),
        (join) => join.onTrue(),
      )
      .select(["users.name", "big.points"])
      .orderBy("users.userId")
      .orderBy("big.points");
    expectTypeOf<Awaited<ReturnType<typeof big.execute>>>().toEqualTypeOf<
      Array<{ name: string; points: number }>
    >();
    expect(await big.execute()).toEqual([
      { name: "Ada", points: 10 },
      { name: "Ada", points: 20 },
      { name: "Grace", points: 30 },
    ]);

    const withMissing = db
      .selectFrom("users")
      .leftJoinLateral(
        (eb) =>
          eb
            .selectFrom("scores")
            .select("scores.points")
            .whereRef("scores.userId", "=", "users.userId")
            .where("scores.points", ">=", 20)
            .as("big"),
        (join) => join.onTrue(),
      )
      .select(["users.name", "big.points"])
      .orderBy("users.userId")
      .orderBy("big.points");
    expectTypeOf<Awaited<ReturnType<typeof withMissing.execute>>>().toEqualTypeOf<
      Array<{ name: string; points: number | null }>
    >();
    expect(await withMissing.execute()).toEqual([
      { name: "Ada", points: 20 },
      { name: "Grace", points: 30 },
      { name: "Katherine", points: null },
      { name: "Solo", points: null },
    ]);

    expect(
      await db
        .selectFrom("teams")
        .crossJoinLateral((eb) =>
          eb
            .selectFrom("users")
            .select(({ fn }) => fn.countAll().as("members"))
            .as("counted"),
        )
        .select(["teams.teamName", "counted.members"])
        .orderBy("teams.teamId")
        .limit(1)
        .execute(),
    ).toEqual([{ teamName: "Red", members: 4 }]);
  });

  it("answers top-N-per-group through a correlated lateral ORDER BY/LIMIT subquery", async () => {
    // The engine ranks the lateral rows per outer row; users without scores drop out of the
    // inner join, and the best score per user comes back.
    const best = await db
      .selectFrom("users")
      .innerJoinLateral(
        (eb) =>
          eb
            .selectFrom("scores")
            .select("scores.points")
            .whereRef("scores.userId", "=", "users.userId")
            .orderBy("scores.points", "desc")
            .limit(1)
            .as("best"),
        (join) => join.onTrue(),
      )
      .select(["users.name", "best.points"])
      .orderBy("users.userId")
      .execute();
    expect(best).toEqual([
      { name: "Ada", points: 20 },
      { name: "Grace", points: 30 },
      { name: "Katherine", points: 5 },
    ]);
  });
});

describe("set operations", () => {
  it("executes union, unionAll, intersect, except and their ALL variants", async () => {
    const union = db
      .selectFrom("users")
      .select("name as label")
      .where("teamId", "=", 1)
      .union(db.selectFrom("teams").select("teamName as label"))
      .orderBy("label");
    expectTypeOf<Awaited<ReturnType<typeof union.execute>>>().toEqualTypeOf<
      Array<{ label: string }>
    >();
    expect(await union.execute()).toEqual([
      { label: "Ada" },
      { label: "Blue" },
      { label: "Empty" },
      { label: "Grace" },
      { label: "Red" },
    ]);

    const unionAll = await db
      .selectFrom("users")
      .select("teamId")
      .where("teamId", "is not", null)
      .unionAll(db.selectFrom("teams").select("teamId"))
      .orderBy("teamId")
      .execute();
    expect(unionAll.map((row) => row.teamId)).toEqual([1, 1, 1, 2, 2, 3]);

    expect(
      await db
        .selectFrom("users")
        .select("teamId")
        .intersect(db.selectFrom("teams").select("teamId"))
        .orderBy("teamId")
        .execute(),
    ).toEqual([{ teamId: 1 }, { teamId: 2 }]);

    expect(
      await db
        .selectFrom("teams")
        .select("teamId")
        .except(
          db
            .selectFrom("users")
            .select("teamId")
            .where("teamId", "is not", null)
            .$narrowType<{ teamId: number }>(),
        )
        .execute(),
    ).toEqual([{ teamId: 3 }]);

    expect(
      (
        await db
          .selectFrom("users")
          .select("teamId")
          .intersectAll(db.selectFrom("users").select("teamId"))
          .execute()
      ).length,
    ).toBe(4);

    expect(
      await db
        .selectFrom("users")
        .select("teamId")
        .where("teamId", "=", 1)
        .exceptAll(db.selectFrom("teams").select("teamId").where("teamId", "=", 1))
        .execute(),
    ).toEqual([{ teamId: 1 }]);
  });
});

describe("common table expressions", () => {
  it("chains CTEs, names column lists, and feeds mutations", async () => {
    const chained = db
      .with("team_sizes", (qb) =>
        qb
          .selectFrom("users")
          .select(["teamId", (eb) => eb.fn.countAll().as("members")])
          .where("teamId", "is not", null)
          .groupBy("teamId"),
      )
      .with("largest", (qb) =>
        qb
          .selectFrom("team_sizes")
          .select("teamId")
          .orderBy("members", "desc")
          .orderBy("teamId")
          .limit(1),
      )
      .selectFrom("largest")
      .innerJoin("teams", "teams.teamId", "largest.teamId")
      .select("teams.teamName");
    expectTypeOf<Awaited<ReturnType<typeof chained.execute>>>().toEqualTypeOf<
      Array<{ teamName: string }>
    >();
    expect(await chained.execute()).toEqual([{ teamName: "Red" }]);

    expect(
      await db
        .with("labels(label)", (qb) => qb.selectFrom("users").select("name as label"))
        .selectFrom("labels")
        .select("label")
        .orderBy("label")
        .limit(1)
        .execute(),
    ).toEqual([{ label: "Ada" }]);

    const removed = await db
      .with("stale", (qb) => qb.selectFrom("scores").select("scoreId").where("points", "<", 10))
      .deleteFrom("scores")
      .where("scoreId", "in", (eb) => eb.selectFrom("stale").select("scoreId"))
      .returning("scoreId")
      .execute();
    expect(removed).toEqual([{ scoreId: 4 }]);
  });

  it("executes recursive CTEs", async () => {
    const counter = db
      .withRecursive("nums(n)", (qb) =>
        qb.selectNoFrom(sql<number>`1`.as("n")).unionAll(
          qb
            .selectFrom("nums")
            .select(sql<number>`"n" + 1`.as("n"))
            .where(sql<number>`"n"`, "<", 5),
        ),
      )
      .selectFrom("nums")
      .selectAll();
    expect(await counter.execute()).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }]);
  });
});

describe("ordering and row limits", () => {
  it("orders with direction, null placement, collation, and expressions", async () => {
    const ordered = await db
      .selectFrom("users")
      .select("name")
      .orderBy("age", (ob) => ob.desc().nullsFirst())
      .execute();
    expect(ordered.map((row) => row.name)).toEqual(["Grace", "Katherine", "Ada", "Solo"]);

    const nullsLast = await db
      .selectFrom("users")
      .select("name")
      .orderBy("age", (ob) => ob.asc().nullsLast())
      .execute();
    expect(nullsLast.map((row) => row.name)).toEqual(["Solo", "Ada", "Katherine", "Grace"]);

    const collated = await db
      .selectFrom("users")
      .select("name")
      .orderBy("name", (ob) => ob.collate("C").desc())
      .limit(1)
      .execute();
    expect(collated).toEqual([{ name: "Solo" }]);

    const byExpression = await db
      .selectFrom("scores")
      .select("scoreId")
      .orderBy((eb) => eb("points", "%", 3))
      .orderBy("scoreId")
      .execute();
    expect(byExpression.map((row) => row.scoreId)).toEqual([3, 1, 2, 4]);
  });

  it("applies limit, offset, standalone offset, and FETCH FIRST WITH TIES", async () => {
    expect(
      await db.selectFrom("users").select("userId").orderBy("userId").limit(2).offset(1).execute(),
    ).toEqual([{ userId: 2 }, { userId: 3 }]);

    expect(
      await db.selectFrom("users").select("userId").orderBy("userId").offset(3).execute(),
    ).toEqual([{ userId: 4 }]);

    await db.insertInto("scores").values({ scoreId: 5, userId: 4, points: 30 }).execute();
    const tied = await db
      .selectFrom("scores")
      .select("points")
      .orderBy("points", "desc")
      .fetch(1, "with ties")
      .execute();
    expect(tied).toEqual([{ points: 30 }, { points: 30 }]);
  });
});

describe("expression toolkit", () => {
  it("evaluates searched and simple CASE expressions", async () => {
    const cased = db
      .selectFrom("users")
      .select((eb) => [
        "name",
        eb
          .case()
          .when("age", ">", 30)
          .then("senior")
          .when("age", "is", null)
          .then("unknown")
          .else("junior")
          .end()
          .as("bracket"),
        eb.case("teamId").when(1).then("red").else("other").end().as("color"),
      ])
      .orderBy("userId");
    // Kysely widens THEN/ELSE literals, so the branches infer as string, not a literal union.
    expectTypeOf<Awaited<ReturnType<typeof cased.execute>>>().toEqualTypeOf<
      Array<{ name: string; bracket: string; color: string }>
    >();
    expect(await cased.execute()).toEqual([
      { name: "Ada", bracket: "senior", color: "red" },
      { name: "Grace", bracket: "unknown", color: "red" },
      { name: "Katherine", bracket: "senior", color: "other" },
      { name: "Solo", bracket: "junior", color: "other" },
    ]);
  });

  it("evaluates predicates: between, is, in, distinctness, pattern matching", async () => {
    const found = await db
      .selectFrom("users")
      .select("name")
      .where((eb) =>
        eb.and([
          eb.between("age", 21, 60),
          eb("name", "is distinct from", "zzz"),
          eb("name", "not in", ["Nobody", "Missing"]),
          eb.parens(eb("name", "like", "A%").or("name", "ilike", "k%")),
        ]),
      )
      .orderBy("userId")
      .execute();
    expect(found).toEqual([{ name: "Ada" }, { name: "Katherine" }]);

    expect(await db.selectFrom("users").select("name").where("age", "is", null).execute()).toEqual([
      { name: "Grace" },
    ]);
    expect(
      (await db.selectFrom("users").select("name").where("age", "is not", null).execute()).length,
    ).toBe(3);

    expect(
      await db
        .selectFrom("users")
        .select("name")
        .where((eb) => eb.betweenSymmetric("age", 40, 30))
        .execute(),
    ).toEqual([{ name: "Ada" }]);

    expect(
      await db
        .selectFrom("users")
        .select("name")
        .where((eb) =>
          eb.not(
            eb.exists(
              eb
                .selectFrom("scores")
                .select("scoreId")
                .whereRef("scores.userId", "=", "users.userId"),
            ),
          ),
        )
        .execute(),
    ).toEqual([{ name: "Solo" }]);
  });

  it("compares row tuples", async () => {
    const rows = await db
      .selectFrom("users")
      .select("name")
      .where((eb) => eb(eb.refTuple("userId", "teamId"), "in", [eb.tuple(1, 1), eb.tuple(3, 2)]))
      .orderBy("userId")
      .execute();
    expect(rows).toEqual([{ name: "Ada" }, { name: "Katherine" }]);
  });

  it("computes arithmetic, concatenation, and unary negation", async () => {
    const computed = db
      .selectFrom("scores")
      .select((eb) => [
        eb("points", "+", 5).as("plus"),
        eb("points", "*", 2).as("doubled"),
        eb("points", "%", 3).as("modulo"),
        eb.neg(eb.ref("points")).as("negated"),
        eb(eb.cast<string>("points", "text"), "||", eb.val(" pts")).as("described"),
      ])
      .where("scoreId", "=", 1);
    expectTypeOf<Awaited<ReturnType<typeof computed.execute>>>().toEqualTypeOf<
      Array<{ plus: number; doubled: number; modulo: number; negated: number; described: string }>
    >();
    expect(await computed.executeTakeFirstOrThrow()).toEqual({
      plus: 15,
      doubled: 20,
      modulo: 1,
      negated: -10,
      described: "10 pts",
    });
  });

  it("selects without FROM and quantifies subqueries with any", async () => {
    expect(
      await db
        .selectNoFrom((eb) => [eb.val(1).as("one"), sql<string>`'two'`.as("two")])
        .executeTakeFirstOrThrow(),
    ).toEqual({ one: 1, two: "two" });

    expect(
      await db
        .selectFrom("users")
        .select("name")
        .where((eb) => eb("userId", "=", eb.fn.any(eb.selectFrom("scores").select("userId"))))
        .orderBy("userId")
        .execute(),
    ).toEqual([{ name: "Ada" }, { name: "Grace" }, { name: "Katherine" }]);
  });
});

describe("aggregates and window functions", () => {
  it("supports DISTINCT, FILTER, and ordered string aggregation", async () => {
    const aggregated = db.selectFrom("scores").select((eb) => [
      eb.fn.count("userId").distinct().as("scorers"),
      eb.fn.sum("points").filterWhere("points", ">=", 10).as("bigPoints"),
      eb.fn
        .agg<string>("string_agg", [eb.cast<string>("points", "text"), eb.val("|")])
        .orderBy("points", "desc")
        .as("ranking"),
    ]);
    expect(await aggregated.executeTakeFirstOrThrow()).toEqual({
      scorers: 3,
      bigPoints: 60,
      ranking: "30|20|10|5",
    });

    const filteredRef = await db
      .selectFrom("scores")
      .select((eb) =>
        eb.fn.count("scoreId").filterWhereRef("points", ">", "userId").as("aboveOwnId"),
      )
      .executeTakeFirstOrThrow();
    expect(filteredRef).toEqual({ aboveOwnId: 4 });
  });

  it("computes windowed aggregates and ranking functions", async () => {
    const windowed = db
      .selectFrom("scores")
      .select((eb) => [
        "scoreId",
        eb.fn
          .sum("points")
          .over((ob) => ob.partitionBy("userId").orderBy("scoreId"))
          .as("running"),
        eb.fn
          .agg<number>("row_number")
          .over((ob) => ob.orderBy("points", "desc"))
          .as("rowNo"),
        eb.fn
          .agg<number>("rank")
          .over((ob) => ob.orderBy("points", "desc"))
          .as("rank"),
        eb.fn
          .agg<number>("dense_rank")
          .over((ob) => ob.orderBy("points", "desc"))
          .as("dense"),
        eb.fn
          .agg<number>("ntile", [eb.lit(2)])
          .over((ob) => ob.orderBy("points"))
          .as("half"),
        eb.fn
          .agg<number | null>("lag", ["points"])
          .over((ob) => ob.orderBy("scoreId"))
          .as("previous"),
        eb.fn
          .agg<number | null>("lead", ["points"])
          .over((ob) => ob.orderBy("scoreId"))
          .as("next"),
        eb.fn
          .agg<number>("first_value", ["points"])
          .over((ob) => ob.partitionBy("userId").orderBy("scoreId"))
          .as("first"),
      ])
      .orderBy("scoreId");
    expect(await windowed.execute()).toEqual([
      {
        scoreId: 1,
        running: 10,
        rowNo: 3,
        rank: 3,
        dense: 3,
        half: 1,
        previous: null,
        next: 20,
        first: 10,
      },
      {
        scoreId: 2,
        running: 30,
        rowNo: 2,
        rank: 2,
        dense: 2,
        half: 2,
        previous: 10,
        next: 30,
        first: 10,
      },
      {
        scoreId: 3,
        running: 30,
        rowNo: 1,
        rank: 1,
        dense: 1,
        half: 2,
        previous: 20,
        next: 5,
        first: 30,
      },
      {
        scoreId: 4,
        running: 5,
        rowNo: 4,
        rank: 4,
        dense: 4,
        half: 1,
        previous: 30,
        next: null,
        first: 5,
      },
    ]);

    const statistics = await db
      .selectFrom("scores")
      .select((eb) => [
        eb.fn.agg<number | null>("var_pop", ["points"]).as("variance"),
        eb.fn.agg<number | null>("stddev_pop", ["points"]).as("deviation"),
        eb.fn.agg<boolean | null>("every", [eb("points", ">", 0)]).as("allPositive"),
        eb.fn.agg<number | null>("any_value", ["points"]).as("sample"),
      ])
      .executeTakeFirstOrThrow();
    expect(statistics.variance).toBeCloseTo(92.1875);
    expect(statistics.deviation).toBeCloseTo(Math.sqrt(92.1875));
    expect(statistics.allPositive).toBe(true);
    expect([5, 10, 20, 30]).toContain(statistics.sample);
  });
});

describe("mutations", () => {
  it("handles ON CONFLICT DO NOTHING and DO UPDATE SET with excluded values", async () => {
    const ignored = await db
      .insertInto("users")
      .values({ userId: 1, name: "Duplicate", age: 1, teamId: null })
      .onConflict((oc) => oc.column("userId").doNothing())
      .executeTakeFirstOrThrow();
    expect(ignored.numInsertedOrUpdatedRows).toBe(0n);
    expect(await db.selectFrom("users").select("name").where("userId", "=", 1).execute()).toEqual([
      { name: "Ada" },
    ]);

    const upserted = await db
      .insertInto("users")
      .values({ userId: 1, name: "Ada Lovelace", age: 37, teamId: 2 })
      .onConflict((oc) =>
        oc.column("userId").doUpdateSet((eb) => ({
          name: eb.ref("excluded.name"),
          age: eb.ref("excluded.age"),
        })),
      )
      .returning(["name", "age", "teamId"])
      .executeTakeFirstOrThrow();
    expect(upserted).toEqual({ name: "Ada Lovelace", age: 37, teamId: 1 });

    const guarded = await db
      .insertInto("users")
      .values({ userId: 4, name: "Solo Prime", age: 21, teamId: null })
      .onConflict((oc) =>
        oc
          .column("userId")
          .doUpdateSet((eb) => ({ name: eb.ref("excluded.name") }))
          .where("users.age", ">", 100),
      )
      .executeTakeFirstOrThrow();
    expect(guarded.numInsertedOrUpdatedRows).toBe(0n);
    expect(await db.selectFrom("users").select("name").where("userId", "=", 4).execute()).toEqual([
      { name: "Solo" },
    ]);
  });

  it("returns expressions from RETURNING with SELECT semantics", async () => {
    const updated = await db
      .updateTable("users")
      .set({ age: 40 })
      .where("userId", "=", 1)
      .returning((eb) => ["name", eb.lit(1).as("one"), sql<number>`"age" * 2`.as("doubleAge")])
      .executeTakeFirstOrThrow();
    expect(updated).toEqual({ name: "Ada", one: 1, doubleAge: 80 });
    const deleted = await db
      .deleteFrom("scores")
      .where("scoreId", "=", 2)
      .returning(sql<string>`'score-' || "points"`.as("label"))
      .executeTakeFirstOrThrow();
    expect(deleted).toEqual({ label: "score-20" });
  });

  it("updates through expressions and subquery predicates", async () => {
    const bumped = db
      .updateTable("scores")
      .set((eb) => ({ points: eb("points", "+", 100) }))
      .where("userId", "in", (eb) =>
        eb.selectFrom("users").select("userId").where("teamId", "=", 1),
      )
      .returningAll();
    expectTypeOf<Awaited<ReturnType<typeof bumped.execute>>>().toEqualTypeOf<
      Array<{ scoreId: number; userId: number; points: number }>
    >();
    expect(await bumped.execute()).toEqual([
      { scoreId: 1, userId: 1, points: 110 },
      { scoreId: 2, userId: 1, points: 120 },
      { scoreId: 3, userId: 2, points: 130 },
    ]);
  });

  it("deletes with correlated predicates and returns rows", async () => {
    const removed = await db
      .deleteFrom("scores")
      .where((eb) =>
        eb.exists(
          eb
            .selectFrom("users")
            .select("userId")
            .whereRef("users.userId", "=", "scores.userId")
            .where("users.teamId", "=", 2),
        ),
      )
      .returningAll()
      .execute();
    expect(removed).toEqual([{ scoreId: 4, userId: 3, points: 5 }]);
  });

  it("merges with condition-narrowed WHEN clauses and DELETE actions", async () => {
    await db
      .insertInto("user_changes")
      .values([
        { userId: 1, newName: "Ada Lovelace", dropped: false },
        { userId: 3, newName: "unused", dropped: true },
        { userId: 99, newName: "Fresh", dropped: false },
      ])
      .execute();
    const result = await db
      .mergeInto("users")
      .using("user_changes", "user_changes.userId", "users.userId")
      .whenMatchedAnd("user_changes.dropped", "=", true)
      .thenDelete()
      .whenMatched()
      .thenUpdateSet((eb) => ({ name: eb.ref("user_changes.newName") }))
      .whenNotMatched()
      .thenInsertValues((eb) => ({
        userId: eb.ref("user_changes.userId"),
        name: eb.ref("user_changes.newName"),
        age: null,
        teamId: null,
      }))
      .executeTakeFirstOrThrow();
    expect(result.numChangedRows).toBe(3n);
    expect(
      await db.selectFrom("users").select(["userId", "name"]).orderBy("userId").execute(),
    ).toEqual([
      { userId: 1, name: "Ada Lovelace" },
      { userId: 2, name: "Grace" },
      { userId: 4, name: "Solo" },
      { userId: 99, name: "Fresh" },
    ]);
  });
});

describe("compile-time refusals for engine-unsupported forms", () => {
  it("refuses SQL EXPLAIN with a pointer at the driver API", async () => {
    await expect(db.selectFrom("users").selectAll().explain()).rejects.toThrow(
      "Minnow does not run SQL EXPLAIN statements",
    );
  });

  it("refuses T-SQL TOP, OUTPUT, and APPLY forms with the PostgreSQL alternative", () => {
    expect(() => db.selectFrom("users").top(1).selectAll().compile()).toThrow(
      "Minnow does not support the T-SQL TOP clause",
    );
    expect(() =>
      db
        .selectFrom("users")
        .select((eb) => [
          "users.name",
          eb
            .selectFrom("scores")
            .select("points")
            .top(1)
            .whereRef("scores.userId", "=", "users.userId")
            .as("points"),
        ])
        .compile(),
    ).toThrow("Minnow does not support the T-SQL TOP clause");
    expect(() =>
      db
        .insertInto("teams")
        .values({ teamId: 9, teamName: "Out" })
        .output("inserted.teamId")
        .compile(),
    ).toThrow("Minnow does not support the T-SQL OUTPUT clause");
    expect(() =>
      db
        .selectFrom("users")
        .crossApply((eb) =>
          eb
            .selectFrom("scores")
            .select("points")
            .whereRef("scores.userId", "=", "users.userId")
            .as("s"),
        )
        .select(["users.name", "s.points"])
        .compile(),
    ).toThrow("Minnow does not support the T-SQL CROSS APPLY and OUTER APPLY joins");
    expect(() =>
      db
        .selectFrom("users")
        .outerApply((eb) =>
          eb
            .selectFrom("scores")
            .select("points")
            .whereRef("scores.userId", "=", "users.userId")
            .as("s"),
        )
        .select(["users.name", "s.points"])
        .compile(),
    ).toThrow("Minnow does not support the T-SQL CROSS APPLY and OUTER APPLY joins");
  });

  it("refuses MySQL and SQLite insert variants with the ON CONFLICT alternative", () => {
    const row = { userId: 90, name: "n", age: null, teamId: null };
    expect(() => db.replaceInto("users").values(row).compile()).toThrow(
      "Minnow does not support MySQL's REPLACE INTO",
    );
    expect(() => db.insertInto("users").values(row).orIgnore().compile()).toThrow(
      "Minnow does not support SQLite's INSERT OR IGNORE/ABORT/FAIL/ROLLBACK actions",
    );
    expect(() => db.insertInto("users").values(row).ignore().compile()).toThrow(
      "Minnow does not support SQLite's INSERT OR IGNORE/ABORT/FAIL/ROLLBACK actions",
    );
    expect(() =>
      db.insertInto("users").values(row).onDuplicateKeyUpdate({ name: "other" }).compile(),
    ).toThrow("Minnow does not support MySQL's ON DUPLICATE KEY UPDATE");
  });

  it("refuses MySQL ORDER BY/LIMIT on UPDATE and DELETE with the keyed rewrite", () => {
    expect(() => db.updateTable("scores").set({ points: 0 }).orderBy("scoreId").compile()).toThrow(
      "Minnow does not support MySQL's ORDER BY/LIMIT on UPDATE",
    );
    expect(() => db.updateTable("scores").set({ points: 0 }).limit(1).compile()).toThrow(
      "Minnow does not support MySQL's ORDER BY/LIMIT on UPDATE",
    );
    expect(() => db.deleteFrom("scores").orderBy("scoreId").compile()).toThrow(
      "Minnow does not support MySQL's ORDER BY/LIMIT on DELETE",
    );
    expect(() => db.deleteFrom("scores").limit(1).compile()).toThrow(
      "Minnow does not support MySQL's ORDER BY/LIMIT on DELETE",
    );
  });

  it("refuses MERGE ... THEN DO NOTHING with the WHEN-narrowing alternative", () => {
    expect(() =>
      db
        .mergeInto("users")
        .using("user_changes", "user_changes.userId", "users.userId")
        .whenMatched()
        .thenDoNothing()
        .compile(),
    ).toThrow("Minnow does not support MERGE ... THEN DO NOTHING");
  });

  it("refuses PostgreSQL-only JSON functions with the Minnow helper named", () => {
    expect(() =>
      db
        .selectFrom("users")
        .select((eb) => eb.fn.jsonAgg("users").as("all"))
        .compile(),
    ).toThrow("Import jsonArrayFrom from @minnowdb/kysely/helpers");
    expect(() =>
      db
        .selectFrom("users")
        .select((eb) => eb.fn.toJson("users").as("row"))
        .compile(),
    ).toThrow("Import jsonBuildObject or jsonObjectFrom from @minnowdb/kysely/helpers");
    expect(() =>
      db
        .selectFrom("users")
        .select((eb) => eb.fn<string>("json_build_object", [eb.val("id"), "userId"]).as("object"))
        .compile(),
    ).toThrow("Import jsonBuildObject from @minnowdb/kysely/helpers");
    expect(() =>
      db
        .selectFrom("users")
        .select((eb) => eb.fn.agg<string>("jsonb_agg", ["name"]).as("names"))
        .compile(),
    ).toThrow("Import jsonArrayFrom from @minnowdb/kysely/helpers");
  });

  it("refuses data-modifying CTEs with the run-first alternative", () => {
    expect(() =>
      db
        .with("gone", (qb) => qb.deleteFrom("scores").where("points", "<", 10).returning("scoreId"))
        .selectFrom("gone")
        .selectAll()
        .compile(),
    ).toThrow("Minnow supports only SELECT queries inside WITH");
  });

  it("refuses schema, type, and materialized-view management the engine lacks", () => {
    expect(() => db.schema.createSchema("app").compile()).toThrow("Minnow has no schemas");
    expect(() => db.schema.dropSchema("app").compile()).toThrow("Minnow has no schemas");
    expect(() => db.schema.dropType("mood").compile()).toThrow("Minnow does not support DROP TYPE");
    expect(() => db.schema.alterType("mood").addValue("meh").compile()).toThrow(
      "Minnow does not support ALTER TYPE",
    );
    expect(() => db.schema.refreshMaterializedView("mv").compile()).toThrow(
      "Minnow does not support materialized views",
    );
  });

  it("refuses unsupported view forms with the working alternative", () => {
    const body = db.selectFrom("users").select("userId");
    expect(() => db.schema.createView("v").materialized().as(body).compile()).toThrow(
      "Minnow does not support materialized views",
    );
    expect(() => db.schema.createView("v").temporary().as(body).compile()).toThrow(
      "Minnow does not support temporary views",
    );
    expect(() => db.schema.createView("v").ifNotExists().as(body).compile()).toThrow(
      "Minnow does not support CREATE VIEW IF NOT EXISTS",
    );
    expect(() => db.schema.createView("v").columns(["a"]).as(body).compile()).toThrow(
      "Minnow does not support view column lists",
    );
  });

  it("refuses unsupported table and column DDL with the working alternative", () => {
    expect(() =>
      db.schema.createTable("t").temporary().addColumn("id", "integer").compile(),
    ).toThrow("Minnow does not support temporary tables");
    expect(() =>
      db.schema
        .createTable("t")
        .addColumn("id", "serial", (col) => col.primaryKey())
        .compile(),
    ).not.toThrow(); // SERIAL is PostgreSQL's auto-increment pseudo-type; the engine accepts it.
    // Auto-increment, identity, and serial columns all compile: the engine reads each as its
    // auto-increment default, so a Kysely migration can declare the key without the schema DSL.
    expect(() =>
      db.schema
        .createTable("t")
        .addColumn("id", "integer", (col) => col.autoIncrement().primaryKey())
        .compile(),
    ).not.toThrow();
    expect(() =>
      db.schema
        .createTable("t")
        .addColumn("id", "integer", (col) => col.generatedAlwaysAsIdentity().primaryKey())
        .compile(),
    ).not.toThrow();
    expect(() =>
      db.schema
        .createTable("t")
        .addColumn("id", "integer", (col) => col.identity().primaryKey())
        .compile(),
    ).not.toThrow();
    expect(() =>
      db.schema
        .createTable("t")
        .addColumn("n", "integer", (col) => col.unsigned())
        .compile(),
    ).toThrow("Minnow does not support MySQL's UNSIGNED integers");
    expect(() =>
      db.schema
        .createTable("t")
        .addColumn("u", "text", (col) => col.unique().nullsNotDistinct())
        .compile(),
    ).toThrow("Minnow does not support NULLS NOT DISTINCT");
    expect(() =>
      db.schema
        .createTable("t")
        .addColumn("d", "integer", (col) => col.generatedAlwaysAs(sql`1 + 1`))
        .compile(),
    ).toThrow("Minnow supports only stored generated columns");
  });

  it("refuses unsupported index forms with the working alternative", () => {
    expect(() =>
      db.schema.createIndex("i").on("users").columns(["name"]).using("gin").compile(),
    ).toThrow("Minnow does not support index access methods");
    expect(() =>
      db.schema
        .createIndex("i")
        .on("users")
        .columns(["name"])
        .where(sql.ref("name"), "is not", null)
        .compile(),
    ).toThrow("Minnow does not support partial indexes");
    expect(() =>
      db.schema
        .createIndex("i")
        .on("users")
        .columns(["name"])
        .unique()
        .nullsNotDistinct()
        .compile(),
    ).toThrow("Minnow does not support NULLS NOT DISTINCT");
    expect(() =>
      db.schema
        .createIndex("i")
        .on("users")
        .column(sql`lower(name)`)
        .compile(),
    ).toThrow("Minnow does not support expression indexes");
  });

  it("refuses unsupported ALTER TABLE forms with the copy-forward alternative", () => {
    const alter = db.schema.alterTable("users");
    expect(() => alter.setSchema("app").compile()).toThrow("Minnow has no schemas");
    expect(() =>
      alter.alterColumn("name", (col) => col.setDataType("varchar(10)")).compile(),
    ).toThrow("Minnow does not support altering a column's type, default, or nullability");
    expect(() => alter.alterColumn("name", (col) => col.setDefault("x")).compile()).toThrow(
      "Minnow does not support altering a column's type, default, or nullability",
    );
    expect(() => alter.alterColumn("age", (col) => col.dropNotNull()).compile()).toThrow(
      "Minnow does not support altering a column's type, default, or nullability",
    );
    expect(() => alter.modifyColumn("name", "text", (col) => col.notNull()).compile()).toThrow(
      "Minnow does not support altering a column's type, default, or nullability",
    );
    expect(() => alter.addUniqueConstraint("u", ["name"]).compile()).toThrow(
      "Minnow does not support altering table constraints after creation",
    );
    expect(() => alter.dropConstraint("u").compile()).toThrow(
      "Minnow does not support altering table constraints after creation",
    );
    expect(() => alter.renameConstraint("u", "v").compile()).toThrow(
      "Minnow does not support altering table constraints after creation",
    );
    expect(() => alter.addIndex("i").columns(["name"]).compile()).toThrow(
      "Minnow does not support MySQL's ALTER TABLE ... ADD/DROP INDEX",
    );
  });
});

describe("supported DDL executes end to end", () => {
  it("creates tables with defaults, checks, references, and composite constraints", async () => {
    await db.schema
      .createTable("projects")
      .ifNotExists()
      .addColumn("projectId", "integer", (col) => col.primaryKey())
      .addColumn("title", "text", (col) => col.notNull().unique())
      .addColumn("budget", "numeric(10, 2)", (col) => col.defaultTo("100.00"))
      .addColumn("active", "boolean", (col) => col.defaultTo(sql`TRUE`).notNull())
      .addColumn("ownerId", "integer", (col) => col.references("users.userId").onDelete("cascade"))
      .addColumn("magnitude", "integer", (col) => col.check(sql`magnitude > 0`))
      .addColumn("doubled", "integer", (col) => col.generatedAlwaysAs(sql`magnitude * 2`).stored())
      .execute();
    await db.schema
      .createTable("assignments")
      .addColumn("projectId", "integer")
      .addColumn("userId", "integer")
      .addColumn("role", "text", (col) => col.notNull())
      .addPrimaryKeyConstraint("assignments_pk", ["projectId", "userId"])
      .addUniqueConstraint("assignments_role", ["projectId", "role"])
      .addCheckConstraint("assignments_role_nonempty", sql`length(role) > 0`)
      .addForeignKeyConstraint("assignments_owner", ["userId"], "users", ["userId"])
      .execute();

    interface ProjectRow {
      projectId: number;
      title: string;
      budget: Generated<string>;
      active: Generated<boolean>;
      ownerId: number | null;
      magnitude: number | null;
      doubled: ColumnType<number | null, never, never>;
    }
    const raw = db as unknown as Kysely<ConformanceDatabase & { projects: ProjectRow }>;
    await raw
      .insertInto("projects")
      .values({ projectId: 1, title: "Minnow", ownerId: 4, magnitude: 21 })
      .execute();
    expect(await raw.selectFrom("projects").selectAll().execute()).toEqual([
      {
        projectId: 1,
        title: "Minnow",
        budget: "100.00",
        active: true,
        ownerId: 4,
        magnitude: 21,
        doubled: 42,
      },
    ]);
    await expect(
      raw.insertInto("projects").values({ projectId: 2, title: "Bad", magnitude: -1 }).execute(),
    ).rejects.toThrow(/check|CHECK/);

    // ON DELETE CASCADE flows from the referenced user.
    await db.deleteFrom("users").where("userId", "=", 4).execute();
    expect(await raw.selectFrom("projects").selectAll().execute()).toEqual([]);
  });

  it("creates and drops indexes, views, and enum types", async () => {
    await db.schema.createIndex("users_name").on("users").columns(["name"]).unique().execute();
    await db.schema
      .createIndex("users_team_name")
      .ifNotExists()
      .on("users")
      .columns(["teamId", "name desc"])
      .execute();
    await db.schema.dropIndex("users_team_name").execute();

    await db.schema
      .createView("adults")
      .as(db.selectFrom("users").select(["userId", "name"]).where("age", ">=", 21))
      .execute();
    await db.schema
      .createView("adults")
      .orReplace()
      .as(db.selectFrom("users").select(["userId", "name"]).where("age", ">=", 30))
      .execute();
    const adults = db as unknown as Kysely<
      ConformanceDatabase & { adults: { userId: number; name: string } }
    >;
    expect(await adults.selectFrom("adults").select("name").orderBy("userId").execute()).toEqual([
      { name: "Ada" },
      { name: "Katherine" },
    ]);
    await db.schema.dropView("adults").execute();

    await db.schema.createType("mood").asEnum(["sad", "ok", "happy"]).execute();
    await db.schema
      .createTable("moods")
      .addColumn("id", "integer", (col) => col.primaryKey())
      .addColumn("feeling", sql`mood`, (col) => col.notNull())
      .execute();
    const moods = db as unknown as Kysely<
      ConformanceDatabase & { moods: { id: number; feeling: "sad" | "ok" | "happy" } }
    >;
    await moods.insertInto("moods").values({ id: 1, feeling: "happy" }).execute();
    expect(await moods.selectFrom("moods").select("feeling").execute()).toEqual([
      { feeling: "happy" },
    ]);
    await expect(
      moods
        .insertInto("moods")
        .values({ id: 2, feeling: "angry" as "happy" })
        .execute(),
    ).rejects.toThrow(/enum|mood/i);

    await db.schema.dropTable("moods").execute();
    await db.schema.dropTable("missing").ifExists().execute();
  });

  it("creates tables from SELECT and sequences through raw SQL", async () => {
    await db.schema
      .createTable("user_names")
      .as(db.selectFrom("users").select(["userId", "name"]))
      .execute();
    const copied = db as unknown as Kysely<
      ConformanceDatabase & { user_names: { userId: number; name: string } }
    >;
    expect(
      (await copied.selectFrom("user_names").selectAll().orderBy("userId").execute()).length,
    ).toBe(4);

    await sql`CREATE SEQUENCE ticket_ids`.execute(db);
    await db.schema
      .createTable("tickets")
      .addColumn("ticketId", "integer", (col) =>
        col.primaryKey().defaultTo(sql`nextval('ticket_ids')`),
      )
      .addColumn("label", "text", (col) => col.notNull())
      .execute();
    const tickets = db as unknown as Kysely<
      ConformanceDatabase & { tickets: { ticketId: Generated<number>; label: string } }
    >;
    await tickets
      .insertInto("tickets")
      .values([{ label: "a" }, { label: "b" }])
      .execute();
    expect(await tickets.selectFrom("tickets").selectAll().orderBy("ticketId").execute()).toEqual([
      { ticketId: 1, label: "a" },
      { ticketId: 2, label: "b" },
    ]);

    await db.schema.alterTable("tickets").addColumn("extra", "text").execute();
    await db.schema.alterTable("tickets").dropColumn("extra").execute();
  });
});

const boundarySchema = schema([
  table("orders", {
    id: column.integer().unique(),
    total: column.numeric({ precision: 12, scale: 2 }),
    open: column.boolean(),
  }),
  table("payments", {
    id: column.integer().unique(),
    orderId: column.integer(),
    amount: column.numeric({ precision: 12, scale: 2 }),
  }),
]);

type BoundaryDatabase = InferKyselyDatabase<typeof boundarySchema>;

function numericOperandsSpanJoins(boundaryDb: Kysely<BoundaryDatabase>): void {
  // Exact NUMERIC selects as a lossless string but compares against numbers or strings, on the
  // base table and across joins alike.
  void boundaryDb.selectFrom("orders").selectAll().where("total", ">=", 12.5);
  void boundaryDb.selectFrom("orders").selectAll().where("total", ">=", "12.50");
  void boundaryDb
    .selectFrom("orders")
    .innerJoin("payments", "payments.orderId", "orders.id")
    .where("payments.amount", ">", 3)
    .whereRef("payments.amount", "=", "orders.total")
    .selectAll();
  // @ts-expect-error a numeric operand accepts numbers and strings, never booleans
  void boundaryDb.selectFrom("orders").selectAll().where("total", ">=", true);
  void boundaryDb
    .selectFrom("orders")
    .innerJoin("payments", "payments.orderId", "orders.id")
    // @ts-expect-error joined numeric operands keep the same boundary types
    .where("payments.amount", ">", false)
    .selectAll();
}

function kyselyTypeUtilitiesSurviveBranding(dbRef: Kysely<ConformanceDatabase>): void {
  const narrowed = dbRef
    .selectFrom("users")
    .select(["name", "age"])
    .where("age", "is not", null)
    .$narrowType<{ age: number }>();
  expectTypeOf<Awaited<ReturnType<typeof narrowed.execute>>>().toEqualTypeOf<
    Array<{ name: string; age: number }>
  >();

  const cast = dbRef.selectFrom("users").select("userId").$castTo<{ userId: 1 | 2 }>();
  expectTypeOf<Awaited<ReturnType<typeof cast.execute>>>().toEqualTypeOf<
    Array<{ userId: 1 | 2 }>
  >();

  const summed = dbRef
    .selectFrom("scores")
    .select((eb) => eb.fn.sum("points").$notNull().as("sum"));
  expectTypeOf<Awaited<ReturnType<typeof summed.execute>>>().toEqualTypeOf<
    Array<{ sum: number }>
  >();
  void narrowed;
  void cast;
  void summed;
}

describe("type-level DX", () => {
  it("keeps operand boundaries and Kysely's type utilities intact", () => {
    expect(numericOperandsSpanJoins).toBeTypeOf("function");
    expect(kyselyTypeUtilitiesSurviveBranding).toBeTypeOf("function");
    expect(boundarySchema.tables.length).toBe(2);
  });
});

describe("sql template tag", () => {
  it("binds identifiers, references, literals, and raw fragments", async () => {
    const built = sql<{ label: string }>`
      SELECT ${sql.ref("name")} AS ${sql.id("label")}
      FROM ${sql.table("users")}
      WHERE ${sql.ref("userId")} = ${1} AND ${sql.lit("Ada")} = ${sql.ref("name")}
      ${sql.raw("ORDER BY 1")}
    `;
    expect((await built.execute(db)).rows).toEqual([{ label: "Ada" }]);

    const joined = sql<{ userId: number }>`
      SELECT ${sql.join([sql.ref("userId")])} FROM users
      WHERE ${sql.ref("userId")} IN (${sql.join([1, 2])}) ORDER BY 1
    `;
    expect((await joined.execute(db)).rows).toEqual([{ userId: 1 }, { userId: 2 }]);

    expect(
      (
        await sql<{ a: number }>`SELECT v.a AS a FROM (VALUES (1), (2)) AS v(a) ORDER BY 1`.execute(
          db,
        )
      ).rows,
    ).toEqual([{ a: 1 }, { a: 2 }]);
  });
});
