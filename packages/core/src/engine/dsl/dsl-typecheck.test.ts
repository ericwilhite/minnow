/* eslint-disable @typescript-eslint/unbound-method -- expectTypeOf inspects method types
   without ever invoking them, so `this` scoping cannot go wrong here */
import { MemoryBlockStore } from "../../storage/index.js";
import { describe, expect, expectTypeOf, it } from "vitest";
import { MinnowDatabase } from "../database.js";
import { column, schema, table } from "../schema.js";
import { Minnow, createMinnow } from "./db.js";
import { type InferDatabase } from "./types.js";

/**
 * The TypeScript experience, pinned. The @ts-expect-error cases are enforced by `npm run
 * typecheck` (the engine tsconfig covers tests): if the API ever starts accepting one of these
 * invalid calls, the build fails. The wide-schema section guards type-level scalability — it
 * exists to keep instantiation cost visible in typecheck time, not to run anything.
 */

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  city: column.string().nullable(),
  joined: column.datetime().nullable(),
});
const orders = table("orders", {
  order_id: column.number().unique(),
  person: column.string().references("people", "name"),
  total: column.number(),
});
const appSchema = schema([people, orders]);
type DB = InferDatabase<typeof appSchema>;

// The factory infers DB from the runtime schema; the declared return type pins that the
// inferred database type is exactly InferDatabase<typeof appSchema>.
function createDb(): Minnow<DB> {
  return createMinnow(new MinnowDatabase(new MemoryBlockStore()), { schema: appSchema });
}

// Never executed — these functions exist only so the type checker enforces the expect-error
// directives below. Some of the invalid calls would also throw at runtime.
function invalidQueriesAreRejected(db: Minnow<DB>): void {
  // @ts-expect-error -- unknown table
  void db.selectFrom("nope");
  // @ts-expect-error -- unknown table in a mutation
  void db.insertInto("nope");
  // @ts-expect-error -- unknown table in a delete
  void db.deleteFrom("nope");

  const q = db.selectFrom("people as p");
  // @ts-expect-error -- unknown column reference
  void q.where("p.nope", "=", 1);
  // @ts-expect-error -- wrong value type for a number column
  void q.where("p.score", ">", "high");
  // @ts-expect-error -- unsupported operator token
  void q.where("p.score", "~", 1);
  // @ts-expect-error -- IN requires an array or subquery, not a scalar
  void q.where("p.score", "in", 10);
  // @ts-expect-error -- IS only compares against null
  void q.where("p.score", "is", 0);
  // @ts-expect-error -- unknown column in a string selection
  void q.select(["p.nope"]);
  // @ts-expect-error -- ordering key must be a context column or output alias
  void q.select(["p.name"]).orderBy("mystery");

  // @ts-expect-error -- SUM only accepts numeric columns
  void q.select((eb) => [eb.fn.sum("p.name").as("s")]);
  // @ts-expect-error -- AVG only accepts numeric columns
  void q.select((eb) => [eb.fn.avg("p.city").as("a")]);
  // @ts-expect-error -- arithmetic only accepts numeric operands
  void q.select((eb) => [eb("p.name", "+", 1).as("n")]);
  // @ts-expect-error -- DATE_TRUNC only accepts datetime columns
  void q.select((eb) => [eb.fn.dateTrunc("day", "p.score").as("d")]);

  // @ts-expect-error -- a scalar operand subquery must yield the compared column's type
  void q.where((eb) => eb("p.score", "=", eb.selectFrom("people").select(["name"])));
  // @ts-expect-error -- an IN subquery must yield the compared column's type
  void q.where("p.score", "in", q.select(["p.name"]));

  const dup = q.innerJoin("orders as p", "p.name", "p.name");
  /* eslint-disable @typescript-eslint/no-unsafe-call --
     the call is intentionally unresolvable: dup is a DuplicateJoinAliasError, not a builder */
  // @ts-expect-error -- a duplicate join alias resolves to an error type, not a builder
  void dup.selectAll();
  /* eslint-enable @typescript-eslint/no-unsafe-call */
}

function invalidMutationsAreRejected(db: Minnow<DB>): void {
  // @ts-expect-error -- missing the non-nullable score column
  void db.insertInto("people").values({ name: "Ada" });
  // @ts-expect-error -- wrong value type
  void db.insertInto("people").values({ name: "Ada", score: "ten" });
  // @ts-expect-error -- unknown column
  void db.insertInto("people").values({ name: "Ada", score: 1, nope: true });
  // @ts-expect-error -- wrong value type in set()
  void db.updateTable("people").set({ score: "ten" });
  // @ts-expect-error -- unknown column in set()
  void db.updateTable("people").set({ nope: 1 });

  const names = db.selectFrom("people").select(["name"]);
  // @ts-expect-error -- union member row {person: string} does not match {name: string}
  void names.union(db.selectFrom("orders").select(["person"]));

  // @ts-expect-error -- mutation predicates do not support subqueries
  void db.deleteFrom("people").where("name", "in", db.selectFrom("orders").select(["person"]));
}

// Derived tables and CTEs must keep their precise row types — no index-signature collapse.
function derivedAndCteColumnsStayChecked(db: Minnow<DB>): void {
  const sub = db
    .selectFrom("orders")
    .groupBy("person")
    .select((eb) => [eb.ref("person").as("person"), eb.fn.sum("total").as("spend")]);

  const derived = db.selectFrom(sub.as("t"));
  void derived.where("t.spend", ">", 10).select(["t.person"]);
  // @ts-expect-error -- unknown column on a derived table
  void derived.where("t.definitely_not_a_column", "=", 1);
  // @ts-expect-error -- unknown column in a derived selection
  void derived.select(["t.owner_of_nothing"]);

  const withCte = db.with("spend", () => sub);
  void withCte.selectFrom("spend").where("spend", ">", 10).select(["person"]);
  // @ts-expect-error -- unknown column on a CTE
  void withCte.selectFrom("spend").where("garbage_column", "=", 1);
}

describe("rejects invalid queries at compile time", () => {
  it("keeps the negative cases in the typechecked build", () => {
    // The assertions live in @ts-expect-error markers above; invoking them would throw.
    expect(typeof invalidQueriesAreRejected).toBe("function");
    expect(typeof invalidMutationsAreRejected).toBe("function");
    expect(typeof derivedAndCteColumnsStayChecked).toBe("function");
  });
});

describe("infers precise row types", () => {
  it("carries aliases, qualification, and nullability into the row type", () => {
    const db = createDb();

    const aliased = db.selectFrom("people as p").select(["p.name as owner", "p.city"]);
    expectTypeOf(aliased.execute).returns.resolves.toEqualTypeOf<
      Array<{ owner: string; city: string | null }>
    >();

    const single = db.selectFrom("people").selectAll();
    expectTypeOf(single.execute).returns.resolves.toEqualTypeOf<
      Array<{ name: string; score: number; city: string | null; joined: Date | null }>
    >();

    // A joined selectAll qualifies every column, exactly as the executor projects SELECT *.
    const joined = db
      .selectFrom("people as p")
      .innerJoin("orders as o", "o.person", "p.name")
      .selectAll();
    expectTypeOf(joined.execute).returns.resolves.toEqualTypeOf<
      Array<{
        "p.name": string;
        "p.score": number;
        "p.city": string | null;
        "p.joined": Date | null;
        "o.order_id": number;
        "o.person": string;
        "o.total": number;
      }>
    >();

    const left = db
      .selectFrom("people as p")
      .leftJoin("orders as o", "o.person", "p.name")
      .select(["p.name", "o.total", "o.person"]);
    expectTypeOf(left.execute).returns.resolves.toEqualTypeOf<
      Array<{ name: string; total: number | null; person: string | null }>
    >();
  });

  it("infers the facade type from the runtime schema", () => {
    const inferred = createMinnow(new MinnowDatabase(new MemoryBlockStore()), {
      schema: appSchema,
    });
    expectTypeOf(inferred).toEqualTypeOf<Minnow<DB>>();
  });

  it("accepts an explicit named database type so tooling prints Minnow<DB>", () => {
    // The documented standard: a named interface keeps hovers and declaration emit compact.
    interface NamedDB extends InferDatabase<typeof appSchema> {}
    const named = createMinnow<NamedDB>(new MinnowDatabase(new MemoryBlockStore()), {
      schema: appSchema,
    });
    expectTypeOf(named).toEqualTypeOf<Minnow<NamedDB>>();
    const q = named.selectFrom("people").select(["name", "city"]);
    expectTypeOf(q.execute).returns.resolves.toEqualTypeOf<
      Array<{ name: string; city: string | null }>
    >();
    // @ts-expect-error -- the named form stays fully checked: unknown column
    void named.selectFrom("people").select(["nope"]);
  });

  it("accepts a single selection without an array and keeps its type", () => {
    const db = createDb();
    const scalar = db.selectFrom("people").select("name");
    expectTypeOf(scalar.execute).returns.resolves.toEqualTypeOf<Array<{ name: string }>>();
    const scalarExpression = db.selectFrom("people").select((eb) => eb.fn.countAll().as("n"));
    expectTypeOf(scalarExpression.execute).returns.resolves.toEqualTypeOf<Array<{ n: number }>>();
  });

  it("exposes output rows type-only through $inferRow / $inferResult", () => {
    const db = createDb();
    const q = db.selectFrom("people").select(["name", "city"]);
    expectTypeOf<(typeof q)["$inferRow"]>().toEqualTypeOf<{ name: string; city: string | null }>();
    expectTypeOf<Array<(typeof q)["$inferRow"]>>().toEqualTypeOf<
      Awaited<ReturnType<typeof q.execute>>
    >();
    const live = q.live();
    expectTypeOf(live.$inferRow).toEqualTypeOf<{ name: string; city: string | null }>();
    const returning = db.deleteFrom("people").returning(["name"]);
    expectTypeOf(returning.$inferResult).toEqualTypeOf<{ name: string }>();
  });

  it("types coalesce by its operands, dropping null when the fallback is non-null", () => {
    const db = createDb();
    const q = db
      .selectFrom("people")
      .select((eb) => [
        eb.fn.coalesce("city", "name").as("place"),
        eb.fn.coalesce("city", eb.val("fallback")).as("sure"),
        eb.fn.coalesce("joined", "joined").as("maybe"),
      ]);
    expectTypeOf(q.execute).returns.resolves.toEqualTypeOf<
      Array<{ place: string; sure: string; maybe: Date | null }>
    >();
  });

  it("types expression results: aggregates, case, and windows", () => {
    const db = createDb();
    const q = db
      .selectFrom("people")
      .select((eb) => [
        eb.fn.countAll().as("everyone"),
        eb.fn.min("joined").as("earliest"),
        eb.fn.sum("score").as("sum"),
        eb.case().when("score", ">", 15).then("high").else("low").end().as("tier"),
        eb.case().when("score", ">", 15).then(1).end().as("maybe"),
        eb.rowNumber().over().as("rowNo"),
      ]);
    expectTypeOf(q.execute).returns.resolves.toEqualTypeOf<
      Array<{
        everyone: number;
        earliest: Date | null;
        sum: number | null;
        tier: "high" | "low";
        maybe: 1 | null;
        rowNo: number;
      }>
    >();
  });

  it("types mutation results and executeTakeFirst variants", () => {
    const db = createDb();
    // Kysely conventions: execute() yields an array of results, executeTakeFirstOrThrow() one.
    expectTypeOf(
      db.insertInto("people").values({ name: "A", score: 1 }).execute,
    ).returns.resolves.toEqualTypeOf<Array<{ readonly numInsertedRows: number }>>();
    expectTypeOf(
      db.updateTable("people").set({ score: 2 }).executeTakeFirstOrThrow,
    ).returns.resolves.toEqualTypeOf<{ readonly numUpdatedRows: number }>();
    expectTypeOf(db.deleteFrom("people").executeTakeFirstOrThrow).returns.resolves.toEqualTypeOf<{
      readonly numDeletedRows: number;
    }>();
    // returning() rewrites the result type to the projected row.
    expectTypeOf(
      db.insertInto("people").values({ name: "A", score: 1 }).returning(["name", "city"]).execute,
    ).returns.resolves.toEqualTypeOf<Array<{ name: string; city: string | null }>>();
    expectTypeOf(
      db.updateTable("people").set({ score: 2 }).returningAll().execute,
    ).returns.resolves.toEqualTypeOf<
      Array<{ name: string; score: number; city: string | null; joined: Date | null }>
    >();
    expectTypeOf(
      db.deleteFrom("people").returning(["name"]).executeTakeFirst,
    ).returns.resolves.toEqualTypeOf<{ name: string } | undefined>();
    const names = db.selectFrom("people").select(["name"]);
    expectTypeOf(names.executeTakeFirst).returns.resolves.toEqualTypeOf<
      { name: string } | undefined
    >();
    expectTypeOf(names.executeTakeFirstOrThrow).returns.resolves.toEqualTypeOf<{
      name: string;
    }>();
  });
});

// --- Wide-schema scalability -------------------------------------------------------------------
//
// 50 tables × 20 columns, all at the type level. If ColumnReference/RowFromSelections ever go
// super-linear, `npm run typecheck` time on this file blows up and flags it.

type Digit = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9;
type TenColumns<TPrefix extends string> = Record<`${TPrefix}${Digit}`, number>;
type WideRow = TenColumns<"a"> & TenColumns<"b"> & { id: number; label: string };
type WideDB = Record<`table_${0 | 1 | 2 | 3 | 4}${Digit}`, WideRow>;

describe("wide-schema type scalability", () => {
  it("keeps precise inference over a 50-table, 22-column database type", () => {
    const db = new Minnow<WideDB>(new MinnowDatabase(new MemoryBlockStore()));
    const q = db
      .selectFrom("table_07 as t")
      .innerJoin("table_23 as u", "u.id", "t.id")
      .where("t.a3", ">", 10)
      .where((eb) => eb.or([eb("u.b9", "=", 1), eb("t.label", "like", "x%")]))
      .select(["t.label", "u.a0", "t.b5 as five"])
      .orderBy("five", "desc")
      .limit(5);
    expectTypeOf(q.execute).returns.resolves.toEqualTypeOf<
      Array<{ label: string; a0: number; five: number }>
    >();
    // @ts-expect-error -- unknown column stays rejected even on the widest schema
    void q.where("t.z9", "=", 1);
    expect(q.compile().plan.base.table).toBe("table_07");
  });
});
