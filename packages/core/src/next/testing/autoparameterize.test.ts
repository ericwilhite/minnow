/**
 * Literal auto-parameterization (lower/parameterize.ts): which literals are lifted, when a lifted
 * plan is accepted, that it computes what the original text computes in PostgreSQL for every
 * value of the key, and the plan-cache gate (one lowering per normalized text).
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AUTOPARAMETERIZE_LITERAL_STATES, createReach } from "../../testing/reach.js";
import type { Lowerer, LoweredStatement, LoweringContext } from "../lower/lowerer.js";
import {
  bindParameterized,
  liftedSlotsAreEquivalent,
  parameterize,
  type NotParameterizable,
  type ParameterizedPlan,
} from "../lower/parameterize.js";
import type { PhysicalType } from "../types.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { pgOutcome } from "./pglite-oracle.js";

const SETUP = [
  "CREATE TABLE customers (id integer PRIMARY KEY, name varchar(40) NOT NULL, region text, tier smallint)",
  "CREATE TABLE big (id bigint PRIMARY KEY, label text)",
  "CREATE TABLE tags (code text PRIMARY KEY, n integer)",
  "CREATE TABLE small (k smallint PRIMARY KEY, v integer)",
  "INSERT INTO customers VALUES (1, 'Ada', 'west', 3), (2, 'Bo', 'east', 1), (3, 'Cy', NULL, 2), (42, 'It''s', 'west', 1)",
  "INSERT INTO big VALUES (1, 'one'), (5000000000, 'five billion')",
  "INSERT INTO tags VALUES ('a', 1), ('b''c', 2), ('', 3)",
  "INSERT INTO small VALUES (1, 10), (2, 20)",
];

function database(): LoweredDatabase {
  const next = new LoweredDatabase();
  for (const sql of SETUP) next.execute(sql);
  return next;
}

/**
 * The owner's plan cache, as the contract describes it (Lowerer.lowerParameterized): a key holds
 * a ParameterizedPlan or the NotParameterizable marker; an original text's own plan is cached
 * under that text only. Counts lowerings.
 */
function planCache(lowerer: Lowerer, context: () => LoweringContext) {
  const families = new Map<string, ParameterizedPlan | NotParameterizable>();
  const texts = new Map<string, LoweredStatement>();
  const counts = { lowerings: 0 };
  const own = (sql: string): LoweredStatement => {
    const cached = texts.get(sql);
    if (cached !== undefined) return cached;
    counts.lowerings += 1;
    const lowered = lowerer.lower(sql, context());
    texts.set(sql, lowered);
    return lowered;
  };
  const plan = (
    sql: string,
    clientParameters: readonly unknown[] = [],
  ): { lowered: LoweredStatement; parameters: unknown[] } => {
    const parameterized = lowerer.parameterize(sql);
    if (parameterized === null) return { lowered: own(sql), parameters: [...clientParameters] };
    let family = families.get(parameterized.key);
    if (family === undefined) {
      counts.lowerings += 1;
      family = lowerer.lowerParameterized(parameterized, context());
      families.set(parameterized.key, family);
    }
    if (family.kind === "notParameterizable")
      return { lowered: own(sql), parameters: [...clientParameters] };
    return {
      lowered: family.statement,
      parameters: bindParameterized(family, parameterized, clientParameters),
    };
  };
  return { plan, families, texts, counts };
}

describe("parameterize: what is lifted", () => {
  it("lifts integer and plain string literals that are the whole operand of =", () => {
    const point = parameterize("SELECT name FROM customers WHERE id = 42");
    expect(point).toMatchObject({
      text: "SELECT name FROM customers WHERE id = $1",
      placeholders: 0,
      values: [42],
    });
    expect(parameterize("SELECT name FROM customers WHERE id = 43")?.key).toBe(point?.key);
    expect(parameterize("SELECT name FROM customers WHERE id = -7")?.values).toEqual([-7]);
    expect(parameterize("SELECT n FROM tags WHERE code = 'b''c'")?.values).toEqual(["b'c"]);
    // After the statement's own placeholders.
    expect(
      parameterize("SELECT name FROM customers WHERE region = $2 AND id = 5 AND tier = $1"),
    ).toMatchObject({
      text: "SELECT name FROM customers WHERE region = $2 AND id = $3 AND tier = $1",
      placeholders: 2,
      values: [5],
    });
    // Assignments stay: `SET tier = 2` is a cast of the literal, not a comparison.
    expect(
      parameterize("UPDATE customers SET tier = 2 WHERE id = 3 RETURNING name")?.values,
    ).toEqual([3]);
  });

  it("keys literals of different types apart", () => {
    const key = (sql: string) => parameterize(sql)?.key;
    const small = key("SELECT label FROM big WHERE id = 1");
    expect(key("SELECT label FROM big WHERE id = 2147483647")).toBe(small);
    expect(key("SELECT label FROM big WHERE id = 2147483648")).not.toBe(small);
    expect(key("SELECT label FROM big WHERE id = 5000000000")).toBe(
      key("SELECT label FROM big WHERE id = -9007199254740991"),
    );
    // Past 2^53 a class of its own: the front end may type those literals differently.
    expect(key("SELECT label FROM big WHERE id = 9007199254740992")).not.toBe(
      key("SELECT label FROM big WHERE id = 9007199254740991"),
    );
    expect(key("SELECT label FROM big WHERE id = 9007199254740992")).toBe(
      key("SELECT label FROM big WHERE id = -9223372036854775807"),
    );
    // Classed by the digits: the front end negates in the unsigned literal's type.
    expect(key("SELECT label FROM big WHERE id = -2147483648")).toBe(
      key("SELECT label FROM big WHERE id = 2147483648"),
    );
    expect(parameterize("SELECT label FROM big WHERE id = 5000000000")?.values).toEqual([
      5000000000n,
    ]);
    expect(key("SELECT 1 FROM tags WHERE code = '1'")).not.toBe(
      key("SELECT 1 FROM tags WHERE code = 1"),
    );
  });

  it("leaves every other literal as written", () => {
    for (const sql of [
      "SELECT name FROM customers WHERE id = 99999999999999999999",
      "SELECT name FROM customers WHERE id = 1.5",
      "SELECT name FROM customers WHERE id = 1e3",
      "SELECT name FROM customers WHERE id = '1'::integer",
      "SELECT name FROM customers WHERE id = 2 + 1",
      "SELECT name FROM customers WHERE id > 5",
      "SELECT name FROM customers WHERE id IN (1, 2)",
      "SELECT name FROM customers ORDER BY 1 LIMIT 5 OFFSET 2",
      "SELECT name FROM customers GROUP BY 1",
      "SELECT name FROM customers WHERE name = E'a\\\\b'",
      "SELECT name FROM customers WHERE id = $1",
      "SELECT name FROM customers WHERE id=-5",
      "SELECT 'id = 5' AS s FROM customers",
      "SELECT name FROM customers -- WHERE id = 5\n",
      "SELECT name FROM customers /* id = /* 5 */ */",
      'SELECT "id = 5" FROM customers',
      "SELECT $$ = 5 $$ AS s",
      "SET search_path = 'x'",
      "CREATE TABLE t (a integer DEFAULT 5)",
      "SELECT name FROM customers WHERE name = 'unterminated",
      // In an escape string `\'` is a quote: `= 1` is inside the string, not a comparison.
      "SELECT id FROM customers WHERE name = E'a\\' = 1 --'",
    ])
      expect(parameterize(sql), sql).toBeNull();
  });
});

describe("lowerParameterized: accepted only where a slot is the literal", () => {
  const lifted = (next: LoweredDatabase, sql: string): LoweredStatement | null => {
    const parameterized = parameterize(sql);
    if (parameterized === null) throw new Error(`nothing lifted: ${sql}`);
    const family = next.lowerer.lowerParameterized(parameterized, next.context());
    expect(family.key).toBe(parameterized.key);
    return family.kind === "parameterized" ? family.statement : null;
  };

  it("accepts point reads and equality filters", () => {
    const next = database();
    const point = lifted(next, "SELECT name, tier FROM customers WHERE id = 42");
    expect(point).toMatchObject({ kind: "query", pointRead: true, liftedParameters: 1 });
    expect(point?.parameters).toEqual([]);
    expect(JSON.stringify(point?.kind === "query" ? point.plan.root : null)).toContain(
      '"op":"keyLookup"',
    );
    // A widening cast (integer literal against a bigint key) and text keys.
    expect(lifted(next, "SELECT label FROM big WHERE id = 1")?.kind).toBe("query");
    expect(lifted(next, "SELECT n FROM tags WHERE code = 'a'")?.kind).toBe("query");
    expect(lifted(next, "SELECT id FROM customers WHERE region = 'west' AND tier = 1")).not.toBe(
      null,
    );
    expect(lifted(next, "DELETE FROM customers WHERE id = 3")?.kind).toBe("modify");
  });

  it("refuses a slot the literal would not equal", () => {
    const next = database();
    // A string literal typed by an integer column: unknown there, text as a slot.
    expect(lifted(next, "SELECT name FROM customers WHERE id = '1'")).toBeNull();
    // Lifted where it compares; the assignment keeps its literal.
    expect(lifted(next, "UPDATE customers SET tier = 2 WHERE id = 3")?.kind).toBe("modify");
    expect(lifted(next, "SELECT id = 1 AS one FROM customers")).not.toBeNull();
  });
});

describe("liftedSlotsAreEquivalent: every use of a slot", () => {
  // Explicit placeholders typed as a lifted literal would be (int4, text), judged as slot 0.
  const judge = (sql: string, type: "int" | "bigint" | "text" = "int"): boolean => {
    const next = database();
    const lowered = next.lowerer.lower(sql, {
      ...next.context(),
      parameterTypes: [
        type === "int"
          ? { kind: "int32", bits: 32 }
          : type === "bigint"
            ? { kind: "int64" }
            : { kind: "text" },
      ],
    });
    if (lowered.kind !== "query" && lowered.kind !== "modify") throw new Error(lowered.kind);
    return liftedSlotsAreEquivalent(lowered.plan, new Set([0]));
  };

  it("accepts = against the same type and index keys, through widening casts", () => {
    expect(judge("SELECT name FROM customers WHERE id = $1")).toBe(true);
    expect(judge("SELECT name FROM customers WHERE $1 = id")).toBe(true);
    expect(judge("SELECT label FROM big WHERE id = $1")).toBe(true);
    expect(judge("SELECT n FROM tags WHERE code = $1", "text")).toBe(true);
    expect(judge("SELECT id FROM customers WHERE tier = $1 AND region = 'west'")).toBe(true);
    // An IN list on the key is lookup tuples; duplicates return a row once (ir.ts keyLookup).
    expect(judge("SELECT id FROM customers WHERE id IN ($1, 2)")).toBe(true);
  });

  it("refuses every other use", () => {
    expect(judge("SELECT id FROM customers LIMIT $1")).toBe(false);
    expect(judge("SELECT id FROM customers ORDER BY id OFFSET $1")).toBe(false);
    expect(judge("SELECT abs($1) AS a FROM customers")).toBe(false);
    expect(judge("SELECT $1 AS a FROM customers")).toBe(false);
    expect(judge("SELECT id FROM customers WHERE id > $1")).toBe(false);
    expect(judge("SELECT id FROM customers WHERE id <> $1")).toBe(false);
    expect(judge("SELECT id FROM customers WHERE id + 0 = $1 + 0")).toBe(false);
    expect(judge("UPDATE customers SET tier = $1 WHERE id = 3")).toBe(false);
    expect(judge("SELECT id FROM customers WHERE CAST($1 AS smallint) = tier")).toBe(false);
    // Narrowing a bigint slot to integer is not the literal either.
    expect(judge("SELECT id FROM customers WHERE CAST($1 AS integer) = id", "bigint")).toBe(false);
  });

  it("is what lowerParameterized applies", () => {
    const next = database();
    // A rewrite whose slot lands in LIMIT: refused even though it lowers.
    expect(
      next.lowerer.lowerParameterized(
        {
          key: "limit",
          text: "SELECT id FROM customers ORDER BY id LIMIT $1",
          placeholders: 0,
          values: [1],
          types: [{ kind: "int32", bits: 32 }],
          literals: ["1"],
        },
        next.context(),
      ),
    ).toEqual({ kind: "notParameterizable", key: "limit" });
  });
});

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  for (const sql of SETUP) await pg.exec(sql);
}, 60_000);

afterAll(async () => {
  await pg.close();
});

describe("a lifted plan computes the original text for every value of its key", () => {
  it("agrees with PostgreSQL on the original texts", async () => {
    const next = database();
    const families: ReadonlyArray<(value: string) => string> = [
      (v) => `SELECT name, tier FROM customers WHERE id = ${v}`,
      (v) => `SELECT id FROM customers WHERE tier = ${v} ORDER BY id`,
      (v) => `SELECT id FROM customers WHERE region = 'west' AND tier = ${v} ORDER BY id`,
      (v) => `SELECT count(*) AS n FROM customers WHERE tier = ${v}`,
      (v) => `SELECT c.name FROM customers c JOIN small s ON s.v = c.tier * 10 WHERE c.id = ${v}`,
      (v) =>
        `SELECT id, CASE WHEN tier = ${v} THEN 'match' ELSE 'other' END AS t FROM customers ORDER BY id`,
      (v) => `SELECT label FROM big WHERE id = ${v}`,
      // A smallint key against integer values outside its range: no row, never an error.
      (v) => `SELECT v FROM small WHERE k = ${v}`,
    ];
    const integers = ["1", "2", "3", "42", "0", "-1", "40000", "2147483647", "-2147483648"];
    const texts: ReadonlyArray<(value: string) => string> = [
      (v) => `SELECT n FROM tags WHERE code = ${v}`,
      (v) => `SELECT id FROM customers WHERE name = ${v} ORDER BY id`,
    ];
    const strings = ["'a'", "'b''c'", "''", "'missing'", "'It''s'"];
    let accepted = 0;
    const check = async (sql: string) => {
      const parameterized = parameterize(sql);
      if (parameterized === null) throw new Error(`nothing lifted: ${sql}`);
      const family = next.lowerer.lowerParameterized(parameterized, next.context());
      if (family.kind !== "parameterized") return;
      const lowered = family.statement;
      if (lowered.kind !== "query" && lowered.kind !== "modify") return;
      accepted += 1;
      const rows = next
        .runPlan(lowered, bindParameterized(family, parameterized, []), [])
        .map((row) => Object.values(row));
      const expected = await pgOutcome(pg, sql);
      expect("rows" in expected ? expected.rows : expected, sql).toEqual(rows);
    };
    for (const family of families) for (const value of integers) await check(family(value));
    for (const family of texts) for (const value of strings) await check(family(value));
    // Every family is accepted (none falls back), so the comparison above is the lifted plan.
    expect(accepted).toBe(families.length * integers.length + texts.length * strings.length);
  });
});

describe("plan-cache gate: one lowering per normalized text", () => {
  it("lowers a literal point-read loop over 10,000 keys once", () => {
    const next = database();
    const cache = planCache(next.lowerer, () => next.context());
    const started = performance.now();
    let queries = 0;
    for (let key = 0; key < 10_000; key += 1)
      if (
        cache.plan(`SELECT name, tier FROM customers WHERE id = ${String(key)}`).lowered.kind ===
        "query"
      )
        queries += 1;
    const perRead = ((performance.now() - started) * 1000) / 10_000;
    expect(queries).toBe(10_000);
    expect(cache.counts.lowerings).toBe(1);
    expect([cache.families.size, cache.texts.size]).toEqual([1, 0]);
    // The key itself is a text scan: far below one lowering (~130 µs) on any host.
    expect(perRead).toBeLessThan(50);
    // The cached plan answers each key.
    const { lowered, parameters } = cache.plan("SELECT name, tier FROM customers WHERE id = 42");
    if (lowered.kind !== "query") throw new Error("Expected a query");
    expect(next.runPlan(lowered, parameters, [])).toEqual([{ name: "It's", tier: 1 }]);
  });

  // Review B6-p2c finding 1: a refused family once cached the first text's own plan under the
  // key, so `id = '2'` ran the `'1'` plan and returned customer 1.
  it("never runs one text's literals for another text of a refused family", () => {
    const next = database();
    const cache = planCache(next.lowerer, () => next.context());
    const names = (sql: string) => {
      const { lowered, parameters } = cache.plan(sql);
      if (lowered.kind !== "query") throw new Error(lowered.kind);
      return next.runPlan(lowered, parameters, []).map((row) => row.name);
    };
    expect(names("SELECT name FROM customers WHERE id = '1'")).toEqual(["Ada"]);
    expect(names("SELECT name FROM customers WHERE id = '2'")).toEqual(["Bo"]);
    expect(names("SELECT name FROM customers WHERE id = '1'")).toEqual(["Ada"]);
    // The key holds the marker; each text its own plan.
    expect([...cache.families.values()].map((family) => family.kind)).toEqual([
      "notParameterizable",
    ]);
    expect([...cache.texts.keys()]).toEqual([
      "SELECT name FROM customers WHERE id = '1'",
      "SELECT name FROM customers WHERE id = '2'",
    ]);
    expect(cache.counts.lowerings).toBe(3);
  });

  it("binds a plan only for a text of its own family", () => {
    const next = database();
    const point = parameterize("SELECT name FROM customers WHERE id = 1");
    const other = parameterize("SELECT name FROM customers WHERE tier = 1");
    if (point === null || other === null) throw new Error("nothing lifted");
    const family = next.lowerer.lowerParameterized(point, next.context());
    if (family.kind !== "parameterized") throw new Error(family.kind);
    expect(bindParameterized(family, point, [])).toEqual([1]);
    expect(() => bindParameterized(family, other, [])).toThrow(
      "a parameterized plan bound for another statement family",
    );
  });
});

describe("a lifted slot has exactly the type the original literal gets", () => {
  // Review B6-p2c finding 2: past 2^53 the scanner said int8 where the original lowers NUMERIC,
  // and `-2147483648` int4 where the original's unary minus gives int8.
  const BOUNDARIES = [
    "0",
    "1",
    "-1",
    "2147483647",
    "-2147483647",
    "2147483648",
    "-2147483648",
    "-2147483649",
    "9007199254740991",
    "-9007199254740991",
    "9007199254740992",
    "9007199254740993",
    "-9007199254740993",
    "9223372036854775807",
    "-9223372036854775807",
    "9223372036854775808",
  ];
  const SHAPES: ReadonlyArray<(value: string) => string> = [
    (v) => `SELECT id, label FROM wide WHERE id = ${v}`,
    (v) => `SELECT id FROM narrow WHERE id = ${v}`,
    (v) => `SELECT count(*) AS n FROM wide WHERE label = 'x' OR id = ${v}`,
    // An untyped client placeholder takes its type from the literal.
    (v) => `SELECT count(*) AS n FROM wide WHERE $1 = ${v}`,
    (v) => `SELECT label FROM wide WHERE id = $1 AND id = ${v}`,
    // A string literal against an integer column: unknown there, refused as a text slot.
    (v) => `SELECT id FROM narrow WHERE id = '${v}'`,
  ];
  const keys = BOUNDARIES.filter((v) => {
    const n = BigInt(v);
    return n >= -9223372036854775808n && n <= 9223372036854775807n;
  });

  /** The type lowering gives `literal` alone, as an expression of its own. */
  const literalType = (next: LoweredDatabase, literal: string): PhysicalType | undefined => {
    const lowered = next.lower(`SELECT ${literal} AS v`);
    return lowered.kind === "query" ? lowered.plan.columns[0]?.type : undefined;
  };

  it("matches the original lowering's types and rows, or refuses", () => {
    const next = new LoweredDatabase();
    next.execute("CREATE TABLE wide (id bigint PRIMARY KEY, label text)");
    next.execute("CREATE TABLE narrow (id integer PRIMARY KEY)");
    // Bound, not literal: the rows are exact whatever the front end makes of a literal.
    for (const key of keys) next.execute("INSERT INTO wide VALUES ($1, $2)", [BigInt(key), key]);
    for (const key of keys.filter((v) => Math.abs(Number(v)) <= 2147483647))
      next.execute("INSERT INTO narrow VALUES ($1) ON CONFLICT DO NOTHING", [BigInt(key)]);
    const reach = createReach("auto-parameterized literals", AUTOPARAMETERIZE_LITERAL_STATES);
    const decisions = new Map<string, string>();
    const failures: string[] = [];
    for (const shape of SHAPES)
      for (const value of BOUNDARIES) {
        reach.beginHistory();
        const sql = shape(value);
        const parameterized = parameterize(sql);
        let original: LoweredStatement;
        try {
          original = next.lower(sql);
        } catch {
          // The original fails on its own: a lifted plan may not exist for it.
          if (parameterized !== null) {
            const family = next.lowerer.lowerParameterized(parameterized, next.context());
            if (family.kind === "parameterized") failures.push(`${sql}: lifted, original fails`);
          }
          continue;
        }
        if (original.kind !== "query") throw new Error(`${sql}: ${original.kind}`);
        // The client's $1 (shapes with one) bound to the literal's value in the original's type.
        const clientType = original.parameters[0];
        const client =
          clientType === undefined
            ? []
            : [clientType.kind === "int32" || clientType.kind === "int64" ? BigInt(value) : value];
        if (parameterized === null) {
          reach.reach("not lifted");
          if (BigInt(value) <= 9223372036854775807n && BigInt(value) >= -9223372036854775807n)
            failures.push(`${sql}: an int8 literal not lifted`);
          continue;
        }
        const family = next.lowerer.lowerParameterized(parameterized, next.context());
        // One decision for every text of a key.
        const decided = decisions.get(parameterized.key);
        if (decided !== undefined && decided !== family.kind)
          failures.push(`${sql}: ${family.kind}, another text of its key ${decided}`);
        decisions.set(parameterized.key, family.kind);
        const own = parameterized.literals.map((literal) => literalType(next, literal));
        const agrees = own.every(
          (type, index) =>
            type !== undefined &&
            JSON.stringify(type) === JSON.stringify(parameterized.types[index]),
        );
        if (family.kind === "notParameterizable") {
          reach.reach("refused");
          if (agrees && !sql.includes("'"))
            failures.push(`${sql}: refused though each literal has its slot's type`);
          continue;
        }
        if (!agrees) {
          failures.push(`${sql}: lifted as ${JSON.stringify(parameterized.types)}`);
          continue;
        }
        const lowered = family.statement;
        if (lowered.kind !== "query") throw new Error(`${sql}: ${lowered.kind}`);
        reach.reach(parameterized.types[0]?.kind === "int32" ? "lifted int4" : "lifted int8");
        // The client's own placeholders keep the original's types.
        if (JSON.stringify(lowered.parameters) !== JSON.stringify(original.parameters)) {
          failures.push(
            `${sql}: client types ${JSON.stringify(lowered.parameters)} vs ${JSON.stringify(original.parameters)}`,
          );
          continue;
        }
        const ours = next.runPlan(lowered, bindParameterized(family, parameterized, client), []);
        const theirs = next.runPlan(original, client, []);
        if (JSON.stringify(ours, bigints) !== JSON.stringify(theirs, bigints))
          failures.push(
            `${sql}: ${JSON.stringify(ours, bigints)} vs ${JSON.stringify(theirs, bigints)}`,
          );
      }
    expect(failures.join("\n")).toBe("");
    reach.assertReached();
  });

  it("lifts -2147483648 as int8 and leaves the client's $1 as the original types it", () => {
    const next = new LoweredDatabase();
    next.execute("CREATE TABLE wide (id bigint PRIMARY KEY, label text)");
    const sql = "SELECT count(*) AS n FROM wide WHERE $1 = -2147483648";
    const parameterized = parameterize(sql);
    expect(parameterized?.types).toEqual([{ kind: "int64" }]);
    if (parameterized === null) throw new Error("nothing lifted");
    const family = next.lowerer.lowerParameterized(parameterized, next.context());
    if (family.kind !== "parameterized") throw new Error(family.kind);
    expect(family.statement.parameters).toEqual(next.lower(sql).parameters);
    expect(family.statement.parameters).toEqual([{ kind: "int64" }]);
  });

  it("past 2^53, lifts only where the original literal is int8 too", () => {
    const next = new LoweredDatabase();
    next.execute("CREATE TABLE wide (id bigint PRIMARY KEY, label text)");
    for (const literal of ["9007199254740992", "9007199254740993", "-9007199254740993"]) {
      const sql = `SELECT count(*) AS n FROM wide WHERE $1 = ${literal}`;
      const parameterized = parameterize(sql);
      if (parameterized === null) throw new Error("nothing lifted");
      const family = next.lowerer.lowerParameterized(parameterized, next.context());
      // Before integer literals past 2^53 lower as int8, they are NUMERIC: not lifted. After, int8.
      const int8 = JSON.stringify(literalType(next, literal)) === JSON.stringify({ kind: "int64" });
      expect(family.kind, literal).toBe(int8 ? "parameterized" : "notParameterizable");
      if (family.kind === "parameterized")
        expect(family.statement.parameters).toEqual(next.lower(sql).parameters);
    }
  });
});

function bigints(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? `${String(value)}n` : value;
}
