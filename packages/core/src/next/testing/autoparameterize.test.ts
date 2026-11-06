/**
 * Literal auto-parameterization (lower/parameterize.ts): which literals are lifted, when a lifted
 * plan is accepted, that it computes what the original text computes in PostgreSQL for every
 * value of the key, and the plan-cache gate (one lowering per normalized text).
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { LoweredStatement } from "../lower/lowerer.js";
import { liftedSlotsAreEquivalent, parameterize } from "../lower/parameterize.js";
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
      key("SELECT label FROM big WHERE id = 9223372036854775807"),
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
    return next.lowerer.lowerParameterized(parameterized, next.context());
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
        },
        next.context(),
      ),
    ).toBeNull();
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
      const lowered = next.lowerer.lowerParameterized(parameterized, next.context());
      if (lowered === null || (lowered.kind !== "query" && lowered.kind !== "modify")) return;
      accepted += 1;
      const rows = next.runPlan(lowered, parameterized.values, []).map((row) => Object.values(row));
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
    // The owner's cache, as the contract describes it (lowerer.ts): key → plan.
    const cache = new Map<string, LoweredStatement>();
    let lowerings = 0;
    const lowerer = next.lowerer;
    const plan = (sql: string) => {
      const parameterized = lowerer.parameterize(sql);
      const key = parameterized?.key ?? sql;
      const cached = cache.get(key);
      if (cached !== undefined) return { lowered: cached, values: parameterized?.values ?? [] };
      lowerings += 1;
      const lowered =
        (parameterized === null
          ? null
          : lowerer.lowerParameterized(parameterized, next.context())) ??
        lowerer.lower(sql, next.context());
      cache.set(key, lowered);
      return { lowered, values: parameterized?.values ?? [] };
    };
    const started = performance.now();
    let queries = 0;
    for (let key = 0; key < 10_000; key += 1)
      if (
        plan(`SELECT name, tier FROM customers WHERE id = ${String(key)}`).lowered.kind === "query"
      )
        queries += 1;
    const perRead = ((performance.now() - started) * 1000) / 10_000;
    expect(queries).toBe(10_000);
    expect(lowerings).toBe(1);
    expect(cache.size).toBe(1);
    // The key itself is a text scan: far below one lowering (~130 µs) on any host.
    expect(perRead).toBeLessThan(50);
    // The cached plan answers each key.
    const { lowered, values } = plan("SELECT name, tier FROM customers WHERE id = 42");
    if (lowered.kind !== "query") throw new Error("Expected a query");
    expect(next.runPlan(lowered, values, [])).toEqual([{ name: "It's", tier: 1 }]);
  });
});
