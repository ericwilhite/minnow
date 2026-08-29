import { describe, expect, it } from "vitest";
import { assertWellFormedString } from "../block-format/unicode.js";
import {
  MAX_SQL_NESTING_DEPTH,
  MAX_SQL_PARAMETERS,
  MAX_SQL_PATTERN_CHARACTERS,
  MAX_SQL_PATTERN_MATCH_STEPS,
  MAX_SQL_SCALAR_RESULT_CHARACTERS,
  MAX_SQL_TEXT_CHARACTERS,
  MAX_SQL_TOKENS,
} from "./cache-limits.js";
import { compileQuery, compileStatement, scalarFunctionValue } from "./query.js";
import { jsonAtPath, jsonIsValid } from "./sql-json.js";
import {
  boundedJsonText,
  exactNumericValue,
  jsonDomainValue,
  normalizeSqlDomainValue,
} from "./sql-domains.js";
import { compileLikePattern, compileSimilarPattern } from "./sql-semantics.js";

describe("SQL compilation resource limits", () => {
  it.each([
    ["", /Enter a SQL statement/],
    ["SAVEPOINT", /Expected savepoint name/],
    ["SAVEPOINT one trailing", /Unexpected input after SAVEPOINT/],
    ["RELEASE one trailing", /Unexpected input after RELEASE/],
    ["ROLLBACK TO one trailing", /Unexpected input after ROLLBACK TO/],
    ["BEGIN READ ONLY", /takes no READ/],
    ["START WORK", /Expected START TRANSACTION/],
    ["CREATE TYPE", /needs a name/],
    ["CREATE TYPE mood TEXT", /supports AS ENUM/],
    ["CREATE TYPE mood AS ENUM (1)", /string literals/],
    ["CREATE TYPE mood AS ENUM ('a' 'b')", /Expected comma/],
    ["CREATE TYPE mood AS ENUM ('a') trailing", /Unexpected input after ENUM/],
    ["CREATE SEQUENCE", /needs a name/],
    ["CREATE SEQUENCE seq START 1", /options are not supported/],
    ["CREATE VIEW 1 AS SELECT 1", /Expected a view name/],
    ["CREATE VIEW v (n) AS SELECT 1", /takes a name and AS/],
    ["CREATE VIEW v AS", /requires a query/],
    ["DROP DATABASE db", /DROP supports/],
    ["CREATE TRIGGER 1 AFTER INSERT ON t BEGIN DELETE FROM t END", /trigger name/],
    ["CREATE TRIGGER tr INSTEAD INSERT ON t BEGIN DELETE FROM t END", /BEFORE and AFTER/],
    ["CREATE TRIGGER tr AFTER SELECT ON t BEGIN DELETE FROM t END", /Trigger event/],
    ["CREATE TRIGGER tr AFTER INSERT t BEGIN DELETE FROM t END", /Expected ON/],
    ["CREATE TRIGGER tr AFTER INSERT ON 1 BEGIN DELETE FROM t END", /trigger table/],
    ["CREATE TRIGGER tr AFTER INSERT ON t FOR ROW BEGIN DELETE FROM t END", /FOR EACH ROW/],
    ["CREATE TRIGGER tr AFTER INSERT ON t DELETE FROM t END", /BEGIN \.\.\. END/],
    ["CREATE TRIGGER tr AFTER INSERT ON t BEGIN ; END", /at least one statement/],
    [
      "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (?) END",
      /cannot contain parameters/,
    ],
    [
      "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1 END",
      /support INSERT, UPDATE, and DELETE/,
    ],
    ["CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t SELECT 1 END", /INSERTs use VALUES/],
    [
      "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t (v) VALUES (1) ON CONFLICT (v) DO NOTHING END",
      /cannot carry ON CONFLICT/,
    ],
    [
      "CREATE TRIGGER tr AFTER INSERT ON t BEGIN DELETE FROM t RETURNING * END",
      /cannot carry RETURNING/,
    ],
    [
      "CREATE TRIGGER tr AFTER INSERT ON t BEGIN DELETE FROM t END trailing",
      /Unexpected input after the trigger body/,
    ],
  ] as const)("rejects malformed statement boundary: %s", (sql, message) => {
    expect(() => compileStatement(sql)).toThrow(message);
  });

  it("rejects oversized and lossy SQL before token allocation", () => {
    expect(() => compileQuery(`SELECT '${"x".repeat(MAX_SQL_TEXT_CHARACTERS)}'`)).toThrow(
      /SQL text cannot exceed/,
    );
    expect(() => compileStatement("SELECT '\uD800'")).toThrow(/unpaired surrogate/);
    expect(() => assertWellFormedString("SELECT 1", "test")).not.toThrow();
  });

  it("bounds tokens, syntax nesting, and numbered parameter slots", () => {
    const members = Array.from({ length: Math.ceil(MAX_SQL_TOKENS / 2) + 2 }, () => "1");
    expect(() => compileQuery(`SELECT ${members.join(",")}`)).toThrow(
      new RegExp(`cannot exceed ${String(MAX_SQL_TOKENS)} tokens`),
    );

    const nested =
      "SELECT " +
      "(".repeat(MAX_SQL_NESTING_DEPTH + 1) +
      "1" +
      ")".repeat(MAX_SQL_NESTING_DEPTH + 1);
    expect(() => compileQuery(nested)).toThrow(
      new RegExp(`cannot exceed ${String(MAX_SQL_NESTING_DEPTH)} levels`),
    );

    expect(() => compileQuery(`SELECT $${String(MAX_SQL_PARAMETERS + 1)}`)).toThrow(
      new RegExp(`cannot exceed ${String(MAX_SQL_PARAMETERS)} parameters`),
    );
  });
});

describe("SQL scalar allocation limits", () => {
  it("fails closed across nullable, numeric, and catalog-resolved scalar branches", () => {
    expect(scalarFunctionValue("GREATEST", [null, undefined])).toBeNull();
    expect(scalarFunctionValue("GREATEST", [null, 1, 3, 2])).toBe(3);
    expect(scalarFunctionValue("LEAST", [null, 3, 1, 2])).toBe(1);
    expect(() => scalarFunctionValue("GROUPING", [1])).toThrow("GROUP BY");
    expect(() => scalarFunctionValue("CURRENT_DATE", [])).toThrow("resolved before execution");
    expect(() => scalarFunctionValue("NEXTVAL", ["sequence"])).toThrow("database catalog");
    expect(() => scalarFunctionValue("CURRVAL", ["sequence"])).toThrow("database catalog");
    expect(scalarFunctionValue("MINNOW_TUPLE_KEY", [1, null])).toBeNull();

    expect(scalarFunctionValue("NULLIF", [4, null])).toBe(4);
    expect(scalarFunctionValue("MOD", [4, null])).toBeNull();
    expect(scalarFunctionValue("MOD", [4, 0])).toBeNull();
    expect(scalarFunctionValue("POWER", [4, null])).toBeNull();
    expect(() => scalarFunctionValue("POWER", [Number.MAX_VALUE, 2])).toThrow("non-finite");
    expect(() => scalarFunctionValue("SQRT", [-1])).toThrow("non-negative");
    expect(scalarFunctionValue("REPLACE", ["abc", null, "x"])).toBeNull();
    expect(scalarFunctionValue("REPLACE", ["abc", "a", null])).toBeNull();
    expect(scalarFunctionValue("REPLACE", ["abc", "", "x"])).toBe("abc");
    expect(scalarFunctionValue("INSTR", ["abc", null])).toBeNull();
    expect(scalarFunctionValue("INSTR", ["abc", "z"])).toBe(0);
    expect(scalarFunctionValue("ROUND", [1.25, null])).toBeNull();
  });

  it("handles Unicode padding, overlay, substring, and every primitive CAST boundary", () => {
    expect(scalarFunctionValue("LPAD", ["😀x", 1])).toBe("😀");
    expect(scalarFunctionValue("LPAD", ["x", 2, null])).toBeNull();
    expect(scalarFunctionValue("LPAD", ["x", 3, ""])).toBe("x");
    expect(scalarFunctionValue("RPAD", ["x", 4, "😀"])).toBe("x😀😀😀");
    expect(() => scalarFunctionValue("LPAD", ["x", -1])).toThrow("non-negative integer");

    expect(scalarFunctionValue("OVERLAY", ["abcd", null, 2])).toBeNull();
    expect(scalarFunctionValue("OVERLAY", ["abcd", "X", 2])).toBe("aXcd");
    expect(() => scalarFunctionValue("OVERLAY", ["abcd", "X", 0])).toThrow("positive integer");
    expect(() => scalarFunctionValue("OVERLAY", ["abcd", "X", 2, -1])).toThrow(
      "non-negative integer",
    );

    expect(scalarFunctionValue("SUBSTR", ["abcdef", null])).toBeNull();
    expect(scalarFunctionValue("SUBSTR", ["abcdef", -2, 2])).toBe("");
    expect(scalarFunctionValue("SUBSTR", ["abcdef", 20, 2])).toBe("");
    expect(scalarFunctionValue("SUBSTR", ["abcdef", 2, null])).toBeNull();
    expect(() => scalarFunctionValue("SUBSTR", ["abcdef", 1.5])).toThrow("integer");
    expect(() => scalarFunctionValue("SUBSTR", ["abcdef", 2, -1])).toThrow("non-negative");

    expect(scalarFunctionValue("CAST", [false, "string"])).toBe("false");
    expect(scalarFunctionValue("CAST", [true, "number"])).toBe(1);
    expect(scalarFunctionValue("CAST", [0, "boolean"])).toBe(false);
    expect(scalarFunctionValue("CAST", [1, "boolean"])).toBe(true);
    expect(scalarFunctionValue("CAST", [" t ", "boolean"])).toBe(true);
    expect(scalarFunctionValue("CAST", [" f ", "boolean"])).toBe(false);
    const date = new Date("2026-01-02T03:04:05.000Z");
    expect(scalarFunctionValue("CAST", [date, "datetime"])).toBe(date);
    expect(() => scalarFunctionValue("CAST", ["never", "datetime"])).toThrow("datetime");
    expect(() => scalarFunctionValue("CAST", [{}, "boolean"])).toThrow("Unsupported CAST");
  });

  it("refuses attacker-selected padding before allocating it", () => {
    expect(() =>
      scalarFunctionValue("LPAD", ["x", MAX_SQL_SCALAR_RESULT_CHARACTERS + 1, "y"]),
    ).toThrow(/LPAD result exceeds/);
    expect(() => scalarFunctionValue("RPAD", ["x", Number.MAX_SAFE_INTEGER, "y"])).toThrow(
      /RPAD result exceeds/,
    );
  });

  it("preflights expanding replacement and case conversion", () => {
    const source = "a".repeat(Math.floor(MAX_SQL_SCALAR_RESULT_CHARACTERS * 0.6));
    expect(() => scalarFunctionValue("REPLACE", [source, "a", "aa"])).toThrow(
      /REPLACE result exceeds/,
    );
    expect(() =>
      scalarFunctionValue("UPPER", ["x".repeat(MAX_SQL_SCALAR_RESULT_CHARACTERS + 1)]),
    ).toThrow(/UPPER input exceeds/);
  });

  it("counts code points without materializing a character array", () => {
    expect(scalarFunctionValue("LENGTH", ["a😀b"])).toBe(3);
    expect(scalarFunctionValue("INSTR", ["a😀bc", "c"])).toBe(4);
    expect(scalarFunctionValue("OCTET_LENGTH", ["a😀b"])).toBe(6);
    expect(
      scalarFunctionValue("SUBSTR", ["x".repeat(MAX_SQL_SCALAR_RESULT_CHARACTERS + 1), 2, 3]),
    ).toBe("xxx");
    expect(scalarFunctionValue("LPAD", ["x".repeat(MAX_SQL_SCALAR_RESULT_CHARACTERS + 1), 3])).toBe(
      "xxx",
    );
  });

  it("bounds JSON parsing and constructor output before expansion", () => {
    const oversized = `"${"x".repeat(MAX_SQL_SCALAR_RESULT_CHARACTERS)}"`;
    expect(() => jsonAtPath(oversized, "$", "JSON_VALUE")).toThrow(/document exceeds/);
    expect(() => jsonIsValid(oversized, "value")).toThrow(/document exceeds/);
    expect(() =>
      scalarFunctionValue("JSON_ARRAY", [
        "x".repeat(Math.ceil(MAX_SQL_SCALAR_RESULT_CHARACTERS / 2)),
        "y".repeat(Math.ceil(MAX_SQL_SCALAR_RESULT_CHARACTERS / 2)),
      ]),
    ).toThrow(/JSON_ARRAY result exceeds/);
  });

  it("bounds and canonicalizes SQL domain containers", () => {
    expect(jsonDomainValue('{"10":1,"2":2}', true)).toContain('{"10":1,"2":2}');
    expect(() =>
      jsonDomainValue(`"${"x".repeat(MAX_SQL_SCALAR_RESULT_CHARACTERS)}"`, true),
    ).toThrow(/JSON value exceeds/);
    const cyclic: unknown[] = [];
    cyclic.push(cyclic);
    expect(() => boundedJsonText(cyclic, false)).toThrow(/cycle/);
    expect(() => boundedJsonText({ value: Number.POSITIVE_INFINITY }, false)).toThrow(
      /must be finite/,
    );
    const date = new Date("2026-08-24T00:00:00.000Z");
    date.toISOString = () => "caller-controlled";
    expect(boundedJsonText(date, false)).toBe('"2026-08-24T00:00:00.000Z"');
  });

  it("does not retain every intermediate decimal power", () => {
    expect(exactNumericValue("1e100000")).toBe(
      `\u0000minnow-domain:numeric:1${"0".repeat(100_000)}`,
    );
    expect(() => exactNumericValue("1".repeat(100_001))).toThrow(/100000 significant digits/);
    expect(() => exactNumericValue("1e100001")).toThrow(/NUMERIC exponent is outside/);
  });

  it("does not reinterpret ordinary ARRAY strings as internal domain tags", () => {
    const sentinel = "\u0000minnow-domain:numeric:1";
    expect(
      normalizeSqlDomainValue({ kind: "array", element: "text" }, `[${JSON.stringify(sentinel)}]`),
    ).toBe(`\u0000minnow-domain:array:[${JSON.stringify(sentinel)}]`);
  });

  it("preserves a declared enum label in the internal tag namespace", () => {
    const sentinel = "\u0000minnow-domain:enum:not-a-tag";
    expect(
      normalizeSqlDomainValue({ kind: "enum", name: "status", values: [sentinel] }, sentinel),
    ).toBe(`\u0000minnow-domain:enum:["status",0,${JSON.stringify(sentinel)}]`);
  });

  it("bounds structural pattern compilation and rejects lossy patterns", () => {
    expect(() => compileLikePattern("x".repeat(MAX_SQL_PATTERN_CHARACTERS + 1))).toThrow(
      /LIKE pattern exceeds/,
    );
    expect(() => compileSimilarPattern("x".repeat(MAX_SQL_PATTERN_CHARACTERS + 1))).toThrow(
      /SIMILAR TO pattern exceeds/,
    );
    expect(() => compileSimilarPattern("\uD800")).toThrow(/unpaired surrogate/);
    expect(() => compileLikePattern("x", false, "")).toThrow(/one character/);
    expect(() => compileLikePattern("x", false, "ab")).toThrow(/one character/);
    expect(() => compileLikePattern("x", false, "😀")).not.toThrow();
  });

  it("applies a deterministic work ceiling to adversarial SQL patterns", () => {
    // The underscore keeps this on the general wildcard machine; pure leading/trailing-wildcard
    // literals intentionally use the constant-memory native substring fast path.
    const matcher = compileLikePattern(`%${"a".repeat(2_048)}_b`);
    expect(() => matcher.test("a".repeat(10_000))).toThrow(
      `${String(MAX_SQL_PATTERN_MATCH_STEPS)} deterministic steps`,
    );
  });
});
