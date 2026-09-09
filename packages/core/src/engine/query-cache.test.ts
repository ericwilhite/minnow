import { describe, expect, it } from "vitest";
import { externalizeQueryResult, markQueryResultExternal } from "./query.js";
import { exactNumericValue, protectedSqlTextValue } from "./sql-domains.js";
import { copyQueryResult, queryResultMemoKey } from "./query-cache.js";

describe("query result cache helpers", () => {
  it("preserves the externalization boundary across defensive cache copies", () => {
    const publicText = exactNumericValue("12.50");
    if (publicText === null) throw new Error("Expected a numeric domain value");
    const internal = {
      columns: ["text", "numeric"],
      columnDomains: [null, null],
      rows: [{ text: protectedSqlTextValue(publicText), numeric: exactNumericValue("42.25") }],
    };
    // Copying an internal result must still allow its first conversion.
    const external = externalizeQueryResult(copyQueryResult(internal));
    expect(external.rows).toEqual([{ text: publicText, numeric: "42.25" }]);
    // Copying a public result must not interpret a legal TEXT tag prefix a second time.
    const cached = copyQueryResult(external);
    expect(externalizeQueryResult(cached)).toBe(cached);
    expect(cached.rows).toEqual(external.rows);
    const marked = markQueryResultExternal({
      ...external,
      rows: [{ text: publicText, numeric: "42.25" }],
    });
    const markedCopy = copyQueryResult(marked);
    expect(externalizeQueryResult(markedCopy)).toBe(markedCopy);
  });

  it("encodes parameter tuples without delimiter collisions", () => {
    const first = queryResultMemoKey("SELECT ? AS x, ? AS y", ["a\u0001string:b", "c"]);
    const second = queryResultMemoKey("SELECT ? AS x, ? AS y", ["a", "b\u0001string:c"]);
    expect(first).not.toBe(second);
  });

  it("distinguishes result-observable parameter types and signed zero", () => {
    const sql = "SELECT ? AS value";
    expect(queryResultMemoKey(sql, [new Date(0)])).not.toBe(queryResultMemoKey(sql, [0]));
    expect(queryResultMemoKey(sql, [-0])).not.toBe(queryResultMemoKey(sql, [0]));
  });

  it("copies Dates and special result-column names defensively", () => {
    const at = new Date(0);
    const result = {
      columns: ["__proto__", "at"],
      columnDomains: [null, null],
      rows: [
        Object.fromEntries([
          ["__proto__", "kept"],
          ["at", at],
        ]),
      ],
    };
    const copy = copyQueryResult(result);
    expect(Object.hasOwn(copy.rows[0] ?? {}, "__proto__")).toBe(true);
    expect(Reflect.get(copy.rows[0] ?? {}, "__proto__")).toBe("kept");
    expect(Object.getPrototypeOf(copy.rows[0])).toBe(Object.prototype);
    expect(copy.rows[0]?.at).toEqual(at);
    expect(copy.rows[0]?.at).not.toBe(at);
    // A Date under the special name is replaced as an own property, not assigned through the setter.
    const dated = copyQueryResult({
      columns: ["__proto__"],
      columnDomains: [null],
      rows: [Object.fromEntries([["__proto__", at]])],
    });
    expect(Object.hasOwn(dated.rows[0] ?? {}, "__proto__")).toBe(true);
    expect(Reflect.get(dated.rows[0] ?? {}, "__proto__")).toEqual(at);
    expect(Reflect.get(dated.rows[0] ?? {}, "__proto__")).not.toBe(at);
    expect(Object.getPrototypeOf(dated.rows[0])).toBe(Object.prototype);
  });

  it("copies rows independently of the cached originals", () => {
    const result = {
      columns: ["n", "s"],
      columnDomains: [null, null],
      rows: [
        { n: 1, s: "a" },
        { n: 2, s: null },
      ],
    };
    const copy = copyQueryResult(result);
    expect(copy).toEqual(result);
    expect(copy.rows[0]).not.toBe(result.rows[0]);
    (copy.rows[0] as Record<string, unknown>).n = 99;
    expect(result.rows[0]?.n).toBe(1);
  });
});
