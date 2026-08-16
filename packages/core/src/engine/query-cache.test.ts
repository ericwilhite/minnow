import { describe, expect, it } from "vitest";
import { copyQueryResult, queryResultMemoKey } from "./query-cache.js";

describe("query result cache helpers", () => {
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
    expect(copy.rows[0]?.at).toEqual(at);
    expect(copy.rows[0]?.at).not.toBe(at);
  });
});
