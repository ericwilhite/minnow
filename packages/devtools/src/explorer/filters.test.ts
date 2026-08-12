import { describe, expect, it } from "vitest";
import { toCatalog } from "./catalog.js";
import { describeFilter, isComplete, operatorsFor, renderFilters, type Filter } from "./filters.js";

const filter = (partial: Partial<Filter>): Filter => ({
  column: "name",
  type: "string",
  operator: "=",
  values: ["Ada"],
  ...partial,
});

describe("operatorsFor", () => {
  it("offers ordering comparisons only where they mean something", () => {
    expect(operatorsFor("boolean")).toEqual(["=", "!=", "in", "is null", "is not null"]);
    expect(operatorsFor("string")).toContain("like");
    expect(operatorsFor("number")).toContain("between");
    expect(operatorsFor("number")).not.toContain("like");
  });
});

describe("isComplete", () => {
  it("counts the values each operator needs", () => {
    expect(isComplete(filter({ operator: "is null", values: [] }))).toBe(true);
    expect(isComplete(filter({ operator: "=", values: [] }))).toBe(false);
    expect(isComplete(filter({ operator: "between", values: [1] }))).toBe(false);
    expect(isComplete(filter({ operator: "between", values: [1, 2] }))).toBe(true);
    expect(isComplete(filter({ operator: "in", values: [] }))).toBe(false);
    expect(isComplete(filter({ operator: "in", values: ["a"] }))).toBe(true);
  });
});

describe("renderFilters", () => {
  it("returns nothing when nothing is filtering, so WHERE can be omitted", () => {
    expect(renderFilters("people", [])).toBeUndefined();
  });

  it("renders each operator's SQL form", () => {
    expect(renderFilters("people", [filter({})])).toBe("(people.name = 'Ada')");
    expect(renderFilters("people", [filter({ operator: "like", values: ["A%"] })])).toBe(
      "(people.name LIKE 'A%')",
    );
    expect(renderFilters("people", [filter({ operator: "is null", values: [] })])).toBe(
      "(people.name IS NULL)",
    );
    expect(renderFilters("people", [filter({ operator: "in", values: ["a", "b"] })])).toBe(
      "(people.name IN ('a', 'b'))",
    );
    expect(
      renderFilters("people", [
        filter({ column: "score", type: "number", operator: "between", values: [1, 9] }),
      ]),
    ).toBe("(people.score BETWEEN 1 AND 9)");
  });

  it("parenthesises each filter so precedence cannot change the meaning", () => {
    expect(
      renderFilters("people", [filter({}), filter({ column: "city", values: ["London"] })]),
    ).toBe("(people.name = 'Ada') AND (people.city = 'London')");
  });

  it("drops an incomplete filter rather than compiling half of it", () => {
    expect(renderFilters("people", [filter({ values: [] })])).toBeUndefined();
    expect(renderFilters("people", [filter({}), filter({ column: "city", values: [] })])).toBe(
      "(people.name = 'Ada')",
    );
  });
});

describe("describeFilter", () => {
  it("reads as the chip label", () => {
    expect(describeFilter(filter({}))).toBe("name = Ada");
    expect(describeFilter(filter({ operator: "is not null", values: [] }))).toBe(
      "name is not null",
    );
    expect(
      describeFilter(
        filter({ column: "score", type: "number", operator: "between", values: [1, 9] }),
      ),
    ).toBe("score between 1 and 9");
    expect(describeFilter(filter({ operator: "in", values: ["a", "b"] }))).toBe("name in (a, b)");
  });
});

describe("toCatalog", () => {
  it("marks the unique key and defaults nullability to false", () => {
    expect(
      toCatalog([
        {
          name: "people",
          uniqueKey: "id",
          columns: [
            { name: "id", type: "number" },
            { name: "city", type: "string", nullable: true },
          ],
        },
      ]),
    ).toEqual([
      {
        name: "people",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number", nullable: false, isUniqueKey: true },
          { name: "city", type: "string", nullable: true, isUniqueKey: false },
        ],
      },
    ]);
  });

  it("leaves uniqueKey absent for a keyless table", () => {
    const [table] = toCatalog([{ name: "events", columns: [{ name: "kind", type: "string" }] }]);
    expect(table?.uniqueKey).toBeUndefined();
    expect(table?.columns[0]?.isUniqueKey).toBe(false);
  });
});
