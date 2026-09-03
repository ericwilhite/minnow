import { describe, expect, it } from "vitest";
import { columnsOf } from "./columns.js";

describe("columnsOf", () => {
  it("types each column by its values, skipping NULLs", () => {
    expect(
      columnsOf({
        columns: ["n", "b", "d", "s", "empty"],
        columnDomains: [null, null, null, null, null],
        rows: [
          { n: null, b: true, d: new Date(0), s: "x", empty: null },
          { n: 2, b: false, d: null, s: "y", empty: null },
        ],
      }),
    ).toEqual([
      { name: "n", type: "number" },
      { name: "b", type: "boolean" },
      { name: "d", type: "datetime" },
      { name: "s", type: "string" },
      { name: "empty" },
    ]);
  });

  it("falls back to text when a column mixes types", () => {
    expect(
      columnsOf({ columns: ["v"], columnDomains: [null], rows: [{ v: 1 }, { v: "one" }] }),
    ).toEqual([{ name: "v", type: "string" }]);
  });

  it("labels a column with its declared domain", () => {
    expect(
      columnsOf({
        columns: ["price", "tags", "doc"],
        columnDomains: [
          { kind: "numeric", precision: 10, scale: 2 },
          { kind: "array", element: "text" },
          { kind: "jsonb" },
        ],
        rows: [{ price: 1.5, tags: "[]", doc: "{}" }],
      }),
    ).toEqual([
      { name: "price", type: "number", label: "NUMERIC(10,2)" },
      { name: "tags", type: "string", label: "TEXT[]" },
      { name: "doc", type: "string", label: "JSONB" },
    ]);
  });
});
