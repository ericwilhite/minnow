import { describe, expect, it } from "vitest";
import { rowToJson, toCsv, toInsertSql, toJson } from "./serialize.js";

const columns = ["id", "name", "at", "ok", "note"];
const rows = [
  {
    id: 1,
    name: 'say "hi", please',
    at: new Date("2026-01-05T10:30:00.250Z"),
    ok: true,
    note: null,
  },
  { id: 2, name: "two\nlines", at: null, ok: false, note: "x" },
];

describe("toCsv", () => {
  it("quotes fields that carry the delimiter, quotes, or newlines, and leaves NULL empty", () => {
    expect(toCsv(columns, rows)).toBe(
      'id,name,at,ok,note\n1,"say ""hi"", please",2026-01-05T10:30:00.250Z,true,\n2,"two\nlines",,false,x\n',
    );
  });

  it("uses a tab delimiter for spreadsheet paste, quoting only what tabs need", () => {
    expect(toCsv(["a", "b"], [{ a: "x,y", b: "p\tq" }], "\t")).toBe('a\tb\nx,y\t"p\tq"\n');
  });
});

describe("toJson", () => {
  it("writes an array of objects in column order with ISO dates", () => {
    expect(JSON.parse(toJson(columns, rows))).toEqual([
      { id: 1, name: 'say "hi", please', at: "2026-01-05T10:30:00.250Z", ok: true, note: null },
      { id: 2, name: "two\nlines", at: null, ok: false, note: "x" },
    ]);
  });

  it("writes one row as a bare object", () => {
    expect(JSON.parse(rowToJson(["a", "b"], { a: 1, b: null }))).toEqual({ a: 1, b: null });
  });
});

describe("toInsertSql", () => {
  it("types every literal by its value and quotes names that need it", () => {
    expect(
      toInsertSql(
        "odd names",
        ["id", "when", "x y"],
        [{ id: 1, when: new Date(0), "x y": "it's" }],
      ),
    ).toBe(
      `INSERT INTO "odd names" (id, when, "x y") VALUES (1, TIMESTAMP '1970-01-01T00:00:00.000Z', 'it''s');`,
    );
    expect(toInsertSql("t", ["a"], [{ a: null }, { a: false }])).toBe(
      "INSERT INTO t (a) VALUES (NULL);\nINSERT INTO t (a) VALUES (FALSE);",
    );
  });
});
