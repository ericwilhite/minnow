import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { describe, expect, it } from "vitest";
import { toCatalog, type TableInfo } from "./catalog.js";
import {
  describeFilter,
  isComplete,
  operatorHint,
  operatorsFor,
  renderFilters,
  type Filter,
} from "./filters.js";

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

  it("puts the substring searches next to equality, where they are looked for", () => {
    expect(operatorsFor("string").slice(0, 4)).toEqual(["=", "!=", "contains", "starts with"]);
  });

  it("keeps substring search off types LIKE cannot sensibly scan", () => {
    expect(operatorsFor("number")).not.toContain("contains");
    expect(operatorsFor("datetime")).not.toContain("starts with");
  });
});

describe("substring filters", () => {
  // A bare LIKE is an exact match, so typing `crea` into one finds nothing — correct SQL that
  // reads as a broken filter. `contains` adds the wildcards instead of expecting them.
  it("wraps a contains value in wildcards", () => {
    expect(
      renderFilters("events", [filter({ column: "kind", operator: "contains", values: ["crea"] })]),
    ).toBe("(events.kind LIKE '%crea%')");
  });

  it("anchors starts with to the front only", () => {
    expect(
      renderFilters("events", [
        filter({ column: "kind", operator: "starts with", values: ["crea"] }),
      ]),
    ).toBe("(events.kind LIKE 'crea%')");
  });

  it("leaves a hand-written pattern exactly as typed", () => {
    expect(
      renderFilters("events", [filter({ column: "kind", operator: "like", values: ["%crea%"] })]),
    ).toBe("(events.kind LIKE '%crea%')");
    expect(
      renderFilters("events", [filter({ column: "kind", operator: "like", values: ["crea"] })]),
    ).toBe("(events.kind LIKE 'crea')");
  });

  it("still escapes quotes in the value it wraps", () => {
    expect(
      renderFilters("events", [filter({ column: "kind", operator: "contains", values: ["O'Ha"] })]),
    ).toBe("(events.kind LIKE '%O''Ha%')");
  });

  it("reads plainly on the chip", () => {
    expect(describeFilter(filter({ column: "kind", operator: "contains", values: ["crea"] }))).toBe(
      "kind contains crea",
    );
    expect(
      describeFilter(filter({ column: "kind", operator: "starts with", values: ["crea"] })),
    ).toBe("kind starts with crea");
  });

  it("hints the pattern shape for like and a bare word for contains", () => {
    expect(operatorHint("like")).toBe("%crea%");
    expect(operatorHint("contains")).toBe("crea");
    expect(operatorHint("=")).toBeUndefined();
  });
});

/**
 * The wildcard behaviour only matters if the engine agrees, and `LIKE` semantics are exactly the
 * kind of thing a string assertion can get confidently wrong.
 */
describe("substring filters against the engine", () => {
  async function seeded(): Promise<{ database: MinnowDatabase; table: TableInfo }> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "events",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "kind", type: "string" },
      ],
    });
    const kinds = ["created", "paid", "cancelled", "re_created", "CREATED", "it's here"];
    for (const [id, kind] of kinds.entries()) await database.insert("events", { id, kind });
    const [table] = toCatalog(await database.listTables());
    if (table === undefined) throw new Error("missing table");
    return { database, table };
  }

  async function search(operator: Filter["operator"], value: string): Promise<string[]> {
    const { database, table } = await seeded();
    const where = renderFilters(table.name, [
      { column: "kind", type: "string", operator, values: [value] },
    ]);
    const result = await database.query(
      `SELECT * FROM events${where === undefined ? "" : ` WHERE ${where}`} ORDER BY events.id`,
    );
    return result.rows.map((row) => String(row.kind));
  }

  it("finds the substring a bare LIKE misses — the reported case", async () => {
    // Typing a fragment into a raw LIKE matches only rows equal to it. Correct SQL, wrong result.
    expect(await search("like", "crea")).toEqual([]);
    // `contains` supplies the wildcards, so the same input finds what it looks like it should.
    expect(await search("contains", "crea")).toEqual(["created", "re_created"]);
  });

  it("anchors starts with to the front", async () => {
    expect(await search("starts with", "crea")).toEqual(["created"]);
    expect(await search("starts with", "reated")).toEqual([]);
  });

  it("leaves a hand-written pattern alone", async () => {
    expect(await search("like", "%crea%")).toEqual(["created", "re_created"]);
    expect(await search("like", "crea%")).toEqual(["created"]);
  });

  it("keeps a quote in the value from breaking the statement", async () => {
    expect(await search("contains", "it's")).toEqual(["it's here"]);
  });

  it("matches case-sensitively, as the engine's LIKE does", async () => {
    expect(await search("contains", "CREA")).toEqual(["CREATED"]);
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
