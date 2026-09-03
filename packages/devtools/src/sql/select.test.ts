import { MinnowDatabase, type QueryRow } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { describe, expect, it } from "vitest";
import { toCatalog, type TableInfo } from "../explorer/catalog.js";
import type { Filter } from "../explorer/filters.js";
import { buildCountQuery, buildPageQuery, cursorFrom, pagingMode, type Sort } from "./select.js";

const keyed: TableInfo = {
  name: "people",
  uniqueKey: "id",
  columns: [
    { name: "id", type: "number", nullable: false, isUniqueKey: true },
    { name: "name", type: "string", nullable: false, isUniqueKey: false },
    { name: "city", type: "string", nullable: true, isUniqueKey: false },
    { name: "joined", type: "datetime", nullable: false, isUniqueKey: false },
  ],
};

/** A datetime's instant; `String(date)` drops the milliseconds, which is what these tests check. */
const instant = (value: QueryRow[string] | undefined): number =>
  value instanceof Date ? value.getTime() : new Date(String(value)).getTime();

const keyless: TableInfo = {
  name: "events",
  columns: [{ name: "kind", type: "string", nullable: false, isUniqueKey: false }],
};

describe("pagingMode", () => {
  it("cursors by default, because the key alone is a total order", () => {
    expect(pagingMode(keyed)).toBe("keyset");
    expect(pagingMode(keyed, { column: "id", direction: "desc" })).toBe("keyset");
    expect(pagingMode(keyed, { column: "name", direction: "asc" })).toBe("keyset");
  });

  it("falls back without a key to break ties", () => {
    expect(pagingMode(keyless)).toBe("offset");
  });

  it("falls back on a nullable column, whose NULLs no comparison can address", () => {
    expect(pagingMode(keyed, { column: "city", direction: "asc" })).toBe("offset");
  });

  it("cursors a datetime, whose literal carries the full instant", () => {
    expect(pagingMode(keyed, { column: "joined", direction: "asc" })).toBe("keyset");
  });
});

describe("buildPageQuery", () => {
  it("orders by the key, qualified like every other column reference", () => {
    expect(buildPageQuery({ table: keyed, filters: [], limit: 50 })).toBe(
      "SELECT * FROM people ORDER BY people.id LIMIT 50",
    );
  });

  it("sorts a keyword-named column, which only qualifying makes parseable", () => {
    const awkward: TableInfo = {
      name: "logs",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number", nullable: false, isUniqueKey: true },
        { name: "case", type: "string", nullable: false, isUniqueKey: false },
      ],
    };
    // Bare, `case` starts a CASE expression; qualified, the engine resolves it against the
    // wildcard's source table.
    expect(
      buildPageQuery({
        table: awkward,
        filters: [],
        sort: { column: "case", direction: "asc" },
        limit: 5,
      }),
    ).toBe("SELECT * FROM logs ORDER BY logs.case, logs.id LIMIT 5");
  });

  it("appends the key so a sort on another column is still a total order", () => {
    expect(
      buildPageQuery({
        table: keyed,
        filters: [],
        sort: { column: "name", direction: "desc" },
        limit: 50,
      }),
    ).toBe("SELECT * FROM people ORDER BY people.name DESC, people.id DESC LIMIT 50");
  });

  it("writes the row-value comparison out by hand", () => {
    expect(
      buildPageQuery({
        table: keyed,
        filters: [],
        sort: { column: "name", direction: "asc" },
        cursor: ["Ada", 7],
        limit: 50,
      }),
    ).toBe(
      "SELECT * FROM people WHERE (people.name > 'Ada' OR (people.name = 'Ada' AND people.id > 7))" +
        " ORDER BY people.name, people.id LIMIT 50",
    );
  });

  it("collapses to a plain inequality when the sort is the key", () => {
    expect(
      buildPageQuery({
        table: keyed,
        filters: [],
        sort: { column: "id", direction: "desc" },
        cursor: [12],
        limit: 10,
      }),
    ).toBe("SELECT * FROM people WHERE people.id < 12 ORDER BY people.id DESC LIMIT 10");
  });

  it("ignores a cursor it cannot honour and counts from the start instead", () => {
    expect(
      buildPageQuery({ table: keyless, filters: [], cursor: ["x"], offset: 100, limit: 25 }),
    ).toBe("SELECT * FROM events LIMIT 25 OFFSET 100");
  });

  it("combines filters with the cursor", () => {
    const filters: Filter[] = [
      { column: "city", type: "string", operator: "=", values: ["London"] },
      { column: "name", type: "string", operator: "like", values: ["A%"] },
    ];
    expect(buildPageQuery({ table: keyed, filters, cursor: [4], limit: 20 })).toBe(
      "SELECT * FROM people WHERE (people.city = 'London') AND (people.name LIKE 'A%')" +
        " AND people.id > 4 ORDER BY people.id LIMIT 20",
    );
  });

  it("quotes a name that is not a bare identifier, on the table and the column alike", () => {
    const odd: TableInfo = {
      name: "odd names",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number", nullable: false, isUniqueKey: true },
        { name: "order date", type: "datetime", nullable: false, isUniqueKey: false },
      ],
    };
    expect(
      buildPageQuery({
        table: odd,
        filters: [],
        sort: { column: "order date", direction: "desc" },
        cursor: [new Date("2026-01-05T10:30:00.250Z"), 3],
        limit: 5,
      }),
    ).toBe(
      'SELECT * FROM "odd names" WHERE ("odd names"."order date" < TIMESTAMP \'2026-01-05T10:30:00.250Z\'' +
        ' OR ("odd names"."order date" = TIMESTAMP \'2026-01-05T10:30:00.250Z\' AND "odd names".id < 3))' +
        ' ORDER BY "odd names"."order date" DESC, "odd names".id DESC LIMIT 5',
    );
    expect(buildCountQuery(odd, [])).toBe('SELECT COUNT(*) AS row_count FROM "odd names"');
  });

  it("escapes a quote in a filter value rather than breaking the statement", () => {
    const filters: Filter[] = [
      { column: "name", type: "string", operator: "=", values: ["O'Hara"] },
    ];
    expect(buildPageQuery({ table: keyed, filters, limit: 5 })).toContain("'O''Hara'");
  });
});

describe("buildCountQuery", () => {
  it("counts with the same filters and no ordering", () => {
    expect(buildCountQuery(keyed, [])).toBe("SELECT COUNT(*) AS row_count FROM people");
    expect(
      buildCountQuery(keyed, [{ column: "city", type: "string", operator: "is null", values: [] }]),
    ).toBe("SELECT COUNT(*) AS row_count FROM people WHERE (people.city IS NULL)");
  });
});

/**
 * The statements above have to survive the real parser, and paging has to visit every row exactly
 * once. A wrong cursor predicate silently skips or repeats rows, which no unit test on strings
 * would catch.
 */
describe("against the engine", () => {
  const rowCount = 500;

  async function seeded(): Promise<{ database: MinnowDatabase; table: TableInfo }> {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 64 });
    await database.createTable({
      name: "people",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "name", type: "string" },
        { name: "city", type: "string", nullable: true },
        { name: "joined", type: "datetime" },
      ],
    });
    for (let index = 0; index < rowCount; index += 1) {
      await database.insert("people", {
        id: index,
        // Deliberately repetitive, so the key tiebreak is exercised rather than incidental.
        name: `person ${String(index % 25).padStart(2, "0")}`,
        city: index % 7 === 0 ? null : `city ${String(index % 5)}`,
        joined: new Date(Date.UTC(2026, 0, 1 + (index % 28))),
      });
    }
    const [definition] = toCatalog(await database.listTables());
    if (definition === undefined) throw new Error("missing table");
    return { database, table: definition };
  }

  async function readAll(
    database: MinnowDatabase,
    table: TableInfo,
    sort: Sort | undefined,
    filters: Filter[] = [],
  ): Promise<QueryRow[]> {
    const limit = 37; // Not a divisor of the row count, so the last page is partial.
    const collected: QueryRow[] = [];
    let cursor = undefined as ReturnType<typeof cursorFrom>;
    for (;;) {
      const sql = buildPageQuery({
        table,
        filters,
        ...(sort === undefined ? {} : { sort }),
        ...(cursor === undefined ? {} : { cursor }),
        offset: collected.length,
        limit,
      });
      const page = await database.query(sql);
      collected.push(...page.rows);
      if (page.rows.length < limit) return collected;
      const last = page.rows[page.rows.length - 1];
      cursor = last === undefined ? undefined : cursorFrom(last, table, sort);
    }
  }

  it("visits every row exactly once when cursoring by the key", async () => {
    const { database, table } = await seeded();
    const rows = await readAll(database, table, undefined);
    expect(rows).toHaveLength(rowCount);
    expect(new Set(rows.map((row) => row.id)).size).toBe(rowCount);
    expect(rows.map((row) => row.id)).toEqual(
      [...rows.map((row) => row.id)].sort((a, b) => Number(a) - Number(b)),
    );
  });

  it("visits every row exactly once when cursoring by a column full of ties", async () => {
    const { database, table } = await seeded();
    const rows = await readAll(database, table, { column: "name", direction: "asc" });
    expect(rows).toHaveLength(rowCount);
    expect(new Set(rows.map((row) => row.id)).size).toBe(rowCount);
    // Sorted by name, then by the key within each run of equal names.
    const names = rows.map((row) => String(row.name));
    expect(names).toEqual([...names].sort());
  });

  it("visits every row exactly once descending", async () => {
    const { database, table } = await seeded();
    const rows = await readAll(database, table, { column: "name", direction: "desc" });
    expect(new Set(rows.map((row) => row.id)).size).toBe(rowCount);
  });

  it("keeps NULLs when the fallback carries the paging", async () => {
    const { database, table } = await seeded();
    expect(pagingMode(table, { column: "city", direction: "asc" })).toBe("offset");
    const rows = await readAll(database, table, { column: "city", direction: "asc" });
    expect(new Set(rows.map((row) => row.id)).size).toBe(rowCount);
    expect(rows.filter((row) => row.city === null)).not.toHaveLength(0);
  });

  it("pages a filtered view and agrees with its own count", async () => {
    const { database, table } = await seeded();
    const filters: Filter[] = [
      { column: "city", type: "string", operator: "=", values: ["city 2"] },
    ];
    const rows = await readAll(database, table, undefined, filters);
    const counted = await database.query(buildCountQuery(table, filters));
    expect(rows.length).toBeGreaterThan(0);
    expect(counted.rows[0]?.row_count).toBe(rows.length);
  });

  it("cursors through a keyword-named column, which only qualifying makes parseable", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 64 });
    await database.createTable({
      name: "logs",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "case", type: "string" },
      ],
    });
    for (let index = 0; index < 120; index += 1) {
      await database.insert("logs", { id: index, case: `case ${String(index % 7)}` });
    }
    const [table] = toCatalog(await database.listTables());
    if (table === undefined) throw new Error("missing table");
    const sort: Sort = { column: "case", direction: "asc" };
    expect(pagingMode(table, sort)).toBe("keyset");
    const rows = await readAll(database, table, sort);
    expect(new Set(rows.map((row) => row.id)).size).toBe(120);
    const cases = rows.map((row) => String(row.case));
    expect(cases).toEqual([...cases].sort());
  });

  it("filters a datetime to the millisecond", async () => {
    const { database, table } = await seeded();
    const at = Date.UTC(2026, 0, 20, 10, 30, 0, 250);
    await database.insert("people", {
      id: rowCount,
      name: "late",
      city: null,
      joined: new Date(at),
    });
    const after: Filter[] = [
      { column: "joined", type: "datetime", operator: ">", values: [new Date(at - 1)] },
    ];
    const rows = await readAll(database, table, undefined, after);
    for (const row of rows) {
      expect(instant(row.joined)).toBeGreaterThan(at - 1);
    }
    expect(rows.map((row) => row.id)).toContain(rowCount);
    const exact: Filter[] = [
      { column: "joined", type: "datetime", operator: "=", values: [new Date(at)] },
    ];
    expect((await readAll(database, table, undefined, exact)).map((row) => row.id)).toEqual([
      rowCount,
    ]);
  });

  it("cursors through a datetime sort, visiting every row exactly once", async () => {
    const { database, table } = await seeded();
    const sort: Sort = { column: "joined", direction: "desc" };
    expect(pagingMode(table, sort)).toBe("keyset");
    const rows = await readAll(database, table, sort);
    expect(new Set(rows.map((row) => row.id)).size).toBe(rowCount);
    const times = rows.map((row) => instant(row.joined));
    expect(times).toEqual([...times].sort((a, b) => b - a));
  });

  it("pages a table whose names need quoting", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 16 });
    await database.createTable({
      name: "odd names",
      uniqueKey: "row id",
      columns: [
        { name: "row id", type: "number" },
        { name: "order date", type: "datetime" },
        { name: 'quo"te', type: "string", nullable: true },
      ],
    });
    for (let index = 0; index < 50; index += 1) {
      await database.insert("odd names", {
        "row id": index,
        "order date": new Date(Date.UTC(2026, 0, 1, index % 5)),
        'quo"te': index % 3 === 0 ? null : `v${String(index)}`,
      });
    }
    const [table] = toCatalog(await database.listTables());
    if (table === undefined) throw new Error("missing table");
    const sort: Sort = { column: "order date", direction: "asc" };
    expect(pagingMode(table, sort)).toBe("keyset");
    const rows = await readAll(database, table, sort, [
      { column: 'quo"te', type: "string", operator: "is not null", values: [] },
    ]);
    expect(rows.map((row) => row["row id"])).toHaveLength(33);
    expect(new Set(rows.map((row) => row["row id"])).size).toBe(33);
  });
});
