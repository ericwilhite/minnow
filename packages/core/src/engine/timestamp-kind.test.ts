/**
 * A column declared TIMESTAMP (without time zone) beside one declared TIMESTAMPTZ. Both hold the
 * same UTC instant and compare, sort, and do arithmetic alike; only their SQL text differs, as
 * PostgreSQL's does in a UTC session: `2024-01-02 10:00:00` against `2024-01-02 10:00:00+00`,
 * and `"2024-01-02T10:00:00"` against `"2024-01-02T10:00:00+00:00"` in JSON. The rendering paths
 * themselves are diffed against PGlite in sql-conformance.test.ts; this suite pins the catalog
 * plumbing, the forms PostgreSQL spells differently, and the schema DSL.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { planMigration, column, schema, table, view, type InferRow } from "./schema.js";
import { deserializeSchema, serializeSchema } from "./schema-wire.js";
import { expectTypeOf } from "vitest";

const OPTIONS = { autoCompact: false, autoCollect: false } as const;
const LOCAL = "2024-01-02 10:00:00";
const ZONED = "2024-01-02 10:00:00+00";

async function stamped(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
  await database.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a TIMESTAMP, b TIMESTAMPTZ, note TEXT)",
  );
  await database.execute(
    "INSERT INTO t VALUES (1, '2024-01-02 10:00:00', '2024-01-02 10:00:00', 'x'), (2, '2024-03-04 05:06:07.5', NULL, 'y')",
  );
  return database;
}

async function kinds(database: MinnowDatabase, name: string): Promise<unknown> {
  const catalog = await database.introspect();
  const record = [...catalog.tables, ...catalog.views].find((candidate) => candidate.name === name);
  return record?.columns.map((entry) => [entry.name, entry.withoutTimeZone === true]);
}

describe("declaring the kind", () => {
  it("records TIMESTAMP spellings as without time zone and TIMESTAMPTZ spellings as instants", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
    await database.execute(
      "CREATE TABLE spellings (plain TIMESTAMP, precise TIMESTAMP(3), spelled TIMESTAMP WITHOUT TIME ZONE, precise_spelled TIMESTAMP(6) WITHOUT TIME ZONE, legacy DATETIME, zoned TIMESTAMPTZ, zoned_precise TIMESTAMPTZ(3), zoned_spelled TIMESTAMP WITH TIME ZONE, zoned_precise_spelled TIMESTAMP(3) WITH TIME ZONE, stamps TIMESTAMP[])",
    );
    expect(await kinds(database, "spellings")).toEqual([
      ["plain", true],
      ["precise", true],
      ["spelled", true],
      ["precise_spelled", true],
      ["legacy", true],
      ["zoned", false],
      ["zoned_precise", false],
      ["zoned_spelled", false],
      ["zoned_precise_spelled", false],
      ["stamps", false],
    ]);
    const listed = (await database.listTables()).find(({ name }) => name === "spellings");
    expect(listed?.columns.map(({ name, withoutTimeZone }) => [name, withoutTimeZone])).toEqual([
      ["plain", true],
      ["precise", true],
      ["spelled", true],
      ["precise_spelled", true],
      ["legacy", true],
      ["zoned", undefined],
      ["zoned_precise", undefined],
      ["zoned_spelled", undefined],
      ["zoned_precise_spelled", undefined],
      ["stamps", undefined],
    ]);
    await database.close();
  });

  it("carries the kind through ADD COLUMN, CREATE TABLE AS, views, and createTable", async () => {
    const database = await stamped();
    await database.execute("ALTER TABLE t ADD COLUMN c TIMESTAMP");
    await database.execute("ALTER TABLE t ADD COLUMN d TIMESTAMPTZ");
    expect(await kinds(database, "t")).toEqual([
      ["id", false],
      ["a", true],
      ["b", false],
      ["note", false],
      ["c", true],
      ["d", false],
    ]);
    // PostgreSQL's CREATE TABLE AS keeps each output's type: a TIMESTAMP column, literal, or cast
    // stays one, and an instant stays an instant.
    await database.execute(
      "CREATE TABLE copied AS SELECT a, b, CAST(b AS TIMESTAMP) AS cast_local, TIMESTAMP '2024-01-01' AS literal, a + INTERVAL '1 hour' AS later, COALESCE(a, b) AS common FROM t",
    );
    expect(await kinds(database, "copied")).toEqual([
      ["a", true],
      ["b", false],
      ["cast_local", true],
      ["literal", true],
      ["later", true],
      ["common", false],
    ]);
    expect((await database.query("SELECT a::text AS a, b::text AS b FROM copied")).rows).toEqual([
      { a: LOCAL, b: ZONED },
      { a: "2024-03-04 05:06:07.5", b: null },
    ]);
    await database.execute("CREATE VIEW v AS SELECT id, a, b FROM t");
    expect(await kinds(database, "v")).toEqual([
      ["id", false],
      ["a", true],
      ["b", false],
    ]);
    await database.createTable({
      name: "api",
      columns: [
        { name: "local_at", type: "datetime", withoutTimeZone: true, nullable: true },
        { name: "zoned_at", type: "datetime", nullable: true },
      ],
    });
    expect(await kinds(database, "api")).toEqual([
      ["local_at", true],
      ["zoned_at", false],
    ]);
    await expect(
      database.createTable({
        name: "misdeclared",
        columns: [{ name: "x", type: "string", withoutTimeZone: true }],
      }),
    ).rejects.toThrow(/TIMESTAMP without time zone requires a datetime column/);
    await database.close();
  });

  it("reads a CAST target's zone after its precision", async () => {
    const database = await stamped();
    expect(
      (
        await database.query(
          "SELECT CAST(b AS TIMESTAMP(3) WITH TIME ZONE)::text AS zoned, CAST(b AS TIMESTAMP(3))::text AS local, CAST(b AS DATETIME)::text AS legacy FROM t WHERE id = 1",
        )
      ).rows,
    ).toEqual([{ zoned: ZONED, local: LOCAL, legacy: LOCAL }]);
    await database.close();
  });
});

describe("rendering a TIMESTAMP column", () => {
  it("drops the zone wherever the value becomes text or JSON", async () => {
    const database = await stamped();
    const sql =
      "SELECT a::text AS cast_a, CAST(b AS VARCHAR) AS cast_b, 'at ' || a AS concat_a, b || '!' AS concat_b, CONCAT(a, '|', b) AS concat_fn, CONCAT_WS(',', a, b) AS ws, FORMAT('%s %L', a, b) AS formatted, QUOTE_LITERAL(a) AS quoted, TO_JSON(a) AS json_a, TO_JSON(b) AS json_b, JSON_BUILD_OBJECT('a', a, 'b', b) AS object, JSON_BUILD_ARRAY(a, b) AS array FROM t WHERE id = 1";
    expect((await database.query(sql)).rows).toEqual([
      {
        cast_a: LOCAL,
        cast_b: ZONED,
        concat_a: `at ${LOCAL}`,
        concat_b: `${ZONED}!`,
        concat_fn: `${LOCAL}|${ZONED}`,
        ws: `${LOCAL},${ZONED}`,
        formatted: `${LOCAL} '${ZONED}'`,
        quoted: `'${LOCAL}'`,
        json_a: '"2024-01-02T10:00:00"',
        json_b: '"2024-01-02T10:00:00+00:00"',
        object: '{"a":"2024-01-02T10:00:00","b":"2024-01-02T10:00:00+00:00"}',
        array: '["2024-01-02T10:00:00","2024-01-02T10:00:00+00:00"]',
      },
    ]);
    // Arrays are JSON text in Minnow; their members follow the JSON rendering.
    expect(
      (
        await database.query(
          "SELECT ARRAY_AGG(a ORDER BY id) AS agg, JSON_AGG(a ORDER BY id) AS json_agg FROM t",
        )
      ).rows,
    ).toEqual([
      {
        agg: '["2024-01-02T10:00:00","2024-03-04T05:06:07.5"]',
        json_agg: '["2024-01-02T10:00:00","2024-03-04T05:06:07.5"]',
      },
    ]);
    expect((await database.query("SELECT ARRAY[a, b] AS built FROM t WHERE id = 1")).rows).toEqual([
      { built: '["2024-01-02T10:00:00","2024-01-02T10:00:00+00:00"]' },
    ]);
    expect((await database.query("SELECT ROW_TO_JSON(t) AS r FROM t WHERE id = 1")).rows).toEqual([
      { r: '{"id":1,"a":"2024-01-02T10:00:00","b":"2024-01-02T10:00:00+00:00","note":"x"}' },
    ]);
    await database.close();
  });

  it("follows the column through views, CTEs, derived tables, set operations, subqueries, and windows", async () => {
    const database = await stamped();
    await database.execute("CREATE VIEW v AS SELECT id, a, b FROM t");
    const cases: Array<[string, unknown[]]> = [
      ["SELECT a::text AS a, b::text AS b FROM v WHERE id = 1", [{ a: LOCAL, b: ZONED }]],
      [
        "WITH s AS (SELECT id, a, b FROM t) SELECT a::text AS a, b::text AS b FROM s WHERE id = 1",
        [{ a: LOCAL, b: ZONED }],
      ],
      ["SELECT x::text AS x FROM (SELECT a AS x FROM t WHERE id = 1) d", [{ x: LOCAL }]],
      [
        "SELECT u::text AS u FROM (SELECT a AS u FROM t WHERE id = 1 UNION ALL SELECT a FROM t WHERE id = 2) s ORDER BY u",
        [{ u: LOCAL }, { u: "2024-03-04 05:06:07.5" }],
      ],
      ["SELECT (SELECT MAX(a) FROM t)::text AS m", [{ m: "2024-03-04 05:06:07.5" }]],
      [
        "SELECT id, CAST(LAG(a) OVER (ORDER BY id) AS TEXT) AS prev, 'x' || FIRST_VALUE(a) OVER (ORDER BY id) AS first FROM t ORDER BY id",
        [
          { id: 1, prev: null, first: `x${LOCAL}` },
          { id: 2, prev: LOCAL, first: `x${LOCAL}` },
        ],
      ],
      ["SELECT MIN(a)::text AS m, 'x' || MAX(b) AS n FROM t", [{ m: LOCAL, n: `x${ZONED}` }]],
      // PostgreSQL's common type of timestamp and timestamptz is timestamptz.
      [
        "SELECT CASE WHEN id = 1 THEN a ELSE b END::text AS c, COALESCE(a, b)::text AS d, GREATEST(a, TIMESTAMP '2024-02-01')::text AS g FROM t WHERE id = 1",
        [{ c: ZONED, d: ZONED, g: "2024-02-01 00:00:00" }],
      ],
      [
        "SELECT (a + INTERVAL '1 hour')::text AS later, DATE_TRUNC('day', a)::text AS day FROM t WHERE id = 1",
        [{ later: "2024-01-02 11:00:00", day: "2024-01-02 00:00:00" }],
      ],
    ];
    for (const [sql, expected] of cases) {
      expect((await database.query(sql, { memoize: false })).rows, sql).toEqual(expected);
    }
    await database.close();
  });

  it("renders it inside generated columns, CHECK constraints, and RETURNING", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
    await database.execute(
      "CREATE TABLE g (id INTEGER PRIMARY KEY, at TIMESTAMP, label TEXT GENERATED ALWAYS AS ('at ' || at) STORED, CHECK (at IS NULL OR at::text NOT LIKE '%+00'))",
    );
    await database.execute("INSERT INTO g (id, at) VALUES (1, '2024-01-02 10:00:00')");
    expect((await database.query("SELECT label FROM g")).rows).toEqual([{ label: `at ${LOCAL}` }]);
    const updated = await database.execute(
      "UPDATE g SET at = at + INTERVAL '1 hour' WHERE id = 1 RETURNING at::text AS t, label",
    );
    expect(updated).toMatchObject({
      returnedRows: [{ t: "2024-01-02 11:00:00", label: "at 2024-01-02 11:00:00" }],
    });
    await database.close();
  });
});

describe("what does not change", () => {
  it("compares, orders, binds, and returns the same instants", async () => {
    const database = await stamped();
    expect(
      (
        await database.query(
          "SELECT a = b AS same, a - b AS diff, a < CAST('2024-01-02 10:00:01+00' AS TIMESTAMPTZ) AS earlier FROM t WHERE id = 1",
        )
      ).rows,
    ).toEqual([{ same: true, diff: "00:00:00", earlier: true }]);
    const rows = (await database.query("SELECT a, b FROM t ORDER BY a")).rows;
    expect(rows[0]?.a).toEqual(new Date("2024-01-02T10:00:00.000Z"));
    expect(rows[0]?.b).toEqual(new Date("2024-01-02T10:00:00.000Z"));
    expect(
      (
        await database.query("SELECT id FROM t WHERE a = ?", {
          params: [new Date("2024-03-04T05:06:07.500Z")],
        })
      ).rows,
    ).toEqual([{ id: 2 }]);
    expect((await database.query("SELECT id FROM t WHERE a = '2024-01-02 10:00:00'")).rows).toEqual(
      [{ id: 1 }],
    );
    await database.close();
  });
});

describe("the schema DSL", () => {
  const events = table("events", {
    id: column.integer().unique(),
    occurred: column.timestamp(),
    recorded: column.datetime().nullable(),
  });

  it("declares TIMESTAMP with column.timestamp() and keeps column.datetime() an instant", async () => {
    expectTypeOf<InferRow<typeof events>>().toEqualTypeOf<{
      id: number;
      occurred: Date;
      recorded: Date | null;
    }>();
    expect(events.columns.occurred).toMatchObject({ type: "datetime", withoutTimeZone: true });
    expect("withoutTimeZone" in events.columns.recorded).toBe(false);

    const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
    await database.migrate(schema([events]));
    expect(await kinds(database, "events")).toEqual([
      ["id", false],
      ["occurred", true],
      ["recorded", false],
    ]);
    await database.insertBatch("events", [
      { id: 1, occurred: new Date("2024-01-02T10:00:00Z"), recorded: null },
    ]);
    expect((await database.query("SELECT occurred::text AS t FROM events")).rows).toEqual([
      { t: LOCAL },
    ]);
    expect((await database.migrate(schema([events]))).steps).toEqual([]);

    // An added column keeps its kind too.
    const evolved = table("events", {
      ...events.columns,
      noticed: column.timestamp().nullable(),
    });
    await database.migrate(schema([evolved]));
    expect((await kinds(database, "events")) as unknown[]).toContainEqual(["noticed", true]);
    await database.close();
  });

  it("refuses to change a column's kind, and refuses it on a non-datetime column", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
    await database.migrate(schema([events]));
    const catalog = await database.introspect();
    const flipped = table("events", {
      id: column.integer().unique(),
      occurred: column.datetime(),
      recorded: column.timestamp().nullable(),
    });
    expect(() => planMigration(catalog, schema([flipped]))).toThrow(
      "Column time zones cannot change: events.occurred is column.timestamp(), schema says column.datetime()",
    );
    await database.close();
    expect(() =>
      table("bad", {
        id: column.integer().unique(),
        // Only reachable through untyped metadata: rebuild a builder with the mark on a string.
        x: { ...column.string(), withoutTimeZone: true as const },
      }),
    ).toThrow("TIMESTAMP without time zone requires a datetime column: bad.x");
  });

  it("round-trips the kind through the wire format", () => {
    const restored = deserializeSchema(serializeSchema(schema([events])));
    const occurred = restored.tables[0]?.columns.occurred;
    const recorded = restored.tables[0]?.columns.recorded;
    expect(occurred?.withoutTimeZone).toBe(true);
    expect(recorded?.withoutTimeZone).toBeUndefined();
  });

  it("checks a declared view's kinds against its query", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
    const matching = view("recent", {
      sql: "SELECT id, occurred, recorded FROM events",
      columns: {
        id: column.integer(),
        occurred: column.timestamp(),
        recorded: column.datetime().nullable(),
      },
    });
    await database.migrate(schema([events], { views: [matching] }));
    expect(await kinds(database, "recent")).toEqual([
      ["id", false],
      ["occurred", true],
      ["recorded", false],
    ]);
    const lying = view("lying", {
      sql: "SELECT id, occurred FROM events",
      columns: { id: column.integer(), occurred: column.datetime() },
    });
    await expect(database.migrate(schema([events], { views: [matching, lying] }))).rejects.toThrow(
      /View lying declares \(id number integer, occurred datetime\) but its query produces \(id number integer, occurred datetime without time zone\)/,
    );
    await database.close();
  });
});
