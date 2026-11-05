/**
 * Under `compileWithDeclaredTypes` the parser records each numeric type's declared width
 * (`declaredTypeName`) for the new core's lowering (next/DECISIONS.md C5). Outside it, which is
 * everywhere the current engine compiles, nothing changes: no field, so no change to query
 * identity, stored schemas or results.
 */
import { describe, expect, it } from "vitest";
import type { Expression } from "../plan/model.js";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import {
  compileCheckExpression,
  compileDefaultExpression,
  compileQuery,
  compileStatement,
  compileWithDeclaredTypes,
} from "./query.js";
import { encodeQueryIdentity } from "./query-identity.js";

const WIDTHS_TABLE =
  "CREATE TABLE t (a SMALLINT, b INT2, c INTEGER, d INT, e INT4, f BIGINT, g INT8, " +
  "h REAL, i FLOAT4, j DOUBLE PRECISION, k FLOAT8, l FLOAT, m FLOAT(24), n FLOAT(25), " +
  "o SERIAL, p BIGSERIAL, q SMALLSERIAL, r TEXT, s NUMERIC(10, 2), t TIMESTAMP, u INT[])";

function createdColumns(sql: string) {
  const statement = compileStatement(sql);
  if (statement.kind !== "create-table") throw new Error("Expected CREATE TABLE");
  return Object.fromEntries(statement.columns.map((column) => [column.name, column]));
}

/** The CAST target literal of the single select item. */
function castTarget(sql: string): Extract<Expression, { kind: "literal" }> {
  const expression = compileQuery(sql).select[0]?.expression;
  const target = expression?.kind === "call" ? expression.arguments[1] : undefined;
  if (expression?.kind !== "call" || expression.name !== "CAST" || target?.kind !== "literal")
    throw new Error("Expected a CAST");
  return target;
}

describe("declared numeric type names for lowering", () => {
  it("records PostgreSQL's canonical name on every numeric column spelling", () => {
    const columns = compileWithDeclaredTypes(() => createdColumns(WIDTHS_TABLE));
    const names = Object.fromEntries(
      Object.entries(columns).map(([name, column]) => [name, column.declaredTypeName]),
    );
    expect(names).toEqual({
      a: "smallint",
      b: "smallint",
      c: "integer",
      d: "integer",
      e: "integer",
      f: "bigint",
      g: "bigint",
      h: "real",
      i: "real",
      j: "double precision",
      k: "double precision",
      l: "double precision",
      m: "real",
      n: "double precision",
      o: "integer",
      p: "bigint",
      q: "smallint",
      r: undefined,
      s: undefined,
      t: undefined,
      u: undefined,
    });
    // Every other field is what the current engine compiles.
    const plain = createdColumns(WIDTHS_TABLE);
    for (const [name, column] of Object.entries(columns))
      expect({ ...column, declaredTypeName: undefined }, name).toEqual(plain[name]);
  });

  it("records VARCHAR(n) and CHAR(n) widths (R17.4)", () => {
    const columns = compileWithDeclaredTypes(() =>
      createdColumns(
        "CREATE TABLE w (a VARCHAR(20), b CHARACTER VARYING(5), c CHAR(3), d CHAR, e CHARACTER, " +
          "f VARCHAR, g TEXT)",
      ),
    );
    expect(Object.values(columns).map((column) => column.declaredLength)).toEqual([
      { length: 20, fixed: false },
      { length: 5, fixed: false },
      { length: 3, fixed: true },
      { length: 1, fixed: true },
      { length: 1, fixed: true },
      undefined,
      undefined,
    ]);
    const plain = createdColumns("CREATE TABLE w (a VARCHAR(20), c CHAR(3))");
    expect(plain.a).toEqual({ name: "a", type: "string", nullable: true });
    compileWithDeclaredTypes(() => {
      expect(castTarget("SELECT CAST(a AS VARCHAR(4)) FROM t")).toEqual({
        kind: "literal",
        value: "string",
        declaredLength: { length: 4, fixed: false },
      });
    });
    expect(castTarget("SELECT CAST(a AS VARCHAR(4)) FROM t")).toEqual({
      kind: "literal",
      value: "string",
    });
  });

  it("records it on ADD COLUMN and JSON_TABLE columns", () => {
    const statement = compileWithDeclaredTypes(() =>
      compileStatement("ALTER TABLE t ADD COLUMN n INT2"),
    );
    if (statement.kind !== "add-column") throw new Error("Expected ADD COLUMN");
    expect(statement.column).toEqual({
      name: "n",
      type: "number",
      integer: true,
      declaredTypeName: "smallint",
      nullable: true,
    });
    const jsonTable = compileWithDeclaredTypes(() =>
      compileQuery(
        "SELECT x FROM JSON_TABLE('[{\"x\": 4}]', '$[*]' COLUMNS (x INT8 PATH '$.x')) AS j",
      ),
    );
    expect(jsonTable).toBeDefined();
  });

  it("records it on CAST and :: targets, an ANY placeholder's array cast, and rule text", () => {
    compileWithDeclaredTypes(() => {
      expect(castTarget("SELECT CAST(a AS SMALLINT) FROM t")).toEqual({
        kind: "literal",
        value: "number-integer",
        declaredTypeName: "smallint",
      });
      expect(castTarget("SELECT a::int8 FROM t")).toEqual({
        kind: "literal",
        value: "number-integer",
        declaredTypeName: "bigint",
      });
      expect(castTarget("SELECT a::real FROM t").declaredTypeName).toBe("real");
      expect(castTarget("SELECT CAST(a AS DOUBLE PRECISION) FROM t").declaredTypeName).toBe(
        "double precision",
      );
      expect(castTarget("SELECT CAST(a AS FLOAT(10)) FROM t").declaredTypeName).toBe("real");
      expect(castTarget("SELECT CAST(a AS TEXT) FROM t")).toEqual({
        kind: "literal",
        value: "string",
      });
      expect(castTarget("SELECT CAST(a AS NUMERIC(5, 1)) FROM t")).toEqual({
        kind: "literal",
        value: "numeric:5:1",
      });
      const any = compileQuery("SELECT a FROM t WHERE a = ANY($1::int2[])");
      expect(JSON.stringify(any)).toContain('"declaredTypeName":"smallint"');
      expect(JSON.stringify(compileCheckExpression("a::int2 > 0", "c"))).toContain(
        '"declaredTypeName":"smallint"',
      );
      expect(JSON.stringify(compileDefaultExpression("CAST(1 AS BIGINT)"))).toContain(
        '"declaredTypeName":"bigint"',
      );
    });
  });

  it("keeps widths apart in query identity, as PostgreSQL keeps them apart", () => {
    const identity = (sql: string) =>
      encodeQueryIdentity(compileWithDeclaredTypes(() => compileQuery(sql)).select[0]?.expression);
    expect(identity("SELECT CAST(a AS SMALLINT) FROM t")).not.toBe(
      identity("SELECT CAST(a AS BIGINT) FROM t"),
    );
    expect(identity("SELECT CAST(a AS INT) FROM t")).toBe(
      identity("SELECT CAST(a AS INTEGER) FROM t"),
    );
  });

  it("restores the default after the scope, also when compilation throws", () => {
    expect(() => compileWithDeclaredTypes(() => compileStatement("CREATE TABLE ("))).toThrow();
    expect(createdColumns("CREATE TABLE t (a INT2)").a).not.toHaveProperty("declaredTypeName");
    const nested = compileWithDeclaredTypes(() => {
      compileWithDeclaredTypes(() => compileStatement("SELECT 1"));
      return createdColumns("CREATE TABLE t (a INT2)");
    });
    expect(nested.a?.declaredTypeName).toBe("smallint");
  });
});

describe("the current engine compiles exactly what it did", () => {
  it("adds no field outside the scope", () => {
    const texts = [
      WIDTHS_TABLE,
      "ALTER TABLE t ADD COLUMN n INT2",
      "SELECT CAST(a AS SMALLINT), a::int8, a::real, CAST(a AS FLOAT(3)) FROM t",
      "SELECT a FROM t WHERE a = ANY($1::int2[])",
      "SELECT x FROM JSON_TABLE('[{\"x\": 4}]', '$[*]' COLUMNS (x INT8 PATH '$.x')) AS j",
      "INSERT INTO t (a) VALUES (CAST(1 AS INT2))",
      "UPDATE t SET a = b::int4 WHERE c::float8 > 0",
    ];
    for (const text of texts)
      expect(JSON.stringify(compileStatement(text)), text).not.toContain("declaredTypeName");
    expect(JSON.stringify(compileQuery(texts[2] ?? ""))).not.toContain("declaredTypeName");
    expect(JSON.stringify(compileCheckExpression("a::int2 > 0", "c"))).not.toContain(
      "declaredTypeName",
    );
  });

  async function seeded(): Promise<{ database: MinnowDatabase; store: MemoryBlockStore }> {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.execute(
      "CREATE TABLE t (id SERIAL PRIMARY KEY, a SMALLINT, b BIGINT, c REAL, d DOUBLE PRECISION, e FLOAT(3))",
    );
    await database.execute("ALTER TABLE t ADD COLUMN f INT2");
    await database.execute(
      "INSERT INTO t (a, b, c, d, e, f) VALUES (1, 9007199254740991, 1.5, 2.25, 0.5, 7), (1, 2, 2.5, 3.5, 1.5, 8), (3, 4, 0.1, 0.2, 0.3, NULL)",
    );
    return { database, store };
  }

  it("stores no declared name in the table record", async () => {
    const { store } = await seeded();
    const table = await store.getTableByName("t");
    if (table === undefined) throw new Error("Expected table t");
    for (const column of table.columns)
      expect(Object.keys(column)).not.toContain("declaredTypeName");
  });

  it("keeps results, including grouping across differently written widths", async () => {
    const { database } = await seeded();
    await expect(
      database.query(
        "SELECT CAST(a AS SMALLINT) AS k, COUNT(*) AS n FROM t GROUP BY CAST(a AS BIGINT) ORDER BY 1",
      ),
    ).resolves.toMatchObject({
      rows: [
        { k: 1, n: 2 },
        { k: 3, n: 1 },
      ],
    });
    await expect(
      database.query(
        "SELECT CAST(c AS REAL) AS k FROM t GROUP BY CAST(c AS DOUBLE PRECISION) ORDER BY 1",
      ),
    ).resolves.toMatchObject({ rows: [{ k: 0.1 }, { k: 1.5 }, { k: 2.5 }] });
    await expect(
      database.query(
        "SELECT b::int2 AS narrow, b::int8 AS wide, c::float4 AS r, e::float(30) AS f FROM t ORDER BY id",
      ),
    ).resolves.toMatchObject({
      rows: [
        { narrow: 9007199254740991, wide: 9007199254740991, r: 1.5, f: 0.5 },
        { narrow: 2, wide: 2, r: 2.5, f: 1.5 },
        { narrow: 4, wide: 4, r: 0.1, f: 0.3 },
      ],
    });
    await expect(
      database.query("SELECT id FROM t WHERE f = ANY($1::int2[]) ORDER BY id", {
        params: [[7, 8]],
      }),
    ).resolves.toMatchObject({ rows: [{ id: 1 }, { id: 2 }] });
    await expect(
      database.query(
        "SELECT x FROM JSON_TABLE('[{\"x\": 4}]', '$[*]' COLUMNS (x INT2 PATH '$.x')) AS j",
      ),
    ).resolves.toMatchObject({ rows: [{ x: 4 }] });
  });
});
