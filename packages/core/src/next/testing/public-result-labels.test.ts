/** PG result labels are separate from unique relational binding identities. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { compileQuery } from "../../engine/query.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_RESULT_LABEL_REACH } from "../../testing/reach.js";
import { MinnowDatabase } from "../owner/database.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { SimLocks } from "../storage/sim-disk.js";
const q = (name: string) => `"${name.replaceAll('"', '""')}"`;
const shapes = [
  "direct",
  "derived",
  "cte",
  "union",
  "except",
  "distinct",
  "group",
  "window",
  "join",
  "view",
  "empty",
  "unquoted",
  "aliasList",
] as const;
function sqlFor(shape: (typeof shapes)[number], label: string, count: number): string {
  const name = q(label),
    from = `FROM generate_series(1,${String(count)})g(i)`,
    projection = `SELECT i ${name},i+1 ${name}`;
  switch (shape) {
    case "aliasList":
      return `SELECT * FROM (${projection} ${from})q(first,second) ORDER BY 1`;
    case "direct":
      return `${projection},i+2 ${q(label + "_2")} ${from} ORDER BY 1`;
    case "derived":
      return `SELECT * FROM (${projection} ${from})q ORDER BY 1`;
    case "cte":
      return `WITH q AS (${projection} ${from})SELECT * FROM q ORDER BY 1`;
    case "union":
      return `${projection} ${from} UNION ALL SELECT i+10 ${name},i+11 ${name} ${from} ORDER BY 1`;
    case "except":
      return `${projection} ${from} EXCEPT SELECT -1 ${name},-2 ${name} ORDER BY 1`;
    case "distinct":
      return `SELECT DISTINCT i ${name},i+1 ${name} ${from} ORDER BY 1`;
    case "group":
      return `SELECT i%2 ${name},SUM(i)::int ${name} ${from} GROUP BY 1 ORDER BY 1`;
    case "window":
      return `SELECT i ${name},(row_number() OVER(ORDER BY i)+10)::int ${name} ${from} ORDER BY 1`;
    case "join":
      return `SELECT l.*,r.* FROM t l JOIN u r ON l.id=r.id WHERE l.id<=${String(count)} ORDER BY 1`;
    case "view":
      return `SELECT v.*,v.a FROM v WHERE id<=${String(count)} ORDER BY 1`;
    case "empty":
      return `SELECT 1 ${name},2 ${name} WHERE false`;
    case "unquoted":
      return "SELECT 1 AS MiXeD,2 AS MiXeD";
  }
}
for (const seed of seedsFor("next-public-result-labels", [46962, 46963]))
  it(`matches PG labels through preparation and frames (${String(seed)})`, async () => {
    const pg = new PGlite(),
      native = await MinnowDatabase.open({
        backend: new MemoryBackend({ locks: new SimLocks() }),
        memoryLimitBytes: 64 << 20,
      });
    const reach = createReach("public result labels", PUBLIC_RESULT_LABEL_REACH);
    try {
      for (const ddl of [
        "CREATE TABLE t(id INT PRIMARY KEY,a INT)",
        "CREATE TABLE u(id INT PRIMARY KEY,a INT)",
        "INSERT INTO t VALUES(1,11),(2,22),(3,33),(4,44)",
        "INSERT INTO u VALUES(1,-11),(2,-22),(3,-33),(4,-44)",
        "CREATE VIEW v AS SELECT id,a FROM t",
      ]) {
        await pg.exec(ddl);
        await native.execute(ddl);
      }
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...shapes),
          fc.oneof(
            fc.constantFrom("x", "x_2", "Case", "a.b", "__proto__", "constructor", "1", 'z"q'),
            fc
              .array(fc.constantFrom("x", "Y", ".", '"', "🙂", "_", "0"), {
                minLength: 1,
                maxLength: 4,
              })
              .map((parts) => parts.join("")),
          ),
          fc.integer({ min: 1, max: 4 }),
          fc.integer({ min: 1, max: 3 }),
          async (shape, label, count, frameRows) => {
            reach.beginHistory();
            const sql = sqlFor(shape, label, count),
              expected = await pg.query<number[]>(sql, [], { rowMode: "array" }),
              names = expected.fields.map((field) => field.name);
            if (names.some((name) => name.includes("."))) reach.reach("dotted labels");
            if (names.some((name) => /[A-Z]/.test(name))) reach.reach("case-sensitive labels");
            if (names.some((name) => name === "__proto__" || name === "constructor"))
              reach.reach("object-property labels");
            if (names.some((name) => name.includes('"'))) reach.reach("escaped labels");
            const objects = expected.rows.map((tuple) => {
              const row: Record<string, unknown> = {};
              names.forEach((name, i) =>
                Object.defineProperty(row, name, {
                  value: tuple[i],
                  enumerable: true,
                  configurable: true,
                  writable: true,
                }),
              );
              return row;
            });
            if (shape === "direct") {
              expect(compileQuery(sql).select.map((item) => item.alias)).toEqual([
                label,
                `${label}_2`,
                `${label}_2_2`,
              ]);
              reach.reach("0.x binding identities unchanged");
            }
            const result = await native.query(sql);
            expect(result.columns.map((column) => column.name)).toEqual(names);
            expect(result.tuples()).toEqual(expected.rows);
            expect(result.rows()).toEqual(objects);
            reach.reach(shape);
            for (const row of result.rows())
              expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
            const occurrences = new Map<string, number>();
            for (const name of names) occurrences.set(name, (occurrences.get(name) ?? 0) + 1);
            for (const [i, name] of names.entries()) {
              expect(result.values(i)).toEqual(expected.rows.map((row) => row[i]));
              result.column(i);
              result.columnChunks(i);
              if ((occurrences.get(name) ?? 0) > 1) {
                for (const read of [
                  () => result.values(name),
                  () => result.column(name),
                  () => result.columnChunks(name),
                ]) {
                  expect(read).toThrow(TypeError);
                  try {
                    read();
                  } catch (error) {
                    expect(error).toMatchObject({
                      name: "AmbiguousResultColumnError",
                      code: "MINNOW_AMBIGUOUS_RESULT_COLUMN",
                      columnName: name,
                    });
                    expect(String(error)).toContain("ordinal");
                  }
                }
                reach.reach("ambiguous name rejects");
              } else expect(result.values(name)).toEqual(expected.rows.map((row) => row[i]));
            }
            const prepared = await native.prepare(sql);
            try {
              expect(prepared.columns.map((column) => column.name)).toEqual(names);
              expect((await prepared.query()).tuples()).toEqual(expected.rows);
            } finally {
              prepared.close();
            }
            reach.reach("prepared labels");
            const cursor = native.cursor(sql, [], { frameRows });
            try {
              expect((await cursor.columns).map((column) => column.name)).toEqual(names);
              const rows = [];
              for await (const batch of cursor) {
                expect(batch.columns.map((column) => column.name)).toEqual(names);
                for (const tuple of batch.tuples()) rows.push(tuple);
              }
              expect(rows).toEqual(expected.rows);
            } finally {
              await cursor.close();
            }
            reach.reach("cursor labels");
          },
        ),
        {
          seed,
          numRuns: 34,
          examples: [
            ...shapes.map(
              (shape) => [shape, "x", 3, 1] as [(typeof shapes)[number], string, number, number],
            ),
            ...["a.b", "Case", "__proto__", 'z"q'].map(
              (name) => ["direct", name, 2, 1] as [(typeof shapes)[number], string, number, number],
            ),
          ],
        },
      );
      reach.assertReached();
    } finally {
      await native.close();
      await pg.close();
    }
  }, 60000);
