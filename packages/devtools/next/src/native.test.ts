import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { MinnowDatabase } from "../../../core/dist/next/owner/database.js";
import { MemoryBackend } from "../../../core/dist/next/storage/memory-store.js";
import { SimLocks } from "../../../core/dist/next/storage/sim-disk.js";
import { seedsFor } from "../../../core/dist/testing/seeds.js";
import { createReach, NATIVE_DEVTOOLS_REACH } from "../../../core/dist/testing/reach.js";
import { nativeTarget, NativeDevtoolsNotBoundError } from "./target.js";
import { resultPresentation, selectionText, valueText } from "./result.js";
import { editorSchema, catalogRelations } from "./catalog.js";
import { decodeResult } from "../../../core/dist/next/owner/result.js";
import { encodeArrayValue } from "../../../core/dist/next/vector/array-value.js";
function binaryResult(integer: bigint, binary: Uint8Array, nested: boolean) {
  const array = encodeArrayValue(
    { kind: "int64" },
    {
      dimensions: nested ? [2, 2] : [2],
      lowerBounds: nested ? [-2, 3] : [1],
      elements: nested ? [integer, null, integer, 0n] : [integer, null],
    },
  );
  return decodeResult({
    columns: [
      { name: "raw", type: { kind: "bytes" }, sqlType: "bytea", nullable: false, render: null },
      {
        name: "arr",
        type: { kind: "array", element: { kind: "int64" } },
        sqlType: "bigint[]",
        nullable: false,
        render: null,
      },
    ],
    frames: [
      {
        sequence: 0,
        rowCount: 1,
        dictionaries: [],
        columns: [binary, array].map((bytes) => ({
          form: "flat",
          length: 1,
          validity: null,
          data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
        })),
      },
    ],
    summary: {
      command: "SELECT",
      rowCount: 1,
      counts: null,
      lsn: null,
      replayed: false,
      closedEarly: false,
      transaction: "idle",
      catalogVersion: 1,
    },
  });
}
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
for (const seed of seedsFor("next-native-devtools", [46976, 46977]))
  it(`presents canonical data and catalog through the real native facade (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("native devtools", NATIVE_DEVTOOLS_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.bigInt({ min: -(1n << 60n), max: 1n << 60n }),
          fc.integer({ min: -99999999, max: 99999999 }),
          fc.constantFrom("x", "a.b", "Case", "__proto__", "<img src=x>"),
          fc.integer({ min: 0, max: 4 }),
          fc.boolean(),
          async (integer, coefficient, label, count, nested) => {
            reach.beginHistory();
            const locks = new SimLocks();
            const db = await MinnowDatabase.open({
              backend: new MemoryBackend({ locks }),
              memoryLimitBytes: 64 << 20,
            });
            try {
              const target = nativeTarget(db),
                decimal = `${String(coefficient)}.123400`,
                raw = Uint8Array.of(0, 255, 34);
              const sql = `SELECT $1::bigint AS ${quote(label)},$2::numeric(20,6) AS ${quote(label)},TIMESTAMP '2001-02-03 04:05:06.123456' stamp,NULL::text nil,'NULL'::text str FROM generate_series(1,${String(count)})g(i)`;
              const result = await target.query(sql, [integer, decimal]);
              const rowSpy = vi.spyOn(result, "rows"),
                tupleSpy = vi.spyOn(result, "tuples");
              const model = resultPresentation(result),
                expected = (
                  await pg.query<{ j: string[] }>(
                    `SELECT json_build_array(($1::bigint)::text,($2::numeric(20,6))::text,to_char(TIMESTAMP '2001-02-03 04:05:06.123456','YYYY-MM-DD"T"HH24:MI:SS.US')) j`,
                    [integer.toString(), decimal],
                  )
                ).rows[0]?.j;
              expect(model.columns.slice(0, 2).map((column) => column.displayName)).toEqual([
                label,
                label,
              ]);
              expect(new Set(model.columns.map((column) => column.name)).size).toBe(5);
              reach.reach("duplicate positional labels");
              expect(rowSpy).not.toHaveBeenCalled();
              expect(tupleSpy).not.toHaveBeenCalled();
              reach.reach("lazy display");
              expect(model.visibleRows).toBe(count);
              if (count) {
                expect(model.value(0, 0)).toBe(BigInt(expected?.[0] ?? "0"));
                expect(model.value(0, 1)).toBe(expected?.[1]);
                expect(model.value(0, 2)).toBe(expected?.[2]);

                const row = model.rows[0];
                expect(row).toBeDefined();
                expect(row?.["@0"]).toBe(integer.toString());
                expect(row?.["@1"]).toBe(expected?.[1]);
                expect(row?.["@2"]).toBe("2001-02-03T04:05:06.123456");
                expect(row?.["@3"]).toBeNull();
                expect(row?.["@4"]).toBe("NULL");
                expect(
                  selectionText(model.columns, model.rows).split("\n")[0]?.split("\t").slice(0, 2),
                ).toEqual([label, label]);
                expect(result.row(0)[label]).toBe(expected?.[1]);
                reach.reach("object last wins");
                expect(() => result.values(label)).toThrow("ordinal");
              } else reach.reach("empty result");
              // SQL ARRAY construction is still a typed refusal on this base. The
              // array/BYTEA presentation law uses real decoded C9 frames independently.
              await expect(target.query("SELECT ARRAY[1]")).rejects.toMatchObject({
                sqlState: "0A000",
              });
              const binary = resultPresentation(binaryResult(integer, raw, nested));
              expect(binary.value(0, 0)).toEqual(raw);
              expect(binary.value(0, 1)).toEqual(
                nested
                  ? [
                      [integer, null],
                      [integer, 0n],
                    ]
                  : [integer, null],
              );
              if (nested) reach.reach("nested arrays");
              expect(binary.rows[0]?.["@0"]).toBe("\\x00ff22");
              const arrayText = (
                await pg.query<{ j: string }>(
                  nested
                    ? "SELECT to_json(ARRAY[[$1::bigint,NULL],[$1::bigint,0]])::text j"
                    : "SELECT to_json(ARRAY[$1::bigint,NULL])::text j",
                  [integer.toString()],
                )
              ).rows[0]?.j;
              expect(binary.rows[0]?.["@1"]).toBe(arrayText);
              reach.reach("exact bigint");
              reach.reach("numeric scale");
              reach.reach("microsecond timestamp");
              reach.reach("binary and arrays");
              reach.reach("NULL marker");
              rowSpy.mockRestore();
              tupleSpy.mockRestore();
              await target.execute(
                'CREATE TABLE "a.b"(id INT PRIMARY KEY,v INT GENERATED ALWAYS AS(id+1) STORED)',
              );
              await target.execute('CREATE INDEX descending_idx ON "a.b"(v DESC)');
              await target.execute('CREATE VIEW "Case" AS SELECT id FROM "a.b"');
              const catalog = await target.describeCatalog(),
                relations = catalogRelations(catalog),
                schema = editorSchema(catalog);
              expect(relations.map((relation) => [relation.name, relation.kind])).toEqual([
                ["a.b", "table"],
                ["Case", "view"],
              ]);
              expect(schema["a.b"]).toEqual(["id", "v"]);
              expect(schema.Case).toEqual(["id"]);
              expect(
                catalog.tables[0]?.indexes.find((index) => index.name === "descending_idx")
                  ?.directions,
              ).toEqual(["desc"]);
              expect(catalog.tables[0]?.columns[1]?.generatedSql).toContain("id");
              reach.reach("catalog metadata");
              reach.reach("quoted completion");
              const explain = await target.explain('INSERT INTO "a.b"(id) VALUES(42)');
              expect(explain.text).toContain("not executed");
              expect((await target.query('SELECT count(*)::int n FROM "a.b"')).tuples()).toEqual([
                [0],
              ]);
              reach.reach("logical explain");
              const cursor = target.cursor("SELECT i FROM generate_series(1,5)g(i)", [], {
                frameRows: 1,
              });
              expect((await cursor.next()).done).toBe(false);
              await cursor.close();
              expect((await target.query("SELECT 1")).rowCount).toBe(1);
              reach.reach("cursor close");
              expect(() => target.follow()).toThrow(NativeDevtoolsNotBoundError);
              try {
                target.follow();
              } catch (error) {
                expect(error).toMatchObject({
                  code: "MINNOW_DEVTOOLS_NOT_BOUND",
                  sqlState: "0A000",
                });
              }
              reach.reach("live not bound");
              const bounded = resultPresentation(
                await target.query("SELECT i FROM generate_series(1,1002)g(i)"),
              );
              expect(bounded.visibleRows).toBe(1000);
              expect(bounded.truncated).toBe(true);
              expect(() => bounded.value(1000, 0)).toThrow(RangeError);
              reach.reach("preview bound");
              expect(valueText([integer, raw, "NULL", null])).toBe(
                `[${String(integer)},"\\\\x00ff22","NULL",null]`,
              );
            } finally {
              await db.close();
              expect(await locks.query()).toEqual([]);
            }
            reach.reach("native owner retired");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [9007199254740993n, -3, "x", 2, false],
            [-9007199254740993n, 2, "__proto__", 0, false],
            [9007199254740993n, -3, "x", 2, true],
            [-9007199254740993n, 2, "__proto__", 0, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
