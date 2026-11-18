import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { encodeArrayValue } from "../../../core/dist/next/vector/array-value.js";
import { decodeResult } from "../../../core/dist/next/owner/result.js";
import type { Param, CursorOptions } from "../../../core/dist/next/owner/api.js";
import { expect, it, vi } from "vitest";
import { MinnowDatabase } from "../../../core/dist/next/owner/database.js";
import { MemoryBackend } from "../../../core/dist/next/storage/memory-store.js";
import { SimLocks } from "../../../core/dist/next/storage/sim-disk.js";
import { seedsFor } from "../../../core/dist/testing/seeds.js";
import { createReach, NATIVE_EXPORT_REACH } from "../../../core/dist/testing/reach.js";
import { streamCsv, streamNdjson, type QueryCursorSource } from "./index.js";
async function text(stream: ReadableStream<Uint8Array>): Promise<string> {
  return new Response(stream).text();
}
// Independent record parser: checks escaped fields without building the expected escaped spelling.
function csv(source: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [],
    field = "",
    quoted = false;
  for (let i = 0; i < source.length; i++) {
    const c = source[i] ?? "";
    if (c === '"') {
      if (quoted && source[i + 1] === '"') {
        field += '"';
        i++;
      } else quoted = !quoted;
    } else if (!quoted && c === ",") {
      row.push(field);
      field = "";
    } else if (!quoted && c === "\r" && source[i + 1] === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      i++;
    } else field += c;
  }
  expect(quoted).toBe(false);
  expect(row).toEqual([]);
  expect(field).toBe("");
  return rows;
}
for (const seed of seedsFor("next-native-exports", [46958, 46959]))
  it(`exports exact canonical frames and drains the cursor (${String(seed)})`, async () => {
    const reach = createReach("native exports", NATIVE_EXPORT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.bigInt({ min: -(1n << 62n), max: 1n << 62n }),
        fc.string({ maxLength: 32 }).map((x) => x.replaceAll("\0", "�")),
        fc.integer({ min: 2, max: 5 }),
        async (n, v, count) => {
          reach.beginHistory();
          const native = await MinnowDatabase.open({
            backend: new MemoryBackend({ locks: new SimLocks() }),
            memoryLimitBytes: 64 << 20,
          });
          try {
            const sql = `SELECT i,$1::bigint n,$2::text v,12.3400::numeric d,TIMESTAMP '2000-02-29 12:34:56.123456' t FROM generate_series(1,${String(count)}) g(i)`;
            const options = { params: [n, v], frameRows: 1 };
            const output = await text(streamNdjson(native, sql, options));
            expect(output).toBe(
              Array.from(
                { length: count },
                (_, i) =>
                  `{"i":${String(i + 1)},"n":${n.toString()},"v":${JSON.stringify(v)},"d":"12.3400","t":"2000-02-29T12:34:56.123456"}`,
              ).join("\n") + "\n",
            );
            reach.reach("BIGINT decimal tokens");
            reach.reach("NUMERIC and temporal text");
            reach.reach("NULL and arrays");
            // SQL BYTEA/array construction is not enabled on this base. These use
            // the real canonical decoder and C9 codec, separately from native SQL.
            const binary = new Uint8Array([0, 255, Number(BigInt.asUintN(8, n))]);
            const expectedHex =
              "\\x" + Array.from(binary, (byte) => byte.toString(16).padStart(2, "0")).join("");
            const array = encodeArrayValue(
              { kind: "int64" },
              {
                dimensions: [2, 2],
                lowerBounds: [-2, 1],
                elements: [1n, null, 9007199254740993n, n],
              },
            );
            const binarySource = (duplicate: boolean): QueryCursorSource => ({
              cursor<TRow>() {
                const result = decodeResult<TRow>({
                  columns: [
                    {
                      name: duplicate ? "x" : "b",
                      type: { kind: "bytes" },
                      sqlType: "bytea",
                      nullable: false,
                      render: null,
                    },
                    {
                      name: "x",
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
                    rowCount: null,
                    counts: null,
                    lsn: null,
                    replayed: false,
                    closedEarly: false,
                    transaction: "idle",
                    catalogVersion: 1,
                  },
                });
                let sent = false;
                return {
                  columns: Promise.resolve(result.columns),
                  async next() {
                    if (sent) return { done: true as const, value: undefined };
                    sent = true;
                    return { done: false as const, value: result };
                  },
                  async close() {
                    sent = true;
                  },
                  [Symbol.asyncIterator]() {
                    return this;
                  },
                };
              },
            });
            const arrayText = `[[1,null],[9007199254740993,${n.toString()}]]`;
            expect(await text(streamNdjson(binarySource(true), "frame"))).toBe(
              `{"x":${arrayText}}\n`,
            );
            expect(csv(await text(streamCsv(binarySource(true), "frame")))).toEqual([
              ["x", "x"],
              [expectedHex, arrayText],
            ]);
            expect(await text(streamNdjson(binarySource(false), "frame"))).toBe(
              `{"b":${JSON.stringify(expectedHex)},"x":${arrayText}}\n`,
            );
            reach.reach("bytea hex");
            reach.reach("multiple frames");
            const records = csv(await text(streamCsv(native, sql, options)));
            expect(records[0]).toEqual(["i", "n", "v", "d", "t"]);
            expect(records.slice(1)).toEqual(
              Array.from({ length: count }, (_, i) => [
                String(i + 1),
                n.toString(),
                v,
                "12.3400",
                "2000-02-29T12:34:56.123456",
              ]),
            );
            reach.reach("escaped CSV");
            expect(await text(streamCsv(native, "SELECT 1 x WHERE false"))).toBe("x\r\n");
            reach.reach("empty header");
            reach.reach("duplicate columns");
            let closed = 0;
            const source: QueryCursorSource = {
              cursor<TRow>(sql: string, params?: readonly Param[], options?: CursorOptions) {
                const cursor = native.cursor<TRow>(sql, params, options);
                return {
                  ...cursor,
                  close: async () => {
                    closed++;
                    await cursor.close();
                  },
                };
              },
            };
            const reader = streamNdjson(source, sql, options).getReader();
            expect((await reader.read()).done).toBe(false);
            await reader.cancel();
            expect(closed).toBe(1);
            expect((await native.query("SELECT 1 ok")).tuples()).toEqual([[1]]);
            reach.reach("cancel closes cursor");
          } finally {
            await native.close();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [9007199254740993n, 'comma, quote "\nline', 2],
          [-9007199254740993n, "水🙂", 3],
          [0n, "", 5],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
it("rejects invalid options before opening a query", () => {
  const source: QueryCursorSource = {
    cursor: vi.fn(() => {
      throw new Error("must not start");
    }),
  };
  for (const delimiter of ["", '"', "\n", "ab"])
    expect(() => streamCsv(source, "SELECT", { delimiter })).toThrow(RangeError);
  expect(source.cursor).not.toHaveBeenCalled();
});

it("matches PG duplicate SQL labels, positional CSV and last-wins NDJSON", async () => {
  const pg = new PGlite();
  const native = await MinnowDatabase.open({
    backend: new MemoryBackend({ locks: new SimLocks() }),
    memoryLimitBytes: 64 << 20,
  });
  try {
    const expected = await pg.query<number[]>("SELECT 1 x,2 x", [], { rowMode: "array" });
    const result = await native.query("SELECT 1 x,2 x");
    expect(result.columns.map((column) => column.name)).toEqual(
      expected.fields.map((field) => field.name),
    );
    expect(result.tuples()).toEqual(expected.rows);
    expect(result.rows()).toEqual([{ x: 2 }]);
    expect(await text(streamCsv(native, "SELECT 1 x,2 x"))).toBe("x,x\r\n1,2\r\n");
    expect(await text(streamNdjson(native, "SELECT 1 x,2 x"))).toBe('{"x":2}\n');
  } finally {
    await native.close();
    await pg.close();
  }
});

it("closes canonical streams on non-finite JSON encoding errors", async () => {
  for (const value of [NaN, Infinity, -Infinity]) {
    let closed = 0;
    const source: QueryCursorSource = {
      cursor<TRow>() {
        const result = decodeResult<TRow>({
          columns: [
            {
              name: "n",
              type: { kind: "float64" },
              sqlType: "double precision",
              nullable: false,
              render: null,
            },
          ],
          frames: [
            {
              sequence: 0,
              rowCount: 1,
              dictionaries: [],
              columns: [
                {
                  form: "flat",
                  length: 1,
                  validity: null,
                  data: { layout: "f64", values: Float64Array.of(value) },
                },
              ],
            },
          ],
          summary: {
            command: "SELECT",
            rowCount: null,
            counts: null,
            lsn: null,
            replayed: false,
            closedEarly: false,
            transaction: "idle",
            catalogVersion: 1,
          },
        });
        let sent = false;
        return {
          columns: Promise.resolve(result.columns),
          async next() {
            if (sent) return { done: true as const, value: undefined };
            sent = true;
            return { done: false as const, value: result };
          },
          async close() {
            closed++;
            sent = true;
          },
          [Symbol.asyncIterator]() {
            return this;
          },
        };
      },
    };
    await expect(text(streamNdjson(source, "frame"))).rejects.toThrow(RangeError);
    expect(closed).toBe(1);
  }
});
