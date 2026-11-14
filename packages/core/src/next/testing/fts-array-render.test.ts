/** PG array_out/::text is renderer2 authority; storage bytes do not set the FTS text limit. */
import { expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { createReach, FTS_ARRAY_RENDER_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ftsValue } from "../exec/fts-value.js";
import { encodeArrayValue, type ArrayElement } from "../vector/array-value.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const encoder = new TextEncoder();
function vector(element: PhysicalType, values: ReadonlyArray<ArrayElement | null>): FlatVector {
  const bytes = encodeArrayValue(element, {
    dimensions: values.length ? [values.length] : [],
    lowerBounds: values.length ? [1] : [],
    elements: values,
  });
  return {
    form: "flat",
    type: { kind: "array", element },
    length: 1,
    validity: null,
    data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
  };
}
const whitespace = [
  " ",
  "\t",
  "\n",
  "\r",
  "\v",
  "\f",
  "\u0085",
  "\u00a0",
  "\u1680",
  "\u180e",
  ...Array.from({ length: 11 }, (_, i) => String.fromCharCode(0x2000 + i)),
  "\u2028",
  "\u2029",
  "\u202f",
  "\u205f",
  "\u3000",
  "\ufeff",
];
for (const seed of seedsFor("next-fts-array-render", [0xb805, 0xb806]))
  it(`uses exact PG array text at the character boundary (${String(seed)})`, async () => {
    const pg = new PGlite(),
      core = await new StorageCore().open(),
      db = new MinnowDatabase(new MemoryBlockStore(), { ftsAutoIndexRows: 1_000_000 });
    const random = mulberry32(seed),
      reach = createReach(`FTS array ${String(seed)}`, FTS_ARRAY_RENDER_REACH);
    try {
      await pg.waitReady;
      const numericCases = async () => {
        reach.beginHistory();
        await core.execute("CREATE TABLE docs(a BIGINT[])");
        await db.execute("CREATE TABLE docs(a TEXT)");
        const table = core.harness.path.catalog.tableByName("docs");
        if (!table) throw Error("table");
        for (const [count, first, length] of [
          [8190, 0n, 16381],
          [8191, 0n, 16383],
          [8191, -1n, 16384],
        ] as const) {
          const values = Array<bigint>(count).fill(0n);
          values[0] = first;
          const canonical = (
            await pg.query<{ t: string }>("SELECT $1::bigint[]::text t", [values.map(String)])
          ).rows[0]?.t;
          if (canonical === undefined) throw Error("PG bigint array");
          expect(canonical.length).toBe(length);
          const input = vector({ kind: "int64" }, values);
          if (input.data.layout !== "varlen") throw Error("array bytes");
          if (count === 8191) {
            expect(input.data.bytes.length).toBe(65540);
            reach.reach("BIGINT encoded over64KiB");
          }
          expect(ftsValue(input, 0)).toBe(canonical);
          await core.insertVectors(table, { length: 1, selection: null, columns: [input] });
          await db.insertBatch("docs", [{ a: canonical }]);
        }
        const nullable: Array<bigint | null> = Array<bigint>(8190).fill(0n);
        nullable[0] = null;
        const nullText = (
          await pg.query<{ t: string }>("SELECT $1::bigint[]::text t", [
            nullable.map((value) => (value === null ? null : String(value))),
          ])
        ).rows[0]?.t;
        if (nullText === undefined) throw Error("PG NULL array");
        expect(nullText.length).toBe(16384);
        const nullableVector = vector({ kind: "int64" }, nullable);
        if (nullableVector.data.layout !== "varlen") throw Error("NULL array bytes");
        expect(nullableVector.data.bytes.length).toBeGreaterThan(65536);
        expect(ftsValue(nullableVector, 0)).toBe(nullText);
        await core.insertVectors(table, { length: 1, selection: null, columns: [nullableVector] });
        await db.insertBatch("docs", [{ a: nullText }]);
        const sql = "SELECT MATCH(a) AGAINST '0' m,BM25(a) AGAINST '0' s FROM docs";
        const actual = await core.execute(sql);
        if (!("batches" in actual)) throw Error("query");
        expect(oracleRows(actual)).toEqual((await db.query(sql, { memoize: false })).rows);
        reach.reach("MATCH BM25 SQL");
        const tooLong = Array<bigint>(8191).fill(0n);
        tooLong[0] = -10n;
        expect(
          (await pg.query<{ t: string }>("SELECT $1::bigint[]::text t", [tooLong.map(String)]))
            .rows[0]?.t.length,
        ).toBe(16385);
        expect(() => ftsValue(vector({ kind: "int64" }, tooLong), 0)).toThrow("16384");
        const source = "a".repeat(16379) + "\u00a0b";
        const expected = (await pg.query<{ t: string }>("SELECT $1::text[]::text t", [[source]]))
          .rows[0]?.t;
        if (expected === undefined) throw Error("PG text array");
        expect(expected.length).toBe(16383);
        await core.execute("CREATE TABLE unicode_docs(a TEXT[])");
        await db.execute("CREATE TABLE unicode_docs(a TEXT)");
        const unicode = core.harness.path.catalog.tableByName("unicode_docs");
        if (!unicode) throw Error("unicode table");
        await core.insertVectors(unicode, {
          length: 1,
          selection: null,
          columns: [vector({ kind: "text" }, [encoder.encode(source)])],
        });
        await db.insertBatch("unicode_docs", [{ a: expected }]);
        const query = "SELECT MATCH(a) AGAINST 'b' m,BM25(a) AGAINST 'b' s FROM unicode_docs";
        const result = await core.execute(query);
        if (!("batches" in result)) throw Error("query");
        expect(oracleRows(result)).toEqual((await db.query(query, { memoize: false })).rows);
      };
      if (seed & 1) await numericCases();
      for (const separator of whitespace) {
        reach.beginHistory();
        const position = separator === "\ufeff" ? 0 : Math.floor(random() * 3);
        const word =
          position === 0 ? `${separator}ab` : position === 1 ? `ab${separator}` : `a${separator}b`;
        const base = (await pg.query<{ t: string }>("SELECT $1::text[]::text t", [[word]])).rows[0]
          ?.t;
        if (base === undefined) throw Error("PG array");
        for (const length of [16383, 16384, 16385]) {
          const padding = "a".repeat(length - base.length);
          const value = position === 0 ? word + padding : padding + word;
          const expected = (await pg.query<{ t: string }>("SELECT $1::text[]::text t", [[value]]))
            .rows[0]?.t;
          expect(expected?.length).toBe(length);
          const input = vector({ kind: "text" }, [encoder.encode(value)]);
          if (length > 16384) {
            expect(() => ftsValue(input, 0)).toThrow("16384");
            reach.reach("above limit refused");
          } else {
            expect(ftsValue(input, 0)).toBe(expected);
            reach.reach(length === 16384 ? "at character limit" : "below character limit");
          }
        }
        // Also vary neighboring ordinary elements/NULLs, retaining all whitespace examples.
        const words = [
          word,
          `${separator}ab`,
          `ab${separator}`,
          ...Array.from({ length: 1 + Math.floor(random() * 4) }, () =>
            random() < 0.25 ? null : `v${String(Math.floor(random() * 10))}`,
          ),
        ];
        const expected = (await pg.query<{ t: string }>("SELECT $1::text[]::text t", [words]))
          .rows[0]?.t;
        expect(
          ftsValue(
            vector(
              { kind: "text" },
              words.map((value) => (value === null ? null : encoder.encode(value))),
            ),
            0,
          ),
        ).toBe(expected);
        reach.reach("leading whitespace");
        reach.reach("trailing whitespace");
        reach.reach("exact PostgreSQL text");
        reach.reach(separator.charCodeAt(0) < 0x80 ? "ASCII whitespace" : "Unicode whitespace");
      }
      if (!(seed & 1)) await numericCases();
      expect(core.spill.bytesInUse).toBe(0);
      reach.reach("drained");
      reach.assertReached();
    } finally {
      await core.close();
      await db.close();
      await pg.close();
    }
  }, 60000);
