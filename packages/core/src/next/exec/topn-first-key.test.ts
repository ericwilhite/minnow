/** First-key prefilters are conservative: a tie must reach PostgreSQL's full ORDER BY. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  ArraySource,
  CollectSink,
  INT,
  BIG,
  DBL,
  BOOL,
  batchesOf,
  vectorOf,
  type Row,
} from "./join-fixtures.js";
import type { Vector } from "../vector/vector.js";
import { drive } from "./pipeline.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
const reach = createReach("next-topn-first", [
  "float specials",
  "wide int64",
  "int32",
  "bool",
  "ascending",
  "descending",
  "NULLS FIRST",
  "NULLS LAST",
  "dictionary",
  "selected",
  "first-key ties",
  "compacted",
  "adjacent int64 words",
  "safe threshold wide input",
  "non-null float specials",
]);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-topn-first", [0x701, 0x325]))
  it(`first-key Top-N vs PGlite seed ${String(seed)}`, async () => {
    const random = mulberry32(seed);
    for (const [type, sqlType, family, pool] of [
      [DBL, "float8", "float specials", [null, NaN, Infinity, -Infinity, -0, 0, 1, 2]],
      [
        BIG,
        "bigint",
        "wide int64",
        [null, -(1n << 62n), -(1n << 53n) - 1n, -1n, 0n, (1n << 53n) + 1n, (1n << 62n) + 1n],
      ],
      [INT, "int", "int32", [null, -2147483648, 0, 1, 2147483647]],
      [BOOL, "bool", "bool", [null, false, true]],
    ] as const) {
      const data: Row[] = Array.from({ length: 16384 }, (_, id) => [
        id,
        family === "wide int64" && id < 8704
          ? 0n
          : (pool[Math.floor(random() * pool.length)] ?? null),
      ]);
      await pg.exec(
        `DROP TABLE IF EXISTS gate;CREATE TABLE gate(id int,x ${sqlType});INSERT INTO gate VALUES ${data.map(([id, x]) => `(${String(id)},${x === null ? "NULL" : `'${String(x)}'::${sqlType}`})`).join(",")}`,
      );
      for (const direction of ["asc", "desc"] as const)
        for (const nulls of ["first", "last"] as const) {
          const dictionary = random() < 0.5;
          const selected = direction === "desc";
          const input = batchesOf([INT, type], data, {
            batchRows: 256,
            dictionary,
            int64: "int64",
          });
          if (dictionary)
            input.forEach((batch, b) => {
              const original = batch.columns[1];
              if (original?.form !== "flat") throw new Error("Expected flat input");
              const entries = data
                .slice(b * 256, b * 256 + batch.length)
                .map(
                  (row) =>
                    row[1] ?? (type.kind === "bool" ? false : type.kind === "int64" ? 0n : 0),
                )
                .reverse();
              const dict = vectorOf(type, entries, { int64: "int64" });
              if (dict.form !== "flat") throw new Error("Expected flat dictionary");
              const vector: Vector = {
                form: "dictionary",
                type,
                length: batch.length,
                codes: Int32Array.from({ length: batch.length }, (_, i) => batch.length - 1 - i),
                dictionary: dict,
                dictionaryId: b + 1,
                validity: original.validity,
              };
              (batch as { columns: typeof batch.columns }).columns = [
                batch.columns[0] ?? vector,
                vector,
              ];
            });
          if (selected)
            for (const batch of input) {
              const ids = Uint16Array.from({ length: batch.length / 2 }, (_, i) => i * 2);
              (batch as { selection: typeof batch.selection }).selection = {
                rows: ids,
                count: ids.length,
              };
            }
          const withTies = random() < 0.5;
          const expected = (
            await pg.query<{ id: number }>(
              `SELECT id FROM gate ${selected ? "WHERE id%2=0" : ""} ORDER BY x ${direction} NULLS ${nulls},id DESC OFFSET 3 FETCH FIRST 7 ROWS ${withTies ? "WITH TIES" : "ONLY"}`,
            )
          ).rows.map((r) => r.id);
          const governor = new Governor(64 << 20);
          const grant = await governor.admit({
            label: "topn",
            minimumBytes: sortMinimumBytes([INT, type]),
            spillable: true,
          });
          const spill = new MemorySpillService();
          const sink = new CollectSink();
          const op = new SortOperator(
            {
              types: [INT, type],
              keys: [
                { column: 1, direction, nulls, collation: null },
                { column: 0, direction: "desc", nulls: "last", collation: null },
              ],
              limit: 7,
              offset: 3,
              withTies,
            },
            { grant, spill, queryId: 1 },
            sink,
          );
          await drive(new ArraySource(input), op);
          await op.closed;
          expect(
            sink.rows.map((row) => row[0]),
            `${family} ${direction} ${nulls}`,
          ).toEqual(expected);
          expect(op.profile.compactions).toBeGreaterThan(0);
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.beginHistory();
          reach.reach(family);
          reach.reach(direction === "asc" ? "ascending" : "descending");
          reach.reach(nulls === "first" ? "NULLS FIRST" : "NULLS LAST");
          reach.reach("first-key ties");
          reach.reach("compacted");
          if (dictionary) reach.reach("dictionary");
          if (selected) reach.reach("selected");
        }
    }
  }, 120000);

// Adjacent wide values must be distinguished by low words, including negative high words.
for (const seed of seedsFor("next-topn-first", [0x701, 0x325]))
  it(`raw int64 first-key words stay exact seed ${String(seed)}`, async () => {
    const { FirstKeyThreshold } = await import("./sort-keys.js");
    const values = [
      -(1n << 62n) - 1n,
      -(1n << 62n),
      -(1n << 53n) - 1n,
      -(1n << 53n),
      -1n,
      0n,
      1n,
      (1n << 53n) - 1n,
      1n << 53n,
      (1n << 53n) + 1n,
      1n << 62n,
      (1n << 62n) + 1n,
    ];
    const random = mulberry32(seed);
    for (const boundary of values)
      for (const direction of ["asc", "desc"] as const) {
        const gate = new FirstKeyThreshold({
          column: 0,
          direction,
          nulls: "last",
          collation: null,
        });
        const threshold = vectorOf(BIG, [boundary], {
          int64:
            (boundary === 0n || random() < 0.5) && boundary >= -1n && boundary <= 1n
              ? "safe"
              : "int64",
        });
        if (threshold.form !== "flat") throw new Error("Expected flat threshold");
        gate.capture(threshold, 0);
        for (const dictionary of [false, true]) {
          const flat = vectorOf(BIG, dictionary ? [...values].reverse() : values, {
            int64: "int64",
          });
          if (flat.form !== "flat") throw new Error("Expected flat dictionary");
          const input: Vector = dictionary
            ? {
                form: "dictionary",
                type: BIG,
                length: values.length,
                codes: Int32Array.from({ length: values.length }, (_, i) => values.length - 1 - i),
                dictionary: flat,
                dictionaryId: 1,
                validity: null,
              }
            : flat;
          const out = new Uint16Array(values.length);
          const count = gate.filter(input, null, values.length, out);
          const expected = values.flatMap((value, i) =>
            (direction === "asc" ? value <= boundary : value >= boundary) ? [i] : [],
          );
          expect([...out.subarray(0, count)]).toEqual(expected);
        }
      }
    reach.beginHistory();
    reach.reach("adjacent int64 words");
    reach.reach("safe threshold wide input");
  });

for (const seed of seedsFor("next-topn-first", [0x701, 0x325]))
  it(`non-null float fast gate vs PGlite seed ${String(seed)}`, async () => {
    const { FirstKeyThreshold } = await import("./sort-keys.js");
    const values = [NaN, -Infinity, -0, 0, 1, Infinity];
    const input = vectorOf(DBL, values);
    if (input.form !== "flat") throw new Error("Expected flat input");
    expect(input.validity).toBeNull();
    for (const boundary of values)
      for (const direction of ["asc", "desc"] as const) {
        const threshold = vectorOf(DBL, [boundary]);
        if (threshold.form !== "flat") throw new Error("Expected flat threshold");
        const gate = new FirstKeyThreshold({
          column: 0,
          direction,
          nulls: "last",
          collation: null,
        });
        gate.capture(threshold, 0);
        const ids = Uint16Array.from({ length: values.length }, (_, i) => values.length - 1 - i);
        const out = new Uint16Array(values.length);
        const count = gate.filter(input, ids, ids.length, out);
        const expected = (
          await pg.query<{ id: number }>(
            `SELECT id FROM (VALUES ${values.map((v, id) => `(${String(id)},'${String(v)}'::float8)`).join(",")}) t(id,x) WHERE x ${direction === "asc" ? "<=" : ">="} '${String(boundary)}'::float8 ORDER BY id DESC`,
          )
        ).rows.map((row) => row.id);
        expect([...out.subarray(0, count)]).toEqual(expected);
        const flatCount = gate.filter(input, null, values.length, out);
        expect([...out.subarray(0, flatCount)]).toEqual([...expected].reverse());
      }
    reach.beginHistory();
    reach.reach("non-null float specials");
  });
