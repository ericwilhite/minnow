/**
 * ORDER BY against PostgreSQL itself (PGlite): integers, bigint, double precision with NaN, −0
 * and infinities, NUMERIC, text in C collation, JSONB, int arrays and booleans, with NULLs, every
 * direction and NULLS placement, LIMIT / OFFSET. The law is PostgreSQL's order; a trailing `id`
 * key makes it total, so the id sequences must match exactly.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import {
  ArraySource,
  BIG,
  BOOL,
  CollectSink,
  DBL,
  INT,
  NUM,
  TEXT,
  batchesOf,
  type Row,
  type Value,
} from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import type { SortColumn } from "./sort-keys.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";

const SEEDS = seedsFor("next-sort-pglite", [0x0bde, 0x5a]);
const JSONB: PhysicalType = { kind: "json", binary: true };
const INTS: PhysicalType = { kind: "array", element: INT };
const TYPES: PhysicalType[] = [INT, INT, BIG, DBL, NUM, TEXT, JSONB, INTS, BOOL];
const NAMES = ["id", "i", "b", "d", "n", "t", "j", "a", "z"];
let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

const JSONS = [
  '{"a":1}',
  "[1,2]",
  "1",
  "1.0",
  '"x"',
  "null",
  "true",
  "[]",
  '{"b":0}',
  '{"aa":0}',
  "2",
  "[1]",
];
const ARRAYS = ["[1,2]", "[1]", "[]", "[1,null]", "[2]", "[1,2,3]", "[null]"];

function rows(random: () => number, n: number): Row[] {
  const pick = <T>(list: readonly T[]) => list[Math.floor(random() * list.length)] as T;
  const maybe = <T>(value: T): T | null => (random() < 0.12 ? null : value);
  return Array.from({ length: n }, (_, id): Row => [
    id,
    maybe(Math.floor(random() * 20) - 10),
    maybe(BigInt(Math.floor(random() * 2e6)) * 4_611_686_018n - 9_000_000_000_000_000_000n / 2n),
    maybe(pick([Number.NaN, -0, 0, 1.5, -2.25, Infinity, -Infinity, 1e300, 3])),
    maybe(Math.floor(random() * 2000) - 1000),
    maybe(pick(["", "a", "ab", "B", "é", "😀", "z", "a\u0000b".replace("\u0000", "-")])),
    maybe(pick(JSONS)),
    maybe(pick(ARRAYS)),
    maybe(random() < 0.5),
  ]);
}

async function load(data: readonly Row[]): Promise<void> {
  await pg.exec(
    'DROP TABLE IF EXISTS s; CREATE TABLE s (id int, i int, b bigint, d double precision, n numeric(12,2), t text COLLATE "C", j jsonb, a int[], z boolean);',
  );
  for (const row of data) {
    const [id, i, b, d, n, t, j, a, z] = row;
    await pg.query(
      "INSERT INTO s VALUES ($1, $2, $3, $4::float8, $5::numeric / 100, $6, $7::jsonb, $8::int[], $9)",
      [
        id,
        i,
        b === null ? null : String(b),
        d === null
          ? null
          : Number.isNaN(d)
            ? "NaN"
            : d === Infinity
              ? "Infinity"
              : d === -Infinity
                ? "-Infinity"
                : Object.is(d, -0)
                  ? "-0"
                  : String(d),
        n,
        t,
        j,
        a === null
          ? null
          : `{${(JSON.parse(a as string) as Array<number | null>).map((v) => (v === null ? "NULL" : String(v))).join(",")}}`,
        z,
      ] as Value[],
    );
  }
}

describe("ORDER BY vs PostgreSQL", () => {
  for (const seed of SEEDS)
    it(`every key type, direction and NULLS placement (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const data = rows(random, 300);
      await load(data);
      for (let round = 0; round < 24; round++) {
        const columns = [1, 2, 3, 4, 5, 6, 7, 8]
          .sort(() => random() - 0.5)
          .slice(0, 1 + Math.floor(random() * 2));
        const keys: SortColumn[] = [
          ...columns.map((column): SortColumn => ({
            column,
            direction: random() < 0.5 ? "asc" : "desc",
            nulls: random() < 0.5 ? "first" : "last",
            collation: null,
          })),
          { column: 0, direction: "asc", nulls: "last", collation: null },
        ];
        const limit = random() < 0.5 ? null : Math.floor(random() * 40);
        const offset = random() < 0.5 ? 0 : Math.floor(random() * 50);
        const order = keys
          .map(
            (k) =>
              `${NAMES[k.column] ?? ""} ${k.direction.toUpperCase()} NULLS ${k.nulls.toUpperCase()}`,
          )
          .join(", ");
        const sql = `SELECT id FROM s ORDER BY ${order}${limit === null ? "" : ` LIMIT ${String(limit)}`} OFFSET ${String(offset)}`;
        const expected = (await pg.query<{ id: number }>(sql)).rows.map((r) => r.id);
        const minimum = sortMinimumBytes(TYPES);
        const tight = random() < 0.3;
        const governor = new Governor(
          tight ? Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7)) : 256 << 20,
        );
        const grant = await governor.admit({
          label: "sort",
          minimumBytes: minimum,
          spillable: true,
        });
        const sink = new CollectSink(random() < 0.3 ? 2 : 0);
        const operator = new SortOperator(
          { types: TYPES, keys, limit, offset, withTies: false },
          { grant, spill: new MemorySpillService(), queryId: 1 },
          sink,
        );
        await drive(
          new ArraySource(batchesOf(TYPES, data, { batchRows: 1 + Math.floor(random() * 64) })),
          operator,
        );
        expect(
          sink.rows.map((row) => row[0]),
          sql,
        ).toEqual(expected);
        expect(governor.usedBytes).toBe(0);
      }
    }, 120_000);
});

describe("ORDER BY vs PostgreSQL at the prefix-word boundaries (review B7-p2b-1 probe)", () => {
  it("text keys around the 4 × PREFIX_WORDS byte prefix, both directions and NULLS", async () => {
    // Keys equal through the prefix words and differing just past them, or ending inside them.
    const values = [
      null,
      "",
      "ab",
      "abc",
      ...[11, 12, 13, 19, 20, 21, 22, 23].map((n) => "a".repeat(n)),
    ];
    values.push(
      "a".repeat(21) + "b",
      "a".repeat(21) + "a",
      "a".repeat(19) + "é",
      "a".repeat(20) + "\u0001",
    );
    const rows: Row[] = Array.from({ length: 1100 }, (_, i) => [
      values[i % values.length] ?? null,
      i,
    ]);
    await pg.exec('DROP TABLE IF EXISTS p; CREATE TABLE p (t text COLLATE "C", id int)');
    await pg.query(
      "INSERT INTO p SELECT v, ord::int - 1 FROM unnest($1::text[]) WITH ORDINALITY a(v, ord)",
      [rows.map((row) => row[0])],
    );
    const types = [TEXT, INT];
    for (const direction of ["asc", "desc"] as const)
      for (const nulls of ["first", "last"] as const) {
        const expected = (
          await pg.query<{ id: number }>(
            `SELECT id FROM p ORDER BY t ${direction.toUpperCase()} NULLS ${nulls.toUpperCase()}, id`,
          )
        ).rows.map((row) => row.id);
        const governor = new Governor(64 << 20);
        const grant = await governor.admit({
          label: "prefix",
          minimumBytes: sortMinimumBytes(types),
          spillable: true,
        });
        const sink = new CollectSink();
        const keys: SortColumn[] = [
          { column: 0, direction, nulls, collation: null },
          { column: 1, direction: "asc", nulls: "last", collation: null },
        ];
        const operator = new SortOperator(
          { types, keys, limit: null, offset: 0, withTies: false },
          { grant, spill: new MemorySpillService(), queryId: 9 },
          sink,
        );
        await drive(new ArraySource(batchesOf(types, rows)), operator);
        expect(
          sink.rows.map((row) => row[1]),
          `${direction} nulls ${nulls}`,
        ).toEqual(expected);
      }
  }, 60_000);
});
