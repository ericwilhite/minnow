/** PostgreSQL is the law; queries run through the real storage/executor and the shipped engine. */
import type { PhysicalType } from "../types.js";
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { createReach, ORDERED_SET_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";
import { ORDERED_SET_CURRENT_REAL_AUTHORITY, ORDERED_SET_LEGACY_BIGINT } from "./divergences.js";
import { oracleRows } from "./differential-adapter.js";
import { batchesOf } from "../exec/join-fixtures.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

function present<T>(value: T | undefined): T {
  if (value === undefined) throw new TypeError("Missing generated dimension");
  return value;
}
function primitiveText(value: unknown): string {
  if (typeof value === "string" || typeof value === "number" || typeof value === "bigint")
    return String(value);
  throw new TypeError("Expected scalar numeric value");
}
function numeric(value: unknown): string {
  const text = primitiveText(value);
  const match = /^(-?)(\d+)(?:\.(\d+))?$/u.exec(text);
  if (!match) return text;
  let coefficient = BigInt((match[1] ?? "") + (match[2] ?? "0") + (match[3] ?? ""));
  let scale = (match[3] ?? "").length;
  while (scale > 0 && coefficient % 10n === 0n) {
    coefficient /= 10n;
    scale--;
  }
  return `${String(coefficient)}e-${String(scale)}`;
}
function canonical(
  rows: ReadonlyArray<Record<string, unknown>>,
  oids: readonly number[],
  postgresWire = false,
): unknown[] {
  return rows.map((row) =>
    Object.values(row).map((v, k) =>
      v === null
        ? null
        : oids[k] === 1700
          ? numeric(v)
          : oids[k] === 20
            ? primitiveText(v)
            : oids[k] === 700 && postgresWire
              ? Math.fround(Number(v))
              : v,
    ),
  );
}
// NUMERIC display scale is part of the next-core contract, as well as its exact value.
function decimalText(rows: ReadonlyArray<Record<string, unknown>>, oids: readonly number[]) {
  return rows.map((row) => Object.values(row).filter((_, index) => oids[index] === 1700));
}
function oid(type: PhysicalType): number {
  switch (type.kind) {
    case "int32":
      return type.bits === 16 ? 21 : 23;
    case "int64":
      return 20;
    case "float32":
      return 700;
    case "float64":
      return 701;
    case "decimal":
      return 1700;
    default:
      throw new TypeError("Unexpected ordered-set result type");
  }
}
const FRACTIONS = [0, 0.25, 0.5, 1, null, -0.25, 1.25, 0.1, 0.9] as const;
const TYPES = ["integer", "double precision", "numeric(12,2)", "bigint"] as const;
interface Shape {
  type: number;
  fraction: number;
  grouped: boolean;
  desc: boolean;
  rows: Array<[number | null, number | null]>;
}
async function check(shape: Shape, reach: ReturnType<typeof createReach<string>>): Promise<void> {
  const core = await new StorageCore(16).open();
  const current = new MinnowDatabase(new MemoryBlockStore(), {
    autoCompact: false,
    autoCollect: false,
  });
  try {
    const type = present(TYPES[shape.type]);
    const fraction = present(FRACTIONS[shape.fraction]);
    const ddl = `CREATE TABLE ordered_data (id integer PRIMARY KEY, g integer, x ${type})`;
    await pg.exec(`DROP TABLE IF EXISTS ordered_data;${ddl}`);
    await core.execute(ddl);
    await current.execute(ddl);
    const rows = shape.rows.map(([g, x], index) => ({
      id: index,
      g,
      x: shape.type === 0 || shape.type === 3 || x === null ? x : x / 10,
    }));
    if (rows.length) {
      await core.writeBatch("insertBatch", "ordered_data", rows);
      await current.insertBatch("ordered_data", rows);
      await pg.query(
        `INSERT INTO ordered_data VALUES ${rows.map((_, k) => `($${String(3 * k + 1)},$${String(3 * k + 2)},$${String(3 * k + 3)})`).join(",")}`,
        rows.flatMap((r) => [r.id, r.g, r.x]),
      );
    }
    const direction = shape.desc ? "DESC" : "ASC";
    const sql = `SELECT ${shape.grouped ? "g," : ""}percentile_cont($1) WITHIN GROUP(ORDER BY x ${direction}) AS cont,percentile_disc($1) WITHIN GROUP(ORDER BY x ${direction}) AS disc,mode() WITHIN GROUP(ORDER BY x ${direction}) AS mode FROM ordered_data ${shape.grouped ? "GROUP BY g ORDER BY g NULLS LAST" : ""}`;
    let expected;
    try {
      expected = await pg.query<Record<string, unknown>>(sql, [fraction]);
    } catch (error) {
      expect((error as { code: string }).code).toBe("22003");
      await expect(core.execute(sql, [fraction])).rejects.toMatchObject({ sqlState: "22003" });
      await expect(current.query(sql, { params: [fraction] })).rejects.toBeInstanceOf(RangeError);
      reach.reach("range error");
      return;
    }
    const actual = await core.execute(sql, [fraction]);
    if (!("batches" in actual)) throw new Error("Expected query rows");
    const oids = expected.fields.map((f) => f.dataTypeID);
    expect(actual.columns.map((c) => oid(c.type))).toEqual(oids);
    expect(decimalText(oracleRows(actual), oids)).toEqual(decimalText(expected.rows, oids));
    expect(canonical(oracleRows(actual), oids), sql).toEqual(canonical(expected.rows, oids, true));
    expect(
      canonical((await current.query(sql, { params: [fraction] })).rows, oids),
      sql + " shipped",
    ).toEqual(canonical(expected.rows, oids, true));
    const counts = new Map<number | null, Map<number, number>>();
    for (const [g, x] of shape.rows) {
      if (x === null) continue;
      const key = shape.grouped ? g : 0;
      const group = counts.get(key) ?? new Map<number, number>();
      group.set(x, (group.get(x) ?? 0) + 1);
      counts.set(key, group);
    }
    for (const group of counts.values()) {
      const frequencies = [...group.values()];
      const best = Math.max(...frequencies);
      if (frequencies.filter((n) => n === best).length > 1) reach.reach("mode tie");
      const n = frequencies.reduce((a, b) => a + b, 0);
      if (fraction !== null && !Number.isInteger(fraction * (n - 1))) reach.reach("interpolation");
    }
    reach.reach("percentile_cont");
    reach.reach("percentile_disc");
    reach.reach("mode");
  } finally {
    await current.close();
    await core.close();
    expect(core.spill.bytesInUse).toBe(0);
  }
}

it.each(seedsFor("next-ordered-set", [0xb601, 0xb602]))(
  "matches PostgreSQL ordered sets (seed %i)",
  async (seed) => {
    const reach = createReach<string>("ordered-set SQL", ORDERED_SET_REACH);
    const examples: Array<[Shape]> = [];
    for (let type = 0; type < TYPES.length; type++)
      for (let fraction = 0; fraction < FRACTIONS.length; fraction++)
        examples.push([
          {
            type,
            fraction,
            grouped: fraction % 2 === 0,
            desc: fraction % 3 === 0,
            rows: [
              [0, 2],
              [0, 4],
              [0, null],
              [null, 4],
              [1, 9],
              [1, 1],
              [1, 1],
              [2, null],
            ],
          },
        ]);
    examples.push(
      [{ type: 0, fraction: 2, grouped: false, desc: false, rows: [] }],
      [{ type: 2, fraction: 2, grouped: true, desc: true, rows: [] }],
      [{ type: 1, fraction: 2, grouped: false, desc: false, rows: [[null, null]] }],
    );
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          type: fc.integer({ min: 0, max: 3 }),
          fraction: fc.integer({ min: 0, max: FRACTIONS.length - 1 }),
          grouped: fc.boolean(),
          desc: fc.boolean(),
          rows: fc.array(
            fc.tuple(
              fc.option(fc.integer({ min: 0, max: 3 }), { nil: null }),
              fc.option(fc.integer({ min: -100, max: 100 }), { nil: null }),
            ),
            { maxLength: 40 },
          ),
        }),
        async (shape) => {
          reach.beginHistory();
          await check(shape, reach);
          reach.reach(present(TYPES[shape.type]));
          reach.reach(shape.grouped ? "grouped" : "scalar");
          reach.reach(shape.desc ? "descending" : "ascending");
          if (!shape.rows.length) reach.reach("empty input");
          if (shape.rows.some((r) => r[1] === null)) reach.reach("null values");
          if (shape.rows.some((r) => r[0] === null)) reach.reach("null group");
          if (FRACTIONS[shape.fraction] === null) reach.reach("null fraction");
          if (FRACTIONS[shape.fraction] === 0) reach.reach("zero fraction");
          if (FRACTIONS[shape.fraction] === 1) reach.reach("one fraction");
        },
      ),
      { seed, numRuns: 64, examples },
    );
    reach.assertReached();
  },
  300_000,
);

it("keeps ordered discrete values exact past 2^53 and across wide numeric tiers", async () => {
  for (const type of ["bigint", "numeric"]) {
    const core = await new StorageCore().open();
    const current = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
    });
    try {
      const ddl = `CREATE TABLE precise_ordered(id integer PRIMARY KEY,x ${type})`;
      await pg.exec(`DROP TABLE IF EXISTS precise_ordered;${ddl}`);
      await core.execute(ddl);
      await current.execute(ddl);
      const values =
        type === "bigint"
          ? ["9007199254740993", "9007199254740994", "9007199254740995", "9007199254740995"]
          : [
              "123456789012345678901234567890.125",
              "123456789012345678901234567890.25",
              "123456789012345678901234567890.375",
              "123456789012345678901234567890.375",
            ];
      const sqlInsert = `INSERT INTO precise_ordered VALUES ${values.map((x, i) => `(${String(i)},${x})`).join(",")}`;
      await pg.exec(sqlInsert);
      if (type === "bigint") {
        let refusal: unknown;
        try {
          await current.execute(sqlInsert);
        } catch (error) {
          refusal = error;
        }
        expect(ORDERED_SET_LEGACY_BIGINT.matches(ddl, values.map(BigInt), refusal)).toBe(true);
        expect(ORDERED_SET_LEGACY_BIGINT.matches(ddl, [1n], refusal)).toBe(false);
        expect(
          ORDERED_SET_LEGACY_BIGINT.matches(ddl, values.map(BigInt), new TypeError("unrelated")),
        ).toBe(false);
      } else await current.execute(sqlInsert);
      if (type === "bigint") {
        const table = core.harness.path.catalog.tableByName("precise_ordered");
        if (table === undefined) throw new Error("Missing exact integer table");
        for (const batch of batchesOf(
          table.columns.map((column) => column.type),
          values.map((x, id) => [id, BigInt(x)]),
          { int64: "int64" },
        ))
          await core.insertVectors(table, batch);
      } else
        await core.writeBatch(
          "insertBatch",
          "precise_ordered",
          values.map((x, id) => ({ id, x })),
        );
      const sql =
        "SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY x) AS c,percentile_disc(0.5) WITHIN GROUP(ORDER BY x) AS d,mode() WITHIN GROUP(ORDER BY x) AS m FROM precise_ordered";
      const expected = await pg.query<Record<string, unknown>>(sql);
      const result = await core.execute(sql);
      if (!("batches" in result)) throw new Error("Expected frames");
      const oids = expected.fields.map((f) => f.dataTypeID);
      expect(result.columns.map((c) => oid(c.type))).toEqual(oids);
      expect(decimalText(oracleRows(result), oids)).toEqual(decimalText(expected.rows, oids));
      expect(canonical(oracleRows(result), oids)).toEqual(canonical(expected.rows, oids, true));
      if (type !== "bigint")
        expect(canonical((await current.query(sql)).rows, oids)).toEqual(
          canonical(expected.rows, oids, true),
        );
    } finally {
      await current.close();
      await core.close();
      expect(core.spill.bytesInUse).toBe(0);
    }
  }
}, 120_000);

it("compares REAL ordered sets with PG float32, and legacy with its registered FLOAT8 witness", async () => {
  const core = await new StorageCore().open();
  const current = new MinnowDatabase(new MemoryBlockStore(), {
    autoCompact: false,
    autoCollect: false,
  });
  try {
    const ddl = "CREATE TABLE real_ordered(id integer PRIMARY KEY,x real)";
    await pg.exec(
      `DROP TABLE IF EXISTS real_ordered;DROP TABLE IF EXISTS ordered_real_source;${ddl};CREATE TABLE ordered_real_source(id integer PRIMARY KEY,x double precision)`,
    );
    await core.execute(ddl);
    await current.execute(ddl);
    const rows = [
      { id: 1, x: 0.1 },
      { id: 2, x: 0.2 },
      { id: 3, x: 0.2 },
      { id: 4, x: 0.3 },
    ];
    await core.writeBatch("insertBatch", "real_ordered", rows);
    await current.insertBatch("real_ordered", rows);
    await pg.exec(
      "INSERT INTO real_ordered VALUES(1,0.1),(2,0.2),(3,0.2),(4,0.3);INSERT INTO ordered_real_source VALUES(1,0.1),(2,0.2),(3,0.2),(4,0.3)",
    );
    const sql =
      "SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY x) AS c,percentile_disc(0.5) WITHIN GROUP(ORDER BY x) AS d,mode() WITHIN GROUP(ORDER BY x) AS m FROM real_ordered";
    const expected = await pg.query<Record<string, unknown>>(sql);
    const actual = await core.execute(sql);
    if (!("batches" in actual)) throw new Error("Expected frames");
    expect(
      canonical(
        oracleRows(actual),
        expected.fields.map((f) => f.dataTypeID),
      ),
    ).toEqual(
      canonical(
        expected.rows,
        expected.fields.map((f) => f.dataTypeID),
        true,
      ),
    );
    const witness = await pg.query<Record<string, unknown>>(
      sql.replace("FROM real_ordered", `FROM ${ORDERED_SET_CURRENT_REAL_AUTHORITY.table}`),
    );
    expect((await current.query(sql)).rows).toEqual(witness.rows);
    expect(expected.rows).not.toEqual(witness.rows);
  } finally {
    await current.close();
    await core.close();
    expect(core.spill.bytesInUse).toBe(0);
  }
}, 120_000);

it("keeps fraction evaluation lazy under LIMIT 0 and matches invalid-overload SQLSTATE", async () => {
  const core = await new StorageCore().open();
  try {
    await core.execute("CREATE TABLE type_ordered(id integer PRIMARY KEY,x integer,t text)");
    await pg.exec(
      "DROP TABLE IF EXISTS type_ordered;CREATE TABLE type_ordered(id integer PRIMARY KEY,x integer,t text)",
    );
    for (const sql of [
      "SELECT percentile_cont(2) WITHIN GROUP(ORDER BY x) FROM type_ordered LIMIT 0",
      "SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY NULL) AS p FROM type_ordered",
    ]) {
      const expected = await pg.query<Record<string, unknown>>(sql);
      const actual = await core.execute(sql);
      if (!("batches" in actual)) throw new Error("Expected frames");
      expect(oracleRows(actual)).toEqual(expected.rows);
    }
    for (const fraction of [-0.01, 1.01, NaN, Infinity]) {
      const sql = "SELECT percentile_cont($1) WITHIN GROUP(ORDER BY x) FROM type_ordered";
      await expect(pg.query(sql, [fraction])).rejects.toMatchObject({ code: "22003" });
      await expect(core.execute(sql, [fraction])).rejects.toMatchObject({ sqlState: "22003" });
    }

    for (const sql of [
      "SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY t) FROM type_ordered",
      "SELECT percentile_disc(TRUE) WITHIN GROUP(ORDER BY x) FROM type_ordered",
    ]) {
      await expect(pg.query(sql)).rejects.toMatchObject({ code: "42883" });
      await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "42883" });
    }
  } finally {
    await core.close();
  }
}, 120_000);
