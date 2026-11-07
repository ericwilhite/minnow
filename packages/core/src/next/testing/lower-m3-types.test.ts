/** Typed M3 edges beyond the legacy engine's safe-number domain: PostgreSQL remains authority. */
import { batchesOf } from "../exec/join-fixtures.js";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { createReach, LOWER_M3_TYPES_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { encodeValue } from "../../testing/differential-harness.js";
import { oracleRows } from "./differential-adapter.js";
import { queryAuthorityValue } from "./divergences.js";
import { StorageCore } from "./storage-core.js";

const reach = createReach("M3 typed SQL", LOWER_M3_TYPES_REACH, { minimum: 2 });
const bag = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((row) => encodeValue(Object.values(row))).sort();
for (const seed of seedsFor("next-lower-m3-types", [0x7733, 0x7734]))
  it(`keeps typed joins, offsets and non-finite keys exact, seed ${String(seed)}`, async () => {
    const core = await new StorageCore(32).open();
    const pg = new PGlite();
    await pg.waitReady;
    const random = mulberry32(seed);
    reach.beginHistory();
    try {
      const domain = [
        null,
        -9223372036854775808n,
        -4611686018427387904n,
        -9007199254740993n,
        0n,
        9007199254740993n,
        4611686018427387904n,
        9223372036854775807n,
      ];
      for (const table of ["p", "b"]) {
        const ddl = `CREATE TABLE ${table}(id INTEGER PRIMARY KEY,k BIGINT,t TEXT,f BOOLEAN,n NUMERIC,x DOUBLE PRECISION)`;
        await core.execute(ddl);
        await pg.exec(ddl);
        const rows = Array.from({ length: 16 }, (_, id) => ({
          id,
          k: domain[id < domain.length ? id : Math.floor(random() * domain.length)] ?? null,
          t: id % 4 === 0 ? null : ["z", "é", "a"][id % 3],
          f: id % 4 === 0 ? null : id % 2 === 0,
          n: id % 3 === 0 ? null : String(2n ** 100n + BigInt(id)),
          x: [null, NaN, Infinity, -Infinity, 0, -0, 1][id % 7],
        }));
        const definition = core.harness.path.catalog.tableByName(table);
        if (definition === undefined) throw new Error("missing typed table");
        const input = rows.map((row) => [
          row.id,
          row.k,
          row.t ?? null,
          row.f,
          row.n,
          row.x ?? null,
        ]);
        for (const batch of batchesOf(
          definition.columns.map((column) => column.type),
          input,
          { int64: "int64" },
        )) {
          // The operator fixture deliberately varies NUMERIC display scale by slot. These SQL
          // inputs are integer coefficients, so every slot has scale zero.
          for (const vector of batch.columns)
            if (vector.form === "flat" && vector.data.layout === "decimal")
              vector.data.scales?.fill(0);
          await core.insertVectors(definition, batch);
        }
        for (const row of rows)
          await pg.query(`INSERT INTO ${table} VALUES($1,$2,$3,$4,$5,$6)`, [
            row.id,
            row.k,
            row.t,
            row.f,
            row.n,
            row.x,
          ]);
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      const check = async (sql: string, params: ReadonlyArray<number | bigint | null> = []) => {
        const prepared = core.prepare(sql);
        const result = await core.execute(sql, params);
        if (!("batches" in result)) throw new Error("rows expected");
        const authority = await pg.query<Record<string, unknown>>(sql, [...params]);
        const expected = authority.rows.map((row) =>
          Object.fromEntries(
            Object.entries(row).map(([name, value], column) => [
              name,
              queryAuthorityValue(
                prepared.plan,
                column,
                typeof value === "bigint" &&
                  value >= BigInt(Number.MIN_SAFE_INTEGER) &&
                  value <= BigInt(Number.MAX_SAFE_INTEGER)
                  ? Number(value)
                  : value,
              ),
            ]),
          ),
        );
        expect(bag(oracleRows(result)), sql).toEqual(bag(expected));
      };
      const power = Math.floor(random() * 6),
        base = 2 ** power,
        step = 2 ** (power - 23),
        offset = step * (1 - 2 ** -26);
      const ddl = "CREATE TABLE r(id INTEGER PRIMARY KEY,x REAL)";
      await core.execute(ddl);
      await pg.exec(ddl);
      const realRows = [
        { id: 0, x: base },
        { id: 1, x: base + step },
      ];
      await core.writeBatch("insertBatch", "r", realRows);
      await pg.query("INSERT INTO r VALUES(0,$1),(1,$2)", [base, base + step]);
      await check(
        `SELECT id,COUNT(*)OVER(ORDER BY x RANGE BETWEEN ${String(offset)} PRECEDING AND CURRENT ROW)AS n FROM r ORDER BY id`,
      );
      await check(
        "SELECT id,COUNT(*)OVER(ORDER BY x RANGE BETWEEN $1 PRECEDING AND CURRENT ROW)AS n FROM r ORDER BY id",
        [offset],
      );
      reach.reach("float4 offset");
      for (const kind of ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"] as const) {
        for (const key of ["k", "t", "f", "n", "x"] as const) {
          await check(
            `SELECT p.id AS pid,b.id AS bid,p.k AS pk,b.k AS bk,p.n AS pn,b.n AS bn FROM p ${kind} JOIN b${kind === "CROSS" ? "" : ` ON p.${key}=b.${key}`} ORDER BY pid NULLS FIRST,bid NULLS FIRST`,
          );
          reach.reach(kind);
          reach.reach(key);
        }
      }
      await check(
        "SELECT p.id AS a,b.id AS b,d.id AS c,e.id AS d FROM p FULL JOIN b ON p.k=b.k JOIN p d ON d.k=b.k FULL JOIN b e ON e.k=d.k WHERE d.id IS NULL ORDER BY a NULLS FIRST,b NULLS FIRST,c NULLS FIRST,d NULLS FIRST",
      );
      await check(
        "SELECT p.id AS a,b.id AS b,d.id AS c FROM p FULL JOIN b ON p.k=b.k JOIN p d ON p.id IS NULL ORDER BY a NULLS FIRST,b NULLS FIRST,c NULLS FIRST",
      );
      reach.reach("outer chain");
      for (const test of ["IN", "NOT IN"] as const) {
        await check(`SELECT id,k ${test}(SELECT k FROM b)AS found FROM p ORDER BY id`);
        reach.reach(test);
      }
      for (const unit of ["ROWS", "GROUPS"]) {
        for (const offset of [
          "2.5",
          "0.4999999999999999999",
          "9007199254740993",
          "9223372036854775807",
        ]) {
          await check(
            `SELECT id,COUNT(*) OVER(ORDER BY id ${unit} BETWEEN ${offset} PRECEDING AND CURRENT ROW)AS n FROM p ORDER BY id`,
          );
          reach.reach("exact offsets");
        }
        await check(
          `SELECT id,COUNT(*) OVER(ORDER BY id ${unit} BETWEEN $1 PRECEDING AND CURRENT ROW)AS n FROM p ORDER BY id`,
          [3],
        );
        reach.reach("parameters");
      }
      await check(
        "SELECT id,COUNT(*)OVER(ORDER BY x RANGE BETWEEN $1 PRECEDING AND $1 FOLLOWING)AS n FROM p ORDER BY id",
        [0],
      );
      reach.reach("non-finite RANGE");
      await check(
        "SELECT id,COUNT(*)OVER(ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)AS n FROM p ORDER BY id",
      );
      reach.reach("int64 RANGE");

      await check(
        "SELECT id,COUNT(*)OVER(ORDER BY x RANGE BETWEEN $1 PRECEDING AND $1 FOLLOWING)AS n FROM p ORDER BY id",
        [Infinity],
      );
      for (const offset of ["2147483648", "9007199254740991"])
        await check(
          `SELECT id,COUNT(*)OVER(ORDER BY id RANGE BETWEEN ${offset} PRECEDING AND CURRENT ROW)AS n FROM p ORDER BY id`,
        );
      for (const offset of ["-1", "NULL"]) {
        const sql = `SELECT id,COUNT(*)OVER(ORDER BY id ROWS BETWEEN ${offset} PRECEDING AND CURRENT ROW)AS n FROM p ORDER BY id`;
        const sqlState = offset === "NULL" ? "22004" : "22013";
        await expect(pg.query(sql)).rejects.toMatchObject({ code: sqlState });
        expect(() => core.prepare(sql)).toThrow(expect.objectContaining({ sqlState }));
      }

      for (const unit of ["ROWS", "RANGE", "GROUPS"]) {
        const sql = `SELECT COUNT(*)OVER(ORDER BY id ${unit} BETWEEN CURRENT ROW AND 1 PRECEDING)AS n FROM p`;
        await expect(pg.query(sql)).rejects.toMatchObject({ code: "42P20" });
        expect(() => core.prepare(sql)).toThrow(expect.objectContaining({ sqlState: "42P20" }));
      }
      await check(
        "SELECT id,SUM(k)OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)AS s,LAG(k)OVER(ORDER BY id)AS prior FROM p ORDER BY id",
      );
      reach.reach("wide sum and lag");
      for (const offset of [NaN, -1, null]) {
        const sql =
          "SELECT id,COUNT(*)OVER(ORDER BY x RANGE BETWEEN $1 PRECEDING AND CURRENT ROW)AS n FROM p ORDER BY id";
        const code = offset === null ? "22004" : "22013";
        await expect(pg.query(sql, [offset])).rejects.toMatchObject({ code });
        await expect(core.execute(sql, [offset])).rejects.toMatchObject({ sqlState: code });
        reach.reach("invalid offsets");
      }
      expect(core.spill.bytesInUse).toBe(0);
    } finally {
      await core.close();
      await pg.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
    reach.reach("released");
  }, 180_000);
it("reaches the typed SQL dimensions", () => reach.assertReached());
