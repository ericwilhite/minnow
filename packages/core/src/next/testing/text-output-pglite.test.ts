/** PG output functions govern ::text and text || anynonarray, not JS coercion. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TEXT_OUTPUT_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { outputCase, outputVector, type OutputShape } from "./text-output-cases.test.fixture.js";
const base: OutputShape = {
  family: 0,
  n: 7,
  scale: 3,
  sign: true,
  nil: false,
  scope: 0,
  bits: 0x8000000000000000n,
};
for (const seed of seedsFor("text-output", [47065, 47066]))
  it(`matches output functions through native storage seed${String(seed)}`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach<string>("text output", TEXT_OUTPUT_REACH),
      created = new Set<number>();
    let history = 0;
    try {
      await pg.waitReady;
      await pg.exec("SET TIME ZONE 'UTC'");
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            family: fc.integer({ min: 0, max: 15 }),
            n: fc.integer({ min: -1024, max: 1024 }),
            scale: fc.integer({ min: 0, max: 12 }),
            sign: fc.boolean(),
            nil: fc.boolean(),
            scope: fc.integer({ min: 0, max: 2 }),
            bits: fc.bigInt({ min: 0n, max: 0xffffffffffffffffn }),
          }),
          async (s) => {
            reach.beginHistory();
            const c = outputCase(s),
              name = `out_${String(s.family)}`,
              id = history++;
            // A failure to materialize an oracle input is a harness bug, never cast-error reach.
            try {
              await pg.query(`SELECT $1::${c.sqlType}`, [c.input]);
            } catch (e) {
              throw new Error("Harness bug: output-function input refused", { cause: e });
            }
            if (!created.has(s.family)) {
              await core.execute(`CREATE TABLE ${name}(i double precision,v ${c.sqlType})`);
              await pg.exec(`CREATE TABLE ${name}(i double precision,v ${c.sqlType})`);
              created.add(s.family);
            }
            const table = core.harness.path.catalog.tableByName(name);
            if (!table) throw new Error("Missing table");
            await core.insertVectors(table, {
              length: 1,
              selection: null,
              columns: [outputVector({ kind: "float64" }, id), outputVector(c.type, c.value)],
            });
            await pg.query(`INSERT INTO ${name} VALUES($1,$2::${c.sqlType})`, [id, c.input]);
            const source =
                s.scope === 0
                  ? `${name} WHERE i=${String(id)}`
                  : `(SELECT v FROM ${name} WHERE i=${String(id)})q`,
              value = s.family === 15 ? "v::text" : "v",
              select = `SELECT v::text t,'x'::text||${value} lhs,${value}||'y'::text rhs FROM ${source}`,
              sql =
                s.scope === 2
                  ? `WITH z AS(SELECT v FROM ${name} WHERE i=${String(id)}) SELECT v::text t,'x'::text||${value} lhs,${value}||'y'::text rhs FROM z`
                  : select;
            const expected = (await pg.query(sql)).rows,
              actual = await core.execute(sql);
            if (!("batches" in actual)) throw new Error("Rows required");
            expect(oracleRows(actual)).toEqual(expected);
            reach.reach(
              [
                "int2",
                "int4",
                "int8",
                "numeric",
                "float4",
                "float8",
                "boolean",
                "date",
                "time",
                "timestamp",
                "timestamptz",
                "interval",
                "uuid",
                "json",
                "jsonb",
                "array",
              ][s.family] ?? "array",
            );
            if (s.nil) reach.reach("NULL");
            if (s.family === 3 && s.scale > 0 && !s.nil) reach.reach("numeric scale");
            if (s.family === 8 && s.scale === 0 && !s.nil) reach.reach("24:00");
            if (
              (s.family === 7 || s.family === 9 || s.family === 10 || s.family === 11) &&
              s.scale < 2 &&
              !s.nil
            )
              reach.reach("infinity");
            reach.reach(["direct", "derived", "CTE"][s.scope] ?? "direct");
            reach.reach("PG exact");
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 96,
          examples: [
            ...Array.from({ length: 16 }, (_, family) => [{ ...base, family }] as [OutputShape]),
            [{ ...base, family: 8, scale: 0 }],
            [{ ...base, family: 7, scale: 0 }],
            [{ ...base, family: 9, scale: 1 }],
            [{ ...base, family: 11, scale: 0 }],
            [{ ...base, nil: true, scope: 1 }],
            [{ ...base, scope: 2 }],
          ],
          endOnFailure: true,
        },
      );
      reach.assertReached();
      // Pin the review's ordinary SQL path independently of storage fixtures.
      const pinned = await core.execute("SELECT 'x'||1 a,TRUE::text b");
      if (!("batches" in pinned)) throw new Error("Rows required");
      expect(oracleRows(pinned)).toEqual((await pg.query("SELECT 'x'||1 a,TRUE::text b")).rows);
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
