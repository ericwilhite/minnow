/** Ordinary SQL over real writes. PostgreSQL owns equality, NULLs and lexical ordering. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STAR_SQL_REACH } from "../../testing/reach.js";
import { StarDictionaryBooks } from "../exec/star-dictionary.js";
import { createExecutor } from "../exec/create.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

afterEach(() => vi.restoreAllMocks());
const literal = (v: string | number | boolean | null): string =>
  v === null ? "NULL" : typeof v === "string" ? `'${v.replaceAll("'", "''")}'` : String(v);
const modes = [
  "stored",
  "single-key",
  "memory",
  "mixed",
  "non-PK",
  "composite",
  "parameter",
  "wide",
  "computed",
  "zero",
] as const;
const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((r) => ({
    x: r.x,
    y: r.y,
    z: r.z,
    n: Number(r.n),
    s: r.s === null ? null : Number(r.s),
  }));

it.each(seedsFor("next-executor-star-dictionary", [0xb76b, 0xb76c]))(
  "uses certified SQL producers and exact generic fallbacks (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const encode = vi.spyOn(StarDictionaryBooks.prototype, "encode");
    const reach = createReach("SQL star dictionary", STAR_SQL_REACH);
    const candidate = createExecutor(),
      control = createExecutor({ starDictionary: false });
    try {
      await pg.waitReady;
      for (const name of ["f_stored", "f_memory", "f_mixed"]) {
        const ddl = `CREATE TABLE ${name}(ka TEXT,kb TEXT,kc TEXT,v DOUBLE PRECISION)`;
        await core.execute(ddl);
        await pg.exec(ddl);
      }
      const keys = Array.from({ length: 12 }, (_, i) => `k${String(i)}`);
      const texts = ["z", "a", "汉", "", null];
      for (const name of ["a", "b", "c", "a_plain", "a_composite", "c_wide"]) {
        const composite = name === "a_composite";
        const ddl = `CREATE TABLE ${name}(k TEXT${name === "a_plain" || composite ? "" : " PRIMARY KEY"},t TEXT,label TEXT,active BOOLEAN,weight DOUBLE PRECISION${composite ? ", extra INTEGER, PRIMARY KEY(k,extra)" : ""})`;
        await core.execute(ddl);
        await pg.exec(ddl);
        const rows = keys.map((k, i) => ({
          k,
          t: texts[(i + (name === "b" ? 2 : 0)) % texts.length] ?? null,
          label: name === "c_wide" ? "w".repeat(140000) : `${String(11 - i)}汉`,
          active: i % 4 !== 1,
          weight: (i % 4) + 1,
        }));
        if (name === "a_plain") {
          const first = rows[0];
          if (!first) throw new Error("Missing dimension fixture");
          rows.push({ ...first, t: "duplicate" });
        }
        const values = composite ? rows.map((r) => ({ ...r, extra: 0 })) : rows;
        await core.writeBatch("insertBatch", name, values);
        await pg.exec(
          `INSERT INTO ${name} VALUES ${values.map((r) => `(${Object.values(r).map(literal).join(",")})`).join(",")}`,
        );
      }
      const key = (i: number): string => {
        const value = keys[i];
        if (value === undefined) throw new Error("Missing fact key");
        return value;
      };
      const fact = Array.from({ length: 4097 }, (_, i) => ({
        ka: i % 17 === 0 ? null : key(i % 12),
        kb: key(Math.floor(i / 3) % 12),
        kc: i % 19 === 0 ? "missing" : key(Math.floor(i / 7) % 12),
        v: i / 4,
      }));
      await core.writeBatch("insertBatch", "f_stored", fact);
      await core.writeBatch("insertBatch", "f_mixed", fact.slice(0, 2049));
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await core.writeBatch("insertBatch", "f_memory", fact);
      await core.writeBatch("insertBatch", "f_mixed", fact.slice(2049));
      for (const name of ["f_stored", "f_memory", "f_mixed"])
        await pg.exec(
          `INSERT INTO ${name} VALUES ${fact.map((r) => `(${Object.values(r).map(literal).join(",")})`).join(",")}`,
        );
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...modes),
          fc.boolean(),
          fc.boolean(),
          async (mode, descending, sumOrder) => {
            reach.beginHistory();
            const table =
              mode === "memory" ? "f_memory" : mode === "mixed" ? "f_mixed" : "f_stored";
            const a = mode === "non-PK" ? "a_plain" : mode === "composite" ? "a_composite" : "a",
              c = mode === "wide" ? "c_wide" : "c";
            const single = mode === "single-key";
            const group = "a.t",
              value = mode === "computed" ? "f.v+0.25" : "f.v";
            const sql = `SELECT ${group} AS x${single ? "" : ",b.t AS y,c.label AS z"},COUNT(*) AS n,SUM(${value}) AS s FROM ${table} f
            JOIN ${a} a ON a.k=f.ka JOIN b ON b.k=f.kb JOIN ${c} c ON c.k=f.kc
            WHERE a.active AND b.weight>1${mode === "wide" ? " AND f.v<1" : mode === "parameter" ? " AND f.v>$1" : ""}
            GROUP BY ${group}${single ? "" : ",b.t,c.label"} ORDER BY ${sumOrder ? "s DESC," : ""}x ${descending ? "DESC" : "ASC"} NULLS FIRST${single ? "" : ",y,z"}
            LIMIT ${String(mode === "zero" ? 0 : single ? 2 : 7)} OFFSET 1`;
            const args = mode === "parameter" ? [0.5] : [];
            encode.mockClear();
            core.executor.prepare = candidate.prepare.bind(candidate);
            const result = await core.execute(sql, args);
            if (result.kind !== "rows" || !("batches" in result))
              throw new Error("Missing star rows");
            const rows = canonical(oracleRows(result));
            const selected =
              mode === "stored" || mode === "single-key" || mode === "memory" || mode === "mixed";
            expect(encode.mock.calls.length > 0).toBe(selected);
            if (selected) {
              expect(
                core.lastProfile?.operators
                  .filter((op) => op.op === "join")
                  .every((op) => op.spilledBytes === 0),
              ).toBe(true);
              reach.reach("resident joins");
            }
            const expected = canonical(
              (await pg.query(sql, args)).rows as Array<Record<string, unknown>>,
            );
            expect(rows).toEqual(expected);
            core.executor.prepare = control.prepare.bind(control);
            const original = await core.execute(sql, args);
            if (original.kind !== "rows" || !("batches" in original))
              throw new Error("Missing control rows");
            expect(rows).toEqual(canonical(oracleRows(original)));
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach(mode);
            reach.reach("PG exact");
            reach.reach("generic exact");
            reach.reach(descending ? "descending" : "ascending");
            reach.reach(sumOrder ? "aggregate order" : "lexical top-N");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["stored", false, false],
            ["single-key", false, false],
            ["memory", true, false],
            ["mixed", false, true],
            ["non-PK", true, false],
            ["composite", false, true],
            ["parameter", false, true],
            ["wide", false, false],
            ["computed", true, true],
            ["zero", false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      encode.mockRestore();
      await core.close();
      await pg.close();
    }
  },
  180000,
);
