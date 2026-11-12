/** Resident recoding preserves the complete native row sequence and PG float transitions. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STAR_FLOAT_REACH } from "../../testing/reach.js";
import { AggregateOperator } from "../exec/agg/aggregate.js";
import { StarDictionaryBooks } from "../exec/star-dictionary.js";
import { createExecutor } from "../exec/create.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
const encode = StarDictionaryBooks.prototype.encode;
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
const pushPrepared = AggregateOperator.prototype.pushPrepared;
afterEach(() => vi.restoreAllMocks());
const bits = (n: number) => {
  if (Number.isNaN(n)) return "NaN";
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, n);
  return view.getBigUint64(0).toString(16);
};
const text = (s: string | null) =>
  s === null ? "NULL::text" : `'${s.replaceAll("'", "''")}'::text`;
const float = (n: number) => `'${Object.is(n, -0) ? "-0" : String(n)}'::double precision`;
const canonical = (rows: Array<Record<string, unknown>>) =>
  rows
    .map((r) => ({
      keys: [r.x, r.y, r.z],
      n: Number(r.n),
      bits: r.s === null ? null : bits(Number(r.s)),
    }))
    .sort((a, b) => JSON.stringify(a.keys).localeCompare(JSON.stringify(b.keys)));
interface Trace {
  keys: Array<string | null>;
  value: number;
}

it.each(seedsFor("next-star-float-order", [0xb771, 0xb772]))(
  "preserves every input and PG operation including specials (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const reach = createReach("star float transition order", STAR_FLOAT_REACH);
    try {
      await pg.waitReady;
      for (const name of ["a", "b", "c"]) {
        await core.execute(`CREATE TABLE ${name}(k TEXT PRIMARY KEY,t TEXT,label TEXT)`);
        await core.writeBatch(
          "insertBatch",
          name,
          Array.from({ length: 8 }, (_, i) => ({
            k: `k${String(i)}`,
            t: i % 3 === 0 ? null : `t${String(i % 2)}`,
            label: i % 3 === 0 ? null : `l${String(i % 2)}`,
          })),
        );
      }
      let history = 0;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("finite", "special", "overflow"),
          fc.constantFrom(0, 65, 2049),
          fc.integer({ min: 0, max: 7 }),
          async (regime, n, shift) => {
            reach.beginHistory();
            const table = `f${String(history++)}`;
            await core.execute(`CREATE TABLE ${table}(ka TEXT,kb TEXT,kc TEXT,v DOUBLE PRECISION)`);
            const values =
              regime === "finite"
                ? [1e16, 1, -1e16, -0, 2 ** -40, -(2 ** -40), 0.25]
                : regime === "special"
                  ? [Infinity, -Infinity, NaN, -0, 1, -1, 0.25]
                  : [1e308];
            await core.writeBatch(
              "insertBatch",
              table,
              Array.from({ length: n }, (_, i) => ({
                ka: `k${String(i % 8)}`,
                kb: `k${String(Math.floor(i / 8) % 8)}`,
                kc: `k${String(Math.floor(i / 64) % 8)}`,
                v: values[(i + shift) % values.length] ?? 0,
              })),
            );
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const sql = `SELECT a.t AS x,b.t AS y,c.label AS z,count(*) AS n,sum(f.v) AS s FROM ${table} f JOIN a ON a.k=f.ka JOIN b ON b.k=f.kb JOIN c ON c.k=f.kc GROUP BY a.t,b.t,c.label`;
            const observe = async (coded: boolean) => {
              const maps = new Map<number, Map<number | null, string | null>>(),
                trace: Trace[] = [];
              let groupBooks: readonly number[] | undefined,
                encoded = 0;
              const observer = vi
                .spyOn(StarDictionaryBooks.prototype, "encode")
                .mockImplementation(function (this: StarDictionaryBooks, scan, batch) {
                  const result = encode.call(this, scan, batch);
                  groupBooks = this.shape.groupBooks;
                  encoded++;
                  for (const [column, book] of (this.shape.columns.get(scan) ?? []).entries()) {
                    if (book < 0) continue;
                    let map = maps.get(book);
                    if (!map) {
                      map = new Map();
                      maps.set(book, map);
                    }
                    for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
                      const row = batch.selection?.rows[k] ?? k,
                        source = batch.columns[column],
                        output = result.columns[column];
                      if (!source || !output) throw new Error("Missing captured star source");
                      const id = valueAt(output, row),
                        value = valueAt(source, row);
                      if (
                        (id !== null && typeof id !== "number") ||
                        (value !== null && typeof value !== "string")
                      )
                        throw new Error("Unexpected star source class");
                      map.set(id, value);
                    }
                  }
                  return result;
                });
              const input = vi
                .spyOn(AggregateOperator.prototype, "pushPrepared")
                .mockImplementation(function (this: AggregateOperator, ...args) {
                  const [batch] = args;
                  const arg = this.rel.aggregates.find((a) => a.fn === "sum")?.args[0];
                  if (arg?.op !== "column") throw new Error("Missing observed float sum");
                  const vector = batch.columns[arg.index];
                  if (!vector) throw new Error("Missing float input");
                  for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
                    const row = batch.selection?.rows[k] ?? k;
                    const keys = this.rel.groupBy.map((index, group) => {
                      const key = batch.columns[index];
                      if (!key) throw new Error("Missing transition key");
                      const value = valueAt(key, row);
                      if (!coded) {
                        if (value === null || typeof value === "string") return value;
                        throw new Error("Expected text control");
                      }
                      const book = groupBooks?.[group],
                        map = book === undefined ? undefined : maps.get(book);
                      if ((value !== null && typeof value !== "number") || !map?.has(value))
                        throw new Error("Missing canonical class map");
                      return map.get(value) ?? null;
                    });
                    const value = valueAt(vector, row);
                    if (typeof value !== "number") throw new Error("Expected float transition");
                    trace.push({ keys, value });
                  }
                  return pushPrepared.call(this, ...args);
                });
              const executor = createExecutor({ starDictionary: coded });
              core.executor.prepare = executor.prepare.bind(executor);
              let error: unknown, result: ReturnType<typeof canonical> | undefined;
              try {
                const output = await core.execute(sql);
                if (output.kind !== "rows" || !("batches" in output))
                  throw new Error("Missing float rows");
                result = canonical(oracleRows(output));
              } catch (caught) {
                error = caught;
              } finally {
                input.mockRestore();
                observer.mockRestore();
              }
              if (coded) expect(encoded).toBeGreaterThan(0);
              return { trace, result, error };
            };
            const control = await observe(false),
              candidate = await observe(true);
            const sequence = (trace: Trace[]) =>
              trace.map((row) => ({ keys: row.keys, bits: bits(row.value) }));
            expect(sequence(candidate.trace)).toEqual(sequence(control.trace));
            if (!control.error) {
              expect(control.trace).toHaveLength(n);
              expect(candidate.trace).toHaveLength(n);
            }
            if (control.error) {
              expect(control.error).toMatchObject({ sqlState: "22003" });
              expect(candidate.error).toMatchObject({ sqlState: "22003" });
            } else {
              expect(candidate.error).toBeUndefined();
              expect(candidate.result).toEqual(control.result);
            }
            if (control.trace.length > 0) {
              const query = `SELECT x,y,z,count(*) AS n,sum(v ORDER BY ord) AS s FROM (VALUES ${control.trace.map((r, i) => `(${r.keys.map(text).join(",")},${float(r.value)},${String(i)})`).join(",")}) t(x,y,z,v,ord) GROUP BY x,y,z`;
              if (control.error)
                await expect(pg.query(query)).rejects.toMatchObject({ code: "22003" });
              else
                expect(candidate.result).toEqual(
                  canonical((await pg.query(query)).rows as Array<Record<string, unknown>>),
                );
              reach.reach("PG ordered transitions");
            }
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach(regime);
            reach.reach(n === 0 ? "empty" : n > 2048 ? "batch boundary" : "nonempty");
            reach.reach("identical native sequence");
            reach.reach("bit exact result");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            ["finite", 65, 0],
            ["finite", 2049, 1],
            ["special", 2049, 2],
            ["overflow", 65, 0],
            ["finite", 0, 0],
          ],
        },
      );
      reach.assertReached();
    } finally {
      vi.restoreAllMocks();
      await core.close();
      await pg.close();
    }
  },
  180000,
);
