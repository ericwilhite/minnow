/** Competing source/transition errors: pipeline.ts demands the next source item
 * only after its preceding batch was consumed. PostgreSQL owns SUM's22003.
 */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, B9_STAR_SOURCE_DEMAND_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { createExecutor } from "../exec/create.js";
import type { Execution } from "../exec/executor.js";
import { StarLookups } from "../exec/join-star-lookups.js";
import { TableScan } from "../exec/table-scan.js";
import type { SourceItem } from "../exec/pipeline.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const SQL =
  "SELECT a.tier AS tier_a,b.tier AS tier_b,c.label AS label_c,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b JOIN sc c ON c.k=f.c WHERE a.active AND b.weight>1 AND c.tier='t1' GROUP BY a.tier,b.tier,c.label ORDER BY s DESC,tier_a,tier_b,label_c LIMIT 50";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});

for (const seed of seedsFor("b9-star-source-demand", [0xb9f91, 0xb9f92]))
  it(`later B source failure cannot preempt demanded SUM seed ${String(seed)}`, async () => {
    const reach = createReach("B9 star source demand", B9_STAR_SOURCE_DEMAND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2049, max: 2052 }),
        fc.boolean(),
        async (bRows, overflow) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open(),
            n = 4097;
          try {
            const ddls = [
              "CREATE TABLE sf(id INTEGER PRIMARY KEY,a TEXT,b TEXT,c TEXT,v DOUBLE PRECISION)",
              ...["sa", "sb", "sc"].map(
                (name) =>
                  `CREATE TABLE ${name}(k TEXT PRIMARY KEY,tier TEXT,label TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
              ),
            ];
            await pg.exec("DROP TABLE IF EXISTS sf,sa,sb,sc");
            for (const ddl of ddls) {
              await core.execute(ddl);
              await pg.exec(ddl);
            }
            const a = { k: "a0", tier: "a", label: "a", weight: 2, active: true },
              c = { k: "c0", tier: "t1", label: "c", weight: 2, active: true },
              b = Array.from({ length: bRows }, (_, i) => ({
                k: `b${String(i).padStart(5, "0")}`,
                tier: "b",
                label: "b",
                weight: 2,
                active: true,
              })),
              f = Array.from({ length: n }, (_, id) => ({
                id,
                a: "a0",
                b: "b00000",
                c: "c0",
                v: overflow ? 1e308 : 0.25,
              }));
            for (const [name, rows] of [
              ["sa", [a]],
              ["sb", b],
              ["sc", [c]],
              ["sf", f],
            ] as const)
              await core.writeBatch("insertBatch", name, [...rows]);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            await pg.exec(
              `INSERT INTO sa VALUES('a0','a','a',2,true);INSERT INTO sc VALUES('c0','t1','c',2,true);INSERT INTO sb SELECT 'b'||lpad(i::text,5,'0'),'b','b',2,true FROM generate_series(0,${String(bRows - 1)})g(i);INSERT INTO sf SELECT i,'a0','b00000','c0',${overflow ? "1e308" : "0.25"}::float8 FROM generate_series(0,${String(n - 1)})g(i)`,
            );
            let expected: ReturnType<typeof oracleRows> = [];
            if (overflow) {
              await expect(pg.query(SQL)).rejects.toMatchObject({ code: "22003" });
              reach.reach("PGlite22003");
            } else {
              const reference = await pg.query<{
                tier_a: string;
                tier_b: string;
                label_c: string;
                n: string;
                s: number;
              }>(SQL);
              expected = reference.rows.map((row) => ({ ...row, n: Number(row.n) }));
            }
            async function observe(
              enabled: boolean,
              fault: boolean,
              earlier: "facts" | "A" | null = null,
            ) {
              const gov = new Governor(256 << 20),
                executor = createExecutor({ starLookups: enabled }),
                bScans = new WeakSet<TableScan>(),
                sources = new WeakSet<TableScan>(),
                phases: string[] = [];
              let firstRows = 0,
                faults = 0,
                dimensions: number;
              let execution: Execution | undefined;
              const later = new Error("later B source item"),
                sourceError = new Error(`earlier ${earlier ?? "none"} source item`);
              // eslint-disable-next-line @typescript-eslint/unbound-method -- original is explicitly called with the scanned source
              const next = TableScan.prototype.next;
              const scan = vi.spyOn(TableScan.prototype, "next").mockImplementation(function (
                this: TableScan,
              ) {
                if (fault && bScans.has(this)) {
                  faults++;
                  throw later;
                }
                const item = next.call(this);
                const seen = (out: SourceItem | null) => {
                  const cols = out?.batch.columns;
                  if (cols && !sources.has(this)) {
                    sources.add(this);
                    const kinds = cols
                      .map((c) => c.type.kind)
                      .sort()
                      .join(",");
                    phases.push(
                      kinds === "text,text,text"
                        ? "C"
                        : kinds === "float64,text,text,text"
                          ? "facts"
                          : kinds === "bool,text,text"
                            ? "A"
                            : kinds === "float64,text,text"
                              ? "B"
                              : kinds,
                    );
                  }
                  if (cols && earlier !== null) {
                    const kinds = cols
                      .map((c) => c.type.kind)
                      .sort()
                      .join(",");
                    if (
                      (earlier === "facts" && kinds === "float64,text,text,text") ||
                      (earlier === "A" && kinds === "bool,text,text")
                    )
                      throw sourceError;
                  }
                  if (
                    cols?.length === 3 &&
                    cols.filter((c) => c.type.kind === "text").length === 2 &&
                    cols.filter((c) => c.type.kind === "float64").length === 1
                  ) {
                    bScans.add(this);
                    if (firstRows === 0) firstRows = out?.batch.length ?? 0;
                  }
                  return out;
                };
                return item instanceof Promise ? item.then(seen) : seen(item);
              });
              const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                const p = executor.prepare(plan);
                return {
                  ...p,
                  execute: (request) => {
                    execution = p.execute({ ...request, governor: gov });
                    return execution;
                  },
                };
              });
              const dimension = vi.spyOn(StarLookups.prototype, "dimension");
              let error: unknown;
              let rows: ReturnType<typeof oracleRows> = [];
              try {
                const result = await core.execute(SQL);
                if (!("batches" in result)) throw new Error("Missing star result");
                rows = oracleRows(result);
              } catch (e) {
                error = e;
              } finally {
                dimensions = dimension.mock.calls.length;
                scan.mockRestore();
                prepare.mockRestore();
                dimension.mockRestore();
              }
              await execution?.settled?.();
              expect(gov.usedBytes).toBe(0);
              return {
                error,
                rows,
                phases,
                firstRows,
                faults,
                dimensions,
                later,
                sourceError,
              };
            }
            const native = await observe(false, false),
              selected = await observe(true, false);
            for (const result of [native, selected]) {
              // Exact dictionary proof reads precede both native and fused execution.
              // Compare their whole trace below; this suffix is the consumed source contract.
              expect(result.phases.slice(-4)).toEqual(["C", "facts", "A", "B"]);
              if (overflow) expect(result.error).toMatchObject({ sqlState: "22003" });
              else {
                expect(result.error).toBeUndefined();
                expect(result.rows).toEqual(expected);
              }
            }
            expect(selected.phases).toEqual(native.phases);
            reach.reach("native source phase order");
            expect(selected.dimensions, "positive lookup witness").toBeGreaterThanOrEqual(2);
            expect(native.firstRows).toBeGreaterThan(0);
            expect(native.firstRows).toBeLessThan(bRows);
            reach.reach("multiple B batches");
            if (overflow) reach.reach("native22003");
            reach.reach("selected lookup no fault");
            for (const source of ["facts", "A"] as const) {
              const nativeFailure = await observe(false, true, source),
                lookupFailure = await observe(true, true, source);
              expect(nativeFailure.error).toBe(nativeFailure.sourceError);
              expect(lookupFailure.error).toBe(lookupFailure.sourceError);
              expect(lookupFailure.phases).toEqual(nativeFailure.phases);
              expect(nativeFailure.faults).toBe(0);
              expect(lookupFailure.faults).toBe(0);
              reach.reach(
                source === "facts" ? "facts fail before A and B demand" : "A fails before B demand",
              );
            }
            const control = await observe(false, true),
              candidate = await observe(true, true);
            expect(candidate.dimensions).toBeGreaterThan(0);
            expect(candidate.phases).toEqual(control.phases);
            if (overflow) {
              expect(control.error).toMatchObject({ sqlState: "22003" });
              expect(control.faults).toBe(0);
              reach.reach("native stops before late B failure");
              expect(
                candidate.error,
                "later B source failure preempted demanded SUM",
              ).toMatchObject({ sqlState: "22003" });
              expect(candidate.faults, "later B source item was not demanded before SUM").toBe(0);
              reach.reach("lookup retains22003");
            } else {
              expect(control.error).toBe(control.later);
              expect(candidate.error).toBe(candidate.later);
              expect(control.faults).toBe(1);
              expect(candidate.faults).toBe(1);
              reach.reach("finite prefix forwards B failure");
            }
          } finally {
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero charges");
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [2049, true],
          [2052, true],
          [2049, false],
          [2052, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
