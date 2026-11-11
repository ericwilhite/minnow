/** Governor rules 2/4/5: a complete execution must progress without overlapping step loans. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SORT_PROGRESS_PRESSURE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { SortOperator } from "../exec/sort.js";
import { Governor } from "../memory/governor.js";
import { StepLoans } from "../memory/step-loans.js";
import { ChunkReader } from "../page/reader.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import type { Batch } from "../vector/vector.js";

const MiB = 1024 * 1024;
const names = ["v0", "v1", "v2", "v3", "v4"];
const columns = names.join(",");
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const reach = createReach("sort progress pressure", SORT_PROGRESS_PRESSURE_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

async function consume(execution: Execution, prepared: PreparedPlan) {
  const batches: Batch[] = [];
  for (;;) {
    const frame = await execution.results.next();
    if (frame === null) break;
    try {
      const payload = structuredClone(frame.payload);
      batches.push({
        length: payload.rowCount,
        selection: null,
        columns: payload.columns.map((v, i) => {
          if (v.form !== "flat") throw new Error("expected flat TEXT frame");
          const type = prepared.columns[i]?.type;
          if (!type) throw new Error("missing output type");
          return { ...v, type };
        }),
      });
    } finally {
      frame.release();
    }
  }
  await execution.executed;
  await execution.delivered;
  return oracleRows({ columns: prepared.columns, batches });
}
for (const seed of seedsFor("next-sort-progress-pressure", [0xb731, 0xb742]))
  for (const cancel of [false, true])
    it(`wide stored sorts release progress before another loan (${String(seed)}, cancel=${String(cancel)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            rows: fc.integer({ min: 1, max: 3 }),
            chars: fc.integer({ min: 350000, max: 370000 }),
            desc: fc.boolean(),
            nullTail: fc.boolean(),
            withTies: fc.boolean(),
          }),
          async ({ rows, chars, desc, nullTail, withTies }) => {
            reach.beginHistory();
            const core = await new StorageCore().open();
            const governor = new Governor(128 * MiB);
            const resident = await governor.admit({
              label: "competing resident",
              minimumBytes: 0,
              spillable: true,
            });
            const executions: Execution[] = [],
              releases: Array<() => void> = [];
            const active = new Map<StepLoans, number>();
            // eslint-disable-next-line @typescript-eslint/unbound-method -- restored below; invoked with the original receiver through call.
            const originalBorrow = StepLoans.prototype.borrow;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- restored below; invoked with the original receiver through call.
            const originalClose = StepLoans.prototype.close;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- restored below; invoked with the original receiver through call.
            const originalPush = SortOperator.prototype.push;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- restored below; invoked with the original receiver through call.
            const originalReady = SortOperator.prototype.ready;
            const entered = deferred(),
              continueFirst = deferred();
            const first = new Set<SortOperator>();
            let pushes = 0,
              loans = 0;
            StepLoans.prototype.borrow = async function (bytes, signal) {
              if (this.label.includes("/sort#")) {
                expect(
                  active.get(this) ?? 0,
                  "sort must repay its loan before asking for another",
                ).toBe(0);
                expect(
                  bytes,
                  "the complete admitted progress lane must cover the step",
                ).toBeLessThanOrEqual(this.capacity);
                loans++;
              }
              const loan = await originalBorrow.call(this, bytes, signal);
              active.set(this, bytes);
              return {
                ...loan,
                repay: () => {
                  loan.repay();
                  active.delete(this);
                },
              };
            };
            StepLoans.prototype.close = function () {
              originalClose.call(this);
              active.delete(this);
            };
            SortOperator.prototype.push = function (batch, transfer) {
              pushes++;
              if (first.size === 0) {
                first.add(this);
                const available = Math.max(
                  0,
                  governor.capBytes - governor.reserveBytes - governor.usedBytes,
                );
                expect(resident.allocate(available)).toBe(true);
                reach.reach("normal room exhausted");
              }
              if (first.has(this)) entered.resolve();
              const pushed = originalPush.call(this, batch, transfer);
              return first.has(this) ? "blocked" : pushed;
            };
            SortOperator.prototype.ready = function () {
              const ready = originalReady.call(this);
              return first.has(this)
                ? Promise.all([ready, continueFirst.promise]).then(() => undefined)
                : ready;
            };
            try {
              await core.execute(
                `CREATE TABLE t (${names.map((name) => `${name} TEXT`).join(",")})`,
              );
              await pg.exec(
                `DROP TABLE IF EXISTS t; CREATE TABLE t (${names.map((name) => `${name} TEXT`).join(",")})`,
              );
              const data = Array.from({ length: rows }, (_, i) =>
                Object.fromEntries(
                  names.map((name, c) => [
                    name,
                    nullTail && c === 4 && i === 1
                      ? null
                      : `${withTies && c === 0 ? "peer" : String.fromCharCode(97 + rows - i)}${"汉".repeat(chars + c)}${String(c)}`,
                  ]),
                ),
              );
              await core.writeBatch("insertBatch", "t", data);
              for (const row of data)
                await pg.query(
                  `INSERT INTO t VALUES ($1,$2,$3,$4,$5)`,
                  names.map((name) => row[name]),
                );
              const ordered = `SELECT ${columns} FROM t ORDER BY v0 ${desc ? "DESC" : "ASC"} NULLS LAST`;
              const sql = withTies
                ? `SELECT * FROM (${ordered} FETCH FIRST 2 ROWS WITH TIES) q ORDER BY v0 ${desc ? "DESC" : "ASC"} NULLS LAST,v1`
                : ordered;
              if (withTies) reach.reach("with ties boundary row");
              if (withTies && rows > 2) reach.reach("peer rows across runs");
              const expected = (await pg.query(sql)).rows;
              const prepared = core.prepare(sql);
              // All executions use the same immutable physical input. Background publication
              // may give a later snapshot a different, legitimately larger read minimum.
              const snapshot = core.harness.path.pin();
              releases.push(() => snapshot.release());
              const start = (id: number) => {
                const execution = prepared.execute({
                  snapshot,
                  statement: null,
                  parameters: [],
                  statementTime: 0n,
                  governor,
                  spill: core.spill,
                  queryId: id,
                  reads: {
                    open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                  },
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted",
                });
                executions.push(execution);
                return execution;
              };
              const a = start(101);
              const completedA = consume(a, prepared);
              void completedA.catch(() => undefined);
              await Promise.race([
                entered.promise,
                completedA.then(() => {
                  throw new Error("sort was not entered");
                }),
              ]);
              const b = start(102);
              const completedB = consume(b, prepared);
              void completedB.catch(() => undefined);
              await Promise.resolve();
              expect(b.state).toBe("running");
              expect(pushes).toBe(1);
              // Metadata sizing has its own complete admission; it may own scratch. The
              // queued sort itself must not have started or own a progress loan.
              expect(
                [...active.keys()].some((owner) => owner.label.includes("query#102/sort#")),
              ).toBe(false);
              reach.reach("five wide columns");
              reach.reach("two sorts fit alone");
              reach.reach("queued sort has no loan");
              if (cancel) {
                b.cancel(new Error("cancel queued sort"));
                await expect(completedB).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED" });
                await expect(b.executed).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED" });
                await expect(b.delivered).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED" });
                reach.reach("cancel waiting sort");
              }
              continueFirst.resolve();
              const resultA = await completedA;
              // A must finish under pressure. Only then may the busy competing execution end;
              // a fresh execution's metadata sizing is allowed to wait for ordinary capacity.
              resident.close();
              expect(
                JSON.stringify(resultA) === JSON.stringify(expected),
                "PGlite-exact first sorted output",
              ).toBe(true);
              if (!cancel) {
                const resultB = await completedB;
                expect(
                  JSON.stringify(resultB) === JSON.stringify(expected),
                  "PGlite-exact second sorted output",
                ).toBe(true);
                reach.reach("both complete");
              } else {
                const c = start(103);
                const resultC = await consume(c, prepared);
                expect(
                  JSON.stringify(resultC) === JSON.stringify(expected),
                  "PGlite-exact sort after queued cancellation",
                ).toBe(true);
                reach.reach("later admitted sort");
              }
              expect(loans).toBeGreaterThan(0);
              reach.reach("single owner loan");
              reach.reach("exact PG output");
            } finally {
              continueFirst.resolve();
              for (const execution of executions) execution.cancel(new Error("test cleanup"));
              await Promise.allSettled(
                executions.flatMap((execution) => [execution.executed, execution.delivered]),
              );
              for (const release of releases) release();
              resident.close();
              await core.close();
              StepLoans.prototype.borrow = originalBorrow;
              StepLoans.prototype.close = originalClose;
              SortOperator.prototype.push = originalPush;
              SortOperator.prototype.ready = originalReady;
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              expect(active.size).toBe(0);
              reach.reach("zero resources");
            }
          },
        ),
        {
          seed,
          numRuns: 3,
          examples: [
            [{ rows: 1, chars: 360000, desc: false, nullTail: false, withTies: false }],
            [{ rows: 3, chars: 360000, desc: false, nullTail: true, withTies: true }],
          ],
        },
      );
    }, 120000);
