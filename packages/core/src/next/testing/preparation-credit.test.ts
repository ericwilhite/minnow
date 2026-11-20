/**
 * Concurrent queries prepare under FIFO contention without hold-and-wait (exec/preparation.ts).
 *
 * Laws (memory governor contract, memory/governor.ts): an admission never waits while its query
 * holds preparation memory; no refusal below the cap. Each history runs 2–4 queries on tables with
 * a flushed group plus resident rows (so each query retains an admitted source recipe), queues
 * one or two rounds of a competitor that only the queries' preparation holdings block, and may
 * cancel one query while it waits for its credit. Every query that is not cancelled must finish with the shadow model's rows;
 * each credit wait must hold nothing; restarts stay within the geometric bound; and the governor
 * returns to zero.
 */
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, PREPARATION_CREDIT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { PreparationGovernor, type PreparationWouldWait } from "../exec/preparation.js";
import { Governor, type Grant } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const TABLES = [
  { name: "t0", flushed: 3000, resident: 100, mul: 7, mod: 101 },
  { name: "t1", flushed: 2000, resident: 250, mul: 13, mod: 97 },
] as const;
interface Row {
  id: number;
  v: number;
}
const rows = TABLES.map((table) =>
  Array.from({ length: table.flushed + table.resident }, (_, i): Row => ({
    id: i + 1,
    v: ((i + 1) * table.mul) % table.mod,
  })),
);

interface Query {
  table: number;
  shape: number;
  a: number;
  b: number;
}
/** The SQL and the shadow model's expected rows (each row as numbers or null, in order). */
function plan(query: Query): { sql: string; expected: Array<Array<number | null>> } {
  const name = TABLES[query.table]?.name ?? "t0";
  const data = rows[query.table] ?? [];
  if (query.shape === 0) {
    const limit = 1 + (query.a % 20),
      offset = query.b % (data.length + 10);
    return {
      sql: `SELECT id, v FROM ${name} ORDER BY id LIMIT ${String(limit)} OFFSET ${String(offset)}`,
      expected: data.slice(offset, offset + limit).map((row) => [row.id, row.v]),
    };
  }
  if (query.shape === 1) {
    const k = query.a % 101;
    const kept = data.filter((row) => row.v >= k);
    return {
      sql: `SELECT count(*) AS c, sum(v) AS s FROM ${name} WHERE v >= ${String(k)}`,
      expected: [[kept.length, kept.length === 0 ? null : kept.reduce((sum, r) => sum + r.v, 0)]],
    };
  }
  const low = 1 + (query.a % data.length),
    high = low + (query.b % 300);
  return {
    sql: `SELECT id, v FROM ${name} WHERE id BETWEEN ${String(low)} AND ${String(high)} ORDER BY id`,
    expected: data.filter((row) => row.id >= low && row.id <= high).map((row) => [row.id, row.v]),
  };
}

let core: StorageCore;
beforeAll(async () => {
  core = await new StorageCore(8192).open();
  for (const table of TABLES)
    await core.execute(`CREATE TABLE ${table.name} (id INTEGER PRIMARY KEY, v INTEGER)`);
  // One flushed group per table, then resident rows: each query retains a source recipe.
  for (const [t, table] of TABLES.entries())
    await core.writeBatch(
      "insertBatch",
      table.name,
      (rows[t] ?? []).slice(0, table.flushed).map((row) => ({ ...row })),
    );
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
  for (const [t, table] of TABLES.entries())
    await core.writeBatch(
      "insertBatch",
      table.name,
      (rows[t] ?? []).slice(table.flushed).map((row) => ({ ...row })),
    );
}, 120_000);
afterAll(async () => {
  await core.close();
});

const query = fc.record({
  table: fc.integer({ min: 0, max: TABLES.length - 1 }),
  shape: fc.integer({ min: 0, max: 2 }),
  a: fc.nat(1 << 16),
  b: fc.nat(1 << 16),
});

for (const seed of seedsFor("next-preparation-credit", [0x9c4e, 0x9c4f]))
  it(`prepares concurrent queries without hold-and-wait seed ${String(seed)}`, async () => {
    const reach = createReach("preparation credit", PREPARATION_CREDIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(query, { minLength: 2, maxLength: 4 }),
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: -1, max: 3 }),
        fc.integer({ min: 1, max: 2 }),
        async (queries, trigger, quarters, holdMs, cancel, rounds) => {
          reach.beginHistory();
          const governor = new Governor(64 << 20);
          const capacity = governor.capBytes - governor.reserveBytes;
          const facades = new Set<PreparationGovernor>();
          const controllers = queries.map(() => new AbortController());
          const indexOf = new Map<number, number>();
          const nativePrepare = core.executor.prepare.bind(core.executor);
          let next = 0;
          const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((physical) => {
            const prepared = nativePrepare(physical);
            return {
              ...prepared,
              execute: (request) => {
                const index = controllers.findIndex((c) => c.signal === request.signal);
                indexOf.set(request.queryId, index < 0 ? next : index);
                next++;
                return prepared.execute({ ...request, governor });
              },
            };
          });
          // Facades are found where they admit; the credit label names the query.
          // eslint-disable-next-line @typescript-eslint/unbound-method -- wrapped with its receiver
          const facadeTry = PreparationGovernor.prototype.tryAdmit;
          const tries = vi
            .spyOn(PreparationGovernor.prototype, "tryAdmit")
            .mockImplementation(function (this: PreparationGovernor, options) {
              facades.add(this);
              return facadeTry.call(this, options);
            });
          const restarted = new Set<PreparationGovernor>();
          const firstWanted = new Map<PreparationGovernor, number>();
          let cancelled = -1;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- wrapped with its receiver
          const regrow = PreparationGovernor.prototype.regrow;
          const regrows = vi
            .spyOn(PreparationGovernor.prototype, "regrow")
            .mockImplementation(function (
              this: PreparationGovernor,
              wait: PreparationWouldWait,
              signal: AbortSignal,
            ) {
              // The executor released the whole attempt before it waits.
              expect(this.heldBytes, `${this.label}: the attempt's grants were released`).toBe(0);
              waiting.set(`${this.label}/preparation-credit`, this);
              reach.reach("restart");
              if (wait.held > 0) reach.reach("restart released a source recipe");
              if (this.attempts >= 2) reach.reach("credit grew");
              restarted.add(this);
              if (restarted.size >= 2) reach.reach("concurrent restarts");
              if (!firstWanted.has(this)) firstWanted.set(this, wait.held + wait.requested);
              const id = Number(/^query#(\d+)$/.exec(this.label)?.[1]);
              if (cancelled < 0 && cancel >= 0 && indexOf.get(id) === cancel % queries.length) {
                cancelled = cancel % queries.length;
                controllers[cancelled]?.abort(new Error("cancelled during credit wait"));
                reach.reach("cancel during credit wait");
              }
              return regrow.call(this, wait, signal);
            });
          // At the credit request itself, the query holds no preparation memory at all.
          const waiting = new Map<string, PreparationGovernor>();
          const admitGroup = governor.admitGroup.bind(governor);
          const groups = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
            const credit = options.grants.find((part) =>
              part.label.endsWith("/preparation-credit"),
            );
            const facade = credit === undefined ? undefined : waiting.get(credit.label);
            if (facade !== undefined) {
              expect(
                facade.credit,
                `${facade.label}: old credit released before the wait`,
              ).toBeNull();
              expect(facade.heldBytes, `${facade.label}: credit wait holds nothing`).toBe(0);
            }
            return admitGroup(options);
          });
          const carve = governor.carve.bind(governor);
          const carves = vi.spyOn(governor, "carve").mockImplementation((source, ...rest) => {
            const grant = carve(source, ...rest);
            if (grant !== null) reach.reach("served from credit");
            return grant;
          });
          // A competitor queued once the queries hold preparation memory, needing some of it.
          // Each round queues one competitor; the next round re-arms after it is released.
          const queued: { competitor: Promise<Grant> | null; round: number } = {
            competitor: null,
            round: 0,
          };
          let calls = 0;
          const tryAdmit = governor.tryAdmit.bind(governor);
          const governorTries = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
            if (queued.competitor === null && queued.round < rounds && ++calls >= trigger) {
              let preparing = 0;
              for (const facade of facades) if (facade.preparing) preparing += facade.heldBytes;
              if (preparing > 0) {
                queued.round++;
                queued.competitor = governor.admit({
                  label: "competitor",
                  minimumBytes:
                    capacity -
                    governor.usedBytes +
                    Math.max(1, Math.floor((preparing * quarters) / 4)),
                  spillable: false,
                });
              }
            }
            return tryAdmit(options);
          });
          const timers: Array<ReturnType<typeof setTimeout>> = [];
          try {
            const runs = queries.map((q, i) => {
              const { sql, expected } = plan(q);
              return core
                .execute(sql, [], controllers[i]?.signal)
                .then((result) => ({ i, sql, expected, result, error: null as unknown }))
                .catch((error: unknown) => ({ i, sql, expected, result: null, error }));
            });
            const done = { settled: false };
            const competed = (async () => {
              for (let round = 0; round < rounds; round++) {
                for (
                  let waited = 0;
                  queued.competitor === null && !done.settled && waited < 200;
                  waited++
                )
                  await new Promise((resolve) => setTimeout(resolve, 1));
                const competitor = queued.competitor;
                if (competitor === null) return;
                const grant = await competitor;
                await new Promise((resolve) => timers.push(setTimeout(resolve, holdMs)));
                grant.close();
                calls = 0;
                queued.competitor = null;
              }
            })();
            const outcome = await Promise.race([
              Promise.all(runs).finally(() => {
                done.settled = true;
              }),
              new Promise<"deadlock">((resolve) =>
                timers.push(setTimeout(() => resolve("deadlock"), 10_000)),
              ),
            ]);
            if (outcome === "deadlock") {
              for (const controller of controllers) controller.abort(new Error("deadlocked"));
              throw new Error("a query waited while holding preparation memory (deadlock)");
            }
            await competed;
            for (const { i, sql, expected, result, error } of outcome) {
              if (i === cancelled && error !== null) continue;
              expect(error, sql).toBeNull();
              if (result?.kind !== "rows" || !("batches" in result))
                throw new Error(`rows: ${sql}`);
              const actual = oracleRows(result).map((row) =>
                Object.values(row).map((value) => (value === null ? null : Number(value))),
              );
              expect(actual, sql).toEqual(expected);
            }
            for (const facade of facades) {
              expect(facade.credit, `${facade.label}: no credit outlives its query`).toBeNull();
              const first = firstWanted.get(facade);
              if (first !== undefined)
                expect(facade.attempts - 1, `${facade.label}: restarts`).toBeLessThanOrEqual(
                  Math.ceil(Math.log2(capacity / first)) + 1,
                );
            }
            expect(governor.usedBytes, "every grant returned").toBe(0);
          } finally {
            for (const timer of timers) clearTimeout(timer);
            governorTries.mockRestore();
            groups.mockRestore();
            carves.mockRestore();
            regrows.mockRestore();
            tries.mockRestore();
            prepare.mockRestore();
          }
        },
      ),
      // A deadlocked history is reported at once (each one costs the whole deadlock window);
      // MINNOW_SEED replays it.
      { seed, numRuns: 12, endOnFailure: true },
    );
    reach.assertReached();
  }, 300_000);
