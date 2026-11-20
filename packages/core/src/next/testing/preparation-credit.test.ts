/**
 * Concurrent queries prepare under FIFO contention without hold-and-wait (exec/preparation.ts).
 *
 * Laws (memory governor contract, memory/governor.ts): an admission never waits while its query
 * holds preparation memory; no refusal below the cap; a restarted query reads its own pinned
 * snapshot. Each history runs 2–4 queries (single-table scans, sorts, aggregates and a join) on
 * tables with a flushed group plus resident rows, so each query retains an admitted source recipe.
 * It queues one or two rounds of a competitor that only the queries' preparation holdings block,
 * may publish new rows between a query's attempts, and may cancel one query while it waits for its
 * credit. Then two probe phases make every history reach the restart transitions: two concurrent
 * ordered scans forced to restart, with rows published between their attempts while a join runs,
 * all of which must return the shadow rows as of their start; and one query forced through two restarts
 * (its first credit used up, so it waits for a grown one) and cancelled inside that wait. Every query that is not cancelled must finish with the shadow model's
 * rows as of its start; each credit wait must hold nothing; restarts stay within the geometric
 * bound; and the governor returns to zero.
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
const value = (t: number, id: number) => (id * (TABLES[t]?.mul ?? 1)) % (TABLES[t]?.mod ?? 1);
/** The shadow model: every committed row, in id order (rows are only appended). */
const rows = TABLES.map((table, t) =>
  Array.from({ length: table.flushed + table.resident }, (_, i): Row => ({
    id: i + 1,
    v: value(t, i + 1),
  })),
);

interface Query {
  table: number;
  shape: number;
  a: number;
  b: number;
}
type Expected = Array<Array<number | null>>;
const total = (kept: readonly Row[]): Expected => [
  [kept.length, kept.length === 0 ? null : kept.reduce((sum, row) => sum + row.v, 0)],
];
/** The SQL and the shadow model's rows as of now (each row as numbers or null, in order). */
function plan(query: Query): { sql: string; expected: Expected; join: boolean } {
  const name = TABLES[query.table]?.name ?? "t0";
  const data = rows[query.table] ?? [];
  if (query.shape === 0) {
    const limit = 1 + (query.a % 20),
      offset = query.b % (data.length + 10);
    return {
      sql: `SELECT id, v FROM ${name} ORDER BY id LIMIT ${String(limit)} OFFSET ${String(offset)}`,
      expected: data.slice(offset, offset + limit).map((row) => [row.id, row.v]),
      join: false,
    };
  }
  if (query.shape === 1) {
    const k = query.a % 101;
    return {
      sql: `SELECT count(*) AS c, sum(v) AS s FROM ${name} WHERE v >= ${String(k)}`,
      expected: total(data.filter((row) => row.v >= k)),
      join: false,
    };
  }
  if (query.shape === 2) {
    const low = 1 + (query.a % data.length),
      high = low + (query.b % 300);
    return {
      sql: `SELECT id, v FROM ${name} WHERE id BETWEEN ${String(low)} AND ${String(high)} ORDER BY id`,
      expected: data.filter((row) => row.id >= low && row.id <= high).map((row) => [row.id, row.v]),
      join: false,
    };
  }
  // A join of both tables on id (each side retains its own source recipe).
  const k = query.a % 97;
  const right = new Map((rows[1] ?? []).map((row) => [row.id, row.v]));
  return {
    sql: `SELECT count(*) AS c, sum(t0.v) AS s FROM t0 JOIN t1 ON t1.id = t0.id WHERE t1.v >= ${String(k)}`,
    expected: total((rows[0] ?? []).filter((row) => (right.get(row.id) ?? -1) >= k)),
    join: true,
  };
}

let core: StorageCore;
/** Appends `count` rows to table `t`: committed in the owner and in the shadow model. */
async function append(t: number, count: number): Promise<void> {
  const data = rows[t] ?? [];
  const added = Array.from({ length: count }, (_, i): Row => {
    const id = data.length + i + 1;
    return { id, v: value(t, id) };
  });
  await core.writeBatch(
    "insertBatch",
    TABLES[t]?.name ?? "t0",
    added.map((row) => ({ ...row })),
  );
  data.push(...added);
}
/** Publishes the resident rows into a group, then leaves new resident rows (recipes stay). */
async function publish(t: number): Promise<void> {
  await append(t, 7);
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
  for (const other of TABLES.keys()) await append(other, 3);
}
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
  shape: fc.integer({ min: 0, max: 3 }),
  a: fc.nat(1 << 16),
  b: fc.nat(1 << 16),
});
const normalise = (result: Awaited<ReturnType<StorageCore["execute"]>>, sql: string) => {
  if (result.kind !== "rows" || !("batches" in result)) throw new Error(`rows: ${sql}`);
  return oracleRows(result).map((row) =>
    Object.values(row).map((value) => (value === null ? null : Number(value))),
  );
};

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
        fc.integer({ min: -1, max: TABLES.length - 1 }),
        async (queries, trigger, quarters, holdMs, cancel, rounds, publishTable) => {
          reach.beginHistory();
          const governor = new Governor(64 << 20);
          const capacity = governor.capBytes - governor.reserveBytes;
          const facades = new Set<PreparationGovernor>();
          const controllers = queries.map(() => new AbortController());
          const probe = new AbortController();
          const indexOf = new Map<number, number>();
          const nativePrepare = core.executor.prepare.bind(core.executor);
          const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((physical) => {
            const prepared = nativePrepare(physical);
            return {
              ...prepared,
              execute: (request) => {
                indexOf.set(
                  request.queryId,
                  controllers.findIndex((c) => c.signal === request.signal),
                );
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
          const tryAdmit = governor.tryAdmit.bind(governor);
          // The probe phases: only source recipes are admitted at once, so each probe query holds
          // its recipe when its next admission would wait. "restart": two concurrent scans
          // restart, with a publish between attempts, and (with a concurrent join) must still
          // read their own snapshots.
          // "cancel": the probe's first credit is used up, so its second attempt waits again
          // for a grown credit; that wait is held up and cancelled.
          const probing: {
            on: boolean;
            mode: "restart" | "cancel";
            holder: Grant | null;
            restarts: number;
            restarted: Set<PreparationGovernor>;
          } = { on: false, mode: "restart", holder: null, restarts: 0, restarted: new Set() };
          const restarted = new Set<PreparationGovernor>();
          const firstWanted = new Map<PreparationGovernor, number>();
          const published: { pending: boolean } = { pending: publishTable >= 0 };
          let cancelled = -1;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- wrapped with its receiver
          const regrow = PreparationGovernor.prototype.regrow;
          const regrows = vi
            .spyOn(PreparationGovernor.prototype, "regrow")
            .mockImplementation(async function (
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
              if (!firstWanted.has(this)) firstWanted.set(this, wait.needed);
              if (probing.on && probing.mode === "restart") {
                probing.restarted.add(this);
                if (probing.restarted.size >= 2) reach.reach("concurrent restarts");
                if (probing.restarts++ === 0) {
                  // New rows are published between this query's attempts; its next attempt
                  // (and the other query's) must still read its own pinned snapshot.
                  await publish(Math.max(0, publishTable));
                  reach.reach("publish between attempts");
                }
                return regrow.call(this, wait, signal);
              }
              if (probing.on && probing.restarts++ === 0) {
                await regrow.call(this, wait, signal);
                const credit = this.credit;
                if (credit === null) throw new Error("the probe's first credit was not admitted");
                expect(credit.allocate(credit.held - credit.allocated - credit.loaned)).toBe(true);
                return;
              }
              if (probing.on) {
                expect(this.attempts, "the probe restarts a second time").toBe(2);
                // Hold up the grown credit's wait with every free byte, then cancel inside it.
                probing.holder = tryAdmit({
                  label: "probe holder",
                  minimumBytes: capacity - governor.usedBytes,
                  spillable: false,
                });
                const pending = regrow.call(this, wait, signal);
                probe.abort(new Error("cancelled during credit wait"));
                reach.reach("cancel during credit wait");
                return pending;
              }
              if (published.pending) {
                // New rows are published between this query's attempts; its next attempt must
                // still read its own pinned snapshot.
                published.pending = false;
                await publish(publishTable);
              }
              const id = Number(/^query#(\d+)$/.exec(this.label)?.[1]);
              if (cancelled < 0 && cancel >= 0 && indexOf.get(id) === cancel % queries.length) {
                cancelled = cancel % queries.length;
                controllers[cancelled]?.abort(new Error("cancelled during credit wait"));
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
          const canAdmitNow = governor.canAdmitNow.bind(governor);
          const nows = vi
            .spyOn(governor, "canAdmitNow")
            .mockImplementation((bytes, progress) => !probing.on && canAdmitNow(bytes, progress));
          // A competitor queued once the queries hold preparation memory, needing some of it.
          // Each round queues one competitor; the next round re-arms after it is released.
          const queued: { competitor: Promise<Grant> | null; round: number } = {
            competitor: null,
            round: 0,
          };
          let calls = 0;
          const governorTries = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
            if (probing.on)
              return options.label.endsWith("/source-recipe") ? tryAdmit(options) : null;
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
          const deadline = <T>(work: Promise<T>, what: string) =>
            Promise.race([
              work,
              new Promise<never>((_, reject) =>
                timers.push(setTimeout(() => reject(new Error(what)), 10_000)),
              ),
            ]);
          try {
            const runs = queries.map((q, i) => {
              // The shadow rows as of the query's start: execute pins its snapshot at once.
              const { sql, expected, join } = plan(q);
              if (join) reach.reach("join");
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
            const outcome = await deadline(
              Promise.all(runs).finally(() => {
                done.settled = true;
              }),
              "a query waited while holding preparation memory (deadlock)",
            ).catch((error: unknown) => {
              for (const controller of controllers) controller.abort(new Error("deadlocked"));
              throw error;
            });
            await competed;
            for (const { i, sql, expected, result, error } of outcome) {
              if (i === cancelled && error !== null) continue;
              expect(error, sql).toBeNull();
              if (result === null) throw new Error(`rows: ${sql}`);
              expect(normalise(result, sql), sql).toEqual(expected);
            }
            // Probe "restart": two concurrent queries restart, rows published between attempts.
            probing.on = true;
            // Two ordered scans retain source recipes, so both restart; the join holds nothing
            // when it waits (it waits as usual), and runs across the same publication.
            const restartProbes = [
              plan({ table: 0, shape: 0, a: 4, b: 5 }),
              plan({ table: 1, shape: 0, a: 7, b: 2200 }),
              plan({ table: 0, shape: 3, a: 11, b: 0 }),
            ];
            const probeRuns = restartProbes.map(({ sql, join }) => {
              if (join) reach.reach("join");
              return core.execute(sql);
            });
            const probeResults = await deadline(
              Promise.all(probeRuns),
              "a restart probe waited while holding preparation memory (deadlock)",
            );
            for (const [k, result] of probeResults.entries()) {
              const { sql, expected } = restartProbes[k] ?? { sql: "", expected: [] };
              expect(normalise(result, sql), `${sql} (as of its start)`).toEqual(expected);
            }
            expect(probing.restarted.size, "both scan probes restarted").toBeGreaterThanOrEqual(2);
            // Probe "cancel": forced into a grown credit's wait, cancelled inside it.
            probing.mode = "cancel";
            probing.restarts = 0;
            const probed = core.execute(
              "SELECT id, v FROM t0 ORDER BY id LIMIT 3 OFFSET 10",
              [],
              probe.signal,
            );
            await expect(
              deadline(probed, "the probe never reached a credit wait"),
            ).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED", sqlState: "57014" });
            expect(probe.signal.aborted, "the probe was cancelled inside its credit wait").toBe(
              true,
            );
            probing.on = false;
            probing.holder?.close();
            probing.holder = null;
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
            probing.on = false;
            probing.holder?.close();
            for (const timer of timers) clearTimeout(timer);
            governorTries.mockRestore();
            nows.mockRestore();
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
