/** Peak is physical merge rows plus one saved key, not one whole merge per alias/helper. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  AGG_STEP_PEAK_REACH,
  AGG_PACKED_BINDINGS_REACH,
  AGG_COLUMN_PROGRESS_REACH,
} from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { HybridAggregateOperator } from "../exec/agg/hybrid.js";
import { GroupCheckpoint } from "../exec/agg/checkpoint.js";
import { SortOperator } from "../exec/sort.js";
import { Pool } from "../exec/spill-pool.js";
import { oracleRows } from "./differential-adapter.js";
import type { NextStatementResult } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const restore = QueryRuntime.prototype.restorePhase;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const admit = Governor.prototype.admitGroup;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const trim = Pool.prototype.trimStep;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const hybridPush = HybridAggregateOperator.prototype.push;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const hybridReady = HybridAggregateOperator.prototype.ready;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const save = GroupCheckpoint.prototype.save;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const sortPush = SortOperator.prototype.push;
function resultRows(result: NextStatementResult) {
  if (!("batches" in result)) throw new Error("missing aggregate rows");
  return oracleRows(result);
}
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("next-agg-step-peak", [0xb795, 0xb796]))(
  "shares restore peak seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("aggregate physical step peak", AGG_STEP_PEAK_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 16 }),
          fc.integer({ min: 900000, max: 1100000 }),
          fc.integer({ min: 6, max: 12 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (keys, bytes, rows, unicode, cancel, distinct, prefix) => {
            reach.beginHistory();
            const governor = new Governor(256 << 20);
            let blocker = await governor.admit({
              label: "pinned competing holder",
              minimumBytes: 0,
              spillable: true,
            });
            const abort = new AbortController(),
              mocks: Array<{ mockRestore(): void }> = [];
            let occupied = false,
              restored = false,
              shared = false;
            const observed = { liveKey: false, savedPrefix: false };
            const timer = setTimeout(
              () => abort.abort(new Error("aggregate step progress deadline")),
              30000,
            );
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserve.apply(this, args);
                  if (this.request.governor === governor && !occupied && !(prefix && !distinct)) {
                    expect(
                      blocker.allocate(
                        governor.capBytes - governor.reserveBytes - governor.usedBytes,
                      ),
                    ).toBe(true);
                    occupied = true;
                  }
                }),
              );
              const pin = { next: false };
              mocks.push(
                vi.spyOn(HybridAggregateOperator.prototype, "push").mockImplementation(function (
                  this: HybridAggregateOperator,
                  ...args
                ) {
                  const result = hybridPush.apply(this, args);
                  if (prefix && !distinct && !occupied) {
                    pin.next = true;
                    return "blocked";
                  }
                  return result;
                }),
              );
              mocks.push(
                vi
                  .spyOn(HybridAggregateOperator.prototype, "ready")
                  .mockImplementation(async function (this: HybridAggregateOperator) {
                    await hybridReady.call(this);
                    if (pin.next) {
                      pin.next = false;
                      expect(
                        blocker.allocate(
                          governor.capBytes - governor.reserveBytes - governor.usedBytes,
                        ),
                      ).toBe(true);
                      await blocker.reclaimRoom?.(1, abort.signal);
                      expect(
                        blocker.allocate(
                          governor.capBytes - governor.reserveBytes - governor.usedBytes,
                        ),
                      ).toBe(true);
                      occupied = true;
                    }
                  }),
              );
              mocks.push(
                vi.spyOn(GroupCheckpoint.prototype, "save").mockImplementation(function (
                  this: GroupCheckpoint,
                  ...args
                ) {
                  expect(this.keyCount).toBe((distinct ? keys : 1) + 1);
                  if (args[1] > 0) observed.savedPrefix = true;
                  return save.apply(this, args);
                }),
              );
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "restorePhase").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  if (this.request.governor !== governor) return restore.apply(this, args);
                  restored = true;
                  blocker.close(); // finite competing holder: release for whole phase admission
                  await restore.apply(this, args);
                  blocker = await governor.admit({
                    label: "repinned competing holder",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  expect(
                    blocker.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                  reach.reach("repinned after admission");
                }),
              );
              mocks.push(
                vi.spyOn(Governor.prototype, "admitGroup").mockImplementation(function (
                  this: Governor,
                  options,
                ) {
                  if (
                    this === governor &&
                    options.grants.some((p) => p.label.endsWith("/step-peak"))
                  ) {
                    shared = true;
                    expect(
                      options.grants
                        .filter(
                          (p) => p.label.includes("/aggregate#") && p.label.endsWith("/progress"),
                        )
                        .every((p) => p.minimumBytes === 0),
                    ).toBe(true);
                  }
                  return admit.call(this, options);
                }),
              );
              mocks.push(
                vi.spyOn(SortOperator.prototype, "push").mockImplementation(function (
                  this: SortOperator,
                  ...args
                ) {
                  if (
                    restored &&
                    this.spec.types.at(-1)?.kind === "bytes" &&
                    this.spec.types.length > 4
                  ) {
                    expect(this.profile.mode).toBe("compare");
                    reach.reach("raw restore pages");
                  }
                  return sortPush.apply(this, args);
                }),
              );
              mocks.push(
                vi.spyOn(Pool.prototype, "trimStep").mockImplementation(function (this: Pool) {
                  trim.call(this);
                  if (restored && this.used > 1 << 20) {
                    observed.liveKey = true;
                    if (cancel) queueMicrotask(() => abort.abort());
                  }
                }),
              );
              const prepare = core.executor.prepare.bind(core.executor);
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const p = prepare(plan);
                  return {
                    ...p,
                    execute: (r) => p.execute({ ...r, governor, signal: abort.signal }),
                  };
                }),
              );
              const fields = Array.from(
                  { length: keys },
                  (_, i) => `$${String(distinct ? i + 1 : 1)}::text k${String(i)}`,
                ),
                group = Array.from({ length: keys }, (_, i) => `k${String(i)}`);
              const sql = `SELECT i,count(*) n,sum(i) s,avg(i)::float8 a,min(i) lo,max(i) hi FROM (SELECT ${fields.join(",")},i FROM generate_series(1,${String(rows)})g(i))q GROUP BY ${group.join(",")},i ORDER BY i`;
              const value = unicode ? "汉".repeat(Math.floor(bytes / 3)) : "x".repeat(bytes);
              const args = distinct
                ? Array.from({ length: keys }, (_, i) => value + String(i))
                : [value];
              if (cancel) {
                await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: "57014" });
                reach.reach("cancel live key");
              } else {
                expect(resultRows(await core.execute(sql, args))).toEqual(
                  (await pg.query(sql, args)).rows,
                );
                reach.reach("PG exact");
              }
              expect(occupied).toBe(true);
              if (prefix && !distinct) expect(observed.savedPrefix).toBe(true);
              if (observed.savedPrefix) reach.reach("saved prefix");
              expect(restored).toBe(true);
              expect(shared).toBe(true);
              if (observed.liveKey) reach.reach("trim live key");
              reach.reach(distinct ? "distinct keys" : "repeated keys");
              reach.reach("restore");
              reach.reach("shared peak");
              reach.reach(unicode ? "UTF8" : "ASCII");
              if (keys === 1) reach.reach("one logical wide key");
              if (keys === 16) reach.reach("sixteen logical keys");
            } finally {
              clearTimeout(timer);
              for (const mock of mocks.reverse()) mock.mockRestore();
              blocker.close();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero");
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [1, 1100000, 6, false, false, false, false],
            [16, 1100000, 8, true, false, false, true],
            [16, 1100000, 8, false, true, false, true],
            [8, 1100000, 6, false, false, true, false],
            [16, 900000, 6, true, false, true, false],
            [12, 1100000, 6, false, true, true, false],
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

it.each(seedsFor("next-agg-packed-bindings", [0xb797, 0xb798]))(
  "preserves logical bindings seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("aggregate packed bindings", AGG_PACKED_BINDINGS_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.array(fc.integer({ min: 0, max: 4 }), { minLength: 1, maxLength: 16 }),
          fc.boolean(),
          async (slots, nullFirst) => {
            reach.beginHistory();
            const expressions = [
              "$1::text",
              "$2::text",
              "i::numeric(5,1)",
              "coalesce($1::text,$3::text)",
              "CASE WHEN i<=2 THEN $1::text ELSE $2::text END",
            ];
            const fields = slots.map((slot, i) => {
                const expression = expressions[slot];
                if (expression === undefined) throw new Error("unknown generated binding");
                return `${expression} k${String(i)}`;
              }),
              keys = slots.map((_, i) => `k${String(i)}`);
            const args = [nullFirst ? null : "汉alpha", "beta汉", "fallback"];
            const sql = `SELECT ${keys.map((key, i) => (slots[i] === 2 ? `${key}::float8 AS ${key}` : key)).join(",")}, count(*) n,sum(i) FILTER(WHERE i>2) s,avg(i)::float8 a,min($2::text) lo,max($1::text) hi,max($3::text) fallback FROM (SELECT ${fields.join(",")},i FROM generate_series(1,5)g(i))q GROUP BY ${keys.join(",")} ORDER BY ${keys.join(",")}`;
            expect(resultRows(await core.execute(sql, args))).toEqual(
              (await pg.query(sql, args)).rows,
            );
            if (new Set(slots).size < slots.length) reach.reach("logical aliases");
            if (slots.includes(0) && slots.includes(1)) reach.reach("distinct params");
            if (slots.includes(2)) reach.reach("typmod preserved");
            if (slots.includes(4)) reach.reach("computed key");
            if (nullFirst) reach.reach("nulls");
            reach.reach("remapped arguments");
            reach.reach("PG exact");
            expect(core.spill.bytesInUse).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [[4, 4, 0, 1, 3], false],
            [[4, 4, 0, 1, 3], true],
            [[0, 1, 2, 0, 3, 1], false],
            [[0, 1, 2, 0, 3, 1], true],
            [[3, 3, 0, 0], true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  60000,
);

it.each(seedsFor("next-agg-column-progress", [0xb799, 0xb79a]))(
  "repeated stored key under pressure seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("aggregate stored key pressure", AGG_COLUMN_PROGRESS_REACH);
    try {
      await pg.waitReady;
      await core.execute("CREATE TABLE t(a TEXT)");
      await pg.exec("CREATE TABLE t(a TEXT)");
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 16 }),
          fc.boolean(),
          fc.boolean(),
          async (keys, unicode, nullable) => {
            reach.beginHistory();
            const value = unicode ? "汉".repeat(266666) : "x".repeat(800000);
            await core.execute("DROP TABLE t");
            await core.execute("CREATE TABLE t(a TEXT)");
            await pg.exec("DROP TABLE t; CREATE TABLE t(a TEXT)");
            await core.writeBatch("insertBatch", "t", [
              { a: nullable ? null : value },
              { a: value },
            ]);
            await pg.query("INSERT INTO t VALUES($1),($2)", [nullable ? null : value, value]);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const governor = new Governor(256 << 20),
              blocker = await governor.admit({
                label: "pinned stored-key competitor",
                minimumBytes: 0,
                spillable: true,
              });
            const mocks: Array<{ mockRestore(): void }> = [];
            let occupied = false;
            const abort = new AbortController(),
              timer = setTimeout(
                () => abort.abort(new Error("stored key progress deadline")),
                30000,
              );
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserve.apply(this, args);
                  if (this.request.governor === governor && !occupied) {
                    expect(
                      blocker.allocate(
                        governor.capBytes - governor.reserveBytes - governor.usedBytes,
                      ),
                    ).toBe(true);
                    occupied = true;
                  }
                }),
              );
              const prepare = core.executor.prepare.bind(core.executor);
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const p = prepare(plan);
                  return {
                    ...p,
                    execute: (r) => p.execute({ ...r, governor, signal: abort.signal }),
                  };
                }),
              );
              const fields = Array.from({ length: keys }, (_, i) => `a k${String(i)}`),
                group = Array.from({ length: keys }, (_, i) => `k${String(i)}`);
              const sql = `SELECT count(*) n FROM (SELECT ${fields.join(",")} FROM t)q GROUP BY ${group.join(",")} ORDER BY n`;
              expect(resultRows(await core.execute(sql))).toEqual((await pg.query(sql)).rows);
              expect(occupied).toBe(true);
              reach.reach("pinned");
              reach.reach("PG exact");
              if (keys === 1) reach.reach("one key");
              if (keys === 16) reach.reach("sixteen keys");
              if (nullable) reach.reach("NULL");
              reach.reach(unicode ? "UTF8" : "ASCII");
            } finally {
              clearTimeout(timer);
              for (const m of mocks.reverse()) m.mockRestore();
              blocker.close();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero");
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [1, false, false],
            [16, true, true],
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
  120000,
);
