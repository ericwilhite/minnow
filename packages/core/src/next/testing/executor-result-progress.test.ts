/** Result admission follows emitted fields; pressure may bound an unfiltered read to one row. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, HASH_RESULT_PROGRESS_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { TableScan } from "../exec/table-scan.js";
import { QueryRuntime } from "../exec/run.js";
import { taskClock } from "../task.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied at invocation
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied at invocation
const restore = QueryRuntime.prototype.restorePhase;
// eslint-disable-next-line @typescript-eslint/unbound-method -- receiver supplied at invocation
const admit = Governor.prototype.admitGroup;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const transfer = Governor.prototype.transferCredit;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const next = TableScan.prototype.next;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("hash-result-progress", [0xb7c3, 0xb7c4]))(
  "funds only emitted output and preserves fitting peaks seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("hash result progress", HASH_RESULT_PROGRESS_REACH);
    const large = "x".repeat(800000);
    try {
      await pg.waitReady;
      await core.execute("CREATE TABLE t(a TEXT,i INTEGER)");
      await core.writeBatch(
        "insertBatch",
        "t",
        [1, 2].map((i) => ({ a: large, i })),
      );
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await pg.exec("CREATE TABLE t(a TEXT,i INTEGER)");
      await pg.query("INSERT INTO t VALUES($1,1),($1,2)", [large]);
      await core.execute("CREATE TABLE tail(a TEXT,i INTEGER)");
      await core.writeBatch(
        "insertBatch",
        "tail",
        [1, 2, 3, 4].map((i) => ({ a: i <= 2 ? large : "k", i })),
      );
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await pg.exec("CREATE TABLE tail(a TEXT,i INTEGER)");
      await pg.query("INSERT INTO tail VALUES($1,1),($1,2),('k',3),('k',4)", [large]);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 16 }),
          fc.constantFrom(16, 32, 64, 128),
          fc.constantFrom("short", "case", "nested", "nullif"),
          fc.constantFrom("count", "key", "max", "aliases"),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (count, cap, shape, output, column, pin, tail) => {
            reach.beginHistory();
            const leaf = column ? "a" : "$1::text";
            const expr =
              shape === "short"
                ? `coalesce('k'::text,${leaf})`
                : shape === "case"
                  ? `CASE WHEN i=1 THEN ${leaf} ELSE 'k'::text END`
                  : shape === "nested"
                    ? `coalesce(NULL::text,nullif(${leaf},'absent'::text),'k'::text)`
                    : `nullif(${leaf},'absent'::text)`;
            const keys = Array.from({ length: count }, (_, i) => `k${String(i)}`);
            const sql = `SELECT ${output === "key" ? "k0," : output === "aliases" ? "k0 a,k0 b,k0 c," : output === "max" ? "max(k0) m," : ""}count(*) n FROM (SELECT ${keys.map((k) => `${expr} ${k}`).join(",")} FROM ${column ? (tail ? "tail" : "t") : "generate_series(1,2)g(i)"})q GROUP BY ${keys.join(",")}`;
            const args = column ? [] : [large];
            // Variable transition helpers have a separate retained-state minimum. This
            // dimension tests them at 64/128 MiB; the 16/32 MiB anchors test COUNT/key rows.
            const bytes = (output === "max" ? Math.max(cap, 64) : cap) * (1 << 20);
            const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
              rows.map((r) => JSON.stringify(r)).sort();
            const expected = canonical((await pg.query<Record<string, unknown>>(sql, args)).rows);
            const run = async (legacy: boolean) => {
              const governor = new Governor(bytes);
              let holder = await governor.admit({
                label: "competitor",
                minimumBytes: 0,
                spillable: true,
              });
              const mocks: Array<{ mockRestore(): void }> = [];
              const progress: number[] = [];
              let occupied = false,
                tick = 0,
                small = false;
              const prepare = core.executor.prepare.bind(core.executor);
              try {
                mocks.push(vi.spyOn(taskClock, "now").mockImplementation(() => ++tick));
                taskClock.begin();
                if (legacy && Object.hasOwn(QueryRuntime.prototype, "resultRowProgress"))
                  mocks.push(
                    vi
                      .spyOn(QueryRuntime.prototype, "resultRowProgress")
                      .mockImplementation(() => undefined),
                  );
                const fill = () => {
                  expect(
                    holder.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
                  ).toBe(true);
                  occupied = true;
                };
                mocks.push(
                  vi.spyOn(Governor.prototype, "admitGroup").mockImplementation(function (
                    this: Governor,
                    options,
                  ) {
                    if (this === governor)
                      for (const part of options.grants)
                        if (part.label.endsWith("/result/progress"))
                          progress.push(part.minimumBytes);
                    return admit.call(this, options);
                  }),
                );
                mocks.push(
                  vi.spyOn(Governor.prototype, "transferCredit").mockImplementation(function (
                    this: Governor,
                    from,
                    to,
                    bytes,
                  ) {
                    if (
                      this === governor &&
                      from.label.endsWith("/fixed-buffers") &&
                      to.label.endsWith("/result/progress")
                    )
                      progress.push(bytes);
                    return transfer.call(this, from, to, bytes);
                  }),
                );
                mocks.push(
                  vi.spyOn(TableScan.prototype, "next").mockImplementation(function (
                    this: TableScan,
                  ) {
                    const check = (item: Awaited<ReturnType<typeof next>>) => {
                      if (small && item)
                        expect(
                          item.batch.selection?.count ?? item.batch.length,
                        ).toBeLessThanOrEqual(1);
                      return item;
                    };
                    const item = next.call(this);
                    return item instanceof Promise ? item.then(check) : check(item);
                  }),
                );
                mocks.push(
                  vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                    this: QueryRuntime,
                    ...a
                  ) {
                    await reserve.apply(this, a);
                    if (this.request.governor === governor) {
                      small ||= a[0].some((entry) => this.sourceBatchRows(entry.rel.id) === 1);
                      if (pin && !occupied) fill();
                    }
                  }),
                );
                mocks.push(
                  vi
                    .spyOn(QueryRuntime.prototype, "restorePhase")
                    .mockImplementation(async function (this: QueryRuntime, ...a) {
                      if (this.request.governor !== governor) return restore.apply(this, a);
                      holder.close();
                      await restore.apply(this, a);
                      holder = await governor.admit({
                        label: "repin",
                        minimumBytes: 0,
                        spillable: true,
                      });
                      if (pin) fill();
                    }),
                );
                mocks.push(
                  vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                    const q = prepare(plan);
                    return {
                      ...q,
                      execute: (request) =>
                        q.execute({ ...request, governor, signal: AbortSignal.timeout(30000) }),
                    };
                  }),
                );
                const result = await core.execute(sql, args);
                if (result.kind !== "rows" || !("batches" in result))
                  throw new Error("missing rows");
                const got = canonical(oracleRows(result));
                expect(got).toEqual(expected);
                return { peak: core.lastProfile?.peakHeldBytes ?? 0, progress, small };
              } finally {
                for (const m of mocks.reverse()) m.mockRestore();
                holder.close();
                taskClock.begin();
                expect(governor.usedBytes).toBe(0);
                expect(core.spill.bytesInUse).toBe(0);
              }
            };
            // The legacy row recipe is the parent's output accounting. Source/transition
            // grants remain charged; independently archived main is also compared in evidence.
            let parent: Awaited<ReturnType<typeof run>> | null = null;
            try {
              parent = await run(true);
            } catch (error) {
              expect(error).toMatchObject({ code: "MINNOW_MEMORY_ADMISSION" });
            }
            const got = await run(false);
            if (parent) {
              expect(got.peak).toBeLessThanOrEqual(parent.peak);
              reach.reach("fitting peak");
            }
            if (output === "count") {
              expect(got.progress.length).toBeGreaterThan(0);
              expect(got.progress.every((n) => n === 0)).toBe(true);
              reach.reach("COUNT only");
            }
            if (output === "key") reach.reach("emitted key");
            if (output === "aliases") reach.reach("emitted aliases");
            if (output === "max") reach.reach("variable aggregate");
            if (got.small) {
              reach.reach("one-row scan");
              if (column && tail) reach.reach("small row tail");
            }
            if (column) reach.reach("column");
            else reach.reach("parameter");
            if (pin) reach.reach("competitor");
            if (count === 12) reach.reach("12 keys");
            if (count === 16) reach.reach("16 keys");
            if (cap === 16 && output !== "max") reach.reach("16 MiB");
            if (cap === 32 && output !== "max") reach.reach("32 MiB");
            if (cap === 64) reach.reach("64 MiB");
            if (cap === 128) reach.reach("128 MiB");
            if (shape === "short") reach.reach("unreachable wide branch");
            reach.reach("PG exact");
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 30, // Eighteen witnesses (including tails/aliases) plus twelve random histories.
          endOnFailure: true,
          examples: [
            [1, 16, "short", "count", true, false, false],
            [12, 16, "short", "count", true, true, false],
            [16, 16, "short", "count", true, false, false],
            [12, 16, "case", "count", true, true, false],
            [16, 16, "nested", "count", true, false, false],
            [12, 16, "nullif", "count", true, true, false],
            [12, 32, "case", "count", false, true, false],
            [16, 64, "nested", "count", false, false, false],
            [16, 128, "nullif", "count", false, true, false],
            [1, 16, "case", "key", true, true, false],
            [12, 32, "short", "key", true, false, false],
            [16, 64, "nested", "key", false, true, false],
            [3, 64, "nullif", "max", true, true, false],
            [3, 128, "case", "max", false, false, false],
            [12, 16, "case", "count", true, true, true],
            [16, 16, "nested", "count", true, false, true],
            [12, 16, "case", "aliases", true, true, false],
            [16, 32, "nullif", "aliases", false, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  180000,
);
