/** A wide input is not necessarily a wide key, and only active borrowing lanes own progress. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, AGG_ADMISSION_PEAK_REACH } from "../../testing/reach.js";
import { taskClock } from "../task.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { Governor } from "../memory/governor.js";
import { StepLoans } from "../memory/step-loans.js";
import { QueryRuntime } from "../exec/run.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// Capture originals once; a timeout must not make the next history capture a spy.
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const envelope = QueryRuntime.prototype.progressEnvelope;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const restore = QueryRuntime.prototype.restorePhase;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const borrow = StepLoans.prototype.borrow;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const admitGroup = Governor.prototype.admitGroup;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied
const transfer = Governor.prototype.transferCredit;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("next-agg-admission-peak", [0xb793, 0xb794]))(
  "preserves fitting parent peaks and reachable output widths seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("aggregate admission peak", AGG_ADMISSION_PEAK_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 4 }),
          fc.integer({ min: 980000, max: 1000000 }),
          fc.constantFrom(
            "short",
            "null-prefix",
            "direct",
            "max",
            "case",
            "nullif",
            "nested",
            "nullable-max",
          ),
          fc.boolean(),
          fc.boolean(),
          async (n, bytes, shape, unicode, near) => {
            reach.beginHistory();
            const large = unicode ? "汉".repeat(Math.floor(bytes / 3)) : "x".repeat(bytes);
            const args =
              shape === "nullable-max"
                ? [null, large]
                : shape === "max"
                  ? [large, unicode ? "字".repeat(large.length) : "y".repeat(large.length)]
                  : [large];
            const expr =
              shape === "short"
                ? "coalesce('k'::text,$1::text)"
                : shape === "null-prefix"
                  ? "coalesce(NULL::text,'k'::text,$1::text)"
                  : shape === "nested"
                    ? "coalesce(coalesce('k'::text,$1::text),$1::text)"
                    : shape === "case"
                      ? "CASE WHEN false THEN $1::text ELSE 'k'::text END"
                      : shape === "nullif"
                        ? "nullif('k'::text,$1::text)"
                        : shape === "max" || shape === "nullable-max"
                          ? "coalesce($1::text,$2::text)"
                          : "$1::text";
            const keys = Array.from({ length: n }, (_, i) => `a${String(i)}`);
            const sql = `SELECT count(*) n FROM (SELECT ${keys.map((k) => `${expr} AS ${k}`).join(",")} FROM generate_series(1,2))q GROUP BY ${keys.join(",")}`;
            const expected = (await pg.query(sql, args)).rows;
            const run = async (legacy: boolean, cap: number, query = sql, values = args) => {
              const governor = new Governor(cap),
                mocks: Array<{ mockRestore(): void }> = [];
              let restores = 0,
                loans = 0;
              const budgets: number[] = [];
              try {
                // Exact peak/cap comparisons require the same cooperative batching schedule.
                // One logical millisecond per check keeps real task yields (never disabled),
                // independent of GC/host pauses. Other pressure/reviewer replays use real time.
                let tick = 0;
                mocks.push(vi.spyOn(taskClock, "now").mockImplementation(() => ++tick));
                taskClock.begin();
                mocks.push(
                  vi.spyOn(Governor.prototype, "admitGroup").mockImplementation(function (
                    this: Governor,
                    options,
                  ) {
                    if (this === governor)
                      for (const part of options.grants)
                        if (part.label.includes("/aggregate#") && part.label.endsWith("/progress"))
                          budgets.push(part.minimumBytes);
                    return admitGroup.call(this, options);
                  }),
                );
                mocks.push(
                  vi.spyOn(Governor.prototype, "transferCredit").mockImplementation(function (
                    this: Governor,
                    source,
                    target,
                    bytes,
                  ) {
                    if (
                      this === governor &&
                      target.label.includes("/aggregate#") &&
                      target.label.endsWith("/progress")
                    )
                      budgets.push(bytes);
                    return transfer.call(this, source, target, bytes);
                  }),
                );
                if (legacy) {
                  // Exact pre-1355 admission: one old input envelope on every owner/helper.
                  // The shared staging correction is dormant: these parent controls stay hash.
                  mocks.push(
                    vi
                      .spyOn(QueryRuntime.prototype, "progressEnvelope")
                      .mockImplementation(function (this: QueryRuntime, bytes, multiple) {
                        return envelope.call(this, bytes, multiple);
                      }),
                  );
                  if (Object.hasOwn(QueryRuntime.prototype, "aggregateProgress"))
                    mocks.push(
                      vi
                        .spyOn(QueryRuntime.prototype, "aggregateProgress")
                        .mockImplementation(() => undefined),
                    );
                }
                mocks.push(
                  vi.spyOn(QueryRuntime.prototype, "restorePhase").mockImplementation(function (
                    this: QueryRuntime,
                    ...a
                  ) {
                    if (this.request.governor === governor) restores++;
                    return restore.apply(this, a);
                  }),
                );
                mocks.push(
                  vi.spyOn(StepLoans.prototype, "borrow").mockImplementation(function (
                    this: StepLoans,
                    bytes,
                    signal,
                  ) {
                    if (this.label.includes("/aggregate#")) {
                      loans++;
                      expect(bytes).toBeLessThanOrEqual(this.capacity);
                    }
                    return borrow.call(this, bytes, signal);
                  }),
                );
                const prepare = core.executor.prepare.bind(core.executor);
                mocks.push(
                  vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                    const p = prepare(plan);
                    return { ...p, execute: (request) => p.execute({ ...request, governor }) };
                  }),
                );
                const result = await core.execute(query, values);
                if (result.kind !== "rows" || !("batches" in result))
                  throw new Error("missing aggregate rows");
                expect(oracleRows(result)).toEqual(expected);
                const spill =
                  core.lastProfile?.operators.reduce((sum, p) => sum + p.spilledBytes, 0) ?? 0;
                return { peak: governor.peakBytes, restores, loans, spill, budgets };
              } finally {
                for (const mock of mocks.reverse()) mock.mockRestore();
                taskClock.begin();
                expect(governor.usedBytes).toBe(0);
                expect(core.spill.bytesInUse).toBe(0);
              }
            };
            const anchor = await run(true, 128 << 20);
            expect(anchor.restores).toBe(0);
            expect(anchor.spill).toBe(0);
            // Compensate the governor's ordinary 1/8 reserve, then just 64KiB slack.
            // Same fixture/cap on both paths; no resource expectation derived from candidate.
            const cap = near
              ? Math.ceil((anchor.peak * 8) / 7 / 65536) * 65536 + 65536
              : (shape === "short" || shape === "null-prefix"
                  ? 32
                  : shape === "max" || shape === "nullable-max"
                    ? 128
                    : 64) << 20;
            const parent = await run(true, cap),
              candidate = await run(false, cap);
            expect(parent.restores).toBe(0);
            expect(parent.spill).toBe(0);
            expect(candidate.peak).toBeLessThanOrEqual(parent.peak);
            if (["short", "null-prefix", "case", "nullif", "nested"].includes(shape)) {
              expect(candidate.budgets.length).toBeGreaterThan(0);
              expect(Math.max(...candidate.budgets)).toBeLessThanOrEqual(MAX_BLOB_BYTES);
              reach.reach("small output lane");
            }
            if (shape === "max" || shape === "nullable-max") {
              const direct = await run(false, cap, sql.replaceAll(expr, "$1::text"), [large]);
              expect(Math.max(...candidate.budgets)).toBe(Math.max(...direct.budgets));
              reach.reach("max not sum");
            }
            expect(candidate.restores).toBe(0);
            expect(candidate.spill).toBe(0);
            reach.reach(shape);
            reach.reach(unicode ? "UTF8" : "ASCII");
            if (near) reach.reach("near parent peak");
            if (n === 3 && bytes === 1000000 && !unicode && shape === "short" && !near)
              reach.reach("32MiB short witness");
            if (n === 3 && bytes === 1000000 && !unicode && shape === "direct" && !near)
              reach.reach("64MiB restore witness");
            reach.reach("PG exact");
            reach.reach("peak no higher");
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [3, 1000000, "short", false, false],
            [3, 1000000, "direct", false, false],
            [3, 1000000, "max", false, true],
            [4, 980000, "case", true, true],
            [2, 1000000, "nullif", false, true],
            [4, 1000000, "direct", false, true],
            [3, 1000000, "null-prefix", false, false],
            [3, 1000000, "nested", false, true],
            [3, 1000000, "nullable-max", false, true],
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
