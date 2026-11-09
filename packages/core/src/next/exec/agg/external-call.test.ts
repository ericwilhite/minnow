/** External call states stay constant per group while their DISTINCT sorts spill. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { AggregateCall } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { I32, I64, F64, col, numbers, valueAt, sqlLiteral } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { afterYield, yieldTask } from "../pipeline.js";
import { Slice } from "../slices.js";
import { sortMinimumBytes } from "../sort.js";
import { ExternalCall, externalCallMinimum } from "./external-call.js";
import { SortGrants } from "./sort-grants.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});

it.each(seedsFor("aggregate-external-call", [0xb497, 0xb498]))(
  "keeps exact transitions through spill and repeated groups (seed %i)",
  async (seed) => {
    const reach = createReach("external aggregate call", [
      "SUM",
      "AVG",
      "DISTINCT",
      "DISTINCT_FLOAT",
      "spilled",
      "restarted",
      "PG",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 17000, max: 19000 }),
        fc.integer({ min: 1, max: 9 }),
        async (count, salt) => {
          reach.beginHistory();
          for (const mode of ["SUM", "AVG", "DISTINCT", "DISTINCT_FLOAT"] as const) {
            const distinct = mode === "DISTINCT" || mode === "DISTINCT_FLOAT",
              type = mode === "DISTINCT" ? I32 : F64;
            const call: AggregateCall = {
              fn: mode === "AVG" ? "avg" : "sum",
              args: [col(0, type)],
              type: mode === "DISTINCT" ? I64 : F64,
              filter: null,
              distinct,
              orderBy: [],
            };
            const minimum =
              externalCallMinimum(call) + (distinct ? sortMinimumBytes([type, F64]) : 0);
            const governor = new Governor(Math.max(16 << 20, 2 * minimum));
            const parts = await governor.admitGroup({
              label: "external call",
              grants: [
                { label: "fixed", minimumBytes: minimum, spillable: true },
                ...Array.from({ length: distinct ? 2 : 1 }, (_, i) => ({
                  label: `progress${String(i)}`,
                  minimumBytes: 0,
                  spillable: true,
                })),
              ],
            });
            const grant = parts[0];
            if (!grant) throw new Error("Missing atomic minimum");
            const filler = await governor.admit({
              label: "competing pinned query",
              minimumBytes: governor.capBytes - governor.reserveBytes - minimum,
              spillable: false,
            });
            const shared = new SortGrants(grant, parts.slice(1));
            const arena = new OutputArena(grant, 2048),
              spill = new MemorySpillService(),
              slice = new Slice(15);
            let files = 0;
            const create = spill.create.bind(spill);
            spill.create = (query) => {
              files++;
              return create(query);
            };
            const host = {
              grants: shared,
              context: { arena, input: [type], params: { slots: [] } },
              check() {
                expect(governor.usedBytes).toBeLessThanOrEqual(governor.capBytes);
              },
              async pace() {
                if (slice.due()) {
                  await yieldTask();
                  afterYield(slice.clock);
                }
              },
              sortContext(owner: typeof grant) {
                return { grant: owner, spill, queryId: 1 };
              },
            };
            const state = new ExternalCall(call, host, 0);
            try {
              for (let group = 0; group < 2; group++) {
                const rows: number[] = [];
                for (let from = 0; from < count; from += 2048) {
                  const values = Array.from({ length: Math.min(2048, count - from) }, (_, i) =>
                    mode === "DISTINCT"
                      ? ((from + i) * 7919 + salt + group) % count
                      : (from + i) % 3 === 0
                        ? mode === "DISTINCT_FLOAT"
                          ? 1e16
                          : 2 ** 30
                        : (from + i) % 3 === 1
                          ? -(mode === "DISTINCT_FLOAT" ? 1e16 : 2 ** 30)
                          : ((from + i + salt) % 17) / 8,
                  );
                  rows.push(...values);
                  const batch = {
                    length: values.length,
                    columns: [numbers(type, values)],
                    selection: null,
                  };
                  await state.add(
                    batch,
                    0,
                    batch.length,
                    numbers(
                      F64,
                      values.map((_, i) => from + i),
                    ),
                  );
                }
                const value = valueAt(await state.emit(), 0);
                const fn = mode === "AVG" ? "avg" : "sum";
                const result = await pg.query<{ v: string }>(
                  `SELECT ${fn}(${distinct ? "DISTINCT " : ""}v)::text v FROM (VALUES ${rows.map((v) => `(${sqlLiteral(mode === "DISTINCT" ? "int4" : "float8", v)})`).join(",")}) t(v)`,
                );
                expect(Number(value)).toBe(Number(result.rows[0]?.v));
                if (group === 0) state.restart();
              }
              if (distinct) {
                expect(files).toBeGreaterThan(0);
                reach.reach("spilled");
              }
              reach.reach(mode);
              reach.reach("PG");
              reach.reach("restarted");
            } finally {
              state.abort(new Error("test cleanup"));
              await state.close();
              arena.close();
              shared.close();
              for (const part of parts) part.close();
              filler.close();
            }
            expect(governor.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
            reach.reach("released");
          }
        },
      ),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("aggregate-external-distinct-zero", [0xb4a5, 0xb4a6]))(
  "retains DISTINCT extrema representatives for signed zeros (seed %i)",
  async (seed) => {
    const reach = createReach("external DISTINCT zero representative", [
      "MIN",
      "MAX",
      "negative first",
      "positive first",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (negativeFirst) => {
        reach.beginHistory();
        for (const fn of ["min", "max"] as const) {
          const call: AggregateCall = {
            fn,
            args: [col(0, F64)],
            type: F64,
            filter: null,
            distinct: true,
            orderBy: [],
          };
          const minimum = externalCallMinimum(call) + sortMinimumBytes([F64, F64]),
            governor = new Governor(16 << 20);
          const parts = await governor.admitGroup({
            label: "distinct zero",
            grants: [
              { label: "fixed", minimumBytes: minimum, spillable: true },
              { label: "call", minimumBytes: 0, spillable: true },
              { label: "sort", minimumBytes: 0, spillable: true },
            ],
          });
          const grant = parts[0];
          if (!grant) throw new Error("Missing zero fixture minimum");
          const shared = new SortGrants(grant, parts.slice(1)),
            arena = new OutputArena(grant, 2048),
            spill = new MemorySpillService();
          const state = new ExternalCall(
            call,
            {
              grants: shared,
              context: { arena, input: [F64], params: { slots: [] } },
              check: () => undefined,
              pace: () => Promise.resolve(),
              sortContext: (grant) => ({ grant, spill, queryId: 1 }),
            },
            0,
          );
          try {
            const values = negativeFirst ? [-0, 0] : [0, -0];
            await state.add(
              { length: 2, columns: [numbers(F64, values)], selection: null },
              0,
              2,
              numbers(F64, [0, 1]),
            );
            const result = await pg.query<{ v: string }>(
              `SELECT ${fn}(DISTINCT v)::text v FROM (VALUES ${values.map((v) => `(${sqlLiteral("float8", v)})`).join(",")}) t(v)`,
            );
            expect(Number(valueAt(await state.emit(), 0))).toBe(Number(result.rows[0]?.v));
            reach.reach(fn === "min" ? "MIN" : "MAX");
            reach.reach(negativeFirst ? "negative first" : "positive first");
          } finally {
            state.abort(new Error("test cleanup"));
            await state.close();
            arena.close();
            shared.close();
            expect(parts.reduce((n, p) => n + p.allocated, 0)).toBe(0);
            for (const p of parts) p.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 4, examples: [[true], [false]] },
    );
    reach.assertReached();
  },
);
