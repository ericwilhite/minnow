/** Every owned typed buffer is funded before its constructor, inside the complete minimum. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import type { FlatVector } from "../vector/vector.js";
import type { Rel } from "../ir/ir.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { vectorOf, rowsOf } from "../exec/join-fixtures.js";
import { PercentileOperator, percentileMinimum } from "../exec/agg/percentile.js";

it.each(seedsFor("next-percentile-charge", [0xe159, 0xe15a]))(
  "funds native buffers before construction (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open();
    try {
      await core.execute("CREATE TABLE t(g text,x double precision)");
      let rel: Rel = core.prepare(
        "SELECT g,percentile_cont(.5) WITHIN GROUP(ORDER BY x) p FROM t GROUP BY g",
      ).plan.root;
      while (rel.op !== "aggregate") {
        if (!("input" in rel)) throw Error("No percentile relation");
        rel = rel.input;
      }
      // Keep expression compilation outside this row-bank allocation check: a prepared
      // float literal is caller-owned, just like the decoded fixture input vectors.
      const fraction: FlatVector = {
        form: "flat",
        type: { kind: "float64" },
        length: 1,
        validity: null,
        data: { layout: "f64", values: Float64Array.of(0.5) },
      };
      const aggregate: Extract<Rel, { op: "aggregate" }> = {
        ...rel,
        aggregates: rel.aggregates.map((call) => ({
          ...call,
          args: [{ op: "literal" as const, type: fraction.type, value: fraction }],
        })),
      };
      const reach = createReach("percentile native charging", [
        "float values",
        "null bits",
        "group ids",
        "funded",
        "released",
        "cancel readiness",
      ]);
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 8192, max: 16384 }), async (rows) => {
          reach.beginHistory();
          const bound = { rows, groups: 16, keyMinimum: 2 << 20 },
            minimum = percentileMinimum(aggregate, bound, 0),
            governor = new Governor(16 << 20);
          const grant = await governor.admit({
            label: "prepaid percentile",
            minimumBytes: minimum,
            spillable: false,
          });
          let funded = 0,
            constructed = 0,
            operator: PercentileOperator | undefined;
          const kinds = new Set<string>();
          const charge = {
            allocate: (bytes: number) => {
              if (grant.allocated + bytes > minimum) return false;
              if (!grant.allocate(bytes)) return false;
              funded += bytes;
              return true;
            },
            free: (bytes: number) => grant.free(bytes),
          };
          const arena = new OutputArena(charge, 2048),
            reason = new Error("test cleanup");
          try {
            for (const name of [
              "Float64Array",
              "Uint8Array",
              "Uint16Array",
              "Uint32Array",
              "Int32Array",
            ] as const) {
              const original = globalThis[name];
              vi.stubGlobal(
                name,
                new Proxy(original, {
                  construct(Type, args) {
                    const first: unknown = args[0];
                    if (
                      !(first instanceof ArrayBuffer) &&
                      !(
                        typeof SharedArrayBuffer !== "undefined" &&
                        first instanceof SharedArrayBuffer
                      )
                    ) {
                      const length =
                        typeof first === "number"
                          ? first
                          : first === undefined
                            ? 0
                            : (first as { length: number }).length;
                      constructed += length * Type.BYTES_PER_ELEMENT;
                      if (constructed > funded)
                        throw new Error(`Typed allocation preceded charge: ${name}`);
                      kinds.add(name);
                    }
                    return Reflect.construct(Type, args) as object;
                  },
                }),
              );
            }
            operator = new PercentileOperator(
              aggregate,
              {
                input: aggregate.input.output.map((field) => field.type),
                arena,
                params: { slots: [] },
              },
              charge,
              bound,
              new AbortController().signal,
              {
                push: () => "ready",
                ready: () => Promise.resolve(),
                finish: () => undefined,
                abort: () => undefined,
              },
            );
          } finally {
            vi.unstubAllGlobals();
            operator?.abort(reason);
            try {
              arena.close();
              expect(grant.allocated).toBe(0);
            } finally {
              grant.close();
            }
          }
          await expect(operator.ready()).rejects.toBe(reason);
          reach.reach("cancel readiness");
          expect(kinds.has("Float64Array")).toBe(true);
          expect(kinds.has("Uint8Array")).toBe(true);
          expect(kinds.has("Int32Array")).toBe(true);
          expect(constructed).toBeGreaterThan(0);
          expect(governor.usedBytes).toBe(0);
          reach.reach("float values");
          reach.reach("null bits");
          reach.reach("group ids");
          reach.reach("funded");
          reach.reach("released");
        }),
        { seed, numRuns: 4 },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  },
  30000,
);

it.each(seedsFor("next-percentile-output-cancel", [0xe15b, 0xe15c]))(
  "owns output across reentrant cancellation (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open();
    const reach = createReach("percentile output cancellation", [
      "ready sink",
      "blocked sink",
      "abort method",
      "abort signal",
      "owned borrow",
      "no finish",
      "released",
    ]);
    try {
      await core.execute("CREATE TABLE t(g text,x double precision)");
      let rel: Rel = core.prepare("SELECT percentile_cont(.5) WITHIN GROUP(ORDER BY x) p FROM t")
        .plan.root;
      while (rel.op !== "aggregate") {
        if (!("input" in rel)) throw new Error("No scalar percentile");
        rel = rel.input;
      }
      const aggregate = rel,
        input = aggregate.input.output.map((field) => field.type);
      const batch = {
        length: 5,
        selection: null,
        columns: input.map((type) => vectorOf(type, [7, null, 1, 2, 4])),
      };
      await fc.assert(
        fc.asyncProperty(fc.boolean(), fc.boolean(), async (blocked, signalOnly) => {
          reach.beginHistory();
          const bound = { rows: 5, groups: 1, keyMinimum: 0 },
            minimum = percentileMinimum(aggregate, bound, 0),
            governor = new Governor(16 << 20),
            grant = await governor.admit({
              label: "percentile/output",
              minimumBytes: minimum,
              spillable: false,
            }),
            arena = new OutputArena(grant, 2048),
            controller = new AbortController(),
            reason = new Error("cancel percentile output");
          let operator: PercentileOperator | undefined;
          const finish = vi.fn(() => undefined);
          try {
            operator = new PercentileOperator(
              aggregate,
              { input, arena, params: { slots: [] } },
              grant,
              bound,
              controller.signal,
              {
                push(output) {
                  expect(rowsOf(output)).toEqual([[3]]);
                  expect(grant.allocated).toBeGreaterThan(0);
                  if (signalOnly) controller.abort(reason);
                  else operator?.abort(reason);
                  // The callback may cancel the producer, but it still owns its finish job.
                  expect(grant.allocated).toBeGreaterThan(0);
                  reach.reach("owned borrow");
                  return blocked ? "blocked" : "ready";
                },
                ready: () => Promise.reject(reason),
                finish,
                abort: () => undefined,
              },
            );
            expect(operator.push(batch)).toBe("ready");
            const work = operator.finish();
            await expect(work).rejects.toBe(reason);
            await expect(operator.closed).rejects.toBe(reason);
            await expect(operator.ready()).rejects.toBe(reason);
            expect(finish).not.toHaveBeenCalled();
            reach.reach("no finish");
            reach.reach(blocked ? "blocked sink" : "ready sink");
            reach.reach(signalOnly ? "abort signal" : "abort method");
          } finally {
            operator?.abort(reason);
            await operator?.closed.catch(() => undefined);
            try {
              arena.close();
              expect(grant.allocated).toBe(0);
            } finally {
              grant.close();
            }
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("released");
        }),
        {
          seed,
          numRuns: 4,
          examples: [
            [false, false],
            [false, true],
            [true, false],
            [true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  },
  30000,
);
