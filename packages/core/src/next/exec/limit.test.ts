import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Batch } from "../vector/vector.js";
import { I32, numbers, valueAt } from "./kernels/test-support.js";
import { OutputArena } from "./kernels/vectors.js";
import { LimitOperator } from "./limit.js";
import type { Operator } from "./pipeline.js";

const history = fc.record({
  batches: fc.array(
    fc.array(fc.integer({ min: -100, max: 100 }), { minLength: 1, maxLength: 16 }),
    { minLength: 1, maxLength: 8 },
  ),
  selection: fc.constantFrom("dense", "reverse", "even"),
  offset: fc.integer({ min: 0, max: 80 }),
  limit: fc.oneof(fc.constant(null), fc.constant(0), fc.integer({ min: 0, max: 30 })),
});

for (const seed of seedsFor("next-exec-limit", [0x1a11, 0x1a12]))
  it(`LIMIT/OFFSET slices the logical selected stream (${String(seed)})`, () => {
    const reach = createReach(`limit ${String(seed)}`, [
      "dense",
      "reverse",
      "even",
      "zero",
      "unbounded",
      "offsetAcrossBatches",
      "limitReached",
    ] as const);
    fc.assert(
      fc.property(history, ({ batches, selection, offset, limit }) => {
        reach.beginHistory();
        reach.reach(selection);
        if (limit === null) reach.reach("unbounded");
        if (limit === 0) reach.reach("zero");
        let allocated = 0;
        const arena = new OutputArena(
          {
            allocate: (n) => {
              allocated += n;
              return true;
            },
            free: (n) => {
              allocated -= n;
            },
          },
          16,
        );
        const actual: unknown[] = [];
        const sink: Operator = {
          push(batch) {
            const n = batch.selection?.count ?? batch.length;
            const vector = batch.columns[0];
            if (vector === undefined) throw new Error("missing test column");
            for (let k = 0; k < n; k++) actual.push(valueAt(vector, batch.selection?.rows[k] ?? k));
            return "ready";
          },
          ready: () => Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        };
        const op = new LimitOperator(limit, offset, arena, sink);
        const expected: number[] = [];
        let released = 0;
        try {
          for (const values of batches) {
            const ids = values.map((_, i) => i);
            if (selection === "reverse") ids.reverse();
            else if (selection === "even")
              ids.splice(0, ids.length, ...ids.filter((i) => i % 2 === 0));
            const before = expected.length;
            expected.push(...ids.map((i) => values[i] ?? 0));
            if (before > 0 && before < offset && expected.length > offset)
              reach.reach("offsetAcrossBatches");
            expect(
              op.push(
                {
                  length: values.length,
                  columns: [numbers(I32, values)],
                  selection:
                    selection === "dense"
                      ? null
                      : { rows: Uint16Array.from(ids), count: ids.length },
                },
                {
                  release: () => {
                    released++;
                  },
                },
              ),
            ).toBe("ready");
            expect(allocated).toBe(32);
          }
          if (limit !== null && expected.length >= offset + limit) reach.reach("limitReached");
          expect(actual).toEqual(
            expected.slice(offset, limit === null ? undefined : offset + limit),
          );
          expect(released).toBe(batches.length);
          expect(op.done).toBe(
            (limit !== null && expected.length >= offset + limit) || limit === 0,
          );
        } finally {
          arena.close();
        }
        expect(allocated).toBe(0);
      }),
      { seed, numRuns: 150 },
    );
    reach.assertReached();
  });

function blocked(reject: boolean) {
  let releaseReady: () => void = () => undefined;
  const pending = new Promise<void>((resolve) => {
    releaseReady = resolve;
  });
  const sink: Operator = {
    push: () => "blocked",
    ready: () => (reject ? Promise.reject(new Error("gone")) : pending),
    finish: () => undefined,
    abort: () => undefined,
  };
  const arena = new OutputArena({ allocate: () => true, free: () => undefined }, 16);
  const op = new LimitOperator(1, 0, arena, sink);
  let releases = 0;
  const batch: Batch = { length: 1, columns: [numbers(I32, [7])], selection: null };
  return {
    op,
    arena,
    batch,
    transfer: {
      release: () => {
        releases++;
      },
    },
    releaseReady,
    releases: () => releases,
  };
}

it("holds an incoming transfer until readiness and releases it only once", async () => {
  const h = blocked(false);
  expect(h.op.push(h.batch, h.transfer)).toBe("blocked");
  const ready = h.op.ready();
  expect(h.releases()).toBe(0);
  h.releaseReady();
  await ready;
  expect(h.releases()).toBe(1);
  h.op.abort(new Error("closed"));
  expect(h.releases()).toBe(1);
  h.arena.close();
});

it("returns the transfer when readiness rejects", async () => {
  const h = blocked(true);
  expect(h.op.push(h.batch, h.transfer)).toBe("blocked");
  await expect(h.op.ready()).rejects.toThrow("gone");
  expect(h.releases()).toBe(1);
  h.op.abort(new Error("closed"));
  expect(h.releases()).toBe(1);
  h.arena.close();
});
