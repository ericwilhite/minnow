/** Admission follows the sort owner's formula, including future workspace corrections. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, AGG_SORT_WORKSPACE_REACH } from "../../../testing/reach.js";
import * as sort from "../sort.js";
import { aggregateLaneBytes, type AggregateProgressLayout } from "./parameter-copies.js";
const original = sort.sortMergeWorkspaceBytes;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("aggregate-shared-sort-workspace", [0xb7a7, 0xb7a8]))(
  "DISTINCT helpers delegate their complete merge bound to sort seed%i",
  (seed) => {
    const reach = createReach("shared sort workspace", AGG_SORT_WORKSPACE_REACH);
    const layout: AggregateProgressLayout = {
      simple: true,
      input: [],
      keys: [],
      calls: [
        { width: { op: "param", slot: 0 }, variable: true, distinct: true, minimum: 128 * 1024 },
      ],
    };
    fc.assert(
      fc.property(
        fc.integer({ min: 524287, max: 1300000 }),
        fc.constantFrom("value", "restore-value", "ordinal"),
        fc.integer({ min: 17, max: 32768 }),
        (width, kind, delta) => {
          reach.beginHistory();
          expect(sort.sortMergeWorkspaceBytes).toBeTypeOf("function");
          const recipe = kind === "value" ? "external" : "restore";
          const slot = kind === "ordinal" ? 4 : 3;
          const before = aggregateLaneBytes(layout, recipe, slot, 8 << 20, [width]);
          const owner = vi
            .spyOn(sort, "sortMergeWorkspaceBytes")
            .mockImplementation((...args) => original(...args) + delta);
          try {
            const after = aggregateLaneBytes(layout, recipe, slot, 8 << 20, [width]);
            expect(owner).toHaveBeenCalledTimes(1);
            expect(after - before).toBe(delta);
            const bound = owner.mock.calls[0];
            if (!bound) throw new Error("missing shared formula call");
            expect(bound[0]).toBe(width);
            expect(bound[1]).toBeGreaterThanOrEqual(width);
            expect(bound[2]).toBeGreaterThan(0);
            reach.reach(kind === "ordinal" ? "ordinal sort" : "value sort");
            reach.reach(bound[1] === width ? "exact arena" : "rounded arena");
            reach.reach(bound[1] > bound[0] + 4096 ? "arena output" : "page output");
            reach.reach("owner correction propagated");
            reach.reach("metadata retained");
          } finally {
            owner.mockRestore();
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [524288, "value", 17],
          [524289, "value", 31],
          [1048575, "ordinal", 127],
          [1048576, "restore-value", 256],
          [1048577, "ordinal", 1024],
        ],
      },
    );
    reach.assertReached();
  },
);
