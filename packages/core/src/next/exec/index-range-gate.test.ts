/** Native cursors compile through the complete query minimum with every key-order option. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import { createExecutor } from "./create.js";

it.each(seedsFor("next-index-range-gate", [0xe133, 0xe134]))(
  "compiles native query cursors without diagnostic opt-in (seed %i)",
  (seed) => {
    const reach = createReach("index range gate", [
      "unset",
      "false",
      "opt-in",
      "zero",
      "positive",
      "ascending",
      "descending",
    ]);
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 20 }), fc.boolean(), (limit, descending) => {
        reach.beginHistory();
        const type = { kind: "int32", bits: 32 } as const,
          output = [{ name: "v", type, nullable: false }];
        const input: Rel = {
          id: 1,
          op: "indexRange",
          output,
          tableId: 1,
          indexId: 1,
          columns: [1],
          lower: null,
          upper: null,
          emitLocator: false,
          descending,
        };
        const plan: PhysicalPlan = {
          root: {
            id: 2,
            op: "limit",
            output,
            limit: { kind: "const", value: limit },
            offset: null,
            input,
          },
          columns: output,
          parameters: [],
          placeholderCount: 0,
          clockSlot: null,
          initPlans: [],
          catalogVersion: 1,
          volatile: false,
        };
        for (const [name, options] of [
          ["unset", {}],
          ["false", { keyOrder: false }],
        ] as const) {
          expect(() => createExecutor(options).prepare(plan)).not.toThrow();
          reach.reach(name);
        }
        expect(() => createExecutor({ keyOrder: true }).prepare(plan)).not.toThrow();
        reach.reach("opt-in");
        reach.reach(limit === 0 ? "zero" : "positive");
        reach.reach(descending ? "descending" : "ascending");
      }),
      {
        seed,
        numRuns: 12,
        examples: [
          [0, false],
          [7, true],
        ],
      },
    );
    reach.assertReached();
  },
);
