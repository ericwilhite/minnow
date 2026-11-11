/** Ownership is an ancestor path, not the set of live actors in nested finish() calls. */
import fc from "fast-check";
import { expect, it } from "vitest";
import type { Rel } from "../ir/ir.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_PARENT_REACH } from "../../testing/reach.js";
import { jsonRetainingParents } from "./json-plan.js";
it.each(seedsFor("next-jsonb-parent-path", [0xb68e, 0xb68f]))(
  "isolates retaining ancestors (seed %i)",
  (seed) => {
    const reach = createReach<string>("JSONB retaining ancestors", JSONB_PARENT_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 6 }),
        fc.integer({ min: 0, max: 127 }),
        fc.boolean(),
        (depth, bits, blocked) => {
          reach.beginHistory();
          let id = 0;
          const leaf = (): Rel => ({ id: id++, op: "values", rows: [], output: [] });
          const except = (left: Rel, right: Rel): Rel => ({
            id: id++,
            op: "setOp",
            kind: "except",
            all: false,
            inputs: [left, right],
            output: [],
          });
          const descendant = except(leaf(), leaf());
          const target: Rel = { id: id++, op: "project", input: descendant, exprs: [], output: [] };
          let root: Rel = target;
          const expected = new Set<number>();
          for (let i = 0; i < depth; i++) {
            const sibling = except(leaf(), leaf());
            const right = ((bits >>> i) & 1) !== 0;
            root = right ? except(sibling, root) : except(root, sibling);
            expected.add(root.id);
            root = { id: id++, op: "project", input: root, exprs: [], output: [] };
            reach.reach(right ? "right arm" : "left arm");
          }
          if (blocked)
            root = {
              id: id++,
              op: "window",
              input: root,
              partitionBy: [],
              orderBy: [],
              functions: [],
              output: [],
            };
          const parents = jsonRetainingParents(root);
          expect(parents.supported.has(target.id)).toBe(!blocked);
          expect(parents.excepts.get(target.id)).toEqual(blocked ? undefined : expected);
          reach.reach(blocked ? "retaining seam refused" : "unary/EXCEPT supported");
          reach.reach("descendant excluded");
          reach.reach("sibling excluded");
        },
      ),
      {
        seed,
        numRuns: 30,
        examples: [
          [0, 0, false],
          [6, 0, false],
          [6, 127, false],
          [3, 15, true],
        ],
      },
    );
    reach.assertReached();
  },
);
