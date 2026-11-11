/** Ownership is an ancestor path, not the set of live actors in nested finish() calls. */
import fc from "fast-check";
import { expect, it } from "vitest";
import type { Rel } from "../ir/ir.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_PARENT_REACH, RETAINING_GRAPH_REACH } from "../../testing/reach.js";
import { phaseRetainingParents } from "./json-plan.js";
it.each(seedsFor("next-jsonb-parent-path", [0xb68e, 0xb68f]))(
  "isolates retaining ancestors (seed %i)",
  (seed) => {
    const reach = createReach<string>("JSONB retaining ancestors", JSONB_PARENT_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 6 }),
        fc.integer({ min: 0, max: 127 }),
        fc.boolean(),
        (depth, bits, windowParent) => {
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
          if (windowParent)
            root = {
              id: id++,
              op: "window",
              input: root,
              partitionBy: [],
              orderBy: [],
              functions: [],
              output: [],
            };
          const parents = phaseRetainingParents(root);
          expect(parents.supported.has(target.id)).toBe(true);
          expect(parents.excepts.get(target.id)).toEqual(expected);
          reach.reach(windowParent ? "retaining window supported" : "unary/EXCEPT supported");
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

it.each(seedsFor("next-retaining-producer-graph", [0xb6a8, 0xb6a9]))(
  "keeps native retention on each producer's ancestor path (seed %i)",
  (seed) => {
    const reach = createReach<string>("native producer ancestry", RETAINING_GRAPH_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 6 }),
        fc.integer({ min: 0, max: 127 }),
        fc.boolean(),
        (depth, bits, aggregate) => {
          reach.beginHistory();
          let id = 0;
          const leaf = (): Rel => ({ id: id++, op: "values", rows: [], output: [] });
          const join = (left: Rel, right: Rel): Rel => ({
            id: id++,
            op: "join",
            left,
            right,
            kind: "full",
            algorithm: "nestedLoop",
            leftKeys: [],
            rightKeys: [],
            nullSafe: [],
            residual: null,
            publishes: [],
            output: [],
          });
          const window = (input: Rel): Rel => ({
            id: id++,
            op: "window",
            input,
            partitionBy: [],
            orderBy: [],
            functions: [],
            output: [],
          });
          const except = (left: Rel, right: Rel): Rel => ({
            id: id++,
            op: "setOp",
            kind: "except",
            all: false,
            inputs: [left, right],
            output: [],
          });
          const child = window(join(leaf(), leaf()));
          const target: Rel = aggregate
            ? {
                id: id++,
                op: "aggregate",
                input: child,
                groupBy: [],
                groupingSets: null,
                aggregates: [],
                output: [],
              }
            : { id: id++, op: "project", input: child, exprs: [], output: [] };
          let root: Rel = target;
          const expected = {
            joins: new Set<number>(),
            windows: new Set<number>(),
            excepts: new Set<number>(),
          };
          for (let i = 0; i < depth; i++) {
            const sibling = window(join(leaf(), leaf()));
            const right = ((bits >>> i) & 1) !== 0;
            root =
              i % 3 === 0
                ? join(right ? sibling : root, right ? root : sibling)
                : i % 3 === 1
                  ? window(root)
                  : except(right ? sibling : root, right ? root : sibling);
            const kind = i % 3 === 0 ? "joins" : i % 3 === 1 ? "windows" : "excepts";
            expected[kind].add(root.id);
            reach.reach(
              i % 3 === 0 ? "join ancestor" : i % 3 === 1 ? "window ancestor" : "EXCEPT ancestor",
            );
            reach.reach(right ? "right path" : "left path");
          }
          const result = phaseRetainingParents(root);
          expect(result.supported.has(target.id)).toBe(true);
          for (const kind of ["joins", "windows", "excepts"] as const)
            expect(result[kind].get(target.id)).toEqual(expected[kind]);
          reach.reach(aggregate ? "aggregate producer" : "JSON producer");
          reach.reach("descendant excluded");
          reach.reach("sibling excluded");
          if (depth === 0) reach.reach("no retaining ancestor");
        },
      ),
      {
        seed,
        numRuns: 48,
        examples: [
          [0, 0, false],
          [0, 0, true],
          [6, 0, false],
          [6, 127, false],
          [6, 0, true],
          [6, 127, true],
        ],
      },
    );
    reach.assertReached();
  },
);
