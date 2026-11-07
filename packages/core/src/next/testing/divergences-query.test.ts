import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { queryAuthorityValue } from "./divergences.js";

const FLOAT: PhysicalType = { kind: "float64" };
const INT: PhysicalType = { kind: "int32", bits: 32 };
const NUMERIC: PhysicalType = { kind: "decimal", precision: null, scale: null };
const cases = [
  "direct",
  "projected",
  "barriers",
  "sum",
  "numeric",
  "computed",
  "wrong-column",
  "window",
  "window-numeric",
  "window-sum",
] as const;
it("applies C11 only to a direct integer AVG output, never tolerating a different float", () => {
  const reach = createReach("C11 result lineage", cases);
  for (const seed of seedsFor("next-executor-c11", [0xc111, 0xc112]))
    fc.assert(
      fc.property(
        fc.constantFrom(...cases),
        fc.integer({ min: -100000, max: 100000 }),
        (variant, coefficient) => {
          reach.beginHistory();
          reach.reach(variant);
          const inputType = variant === "numeric" || variant === "window-numeric" ? NUMERIC : INT;
          const column = (index: number, type: PhysicalType): ScalarExpr => ({
            op: "column",
            index,
            type,
          });
          const output = [
            { name: "key", type: INT, nullable: false },
            { name: "average", type: FLOAT, nullable: true },
          ];
          const input: Rel = {
            id: 1,
            op: "values",
            output: [{ name: "v", type: inputType, nullable: true }],
            rows: [],
          };
          let root: Rel = {
            id: 2,
            op: "aggregate",
            input,
            output,
            groupBy: [0],
            groupingSets: null,
            aggregates: [
              {
                fn: variant === "sum" ? "sum" : "avg",
                args: [column(0, inputType)],
                type: FLOAT,
                filter: null,
                orderBy: [],
                distinct: false,
              },
            ],
          };
          if (variant.startsWith("window"))
            root = {
              op: "window",
              id: 2,
              input,
              output,
              partitionBy: [],
              orderBy: [],
              functions: [
                {
                  fn: variant === "window-sum" ? "sum" : "avg",
                  args: [column(0, inputType)],
                  frame: null,
                  type: FLOAT,
                },
              ],
            };
          let position = variant === "wrong-column" ? 0 : 1;
          if (variant === "projected" || variant === "computed") {
            root = {
              id: 3,
              op: "project",
              input: root,
              output: [{ name: "average", type: FLOAT, nullable: true }],
              exprs: [
                variant === "computed"
                  ? { op: "call", fn: "neg", args: [column(1, FLOAT)], type: FLOAT }
                  : column(1, FLOAT),
              ],
            };
            position = 0;
          }
          if (variant === "barriers")
            root = {
              id: 5,
              op: "limit",
              input: {
                id: 4,
                op: "sort",
                input: root,
                output,
                keys: [],
                limit: null,
                offset: null,
                withTies: false,
              },
              output,
              limit: null,
              offset: null,
            };
          const plan: PhysicalPlan = {
            root,
            columns: root.output,
            parameters: [],
            placeholderCount: 0,
            clockSlot: null,
            initPlans: [],
            catalogVersion: 1,
            volatile: false,
          };
          const text = (coefficient / 100).toFixed(2),
            changed = ["direct", "projected", "barriers", "window"].includes(variant);
          expect(queryAuthorityValue(plan, position, text)).toBe(changed ? Number(text) : text);
          expect(queryAuthorityValue(plan, position, "NaN")).toBe("NaN");
          expect(queryAuthorityValue(plan, position, null)).toBeNull();
          if (changed)
            expect(Object.is(queryAuthorityValue(plan, position, text), Number(text) + 0.125)).toBe(
              false,
            );
        },
      ),
      { seed, numRuns: 200 },
    );
  reach.assertReached();
});
