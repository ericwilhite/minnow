/** Plan cardinalities must not become JavaScript call-argument cardinalities. */
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, EXEC_CALL_LIST_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { expressionInputBytes } from "./input-size-plan.js";
import { prepareParameters } from "./parameters.js";
import { jsonGroupWorkspaceMultiplier } from "./agg/json-group-keys.js";
import { aggregateLaneBytes, type AggregateProgressLayout } from "./agg/parameter-copies.js";
import { FUNCTIONS } from "../ir/functions.js";

const text: PhysicalType = { kind: "text" };
const empty: Rel = { op: "values", id: 1, output: [], rows: [] };
const arg: ScalarExpr = { op: "param", slot: 0, type: text };
/** An expression with no size rule (a text function: no kernel sizes it yet). A cast to text is
 * bounded from its type now, so it no longer serves here. */
const unknown: ScalarExpr = { op: "call", fn: "replace", args: [arg, arg, arg], type: text };

for (const seed of seedsFor("next-exec-call-lists", [47409, 47410]))
  it(`sizes list cardinalities without a native argument limit seed ${String(seed)}`, async () => {
    const reach = createReach("executor call lists", EXEC_CALL_LIST_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(fc.integer({ min: 0, max: 8192 }), fc.integer({ min: 150000, max: 220000 })),
        fc.integer({ min: 1, max: 64 }),
        async (length, width) => {
          reach.beginHistory();
          reach.reach(
            length === 0 ? "empty" : length > 131072 ? "above native argument limit" : "ordinary",
          );
          const args = Array<ScalarExpr>(length).fill(arg);
          const sizes = { parameter: () => width, source: () => 0 };
          const coalesce: ScalarExpr = { op: "call", fn: "coalesce", args, type: text };
          expect(expressionInputBytes(coalesce, empty, sizes)).toBe(length === 0 ? 0 : width);
          if (length > 0) {
            // An unsized operand makes the entire maximum unknown, even after known operands.
            args[length - 1] = unknown;
            expect(expressionInputBytes(coalesce, empty, sizes)).toBeNull();
          }
          reach.reach("nullable maximum");
          const parameters = Array<PhysicalType>(length).fill({ kind: "int32", bits: 32 });
          const values: Array<number | string> = Array<number>(length).fill(0);
          if (length > 0) {
            parameters[0] = text;
            values[0] = "é".repeat(width);
          }
          const plan: PhysicalPlan = {
            root: empty,
            columns: [],
            parameters,
            placeholderCount: length,
            clockSlot: null,
            initPlans: [],
            catalogVersion: 1,
            volatile: false,
          };
          const bound = await prepareParameters(plan, values);
          // Per-slot extents describe retained UTF8, not the shared temporary UTF16 slice.
          const maximum = length === 0 ? 0 : 512 + 2 * width;
          expect(bound.largestSlot).toBe(maximum);
          expect(bound.logicalBytes).toBe(length === 0 ? 0 : 512 * (length - 1) + maximum);
          expect(bound.bytes).toBe(
            length === 0 ? 0 : 512 * (length - 1) + maximum + 2 * width + 24,
          );
          expect(bound.slotBytes).toHaveLength(length);
          reach.reach("parameter maximum and sum");
          const keys = Array<number>(length).fill(0);
          expect(jsonGroupWorkspaceMultiplier([{ kind: "json", binary: true }], keys)).toBe(
            length === 0 ? 1 : 13,
          );
          reach.reach("structural key maximum");
          const call = {
            width: { op: "bytes" as const, bytes: width },
            variable: true,
            distinct: false,
            minimum: 0,
          };
          const layout: AggregateProgressLayout = {
            simple: false,
            input: [],
            keys: [],
            calls: Array<typeof call>(length).fill(call),
          };
          const lane = aggregateLaneBytes(layout, "external", 1, 0, []);
          expect(lane).toBe(
            aggregateLaneBytes(
              { ...layout, calls: length === 0 ? [] : [call] },
              "external",
              1,
              0,
              [],
            ),
          );
          reach.reach("aggregate lane maximum");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [0, 1],
          [1, 64],
          [180000, 7],
        ],
      },
    );
    reach.assertReached();
  });

const next = join(import.meta.dirname, "..");
const layers = [
  ".",
  ...readdirSync(next, { withFileTypes: true })
    .filter((e) => e.isDirectory())
    .map((e) => e.name),
];
it.each(layers)("keeps only proven bounded call spreads in rebuild layer %s", (layer) => {
  // The finite overload registry cannot grow this exception without a new bound or a loop.
  for (const overloads of Object.values(FUNCTIONS))
    expect(overloads.length).toBeLessThanOrEqual(32);
  // FTS strings <=128 units, metadata read scopes <=6 frames, one-page append returns
  // one blob, chaos fixtures seal <=12/8 pages, crash fixture inserts exactly8 IDs.
  // The remaining adapters forward fixed method tuples, not caller-sized lists.
  const bounded = new Map([
    ["exec/kernels/fts.ts", new Set(["String.fromCharCode"])],
    ["exec/late-fetch.ts", new Set(["this.source.chunk", "this.source.readHint"])],
    ["format/fts-append.ts", new Set(["String.fromCharCode"])],
    ["format/fts-codec.ts", new Set(["String.fromCharCode"])],
    ["lower/coerce.ts", new Set(["Math.max"])],
    ["storage/metadata-journal.ts", new Set(["this.#frames.push"])],
    ["storage/extents.examples.ts", new Set(["pending.push"])],
    ["spill/chaos-workload.ts", new Set(["written.push"])],
    ["write/crash-sweep.test-support.ts", new Set(["kept.push"])],
    ["write/engine.ts", new Set(["write"])],
    ["owner/fts-transaction.test-support.ts", new Set(["open"])],
    ["write/directory-publication-compat.test-support.ts", new Set(["begin"])],
    ["testing/json-group-generated-lifetime.test.fixture.ts", new Set(["decode"])],
    // The snapshot corpus draws one of its seven fixed mutation kinds (`MUTATIONS`).
    ["testing/grammar-corpus.ts", new Set(["fc.constantFrom"])],
  ]);
  const unbounded: string[] = [];
  function walk(directory: string, descend = true) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) {
        if (descend) walk(path);
        continue;
      }
      if (!path.endsWith(".ts") || /\.(test|bench)\.ts$/.test(path)) continue;
      const source = ts.createSourceFile(
        path,
        readFileSync(path, "utf8"),
        ts.ScriptTarget.Latest,
        true,
      );
      function visit(node: ts.Node) {
        if (
          (ts.isCallExpression(node) || ts.isNewExpression(node)) &&
          node.arguments?.some(ts.isSpreadElement)
        ) {
          const name = relative(next, path).replaceAll("\\", "/");
          if (!bounded.get(name)?.has(node.expression.getText(source)))
            unbounded.push(
              `${name}:${String(source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1)}`,
            );
        }
        ts.forEachChild(node, visit);
      }
      visit(source);
    }
  }
  // Root files are checked once; separate layer bodies keep coverage work bounded.
  walk(join(next, layer), layer !== ".");
  expect(unbounded).toEqual([]);
});
