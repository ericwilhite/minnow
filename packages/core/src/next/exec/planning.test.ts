/** Planning may resolve metadata, but execution's working pools need actual admission. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import type { PhysicalType } from "../types.js";
import type { AggregateCall, PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import { Governor } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { AggregateOperator } from "./agg/aggregate.js";
import { createExecutor } from "./create.js";
import { bindCall } from "./kernels/bindings.js";
import { compileExpr, type CompileContext } from "./kernels/compile.js";
import { OutputArena } from "./kernels/vectors.js";
import {
  I32,
  I64,
  F64,
  F32,
  BOOL,
  TEXT,
  DATE,
  TS,
  decimal,
  numbers,
} from "./kernels/test-support.js";
import { nullParameter } from "./parameters.js";
import { measureCompilerBuffers } from "./plan.js";
import type { Operator } from "./pipeline.js";

const SINK: Operator = {
  push: () => "ready",
  ready: () => Promise.resolve(),
  finish: () => undefined,
  abort: () => undefined,
};
const TYPES = {
  i32: I32,
  i64: I64,
  f64: F64,
  f32: F32,
  decimal: decimal(12, 2),
  variableDecimal: decimal(null, null),
  bool: BOOL,
  text: TEXT,
  date: DATE,
  timestamp: TS,
};
const SHAPES = [
  "column",
  "param",
  "case",
  "coalesce",
  "nullif",
  "compare",
  "inList",
  "logic",
  "arithmetic",
  "cast",
] as const;
const MODES = ["kernel", "scalarAggregate", "groupedAggregate"] as const;

it("bounds actual batch-sized kernel/state buffers from small compiler probes", () => {
  const reach = createReach("compiler minimum", [...Object.keys(TYPES), ...SHAPES, ...MODES]);
  for (const seed of seedsFor("next-executor-planning", [0xb001, 0xb002]))
    fc.assert(
      fc.property(
        fc.constantFrom(...(Object.keys(TYPES) as Array<keyof typeof TYPES>)),
        fc.constantFrom(...SHAPES),
        fc.constantFrom(...MODES),
        (kind, shape, mode) => {
          reach.beginHistory();
          reach.reach(kind);
          reach.reach(shape);
          reach.reach(mode);
          const type = TYPES[kind],
            input = [type, type];
          const col = (index: number): ScalarExpr => ({ op: "column", index, type });
          const param: ScalarExpr = { op: "param", slot: 0, type };
          const comparison: ScalarExpr = {
            op: "compare",
            cmp: "=",
            left: col(0),
            right: col(1),
            collation: null,
            type: BOOL,
          };
          const numeric = ["int32", "int64", "float32", "float64", "decimal"].includes(type.kind);
          let expr: ScalarExpr;
          switch (shape) {
            case "column":
              expr = col(0);
              break;
            case "param":
              expr = param;
              break;
            case "case":
              expr = {
                op: "case",
                branches: [{ when: comparison, then: col(0) }],
                otherwise: param,
                type,
              };
              break;
            case "coalesce":
            case "nullif":
              expr = { op: "call", fn: shape, args: [col(0), param], type };
              break;
            case "compare":
              expr = comparison;
              break;
            case "inList":
              expr = {
                op: "inList",
                negated: false,
                arg: col(0),
                list: [col(1), param],
                collation: null,
                type: BOOL,
              };
              break;
            case "logic":
              expr = {
                op: "and",
                args: [comparison, { op: "not", arg: comparison, type: BOOL }],
                type: BOOL,
              };
              break;
            case "arithmetic":
              expr = numeric
                ? {
                    op: "call",
                    fn: "mul",
                    args: [{ op: "call", fn: "add", args: [col(0), param], type }, col(1)],
                    type,
                  }
                : col(0);
              break;
            case "cast":
              expr = { op: "cast", input: col(0), type: numeric ? F64 : type };
              break;
          }
          const aggregate: AggregateCall = {
            fn: type.kind === "bool" ? "bool_and" : "min",
            args: [expr],
            type: expr.type,
            distinct: false,
            filter: null,
            orderBy: [],
          };
          // A comparison's output, rather than its original input, is what MIN would see.
          // bool_and is the PostgreSQL aggregate for boolean inputs.
          const call =
            expr.type.kind === "bool" ? { ...aggregate, fn: "bool_and" as const } : aggregate;
          const rel: Extract<Rel, { op: "aggregate" }> = {
            id: 2,
            op: "aggregate",
            input: {
              id: 1,
              op: "values",
              output: input.map((type, k) => ({ name: `c${String(k)}`, type, nullable: true })),
              rows: [],
            },
            output: [
              ...(mode === "groupedAggregate" ? [{ name: "key", type, nullable: true }] : []),
              { name: "value", type: expr.type, nullable: true },
            ],
            groupBy: mode === "groupedAggregate" ? [0] : [],
            groupingSets: null,
            aggregates: [call],
          };
          const setup = (context: CompileContext, charge: ByteCharge) =>
            mode === "kernel"
              ? compileExpr(expr, context)
              : new AggregateOperator(rel, context, charge, SINK);
          const dispose = (value: ReturnType<typeof setup>) => {
            if (value instanceof AggregateOperator) value.abort(undefined);
          };
          const estimated = measureCompilerBuffers(
            { parameters: [type], resolveCall: bindCall },
            input,
            BATCH_ROWS,
            setup,
            dispose,
          );
          let held = 0;
          const charge = {
            allocate: (bytes: number) => {
              held += bytes;
              return true;
            },
            free: (bytes: number) => {
              held -= bytes;
            },
          };
          const arena = new OutputArena(charge, BATCH_ROWS);
          try {
            const value = setup({ arena, input, params: { slots: [nullParameter(type)] } }, charge);
            const actual = held;
            expect(estimated, `${kind}/${shape}/${mode}`).toBeGreaterThanOrEqual(actual);
            expect(estimated, `${kind}/${shape}/${mode}`).toBeLessThanOrEqual(actual + 256);
            dispose(value);
          } finally {
            arena.close();
          }
          expect(held).toBe(0);
        },
      ),
      { seed, numRuns: 300 },
    );
  reach.assertReached();
});

it("prepares a join/window/sort graph without admitting execution grants", () => {
  const output = [{ name: "v", type: I32, nullable: false }];
  const value = (id: number): Rel => ({
    id,
    op: "values",
    output,
    rows: [[{ op: "literal", value: numbers(I32, [1]), type: I32 }]],
  });
  const col = (index: number, type: PhysicalType): ScalarExpr => ({ op: "column", index, type });
  const join: Rel = {
    id: 3,
    op: "join",
    left: value(1),
    right: value(2),
    output: [...output, { ...output[0], name: "w", type: I32, nullable: false }],
    algorithm: "hash",
    kind: "inner",
    leftKeys: [col(0, I32)],
    rightKeys: [col(0, I32)],
    nullSafe: [false],
    residual: null,
    publishes: [],
  };
  const window: Rel = {
    id: 4,
    op: "window",
    input: join,
    output: [...join.output, { name: "rn", type: I64, nullable: false }],
    partitionBy: [],
    orderBy: [],
    functions: [{ fn: "row_number", args: [], frame: null, type: I64 }],
  };
  const root: Rel = {
    id: 5,
    op: "sort",
    input: window,
    output: window.output,
    keys: [{ expr: col(2, I64), direction: "asc", nulls: "last", collation: null }],
    limit: null,
    offset: null,
    withTies: false,
  };
  const plan: PhysicalPlan = {
    root,
    columns: root.output,
    parameters: [],
    placeholderCount: 0,
    clockSlot: null,
    initPlans: [],
    catalogVersion: 0,
    volatile: false,
  };
  const admit = vi.spyOn(Governor.prototype, "tryAdmit");
  try {
    createExecutor().prepare(plan);
    expect(admit).not.toHaveBeenCalled();
  } finally {
    admit.mockRestore();
  }
});
