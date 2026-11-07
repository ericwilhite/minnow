import { expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import type { Snapshot } from "../access/snapshot.js";
import type { OutputColumn, PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { oracleRows } from "./differential-adapter.js";
import type { PhysicalType } from "../types.js";
import type { Batch, FlatVector } from "../vector/vector.js";
import { createExecutor } from "../exec/create.js";
import { UnsupportedPlanError } from "../exec/errors.js";
import type { ExecuteRequest, ExecutionSummary } from "../exec/executor.js";
import {
  I32,
  I64,
  F64,
  BOOL,
  TEXT,
  numbers,
  texts,
  bigints,
} from "../exec/kernels/test-support.js";
import { beginTask } from "../exec/pipeline.js";
import type { ChunkReads } from "../exec/scan.js";

const column = (index: number, type: PhysicalType): ScalarExpr => ({ op: "column", index, type });
const literal = (value: FlatVector): ScalarExpr => ({ op: "literal", value, type: value.type });
let id = 0;
function output(input: readonly PhysicalType[]): OutputColumn[] {
  return input.map((type, k) => ({ name: `c${String(k)}`, type, nullable: true }));
}
function values(
  rows: ReadonlyArray<ReadonlyArray<number | null>>,
  input: readonly PhysicalType[] = [I32],
): Rel {
  return {
    id: ++id,
    op: "values",
    output: output(input),
    rows: rows.map((row) =>
      row.map((value, k) => {
        const type = input[k];
        if (type === undefined) throw new Error("missing input type");
        return literal(numbers(type, [value]));
      }),
    ),
  };
}
function plan(root: Rel): PhysicalPlan {
  return {
    root,
    columns: root.output,
    parameters: [],
    placeholderCount: 0,
    clockSlot: null,
    initPlans: [],
    catalogVersion: new MemoryCatalog().view.version,
    volatile: false,
  };
}
async function run(
  root: Rel,
  cap = 64 * 1024 * 1024,
  inspect?: (summary: ExecutionSummary) => void,
) {
  const p = plan(root),
    prepared = createExecutor().prepare(p),
    governor = new Governor(cap);
  const snapshot = {
    catalog: { version: p.catalogVersion },
    observedLsn: 0,
  } as unknown as Snapshot;
  const request: ExecuteRequest = {
    snapshot,
    statement: null,
    parameters: prepared.bind([]),
    statementTime: 0n,
    reads: {} as ChunkReads,
    governor,
    spill: new MemorySpillService(),
    queryId: 1,
    sequences: null,
    sequenceSession: null,
    returning: "atExecuted",
    resultWindowBytes: 1024,
  };
  beginTask();
  const execution = prepared.execute(request);
  const batches: Batch[] = [];
  for (;;) {
    const frame = await execution.results.next();
    if (frame === null) break;
    batches.push({
      length: frame.payload.rowCount,
      selection: null,
      columns: frame.payload.columns.map(
        (vector, k) => ({ ...vector, type: p.columns[k]?.type }) as FlatVector,
      ),
    });
    frame.release();
  }
  const summary = await execution.executed;
  inspect?.(summary);
  await execution.delivered;
  expect(governor.usedBytes).toBe(0);
  expect(governor.peakBytes).toBeLessThanOrEqual(cap);
  return oracleRows({ columns: p.columns, batches });
}

it("builds a values/filter/project/offset/limit pipeline", async () => {
  const input = values([[0], [1], [2], [3], [4]]);
  const filter: Rel = {
    id: ++id,
    op: "filter",
    input,
    output: input.output,
    predicate: {
      op: "compare",
      cmp: ">",
      left: column(0, I32),
      right: literal(numbers(I32, [0])),
      collation: null,
      type: BOOL,
    },
  };
  const project: Rel = {
    id: ++id,
    op: "project",
    input: filter,
    output: filter.output,
    exprs: [
      { op: "call", fn: "add", args: [column(0, I32), literal(numbers(I32, [10]))], type: I32 },
    ],
  };
  const root: Rel = {
    id: ++id,
    op: "limit",
    input: project,
    output: project.output,
    offset: { kind: "const", value: 1 },
    limit: { kind: "const", value: 2 },
  };
  expect(await run(root)).toEqual([{ c0: 12 }, { c0: 13 }]);
});

it("skips unopened input and failing expressions for LIMIT zero", async () => {
  const bad: ScalarExpr = {
    op: "call",
    fn: "div",
    args: [literal(numbers(I32, [1])), literal(numbers(I32, [0]))],
    type: I32,
  };
  const input: Rel = { id: ++id, op: "values", output: output([I32]), rows: [[bad]] };
  expect(
    await run({
      id: ++id,
      op: "limit",
      input,
      output: input.output,
      limit: { kind: "const", value: 0 },
      offset: null,
    }),
  ).toEqual([]);
});

it("runs hash aggregation and a computed-key top N", async () => {
  const input = values(
    [
      [2, 3],
      [1, 4],
      [2, 5],
      [1, null],
    ],
    [I32, I32],
  );
  const aggregate: Rel = {
    id: ++id,
    op: "aggregate",
    input,
    groupBy: [0],
    groupingSets: null,
    output: output([I32, I64]),
    aggregates: [
      { fn: "sum", args: [column(1, I32)], distinct: false, filter: null, orderBy: [], type: I64 },
    ],
  };
  const root: Rel = {
    id: ++id,
    op: "sort",
    input: aggregate,
    output: aggregate.output,
    keys: [
      {
        expr: { op: "call", fn: "neg", args: [column(1, I64)], type: I64 },
        direction: "asc",
        nulls: "last",
        collation: null,
      },
    ],
    limit: { kind: "const", value: 1 },
    offset: null,
    withTies: false,
  };
  expect(await run(root)).toEqual([{ c0: 2, c1: 8 }]);
});

it("maps computed join keys and residuals back to original left/right columns", async () => {
  const left = values(
      [
        [1, 8],
        [2, 4],
      ],
      [I32, I32],
    ),
    right = values(
      [
        [2, 9],
        [3, 2],
      ],
      [I32, I32],
    );
  const root: Rel = {
    id: ++id,
    op: "join",
    algorithm: "hash",
    kind: "left",
    left,
    right,
    leftKeys: [
      { op: "call", fn: "add", args: [column(0, I32), literal(numbers(I32, [1]))], type: I32 },
    ],
    rightKeys: [column(0, I32)],
    nullSafe: [false],
    publishes: [],
    residual: {
      op: "compare",
      cmp: "<",
      left: column(1, I32),
      right: column(3, I32),
      collation: null,
      type: BOOL,
    },
    output: output([I32, I32, I32, I32]),
  };
  expect(await run(root)).toEqual([
    { c0: 1, c1: 8, c2: 2, c3: 9 },
    { c0: 2, c1: 4, c2: null, c3: null },
  ]);
});

it("runs a window with a computed argument and strips hidden columns", async () => {
  const input = values(
    [
      [1, 4],
      [1, 2],
      [2, 9],
    ],
    [I32, I32],
  );
  const root: Rel = {
    id: ++id,
    op: "window",
    input,
    partitionBy: [0],
    orderBy: [{ expr: column(1, I32), direction: "asc", nulls: "last", collation: null }],
    functions: [
      { fn: "row_number", args: [], frame: null, type: I64 },
      {
        fn: "sum",
        args: [{ op: "cast", input: column(1, I32), type: F64 }],
        frame: null,
        type: F64,
      },
    ],
    output: output([I32, I32, I64, F64]),
  };
  expect(await run(root, 8 * 1024 * 1024)).toEqual([
    { c0: 1, c1: 2, c2: 1, c3: 2 },
    { c0: 1, c1: 4, c2: 2, c3: 6 },
    { c0: 2, c1: 9, c2: 1, c3: 9 },
  ]);
});

it("bounds enormous ROWS/GROUPS offsets by the finite partition and refuses inexact RANGE at prepare", async () => {
  const input = values([[1], [1], [2]]);
  const offset = literal(bigints(I64, [9223372036854775807n]));
  const window = (unit: "rows" | "groups" | "range", offset: ScalarExpr): Rel => ({
    id: ++id,
    op: "window",
    input,
    output: output([I32, I64]),
    partitionBy: [],
    orderBy: [{ expr: column(0, I32), direction: "asc", nulls: "last", collation: null }],
    functions: [
      {
        fn: "sum",
        args: [column(0, I32)],
        type: I64,
        frame: {
          unit,
          start: { kind: "preceding", offset },
          end: { kind: "current-row", offset: null },
          exclude: "no-others",
        },
      },
    ],
  });
  expect(await run(window("rows", offset))).toEqual([
    { c0: 1, c1: 1 },
    { c0: 1, c1: 2 },
    { c0: 2, c1: 4 },
  ]);
  expect(await run(window("groups", offset))).toEqual([
    { c0: 1, c1: 2 },
    { c0: 1, c1: 2 },
    { c0: 2, c1: 4 },
  ]);
  expect(() => createExecutor().prepare(plan(window("range", offset)))).toThrow(
    UnsupportedPlanError,
  );
  expect(() =>
    createExecutor().prepare({
      ...plan(window("range", { op: "param", slot: 0, type: I64 })),
      parameters: [I64],
      placeholderCount: 1,
    }),
  ).toThrow(UnsupportedPlanError);
  expect(() => createExecutor().prepare(plan(window("rows", literal(texts(["1"])))))).toThrow(
    UnsupportedPlanError,
  );
});

it("spills a real join pipeline and admits its reserved probe after the build fills memory", async () => {
  const left = values([[5]]),
    payload = "payload-".repeat(96);
  const right: Rel = {
    id: ++id,
    op: "values",
    output: output([I32, TEXT]),
    rows: Array.from({ length: 12000 }, (_, key) => [
      literal(numbers(I32, [key])),
      literal(texts([payload])),
    ]),
  };
  const root: Rel = {
    id: ++id,
    op: "join",
    algorithm: "hash",
    kind: "inner",
    left,
    right,
    leftKeys: [column(0, I32)],
    rightKeys: [column(0, I32)],
    nullSafe: [false],
    residual: null,
    publishes: [],
    output: output([I32, I32, TEXT]),
  };
  expect(
    await run(root, 8 * 1024 * 1024, (summary) => {
      expect(
        summary.profile.operators.find((profile) => profile.relId === root.id)?.spilledBytes,
      ).toBeGreaterThan(0);
    }),
  ).toEqual([{ c0: 5, c1: 5, c2: payload }]);
}, 30_000);

it("keeps sort and window spilling and cleanup active through the factory", async () => {
  const payload = literal(texts(["payload-".repeat(96)]));
  const input: Rel = {
    id: ++id,
    op: "values",
    output: output([I32, TEXT]),
    rows: Array.from({ length: 12000 }, (_, key) => [literal(numbers(I32, [key])), payload]),
  };
  const keys = [
    { expr: column(0, I32), direction: "asc" as const, nulls: "last" as const, collation: null },
  ];
  const sort: Rel = {
    id: ++id,
    op: "sort",
    input,
    output: input.output,
    keys,
    limit: null,
    offset: null,
    withTies: false,
  };
  const window: Rel = {
    id: ++id,
    op: "window",
    input,
    output: output([I32, TEXT, I64]),
    partitionBy: [],
    orderBy: keys,
    functions: [{ fn: "row_number", args: [], frame: null, type: I64 }],
  };
  for (const root of [sort, window]) {
    const rows = await run(root, 8 * 1024 * 1024, (summary) => {
      expect(
        summary.profile.operators.find((profile) => profile.relId === root.id)?.spilledBytes,
      ).toBeGreaterThan(0);
    });
    expect(rows).toHaveLength(12000);
    expect(rows[0]?.c0).toBe(0);
    expect(rows.at(-1)?.c0).toBe(11999);
    if (root.op === "window") {
      expect(rows[0]?.c2).toBe(1);
      expect(rows.at(-1)?.c2).toBe(12000);
    }
  }
}, 30_000);

it("refuses unsupported nodes and kernels before execution", () => {
  const input = values([[1]]);
  expect(() =>
    createExecutor().prepare(
      plan({
        id: ++id,
        op: "setOp",
        kind: "union",
        all: true,
        inputs: [input],
        output: input.output,
      }),
    ),
  ).toThrow(UnsupportedPlanError);
  expect(() =>
    createExecutor().prepare(
      plan({
        id: ++id,
        op: "project",
        input,
        output: input.output,
        exprs: [{ op: "call", fn: "not_a_kernel", args: [], type: I32 }],
      }),
    ),
  ).toThrow(UnsupportedPlanError);
  expect(() =>
    createExecutor().prepare(
      plan({
        id: ++id,
        op: "window",
        input,
        output: output([I32, I64]),
        partitionBy: [],
        orderBy: [],
        functions: [{ fn: "not_a_window", args: [], frame: null, type: I64 }],
      }),
    ),
  ).toThrow(UnsupportedPlanError);
});

it("keeps kernel buffers and parameter slots local to concurrent executions of one prepared plan", async () => {
  const input = values(Array.from({ length: 20 }, (_, k) => [k]));
  const root: Rel = {
    id: ++id,
    op: "project",
    input,
    output: input.output,
    exprs: [
      {
        op: "call",
        fn: "add",
        args: [column(0, I32), { op: "param", slot: 0, type: I32 }],
        type: I32,
      },
    ],
  };
  const p = { ...plan(root), parameters: [I32], placeholderCount: 1 },
    prepared = createExecutor().prepare(p);
  const governor = new Governor(16 * 1024 * 1024);
  const start = (value: number, queryId: number) =>
    prepared.execute({
      snapshot: { catalog: { version: p.catalogVersion }, observedLsn: 0 } as unknown as Snapshot,
      statement: null,
      parameters: prepared.bind([value]),
      statementTime: 0n,
      governor,
      reads: {} as ChunkReads,
      spill: new MemorySpillService(),
      queryId,
      sequences: null,
      sequenceSession: null,
      returning: "atExecuted",
      resultWindowBytes: 4,
    });
  beginTask();
  const first = start(100, 1),
    second = start(200, 2);
  const drain = async (execution: ReturnType<typeof start>) => {
    const out: number[] = [];
    for (;;) {
      const frame = await execution.results.next();
      if (frame === null) break;
      const vector = frame.payload.columns[0];
      if (vector?.form !== "flat" || vector.data.layout !== "i32")
        throw new Error("unexpected frame");
      out.push(...vector.data.values.subarray(0, frame.payload.rowCount));
      frame.release();
    }
    await execution.executed;
    await execution.delivered;
    return out;
  };
  const rows = await Promise.all([drain(first), drain(second)]);
  expect(rows).toEqual([
    Array.from({ length: 20 }, (_, k) => 100 + k),
    Array.from({ length: 20 }, (_, k) => 200 + k),
  ]);
  expect(governor.usedBytes).toBe(0);
});

it("counts driven pipelines independently of reserved grants and skipped inputs", async () => {
  const input = values([[1]]);
  await run(input, 8 * 1024 * 1024, (summary) => expect(summary.profile.pipelines).toBe(1));
  await run(
    {
      id: ++id,
      op: "limit",
      input,
      output: input.output,
      limit: { kind: "const", value: 0 },
      offset: null,
    },
    8 * 1024 * 1024,
    (summary) => expect(summary.profile.pipelines).toBe(0),
  );
});

it("reserves no unopened window subtrees beneath LIMIT zero", async () => {
  const reach = createReach("zero limit admission", [
    "constant",
    "parameter",
    "sort",
    "plain limit",
    "projection failure",
    "order failure",
    "offset",
    "unopened windows",
  ] as const);
  for (const seed of seedsFor("next-zero-limit-admission", [0x1011, 0x1012]))
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 5 }),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom("none", "projection", "order"),
        fc.integer({ min: 0, max: 12 }),
        async (depth, parameter, sorted, failure, offset) => {
          reach.beginHistory();
          let root = values([[0], [1]]);
          const bad: ScalarExpr = {
            op: "call",
            fn: "div",
            args: [literal(numbers(I32, [1])), column(0, I32)],
            type: I32,
          };
          if (failure === "projection")
            root = { id: ++id, op: "project", input: root, output: root.output, exprs: [bad] };
          for (let k = 0; k < depth; k++)
            root = {
              id: ++id,
              op: "window",
              input: root,
              output: output([...root.output.map((c) => c.type), I64]),
              partitionBy: [],
              orderBy: [{ expr: column(0, I32), direction: "asc", nulls: "last", collation: null }],
              functions: [{ fn: "row_number", args: [], frame: null, type: I64 }],
            };
          const common = {
            id: ++id,
            input: root,
            output: root.output,
            limit: parameter
              ? { kind: "param" as const, slot: 0 }
              : { kind: "const" as const, value: 0 },
            offset: { kind: "const" as const, value: offset },
          };
          root = sorted
            ? {
                ...common,
                op: "sort",
                keys: [
                  {
                    expr: failure === "order" ? bad : column(0, I32),
                    direction: "asc",
                    nulls: "last",
                    collation: null,
                  },
                ],
                withTies: false,
              }
            : { ...common, op: "limit" };
          const physical = plan(root);
          const prepared = createExecutor().prepare({
            ...physical,
            parameters: parameter ? [I64] : [],
            placeholderCount: parameter ? 1 : 0,
          });
          const governor = new Governor(8 * 1024 * 1024);
          const execution = prepared.execute({
            snapshot: {
              catalog: { version: physical.catalogVersion },
              observedLsn: 0,
            } as unknown as Snapshot,
            statement: null,
            parameters: prepared.bind(parameter ? [0] : []),
            statementTime: 0n,
            reads: {} as ChunkReads,
            governor,
            spill: new MemorySpillService(),
            queryId: 1,
            sequences: null,
            sequenceSession: null,
            returning: "atExecuted",
          });
          try {
            expect(await execution.results.next()).toBeNull();
            expect((await execution.executed).profile.pipelines).toBe(0);
            await execution.delivered;
          } finally {
            execution.cancel();
            await Promise.allSettled([execution.executed, execution.delivered]);
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach(parameter ? "parameter" : "constant");
          reach.reach(sorted ? "sort" : "plain limit");
          if (failure === "projection") reach.reach("projection failure");
          if (sorted && failure === "order") reach.reach("order failure");
          if (offset > 0) reach.reach("offset");
          if (depth >= 3) reach.reach("unopened windows");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [3, false, true, "projection", 1],
          [3, true, true, "order", 1],
          [3, false, false, "none", 0],
        ],
      },
    );
  reach.assertReached();
});

it("streams oversized rows through a spilled window and result credits", async () => {
  const length = 600000;
  const payload = literal(texts(["x".repeat(length)]));
  const input: Rel = {
    id: ++id,
    op: "values",
    output: output([I32, TEXT]),
    rows: Array.from({ length: 64 }, (_, key) => [literal(numbers(I32, [key])), payload]),
  };
  const root: Rel = {
    id: ++id,
    op: "window",
    input,
    output: output([I32, TEXT, I64]),
    partitionBy: [],
    orderBy: [{ expr: column(0, I32), direction: "asc", nulls: "last", collation: null }],
    functions: [{ fn: "row_number", args: [], frame: null, type: I64 }],
  };
  const rows = await run(root, 8 * 1024 * 1024, (summary) => {
    expect(
      summary.profile.operators.find((p) => p.relId === root.id)?.spilledBytes,
    ).toBeGreaterThan(0);
  });
  expect(rows).toHaveLength(64);
  expect(rows.map((row) => row.c0)).toEqual(Array.from({ length: 64 }, (_, i) => i));
  expect(rows.map((row) => row.c2)).toEqual(Array.from({ length: 64 }, (_, i) => i + 1));
  expect(rows.map((row) => String(row.c1).length)).toEqual(Array<number>(64).fill(length));
}, 30_000);
