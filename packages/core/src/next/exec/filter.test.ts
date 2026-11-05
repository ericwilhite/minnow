/**
 * The filter and project operators under the pipeline driver: selection vectors (no row copies),
 * conjunct narrowing (a later conjunct never sees a rejected row), null predicates rejected, empty
 * batches skipped, and incoming transfers held until downstream readiness.
 */
import { describe, expect, it } from "vitest";
import type { ScalarExpr } from "../ir/ir.js";
import type { Batch, FlatVector } from "../vector/vector.js";
import { FilterOperator, ProjectOperator, conjuncts } from "./filter.js";
import { OutputArena } from "./kernels/vectors.js";
import {
  BOOL,
  I32,
  UNCHARGED,
  call,
  cmp,
  col,
  lit,
  numbers,
  valueAt,
} from "./kernels/test-support.js";
import {
  drive,
  type BatchSource,
  type Operator,
  type PushResult,
  type SourceItem,
} from "./pipeline.js";

const and = (...args: ScalarExpr[]): ScalarExpr => ({ op: "and", args, type: BOOL });
const int = (value: number) => lit(numbers(I32, [value]));

function context() {
  return { arena: new OutputArena(UNCHARGED, 2048), params: { slots: [] }, input: [I32] };
}

function source(
  batches: ReadonlyArray<ReadonlyArray<number | null>>,
  transfers?: { released: number },
): BatchSource {
  let next = 0;
  return {
    next(): SourceItem | null {
      const values = batches[next++];
      if (values === undefined) return null;
      const batch: Batch = {
        length: values.length,
        columns: [numbers(I32, values)],
        selection: null,
      };
      return transfers === undefined
        ? { batch }
        : { batch, transfer: { release: () => (transfers.released += 1) } };
    },
    abort: () => undefined,
  };
}

class Collect implements Operator {
  readonly rows: unknown[][] = [];
  pushes = 0;
  push(batch: Batch): PushResult {
    this.pushes += 1;
    const count = batch.selection === null ? batch.length : batch.selection.count;
    for (let k = 0; k < count; k++) {
      const row = batch.selection === null ? k : (batch.selection.rows[k] ?? 0);
      this.rows.push(batch.columns.map((column) => valueAt(column, row)));
    }
    return "ready";
  }
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    // nothing buffered
  }
  abort(): void {
    // nothing held
  }
}

describe("filter and project", () => {
  it("flattens nested ANDs into conjuncts", () => {
    const a = cmp(">", col(0, I32), int(1));
    expect(conjuncts(and(a, and(a, a)))).toHaveLength(3);
  });

  it("narrows conjunct by conjunct: no division by a rejected row's zero", () => {
    const predicate = and(
      cmp("<>", col(0, I32), int(20)),
      cmp(">", call("div", I32, int(100), call("sub", I32, col(0, I32), int(20))), int(0)),
    );
    const sink = new Collect();
    const project = new ProjectOperator([call("mul", I32, col(0, I32), int(2))], context(), sink);
    const run = drive(
      source([[20, 21, 19, null, 70]]),
      new FilterOperator(predicate, context(), project),
      undefined,
      { now: () => 0 }, // a frozen clock: only a real Promise could make it async
    );
    expect(run).toBeUndefined(); // fully synchronous
    expect(sink.rows).toEqual([[42], [140]]);
  });

  it("drops null predicates, skips empty batches, passes all-true batches without a selection", () => {
    const sink = new Collect();
    let selections = 0;
    const spy: Operator = {
      push: (batch) => {
        if (batch.selection !== null) selections += 1;
        return sink.push(batch);
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    void drive(
      source([
        [1, 2],
        [null, 0],
        [5, null],
      ]),
      new FilterOperator(cmp(">", col(0, I32), int(0)), context(), spy),
    );
    expect(sink.rows).toEqual([[1], [2], [5]]);
    expect(sink.pushes).toBe(2);
    expect(selections).toBe(1);
  });

  it("rejects null predicate rows whatever their garbage says", () => {
    const sink = new Collect();
    const batch: Batch = { length: 3, columns: [numbers(I32, [1, null, 2], 99)], selection: null };
    const one: BatchSource = {
      next: (() => {
        let sent = false;
        return () => (sent ? null : ((sent = true), { batch }));
      })(),
      abort: () => undefined,
    };
    void drive(one, new FilterOperator(cmp(">", col(0, I32), int(0)), context(), sink));
    expect(sink.rows).toEqual([[1], [2]]);
  });

  it("holds an incoming transfer until downstream is ready", async () => {
    const transfers = { released: 0 };
    let unblock: () => void = () => undefined;
    let blocked = true;
    const slow: Operator = {
      push: () => (blocked ? "blocked" : "ready"),
      ready: () =>
        new Promise<void>((resolve) => {
          unblock = () => {
            blocked = false;
            resolve();
          };
        }),
      finish: () => undefined,
      abort: () => undefined,
    };
    const run = drive(
      source([[1], [2]], transfers),
      new FilterOperator(cmp(">", col(0, I32), int(0)), context(), slow),
    );
    expect(transfers.released).toBe(0);
    unblock();
    await run;
    expect(transfers.released).toBe(2);
  });

  it("reads parameters bound after compilation", () => {
    const params = { slots: [numbers(I32, [3])] as FlatVector[] };
    const sink = new Collect();
    const filter = new FilterOperator(
      cmp(">", col(0, I32), { op: "param", slot: 0, type: I32 }),
      { ...context(), params },
      sink,
    );
    void drive(source([[1, 5]]), filter);
    params.slots = [numbers(I32, [0])];
    void drive(source([[1, 5]]), filter);
    expect(sink.rows).toEqual([[5], [1], [5]]);
  });
});
