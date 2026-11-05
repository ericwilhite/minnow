/**
 * How the consuming tracks call the executor's entry point (exec/executor.ts):
 * - the owner (B5): prepare → bind → execute → pull frames, post them, release on credit → await
 *   `done` → commit → release the pin; cancellation; a stale plan;
 * - the write path (B3): the `ModifyTarget` call sequence a DML plan makes.
 *
 * `ToyExecutor` runs only `values` over int32 literals and parameters, plus `modify` insert over
 * them, so the protocol is visible. The Phase 2 executor replaces it behind the same interfaces.
 */
import { describe, expect, it } from "vitest";
import type { OutputColumn, PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import { Governor, type MemoryGovernor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { allocateSelection } from "../vector/validity.js";
import { allocateFlat, type Batch, type FlatVector, type Selection } from "../vector/vector.js";
import {
  ParameterBindError,
  PlanStaleError,
  QueryCancelledError,
  UnsupportedPlanError,
} from "./errors.js";
import type {
  BoundParameters,
  ExecuteRequest,
  Execution,
  ExecutionSummary,
  Executor,
  ParameterValue,
  PreparedPlan,
} from "./executor.js";
import { SnapshotReleasedError, type RowFetcher, type SnapshotPin } from "./fetch.js";
import type { ConflictMode, InsertResult, ModifyTarget } from "./modify.js";
import { ResultChannel, frameByteLength, frameTransferList, type ResultFrame } from "./result.js";
import type { QuerySnapshot } from "./snapshot.js";

const INT = { kind: "int32", bits: 32 } as const;
const TABLE = 7;

function i32(vector: FlatVector): Int32Array {
  if (vector.data.layout !== "i32") throw new TypeError("int32 expected");
  return vector.data.values;
}

// ---------------------------------------------------------------- the toy executor

class ToyExecutor implements Executor {
  prepare(plan: PhysicalPlan): PreparedPlan {
    const values = plan.root.op === "modify" ? plan.root.input : plan.root;
    if (values.op !== "values" || (plan.root.op === "modify" && plan.root.kind !== "insert"))
      throw new UnsupportedPlanError(`${plan.root.op} in the toy executor`);
    return new ToyPrepared(plan, values);
  }
}

class ToyPrepared implements PreparedPlan {
  readonly columns: readonly OutputColumn[];
  readonly kind: "query" | "modify";
  readonly usesSequences = false;
  constructor(
    readonly plan: PhysicalPlan,
    readonly values: Extract<Rel, { op: "values" }>,
  ) {
    this.columns = plan.columns;
    this.kind = plan.root.op === "modify" ? "modify" : "query";
  }

  bind(values: readonly ParameterValue[]): BoundParameters {
    if (values.length !== this.plan.placeholderCount)
      throw new ParameterBindError(
        `Expected ${String(this.plan.placeholderCount)} parameters`,
        null,
      );
    return {
      slots: this.plan.parameters.map((_, slot) => {
        const value = values[slot];
        if (!Number.isInteger(value))
          throw new ParameterBindError(`$${String(slot + 1)}: int`, slot);
        const vector = allocateFlat(INT, 1);
        i32(vector)[0] = value as number;
        vector.length = 1;
        return vector;
      }),
    };
  }

  execute(request: ExecuteRequest): Execution {
    const cancel = new AbortController();
    const results = new ResultChannel(this.columns, {
      ...(request.resultWindowBytes === undefined
        ? {}
        : { windowBytes: request.resultWindowBytes }),
      onCancel: (reason) => cancel.abort(reason),
    });
    const signal =
      request.signal === undefined
        ? cancel.signal
        : AbortSignal.any([cancel.signal, request.signal]);
    const done = this.#run(request, results, signal);
    done.catch(() => undefined);
    return { results, done, cancel: (reason) => cancel.abort(reason) };
  }

  async #run(
    request: ExecuteRequest,
    results: ResultChannel,
    signal: AbortSignal,
  ): Promise<ExecutionSummary> {
    const { snapshot, governor, modify } = request;
    const grant = await governor.admit({
      label: `query#${String(request.queryId)}/values`,
      minimumBytes: 64 << 10,
      spillable: false,
    });
    try {
      if (snapshot.catalogVersion !== this.plan.catalogVersion)
        throw new PlanStaleError(this.plan.catalogVersion, snapshot.catalogVersion);
      if ((this.kind === "modify") !== (modify !== null))
        throw new TypeError("A modify target is required exactly for modify plans");
      await Promise.resolve();
      if (signal.aborted) throw new QueryCancelledError(signal.reason);
      const batch = this.#evaluate(request.parameters);
      let rowCount = batch.length;
      let inserted = 0;
      let output = batch;
      if (modify !== null && this.plan.root.op === "modify") {
        const conflict: ConflictMode =
          this.plan.root.onConflict === null
            ? { kind: "error" }
            : { kind: "nothing", indexId: this.plan.root.onConflict.indexId };
        const result = await modify.insert(batch, conflict, signal);
        inserted = rowCount = result.inserted.count;
        output = { ...batch, selection: result.inserted };
        await modify.finish();
      }
      if (this.columns.length > 0) {
        const values = i32(output.columns[0] as FlatVector);
        const rows = Int32Array.from(
          { length: output.selection?.count ?? output.length },
          (_, k) => values[output.selection === null ? k : (output.selection.rows[k] ?? 0)] ?? 0,
        );
        const columns = [
          { form: "flat", validity: null, data: { layout: "i32", values: rows } },
        ] as const;
        const bytes = frameByteLength(columns);
        if (!grant.allocate(bytes)) throw new Error("toy: frame does not fit");
        if (
          results.offer({
            rowCount: rows.length,
            byteLength: bytes,
            columns,
            free: () => grant.free(bytes),
          }) === "blocked"
        )
          await results.ready();
      }
      results.end();
      return {
        rowCount,
        counts: { inserted, updated: 0, deleted: 0 },
        profile: { operators: [], pipelines: 1, memoryWaitMs: 0, peakHeldBytes: grant.held },
      };
    } catch (error) {
      const failure = signal.aborted ? new QueryCancelledError(signal.reason) : error;
      modify?.abort(failure);
      results.fail(failure);
      throw failure;
    } finally {
      grant.close();
    }
  }

  #evaluate(parameters: BoundParameters): Batch {
    const vector = allocateFlat(INT, this.values.rows.length);
    const values = i32(vector);
    this.values.rows.forEach((row, index) => {
      const expr: ScalarExpr | undefined = row[0];
      if (expr?.op === "literal") values[index] = i32(expr.value)[0] ?? 0;
      else if (expr?.op === "param") {
        const slot = parameters.slots[expr.slot];
        if (slot === undefined) throw new RangeError(`No slot ${String(expr.slot)}`);
        values[index] = i32(slot)[0] ?? 0;
      } else throw new UnsupportedPlanError(`${String(expr?.op)} in the toy executor`);
    });
    vector.length = this.values.rows.length;
    return { length: vector.length, columns: [vector], selection: null };
  }
}

// ---------------------------------------------------------------- plans and doubles

function literal(value: number): ScalarExpr {
  const vector = allocateFlat(INT, 1);
  i32(vector)[0] = value;
  vector.length = 1;
  return { op: "literal", value: vector, type: INT };
}

const OUT: OutputColumn[] = [{ name: "n", type: INT, nullable: false }];

function valuesPlan(rows: ScalarExpr[], placeholders = 0, catalogVersion = 3): PhysicalPlan {
  return {
    root: { id: 1, op: "values", output: OUT, rows: rows.map((expr) => [expr]) },
    columns: OUT,
    parameters: Array.from({ length: placeholders }, () => INT),
    placeholderCount: placeholders,
    clockSlot: null,
    initPlans: [],
    catalogVersion,
    volatile: false,
  };
}

function insertPlan(values: number[], returning: boolean): PhysicalPlan {
  const input = valuesPlan(values.map(literal)).root;
  return {
    ...valuesPlan([]),
    root: {
      id: 2,
      op: "modify",
      kind: "insert",
      tableId: TABLE,
      input,
      output: returning ? OUT : [],
      onConflict: { indexId: 1, action: "nothing", assignments: [], where: null },
      returning: returning ? [{ op: "column", index: 0, type: INT }] : null,
    },
    columns: returning ? OUT : [],
  };
}

class Pin implements SnapshotPin {
  released = false;
  readonly rootSequence = 12;
  release(): void {
    this.released = true;
  }
}

function snapshotOf(pin: SnapshotPin, catalogVersion = 3): QuerySnapshot {
  const rows: RowFetcher = { fetch: () => Promise.reject(new SnapshotReleasedError()) };
  return {
    pin,
    catalogVersion,
    rows,
    openTable: () => Promise.reject(new Error("the toy never scans")),
  };
}

/** The write path's statement writer, as a recording double over a set of unique keys. */
class RecordingTarget implements ModifyTarget {
  readonly tableId = TABLE;
  readonly calls: string[] = [];
  readonly keys = new Set<number>([2]);
  readonly #inserted: Selection = allocateSelection(2048);

  insert(rows: Batch, conflict: ConflictMode): Promise<InsertResult> {
    this.calls.push(`insert:${conflict.kind}`);
    const values = i32(rows.columns[0] as FlatVector);
    this.#inserted.count = 0;
    for (let k = 0; k < (rows.selection?.count ?? rows.length); k++) {
      const row = rows.selection === null ? k : (rows.selection.rows[k] ?? 0);
      const key = values[row] ?? 0;
      if (this.keys.has(key)) continue; // DO NOTHING: skipped, not counted, not returned
      this.keys.add(key);
      this.#inserted.rows[this.#inserted.count++] = row;
    }
    const empty = { length: 0, columns: [], selection: null };
    return Promise.resolve({
      inserted: this.#inserted,
      conflicts: {
        count: 0,
        rows: new Uint16Array(0),
        existing: empty,
        locators: allocateFlat({ kind: "locator", tableId: TABLE }, 0),
      },
    });
  }
  update(): Promise<Selection> {
    return Promise.reject(new Error("not in this example"));
  }
  delete(): Promise<Selection> {
    return Promise.reject(new Error("not in this example"));
  }
  finish(): Promise<void> {
    this.calls.push("finish");
    return Promise.resolve();
  }
  abort(): void {
    this.calls.push("abort");
  }
}

interface Posted {
  readonly frame: ResultFrame;
  readonly transfer: ArrayBuffer[];
}

/** The owner's side of one statement: everything B5 does around the executor. */
async function ownerRuns(
  executor: Executor,
  plan: PhysicalPlan,
  values: ParameterValue[],
  options: { governor: MemoryGovernor; pin?: Pin; modify?: ModifyTarget; snapshotVersion?: number },
): Promise<{ posted: Posted[]; summary: ExecutionSummary; pin: Pin }> {
  const prepared = executor.prepare(plan); // cached with the plan in the owner
  const pin = options.pin ?? new Pin();
  const execution = prepared.execute({
    parameters: prepared.bind(values),
    statementTime: 784_111_777_000_000n,
    snapshot: snapshotOf(pin, options.snapshotVersion),
    governor: options.governor,
    spill: new MemorySpillService(),
    queryId: 1,
    modify: options.modify ?? null,
    sequences: null,
  });
  const posted: Posted[] = [];
  try {
    for (
      let frame = await execution.results.next();
      frame !== null;
      frame = await execution.results.next()
    ) {
      posted.push({ frame, transfer: frameTransferList(frame.columns) }); // postMessage(frame, transfer)
      frame.release(); // on the client's credit message
    }
    const summary = await execution.done; // then commit (autocommit DML)
    return { posted, summary, pin };
  } finally {
    await execution.done.catch(() => undefined); // grants and spill closed: the pin may go
    pin.release();
  }
}

// ---------------------------------------------------------------- the examples

describe("executor entry point (examples)", () => {
  it("runs a query: bind, stream frames, settle, release the pin", async () => {
    const governor = new Governor(16 << 20);
    const plan = valuesPlan([literal(1), { op: "param", slot: 0, type: INT }], 1);
    const { posted, summary, pin } = await ownerRuns(new ToyExecutor(), plan, [41], { governor });
    expect(
      posted.map(({ frame }) => [
        ...(frame.columns[0]?.form === "flat" && frame.columns[0].data.layout === "i32"
          ? frame.columns[0].data.values
          : []),
      ]),
    ).toEqual([[1, 41]]);
    expect(posted[0]?.transfer).toHaveLength(1);
    expect(summary.rowCount).toBe(2);
    expect(pin.released).toBe(true);
    expect(governor.usedBytes).toBe(0);
  });

  it("refuses a bad parameter before executing", () => {
    const prepared = new ToyExecutor().prepare(
      valuesPlan([{ op: "param", slot: 0, type: INT }], 1),
    );
    expect(() => prepared.bind([])).toThrow(ParameterBindError);
    expect(() => prepared.bind(["x"])).toThrow(expect.objectContaining({ slot: 0 }));
  });

  it("fails a stale plan with PlanStaleError and nothing run", async () => {
    const governor = new Governor(16 << 20);
    const run = ownerRuns(new ToyExecutor(), valuesPlan([literal(1)]), [], {
      governor,
      snapshotVersion: 4,
    });
    await expect(run).rejects.toBeInstanceOf(PlanStaleError);
    expect(governor.usedBytes).toBe(0);
  });

  it("cancels: done rejects with QueryCancelledError after cleanup", async () => {
    const governor = new Governor(16 << 20);
    const prepared = new ToyExecutor().prepare(valuesPlan([literal(1)]));
    const execution = prepared.execute({
      parameters: prepared.bind([]),
      statementTime: 0n,
      snapshot: snapshotOf(new Pin()),
      governor,
      spill: new MemorySpillService(),
      queryId: 2,
      modify: null,
      sequences: null,
    });
    execution.results.cancel("client closed the cursor");
    await expect(execution.done).rejects.toBeInstanceOf(QueryCancelledError);
    await expect(execution.results.next()).rejects.toBeInstanceOf(QueryCancelledError);
    expect(governor.usedBytes).toBe(0);
  });

  it("hands DML rows to the write path: insert, then finish; RETURNING only what took effect", async () => {
    const governor = new Governor(16 << 20);
    const target = new RecordingTarget();
    const { posted, summary } = await ownerRuns(
      new ToyExecutor(),
      insertPlan([1, 2, 3], true),
      [],
      {
        governor,
        modify: target,
      },
    );
    expect(target.calls).toEqual(["insert:nothing", "finish"]);
    expect(summary.counts).toEqual({ inserted: 2, updated: 0, deleted: 0 });
    expect(
      posted.flatMap(({ frame }) =>
        frame.columns[0]?.form === "flat" && frame.columns[0].data.layout === "i32"
          ? [...frame.columns[0].data.values]
          : [],
      ),
    ).toEqual([1, 3]);
  });

  it("aborts the write path's statement on failure", async () => {
    const governor = new Governor(16 << 20);
    const target = new RecordingTarget();
    const run = ownerRuns(new ToyExecutor(), insertPlan([1], false), [], {
      governor,
      modify: target,
      snapshotVersion: 9,
    });
    await expect(run).rejects.toBeInstanceOf(PlanStaleError);
    expect(target.calls).toEqual(["abort"]);
  });
});
