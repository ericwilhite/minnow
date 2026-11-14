/**
 * How the consuming tracks call the executor's entry point (exec/executor.ts) over the access
 * contracts (access/):
 * - the owner (B5): prepare → execute with raw values (binding follows admission); a cached read completes inside `execute` and its
 *   frame is polled with no await (R6); frames are posted as `payload` + `transfer` and released on
 *   credit; `delivered` closes the result grant (R3); a stale plan; cancellation;
 * - DML (B3's `Statement`): insert through a `TableWriter`, `statement.end()` before `executed`,
 *   RETURNING held in the spool until `acknowledgeCommit()` for autocommit and released at
 *   `executed` inside a transaction (R2, R11); `rollback()` and a discarded spool on failure.
 *
 * `ToyExecutor` runs only `values` over int32 literals and parameters, plus `modify` insert over
 * them, so the protocol is visible. The Phase 2 executor replaces it behind the same interfaces.
 */
import { describe, expect, it } from "vitest";
import type { CatalogView } from "../access/catalog.js";
import type { Statement, TableWriter } from "../access/modify.js";
import { readBound, statementBound, type Snapshot } from "../access/snapshot.js";
import type { OutputColumn, PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import { Governor, type Grant, type MemoryGovernor } from "../memory/governor.js";
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
  ExecutionState,
  ExecutionSummary,
  Executor,
  ParameterValue,
  PreparedPlan,
} from "./executor.js";
import { ResultChannel, frameByteLength, type FramePayload, type ResultFrame } from "./result.js";
import type { ChunkReads } from "./scan.js";

const INT = { kind: "int32", bits: 32 } as const;
const TABLE = 7;

function i32(vector: FlatVector): Int32Array {
  if (vector.data.layout !== "i32") throw new TypeError("int32 expected");
  return vector.data.values;
}

const settle = async <T>(value: T | Promise<T>): Promise<T> => value;

// ---------------------------------------------------------------- the toy executor

class ToyExecutor implements Executor {
  prepare(plan: PhysicalPlan): PreparedPlan {
    const values = plan.root.op === "modify" ? plan.root.input : plan.root;
    if (values.op !== "values" || (plan.root.op === "modify" && plan.root.kind !== "insert"))
      throw new UnsupportedPlanError(`${plan.root.op} in the toy executor`);
    return new ToyPrepared(plan, values);
  }
}

class ToyExecution implements Execution {
  state: ExecutionState = "running";
  summary: ExecutionSummary | null = null;
  executed: Promise<ExecutionSummary> = new Promise<ExecutionSummary>(() => undefined);
  readonly delivered: Promise<void>;
  /** RETURNING payloads held until commit (the toy's in-memory spool). */
  spool: Array<{ payload: Omit<FramePayload, "sequence">; bytes: number }> | null = null;
  constructor(
    readonly results: ResultChannel,
    readonly grant: Grant,
    readonly abort: AbortController,
  ) {
    this.delivered = results.delivered.finally(() => grant.close());
    this.delivered.catch(() => undefined);
  }
  acknowledgeCommit(): void {
    const spool = this.spool;
    this.spool = null;
    if (spool === null) return;
    for (const { payload, bytes } of spool)
      this.results.offer({ payload, requiresLsn: 0, free: () => this.grant.free(bytes) });
    this.results.end();
  }
  cancel(reason?: unknown): void {
    this.abort.abort(reason);
    this.results.cancel(reason);
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

  bind(values: readonly ParameterValue[], grant: Grant): BoundParameters {
    if (values.length !== this.plan.placeholderCount)
      throw new ParameterBindError(
        `Expected ${String(this.plan.placeholderCount)} parameters`,
        null,
      );
    return {
      slots: this.plan.parameters.map((_, slot) => {
        const value = values[slot];
        if (typeof value !== "number" || !Number.isInteger(value))
          throw new ParameterBindError(`$${String(slot + 1)}: integer expected`, slot);
        if (!grant.allocate(4)) throw new RangeError("toy: parameter charge refused");
        const vector = allocateFlat(INT, 1);
        i32(vector)[0] = value;
        vector.length = 1;
        return vector;
      }),
    };
  }

  execute(request: ExecuteRequest): Execution {
    if ((this.kind === "modify") !== (request.statement !== null))
      throw new TypeError("A statement is required exactly for modify plans");
    const label = `query#${String(request.queryId)}/result`;
    const minimumBytes = 64 << 10;
    // R6: synchronous admission when it fits; the toy does not implement the async fallback.
    const grant = request.governor.tryAdmit({ label, minimumBytes, spillable: true });
    if (grant === null) throw new Error("toy: no synchronous admission");
    const abort = new AbortController();
    const results = new ResultChannel(this.columns, {
      onCancel: (reason) => abort.abort(reason),
    });
    const execution = new ToyExecution(results, grant, abort);
    const fail = (error: unknown): never => {
      const failure = abort.signal.aborted ? new QueryCancelledError(abort.signal.reason) : error;
      execution.state = abort.signal.aborted ? "cancelled" : "failed";
      request.statement?.rollback();
      execution.spool = null;
      results.fail(failure);
      throw failure;
    };
    const done = (summary: ExecutionSummary): ExecutionSummary => {
      execution.state = "executed";
      execution.summary = summary;
      return summary;
    };
    const run = (): ExecutionSummary | Promise<ExecutionSummary> => {
      const view = request.statement?.view ?? request.snapshot;
      if (view === null) throw new TypeError("no view");
      if (view.catalog.version !== this.plan.catalogVersion)
        throw new PlanStaleError(this.plan.catalogVersion, view.catalog.version);
      const batch = this.#evaluate(this.bind(request.parameters, grant));
      if (request.statement !== null)
        return this.#modify(request, request.statement, batch, execution, done);
      // A query: synchronous from start to the last frame when nothing it reads awaits.
      this.#offer(execution, batch, view.observedLsn);
      results.end();
      return done(this.#summary(batch.length, 0, view));
    };
    let outcome: ExecutionSummary | Promise<ExecutionSummary>;
    try {
      outcome = run();
    } catch (error) {
      outcome = Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
    execution.executed = Promise.resolve(outcome).catch(fail);
    execution.executed.catch(() => undefined);
    return execution;
  }

  async #modify(
    request: ExecuteRequest,
    statement: Statement,
    batch: Batch,
    execution: ToyExecution,
    done: (summary: ExecutionSummary) => ExecutionSummary,
  ): Promise<ExecutionSummary> {
    const writer = statement.table(TABLE);
    const written = allocateSelection(batch.length);
    const inserted = await settle(writer.insert(batch, written));
    if (execution.abort.signal.aborted)
      throw new QueryCancelledError(execution.abort.signal.reason);
    await statement.end();
    if (this.columns.length > 0) {
      // RETURNING over exactly the rows written, into the spool.
      const rows = Int32Array.from({ length: written.count }, (_, k) => {
        return i32(batch.columns[0] as FlatVector)[written.rows[k] ?? 0] ?? 0;
      });
      const payload = {
        rowCount: rows.length,
        columns: [
          {
            form: "flat",
            length: rows.length,
            validity: null,
            data: { layout: "i32", values: rows },
          },
        ],
        dictionaries: [],
      } as const;
      const bytes = frameByteLength(payload);
      if (!execution.grant.allocate(bytes)) throw new Error("toy: spool does not fit");
      execution.spool = [{ payload, bytes }];
    } else execution.spool = [];
    const summary = done(this.#summary(inserted, inserted, statement.view));
    if (request.returning === "atExecuted") execution.acknowledgeCommit();
    return summary;
  }

  #offer(execution: ToyExecution, batch: Batch, requiresLsn: number): void {
    const rows = Int32Array.from(i32(batch.columns[0] as FlatVector).subarray(0, batch.length));
    const payload = {
      rowCount: rows.length,
      columns: [
        {
          form: "flat",
          length: rows.length,
          validity: null,
          data: { layout: "i32", values: rows },
        },
      ],
      dictionaries: [],
    } as const;
    const bytes = frameByteLength(payload);
    if (!execution.grant.allocate(bytes)) throw new Error("toy: frame does not fit");
    execution.results.offer({ payload, requiresLsn, free: () => execution.grant.free(bytes) });
  }

  #summary(rowCount: number, inserted: number, view: Snapshot): ExecutionSummary {
    return {
      rowCount,
      counts: { inserted, updated: 0, deleted: 0 },
      requiresLsn: view.observedLsn,
      profile: {
        operators: [],
        pipelines: 1,
        memoryWaitMs: 0,
        peakHeldBytes: 0,
        synchronous: this.kind === "query",
      },
    };
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
      assigned: [],
      onRepeat: "error",
      onConflict: null,
      returning: returning ? [{ op: "column", index: 0, type: INT }] : null,
    },
    columns: returning ? OUT : [],
  };
}

function catalogAt(version: number): CatalogView {
  return {
    version,
    tables: [],
    sequences: [],
    views: [],
    types: [],
    fts: [],
    table: () => undefined,
    tableByName: () => undefined,
    sequence: () => undefined,
    type: () => undefined,
    typeByName: () => undefined,
  };
}

function viewOf(bound: number, catalogVersion = 3): Snapshot & { releases: number } {
  return {
    rootSequence: 12,
    released: false,
    bound,
    observedLsn: 41,
    catalog: catalogAt(catalogVersion),
    releases: 0,
    pages: {
      read: () => Promise.reject(new Error("the toy never reads pages")),
      peek: () => null,
    },
    table() {
      throw new Error("the toy never scans");
    },
    index() {
      throw new Error("the toy never probes");
    },
    release() {
      this.releases += 1;
    },
  };
}

/** B3's statement, as a recording double: keys already present are a unique violation at end(). */
class RecordingStatement implements Statement {
  readonly ordinal = 1;
  readonly view: Snapshot;
  readonly catalog: CatalogView;
  readonly calls: string[] = [];
  readonly keys = new Set<number>();
  constructor(
    readonly existing: readonly number[],
    catalogVersion = 3,
  ) {
    this.view = viewOf(statementBound(1), catalogVersion);
    this.catalog = this.view.catalog;
  }
  table(tableId: number): TableWriter {
    this.calls.push(`table:${String(tableId)}`);
    const insert = (batch: Batch, written: Selection | null): number => {
      this.calls.push("insert");
      const values = i32(batch.columns[0] as FlatVector);
      for (let row = 0; row < batch.length; row++) {
        this.keys.add(values[row] ?? 0);
        if (written !== null) written.rows[written.count++] = row;
      }
      return batch.length;
    };
    const refuse = (): never => {
      throw new Error("not in this example");
    };
    return { tableId, insert, update: refuse, delete: refuse, conflicts: refuse };
  }
  changeCatalog(): never {
    throw new Error("not in this example");
  }
  end(): Promise<void> {
    this.calls.push("end");
    for (const key of this.existing)
      if (this.keys.has(key)) return Promise.reject(new RangeError(`duplicate key ${String(key)}`));
    return Promise.resolve();
  }
  rollback(): void {
    this.calls.push("rollback");
  }
}

const reads: ChunkReads = {
  open: () => {
    throw new Error("the toy never reads pages");
  },
};

function requestFor(
  values: ParameterValue[],
  governor: MemoryGovernor,
  target: { snapshot: Snapshot } | { statement: Statement },
  returning: "atExecuted" | "atCommit" = "atExecuted",
): ExecuteRequest {
  const base = {
    parameters: values,
    statementTime: 784_111_777_000_000n,
    reads,
    governor,
    spill: new MemorySpillService(),
    queryId: 1,
    sequences: null,
    sequenceSession: null,
    returning,
  };
  return "snapshot" in target
    ? { ...base, snapshot: target.snapshot, statement: null }
    : { ...base, snapshot: null, statement: target.statement };
}

/** The owner posts a frame as plain data plus its transfer list, and releases it on credit. */
function post(frame: ResultFrame, posted: number[][]): void {
  const clone = structuredClone(frame.payload, { transfer: [...frame.transfer] });
  const column = clone.columns[0];
  if (column?.form === "flat" && column.data.layout === "i32") posted.push([...column.data.values]);
  frame.release(); // on the client's credit message
}

// ---------------------------------------------------------------- the examples

describe("executor entry point (examples)", () => {
  it("a cached read runs inside execute: poll the frame, no await (R6)", async () => {
    const governor = new Governor(16 << 20);
    const prepared = new ToyExecutor().prepare(
      valuesPlan([literal(1), { op: "param", slot: 0, type: INT }], 1),
    );
    const view = viewOf(readBound(41));
    const execution = prepared.execute(requestFor([41], governor, { snapshot: view }));
    expect(execution.state).toBe("executed");
    expect(execution.summary?.requiresLsn).toBe(41); // writePath.durable(41) is null: no wait
    view.release(); // the owner may drop the pin at `executed`
    const posted: number[][] = [];
    for (let frame = execution.results.poll(); frame; frame = execution.results.poll())
      post(frame, posted);
    expect(posted).toEqual([[1, 41]]);
    await execution.delivered; // every frame released: the result grant is closed
    expect(governor.usedBytes).toBe(0);
  });

  it("refuses a bad parameter before executing", () => {
    const prepared = new ToyExecutor().prepare(
      valuesPlan([{ op: "param", slot: 0, type: INT }], 1),
    );
    const grant = new Governor(8 << 20).tryAdmit({
      label: "bind-test",
      minimumBytes: 64,
      spillable: false,
    });
    if (grant === null) throw new Error("missing grant");
    try {
      expect(() => prepared.bind([], grant)).toThrow(ParameterBindError);
      expect(() => prepared.bind(["x"], grant)).toThrow(expect.objectContaining({ slot: 0 }));
    } finally {
      grant.close();
    }
  });

  it("fails a stale plan with PlanStaleError and nothing run", async () => {
    const governor = new Governor(16 << 20);
    const prepared = new ToyExecutor().prepare(valuesPlan([literal(1)]));
    const execution = prepared.execute(
      requestFor([], governor, { snapshot: viewOf(readBound(1), 4) }),
    );
    await expect(execution.executed).rejects.toBeInstanceOf(PlanStaleError);
    await expect(execution.delivered).rejects.toBeInstanceOf(PlanStaleError);
    expect(governor.usedBytes).toBe(0);
  });

  it("cancellation closes the result grant at once", async () => {
    const governor = new Governor(16 << 20);
    const prepared = new ToyExecutor().prepare(valuesPlan([literal(1)]));
    const execution = prepared.execute(
      requestFor([], governor, { snapshot: viewOf(readBound(1)) }),
    );
    execution.results.cancel("client disconnected");
    await expect(execution.delivered).rejects.toBeInstanceOf(QueryCancelledError);
    await expect(execution.results.next()).rejects.toBeInstanceOf(QueryCancelledError);
    expect(governor.usedBytes).toBe(0);
  });

  it("autocommit DML: end() before executed, RETURNING held until acknowledgeCommit", async () => {
    const governor = new Governor(16 << 20);
    const statement = new RecordingStatement([]);
    const prepared = new ToyExecutor().prepare(insertPlan([1, 2, 3], true));
    const execution = prepared.execute(requestFor([], governor, { statement }, "atCommit"));
    const summary = await execution.executed;
    expect(statement.calls).toEqual(["table:7", "insert", "end"]);
    expect(summary.counts).toEqual({ inserted: 3, updated: 0, deleted: 0 });
    expect(execution.results.poll()).toBeUndefined(); // nothing before the commit
    // …owner: transaction.commit(request), then receipt.durable under strict…
    execution.acknowledgeCommit();
    const posted: number[][] = [];
    for (let frame = await execution.results.next(); frame; frame = await execution.results.next())
      post(frame, posted);
    expect(posted).toEqual([[1, 2, 3]]);
    await execution.delivered;
    expect(governor.usedBytes).toBe(0);
  });

  it("inside a transaction, RETURNING flows at executed (R11)", async () => {
    const governor = new Governor(16 << 20);
    const prepared = new ToyExecutor().prepare(insertPlan([5], true));
    const execution = prepared.execute(
      requestFor([], governor, { statement: new RecordingStatement([]) }),
    );
    await execution.executed;
    expect(execution.results.poll()?.payload.rowCount).toBe(1);
  });

  it("a failed statement rolls back and discards the spool", async () => {
    const governor = new Governor(16 << 20);
    const statement = new RecordingStatement([2]);
    const prepared = new ToyExecutor().prepare(insertPlan([1, 2], true));
    const execution = prepared.execute(requestFor([], governor, { statement }, "atCommit"));
    await expect(execution.executed).rejects.toThrow("duplicate key 2");
    expect(statement.calls).toEqual(["table:7", "insert", "end", "rollback"]);
    expect(execution.state).toBe("failed");
    await expect(execution.delivered).rejects.toThrow("duplicate key 2");
    expect(governor.usedBytes).toBe(0);
  });
});
