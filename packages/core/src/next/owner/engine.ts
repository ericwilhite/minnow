/**
 * The owner worker's inside: the boundary between the protocol server (this track) and the
 * engine composed from the other tracks. The server owns connections, the request ledger,
 * liveness, cancellation and result frames; the engine owns planning, execution, writes and
 * storage. The composition root (owner/, Phase 2) implements `OwnerEngine` from:
 * - the front end + `Lowerer` (ir/lowering.ts), with a plan cache keyed by (text, catalog version);
 * - the executor entry (B4): `PhysicalPlan` + parameters + snapshot + grants → batches pushed into
 *   an `Operator`;
 * - the write path and key index (B3): snapshots and catalog access, `modify` with counts and
 *   RETURNING batches, group commit carrying the request identity (log-frame.ts
 *   `CommitFrameBody.request`);
 * - the store (B1) behind `acquireFenced`.
 *
 * ## Threading and ordering
 * - Everything runs on the owner worker's one thread. Planning is synchronous; execution and
 *   commit are async and yield at the pipeline driver's waits (exec/pipeline.ts).
 * - Requests of one session run one at a time, in arrival order. Sessions run concurrently;
 *   write conflicts between them are the write path's (B3) to detect or serialize, reported as
 *   errors that cross the wire like any other.
 *
 * ## Results
 * - Rows leave through a `FrameSink`, the pipeline's last operator. Its `push` returns `blocked`
 *   when the client has no credit, so a slow consumer pauses the pipeline instead of growing a
 *   buffer (exec/pipeline.ts "Results"). Each frame is built and transferred as frame.ts says.
 *
 * ## Exactly-once
 * - The server admits each request through `RequestLedger.admit` before calling the engine.
 * - An effectful request (an autocommit write or DDL, or `commit`) passes its identity in
 *   `RunContext.request`; the engine writes it into the commit frame and resolves the commit only
 *   when it is durable (strict) or applied (relaxed). The server then records the outcome.
 * - Requests with no durable effect (reads, statements inside a transaction, rollback) are
 *   settled without an outcome, so a retry re-executes them (`RequestLedger` needs a way to
 *   settle these: see the report).
 *
 * ## Cancellation and liveness
 * - `RunContext.signal` aborts when the client cancels, when its liveness lock is released, or
 *   when the owner steps down. The engine stops at its next wait and releases grants, spill and
 *   pins (pipeline `abort`). A commit already in the group-commit flush completes; the signal
 *   then has no effect and the reply reports the commit.
 */
import type { PhysicalType } from "../types.js";
import type { Operator } from "../exec/pipeline.js";
import type { RequestIdentity } from "../format/ledger.js";
import type { ResultFrame, WireInputColumn } from "./frame.js";
import type { StatementSummary, WireColumnMeta, WireValue } from "./protocol.js";

/** A statement planned for the current catalog version. Cached; shared across connections. */
export interface PlannedStatement {
  readonly text: string;
  readonly kind: "query" | "modify" | "ddl" | "transactionControl" | "utility";
  readonly columns: readonly WireColumnMeta[];
  readonly parameters: readonly PhysicalType[];
  readonly controlsTransaction: boolean;
  readonly catalogVersion: number;
}

export interface RunContext {
  readonly signal: AbortSignal;
  /** The request identity to write into the commit frame, or null if this run commits nothing. */
  readonly request: RequestIdentity | null;
}

/** What a run reports; the server adds `replayed`, `closedEarly` and the session state. */
export type RunOutcome = Pick<StatementSummary, "command" | "rowCount" | "counts" | "lsn">;

/** One session's transaction state and snapshot pins. Created per connection session id. */
export interface EngineSession {
  readonly transaction: StatementSummary["transaction"];
  /** Runs `statement` (re-planned by the engine if the catalog moved); rows go to `sink`. */
  run(
    statement: PlannedStatement,
    params: readonly WireValue[],
    sink: Operator,
    context: RunContext,
  ): Promise<RunOutcome>;
  /** A batch write (protocol.ts `WriteRequest`); RETURNING rows go to `sink`. */
  write(
    input: {
      readonly table: string;
      readonly op: "insert" | "upsert" | "update" | "delete";
      readonly names: readonly string[];
      readonly columns: readonly WireInputColumn[];
      readonly rowCount: number;
      readonly ignoreConflicts: boolean;
      readonly returning: readonly string[] | null;
    },
    sink: Operator,
    context: RunContext,
  ): Promise<RunOutcome>;
  begin(readOnly: boolean): void;
  commit(context: RunContext): Promise<RunOutcome>;
  rollback(): void;
  /** Rolls back anything open and drops every pin. Idempotent. */
  close(): void;
}

export interface OwnerEngine {
  readonly catalogVersion: number;
  /** Parses, rewrites and lowers `text`, or returns the cached plan. Throws SQL errors. */
  plan(text: string): PlannedStatement;
  openSession(): EngineSession;
  /**
   * Tables a statement's result depends on, for live queries: after a commit touching any of
   * them, subscriptions on the statement are re-run (coalesced while unacknowledged).
   */
  readSet(statement: PlannedStatement): readonly number[];
  /** Calls `listener` after each commit with the ids of the tables it changed. */
  onCommit(listener: (tables: readonly number[]) => void): () => void;
  /** Admin calls (protocol.ts `AdminOp`), each with its structured-clone value. */
  admin(op: string, args: readonly WireValue[], context: RunContext): Promise<unknown>;
  /** Flushes pending relaxed commits, then closes the store (graceful step-down). */
  close(): Promise<void>;
}

/**
 * The result sink (implemented by this track): the last `Operator` of a pipeline. It encodes each
 * pushed batch into a `ResultFrame` (frame.ts), posts it with its transfer list, and returns
 * `blocked` when the request has no credit left; a `credit` signal resolves `ready()`. `finish`
 * posts nothing itself: the server's terminal `done` carries the last frame inline when one is
 * pending, so a one-frame result is one message.
 */
export interface FrameSink extends Operator {
  /** The frame held back for the terminal `done`, if any. */
  takePending(): ResultFrame | null;
  /** Adds credit (protocol.ts `Credit`); `UNLIMITED_CREDITS` never blocks. */
  grant(frames: number): void;
}
