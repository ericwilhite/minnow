/**
 * Typed engine errors, kept free of engine imports so the main-thread worker client can rehydrate
 * them (name, fields, instanceof) without bundling the executor.
 */

type ErrorValue = boolean | number | string | Date;

/** An idle SQL transaction rolled back; later statements must not become autocommits. */
export class TransactionExpiredError extends Error {
  override readonly name = "TransactionExpiredError";

  constructor() {
    super("The SQL transaction expired and rolled back; issue ROLLBACK or BEGIN before continuing");
  }
}

/** The worker stopped responding within the configured request deadline. */
export class DatabaseWorkerTimeoutError extends ConnectionLostError {
  override readonly name = "DatabaseWorkerTimeoutError";
  constructor(
    readonly method: string,
    readonly timeoutMs: number,
  ) {
    super(`Database worker ${method} did not respond within ${String(timeoutMs)}ms`);
  }
}

/**
 * The worker's channel failed underneath every pending call: the worker raised a script error it
 * did not handle, or sent a frame that could not be read. `reason` says which; `cause` carries
 * the worker's own error when the browser handed it over.
 */
export class DatabaseWorkerFailedError extends ConnectionLostError {
  override readonly name = "DatabaseWorkerFailedError";
  constructor(
    readonly reason: "error" | "messageerror" | "reopened",
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

/**
 * The store an `auto` descriptor remembered for this database cannot be opened here — OPFS in a
 * context without synchronous access handles, typically — and the database is not reopened,
 * empty, on the other store.
 */
export class DatabaseStoreUnavailableError extends Error {
  override readonly name = "DatabaseStoreUnavailableError";
  constructor(
    readonly store: "opfs" | "indexeddb",
    readonly databaseName: string,
    message: string,
  ) {
    super(message);
  }
}

/** A transport failure cannot prove whether this operation published before its reply was lost. */
export class DatabaseWorkerOutcomeUnknownError extends UnknownOutcomeError {
  override readonly name = "DatabaseWorkerOutcomeUnknownError";
  constructor(
    readonly method: string,
    readonly requestId: string,
    options?: ErrorOptions,
  ) {
    super(
      `Database worker outcome is unknown for ${method}; reconcile durable application IDs before retrying`,
      options,
    );
  }
}

function formatValue(value: ErrorValue): string {
  return value instanceof Date ? dateIsoString(value) : String(value);
}

/** A database operation named a table that is not present in the current catalog. */
export class UnknownTableError extends TypeError {
  override readonly name = "UnknownTableError";

  constructor(readonly tableName: string) {
    super(`Unknown table: ${tableName}`);
  }
}

/** Refuses another read before one database instance retains an unbounded request backlog. */
export class DatabaseReadBacklogError extends Error {
  override readonly name = "DatabaseReadBacklogError";

  constructor(readonly limit = 256) {
    super(`A database cannot retain more than ${String(limit)} active reads; await a read`);
  }
}

/** A live-query owner reached one of its documented resident-resource ceilings. */
export class LiveQueryLimitError extends Error {
  override readonly name = "LiveQueryLimitError";

  constructor(
    readonly resource: "set" | "group" | "subscription" | "byte",
    readonly limit: number,
  ) {
    super(
      resource === "set"
        ? `A database cannot retain more than ${String(limit)} live-query sets`
        : resource === "byte"
          ? `A live-query set cannot retain more than ${String(limit)} modeled bytes`
          : `A live-query set cannot retain more than ${String(limit)} ${resource} records`,
    );
  }
}

/**
 * A storage step before the commit — a row-id or auto-increment reservation, a lease renewal, a
 * journal write — lost its acknowledgement. Nothing of the write can have published, because no
 * publishing call was issued, so unlike the `UnknownOutcomeError` it carries as `cause` this is a
 * definite failure: repeating the write is safe. The cause still says whether the connection
 * survived (`classifyError(error).connectionUsable`).
 */
export class WriteNotPublishedError extends Error {
  override readonly name = "WriteNotPublishedError";

  constructor(cause: unknown) {
    super(
      "A storage step before the commit lost its acknowledgement; nothing of the write was " +
        "published, and repeating it is safe",
      { cause },
    );
  }
}

export class UniqueConstraintError extends Error {
  override readonly name = "UniqueConstraintError";

  constructor(
    readonly tableName: string,
    readonly columnName: string,
    readonly value: ErrorValue,
  ) {
    super(`Duplicate value for ${tableName}.${columnName}: ${formatValue(value)}`);
  }
}

export class MissingKeyError extends Error {
  override readonly name = "MissingKeyError";

  constructor(
    readonly tableName: string,
    readonly columnName: string,
    readonly value: ErrorValue,
  ) {
    super(`Missing value for ${tableName}.${columnName}: ${formatValue(value)}`);
  }
}

export class CompactionMemoryBudgetError extends Error {
  override readonly name = "CompactionMemoryBudgetError";

  constructor(
    readonly budgetBytes: number,
    readonly minimumBytes: number,
  ) {
    super(
      `Compaction needs at least ${String(minimumBytes)} bytes of working memory; budget is ${String(budgetBytes)} bytes`,
    );
  }
}

export class CompactionWriteAmplificationError extends Error {
  override readonly name = "CompactionWriteAmplificationError";

  constructor(
    readonly outputBytes: number,
    readonly maximumOutputBytes: number,
  ) {
    super(
      `Compaction output would use ${String(outputBytes)} stored bytes; limit is ${String(maximumOutputBytes)} bytes`,
    );
  }
}

export class CompactionJobCancelledError extends Error {
  override readonly name = "CompactionJobCancelledError";

  constructor(readonly jobId: string) {
    super(`Compaction job cancelled: ${jobId}`);
  }
}

/** Refuses more growth after automatic collection repeatedly fails at a generous debt limit. */
export class MaintenanceBacklogError extends Error {
  override readonly name = "MaintenanceBacklogError";

  constructor(
    readonly pendingCommits: number,
    readonly causeMessage: string | null,
  ) {
    super(
      `Automatic storage collection could not keep up after ${String(pendingCommits)} commits${
        causeMessage === null ? "" : `: ${causeMessage}`
      }`,
    );
  }
}

/**
 * A paged visible-segment scan cannot continue after its table is dropped or replaced. Segment
 * metadata for a dropped table is removed immediately, so returning an empty or partial page
 * would silently misrepresent the captured scan.
 */
export class VisibleSegmentCursorStaleError extends Error {
  override readonly name = "VisibleSegmentCursorStaleError";

  constructor(
    readonly tableName: string,
    readonly capturedTableId: string,
    readonly currentTableId: string | null,
  ) {
    super(
      `Visible segment cursor for ${tableName} is stale: captured table ${capturedTableId}; ${
        currentTableId === null
          ? "the table no longer exists"
          : `current table is ${currentTableId}`
      }`,
    );
  }
}

/**
 * SQL that failed to compile, located in the text the caller passed. `offset` and `length` are
 * character positions into that exact string — leading whitespace included — so an editor can
 * underline the token that failed without re-deriving the position from the message.
 *
 * `length` is 0 where the failure has no width: an empty statement, or a query that ended before
 * the parser expected it to. Errors raised after parsing succeeds (plan optimization, execution)
 * carry no position and stay plain `TypeError`s.
 *
 * It extends `TypeError` because that is what compilation failures threw before positions existed.
 */
export class SqlCompileError extends TypeError {
  override readonly name = "SqlCompileError";

  constructor(
    message: string,
    readonly offset: number,
    readonly length: number,
  ) {
    super(message);
  }
}
import { dateIsoString } from "../date-value.js";
import {
  BlockReadBatchTooLargeError,
  CompactionBacklogError,
  CompactionJobConflictError,
  ConnectionLostError,
  GarbageCollectionJobConflictError,
  IndexedDbSchemaUpgradeBlockedError,
  LeaseConflictError,
  LeaseExpiredError,
  LeaseOwnerConflictError,
  OpfsCoordinationError,
  OpfsDatabaseInUseError,
  PostingBuildConflictError,
  SchemaConflictError,
  SnapshotImportConflictError,
  SnapshotManifestMissingError,
  StorageCorruptionError,
  StorageFormatVersionError,
  StorageResourceLimitError,
  TableInUseError,
  TableRecordConflictError,
  TempOwnerConflictError,
  TransactionRecordConflictError,
  UniqueIndexCoverageError,
  UniqueKeyBuildConflictError,
  UniqueKeyConflictError,
  UnknownOutcomeError,
  WriteConflictError,
} from "../storage/types.js";

/**
 * What an error means for the caller, in the three terms that decide what to do next. Every
 * error Minnow throws maps to exactly one kind; an unrecognized error is `"other"` with the
 * cautious answers.
 */
export type ErrorKind =
  /** The operation may have happened. Reconcile a stable id or revision before retrying. */
  | "unknown-outcome"
  /** This connection is finished; open a new one. Pending mutations were reported separately. */
  | "connection-lost"
  /** Lost a race with another writer or a schema change; nothing happened. Retry as is. */
  | "conflict"
  /** The request itself is wrong (a duplicate key, a bad table, a compile error). Fix it. */
  | "rejected"
  /** A momentary condition: backpressure, coordination, an expired scope. Retry with backoff. */
  | "transient"
  /** A limit was reached: quota, memory, a resource ceiling. Free something, then retry. */
  | "resource"
  /** The stored data is unreadable by this build. Do not retry, and do not delete. */
  | "corruption"
  /** The caller cancelled it; nothing happened. */
  | "cancelled"
  | "other";

export interface ErrorClassification {
  kind: ErrorKind;
  /** True when the operation may have published despite the error. Never replay when true. */
  mayHavePublished: boolean;
  /** Whether repeating the same call is sound: as is, only after reconciling, or not at all. */
  retry: "safe" | "after-reconcile" | "never";
  /** False when every later call on the same connection will fail the same way. */
  connectionUsable: boolean;
}

type ErrorClass = abstract new (...args: never[]) => Error;

/**
 * A class and the literal `name` its instances carry. Classification matches either: `instanceof`
 * is the normal path, and the name is the fallback for an error whose prototype is not this
 * module's class — a second copy of the module in a bundle, or an error rehydrated by an older
 * client — so the answer never silently degrades to `"other"` for an error the engine knows.
 */
type ErrorEntry = readonly [name: string, constructor: ErrorClass];

const UNKNOWN_OUTCOME_ERRORS: readonly ErrorEntry[] = [
  ["UnknownOutcomeError", UnknownOutcomeError],
  ["OpfsUncertainOutcomeError", UnknownOutcomeError],
  ["StorageUnresponsiveError", UnknownOutcomeError],
  ["DatabaseWorkerOutcomeUnknownError", DatabaseWorkerOutcomeUnknownError],
];

const CONNECTION_LOST_ERRORS: readonly ErrorEntry[] = [
  ["ConnectionLostError", ConnectionLostError],
  ["DatabaseWorkerTimeoutError", DatabaseWorkerTimeoutError],
  ["DatabaseWorkerFailedError", DatabaseWorkerFailedError],
  ["StorageConnectionClosedError", ConnectionLostError],
];

const CORRUPTION_ERRORS: readonly ErrorEntry[] = [
  ["StorageCorruptionError", StorageCorruptionError],
  ["StorageFormatVersionError", StorageFormatVersionError],
];

const CONFLICT_ERRORS: readonly ErrorEntry[] = [
  ["WriteConflictError", WriteConflictError],
  ["SchemaConflictError", SchemaConflictError],
  ["TableRecordConflictError", TableRecordConflictError],
  ["TransactionRecordConflictError", TransactionRecordConflictError],
  ["LeaseConflictError", LeaseConflictError],
  ["LeaseOwnerConflictError", LeaseOwnerConflictError],
  ["CompactionJobConflictError", CompactionJobConflictError],
  ["GarbageCollectionJobConflictError", GarbageCollectionJobConflictError],
  ["TempOwnerConflictError", TempOwnerConflictError],
  ["UniqueKeyBuildConflictError", UniqueKeyBuildConflictError],
  ["PostingBuildConflictError", PostingBuildConflictError],
  ["SnapshotImportConflictError", SnapshotImportConflictError],
  ["SnapshotManifestMissingError", SnapshotManifestMissingError],
  ["TableInUseError", TableInUseError],
  ["OpfsDatabaseInUseError", OpfsDatabaseInUseError],
];

const TRANSIENT_ERRORS: readonly ErrorEntry[] = [
  ["OpfsCoordinationError", OpfsCoordinationError],
  ["DatabaseReadBacklogError", DatabaseReadBacklogError],
  ["MaintenanceBacklogError", MaintenanceBacklogError],
  ["CompactionBacklogError", CompactionBacklogError],
  ["LiveQueryLimitError", LiveQueryLimitError],
  ["LeaseExpiredError", LeaseExpiredError],
  ["TransactionExpiredError", TransactionExpiredError],
  ["IndexedDbSchemaUpgradeBlockedError", IndexedDbSchemaUpgradeBlockedError],
  ["VisibleSegmentCursorStaleError", VisibleSegmentCursorStaleError],
];

const RESOURCE_ERRORS: readonly ErrorEntry[] = [
  ["StorageResourceLimitError", StorageResourceLimitError],
  ["BlockReadBatchTooLargeError", BlockReadBatchTooLargeError],
  ["CompactionMemoryBudgetError", CompactionMemoryBudgetError],
  ["CompactionWriteAmplificationError", CompactionWriteAmplificationError],
];

const REJECTED_ERRORS: readonly ErrorEntry[] = [
  ["UniqueConstraintError", UniqueConstraintError],
  ["UniqueKeyConflictError", UniqueKeyConflictError],
  ["UniqueIndexCoverageError", UniqueIndexCoverageError],
  ["MissingKeyError", MissingKeyError],
  ["UnknownTableError", UnknownTableError],
  ["SqlCompileError", SqlCompileError],
  ["CompactionJobCancelledError", CompactionJobCancelledError],
];

function hasErrorName(error: unknown, name: string): boolean {
  return typeof error === "object" && error !== null && (error as { name?: unknown }).name === name;
}

function isAnyOf(error: unknown, entries: readonly ErrorEntry[]): boolean {
  return entries.some(
    ([name, constructor]) => error instanceof constructor || hasErrorName(error, name),
  );
}

/** Whether the error, or anything in its `cause` chain, says the connection is finished. */
function causedByConnectionLoss(error: unknown): boolean {
  const seen = new Set<unknown>();
  for (let current = error; typeof current === "object" && current !== null;) {
    if (seen.has(current)) return false;
    seen.add(current);
    if (isAnyOf(current, CONNECTION_LOST_ERRORS)) return true;
    current = (current as { cause?: unknown }).cause;
  }
  return false;
}

/**
 * Classifies any error into what it means for the caller: may the operation have published, is
 * a retry sound, and is the connection still usable. Works on errors rehydrated from the worker
 * or a follower hop, and on the platform's own `QuotaExceededError` and `AbortError`. An error
 * is recognised by its class or, failing that, by its `name`, so identity does not depend on
 * one copy of this module or on class names surviving a bundler.
 */
export function classifyError(error: unknown): ErrorClassification {
  if (isAnyOf(error, UNKNOWN_OUTCOME_ERRORS)) {
    return {
      kind: "unknown-outcome",
      mayHavePublished: true,
      retry: "after-reconcile",
      connectionUsable: !causedByConnectionLoss((error as { cause?: unknown }).cause),
    };
  }
  if (isAnyOf(error, CONNECTION_LOST_ERRORS)) {
    return {
      kind: "connection-lost",
      mayHavePublished: false,
      retry: "never",
      connectionUsable: false,
    };
  }
  if (isAnyOf(error, CORRUPTION_ERRORS)) {
    return { kind: "corruption", mayHavePublished: false, retry: "never", connectionUsable: true };
  }
  if (isAnyOf(error, CONFLICT_ERRORS)) {
    return { kind: "conflict", mayHavePublished: false, retry: "safe", connectionUsable: true };
  }
  if (error instanceof WriteNotPublishedError || hasErrorName(error, "WriteNotPublishedError")) {
    // Nothing published, but the cause may still have ended the connection.
    return {
      kind: "transient",
      mayHavePublished: false,
      retry: "safe",
      connectionUsable: !causedByConnectionLoss((error as { cause?: unknown }).cause),
    };
  }
  if (isAnyOf(error, TRANSIENT_ERRORS)) {
    return { kind: "transient", mayHavePublished: false, retry: "safe", connectionUsable: true };
  }
  if (isAnyOf(error, RESOURCE_ERRORS) || hasErrorName(error, "QuotaExceededError")) {
    return {
      kind: "resource",
      mayHavePublished: false,
      retry: "after-reconcile",
      connectionUsable: true,
    };
  }
  if (
    isAnyOf(error, REJECTED_ERRORS) ||
    error instanceof TypeError ||
    error instanceof RangeError ||
    error instanceof SyntaxError
  ) {
    return { kind: "rejected", mayHavePublished: false, retry: "never", connectionUsable: true };
  }
  if (hasErrorName(error, "AbortError")) {
    return { kind: "cancelled", mayHavePublished: false, retry: "safe", connectionUsable: true };
  }
  return { kind: "other", mayHavePublished: false, retry: "never", connectionUsable: true };
}

/**
 * A writer has waited for its turn without the holder changing. Reported through
 * `onBackgroundError` with the context "write admission"; the waiter keeps waiting. The usual
 * cause in one context is a `write()` callback awaiting another write on the same database,
 * which can only proceed once the callback returns. Across tabs it is a tab holding a turn
 * inside a long callback, or one the browser paused mid-write.
 */
export class WriteAdmissionStalledError extends Error {
  override readonly name = "WriteAdmissionStalledError";

  constructor(
    readonly waitedMs: number,
    readonly holder: "this-context" | "other-context",
    readonly holderKind?: string,
  ) {
    super(
      holder === "this-context"
        ? `A write has waited ${String(waitedMs)}ms for a ${holderKind ?? "writer"} on this ` +
            "database to finish; a write() callback that awaits another write on the same " +
            "database waits on itself"
        : `A write has waited ${String(waitedMs)}ms for another tab to finish its write; it ` +
            "proceeds when that tab lets go or the browser releases its lock",
    );
  }
}
