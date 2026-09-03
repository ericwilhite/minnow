import type {
  BatchRow,
  Catalog,
  DeleteBatchInput,
  DeleteBatchResult,
  ExecuteResult,
  InsertBatchResult,
  QueryOptions,
  QueryResult,
  TableDefinition,
  UpdateBatchInput,
  UpdateBatchResult,
} from "@minnowdb/core";
import type {
  SnapshotExportProgress,
  SnapshotLoadProgress,
} from "@minnowdb/core/storage/snapshots";

/**
 * The slice of the database the devtools drive. `MinnowDatabase` and `MinnowDatabaseClient`
 * both satisfy it as-is; declaring it structurally keeps the panel decoupled from either class
 * and lets tests pass a stub.
 */
export interface DevtoolsTarget {
  listTables(): Promise<TableDefinition[]>;
  /** Rich catalog metadata when the target exposes the current introspection API. */
  introspect?(): Promise<Catalog>;
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
  explain(sql: string): Promise<string>;
  execute(sql: string): Promise<ExecuteResult>;
}

/**
 * The keyed write API the row editor uses. It is separate from `DevtoolsTarget` because browsing
 * needs none of it: a target that only reads is a perfectly good one, and the editor turns itself
 * off rather than failing when these are missing.
 */
export interface EditableTarget extends DevtoolsTarget {
  insert(tableName: string, row: BatchRow): Promise<InsertBatchResult>;
  updateBatch(tableName: string, input: UpdateBatchInput): Promise<UpdateBatchResult>;
  deleteBatch(tableName: string, input: DeleteBatchInput): Promise<DeleteBatchResult>;
}

/**
 * The snapshot API the download and restore actions drive. Separate from `DevtoolsTarget` for the
 * same reason the write API is: a target that cannot copy itself out is still a perfectly good one
 * to browse, so the actions take themselves off the title bar rather than failing when clicked.
 *
 * Both methods are on `MinnowDatabase` and `MinnowDatabaseClient` whatever their block store can
 * do, so their presence proves the API is reachable, not that this particular store implements it;
 * a store that cannot says so when the action runs, and the panel shows what it said.
 */
export interface SnapshotTarget extends DevtoolsTarget {
  exportSnapshot(options?: {
    onProgress?: (progress: SnapshotExportProgress) => void;
  }): Promise<Uint8Array>;
  importSnapshot(
    bytes: Uint8Array,
    options?: { onProgress?: (progress: SnapshotLoadProgress) => void },
  ): Promise<void>;
}

/**
 * The batched read the whole-result download uses. Both `MinnowDatabase` and the worker client
 * offer it; a target without it downloads what the grid holds.
 */
export interface CursorTarget extends DevtoolsTarget {
  queryCursor(sql: string): AsyncIterable<QueryResult>;
}

export function isCursorTarget(target: DevtoolsTarget): target is CursorTarget {
  return typeof (target as { queryCursor?: unknown }).queryCursor === "function";
}

const snapshotMethods = ["exportSnapshot", "importSnapshot"] as const;

/** Whether this target can copy a database out and load one back. */
export function isSnapshotTarget(target: DevtoolsTarget): target is SnapshotTarget {
  return snapshotMethods.every(
    (method) => typeof (target as unknown as Record<string, unknown>)[method] === "function",
  );
}

const writeMethods = ["insert", "updateBatch", "deleteBatch"] as const;

/** Whether this target can write at all, independent of whether it is allowed to. */
export function isEditableTarget(target: DevtoolsTarget): target is EditableTarget {
  return writeMethods.every(
    (method) => typeof (target as unknown as Record<string, unknown>)[method] === "function",
  );
}

/** What the devtools accept: a database, a worker client, or the typed facade over either. */
export type DevtoolsAttachable = DevtoolsTarget | { readonly driver: unknown };

const requiredMethods = ["listTables", "query", "explain", "execute"] as const;

function missingMethods(value: object): string[] {
  return requiredMethods.filter(
    (method) => typeof (value as Record<string, unknown>)[method] !== "function",
  );
}

/**
 * Resolves whatever the caller passed to a target the panel can drive. A `Minnow` facade is
 * unwrapped to the driver behind it, because the facade itself exposes no catalog or raw SQL.
 * Anything still missing a method fails here, naming what is absent, rather than at the first
 * click inside the panel.
 */
export function resolveTarget(value: unknown): DevtoolsTarget {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Minnow devtools need a database, a worker client, or a Minnow facade");
  }
  const direct = missingMethods(value);
  if (direct.length === 0) return value as DevtoolsTarget;

  const driver: unknown = (value as { driver?: unknown }).driver;
  if (typeof driver === "object" && driver !== null && missingMethods(driver).length === 0) {
    return driver as DevtoolsTarget;
  }
  throw new TypeError(
    `Minnow devtools cannot use this object; it is missing: ${direct.join(", ")}`,
  );
}

/**
 * Whether queries run off the main thread. The worker client answers every call over a channel
 * and so carries `ready`; an in-page `MinnowDatabase` does not. Only the badge depends on this —
 * a wrong guess costs a label, never correctness.
 */
export function runsOffMainThread(target: DevtoolsTarget): boolean {
  return typeof (target as { ready?: unknown }).ready === "function";
}
