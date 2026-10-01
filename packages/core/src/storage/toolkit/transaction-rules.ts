/** Pure transaction rules shared by native IndexedDB transactions and RecordCore.
 * No I/O, mutation, clock or adapter cache belongs here. Callers validate before opening or
 * publishing an atomic adapter transition; storage mechanics remain adapter-owned. */
import {
  MAX_AUTO_INCREMENT_EXCLUSIVE_END,
  type BeginTransactionInput,
  type TransactionRecord,
  type TransactionRecordUpdate,
  type SegmentRecord,
  type TableRecord,
  type TableRecordUpdate,
  secondaryIndexColumnIds,
  validateStorageId,
  MAX_LEVEL_ZERO_SEGMENTS,
  SchemaConflictError,
  WriteConflictError,
} from "../types.js";

export function validateAutoIncrementReservation(count: number, atLeast: bigint | undefined): void {
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new RangeError("Auto-increment reservation count must be a non-negative whole number");
  }
  if (
    atLeast !== undefined &&
    (typeof atLeast !== "bigint" || atLeast < 1n || atLeast > MAX_AUTO_INCREMENT_EXCLUSIVE_END)
  ) {
    throw new RangeError(
      `Auto-increment bump target must be between 1 and ${String(MAX_AUTO_INCREMENT_EXCLUSIVE_END)}`,
    );
  }
}

export function validateBeginTransactionInput(input: BeginTransactionInput): void {
  if (input.record.pendingBlockIds.length > 0 || input.record.pendingSegmentIds.length > 0) {
    throw new TypeError("A fresh transaction cannot begin with pending artifacts");
  }
  if (
    input.record.pendingTable !== undefined ||
    input.record.pendingTableNextRowId !== undefined ||
    input.record.catalogEpochGuard !== undefined ||
    (input.record as TransactionRecord).schemaEpochGuard !== undefined
  ) {
    throw new TypeError("Storage-owned transaction state cannot be supplied at begin");
  }
}

export function assertGenericTransactionUpdateAllowed(
  record: Pick<TransactionRecord, "status">,
  update: TransactionRecordUpdate,
): void {
  if (record.status !== "active") {
    throw new TypeError(`Only active transactions can be updated; found ${record.status}`);
  }
  if (update.status === "committed") {
    throw new TypeError("Use commitTransaction to commit a transaction");
  }
  if (Reflect.has(update, "committedVersion")) {
    throw new TypeError("Only commitTransaction can set a committed transaction version");
  }
}

/** Validate freshness at the same atomic publication boundary on every adapter. */
export function assertCommitSchema(record: TransactionRecord, schemaEpoch: number): void {
  if (record.schemaEpochGuard !== schemaEpoch) {
    throw new SchemaConflictError(record.schemaEpochGuard ?? -1, schemaEpoch);
  }
}

export function assertCommitSnapshot(
  record: TransactionRecord,
  expected: number | null,
  actual: number | null,
): void {
  if (actual !== expected) throw new WriteConflictError(expected, actual);
  if (record.snapshotVersion !== expected) {
    throw new Error("Transaction snapshot does not match the expected manifest");
  }
}

/** Pure admission plan; visibility counts and atomic writes remain adapter-owned. Counting
 * the pending journal once also avoids rescanning it for every table in a multi-table commit. */
export function planLevelZeroAdmissions(
  limits: ReadonlyArray<{ tableId: string; limit: number }>,
  pendingSegments: readonly SegmentRecord[],
): ReadonlyMap<string, { limit: number; added: number }> {
  const pending = new Map<string, number>();
  for (const segment of pendingSegments) {
    if (segment.level !== 0) continue;
    pending.set(segment.tableId, (pending.get(segment.tableId) ?? 0) + 1);
  }
  if (limits.length !== pending.size) {
    throw new TypeError("Level-zero segment limits must exactly cover pending level-zero tables");
  }
  const plan = new Map<string, { limit: number; added: number }>();
  for (const entry of limits) {
    validateStorageId(entry.tableId, "Level-zero table ID");
    if (
      !Number.isSafeInteger(entry.limit) ||
      entry.limit <= 0 ||
      entry.limit > MAX_LEVEL_ZERO_SEGMENTS
    ) {
      throw new RangeError(
        `Level-zero segment limit must be between 1 and ${String(MAX_LEVEL_ZERO_SEGMENTS)}`,
      );
    }
    if (plan.has(entry.tableId)) {
      throw new TypeError(`Level-zero segment limit is duplicated: ${entry.tableId}`);
    }
    const added = pending.get(entry.tableId);
    if (added === undefined) {
      throw new TypeError(`Level-zero segment limit has no pending table: ${entry.tableId}`);
    }
    plan.set(entry.tableId, { limit: entry.limit, added });
  }
  return plan;
}

/** A catalog alteration retains only accelerators whose entire column tuple survives. This
 * plan has no I/O and does not mutate caller records; each adapter validates and publishes it
 * within its own atomic catalog transition. */
export function planTableAccelerators(
  record: TableRecord,
  update: TableRecordUpdate,
): {
  nextFts: TableRecordUpdate["ftsColumns"];
  nextSecondary: TableRecordUpdate["secondaryIndexes"];
  retainedColumnIds: ReadonlySet<string> | undefined;
} {
  let nextFts = update.ftsColumns === undefined ? record.ftsColumns : update.ftsColumns;
  let nextSecondary =
    update.secondaryIndexes === undefined ? record.secondaryIndexes : update.secondaryIndexes;
  const retainedColumnIds =
    update.columns === undefined ? undefined : new Set(update.columns.map(({ id }) => id));
  if (nextFts != null && retainedColumnIds !== undefined) {
    nextFts = Object.fromEntries(
      Object.entries(nextFts).filter(([id]) => retainedColumnIds.has(id)),
    );
    if (Object.keys(nextFts).length === 0) nextFts = null;
  }
  if (nextSecondary != null && retainedColumnIds !== undefined) {
    nextSecondary = Object.fromEntries(
      Object.entries(nextSecondary).filter(([, index]) =>
        secondaryIndexColumnIds(index).every((id) => retainedColumnIds.has(id)),
      ),
    );
    if (Object.keys(nextSecondary).length === 0) nextSecondary = null;
  }
  return { nextFts, nextSecondary, retainedColumnIds };
}
