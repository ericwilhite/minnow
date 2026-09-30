/** Pure transaction rules shared by native IndexedDB transactions and RecordCore.
 * No I/O, mutation, clock or adapter cache belongs here. Callers validate before opening or
 * publishing an atomic adapter transition; storage mechanics remain adapter-owned. */
import {
  MAX_AUTO_INCREMENT_EXCLUSIVE_END,
  type BeginTransactionInput,
  type TransactionRecord,
  type TransactionRecordUpdate,
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
