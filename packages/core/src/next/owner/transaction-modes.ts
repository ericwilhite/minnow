/** Owner policy over lowering's public mode/effect metadata; no SQL is parsed here. */
import type { TransactionModeItem, TransactionModes } from "../lower/lowerer.js";
import type { SequenceAccess } from "../access/write-path.js";
import { ReadOnlyTransactionError, TransactionModeError } from "./errors.js";

export type AppliedTransactionModes = {
  readonly [K in keyof TransactionModes]: NonNullable<TransactionModes[K]>;
};

/** PG validates each attribute against its own restrictions, in instruction order. */
export function changedTransactionModes(
  current: AppliedTransactionModes,
  items: readonly TransactionModeItem[],
  snapshotTaken: boolean,
  inSubtransaction: boolean,
): AppliedTransactionModes {
  let next = { ...current };
  for (const item of items) {
    switch (item.kind) {
      case "access": {
        const readOnly = item.value === "readOnly";
        if (!readOnly && next.readOnly && (snapshotTaken || inSubtransaction))
          throw new TransactionModeError(
            "Cannot set transaction read-write mode after a query or inside a subtransaction",
          );
        next = { ...next, readOnly };
        break;
      }
      case "isolation":
        if (item.value !== next.isolation && (snapshotTaken || inSubtransaction))
          throw new TransactionModeError(
            "Cannot change transaction isolation after a query or inside a subtransaction",
          );
        next = { ...next, isolation: item.value };
        break;
      case "deferrable":
        if (snapshotTaken || inSubtransaction)
          throw new TransactionModeError(
            "Cannot set transaction deferrable mode after a query or inside a subtransaction",
          );
        next = { ...next, deferrable: item.value };
        break;
    }
  }
  return next;
}

/** Mutations are checked only when execution reaches sequence access; currval stays readable. */
export function guardedSequences(access: SequenceAccess, readOnly: () => boolean): SequenceAccess {
  const writable = (command: string) => {
    if (readOnly()) throw new ReadOnlyTransactionError(command);
  };
  return {
    next: (id, count, out, offset) => {
      writable("nextval()");
      return access.next(id, count, out, offset);
    },
    set: (id, value, isCalled) => {
      writable("setval()");
      return access.set(id, value, isCalled);
    },
  };
}
