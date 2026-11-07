/** Owner policy over lowering's public mode/effect metadata; no SQL is parsed here. */
import type { TransactionModes } from "../lower/lowerer.js";
import type { SequenceAccess } from "../access/write-path.js";
import { ReadOnlyTransactionError, TransactionModeError } from "./errors.js";

export type AppliedTransactionModes = {
  readonly [K in keyof TransactionModes]: NonNullable<TransactionModes[K]>;
};

/** Pending B6 public metadata: instructions in SQL order, alongside the final modes. */
export type TransactionModeItem =
  | { readonly kind: "access"; readonly value: "readOnly" | "readWrite" }
  | { readonly kind: "isolation"; readonly value: AppliedTransactionModes["isolation"] }
  | { readonly kind: "deferrable"; readonly value: boolean };
export interface OrderedTransactionModes {
  readonly modeItems?: readonly TransactionModeItem[];
}
/** Effective locks after view/CTE expansion. Pending B6's public lowering field. */
export interface EffectiveRowLocking {
  readonly rowLocking?: "none" | "UPDATE" | "NO KEY UPDATE" | "SHARE" | "KEY SHARE";
}

/** PG validates each attribute against its own restrictions, in instruction order. */
export function changedTransactionModes(
  current: AppliedTransactionModes,
  modes: TransactionModes,
  items: readonly TransactionModeItem[] | undefined,
  snapshotTaken: boolean,
  inSubtransaction: boolean,
): AppliedTransactionModes {
  const ordered: TransactionModeItem[] = [];
  if (modes.isolation !== null) ordered.push({ kind: "isolation", value: modes.isolation });
  if (modes.readOnly !== null)
    ordered.push({ kind: "access", value: modes.readOnly ? "readOnly" : "readWrite" });
  if (modes.deferrable !== null) ordered.push({ kind: "deferrable", value: modes.deferrable });
  let next = { ...current };
  for (const item of items ?? ordered) {
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
