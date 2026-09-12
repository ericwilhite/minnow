import type { TableColumnRecord, TableRecord } from "../storage/types.js";
import type { BatchValue } from "./batch.js";

/**
 * A write scope's per-table write set: the memtable of statements not yet encoded into
 * segments, and a mirror of what its staged segments already hold per key.
 *
 * Every plain mutation inside a scope folds into the set instead of encoding a segment of its
 * own — an insert, an update, a delete, and an upsert of the same key become one net effect —
 * and the set turns into a handful of segments only when something needs them: a read of the
 * table, a savepoint, the commit, or a block's worth of rows waiting. The mirror lets the keyed
 * lookups the engine makes for its own checks (upsert classification, pre-images for CHECK
 * constraints and indexes) answer from the set plus the committed snapshot, so those checks
 * never force the encoding either.
 */

/** A scope's net effect on one key: a whole row for an insert or upsert, a patch for an update. */
export interface ScopeRowEffect {
  kind: "insert" | "upsert" | "update" | "delete";
  /** Values by table column position; an update leaves the columns it did not touch undefined. */
  values: Array<BatchValue | undefined>;
}

export interface ScopeRowEntry {
  /** The key value; absent for a keyless table's rows, which only ever insert. */
  key: Exclude<BatchValue, null> | undefined;
  /** Net effect of the segments the scope has already staged for this key. */
  staged?: ScopeRowEffect;
  /** Net effect of the statements not yet encoded. */
  pending?: ScopeRowEffect;
}

export interface ScopeWriteSet {
  table: TableRecord;
  keyColumn: TableColumnRecord | undefined;
  rows: Map<string, ScopeRowEntry>;
  /** Keys with a pending effect. */
  pendingRows: number;
  /** Estimated bytes of the pending effects. */
  pendingBytes: number;
  /** Estimated bytes the mirror retains for staged effects. */
  mirrorBytes: number;
  /** A segment for this table was staged outside the write set: the mirror is incomplete. */
  opaque: boolean;
  /** Synthetic token source for a keyless table's rows. */
  unkeyedSequence: number;
}

export interface ScopeWriteState {
  tables: Map<string, ScopeWriteSet>;
  /**
   * False once the mirrors were dropped — over budget, or after a savepoint rollback — so keyed
   * lookups read through the transaction overlay from then on.
   */
  mirrored: boolean;
  /** Bumped before any set changes, so a statement that fails after buffering poisons its scope. */
  generation: number;
  /** An encoding that failed part-way: the scope can only roll back. */
  failure?: unknown;
  /** The flush in progress, if any; the next one waits for it. */
  flushing: Promise<void>;
}

/** Pending effects encode at this many estimated bytes per scope; mirrors drop past it too. */
export const DEFAULT_SCOPE_WRITE_SET_BUDGET_BYTES = 64 * 1024 * 1024;

/** Test seam: shrinks the budget so the mirror-dropped path runs on small data. */
export const scopeWriteSetTestHooks = {
  defaultBudgetBytes: DEFAULT_SCOPE_WRITE_SET_BUDGET_BYTES,
  budgetBytes: DEFAULT_SCOPE_WRITE_SET_BUDGET_BYTES,
};

/**
 * The net effect of `next` applied after `base`. A row deleted and inserted again in one scope
 * replaces the committed row in place; an upsert over a row the scope inserted stays an insert,
 * since nothing committed exists to replace; an update patches whatever came before it.
 */
export function composeScopeEffect(
  base: ScopeRowEffect | undefined,
  next: ScopeRowEffect,
): ScopeRowEffect {
  switch (next.kind) {
    case "delete":
      return next;
    case "insert":
      return base?.kind === "delete" ? { kind: "upsert", values: next.values } : next;
    case "upsert":
      return base?.kind === "insert" ? { kind: "insert", values: next.values } : next;
    case "update": {
      if (base === undefined) return next;
      if (base.kind === "delete") throw new Error("A deleted key cannot take an update");
      const values = base.values.slice();
      next.values.forEach((value, position) => {
        if (value !== undefined) values[position] = value;
      });
      return { kind: base.kind, values };
    }
  }
}

/** What the scope holds for a key, staged and pending together. */
export function netScopeEffect(entry: ScopeRowEntry): ScopeRowEffect | undefined {
  return entry.pending === undefined
    ? entry.staged
    : composeScopeEffect(entry.staged, entry.pending);
}
