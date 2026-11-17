/** Prepared owner-private inventory. Never persisted in a manifest, never a recovery authority. */
import type { TaskWork } from "../task.js";
import type { BlobRef, ExtentRef, ByteBudget } from "./extents.js";
export type RetirementInput = Iterable<ExtentRef | BlobRef> | AsyncIterable<ExtentRef | BlobRef>;
export interface RetirementOptions extends TaskWork {
  readonly budget: ByteBudget;
  /** Native retained owners can outlive the creating admission; move only their fixed
   * descriptor allowance into non-evictable memory before exposing the result. */
  readonly retainedOwner?: boolean;
  /** Internal factory funding; working cursors still use budget. */
  readonly ownerBudget?: ByteBudget;
  /** Page candidates deduplicate exactly; strict callers may instead reject repeated extents. */
  readonly duplicates?: "exact" | "refuse";
  /** References in the NEW root, excluding old roots/pins (R8 already protects those).
   * Required for page candidates that can share/pack a live extent. */
  readonly retained?: RetirementInput;
}
export interface RetirementCursor {
  readonly offset: number;
  readonly length: number;
  /** Scalar fast path inside one fixed window; no metadata frame escapes across awaits. */
  next(work?: TaskWork): boolean | Promise<boolean>;
  close(): Promise<void>;
}
/** Pins only its private descriptor file, NOT data: retain the source root pin separately. */
export interface RetirementInventory {
  readonly count: number;
  cursor(options: { readonly budget: ByteBudget; readonly signal?: AbortSignal }): RetirementCursor;
  /** Stops new uses, drains accepted readers/publications and returns all owned credit. */
  release(): Promise<void>;
}
