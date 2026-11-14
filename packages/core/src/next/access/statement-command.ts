import type { Batch } from "../vector/vector.js";

/** Native command/trigger boundary. Capability remains absent until its bounded registry and
 * executor hooks are installed; this contract does not enable trigger or RI scheduling.
 * Queue packets contain scalar stable IDs only. The admitted root owns their paged registries,
 * image/target spools and pins, and validates root origin plus command/frame liveness on use. */

export type WriteEvent = "insert" | "update" | "delete";
export interface CommandToken {
  readonly rootId: number;
  readonly id: number;
  readonly frameId: number;
}
export interface RowImageRef {
  readonly spoolId: number;
  readonly row: number;
}
/** Captured native child identities/provenance, never a resident row-ID array. `follow` captures
 * original matches for all parent keys before any RI write, preserving key-swap behavior.
 * Informational NOT ENFORCED references also follow; capture them by bounded scan/spool
 * when no support index exists. Their delete actions and existence checks remain skipped. */
export interface RowTargetRange {
  readonly spoolId: number;
  readonly from: number;
  readonly count: number;
}
export interface ReferentialEntry {
  readonly id: number;
  readonly commandId: number;
  readonly frameId: number;
  readonly fkId: number;
  readonly event: "childInsert" | "childUpdate" | "parentDelete" | "parentUpdate";
  readonly kind:
    "checkReference" | "follow" | "cascade" | "setNull" | "setDefault" | "restrict" | "noAction";
  /** Durable native name, ordered as UTF8 with user AFTER names. */
  readonly sortName: string;
  readonly images: RowImageRef;
  /** Captured original-identity range when materialized. Deferred follow plans resolve through
   * id in the original-identity registry; never recapture an earlier follow rewrite by value. */
  readonly targets: RowTargetRange | null;
  readonly triggerBudget: number;
}
export interface NativeRowHooks {
  /** Generated NEW values are placeholders here. One row continues to rules/primary before
   * the next row's BEFORE runs; a later BEFORE error must not mask an earlier row's CHECK. */
  beforeRow(
    command: CommandToken,
    tableId: number,
    event: WriteEvent,
    images: RowImageRef,
    budget: number,
  ): Promise<void>;
  /** Accepted OLD/NEW only. Append AFTER packets; never drain them synchronously for RI.
   * Body commands drain their nested frame; RI children append to the containing outer frame. */
  afterRow(
    command: CommandToken,
    tableId: number,
    event: WriteEvent,
    images: RowImageRef,
    referential: readonly ReferentialEntry[],
    budget: number,
  ): Promise<void>;
}
/** One root-funded borrow, exactly one physical row in each non-null image. Columns are in
 * the owner's complete physical table-column order. Borrowers neither mutate nor retain vectors
 * after release. Release is synchronous/idempotent; pending I/O finishes before lease delivery. */
export interface RowImageLease {
  readonly tableId: number;
  readonly old: Batch | null;
  readonly new: Batch | null;
  release(): void;
}
export interface NativeCommandBoundary {
  /** One image helper slot in the shared root scope, independent of RI depth. Caller copies only
   * referenced bindings to prepaid workspace, then releases BEFORE any producer/admission wait.
   * Repeated bindings share the copied vector; no per-occurrence payload copies or JS row values.
   * See exec/row-image-layout.ts for the fixed minimum and exact-payload progress envelope. */
  readRowImages(images: RowImageRef, signal: AbortSignal): Promise<RowImageLease>;
  beginCommand(frameId: number, triggerBudget: number): CommandToken;
  /** Includes child c_ reference checks, not just parent a_ actions. Exposed after primary succeeds. */
  rowReferentialEntries(command: CommandToken, row: RowImageRef): AsyncIterable<ReferentialEntry>;
  /** Non-ending command checkpoint BEFORE any AFTER event for this command. */
  verifyCommandUnique(command: CommandToken, signal: AbortSignal): Promise<void>;
  /** Queue adapter: validate/resolve the root-owned scalar ID internally, then execute the
   * immutable entry. Foreign/retired IDs refuse before effects. No entry/key buffers escape
   * to the queue. Resolve I/O is fenced before
   * callbacks; decoded entry metadata uses the root's prepaid ordinary frame workspace. */
  executeReferentialId(id: number, hooks: NativeRowHooks, signal: AbortSignal): Promise<void>;
  /** Exactly one parent row/one FK event. Seal the complete child input before mutation;
   * release input/pin/producer progress before writer admission. No nested governor admission.
   * Follow uses captured identities; explicit actions use their distinct catalog mode.
   * RESTRICT is23001; NO ACTION/missing-parent23503. RI preserves trigger budget. */
  executeReferential(
    entry: ReferentialEntry,
    hooks: NativeRowHooks,
    signal: AbortSignal,
  ): Promise<void>;
  /** Residual work for this command only; do not drain outer RI or end/commit the root. */
  verifyCommandReferences(command: CommandToken, signal: AbortSignal): Promise<void>;
}
