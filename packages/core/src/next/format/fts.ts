/** Native FTS v1: bounded immutable trees, stable logical document IDs, no positions.
 * Portable definitions live in access/fts-def.ts; these references never enter MCAT.
 */
import type { FtsSemantics } from "../fts-config.js";
import type { BlobRef } from "../storage/extents.js";

/** Bounds and counts describe the complete referenced subtree, not just its first page. */
export interface FtsIdRef {
  readonly ref: BlobRef;
  readonly first: number;
  readonly last: number;
  readonly count: number;
}
export interface FtsDocumentRef extends FtsIdRef {
  readonly tokens: bigint;
}
/** JS/UTF16 ordering and startsWith, including v1's possible truncated high surrogate. */
export interface FtsTermRef {
  readonly ref: BlobRef;
  readonly first: string;
  readonly last: string;
  readonly count: bigint;
  readonly tokens: bigint;
}
export interface FtsDeltaRef extends FtsIdRef {
  /** first/last above are commit LSNs; document ID bounds are separate. */
  readonly firstDocument: number;
  readonly lastDocument: number;
  readonly documents: number;
  readonly tokens: bigint;
}

/** One paged binding per table, including its keyed-mutation history with no declarations. */
export interface FtsDirectoryRoot {
  readonly kind: "directory";
  readonly tableId: number;
  readonly mutationEpoch: number;
  /** Zero: children are column roots; otherwise directory children have depth-1. */
  readonly depth: number;
  readonly count: number;
  readonly children: readonly FtsIdRef[];
}
export interface FtsColumnRoot extends FtsSemantics {
  readonly kind: "column";
  readonly ftsId: number;
  readonly tableId: number;
  readonly columnId: number;
  readonly schemaVersion: number;
  readonly generation: number;
  readonly mutationEpoch: number;
  readonly state: "building" | "ready" | "invalid";
  /** Only building roots have an owner/lease. A lease never confers publication authority. */
  readonly owner: string | null;
  readonly leaseUntil: number;
  readonly baseLsn: number;
  readonly throughLsn: number;
  readonly nextRowId: number;
  /** Effective base + delta totals, including NULL and empty documents. */
  readonly documentCount: number;
  readonly tokenCount: bigint;
  readonly terms: FtsTermRef | null;
  readonly documents: FtsDocumentRef | null;
  readonly deltas: FtsDeltaRef | null;
}
export type FtsRoot = FtsDirectoryRoot | FtsColumnRoot;

interface Branch<Child, Count = number> {
  readonly kind: "branch";
  /** At least1; children are depth-1, with leaves at0. */
  readonly depth: number;
  readonly count: Count;
  readonly children: readonly Child[];
}
export interface FtsTerm {
  readonly term: string;
  /** tokens is the sum of frequencies in this postings subtree. */
  readonly postings: FtsDocumentRef;
}
export type FtsTerms =
  | { readonly kind: "leaf"; readonly entries: readonly FtsTerm[] }
  | (Branch<FtsTermRef, bigint> & { readonly tokens: bigint });
export type FtsPostings =
  | {
      readonly kind: "leaf";
      readonly ids: Float64Array;
      readonly frequencies: Uint16Array;
    }
  | (Branch<FtsDocumentRef> & { readonly tokens: bigint });
export type FtsDocuments =
  | {
      readonly kind: "leaf";
      readonly ids: Float64Array;
      readonly lengths: Uint16Array;
      /**0: non-renderable/SQLNULL;1: renderable (possibly an empty string). */
      readonly renderable: Uint8Array;
    }
  | (Branch<FtsDocumentRef> & { readonly tokens: bigint });

export interface FtsDeltaEntry {
  readonly lsn: number;
  readonly documents: FtsDocumentRef;
  readonly terms: FtsTermRef | null;
}
interface DeltaIdentity {
  readonly ftsId: number;
  readonly generation: number;
  readonly mutationEpoch: number;
}
export type FtsDelta = DeltaIdentity &
  (
    | { readonly kind: "leaf"; readonly entries: readonly FtsDeltaEntry[] }
    | (Branch<FtsDeltaRef> & { readonly documents: number; readonly tokens: bigint })
  );
