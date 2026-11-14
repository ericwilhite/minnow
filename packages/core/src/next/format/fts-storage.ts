/** Native FTS page IO. The caller owns publication/abort; this layer never publishes roots.
 * Encoded buffers remain charged through write settlement. Read leases and decoded ownership
 * survive every await/yield, including cancellation, and are retired in that order.
 */
import type {
  BlobRef,
  ByteBudget,
  ExtentReader,
  ExtentWriter,
  ReadLease,
} from "../storage/extents.js";
import { pauseIfDue } from "../task.js";
import { ManifestCorruptionError } from "./bytes.js";
import {
  encodeFts,
  decodeFts,
  ftsDocumentSummary,
  ftsPostingSummary,
  ftsTermSummary,
  ftsDeltaSummary,
  type FtsValues,
  type FtsFormatName,
} from "./fts-codec.js";
import type { FtsIdRef, FtsTermRef, FtsDeltaRef } from "./fts.js";

export async function writeFtsNode<K extends FtsFormatName>(
  kind: K,
  value: FtsValues[K],
  budget: ByteBudget,
  writer: ExtentWriter,
  signal?: AbortSignal,
): Promise<BlobRef> {
  signal?.throwIfAborted();
  const encoded = encodeFts(kind, value, budget);
  try {
    const result = await writer.write([encoded.bytes]);
    signal?.throwIfAborted();
    const ref = result.blobs[0];
    if (!ref) throw new Error("FTS writer omitted its blob");
    return ref;
  } finally {
    encoded.close();
  }
}

export interface FtsNodeLease<K extends FtsFormatName> {
  readonly value: FtsValues[K];
  release(): void;
}
export async function readFtsNode<K extends FtsFormatName>(
  kind: K,
  ref: BlobRef,
  budget: ByteBudget,
  reader: ExtentReader,
  signal?: AbortSignal,
): Promise<FtsNodeLease<K>> {
  let lease: ReadLease | undefined = await reader.read([ref], {
    budget,
    ...(signal ? { signal } : {}),
  });
  let parsed: ReturnType<typeof decodeFts<K>> | undefined;
  try {
    signal?.throwIfAborted();
    const bytes = lease.bytes[0];
    if (!bytes) throw new ManifestCorruptionError("Missing FTS page bytes");
    parsed = decodeFts(kind, bytes, budget);
    const charge = parsed.charge;
    let live: FtsValues[K] | undefined = parsed.value;
    parsed = undefined;
    return {
      get value() {
        if (live === undefined) throw new Error("Released FTS node");
        return live;
      },
      release() {
        if (live === undefined) return;
        live = undefined;
        const retiring = lease;
        lease = undefined;
        budget.free(charge);
        retiring?.release();
      },
    };
  } catch (error) {
    if (parsed) budget.free(parsed.charge);
    const retiring = lease;
    lease = undefined;
    retiring.release();
    throw error;
  }
}

export type FtsStorageEvent = {
  [K in FtsFormatName]: {
    readonly kind: K;
    readonly ref: BlobRef;
    readonly value: FtsValues[K];
  };
}[FtsFormatName];
type Bounds = FtsIdRef | FtsTermRef | FtsDeltaRef;
function check(ok: boolean, message: string): asserts ok {
  if (!ok) throw new ManifestCorruptionError(message);
}
/** One bounded page per tree level; data is borrowed only until the consumer resumes.
 * There is no global visited set: immutable DAGs are legal and fixed depth bounds cycles.
 * Parent summaries are checked before exposing a child, not trusted for reclamation.
 */
export async function* walkFtsTree(
  ref: BlobRef,
  budget: ByteBudget,
  reader: ExtentReader,
  signal?: AbortSignal,
): AsyncGenerator<FtsStorageEvent> {
  const work = signal ? { signal } : undefined;
  async function* node(
    kind: FtsFormatName,
    ref: BlobRef,
    expected?: {
      depth?: number;
      bounds?: Bounds;
      table?: number;
      epoch?: number;
      column?: number;
      identity?: { ftsId: number; generation: number; mutationEpoch: number };
    },
  ): AsyncGenerator<FtsStorageEvent> {
    await pauseIfDue(work);
    const held = await readFtsNode(kind, ref, budget, reader, signal);
    try {
      const value = held.value;
      let summary: { depth: number } & Record<string, unknown>;
      if (kind === "ftsRoot") {
        const root = value as FtsValues["ftsRoot"];
        summary =
          root.kind === "directory"
            ? {
                depth: root.depth,
                count: root.count,
                first: root.children[0]?.first ?? 0,
                last: root.children.at(-1)?.last ?? 0,
              }
            : { depth: -1, count: 1, first: root.columnId, last: root.columnId };
        if (expected)
          check(
            root.tableId === expected.table && root.mutationEpoch === expected.epoch,
            "FTS child root identity disagrees with parent",
          );
        if (root.kind === "column" && expected?.column !== undefined)
          check(root.columnId === expected.column, "FTS column root disagrees with directory");
      } else if (kind === "ftsTerms") summary = ftsTermSummary(value as FtsValues["ftsTerms"]);
      else if (kind === "ftsDocuments")
        summary = { ...ftsDocumentSummary(value as FtsValues["ftsDocuments"]) };
      else if (kind === "ftsPostings")
        summary = { ...ftsPostingSummary(value as FtsValues["ftsPostings"]) };
      else {
        const delta = value as FtsValues["ftsDelta"];
        summary = ftsDeltaSummary(delta);
        if (expected?.identity)
          for (const key of ["ftsId", "generation", "mutationEpoch"] as const)
            check(delta[key] === expected.identity[key], "FTS delta identity disagrees with root");
      }
      if (expected) {
        if (expected.depth !== undefined)
          check(summary.depth === expected.depth, "FTS child depth disagrees with parent");
        if (expected.bounds)
          for (const [key, val] of Object.entries(expected.bounds))
            if (key !== "ref")
              check(summary[key] === val, "FTS child summary disagrees with parent");
      }
      yield { kind, ref, value } as FtsStorageEvent;
      if (kind === "ftsRoot") {
        const root = value as FtsValues["ftsRoot"];
        if (root.kind === "directory") {
          for (const child of root.children)
            yield* node("ftsRoot", child.ref, {
              depth: root.depth - 1,
              bounds: child,
              table: root.tableId,
              epoch: root.mutationEpoch,
              ...(root.depth === 0 ? { column: child.first } : {}),
            });
        } else {
          if (root.terms) yield* subtree("ftsTerms", root.terms);
          if (root.documents) yield* subtree("ftsDocuments", root.documents);
          if (root.deltas) yield* subtree("ftsDelta", root.deltas, root);
        }
      } else if (kind === "ftsTerms") {
        const terms = value as FtsValues["ftsTerms"];
        if (terms.kind === "branch")
          for (const child of terms.children)
            yield* node("ftsTerms", child.ref, { depth: terms.depth - 1, bounds: child });
        else for (const entry of terms.entries) yield* subtree("ftsPostings", entry.postings);
      } else if (kind === "ftsDocuments" || kind === "ftsPostings") {
        const rows = value as FtsValues["ftsDocuments"] | FtsValues["ftsPostings"];
        if (rows.kind === "branch")
          for (const child of rows.children)
            yield* node(kind, child.ref, { depth: rows.depth - 1, bounds: child });
      } else {
        const delta = value as FtsValues["ftsDelta"];
        if (delta.kind === "branch")
          for (const child of delta.children)
            yield* node("ftsDelta", child.ref, {
              depth: delta.depth - 1,
              bounds: child,
              identity: delta,
            });
        else
          for (const entry of delta.entries) {
            yield* subtree("ftsDocuments", entry.documents);
            if (entry.terms) yield* subtree("ftsTerms", entry.terms);
          }
      }
    } finally {
      held.release();
    }
  }
  async function* subtree(
    kind: Exclude<FtsFormatName, "ftsRoot">,
    bounds: Bounds,
    identity?: { ftsId: number; generation: number; mutationEpoch: number },
  ): AsyncGenerator<FtsStorageEvent> {
    yield* node(kind, bounds.ref, { bounds, ...(identity ? { identity } : {}) });
  }
  yield* node("ftsRoot", ref);
}
