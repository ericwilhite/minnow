/** Streaming insert-delta pages. Input ordering belongs to the caller's charged sort/spool.
 * No publication occurs here: the caller stages the returned refs before claiming them in WAL.
 */
import { FTS_NATIVE_GRAMMAR as G } from "../durable-format.js";
import { MAX_FTS_TOKENS_PER_DOCUMENT } from "../full-text.js";
import type { BlobRef, ByteBudget, ExtentWriter } from "../storage/extents.js";
import { pauseIfDue } from "../task.js";
import { valid } from "./bytes.js";
import { ParseBudgetError } from "./manifest-codec.js";
import { writeFtsNode } from "./fts-storage.js";
import {
  FTS_CODEC_SCRATCH_BYTES,
  FTS_ENCODED_OVERHEAD,
  ftsDocumentSummary,
  ftsPostingSummary,
  ftsTermSummary,
  type FtsFormatName,
  type FtsValues,
} from "./fts-codec.js";
import type { FtsDocumentRef, FtsTermRef, FtsTerm } from "./fts.js";

/** One bounded leaf plus fixed fanout/depth descriptors, including owned UTF16 key fences.
 * Caller additionally prepays one encoded page + codec scratch and its input read window.
 */
/** Covers the fixed fields, wide-number boxes, and both owned UTF16 fences. */
export const FTS_APPEND_DESCRIPTOR_BYTES = 1024;
export const FTS_APPEND_WORKSPACE_BYTES =
  16 * 1024 +
  G.leafRows * 11 +
  3 * (G.maximumDepth + 1) * G.fanOut * FTS_APPEND_DESCRIPTOR_BYTES +
  G.termEntries * FTS_APPEND_DESCRIPTOR_BYTES;
export const FTS_APPEND_MINIMUM_BYTES =
  FTS_APPEND_WORKSPACE_BYTES + G.pageBytes + FTS_CODEC_SCRATCH_BYTES + FTS_ENCODED_OVERHEAD;
export const FTS_APPEND_RESULT_BYTES = 2 * FTS_APPEND_DESCRIPTOR_BYTES;
export interface FtsInsertDeltaLease {
  readonly documents: FtsDocumentRef;
  readonly terms: FtsTermRef | null;
  release(): void;
}
class DeltaLease implements FtsInsertDeltaLease {
  constructor(
    private value: { documents: FtsDocumentRef; terms: FtsTermRef | null } | undefined,
    private readonly budget: ByteBudget,
  ) {}
  get documents(): FtsDocumentRef {
    if (!this.value) throw new Error("Released FTS insert delta");
    return this.value.documents;
  }
  get terms(): FtsTermRef | null {
    if (!this.value) throw new Error("Released FTS insert delta");
    return this.value.terms;
  }
  release(): void {
    if (!this.value) return;
    this.value = undefined;
    this.budget.free(FTS_APPEND_RESULT_BYTES);
  }
}
export interface FtsInsertDocument {
  readonly id: number;
  readonly length: number;
  readonly renderable: boolean;
}
export interface FtsInsertPosting {
  readonly term: string;
  readonly id: number;
  readonly frequency: number;
}
type RowKind = "ftsDocuments" | "ftsPostings";
type Ref = FtsDocumentRef | FtsTermRef;
interface Tree<R extends Ref> {
  push(ref: R): Promise<void>;
  finish(): Promise<R>;
}
/** At most fanout descriptors at each level, no whole-delta directory or native sort. */
function tree<R extends Ref>(
  branch: (level: number, children: readonly R[]) => Promise<R>,
): Tree<R> {
  const levels: R[][] = [];
  const pushAt = async (level: number, ref: R): Promise<void> => {
    valid(level <= G.maximumDepth, "FTS append exceeds registered tree depth");
    const entries = levels[level] ?? (levels[level] = []);
    entries.push(ref);
    if (entries.length === G.fanOut) {
      const parent = await branch(level + 1, entries);
      entries.length = 0;
      await pushAt(level + 1, parent);
    }
  };
  return {
    push: (ref) => pushAt(0, ref),
    async finish() {
      for (let level = 0; level < levels.length; level++) {
        const entries = levels[level];
        if (!entries?.length) continue;
        if (entries.length === 1 && !levels.slice(level + 1).some((list) => list.length)) {
          const only = entries[0];
          if (!only) throw new Error("Missing FTS tree frontier");
          return only;
        }
        const parent = await branch(level + 1, entries);
        entries.length = 0;
        await pushAt(level + 1, parent);
      }
      throw new Error("An FTS append tree has no leaves");
    },
  };
}

/** Documents ascending by logical id; postings ascending by UTF16 term then logical id.
 * Both streams must describe the same accepted inserts. Totals/bounds are checked here;
 * generation/epoch, exact document membership and WAL coverage are publisher obligations.
 */
export async function writeFtsInsertDelta(
  documents: AsyncIterable<FtsInsertDocument>,
  postings: AsyncIterable<FtsInsertPosting>,
  budget: ByteBudget,
  writer: ExtentWriter,
  signal?: AbortSignal,
): Promise<FtsInsertDeltaLease> {
  signal?.throwIfAborted();
  if (!budget.allocate(FTS_APPEND_WORKSPACE_BYTES))
    throw new ParseBudgetError(FTS_APPEND_WORKSPACE_BYTES);
  let transferred = false;
  try {
    const ids = new Float64Array(G.leafRows),
      words = new Uint16Array(G.leafRows),
      present = new Uint8Array(G.leafRows),
      termUnits = new Uint16Array(G.termUnits);
    const put = <K extends FtsFormatName>(kind: K, value: FtsValues[K]): Promise<BlobRef> =>
      writeFtsNode(kind, value, budget, writer, signal);
    const rowTree = (kind: RowKind) =>
      tree<FtsDocumentRef>(async (depth, children) => {
        const node = {
          kind: "branch" as const,
          depth,
          count: children.reduce((n, r) => n + r.count, 0),
          tokens: children.reduce((n, r) => n + r.tokens, 0n),
          children,
        };
        const { depth: _depth, ...summary } =
          kind === "ftsDocuments" ? ftsDocumentSummary(node) : ftsPostingSummary(node);
        void _depth;
        return { ref: await put(kind, node), ...summary };
      });
    const flushRows = async (kind: RowKind, count: number, target: Tree<FtsDocumentRef>) => {
      const node =
        kind === "ftsDocuments"
          ? {
              kind: "leaf" as const,
              ids: ids.subarray(0, count),
              lengths: words.subarray(0, count),
              renderable: present.subarray(0, count),
            }
          : {
              kind: "leaf" as const,
              ids: ids.subarray(0, count),
              frequencies: words.subarray(0, count),
            };
      const summary =
        kind === "ftsDocuments"
          ? ftsDocumentSummary(node as FtsValues["ftsDocuments"])
          : ftsPostingSummary(node as FtsValues["ftsPostings"]);
      const { depth: _depth, ...bounds } = summary;
      void _depth;
      await target.push({ ref: await put(kind, node as FtsValues[typeof kind]), ...bounds });
    };
    const docs = rowTree("ftsDocuments");
    let count = 0,
      previous = 0;
    for await (const doc of documents) {
      await pauseIfDue(signal ? { signal } : undefined);
      valid(Number.isSafeInteger(doc.id) && doc.id > previous, "FTS insert document order");
      valid(
        Number.isInteger(doc.length) &&
          doc.length >= 0 &&
          doc.length <= MAX_FTS_TOKENS_PER_DOCUMENT &&
          (doc.renderable || doc.length === 0),
        "Invalid FTS insert document",
      );
      ids[count] = doc.id;
      words[count] = doc.length;
      present[count] = doc.renderable ? 1 : 0;
      previous = doc.id;
      if (++count === G.leafRows) {
        await flushRows("ftsDocuments", count, docs);
        count = 0;
      }
    }
    if (count || previous === 0) await flushRows("ftsDocuments", count, docs);
    const documentRef = await docs.finish();
    const termTree = tree<FtsTermRef>(async (depth, children) => {
      const node = {
        kind: "branch" as const,
        depth,
        count: children.reduce((n, r) => n + r.count, 0n),
        tokens: children.reduce((n, r) => n + r.tokens, 0n),
        children,
      };
      const { depth: _depth, ...summary } = ftsTermSummary(node);
      void _depth;
      return { ref: await put("ftsTerms", node), ...summary };
    });
    const terms: FtsTerm[] = [];
    const flushTerms = async () => {
      const node = { kind: "leaf" as const, entries: terms };
      const { depth: _depth, ...summary } = ftsTermSummary(node);
      void _depth;
      await termTree.push({ ref: await put("ftsTerms", node), ...summary });
      terms.length = 0;
    };
    let term: string | null = null,
      postingTree = rowTree("ftsPostings"),
      termCount = 0,
      total = 0n;
    count = 0;
    previous = 0;
    const finishTerm = async () => {
      if (term === null) return;
      if (count) await flushRows("ftsPostings", count, postingTree);
      terms.push({ term, postings: await postingTree.finish() });
      termCount++;
      if (terms.length === G.termEntries) await flushTerms();
    };
    for await (const post of postings) {
      await pauseIfDue(signal ? { signal } : undefined);
      valid(
        typeof post.term === "string" && post.term.length > 0 && post.term.length <= G.termUnits,
        "Invalid FTS insert term",
      );
      if (post.term !== term) {
        valid(term === null || post.term > term, "FTS insert term order");
        await finishTerm();
        // Do not retain a short slice backed by a large caller-owned input string.
        for (let i = 0; i < post.term.length; i++) termUnits[i] = post.term.charCodeAt(i);
        term = String.fromCharCode(...termUnits.subarray(0, post.term.length));
        postingTree = rowTree("ftsPostings");
        count = 0;
        previous = 0;
      }
      valid(
        Number.isSafeInteger(post.id) &&
          post.id > previous &&
          post.id >= documentRef.first &&
          post.id <= documentRef.last,
        "FTS insert posting bounds/order",
      );
      valid(
        Number.isInteger(post.frequency) &&
          post.frequency > 0 &&
          post.frequency <= MAX_FTS_TOKENS_PER_DOCUMENT,
        "Invalid FTS insert frequency",
      );
      ids[count] = post.id;
      words[count] = post.frequency;
      previous = post.id;
      total += BigInt(post.frequency);
      if (++count === G.leafRows) {
        await flushRows("ftsPostings", count, postingTree);
        count = 0;
      }
    }
    await finishTerm();
    if (terms.length) await flushTerms();
    valid(total === documentRef.tokens, "FTS insert token totals disagree");
    signal?.throwIfAborted();
    const result = new DeltaLease(
      { documents: documentRef, terms: termCount ? await termTree.finish() : null },
      budget,
    );
    transferred = true;
    return result;
  } finally {
    budget.free(FTS_APPEND_WORKSPACE_BYTES - (transferred ? FTS_APPEND_RESULT_BYTES : 0));
  }
}
