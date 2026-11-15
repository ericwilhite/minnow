/** MFTJ1: protected preparation pages and actual-LSN checkpoints. Never portable MCAT.
 * Prepared pages are invisible until one atomic row/WAL body claims their root sequence.
 * A checkpoint links that preparation at its actual (last multipart frame) commit LSN.
 * Linked pages have strictly decreasing ordinals/LSNs, checked before following any edge.
 */
import { DURABLE_FORMATS } from "../durable-format.js";
import type { BlobRef, ByteBudget, ExtentReader, ExtentWriter } from "../storage/extents.js";
import { walkFtsTree } from "./fts-storage.js";
import { pauseIfDue } from "../task.js";
import { ByteReader, ByteWriter, valid } from "./bytes.js";
import { validateFtsReference } from "./fts-codec.js";
import type { FtsDocumentRef, FtsTermRef } from "./fts.js";
import { ParseBudgetError } from "./manifest-codec.js";
import { UnsupportedEncodingError } from "./encoding.js";

export const FTS_JOURNAL_ENTRIES = 64;
export const FTS_JOURNAL_PAGE_BYTES = 64 * 1024;
/** Includes geometric encoder growth + old buffer + finished copy + parsed descriptors. */
export const FTS_JOURNAL_WORKSPACE_BYTES = 256 * 1024;
export interface FtsInsertPart {
  readonly tableId: number;
  readonly schemaVersion: number;
  readonly ftsId: number;
  readonly columnId: number;
  readonly generation: number;
  readonly mutationEpoch: number;
  readonly kind: "insert";
  readonly documents: FtsDocumentRef;
  readonly terms: FtsTermRef | null;
}
/** For table-wide keyed entries mutationEpoch is the first accepted commit LSN in
 * this checkpoint interval. It preserves the snapshot boundary when entries share a page. */
export interface FtsInvalidation {
  readonly kind: "invalidate" | "keyed";
  readonly tableId: number;
  readonly schemaVersion: number;
  /** Zero invalidates all bindings on the table; keyed history survives without declarations. */
  readonly ftsId: number;
  readonly columnId: number;
  readonly generation: number;
  readonly mutationEpoch: number;
}
export type FtsJournalEntry = FtsInsertPart | FtsInvalidation;
export interface FtsJournalLink {
  readonly previous: BlobRef | null;
}
export interface FtsPreparedPage extends FtsJournalLink {
  readonly kind: "prepared";
  readonly transaction: number;
  readonly ordinal: number;
  readonly entries: readonly FtsJournalEntry[];
}
export interface FtsCheckpointPage extends FtsJournalLink {
  readonly kind: "checkpoint";
  readonly transaction: number;
  readonly lsn: number;
  readonly staged: BlobRef;
}
export type FtsJournal = FtsPreparedPage | FtsCheckpointPage;
const grammar = DURABLE_FORMATS.ftsJournal;
function positive(n: number, label: string): void {
  valid(Number.isSafeInteger(n) && n > 0, "Invalid FTS journal " + label);
}
function nonnegative(n: number): void {
  valid(Number.isSafeInteger(n) && n >= 0, "Invalid FTS journal integer");
}
function blob(out: ByteWriter, r: BlobRef): void {
  validateFtsReference(r);
  out.uint(r.extent.offset);
  out.uint(r.extent.length);
  out.uint(r.offset);
  out.uint(r.length);
  out.u32(r.checksum);
}
function readBlob(input: ByteReader): BlobRef {
  const r = {
    extent: { offset: input.uint(), length: input.uint() },
    offset: input.uint(),
    length: input.uint(),
    checksum: input.u32(),
  };
  validateFtsReference(r);
  return r;
}
function optional(out: ByteWriter, ref: BlobRef | null): void {
  out.u8(ref === null ? 0 : 1);
  if (ref) blob(out, ref);
}
function readOptional(input: ByteReader): BlobRef | null {
  const tag = input.u8();
  valid(tag <= 1, "Invalid FTS journal optional flag");
  return tag === 0 ? null : readBlob(input);
}
function document(out: ByteWriter, r: FtsDocumentRef): void {
  positive(r.count, "document count");
  positive(r.first, "first document");
  valid(
    r.last >= r.first &&
      r.count <= r.last - r.first + 1 &&
      r.tokens >= 0n &&
      r.tokens <= BigInt(r.count) * 4096n,
    "Invalid FTS journal document bounds",
  );
  blob(out, r.ref);
  out.uint(r.first);
  out.uint(r.last);
  out.uint(r.count);
  out.bigint(r.tokens);
}
function readDocument(input: ByteReader): FtsDocumentRef {
  return {
    ref: readBlob(input),
    first: input.uint(),
    last: input.uint(),
    count: input.uint(),
    tokens: input.bigint(),
  };
}
function text(out: ByteWriter, s: string): void {
  valid(s.length > 0 && s.length <= 32, "Invalid FTS journal term fence");
  out.uint(s.length);
  for (let i = 0; i < s.length; i++) out.uint(s.charCodeAt(i));
}
function readText(input: ByteReader): string {
  const n = input.count(1);
  valid(n > 0 && n <= 32, "Invalid FTS journal term fence length");
  let s = "";
  for (let i = 0; i < n; i++) {
    const c = input.uint();
    valid(c <= 65535, "Invalid FTS journal UTF16 unit");
    s += String.fromCharCode(c);
  }
  return s;
}
function term(out: ByteWriter, r: FtsTermRef): void {
  valid(
    r.count > 0n && r.count <= r.tokens && (r.count === 1n ? r.first === r.last : r.first < r.last),
    "Invalid FTS journal term bounds",
  );
  blob(out, r.ref);
  text(out, r.first);
  text(out, r.last);
  out.bigint(r.count);
  out.bigint(r.tokens);
}
function readTerm(input: ByteReader): FtsTermRef {
  return {
    ref: readBlob(input),
    first: readText(input),
    last: readText(input),
    count: input.bigint(),
    tokens: input.bigint(),
  };
}
function emit(value: FtsJournal): Uint8Array {
  const out = new ByteWriter();
  out.u32(grammar.magic);
  out.u8(grammar.version);
  out.u8(0);
  out.u8(value.kind === "prepared" ? 0 : 1);
  out.u8(0);
  positive(value.transaction, "transaction");
  out.uint(value.transaction);
  if (value.kind === "checkpoint") {
    positive(value.lsn, "LSN");
    out.uint(value.lsn);
    optional(out, value.previous);
    blob(out, value.staged);
  } else {
    positive(value.ordinal, "ordinal");
    valid(
      (value.ordinal === 1) === (value.previous === null),
      "FTS journal ordinal/previous mismatch",
    );
    valid(
      value.entries.length > 0 && value.entries.length <= FTS_JOURNAL_ENTRIES,
      "Invalid FTS journal entry count",
    );
    out.uint(value.ordinal);
    optional(out, value.previous);
    out.uint(value.entries.length);
    for (const e of value.entries) {
      out.u8(e.kind === "insert" ? 0 : e.kind === "invalidate" ? 1 : 2);
      positive(e.tableId, "table");
      positive(e.schemaVersion, "schema");
      nonnegative(e.ftsId);
      nonnegative(e.columnId);
      nonnegative(e.generation);
      nonnegative(e.mutationEpoch);
      for (const n of [
        e.tableId,
        e.schemaVersion,
        e.ftsId,
        e.columnId,
        e.generation,
        e.mutationEpoch,
      ])
        out.uint(n);
      if (e.kind === "insert") {
        positive(e.ftsId, "index");
        positive(e.columnId, "column");
        positive(e.generation, "generation");
        document(out, e.documents);
        out.u8(e.terms === null ? 0 : 1);
        if (e.terms) {
          term(out, e.terms);
          valid(e.terms.tokens === e.documents.tokens, "FTS journal token totals disagree");
        } else valid(e.documents.tokens === 0n, "FTS journal missing nonempty terms");
      }
    }
  }
  const bytes = out.finish();
  valid(bytes.length <= FTS_JOURNAL_PAGE_BYTES, "FTS journal page exceeds cap");
  return bytes;
}
function parse(bytes: Uint8Array): FtsJournal {
  const input = new ByteReader(bytes);
  valid(input.u32() === grammar.magic, "Bad FTS journal magic");
  const version = input.u8();
  if (version !== grammar.version)
    throw new UnsupportedEncodingError("FTS journal", version, [grammar.version]);
  valid(input.u8() === 0, "Reserved FTS journal flags");
  const kind = input.u8();
  valid(kind <= 1, "Unknown FTS journal node");
  valid(input.u8() === 0, "Reserved FTS journal byte");
  const transaction = input.uint();
  let value: FtsJournal;
  if (kind === 1)
    value = {
      kind: "checkpoint",
      transaction,
      lsn: input.uint(),
      previous: readOptional(input),
      staged: readBlob(input),
    };
  else {
    const ordinal = input.uint(),
      previous = readOptional(input),
      count = input.count(7);
    valid(count <= FTS_JOURNAL_ENTRIES, "FTS journal entry count exceeds cap");
    const entries: FtsJournalEntry[] = [];
    for (let i = 0; i < count; i++) {
      const tag = input.u8();
      valid(tag <= 2, "Unknown FTS journal entry");
      const base = {
        tableId: input.uint(),
        schemaVersion: input.uint(),
        ftsId: input.uint(),
        columnId: input.uint(),
        generation: input.uint(),
        mutationEpoch: input.uint(),
      };
      if (tag === 0) {
        const documents = readDocument(input),
          flag = input.u8();
        valid(flag <= 1, "Invalid FTS journal terms flag");
        entries.push({
          ...base,
          kind: "insert",
          documents,
          terms: flag === 0 ? null : readTerm(input),
        });
      } else entries.push({ ...base, kind: tag === 1 ? "invalidate" : "keyed" });
    }
    value = { kind: "prepared", transaction, ordinal, previous, entries };
  }
  input.end();
  emit(value);
  return value;
}
export async function writeFtsJournal(
  value: FtsJournal,
  budget: ByteBudget,
  writer: ExtentWriter,
  signal?: AbortSignal,
): Promise<BlobRef> {
  signal?.throwIfAborted();
  if (!budget.allocate(FTS_JOURNAL_WORKSPACE_BYTES))
    throw new ParseBudgetError(FTS_JOURNAL_WORKSPACE_BYTES);
  try {
    const bytes = emit(value),
      result = await writer.write([bytes]);
    signal?.throwIfAborted();
    const ref = result.blobs[0];
    valid(ref !== undefined, "Missing FTS journal output");
    return ref;
  } finally {
    budget.free(FTS_JOURNAL_WORKSPACE_BYTES);
  }
}
export async function readFtsJournal(
  ref: BlobRef,
  budget: ByteBudget,
  reader: ExtentReader,
  signal?: AbortSignal,
) {
  signal?.throwIfAborted();
  validateFtsReference(ref);
  valid(ref.length <= FTS_JOURNAL_PAGE_BYTES, "FTS journal reference exceeds cap");
  if (!budget.allocate(FTS_JOURNAL_WORKSPACE_BYTES))
    throw new ParseBudgetError(FTS_JOURNAL_WORKSPACE_BYTES);
  let live: FtsJournal | undefined;
  try {
    const lease = await reader.read([ref], { budget, ...(signal ? { signal } : {}) });
    try {
      signal?.throwIfAborted();
      const bytes = lease.bytes[0];
      valid(bytes !== undefined, "Missing FTS journal input");
      live = parse(bytes);
    } finally {
      lease.release();
    }
  } catch (error) {
    budget.free(FTS_JOURNAL_WORKSPACE_BYTES);
    throw error;
  }
  return {
    get value() {
      if (!live) throw new Error("Released FTS journal");
      return live;
    },
    release() {
      if (live) {
        live = undefined;
        budget.free(FTS_JOURNAL_WORKSPACE_BYTES);
      }
    },
  };
}
/** Borrowed entries, one funded page at a time. No history-sized map or recursion. */
export async function* ftsPreparedEntries(
  ref: BlobRef,
  budget: ByteBudget,
  reader: ExtentReader,
  signal?: AbortSignal,
) {
  let next: BlobRef | null = ref,
    expected: number | undefined,
    transaction: number | undefined;
  while (next) {
    await pauseIfDue(signal ? { signal } : undefined);
    const held = await readFtsJournal(next, budget, reader, signal);
    try {
      const page = held.value;
      valid(page.kind === "prepared", "FTS claim does not name a preparation");
      valid(expected === undefined || page.ordinal === expected, "FTS preparation ordinal differs");
      valid(
        transaction === undefined || page.transaction === transaction,
        "FTS preparation transaction differs",
      );
      transaction = page.transaction;
      expected = page.ordinal - 1;
      next = page.previous;
      for (const entry of page.entries) yield entry;
    } finally {
      held.release();
    }
  }
}

/** Full R25 audit/retirement walk. The caller can deduplicate shared extents, never references. */
export async function* walkFtsJournal(
  initial: BlobRef,
  checkpoint: boolean,
  budget: ByteBudget,
  reader: ExtentReader,
  signal?: AbortSignal,
): AsyncGenerator<{ ref: BlobRef; tableId: number | null }> {
  async function* prepared(
    ref: BlobRef,
    transaction?: number,
  ): AsyncGenerator<{ ref: BlobRef; tableId: number | null }> {
    let next: BlobRef | null = ref,
      ordinal: number | undefined;
    while (next) {
      await pauseIfDue(signal ? { signal } : undefined);
      const held = await readFtsJournal(next, budget, reader, signal);
      try {
        const page = held.value;
        valid(page.kind === "prepared", "FTS prepared chain has checkpoint");
        valid(
          transaction === undefined || page.transaction === transaction,
          "FTS preparation identity differs",
        );
        valid(ordinal === undefined || page.ordinal === ordinal, "FTS preparation ordinal differs");
        transaction = page.transaction;
        ordinal = page.ordinal - 1;
        yield { ref: next, tableId: null };
        next = page.previous;
        for (const e of page.entries)
          if (e.kind === "insert") {
            for await (const event of walkFtsTree(e.documents.ref, budget, reader, signal, {
              kind: "ftsDocuments",
              bounds: e.documents,
            }))
              yield { ref: event.ref, tableId: e.tableId };
            if (e.terms)
              for await (const event of walkFtsTree(e.terms.ref, budget, reader, signal, {
                kind: "ftsTerms",
                bounds: e.terms,
              }))
                yield { ref: event.ref, tableId: e.tableId };
          }
      } finally {
        held.release();
      }
    }
  }
  if (!checkpoint) {
    yield* prepared(initial);
    return;
  }
  let next: BlobRef | null = initial,
    ceiling = Infinity;
  while (next) {
    await pauseIfDue(signal ? { signal } : undefined);
    const held = await readFtsJournal(next, budget, reader, signal);
    try {
      const page = held.value;
      valid(page.kind === "checkpoint", "FTS checkpoint chain has preparation");
      valid(page.lsn < ceiling, "FTS checkpoint LSN does not decrease");
      ceiling = page.lsn;
      yield { ref: next, tableId: null };
      yield* prepared(page.staged, page.transaction);
      next = page.previous;
    } finally {
      held.release();
    }
  }
}

/** Registered standalone codecs. The returned allocation remains owned until close/free. */
export function encodeFtsJournal(value: FtsJournal, budget: ByteBudget) {
  if (!budget.allocate(FTS_JOURNAL_WORKSPACE_BYTES))
    throw new ParseBudgetError(FTS_JOURNAL_WORKSPACE_BYTES);
  let live: Uint8Array | undefined;
  try {
    live = emit(value);
  } catch (error) {
    budget.free(FTS_JOURNAL_WORKSPACE_BYTES);
    throw error;
  }
  return {
    get bytes() {
      if (!live) throw new Error("Released FTS journal encoding");
      return live;
    },
    close() {
      if (live) {
        live = undefined;
        budget.free(FTS_JOURNAL_WORKSPACE_BYTES);
      }
    },
  };
}
export function decodeFtsJournal(bytes: Uint8Array, budget: ByteBudget) {
  valid(bytes.length <= FTS_JOURNAL_PAGE_BYTES, "FTS journal page exceeds cap");
  if (!budget.allocate(FTS_JOURNAL_WORKSPACE_BYTES))
    throw new ParseBudgetError(FTS_JOURNAL_WORKSPACE_BYTES);
  try {
    return { value: parse(bytes), charge: FTS_JOURNAL_WORKSPACE_BYTES };
  } catch (error) {
    budget.free(FTS_JOURNAL_WORKSPACE_BYTES);
    throw error;
  }
}
