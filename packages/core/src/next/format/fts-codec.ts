/** Registered FTS v1 pages: exact owned output, bounded parsing, raw UTF16 term keys. */
import { DURABLE_FORMATS, DURABLE_CODECS, FTS_NATIVE_GRAMMAR as G } from "../durable-format.js";
import { MAX_FTS_TOKENS_PER_DOCUMENT } from "../full-text.js";
import { requireFtsSemantics } from "../fts-config.js";
import type { BlobRef, ByteBudget } from "../storage/extents.js";
import { ByteReader, valid } from "./bytes.js";
import { UnsupportedEncodingError } from "./encoding.js";
import { ParseBudgetError, type Parsed } from "./manifest-codec.js";
import type {
  FtsRoot,
  FtsColumnRoot,
  FtsTerms,
  FtsPostings,
  FtsDocuments,
  FtsDelta,
  FtsIdRef,
  FtsDocumentRef,
  FtsTermRef,
  FtsDeltaRef,
} from "./fts.js";

export interface FtsValues {
  readonly ftsRoot: FtsRoot;
  readonly ftsTerms: FtsTerms;
  readonly ftsPostings: FtsPostings;
  readonly ftsDocuments: FtsDocuments;
  readonly ftsDelta: FtsDelta;
}
export type FtsFormatName = keyof FtsValues;
export const FTS_CODEC_SCRATCH_BYTES = 8192;
export const FTS_ENCODED_OVERHEAD = 256;
const MAX_TOKENS = BigInt(Number.MAX_SAFE_INTEGER) * BigInt(MAX_FTS_TOKENS_PER_DOCUMENT);
const S = DURABLE_CODECS.ftsState;
const N = DURABLE_CODECS.ftsNode;
function whole(n: unknown, label: string, positive = false): asserts n is number {
  valid(
    typeof n === "number" && Number.isSafeInteger(n) && n >= (positive ? 1 : 0),
    "Invalid FTS " + label,
  );
}
function nodeKind(value: { readonly kind: unknown }, expected: string): void {
  valid(value.kind === expected, "Unknown FTS node kind");
}
function configVersion(config: unknown): unknown {
  return config !== null && typeof config === "object" && "version" in config
    ? config.version
    : undefined;
}
function tokens(n: bigint): void {
  valid(typeof n === "bigint" && n >= 0n && n <= MAX_TOKENS, "Invalid FTS token total");
}
function term(s: string): void {
  valid(typeof s === "string" && s.length > 0 && s.length <= G.termUnits, "Invalid FTS term");
}
function blob(r: BlobRef): void {
  whole(r.extent.offset, "extent offset");
  whole(r.extent.length, "extent length", true);
  whole(r.offset, "blob offset");
  whole(r.length, "blob length", true);
  whole(r.checksum, "checksum");
  whole(r.extent.offset + r.extent.length, "extent end", true);
  valid(
    r.checksum <= 0xffffffff &&
      Number.isSafeInteger(r.offset + r.length) &&
      r.offset + r.length <= r.extent.length,
    "FTS reference outside extent",
  );
  valid(r.length <= G.pageBytes, "FTS page reference exceeds cap");
}
function ids(r: FtsIdRef): void {
  blob(r.ref);
  whole(r.count, "subtree count");
  whole(r.first, "first id");
  whole(r.last, "last id");
  valid(
    r.count === 0
      ? r.first === 0 && r.last === 0
      : r.first > 0 && r.last >= r.first && r.count <= r.last - r.first + 1,
    "Invalid FTS id bounds",
  );
}
function docs(r: FtsDocumentRef, posting = false): void {
  ids(r);
  tokens(r.tokens);
  valid(
    r.tokens <= BigInt(r.count) * BigInt(MAX_FTS_TOKENS_PER_DOCUMENT),
    "FTS tokens exceed rows",
  );
  if (posting) valid(r.count > 0 && r.tokens >= BigInt(r.count), "Empty/invalid FTS postings");
}
function terms(r: FtsTermRef): void {
  blob(r.ref);
  tokens(r.count);
  tokens(r.tokens);
  valid(r.count <= r.tokens, "FTS terms exceed tokens");
  if (r.count === 0n)
    valid(r.first === "" && r.last === "" && r.tokens === 0n, "Invalid empty FTS term bounds");
  else {
    term(r.first);
    term(r.last);
    valid(r.first <= r.last, "Invalid FTS term bounds");
  }
}
function delta(r: FtsDeltaRef): void {
  ids(r);
  whole(r.documents, "delta rows");
  whole(r.firstDocument, "first delta document");
  whole(r.lastDocument, "last delta document");
  tokens(r.tokens);
  valid(
    r.tokens <= BigInt(r.documents) * BigInt(MAX_FTS_TOKENS_PER_DOCUMENT),
    "FTS delta token total",
  );
  valid(
    r.count === 0
      ? r.documents === 0 && r.firstDocument === 0 && r.lastDocument === 0
      : r.documents >= r.count &&
          r.firstDocument > 0 &&
          r.lastDocument >= r.firstDocument &&
          r.documents <= r.lastDocument - r.firstDocument + 1,
    "Invalid FTS delta document bounds",
  );
}
function depth(n: number): void {
  whole(n, "depth", true);
  valid(n <= G.maximumDepth, "FTS tree depth exceeds cap");
}
function sumNumber(a: number, b: number): number {
  const n = a + b;
  whole(n, "subtree sum");
  return n;
}
function sumTokens(a: bigint, b: bigint): bigint {
  const n = a + b;
  tokens(n);
  return n;
}

/** No encoded string copy or growing buffer: the first pass only measures/validates. */
class Output {
  length = 0;
  constructor(readonly target: Uint8Array | null) {}
  u8(n: number): void {
    if (this.target) {
      valid(this.length < this.target.length, "FTS measured/emitted size changed");
      this.target[this.length] = n;
    }
    this.length++;
  }
  u16(n: number): void {
    this.u8(n & 255);
    this.u8(n >>> 8);
  }
  u32(n: number): void {
    this.u16(n & 65535);
    this.u16(n >>> 16);
  }
  uint(n: number): void {
    whole(n, "unsigned integer");
    while (n >= 128) {
      this.u8((n % 128) | 128);
      n = Math.floor(n / 128);
    }
    this.u8(n);
  }
  big(n: bigint): void {
    tokens(n);
    while (n >= 128n) {
      this.u8(Number(n & 127n) | 128);
      n >>= 7n;
    }
    this.u8(Number(n));
  }
  text(s: string, maximum: number): void {
    valid(typeof s === "string" && s.length <= maximum, "FTS string exceeds cap");
    this.uint(s.length);
    for (let i = 0; i < s.length; i++) {
      const code = s.charCodeAt(i);
      this.u8(code >>> 8);
      this.u8(code & 255);
    }
  }
  ref(r: BlobRef): void {
    blob(r);
    this.uint(r.extent.offset);
    this.uint(r.extent.length);
    this.uint(r.offset);
    this.uint(r.length);
    this.u32(r.checksum);
  }
  idRef(r: FtsIdRef): void {
    ids(r);
    this.ref(r.ref);
    this.uint(r.first);
    this.uint(r.last);
    this.uint(r.count);
  }
  docRef(r: FtsDocumentRef): void {
    docs(r);
    this.idRef(r);
    this.big(r.tokens);
  }
  termRef(r: FtsTermRef): void {
    terms(r);
    this.ref(r.ref);
    this.text(r.first, G.termUnits);
    this.text(r.last, G.termUnits);
    this.big(r.count);
    this.big(r.tokens);
  }
  deltaRef(r: FtsDeltaRef): void {
    delta(r);
    this.idRef(r);
    this.uint(r.firstDocument);
    this.uint(r.lastDocument);
    this.uint(r.documents);
    this.big(r.tokens);
  }
  nullable<T>(r: T | null, put: (value: T) => void): void {
    this.u8(r === null ? 0 : 1);
    if (r !== null) put(r);
  }
  header(kind: FtsFormatName, branch: boolean): void {
    this.u32(DURABLE_FORMATS[kind].magic);
    this.u8(DURABLE_FORMATS[kind].version);
    this.u8(0);
    this.u8(branch ? N.branch : N.leaf);
    this.u8(0);
  }
}
class Input extends ByteReader {
  readonly words = new Uint16Array(G.ownerUnits);
  u16(): number {
    return this.u8() | (this.u8() << 8);
  }
  big(): bigint {
    let value = 0n;
    for (let i = 0; i < 10; i++) {
      const b = this.u8();
      value |= BigInt(b & 127) << BigInt(7 * i);
      if (b < 128) {
        valid(i === 0 || b !== 0, "Nonminimal FTS counter");
        tokens(value);
        return value;
      }
    }
    valid(false, "FTS counter is too wide");
  }
  text(maximum: number): string {
    const length = this.count(2);
    valid(length <= maximum, "FTS string exceeds cap");
    for (let i = 0; i < length; i++) this.words[i] = (this.u8() << 8) | this.u8();
    return String.fromCharCode(...this.words.subarray(0, length));
  }
  ref(): BlobRef {
    const value = {
      extent: { offset: this.uint(), length: this.uint() },
      offset: this.uint(),
      length: this.uint(),
      checksum: this.u32(),
    };
    blob(value);
    return value;
  }
  idRef(): FtsIdRef {
    const value = { ref: this.ref(), first: this.uint(), last: this.uint(), count: this.uint() };
    ids(value);
    return value;
  }
  docRef(): FtsDocumentRef {
    const value = { ...this.idRef(), tokens: this.big() };
    docs(value);
    return value;
  }
  termRef(): FtsTermRef {
    const value = {
      ref: this.ref(),
      first: this.text(G.termUnits),
      last: this.text(G.termUnits),
      count: this.big(),
      tokens: this.big(),
    };
    terms(value);
    return value;
  }
  deltaRef(): FtsDeltaRef {
    const value = {
      ...this.idRef(),
      firstDocument: this.uint(),
      lastDocument: this.uint(),
      documents: this.uint(),
      tokens: this.big(),
    };
    delta(value);
    return value;
  }
  nullable<T>(get: () => T): T | null {
    const flag = this.u8();
    valid(flag <= 1, "Invalid FTS presence flag");
    return flag === 0 ? null : get();
  }
  header(kind: FtsFormatName): boolean {
    valid(this.u32() === DURABLE_FORMATS[kind].magic, "Wrong FTS page magic");
    const version = this.u8(),
      expected = DURABLE_FORMATS[kind].version;
    if (version !== expected) throw new UnsupportedEncodingError(kind, version, [expected]);
    valid(this.u8() === 0, "Nonzero FTS page flags");
    const tag = this.u8();
    if (tag !== N.leaf && tag !== N.branch)
      throw new UnsupportedEncodingError("FTS node kind", tag, [N.leaf, N.branch]);
    valid(this.u8() === 0, "Nonzero FTS reserved byte");
    return tag === N.branch;
  }
}

/** Count once the owned decoded data/objects. Scratch expires before the parsed value returns.
 * 32B/row bounds the typed lanes and their headers; 512B/descriptor and4x wire bytes bound
 * refs, short strings, counters and containers (no per-document objects). At most64 branch
 * descriptors,1024 terms, or64 delta entries; every produced bigint is at most65bits.
 */
export function ftsDecodeMinimum(kind: FtsFormatName, bytes: number): number {
  whole(bytes, "page length");
  valid(bytes <= G.pageBytes, "FTS page exceeds cap");
  let result = 4096;
  if (kind === "ftsPostings" || kind === "ftsDocuments")
    result += Math.max(
      32 * Math.min(G.leafRows, Math.floor(bytes / 3)),
      512 * Math.min(G.fanOut, Math.floor(bytes / 11)),
    );
  else if (kind === "ftsTerms")
    result += 512 * Math.min(G.termEntries, Math.floor(bytes / 15)) + 4 * bytes;
  else if (kind === "ftsDelta")
    result += 1024 * Math.min(G.deltaEntries, Math.floor(bytes / 14)) + 4 * bytes;
  else result += 512 * Math.min(G.fanOut, Math.floor(bytes / 11)) + 4 * bytes;
  return result + FTS_CODEC_SCRATCH_BYTES;
}

export interface EncodedFts {
  readonly bytes: Uint8Array;
  readonly charge: number;
  close(): void;
}
class Encoded implements EncodedFts {
  #bytes: Uint8Array | undefined;
  constructor(
    bytes: Uint8Array,
    readonly charge: number,
    readonly budget: ByteBudget,
  ) {
    this.#bytes = bytes;
  }
  get bytes(): Uint8Array {
    if (!this.#bytes) throw new Error("FTS encoded bytes were released");
    return this.#bytes;
  }
  close(): void {
    if (!this.#bytes) return;
    this.#bytes = undefined;
    this.budget.free(this.charge);
  }
}
export function encodeFts<K extends FtsFormatName>(
  kind: K,
  value: FtsValues[K],
  budget: ByteBudget,
): EncodedFts {
  if (!budget.allocate(FTS_CODEC_SCRATCH_BYTES))
    throw new ParseBudgetError(FTS_CODEC_SCRATCH_BYTES);
  let charge = 0;
  try {
    const measured = new Output(null);
    emit(measured, kind, value);
    valid(measured.length <= G.pageBytes, "FTS page exceeds cap");
    charge = measured.length + FTS_ENCODED_OVERHEAD;
    if (!budget.allocate(charge)) {
      const needed = charge;
      charge = 0;
      throw new ParseBudgetError(needed);
    }
    const data = new Uint8Array(measured.length),
      out = new Output(data);
    emit(out, kind, value);
    valid(out.length === measured.length, "FTS measured/emitted size changed");
    return new Encoded(data, charge, budget);
  } catch (error) {
    if (charge) budget.free(charge);
    throw error;
  } finally {
    budget.free(FTS_CODEC_SCRATCH_BYTES);
  }
}
export function decodeFts<K extends FtsFormatName>(
  kind: K,
  bytes: Uint8Array,
  budget: ByteBudget,
): Parsed<FtsValues[K]> {
  const charge = ftsDecodeMinimum(kind, bytes.length);
  if (!budget.allocate(charge)) throw new ParseBudgetError(charge);
  try {
    const input = new Input(bytes);
    const value = read(input, kind, input.header(kind));
    input.end();
    // The same structural checks apply to senders and stored bytes; no encoded allocation.
    emit(new Output(null), kind, value);
    budget.free(FTS_CODEC_SCRATCH_BYTES);
    return { value: value as FtsValues[K], charge: charge - FTS_CODEC_SCRATCH_BYTES };
  } catch (error) {
    budget.free(charge);
    throw error;
  }
}
interface IdSummary {
  count: number;
  first: number;
  last: number;
  tokens: bigint;
  depth: number;
}
export function ftsPostingSummary(node: FtsPostings): IdSummary {
  if (node.kind === "leaf") {
    valid(
      node.ids instanceof Float64Array &&
        node.frequencies instanceof Uint16Array &&
        node.ids.length === node.frequencies.length &&
        node.ids.length <= G.leafRows,
      "Invalid FTS postings lanes",
    );
    let sum = 0,
      previous = 0;
    for (let i = 0; i < node.ids.length; i++) {
      const id = node.ids[i] ?? 0,
        frequency = node.frequencies[i] ?? 0;
      whole(id, "document id", true);
      valid(
        id > previous && frequency > 0 && frequency <= MAX_FTS_TOKENS_PER_DOCUMENT,
        "Invalid FTS posting order/frequency",
      );
      previous = id;
      sum += frequency;
    }
    return {
      count: node.ids.length,
      first: node.ids[0] ?? 0,
      last: previous,
      tokens: BigInt(sum),
      depth: 0,
    };
  }
  nodeKind(node, "branch");
  depth(node.depth);
  const summary = idChildren(node.children, true);
  valid(
    summary.count === node.count && summary.tokens === node.tokens,
    "FTS posting summary differs",
  );
  return { ...summary, depth: node.depth };
}
export function ftsDocumentSummary(node: FtsDocuments): IdSummary {
  if (node.kind === "leaf") {
    valid(
      node.ids instanceof Float64Array &&
        node.lengths instanceof Uint16Array &&
        node.renderable instanceof Uint8Array &&
        node.ids.length === node.lengths.length &&
        node.ids.length === node.renderable.length &&
        node.ids.length <= G.leafRows,
      "Invalid FTS document lanes",
    );
    let sum = 0,
      previous = 0;
    for (let i = 0; i < node.ids.length; i++) {
      const id = node.ids[i] ?? 0,
        length = node.lengths[i] ?? 0,
        present = node.renderable[i] ?? 0;
      whole(id, "document id", true);
      valid(
        id > previous &&
          length <= MAX_FTS_TOKENS_PER_DOCUMENT &&
          present <= 1 &&
          (present === 1 || length === 0),
        "Invalid FTS document order/length/NULL",
      );
      previous = id;
      sum += length;
    }
    return {
      count: node.ids.length,
      first: node.ids[0] ?? 0,
      last: previous,
      tokens: BigInt(sum),
      depth: 0,
    };
  }
  nodeKind(node, "branch");
  depth(node.depth);
  const summary = idChildren(node.children, false);
  valid(
    summary.count === node.count && summary.tokens === node.tokens,
    "FTS document summary differs",
  );
  return { ...summary, depth: node.depth };
}
function idChildren(
  children: readonly FtsDocumentRef[],
  posting: boolean,
): Omit<IdSummary, "depth"> {
  valid(children.length > 0 && children.length <= G.fanOut, "Invalid FTS child count");
  let count = 0,
    first = 0,
    last = 0,
    total = 0n;
  for (const child of children) {
    docs(child, posting);
    valid(child.count > 0 && child.first > last, "Overlapping FTS child id ranges");
    if (!count) first = child.first;
    count = sumNumber(count, child.count);
    total = sumTokens(total, child.tokens);
    last = child.last;
  }
  return { count, first, last, tokens: total };
}
export function ftsTermSummary(node: FtsTerms): {
  count: bigint;
  first: string;
  last: string;
  tokens: bigint;
  depth: number;
} {
  if (node.kind === "leaf") {
    valid(node.entries.length <= G.termEntries, "FTS term leaf exceeds cap");
    let previous = "",
      total = 0n;
    for (const entry of node.entries) {
      term(entry.term);
      docs(entry.postings, true);
      valid(entry.term > previous, "Duplicate/out-of-order FTS term");
      previous = entry.term;
      total = sumTokens(total, entry.postings.tokens);
    }
    return {
      count: BigInt(node.entries.length),
      first: node.entries[0]?.term ?? "",
      last: previous,
      tokens: total,
      depth: 0,
    };
  }
  nodeKind(node, "branch");
  depth(node.depth);
  valid(node.children.length > 0 && node.children.length <= G.fanOut, "Invalid FTS term fanout");
  let count = 0n,
    total = 0n,
    first = "",
    last = "";
  for (const child of node.children) {
    terms(child);
    valid(child.count > 0n && child.first > last, "Overlapping FTS term fences");
    if (!count) first = child.first;
    count = sumTokens(count, child.count);
    total = sumTokens(total, child.tokens);
    last = child.last;
  }
  valid(count === node.count && total === node.tokens, "FTS term summary differs");
  return { count, first, last, tokens: total, depth: node.depth };
}
export function ftsDeltaSummary(node: FtsDelta): Omit<FtsDeltaRef, "ref"> & { depth: number } {
  whole(node.ftsId, "delta index", true);
  whole(node.generation, "delta generation", true);
  whole(node.mutationEpoch, "delta mutation epoch");
  let count = 0,
    first = 0,
    last = 0,
    documents = 0,
    total = 0n,
    lo = 0,
    hi = 0;
  if (node.kind === "leaf") {
    valid(node.entries.length <= G.deltaEntries, "FTS delta leaf exceeds cap");
    for (const entry of node.entries) {
      whole(entry.lsn, "delta LSN", true);
      docs(entry.documents);
      valid(entry.documents.count > 0 && entry.lsn > last, "Invalid FTS delta order");
      if (entry.terms) terms(entry.terms);
      valid((entry.terms?.tokens ?? 0n) === entry.documents.tokens, "FTS delta is incomplete");
      if (!count) first = entry.lsn;
      count++;
      last = entry.lsn;
      documents = sumNumber(documents, entry.documents.count);
      total = sumTokens(total, entry.documents.tokens);
      lo = lo === 0 ? entry.documents.first : Math.min(lo, entry.documents.first);
      hi = Math.max(hi, entry.documents.last);
    }
  } else {
    nodeKind(node, "branch");
    depth(node.depth);
    valid(node.children.length > 0 && node.children.length <= G.fanOut, "Invalid FTS delta fanout");
    for (const child of node.children) {
      delta(child);
      valid(child.count > 0 && child.first > last, "Overlapping FTS delta LSN fences");
      if (!count) first = child.first;
      count = sumNumber(count, child.count);
      last = child.last;
      documents = sumNumber(documents, child.documents);
      total = sumTokens(total, child.tokens);
      lo = lo === 0 ? child.firstDocument : Math.min(lo, child.firstDocument);
      hi = Math.max(hi, child.lastDocument);
    }
    valid(
      count === node.count && documents === node.documents && total === node.tokens,
      "FTS delta summary differs",
    );
  }
  // ref is deliberately absent: summaries describe a value before its physical address exists.
  return {
    first,
    last,
    count,
    firstDocument: lo,
    lastDocument: hi,
    documents,
    tokens: total,
    depth: node.kind === "leaf" ? 0 : node.depth,
  };
}
function rootCheck(node: FtsRoot): void {
  whole(node.tableId, "table id", true);
  whole(node.mutationEpoch, "mutation epoch");
  if (node.kind === "directory") {
    whole(node.depth, "directory depth");
    valid(node.depth <= G.maximumDepth, "FTS directory depth");
    valid(
      node.children.length <= G.fanOut && (node.children.length > 0 || node.depth === 0),
      "Invalid FTS directory fanout",
    );
    let count = 0,
      previous = 0;
    for (const child of node.children) {
      ids(child);
      valid(child.count > 0 && child.first > previous, "FTS directory child order");
      if (node.depth === 0)
        valid(child.count === 1 && child.first === child.last, "FTS directory leaf count");
      count = sumNumber(count, child.count);
      previous = child.last;
    }
    valid(count === node.count, "FTS directory count differs");
    return;
  }
  nodeKind(node, "column");
  for (const n of [
    node.ftsId,
    node.columnId,
    node.schemaVersion,
    node.generation,
    node.nextRowId,
    node.tokenizerVersion,
    node.rendererVersion,
    node.scorerVersion,
    configVersion(node.config),
  ])
    whole(n, "root identity/version", true);
  requireFtsSemantics(node);
  valid(Object.hasOwn(S, node.state), "Unknown FTS state");
  whole(node.leaseUntil, "lease expiry");
  whole(node.baseLsn, "base LSN");
  whole(node.throughLsn, "covered LSN");
  whole(node.documentCount, "document count");
  tokens(node.tokenCount);
  valid(node.throughLsn >= node.baseLsn, "FTS coverage precedes its base");
  valid(
    node.state === "building"
      ? typeof node.owner === "string" &&
          node.owner.length > 0 &&
          node.owner.length <= G.ownerUnits &&
          node.leaseUntil > 0
      : node.owner === null && node.leaseUntil === 0,
    "Invalid FTS build lease",
  );
  if (node.documents) docs(node.documents);
  if (node.terms) terms(node.terms);
  if (node.deltas) delta(node.deltas);
  const baseN = node.documents?.count ?? 0,
    baseT = node.documents?.tokens ?? 0n;
  valid((node.terms?.tokens ?? 0n) === baseT, "FTS base is incomplete");
  valid(
    sumNumber(baseN, node.deltas?.documents ?? 0) === node.documentCount &&
      sumTokens(baseT, node.deltas?.tokens ?? 0n) === node.tokenCount,
    "FTS corpus totals differ",
  );
  valid(
    node.documentCount < node.nextRowId &&
      Math.max(node.documents?.last ?? 0, node.deltas?.lastDocument ?? 0) < node.nextRowId &&
      (!node.deltas?.count ||
        (node.deltas.first > node.baseLsn && node.deltas.last <= node.throughLsn)),
    "FTS snapshot bounds differ",
  );
}
function emitRoot(out: Output, node: FtsRoot): void {
  rootCheck(node);
  out.header("ftsRoot", node.kind === "directory");
  out.uint(node.tableId);
  out.uint(node.mutationEpoch);
  if (node.kind === "directory") {
    out.u8(node.depth);
    out.uint(node.count);
    out.uint(node.children.length);
    for (const child of node.children) out.idRef(child);
  } else {
    out.u8(S[node.state]);
    out.u8(node.tokenizerVersion);
    out.u8(node.rendererVersion);
    out.u8(node.scorerVersion);
    out.u8(node.config.version);
    out.uint(node.ftsId);
    out.uint(node.columnId);
    out.uint(node.schemaVersion);
    out.uint(node.generation);
    out.nullable(node.owner, (s) => out.text(s, G.ownerUnits));
    out.uint(node.leaseUntil);
    out.uint(node.baseLsn);
    out.uint(node.throughLsn);
    out.uint(node.nextRowId);
    out.uint(node.documentCount);
    out.big(node.tokenCount);
    out.nullable(node.terms, (r) => out.termRef(r));
    out.nullable(node.documents, (r) => out.docRef(r));
    out.nullable(node.deltas, (r) => out.deltaRef(r));
  }
}
function emit(out: Output, kind: FtsFormatName, value: FtsValues[FtsFormatName]): void {
  if (kind === "ftsRoot") {
    emitRoot(out, value as FtsRoot);
    return;
  }
  if (kind === "ftsTerms") {
    const node = value as FtsTerms;
    ftsTermSummary(node);
    out.header(kind, node.kind === "branch");
    if (node.kind === "leaf") {
      out.uint(node.entries.length);
      for (const entry of node.entries) {
        out.text(entry.term, G.termUnits);
        out.docRef(entry.postings);
      }
    } else {
      out.u8(node.depth);
      out.big(node.count);
      out.big(node.tokens);
      out.uint(node.children.length);
      for (const child of node.children) out.termRef(child);
    }
    return;
  }
  if (kind === "ftsPostings" || kind === "ftsDocuments") {
    const node = value as FtsPostings | FtsDocuments;
    if (kind === "ftsPostings") ftsPostingSummary(node as FtsPostings);
    else ftsDocumentSummary(node as FtsDocuments);
    out.header(kind, node.kind === "branch");
    if (node.kind === "leaf") {
      out.uint(node.ids.length);
      let previous = 0;
      for (let i = 0; i < node.ids.length; i++) {
        const id = node.ids[i] ?? 0;
        out.uint(id - previous);
        previous = id;
        if ("frequencies" in node) out.u16(node.frequencies[i] ?? 0);
        else out.u16(2 * (node.lengths[i] ?? 0) + (node.renderable[i] ?? 0));
      }
    } else {
      out.u8(node.depth);
      out.uint(node.count);
      out.big(node.tokens);
      out.uint(node.children.length);
      for (const child of node.children) out.docRef(child);
    }
    return;
  }
  const node = value as FtsDelta;
  ftsDeltaSummary(node);
  out.header(kind, node.kind === "branch");
  out.uint(node.ftsId);
  out.uint(node.generation);
  out.uint(node.mutationEpoch);
  if (node.kind === "leaf") {
    out.uint(node.entries.length);
    for (const entry of node.entries) {
      out.uint(entry.lsn);
      out.docRef(entry.documents);
      out.nullable(entry.terms, (r) => out.termRef(r));
    }
  } else {
    out.u8(node.depth);
    out.uint(node.count);
    out.uint(node.documents);
    out.big(node.tokens);
    out.uint(node.children.length);
    for (const child of node.children) out.deltaRef(child);
  }
}
function count(input: Input, max: number, minimum: number): number {
  const n = input.count(minimum);
  valid(n <= max, "FTS item count exceeds cap");
  return n;
}
function read(input: Input, kind: FtsFormatName, branch: boolean): FtsValues[FtsFormatName] {
  if (kind === "ftsRoot") {
    const tableId = input.uint(),
      mutationEpoch = input.uint();
    if (branch) {
      const level = input.u8(),
        total = input.uint(),
        n = count(input, G.fanOut, 11);
      const children: FtsIdRef[] = [];
      for (let i = 0; i < n; i++) children.push(input.idRef());
      return { kind: "directory", tableId, mutationEpoch, depth: level, count: total, children };
    }
    const stateId = input.u8();
    const state =
      stateId === S.building
        ? "building"
        : stateId === S.ready
          ? "ready"
          : stateId === S.invalid
            ? "invalid"
            : null;
    if (state === null)
      throw new UnsupportedEncodingError("FTS state", stateId, [S.building, S.ready, S.invalid]);
    const tokenizerVersion = input.u8(),
      rendererVersion = input.u8(),
      scorerVersion = input.u8(),
      config = { version: input.u8() };
    const root: FtsColumnRoot = {
      kind: "column",
      tableId,
      mutationEpoch,
      state,
      tokenizerVersion,
      rendererVersion,
      scorerVersion,
      config,
      ftsId: input.uint(),
      columnId: input.uint(),
      schemaVersion: input.uint(),
      generation: input.uint(),
      owner: input.nullable(() => input.text(G.ownerUnits)),
      leaseUntil: input.uint(),
      baseLsn: input.uint(),
      throughLsn: input.uint(),
      nextRowId: input.uint(),
      documentCount: input.uint(),
      tokenCount: input.big(),
      terms: input.nullable(() => input.termRef()),
      documents: input.nullable(() => input.docRef()),
      deltas: input.nullable(() => input.deltaRef()),
    };
    return root;
  }
  if (kind === "ftsTerms") {
    if (!branch) {
      const n = count(input, G.termEntries, 15),
        entries = [];
      for (let i = 0; i < n; i++)
        entries.push({ term: input.text(G.termUnits), postings: input.docRef() });
      return { kind: "leaf", entries };
    }
    const level = input.u8(),
      total = input.big(),
      sum = input.big(),
      n = count(input, G.fanOut, 16),
      children: FtsTermRef[] = [];
    for (let i = 0; i < n; i++) children.push(input.termRef());
    return { kind: "branch", depth: level, count: total, tokens: sum, children };
  }
  if (kind === "ftsPostings" || kind === "ftsDocuments") {
    if (branch) {
      const level = input.u8(),
        total = input.uint(),
        sum = input.big(),
        n = count(input, G.fanOut, 12),
        children: FtsDocumentRef[] = [];
      for (let i = 0; i < n; i++) children.push(input.docRef());
      return { kind: "branch", depth: level, count: total, tokens: sum, children };
    }
    const n = count(input, G.leafRows, 3),
      ids = new Float64Array(n),
      words = new Uint16Array(n),
      present = kind === "ftsDocuments" ? new Uint8Array(n) : null;
    let previous = 0;
    for (let i = 0; i < n; i++) {
      const step = input.uint();
      valid(step > 0, "Duplicate/out-of-order FTS document");
      previous = sumNumber(previous, step);
      ids[i] = previous;
      const code = input.u16();
      if (present) {
        words[i] = code >>> 1;
        present[i] = code & 1;
      } else words[i] = code;
    }
    return present
      ? { kind: "leaf", ids, lengths: words, renderable: present }
      : { kind: "leaf", ids, frequencies: words };
  }
  const identity = { ftsId: input.uint(), generation: input.uint(), mutationEpoch: input.uint() };
  if (!branch) {
    const n = count(input, G.deltaEntries, 14),
      entries = [];
    for (let i = 0; i < n; i++)
      entries.push({
        lsn: input.uint(),
        documents: input.docRef(),
        terms: input.nullable(() => input.termRef()),
      });
    return { ...identity, kind: "leaf", entries };
  }
  const level = input.u8(),
    total = input.uint(),
    documents = input.uint(),
    sum = input.big(),
    n = count(input, G.fanOut, 16),
    children: FtsDeltaRef[] = [];
  for (let i = 0; i < n; i++) children.push(input.deltaRef());
  return {
    ...identity,
    kind: "branch",
    depth: level,
    count: total,
    documents,
    tokens: sum,
    children,
  };
}
