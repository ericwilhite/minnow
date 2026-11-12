/**
 * Byte codecs of the manifest (ruling R15.3): the root, the paged group directory and the group
 * root. Shapes are format/manifest.ts; this file fixes their bytes (format version 1).
 *
 * ## Common rules
 * - Every blob starts with a 4-byte magic and a version byte; integers are varints (bytes.ts).
 * - **Extent table.** Each blob lists the extents its references live in once; a reference is
 *   `(extent index, offset, length[, checksum])`. Decoding gives every reference into one extent
 *   the same `ExtentRef` object (manifest.ts "Packing"). A reference must lie inside its extent.
 * - **Sizes.** Group directory pages, the directory index and group roots are blobs of at most
 *   `MAX_BLOB_BYTES` (1 MiB); the root is at most `MAX_ROOT_BYTES` (1 MiB). Encoders throw
 *   `RangeError` past a cap; the paged directory never reaches it (`GROUPS_PER_PAGE`).
 * - **Verify once.** Decoders do not hash: the extent space verified the blob (root: the root
 *   slot). They check structure (bounds, counts, orderings, enumerations) and throw
 *   `ManifestCorruptionError`, so a damaged blob is refused, never half-decoded.
 * - **Charged parsing.** Every decoder takes a budget and charges `parseCharge(bytes)` (an upper
 *   bound of what the decoded objects take, from the blob's size) before it builds anything; the
 *   result carries the charge for its holder (a cache or a reader) to free.
 *
 * ## The paged group directory (`TableRoot.groups`)
 * `groups` names an **index** blob: one entry per directory **page** (its `BlobRef`, row-id range,
 * group, row and live-row counts). Each page holds up to `GROUPS_PER_PAGE` `GroupRef`s in row-id
 * order. Page sizes may be any value from 1 to GROUPS_PER_PAGE; partial pages are not a new
 * encoding. A flush keeps an unchanged prefix (`writeGroupDirectory` with `keep`). The write
 * path carries a geometric suffix of partial pages on append, so each appended group is encoded
 * O(log GROUPS_PER_PAGE) times amortized, rather than once per remaining slot in a tail page.
 * Full pages remain shared on append. The write path uses full pages followed by a descending
 * power-of-two tail, preserving this layout across variable append batches and mutations. No group count is
 * limited by the 1 MiB blob cap (about 15,000 full pages plus a bounded partial tail). A table
 * with no groups has a zero-length `groups` reference. The index is still rewritten on every
 * flush, O(pages); a two-level index can bound it if tables reach thousands of pages.
 *
 * ## Canonical decoding (review B2-p2a #7)
 * One value has one encoding, and decoders refuse every other: extent and type tables hold no
 * duplicate, every entry is used, and entries are first used in table order; integers and
 * bigints are minimal; ledger counts are in key order (`<` on UTF-16 code units). A blob over
 * its cap is refused before it is charged.
 */
import { LIST_PAGE_EXTENTS, type ExtentListPage } from "../storage/extent-list.js";
import { MAX_BLOB_BYTES } from "../storage/extents.js";
import type { ByteBudget, ExtentWriter } from "../storage/extents.js";
import type { ColumnStatistics, PhysicalType, StatValue } from "../types.js";
import { ByteReader, ByteWriter, ManifestCorruptionError, valid } from "./bytes.js";
import type { ClientLedgerEntry } from "./ledger.js";
import {
  MANIFEST_FORMAT_VERSION,
  type BlobRef,
  type ChunkData,
  type ColumnChunk,
  type ExtentRef,
  type GroupRef,
  type GroupRoot,
  type IndexRoot,
  type ManifestRoot,
  type RunRef,
  type TableRoot,
  type TableStatistics,
  type ColumnTotals,
} from "./manifest.js";

export { ManifestCorruptionError };

export const MAX_ROOT_BYTES = 1024 * 1024;
/** Groups per directory page: a page of them stays far below 1 MiB (~40–120 B each). */
export const GROUPS_PER_PAGE = 4096;
/** The most rows one group holds (a locator position, vector/locator.ts). */
const MAX_GROUP_ROWS = 65_536;

const MAGIC = {
  root: 0x3152_4d4d, // "MMR1"
  index: 0x3149_474d, // "MGI1"
  page: 0x3150_474d, // "MGP1"
  group: 0x3152_474d, // "MGR1"
  statistics: 0x3154_534d, // "MST1"
} as const;
const VERSION = 1;

/** A decoded value and the bytes charged for it (its holder frees them). */
export interface Parsed<T> {
  readonly value: T;
  readonly charge: number;
}

/** Bytes charged to parse a blob of `length` bytes: decoded objects take at most ~8× the bytes. */
export function parseCharge(length: number): number {
  return 256 + length * 8;
}

/** The parse-budget refusal: the caller borrows a loan of `bytes` and retries. */
export class ParseBudgetError extends Error {
  readonly code = "MINNOW_PARSE_BUDGET";
  constructor(readonly bytes: number) {
    super(`Parsing manifest metadata needs ${String(bytes)} bytes the budget does not have`);
    this.name = "ParseBudgetError";
  }
}

function charged<T>(
  bytes: Uint8Array,
  budget: ByteBudget,
  parse: (reader: ByteReader) => T,
  cap: number = MAX_BLOB_BYTES,
): Parsed<T> {
  // Over its cap, a blob is refused before anything is charged or parsed (review B2-p2a #5).
  if (bytes.length > cap)
    throw new ManifestCorruptionError(
      `A manifest blob of ${String(bytes.length)} bytes passes its ${String(cap)}-byte cap`,
    );
  const charge = parseCharge(bytes.length);
  if (!budget.allocate(charge)) throw new ParseBudgetError(charge);
  try {
    const reader = new ByteReader(bytes);
    const value = parse(reader);
    reader.end();
    return { value, charge };
  } catch (error) {
    budget.free(charge);
    if (error instanceof ManifestCorruptionError) throw error;
    // Anything else from a damaged blob (none is expected) is still corruption, never a crash.
    throw new ManifestCorruptionError(`Manifest blob does not decode: ${String(error)}`, {
      cause: error,
    });
  }
}

// ── References and the extent table ────────────────────────────────────────────────────────────

/** Collects extents while a body is written; `finish` puts the table in front. */
class RefWriter {
  readonly body = new ByteWriter();
  readonly #index = new Map<string, number>();
  readonly #extents: ExtentRef[] = [];

  extent(extent: ExtentRef): void {
    const key = `${String(extent.offset)}:${String(extent.length)}`;
    let index = this.#index.get(key);
    if (index === undefined) {
      index = this.#extents.length;
      this.#index.set(key, index);
      this.#extents.push(extent);
    }
    this.body.uint(index);
  }

  blob(ref: BlobRef): void {
    this.data(ref);
    this.body.u32(ref.checksum);
  }

  data(ref: ChunkData): void {
    if (ref.offset + ref.length > ref.extent.length)
      throw new RangeError("A reference lies outside its extent");
    this.extent(ref.extent);
    this.body.uint(ref.offset);
    this.body.uint(ref.length);
  }

  optionalBlob(ref: BlobRef | null): void {
    this.body.u8(ref === null ? 0 : 1);
    if (ref !== null) this.blob(ref);
  }

  finish(magic: number, cap: number, what: string): Uint8Array {
    const head = new ByteWriter();
    head.u32(magic);
    head.u8(VERSION);
    head.uint(this.#extents.length);
    for (const extent of this.#extents) {
      head.uint(extent.offset);
      head.uint(extent.length);
    }
    const body = this.body.finish();
    const total = head.length + body.length;
    if (total > cap)
      throw new RangeError(`${what} of ${String(total)} bytes passes its ${String(cap)}-byte cap`);
    const out = new Uint8Array(total);
    out.set(head.finish(), 0);
    out.set(body, head.length);
    return out;
  }
}

/**
 * Reads the extent table and references against it. Canonical (review B2-p2a #7): the table has
 * no duplicate extents, its entries are first referenced in table order (what `RefWriter`
 * writes), and every entry is referenced (`end`).
 */
class RefReader {
  readonly extents: ExtentRef[] = [];
  #nextNew = 0;
  constructor(
    readonly bytes: ByteReader,
    magic: number,
    what: string,
  ) {
    valid(bytes.u32() === magic, `Not a ${what}`);
    valid(bytes.u8() === VERSION, `Unknown ${what} version`);
    const count = bytes.count(2);
    // Offsets and lengths are exact safe integers. Avoid formatting every pair; most
    // offsets occur once, and only the legal same-offset/different-length case needs a Set.
    const seen = new Map<number, number | Set<number>>();
    for (let k = 0; k < count; k++) {
      const offset = bytes.uint();
      const length = bytes.uint();
      const previous = seen.get(offset);
      if (previous === undefined) seen.set(offset, length);
      else if (typeof previous === "number") {
        valid(previous !== length, "An extent appears twice in an extent table");
        seen.set(offset, new Set([previous, length]));
      } else {
        valid(!previous.has(length), "An extent appears twice in an extent table");
        previous.add(length);
      }
      this.extents.push({ offset, length });
    }
  }

  #extent(): ExtentRef {
    const index = this.bytes.uint();
    const extent = this.extents[index];
    valid(extent !== undefined, "Reference to an unknown extent");
    valid(index <= this.#nextNew, "Extent table entries are not in first-use order");
    if (index === this.#nextNew) this.#nextNew++;
    return extent;
  }

  /** Every table entry was referenced. */
  end(): void {
    valid(this.#nextNew === this.extents.length, "An extent table entry is never referenced");
  }

  data(): ChunkData {
    const extent = this.#extent();
    const offset = this.bytes.uint();
    const length = this.bytes.uint();
    valid(offset + length <= extent.length, "A reference lies outside its extent");
    return { extent, offset, length };
  }

  blob(): BlobRef {
    const extent = this.#extent();
    const offset = this.bytes.uint();
    const length = this.bytes.uint();
    valid(offset + length <= extent.length, "A reference lies outside its extent");
    return { extent, offset, length, checksum: this.bytes.u32() };
  }

  optionalBlob(): BlobRef | null {
    const flag = this.bytes.u8();
    valid(flag <= 1, "Bad optional reference flag");
    return flag === 0 ? null : this.blob();
  }
}

// ── Physical types and statistics ──────────────────────────────────────────────────────────────

const TYPE_TAGS = [
  "bool",
  "int32",
  "int64",
  "float64",
  "float32",
  "decimal",
  "date",
  "time",
  "timestamp",
  "interval",
  "text",
  "bytes",
  "json",
  "uuid",
  "enum",
  "array",
] as const;

function writeType(out: ByteWriter, type: PhysicalType, depth = 0): void {
  if (depth > 8) throw new RangeError("Array types nest too deeply");
  if (type.kind === "locator") throw new RangeError("Locators are never stored");
  out.u8(TYPE_TAGS.indexOf(type.kind) + 1);
  switch (type.kind) {
    case "int32":
      out.u8(type.bits);
      return;
    case "decimal":
      out.optionalUint(type.precision);
      out.u8(type.scale === null ? 0 : 1);
      if (type.scale !== null) out.int(type.scale);
      return;
    case "timestamp":
      out.u8(type.zoned ? 1 : 0);
      return;
    case "json":
      out.u8(type.binary ? 1 : 0);
      return;
    case "enum":
      out.string(type.name);
      out.uint(type.labels.length);
      for (const label of type.labels) out.string(label);
      return;
    case "array":
      writeType(out, type.element, depth + 1);
      return;
    default:
      return;
  }
}

function readFlag(reader: ByteReader): boolean {
  const flag = reader.u8();
  valid(flag <= 1, "Bad flag byte");
  return flag === 1;
}

function readType(reader: ByteReader, depth = 0): PhysicalType {
  valid(depth <= 8, "Array types nest too deeply");
  const kind = TYPE_TAGS[reader.u8() - 1];
  valid(kind !== undefined, "Unknown physical type");
  switch (kind) {
    case "int32": {
      const bits = reader.u8();
      valid(bits === 16 || bits === 32, "Bad int32 width");
      return { kind, bits };
    }
    case "decimal": {
      const precision = reader.optionalUint();
      const scale = readFlag(reader) ? reader.int() : null;
      return { kind, precision, scale };
    }
    case "timestamp":
      return { kind, zoned: readFlag(reader) };
    case "json":
      return { kind, binary: readFlag(reader) };
    case "enum": {
      const name = reader.string();
      const count = reader.count(1);
      const labels: string[] = [];
      for (let k = 0; k < count; k++) labels.push(reader.string());
      return { kind, name, labels };
    }
    case "array":
      return { kind, element: readType(reader, depth + 1) };
    default:
      return { kind };
  }
}

function writeStat(out: ByteWriter, value: StatValue | null): void {
  if (value === null) out.u8(0);
  else if (typeof value === "boolean") out.u8(value ? 2 : 1);
  else if (typeof value === "number") {
    out.u8(3);
    out.f64(value);
  } else if (typeof value === "bigint") {
    out.u8(4);
    out.bigint(value);
  } else {
    out.u8(5);
    out.bytes(value);
  }
}

function readStat(reader: ByteReader): StatValue | null {
  switch (reader.u8()) {
    case 0:
      return null;
    case 1:
      return false;
    case 2:
      return true;
    case 3:
      return reader.f64();
    case 4:
      return reader.bigint();
    case 5:
      return reader.bytes();
    default:
      throw new ManifestCorruptionError("Unknown statistic tag");
  }
}

function writeStatistics(out: ByteWriter, statistics: ColumnStatistics): void {
  out.uint(statistics.nullCount);
  writeStat(out, statistics.min);
  writeStat(out, statistics.max);
  out.optionalUint(statistics.dictionarySize);
}

function readStatistics(reader: ByteReader): ColumnStatistics {
  const nullCount = reader.uint();
  const min = readStat(reader);
  const max = readStat(reader);
  const dictionarySize = reader.optionalUint();
  return { nullCount, min, max, dictionarySize };
}

// ── Group root ─────────────────────────────────────────────────────────────────────────────────

/**
 * A type's identity: exactly the fields its encoding writes (`writeType`), so key order or extra
 * properties cannot split one type into two table entries.
 */
function typeKey(type: PhysicalType): string {
  switch (type.kind) {
    case "int32":
      return `int32:${String(type.bits)}`;
    case "decimal":
      return `decimal:${String(type.precision)}:${String(type.scale)}`;
    case "timestamp":
      return type.zoned ? "timestamp:z" : "timestamp";
    case "json":
      return type.binary ? "json:b" : "json";
    case "enum":
      return `enum:${JSON.stringify([type.name, type.labels])}`;
    case "array":
      return `array:${typeKey(type.element)}`;
    default:
      return type.kind;
  }
}

/** A group root's bytes (≤ `MAX_BLOB_BYTES`; about 1,600 columns fit even with long bounds). */
export function encodeGroupRoot(root: GroupRoot): Uint8Array {
  const out = new RefWriter();
  const body = out.body;
  body.uint(root.rowCount);
  if (root.rowIds.kind === "dense") body.u8(0);
  else {
    body.u8(1);
    out.blob(root.rowIds.ref);
  }
  // The type table: each distinct column type once (enum labels can be long).
  const types: PhysicalType[] = [];
  const typeIndex = new Map<string, number>();
  for (const chunk of root.columns) {
    const key = typeKey(chunk.type);
    if (!typeIndex.has(key)) {
      typeIndex.set(key, types.length);
      types.push(chunk.type);
    }
  }
  body.uint(types.length);
  for (const type of types) writeType(body, type);
  body.uint(root.columns.length);
  for (const chunk of root.columns) {
    body.uint(chunk.columnId);
    body.uint(typeIndex.get(typeKey(chunk.type)) ?? 0);
    out.data(chunk.data);
    out.blob(chunk.directory);
    writeStatistics(body, chunk.statistics);
    out.optionalBlob(chunk.distinctSketch);
  }
  return out.finish(MAGIC.group, MAX_BLOB_BYTES, "A group root");
}

export function decodeGroupRoot(bytes: Uint8Array, budget: ByteBudget): Parsed<GroupRoot> {
  return charged(bytes, budget, readGroupRoot);
}

/** The complete validator is shared by eager decoding and optional validated views. */
function readGroupRoot(reader: ByteReader, capture?: RootView): GroupRoot {
  const refs = new RefReader(reader, MAGIC.group, "group root");
  const rowCount = reader.uint();
  valid(rowCount >= 1 && rowCount <= MAX_GROUP_ROWS, "Bad group row count");
  const rowIdKind = reader.u8();
  valid(rowIdKind <= 1, "Bad row-id kind");
  const rowIds: GroupRoot["rowIds"] =
    rowIdKind === 0 ? { kind: "dense" } : { kind: "column", ref: refs.blob() };
  const typeCount = reader.count(1);
  const types: PhysicalType[] = [];
  // Canonical like the extent table: distinct types, first used in table order, all used.
  const typeKeys = new Set<string>();
  for (let k = 0; k < typeCount; k++) {
    const type = readType(reader);
    const key = typeKey(type);
    valid(!typeKeys.has(key), "A type appears twice in a group root's type table");
    typeKeys.add(key);
    types.push(type);
  }
  let nextType = 0;
  const count = reader.count(8);
  capture?.begin(refs.extents, types, count);
  const columns: ColumnChunk[] = [];
  const seen = new Set<number>();
  for (let k = 0; k < count; k++) {
    const columnId = reader.uint();
    valid(!seen.has(columnId), "A column appears twice in a group root");
    seen.add(columnId);
    const typeAt = reader.uint();
    const type = types[typeAt];
    valid(type !== undefined, "Reference to an unknown column type");
    valid(typeAt <= nextType, "Type table entries are not in first-use order");
    if (typeAt === nextType) nextType++;
    capture?.column(k, columnId, typeAt, reader.at);
    const data = refs.data();
    const directory = refs.blob();
    const statistics = readStatistics(reader);
    valid(statistics.nullCount <= rowCount, "More nulls than rows");
    const distinctSketch = refs.optionalBlob();
    if (capture === undefined)
      columns.push({ columnId, type, data, directory, statistics, distinctSketch });
  }
  valid(nextType === types.length, "A type table entry is never used");
  refs.end();
  return { rowCount, rowIds, columns };
}

/** Optional probes must not increase a sizing pass's mandatory admission requirement. */
export interface OptionalParseBudget extends ByteBudget {
  optionalAllocate?(bytes: number): boolean;
}
export function optionalParseAllocation(budget: ByteBudget, bytes: number): boolean {
  const optional = budget as OptionalParseBudget;
  return optional.optionalAllocate?.(bytes) ?? budget.allocate(bytes);
}

const ROOT_VIEW_FIXED_BYTES = 1024;
const rootViews = new WeakMap<GroupRoot, RootView>();

function freezeRootType(type: PhysicalType): void {
  if (type.kind === "array") freezeRootType(type.element);
  if (type.kind === "enum") Object.freeze(type.labels);
  Object.freeze(type);
}

/** Private immutable navigation, never a decoded graph of the unselected columns. */
class RootView {
  #extents = new Float64Array(0);
  #types: readonly PhysicalType[] = [];
  #ids = new Float64Array(0);
  #positions = new Uint32Array(0);
  #typeCodes = new Uint32Array(0);
  constructor(readonly of: Uint8Array) {}
  begin(extents: readonly ExtentRef[], types: PhysicalType[], count: number): void {
    this.#extents = new Float64Array(2 * extents.length);
    for (let k = 0; k < extents.length; k++) {
      const extent = extents[k];
      if (!extent) throw new ManifestCorruptionError("Missing validated extent");
      this.#extents[k * 2] = extent.offset;
      this.#extents[k * 2 + 1] = extent.length;
    }
    for (const type of types) freezeRootType(type);
    this.#types = Object.freeze(types);
    this.#ids = new Float64Array(count);
    this.#positions = new Uint32Array(count);
    this.#typeCodes = new Uint32Array(count);
  }
  column(k: number, id: number, type: number, position: number): void {
    this.#ids[k] = id;
    this.#positions[k] = position;
    this.#typeCodes[k] = type;
  }
  get count(): number {
    return this.#ids.length;
  }
  get charge(): number {
    // Full eager-graph bound remains prepaid if a COMPLETE consumer requests columns later.
    return (
      parseCharge(this.of.length) +
      this.of.byteLength +
      ROOT_VIEW_FIXED_BYTES +
      this.#extents.byteLength +
      this.#ids.byteLength +
      this.#positions.byteLength +
      this.#typeCodes.byteLength
    );
  }
  select(wanted?: readonly number[]): ColumnChunk[] {
    const reader = new ByteReader(this.of);
    const extents = new Map<number, ExtentRef>();
    const extent = (): ExtentRef => {
      const index = reader.uint();
      let ref = extents.get(index);
      if (ref === undefined) {
        ref = { offset: this.#extents[index * 2] ?? 0, length: this.#extents[index * 2 + 1] ?? 0 };
        extents.set(index, ref);
      }
      return ref;
    };
    const data = (): ChunkData => {
      const ref = extent();
      return { extent: ref, offset: reader.uint(), length: reader.uint() };
    };
    const blob = (): BlobRef => ({ ...data(), checksum: reader.u32() });
    const columns: ColumnChunk[] = [];
    for (let k = 0; k < this.#ids.length; k++) {
      const columnId = this.#ids[k] ?? 0;
      if (wanted !== undefined && !wanted.includes(columnId)) continue;
      reader.at = this.#positions[k] ?? 0;
      const type = this.#types[this.#typeCodes[k] ?? 0];
      if (!type) throw new ManifestCorruptionError("Missing validated type");
      const chunkData = data(),
        directory = blob(),
        statistics = readStatistics(reader);
      const distinctSketch = reader.u8() === 0 ? null : blob();
      columns.push({ columnId, type, data: chunkData, directory, statistics, distinctSketch });
    }
    return columns;
  }
  root(header: GroupRoot): GroupRoot {
    let complete: readonly ColumnChunk[] | undefined;
    const materialize = () => this.select();
    if (header.rowIds.kind === "column") {
      Object.freeze(header.rowIds.ref.extent);
      Object.freeze(header.rowIds.ref);
    }
    const root: GroupRoot = Object.freeze({
      rowCount: header.rowCount,
      rowIds: Object.freeze(header.rowIds),
      get columns() {
        return (complete ??= materialize());
      },
    });
    rootViews.set(root, this);
    return root;
  }
}

/** Complete semantics are unchanged; scan callers can avoid forcing the complete graph. */
export function selectGroupRootColumns(root: GroupRoot, wanted: readonly number[]): ColumnChunk[] {
  return (
    rootViews.get(root)?.select(wanted) ?? root.columns.filter((c) => wanted.includes(c.columnId))
  );
}
/** Only private compact views create new per-query refs rather than borrowing eager objects. */
export function isValidatedGroupRoot(root: GroupRoot): boolean {
  return rootViews.has(root);
}
export function groupRootColumnCount(root: GroupRoot): number {
  return rootViews.get(root)?.count ?? root.columns.length;
}

/**
 * Optional cache representation. All original checks run before publishing any view. The
 * encoded copy and packed arrays are private; no pool/cache/lease bytes are retained. Refusal
 * returns null so a fitting ordinary decode can proceed with its original minimum.
 */
export function tryDecodeGroupRootView(
  bytes: Uint8Array,
  budget: ByteBudget,
): Parsed<GroupRoot> | null {
  if (bytes.length > MAX_BLOB_BYTES)
    throw new ManifestCorruptionError("A group root exceeds its cap");
  // 16E+16C <= 8*encodedLength: extent/column regions have at least 2E and 8C bytes.
  // Reserve the whole construction peak, including transient complete validation objects.
  const maximum = 2 * parseCharge(bytes.length) + bytes.length + ROOT_VIEW_FIXED_BYTES;
  if (!optionalParseAllocation(budget, maximum)) return null;
  try {
    const view = new RootView(new Uint8Array(bytes));
    const reader = new ByteReader(view.of);
    const header = readGroupRoot(reader, view);
    reader.end();
    const value = view.root(header),
      charge = view.charge;
    budget.free(maximum - charge);
    return { value, charge };
  } catch (error) {
    budget.free(maximum);
    if (error instanceof ManifestCorruptionError) throw error;
    throw new ManifestCorruptionError(`Manifest blob does not decode: ${String(error)}`, {
      cause: error,
    });
  }
}

// ── Table statistics (`TableRoot.statistics`) ──────────────────────────────────────────────────

/**
 * A table's statistics blob (≤ `MAX_BLOB_BYTES`): `uint groupCount, uint columns`, then per column
 * in ascending id `uint columnId, uint nullCount, uint values, uint bytes, flag unbounded, stat min,
 * stat max`. An unbounded column has null bounds; a column without values has null bounds.
 */
export function encodeTableStatistics(statistics: TableStatistics): Uint8Array {
  const out = new ByteWriter();
  out.u32(MAGIC.statistics);
  out.u8(VERSION);
  out.uint(statistics.groupCount);
  out.uint(statistics.columns.length);
  let previous = -1;
  for (const column of statistics.columns) {
    if (column.columnId <= previous)
      throw new RangeError("Table statistics columns are not in ascending id order");
    previous = column.columnId;
    const bounded = !column.unbounded && column.values > 0;
    if (!bounded && (column.min !== null || column.max !== null))
      throw new RangeError("Unbounded or empty column statistics carry bounds");
    out.uint(column.columnId);
    out.uint(column.nullCount);
    out.uint(column.values);
    out.uint(column.bytes);
    out.u8(column.unbounded ? 1 : 0);
    writeStat(out, column.min);
    writeStat(out, column.max);
  }
  const bytes = out.finish();
  if (bytes.length > MAX_BLOB_BYTES) throw new RangeError("Table statistics pass the blob cap");
  return bytes;
}

export function decodeTableStatistics(
  bytes: Uint8Array,
  budget: ByteBudget,
): Parsed<TableStatistics> {
  return charged(bytes, budget, (reader) => {
    valid(reader.u32() === MAGIC.statistics, "Not a table statistics blob");
    valid(reader.u8() === VERSION, "Unknown table statistics version");
    const groupCount = reader.uint();
    const count = reader.count(7);
    const columns: ColumnTotals[] = [];
    let previous = -1;
    for (let k = 0; k < count; k++) {
      const columnId = reader.uint();
      valid(columnId > previous, "Table statistics columns are not in ascending id order");
      previous = columnId;
      const nullCount = reader.uint();
      const values = reader.uint();
      const bytes = reader.uint();
      const unbounded = readFlag(reader);
      const min = readStat(reader);
      const max = readStat(reader);
      valid(
        (min === null) === (max === null) &&
          (unbounded || values === 0 ? min === null : min !== null),
        "Table statistics bounds disagree with their column",
      );
      columns.push({ columnId, nullCount, values, bytes, unbounded, min, max });
    }
    return { groupCount, columns };
  });
}

// ── Group directory pages and index ────────────────────────────────────────────────────────────

/** One page of a table's group directory, as its index lists it. */
export interface GroupPageEntry {
  readonly ref: BlobRef;
  readonly firstRowId: number;
  readonly lastRowId: number;
  readonly groupCount: number;
  readonly rowCount: number;
  readonly liveRowCount: number;
}

function checkGroups(groups: readonly GroupRef[], after: number): void {
  let previous = after;
  for (const group of groups) {
    valid(
      group.firstRowId > previous && group.firstRowId <= group.lastRowId,
      "Groups overlap or are out of row-id order",
    );
    valid(
      group.rowCount >= 1 &&
        group.rowCount <= MAX_GROUP_ROWS &&
        group.rowCount <= group.lastRowId - group.firstRowId + 1,
      "Bad group row count",
    );
    valid(group.liveRowCount <= group.rowCount, "More live rows than rows");
    previous = group.lastRowId;
  }
}

export function encodeGroupPage(groups: readonly GroupRef[]): Uint8Array {
  if (groups.length === 0 || groups.length > GROUPS_PER_PAGE)
    throw new RangeError(`A directory page holds 1 … ${String(GROUPS_PER_PAGE)} groups`);
  try {
    checkGroups(groups, 0);
  } catch (error) {
    throw new RangeError(`Groups to encode are invalid: ${String(error)}`, { cause: error });
  }
  const out = new RefWriter();
  out.body.uint(groups.length);
  for (const group of groups) {
    out.body.uint(group.groupId);
    out.body.uint(group.firstRowId);
    out.body.uint(group.lastRowId - group.firstRowId);
    out.body.uint(group.rowCount);
    out.body.uint(group.liveRowCount);
    out.blob(group.root);
    out.optionalBlob(group.deletions);
  }
  return out.finish(MAGIC.page, MAX_BLOB_BYTES, "A group directory page");
}

export function decodeGroupPage(bytes: Uint8Array, budget: ByteBudget): Parsed<GroupRef[]> {
  return charged(bytes, budget, (reader) => {
    const refs = new RefReader(reader, MAGIC.page, "group directory page");
    const count = reader.count(10);
    valid(count >= 1 && count <= GROUPS_PER_PAGE, "Bad directory page size");
    const groups: GroupRef[] = [];
    for (let k = 0; k < count; k++) {
      const groupId = reader.uint();
      const firstRowId = reader.uint();
      const lastRowId = firstRowId + reader.uint();
      valid(Number.isSafeInteger(lastRowId), "Row id past 2^53");
      const rowCount = reader.uint();
      const liveRowCount = reader.uint();
      const root = refs.blob();
      const deletions = refs.optionalBlob();
      groups.push({ groupId, firstRowId, lastRowId, rowCount, liveRowCount, root, deletions });
    }
    checkGroups(groups, 0);
    refs.end();
    return groups;
  });
}

export function encodeGroupIndex(pages: readonly GroupPageEntry[]): Uint8Array {
  const out = new RefWriter();
  out.body.uint(pages.length);
  let previous = 0;
  for (const page of pages) {
    if (!(page.firstRowId > previous && page.firstRowId <= page.lastRowId && page.groupCount >= 1))
      throw new RangeError("Directory pages overlap or are out of row-id order");
    previous = page.lastRowId;
    out.blob(page.ref);
    out.body.uint(page.firstRowId);
    out.body.uint(page.lastRowId - page.firstRowId);
    out.body.uint(page.groupCount);
    out.body.uint(page.rowCount);
    out.body.uint(page.liveRowCount);
  }
  return out.finish(MAGIC.index, MAX_BLOB_BYTES, "A group directory index");
}

export function decodeGroupIndex(bytes: Uint8Array, budget: ByteBudget): Parsed<GroupPageEntry[]> {
  return charged(bytes, budget, (reader) => {
    const refs = new RefReader(reader, MAGIC.index, "group directory index");
    const count = reader.count(12);
    const pages: GroupPageEntry[] = [];
    let previous = 0;
    for (let k = 0; k < count; k++) {
      const ref = refs.blob();
      const firstRowId = reader.uint();
      const lastRowId = firstRowId + reader.uint();
      const groupCount = reader.uint();
      const rowCount = reader.uint();
      const liveRowCount = reader.uint();
      valid(Number.isSafeInteger(lastRowId), "Row id past 2^53");
      valid(firstRowId > previous, "Directory pages overlap or are out of row-id order");
      valid(groupCount >= 1 && groupCount <= GROUPS_PER_PAGE, "Bad directory page group count");
      valid(
        rowCount >= groupCount &&
          rowCount <= lastRowId - firstRowId + 1 &&
          liveRowCount <= rowCount,
        "Bad directory page row counts",
      );
      previous = lastRowId;
      pages.push({ ref, firstRowId, lastRowId, groupCount, rowCount, liveRowCount });
    }
    refs.end();
    return pages;
  });
}

/** A zero-length `groups` reference: a table without groups (no blob is read). */
export function isEmptyDirectory(ref: BlobRef): boolean {
  return ref.length === 0;
}

/** The index entry of one page of groups. */
function pageEntry(ref: BlobRef, groups: readonly GroupRef[]): GroupPageEntry {
  const first = groups[0];
  const last = groups[groups.length - 1];
  if (first === undefined || last === undefined) throw new RangeError("An empty directory page");
  let rowCount = 0;
  let liveRowCount = 0;
  for (const group of groups) {
    rowCount += group.rowCount;
    liveRowCount += group.liveRowCount;
  }
  return {
    ref,
    firstRowId: first.firstRowId,
    lastRowId: last.lastRowId,
    groupCount: groups.length,
    rowCount,
    liveRowCount,
  };
}

export interface WrittenGroupDirectory {
  /** `TableRoot.groups`. */
  readonly ref: BlobRef;
  readonly pages: readonly GroupPageEntry[];
}

/** Canonical append layout: full pages, then power-of-two partial pages in descending order.
 * Zero has no page. The envelope permits other sizes too; legacy packed tails stay readable.
 */
export function directoryPageGroups(remaining: number): number {
  if (!Number.isSafeInteger(remaining) || remaining < 0)
    throw new RangeError("Invalid directory group count");
  return remaining === 0 ? 0 : 2 ** Math.floor(Math.log2(Math.min(remaining, GROUPS_PER_PAGE)));
}

/**
 * Writes a table's group directory through `writer`: the pages in `keep` as they are (their
 * blobs are reused), then `groups` (which follow them in row-id order) in new pages of up to
 * `GROUPS_PER_PAGE`, then the index. The caller keeps an unchanged prefix and selects the
 * suffix to rewrite. Binary tail sizing keeps the regular write path canonical while the
 * default packed mode preserves the legacy/staging writer behavior. The new pages share one extent; the index is a blob of its own extent.
 */
export async function writeGroupDirectory(
  writer: ExtentWriter,
  groups: readonly GroupRef[],
  keep: readonly GroupPageEntry[] = [],
  tail: "packed" | "binary" = "packed",
): Promise<WrittenGroupDirectory> {
  const last = keep[keep.length - 1];
  const first = groups[0];
  if (last !== undefined && first !== undefined && first.firstRowId <= last.lastRowId)
    throw new RangeError("New groups must follow the kept directory pages");
  const chunks: GroupRef[][] = [];
  for (let start = 0; start < groups.length;) {
    const count = tail === "binary" ? directoryPageGroups(groups.length - start) : GROUPS_PER_PAGE;
    chunks.push(groups.slice(start, start + count));
    start += count;
  }
  const pages = [...keep];
  if (chunks.length > 0) {
    const written = await writer.write(chunks.map((chunk) => encodeGroupPage(chunk)));
    written.blobs.forEach((ref, index) => pages.push(pageEntry(ref, chunks[index] ?? [])));
  }
  const index = await writer.write([encodeGroupIndex(pages)]);
  const ref = index.blobs[0];
  if (ref === undefined) throw new Error("The directory index was not written");
  return { ref, pages };
}

// ── Root ───────────────────────────────────────────────────────────────────────────────────────

function writeRun(out: RefWriter, run: RunRef): void {
  // The level and the continuation flag share one integer: level × 2 + continues.
  out.body.uint(run.level * 2 + (run.continues === true ? 1 : 0));
  out.body.uint(run.entryCount);
  out.blob(run.entries);
  out.blob(run.fences);
  out.blob(run.bloom);
  out.body.bytes(run.minKey);
  out.body.bytes(run.maxKey);
}

function readRun(refs: RefReader): RunRef {
  const reader = refs.bytes;
  const packed = reader.uint();
  const level = Math.floor(packed / 2);
  const entryCount = reader.uint();
  const entries = refs.blob();
  const fences = refs.blob();
  const bloom = refs.blob();
  const minKey = reader.bytes();
  const maxKey = reader.bytes();
  const run = { level, entryCount, entries, fences, bloom, minKey, maxKey };
  return packed % 2 === 1 ? { ...run, continues: true } : run;
}

function writeRuns(out: RefWriter, runs: readonly RunRef[]): void {
  out.body.uint(runs.length);
  for (const run of runs) writeRun(out, run);
}

function readRuns(refs: RefReader): RunRef[] {
  const count = refs.bytes.count(8);
  const runs: RunRef[] = [];
  for (let k = 0; k < count; k++) runs.push(readRun(refs));
  return runs;
}

function writeIndex(out: RefWriter, index: IndexRoot): void {
  out.body.uint(index.indexId);
  out.body.uint(index.columnIds.length);
  for (const id of index.columnIds) out.body.uint(id);
  out.body.u8(index.unique ? 1 : 0);
  writeRuns(out, index.runs);
}

function readIndex(refs: RefReader): IndexRoot {
  const reader = refs.bytes;
  const indexId = reader.uint();
  const count = reader.count(1);
  const columnIds: number[] = [];
  for (let k = 0; k < count; k++) columnIds.push(reader.uint());
  const unique = readFlag(reader);
  return { indexId, columnIds, unique, runs: readRuns(refs) };
}

function writeTable(out: RefWriter, table: TableRoot): void {
  const body = out.body;
  body.uint(table.tableId);
  body.uint(table.schemaVersion);
  body.uint(table.nextRowId);
  body.uint(table.flushedThroughLsn);
  body.uint(table.liveRowCount);
  out.blob(table.groups);
  body.uint(table.groupCount);
  body.uint(table.staged.length);
  for (const staged of table.staged) {
    body.uint(staged.transaction);
    body.uint(staged.firstRowId);
    body.uint(staged.rowCount);
    out.blob(staged.groups);
    body.uint(staged.indexRuns.length);
    for (const runs of staged.indexRuns) {
      body.uint(runs.indexId);
      writeRuns(out, runs.runs);
    }
  }
  body.u8(table.keyIndex === null ? 0 : 1);
  if (table.keyIndex !== null) writeIndex(out, table.keyIndex);
  body.uint(table.secondaryIndexes.length);
  for (const index of table.secondaryIndexes) writeIndex(out, index);
  out.optionalBlob(table.statistics);
}

function readTable(refs: RefReader): TableRoot {
  const reader = refs.bytes;
  const tableId = reader.uint();
  const schemaVersion = reader.uint();
  const nextRowId = reader.uint();
  const flushedThroughLsn = reader.uint();
  const liveRowCount = reader.uint();
  const groups = refs.blob();
  const groupCount = reader.uint();
  const stagedCount = reader.count(8);
  const staged: Array<TableRoot["staged"][number]> = [];
  for (let k = 0; k < stagedCount; k++) {
    const transaction = reader.uint();
    const firstRowId = reader.uint();
    const rowCount = reader.uint();
    const stagedGroups = refs.blob();
    const runCount = reader.count(2);
    const indexRuns: Array<{ indexId: number; runs: RunRef[] }> = [];
    for (let r = 0; r < runCount; r++)
      indexRuns.push({ indexId: reader.uint(), runs: readRuns(refs) });
    staged.push({ transaction, firstRowId, rowCount, groups: stagedGroups, indexRuns });
  }
  const keyIndex = readFlag(reader) ? readIndex(refs) : null;
  const secondaryCount = reader.count(3);
  const secondaryIndexes: IndexRoot[] = [];
  for (let k = 0; k < secondaryCount; k++) secondaryIndexes.push(readIndex(refs));
  const statistics = refs.optionalBlob();
  return {
    tableId,
    schemaVersion,
    nextRowId,
    flushedThroughLsn,
    liveRowCount,
    groups,
    groupCount,
    staged,
    keyIndex,
    secondaryIndexes,
    statistics,
  };
}

function writeClient(out: ByteWriter, client: ClientLedgerEntry): void {
  out.string(client.clientId);
  out.uint(client.resolvedThrough);
  out.uint(client.outcomes.length);
  for (const outcome of client.outcomes) {
    out.uint(outcome.sequence);
    out.uint(outcome.lsn);
    // Sorted by key (UTF-16 code units): the canonical order, so a decoder can see duplicates.
    const counts = Object.entries(outcome.counts).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    out.uint(counts.length);
    for (const [key, value] of counts) {
      out.string(key);
      out.f64(value);
    }
    out.f64(outcome.recordedAtMs);
  }
}

function readClient(reader: ByteReader): ClientLedgerEntry {
  const clientId = reader.string();
  const resolvedThrough = reader.uint();
  const count = reader.count(11);
  const outcomes: Array<ClientLedgerEntry["outcomes"][number]> = [];
  for (let k = 0; k < count; k++) {
    const sequence = reader.uint();
    const lsn = reader.uint();
    const countCount = reader.count(9);
    // A null-prototype map: "__proto__" is a key like any other (review B2-p2a #4).
    const counts = Object.create(null) as Record<string, number>;
    let previous: string | null = null;
    for (let c = 0; c < countCount; c++) {
      const key = reader.string();
      valid(previous === null || key > previous, "Ledger counts are duplicated or out of order");
      previous = key;
      counts[key] = reader.f64();
    }
    outcomes.push({ sequence, lsn, counts, recordedAtMs: reader.f64() });
  }
  return { clientId, resolvedThrough, outcomes };
}

/** The root's bytes (≤ `MAX_ROOT_BYTES`), for the root slot (storage/root-slot.ts). */
export function encodeManifestRoot(root: ManifestRoot): Uint8Array {
  const out = new RefWriter();
  const body = out.body;
  body.uint(root.formatVersion);
  body.uint(root.sequence);
  body.uint(root.epoch);
  body.u32(root.log.salt);
  body.uint(root.log.firstLsn);
  body.uint(root.log.rootSequence);
  body.uint(root.globalLsn);
  body.uint(root.catalogVersion);
  out.blob(root.catalog);
  body.uint(root.nextTableId);
  body.uint(root.sequences.length);
  for (const sequence of root.sequences) {
    body.uint(sequence.sequenceId);
    body.bigint(sequence.reservedThrough);
  }
  body.uint(root.tables.length);
  for (const table of root.tables) writeTable(out, table);
  body.uint(root.clients.length);
  for (const client of root.clients) writeClient(body, client);
  body.uint(root.extentList.length);
  for (const page of root.extentList) {
    out.blob(page.blob);
    body.uint(page.first);
    body.uint(page.count);
  }
  return out.finish(MAGIC.root, MAX_ROOT_BYTES, "A manifest root");
}

export function decodeManifestRoot(bytes: Uint8Array, budget: ByteBudget): Parsed<ManifestRoot> {
  return charged(
    bytes,
    budget,
    (reader) => {
      const refs = new RefReader(reader, MAGIC.root, "manifest root");
      const formatVersion = reader.uint();
      valid(
        formatVersion <= MANIFEST_FORMAT_VERSION,
        `Manifest format ${String(formatVersion)} is newer than this build reads`,
      );
      // Format 1 predates the extent list (ruling R25): only development builds wrote it, so it
      // is refused rather than upgraded (DECISIONS.md "R25").
      valid(
        formatVersion === MANIFEST_FORMAT_VERSION,
        `Manifest format ${String(formatVersion)} was written by a development build before extent lists; no supported release reads it`,
      );
      const sequence = reader.uint();
      const epoch = reader.uint();
      const log = { salt: reader.u32(), firstLsn: reader.uint(), rootSequence: reader.uint() };
      const globalLsn = reader.uint();
      const catalogVersion = reader.uint();
      const catalog = refs.blob();
      const nextTableId = reader.uint();
      const sequenceCount = reader.count(2);
      const sequences: Array<ManifestRoot["sequences"][number]> = [];
      for (let k = 0; k < sequenceCount; k++)
        sequences.push({ sequenceId: reader.uint(), reservedThrough: reader.bigint() });
      const tableCount = reader.count(16);
      const tables: TableRoot[] = [];
      const tableIds = new Set<number>();
      for (let k = 0; k < tableCount; k++) {
        const table = readTable(refs);
        valid(!tableIds.has(table.tableId), "A table appears twice in the root");
        tableIds.add(table.tableId);
        tables.push(table);
      }
      const clientCount = reader.count(3);
      const clients: ClientLedgerEntry[] = [];
      for (let k = 0; k < clientCount; k++) clients.push(readClient(reader));
      const pageCount = reader.count(3);
      const extentList: ExtentListPage[] = [];
      for (let k = 0; k < pageCount; k++) {
        const blob = refs.blob();
        const first = reader.uint();
        const count = reader.uint();
        valid(count >= 1 && count <= LIST_PAGE_EXTENTS, "An extent list page has a bad count");
        valid(
          k === 0 || first > (extentList[k - 1]?.first ?? -1),
          "The extent list's pages are not in offset order",
        );
        extentList.push({ blob, first, count });
      }
      refs.end();
      return {
        formatVersion,
        sequence,
        epoch,
        log,
        globalLsn,
        catalogVersion,
        catalog,
        nextTableId,
        sequences,
        tables,
        clients,
        extentList,
      };
    },
    MAX_ROOT_BYTES,
  );
}
