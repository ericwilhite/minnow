/**
 * Spill pages and partition files: one codec for every operator that spills (ruling R20.2, owned
 * by track B7): hash-join partitions now, aggregation partitions and sort runs next. The page layout is the one spill/contract.ts fixes; the service checksums each
 * page (xxHash32) and refuses a corrupt one on read.
 *
 * ## Page layout (little-endian, every buffer 8-byte aligned)
 * ```text
 * header (32 B): magic "MNSP" | query id | run | partition | sequence | row count | column count | flags
 * per column (16 B): layout u8 | type kind u8 | tier u8 | flags u8 | uniform scale u32 | 0 u32 | 0 u32
 *   flags: 1 validity, 2 per-slot scales, 4 specials
 *   then its buffers, each: byte length u32 | 0 u32 | bytes, padded to 8:
 *     [validity words] then by layout:
 *     bool u8 | i32 i32 | f32 f32 | f64 f64 | fixed16 16 B | varlen offsets (rows + 1) i32, bytes
 *     int64: f64 (tier safe) or i64 | interval: months i32, days i32, µs (f64 or i64)
 *     decimal: [scales u16], coefficients (f64 | i64 | int128 limbs u32×4 | wide: offsets i32 +
 *     two's-complement little-endian bytes), [specials u8]
 * ```
 * Uncompressed: spill is bandwidth-bound and short-lived. Every `VectorData` variant round-trips
 * exactly, the int64 and decimal tiers included (a page keeps its tier).
 *
 * Decoding checks the magic, the counts, every descriptor against the reader's expected types and
 * every length against the page: a mismatch is `SpillCorruptionError`, never a wrong row.
 */
import type { SpillFile, SpillPageRef, SpillService } from "../spill/contract.js";
import { SPILL_PAGE_MAGIC, SpillCorruptionError } from "../spill/contract.js";
import type { PhysicalKind, PhysicalType } from "../types.js";
import { layoutOf } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { FlatVector, VectorData } from "../vector/vector.js";
import { TIER_INT128, TIER_INT64, TIER_SAFE, TIER_WIDE } from "./column-store.js";

const HEADER_BYTES = 32;
const COLUMN_BYTES = 16;
const LAYOUTS = [
  "bool",
  "i32",
  "int64",
  "f64",
  "f32",
  "decimal",
  "varlen",
  "fixed16",
  "interval",
] as const;
const KINDS: readonly PhysicalKind[] = [
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
  "locator",
];

export interface PageHeader {
  readonly queryId: number;
  readonly run: number;
  readonly partition: number;
  readonly sequence: number;
}

const pad8 = (n: number) => (n + 7) & ~7;

function tierOf(data: VectorData): number {
  switch (data.layout) {
    case "int64":
      return data.values.repr === "safe" ? TIER_SAFE : TIER_INT64;
    case "interval":
      return data.micros.repr === "safe" ? TIER_SAFE : TIER_INT64;
    case "decimal":
      switch (data.coefficients.repr) {
        case "safe":
          return TIER_SAFE;
        case "int64":
          return TIER_INT64;
        case "int128":
          return TIER_INT128;
        case "wide":
          return TIER_WIDE;
      }
    // falls through
    default:
      return 0;
  }
}

/** Two's-complement little-endian bytes of a bigint (minimal length, at least one byte). */
function bigintToBytes(value: bigint): Uint8Array {
  let bytes = 1;
  while (BigInt.asIntN(bytes * 8, value) !== value) bytes++;
  const unsigned = BigInt.asUintN(bytes * 8, value);
  const out = new Uint8Array(bytes);
  for (let i = 0; i < bytes; i++) out[i] = Number((unsigned >> BigInt(i * 8)) & 0xffn);
  return out;
}

function bytesToBigint(bytes: Uint8Array, start: number, end: number): bigint {
  let unsigned = 0n;
  for (let i = end - 1; i >= start; i--) unsigned = (unsigned << 8n) | BigInt(bytes[i] ?? 0);
  return BigInt.asIntN((end - start) * 8, unsigned);
}

/** The byte buffers of column `vector` (rows 0 … rows), in page order. */
function buffersOf(
  vector: FlatVector,
  rows: number,
): { flags: number; scale: number; buffers: Uint8Array[] } {
  const data = vector.data;
  const buffers: Uint8Array[] = [];
  let flags = 0;
  let scale = 0xffff_ffff;
  const view = (array: ArrayBufferView, bytes: number) =>
    new Uint8Array(array.buffer, array.byteOffset, bytes);
  if (vector.validity !== null) {
    flags |= 1;
    buffers.push(view(vector.validity, ((rows + 31) >>> 5) * 4));
  }
  switch (data.layout) {
    case "bool":
      buffers.push(view(data.values, rows));
      break;
    case "i32":
    case "f32":
      buffers.push(view(data.values, rows * 4));
      break;
    case "f64":
      buffers.push(view(data.values, rows * 8));
      break;
    case "int64":
      buffers.push(view(data.values.values, rows * 8));
      break;
    case "fixed16":
      buffers.push(view(data.bytes, rows * 16));
      break;
    case "interval":
      buffers.push(
        view(data.months, rows * 4),
        view(data.days, rows * 4),
        view(data.micros.values, rows * 8),
      );
      break;
    case "varlen": {
      const base = data.offsets[0] ?? 0;
      const end = data.offsets[rows] ?? 0;
      const offsets = new Int32Array(rows + 1);
      for (let i = 0; i <= rows; i++) offsets[i] = (data.offsets[i] ?? 0) - base;
      buffers.push(view(offsets, (rows + 1) * 4), data.bytes.subarray(base, end));
      break;
    }
    case "decimal": {
      if (data.scale === null) {
        flags |= 2;
        buffers.push(view(data.scales ?? new Uint16Array(rows), rows * 2));
      } else scale = data.scale;
      const coefficients = data.coefficients;
      if (coefficients.repr === "safe" || coefficients.repr === "int64")
        buffers.push(view(coefficients.values, rows * 8));
      else if (coefficients.repr === "int128") buffers.push(view(coefficients.limbs, rows * 16));
      else {
        const parts = coefficients.values.slice(0, rows).map(bigintToBytes);
        const offsets = new Int32Array(rows + 1);
        let total = 0;
        for (let i = 0; i < rows; i++) {
          total += parts[i]?.length ?? 0;
          offsets[i + 1] = total;
        }
        const bytes = new Uint8Array(total);
        for (let i = 0; i < rows; i++) bytes.set(parts[i] ?? new Uint8Array(0), offsets[i] ?? 0);
        buffers.push(view(offsets, (rows + 1) * 4), bytes);
      }
      if (data.specials !== null) {
        flags |= 4;
        buffers.push(view(data.specials, rows));
      }
      break;
    }
  }
  return { flags, scale, buffers };
}

/** A reusable page buffer, grown under a charge. */
export class PageWriter {
  #bytes: Uint8Array;
  #view: DataView;
  readonly #charge: ByteCharge;
  readonly #initial: number;

  constructor(charge: ByteCharge, initialBytes = 64 * 1024) {
    if (!charge.allocate(initialBytes)) throw new RangeError("A page buffer must be admitted");
    this.#charge = charge;
    this.#initial = initialBytes;
    this.#bytes = new Uint8Array(initialBytes);
    this.#view = new DataView(this.#bytes.buffer);
  }

  get retainedBytes(): number {
    return this.#bytes.byteLength;
  }

  static encodedBytes(columns: readonly FlatVector[], rows: number): number {
    let total = HEADER_BYTES;
    for (const column of columns) {
      total += COLUMN_BYTES;
      for (const buffer of buffersOf(column, rows).buffers) total += 8 + pad8(buffer.length);
    }
    return total;
  }

  /**
   * Encodes rows 0 … rows of `columns` into the buffer and returns a view of the page (valid
   * until the next `encode`), or null when growing the buffer was refused.
   */
  encode(header: PageHeader, columns: readonly FlatVector[], rows: number): Uint8Array | null {
    const parts = columns.map((column) => buffersOf(column, rows));
    let total = HEADER_BYTES;
    for (const part of parts) {
      total += COLUMN_BYTES;
      for (const buffer of part.buffers) total += 8 + pad8(buffer.length);
    }
    if (total > this.#bytes.length) {
      // Grow to the page (rounded to 4 KiB): the new buffer is charged before the old is freed.
      const size = Math.max(this.#initial, (total + 4095) & ~4095);
      if (!this.#charge.allocate(size)) return null;
      this.#charge.free(this.#bytes.length);
      this.#bytes = new Uint8Array(size);
      this.#view = new DataView(this.#bytes.buffer);
    }
    const view = this.#view;
    const bytes = this.#bytes;
    bytes.fill(0, 0, total);
    view.setUint32(0, SPILL_PAGE_MAGIC, true);
    view.setUint32(4, header.queryId >>> 0, true);
    view.setUint32(8, header.run >>> 0, true);
    view.setUint32(12, header.partition >>> 0, true);
    view.setUint32(16, header.sequence >>> 0, true);
    view.setUint32(20, rows, true);
    view.setUint32(24, columns.length, true);
    view.setUint32(28, 0, true);
    let at = HEADER_BYTES;
    for (let c = 0; c < columns.length; c++) {
      const column = columns[c];
      const part = parts[c];
      if (column === undefined || part === undefined) continue;
      view.setUint8(at, LAYOUTS.indexOf(column.data.layout));
      view.setUint8(at + 1, KINDS.indexOf(column.type.kind));
      view.setUint8(at + 2, tierOf(column.data));
      view.setUint8(at + 3, part.flags);
      view.setUint32(at + 4, part.scale, true);
      at += COLUMN_BYTES;
      for (const buffer of part.buffers) {
        view.setUint32(at, buffer.length, true);
        at += 8;
        bytes.set(buffer, at);
        at += pad8(buffer.length);
      }
    }
    return bytes.subarray(0, total);
  }

  /** Bytes growing the buffer for this page would charge (0 when it fits). */
  growthFor(columns: readonly FlatVector[], rows: number): number {
    const total = PageWriter.encodedBytes(columns, rows);
    return total > this.#bytes.length ? Math.max(this.#initial, (total + 4095) & ~4095) : 0;
  }

  /** Back to the initial buffer (when a progress step must give its bytes back). */
  shrinkIf(shrink: boolean): void {
    if (!shrink || this.#bytes.length <= this.#initial) return;
    this.#charge.free(this.#bytes.length - this.#initial);
    this.#bytes = new Uint8Array(this.#initial);
    this.#view = new DataView(this.#bytes.buffer);
  }

  close(): void {
    this.#charge.free(this.#bytes.byteLength);
    this.#bytes = new Uint8Array(0);
  }
}

function corrupt(why: string): never {
  throw new SpillCorruptionError(`Spill page is invalid: ${why}`);
}

/** Header fields of a page (checked magic). */
export function pageHeader(page: Uint8Array): PageHeader & { rows: number; columns: number } {
  if (page.length < HEADER_BYTES) corrupt("short header");
  const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
  if (view.getUint32(0, true) !== SPILL_PAGE_MAGIC) corrupt("bad magic");
  return {
    queryId: view.getUint32(4, true),
    run: view.getUint32(8, true),
    partition: view.getUint32(12, true),
    sequence: view.getUint32(16, true),
    rows: view.getUint32(20, true),
    columns: view.getUint32(24, true),
  };
}

export interface DecodedPage {
  readonly rows: number;
  /** Views of the page's bytes: valid while the caller holds the page. */
  readonly columns: FlatVector[];
  /** Bytes charged for copies (a misaligned page, wide decimals); the caller frees them. */
  readonly charged: number;
}

/**
 * Decodes a page into flat vectors of `types` (length = rows). Every buffer is a view of `page`
 * (they live as long as the page bytes, which the caller holds and has charged). Only what cannot
 * be a view is allocated, charged to `charge` first: a copy of a page that is not 8-byte aligned,
 * and the BigInts of wide decimals. If the charge refuses, nothing stays allocated and the result
 * is `{ needed }`, the bytes to make room for (then decode again).
 */
export function decodePage(
  page: Uint8Array,
  types: readonly PhysicalType[],
  charge: ByteCharge,
): DecodedPage | { readonly needed: number } {
  let copied = 0;
  if (page.byteOffset % 8 !== 0) {
    if (!charge.allocate(page.length)) return { needed: page.length };
    copied = page.length;
    page = page.slice();
  }
  // Sizing and decoding share one refund scope: whatever throws, the copy's charge goes back.
  let wide: number;
  try {
    wide = wideBytes(page, types);
    if (wide > 0 && !charge.allocate(wide)) {
      if (copied > 0) charge.free(copied);
      return { needed: copied + wide };
    }
  } catch (error) {
    if (copied > 0) charge.free(copied);
    throw error;
  }
  try {
    return { ...decodeViews(page, types), charged: copied + wide };
  } catch (error) {
    charge.free(copied + wide);
    throw error;
  }
}

/** Bytes the BigInts of a page's wide decimal columns occupy (0 without any). */
function wideBytes(page: Uint8Array, types: readonly PhysicalType[]): number {
  if (!types.some((type) => type.kind === "decimal")) return 0;
  let total = 0;
  // A BigInt of N magnitude bytes: a header plus 64-bit digits (memory/governor.ts
  // `bigintBytes`), computed from N alone, so nothing value-sized exists before admission.
  decodeViews(page, types, (length) => {
    total += 16 + 8 * Math.max(1, Math.ceil(length / 8));
  });
  return total;
}

/** The structural decode, every buffer a view; `onWide` sees each wide value's byte length. */
function decodeViews(
  page: Uint8Array,
  types: readonly PhysicalType[],
  onWide?: (length: number) => void,
): { rows: number; columns: FlatVector[] } {
  const header = pageHeader(page);
  const rows = header.rows;
  if (header.columns !== types.length) corrupt("column count");
  const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
  let at = HEADER_BYTES;
  const take = (expected: number | null): Uint8Array => {
    if (at + 8 > page.length) corrupt("truncated buffer header");
    const length = view.getUint32(at, true);
    at += 8;
    if (expected !== null && length !== expected) corrupt("buffer length");
    if (at + length > page.length) corrupt("truncated buffer");
    const bytes = page.subarray(at, at + length);
    at += pad8(length);
    return bytes;
  };
  // Buffers are 8-byte aligned within an 8-byte-aligned page: every typed view is legal.
  const typed = <T>(
    make: (buffer: ArrayBufferLike, offset: number, length: number) => T,
    bytes: Uint8Array,
    width: number,
  ): T => {
    if (bytes.byteOffset % width !== 0) corrupt("misaligned buffer");
    return make(bytes.buffer, bytes.byteOffset, bytes.byteLength / width);
  };
  const columns: FlatVector[] = [];
  for (const type of types) {
    if (at + COLUMN_BYTES > page.length) corrupt("truncated column");
    const layout = LAYOUTS[view.getUint8(at)];
    const kind = KINDS[view.getUint8(at + 1)];
    const tier = view.getUint8(at + 2);
    const flags = view.getUint8(at + 3);
    const scale = view.getUint32(at + 4, true);
    at += COLUMN_BYTES;
    if (layout !== layoutOf(type) || kind !== type.kind) corrupt("column type");
    let validity: Uint32Array | null = null;
    if ((flags & 1) !== 0)
      validity = typed((b, o, l) => new Uint32Array(b, o, l), take(((rows + 31) >>> 5) * 4), 4);
    let data: VectorData;
    const int64 = (bytes: Uint8Array) =>
      tier === TIER_SAFE
        ? ({
            repr: "safe",
            values: typed((b, o, l) => new Float64Array(b, o, l), bytes, 8),
          } as const)
        : ({
            repr: "int64",
            values: typed((b, o, l) => new BigInt64Array(b, o, l), bytes, 8),
          } as const);
    switch (layout) {
      case "bool":
        data = { layout, values: take(rows) };
        break;
      case "i32":
        data = { layout, values: typed((b, o, l) => new Int32Array(b, o, l), take(rows * 4), 4) };
        break;
      case "f32":
        data = { layout, values: typed((b, o, l) => new Float32Array(b, o, l), take(rows * 4), 4) };
        break;
      case "f64":
        data = { layout, values: typed((b, o, l) => new Float64Array(b, o, l), take(rows * 8), 8) };
        break;
      case "int64":
        if (tier > TIER_INT64) corrupt("int64 tier");
        data = { layout, values: int64(take(rows * 8)) };
        break;
      case "fixed16":
        data = { layout, bytes: take(rows * 16) };
        break;
      case "interval": {
        if (tier > TIER_INT64) corrupt("interval tier");
        const months = typed((b, o, l) => new Int32Array(b, o, l), take(rows * 4), 4);
        const days = typed((b, o, l) => new Int32Array(b, o, l), take(rows * 4), 4);
        data = { layout, months, days, micros: int64(take(rows * 8)) };
        break;
      }
      case "varlen": {
        const offsets = typed((b, o, l) => new Int32Array(b, o, l), take((rows + 1) * 4), 4);
        const bytes = take(null);
        if ((offsets[0] ?? 0) !== 0 || (offsets[rows] ?? 0) !== bytes.length)
          corrupt("varlen offsets");
        for (let i = 0; i < rows; i++)
          if ((offsets[i + 1] ?? 0) < (offsets[i] ?? 0)) corrupt("varlen offsets order");
        data = { layout, bytes, offsets };
        break;
      }
      case "decimal": {
        const declared = type.kind === "decimal" ? type.scale : null;
        let scales: Uint16Array | null = null;
        if ((flags & 2) !== 0)
          scales = typed((b, o, l) => new Uint16Array(b, o, l), take(rows * 2), 2);
        else if (declared === null && scale === 0xffff_ffff) corrupt("decimal scale");
        let coefficients: Extract<VectorData, { layout: "decimal" }>["coefficients"];
        if (tier === TIER_SAFE || tier === TIER_INT64) coefficients = int64(take(rows * 8));
        else if (tier === TIER_INT128)
          coefficients = {
            repr: "int128",
            limbs: typed((b, o, l) => new Uint32Array(b, o, l), take(rows * 16), 4),
          };
        else if (tier === TIER_WIDE) {
          const offsets = typed((b, o, l) => new Int32Array(b, o, l), take((rows + 1) * 4), 4);
          const bytes = take(null);
          const values: bigint[] = [];
          for (let i = 0; i < rows; i++) {
            const start = offsets[i] ?? 0;
            const end = offsets[i + 1] ?? 0;
            if (end < start || end > bytes.length) corrupt("wide decimal offsets");
            if (onWide !== undefined) onWide(end - start);
            else values.push(bytesToBigint(bytes, start, end));
          }
          coefficients = { repr: "wide", values };
        } else corrupt("decimal tier");
        const specials = (flags & 4) !== 0 ? take(rows) : null;
        data = {
          layout,
          scale: scales === null ? scale : null,
          scales,
          coefficients,
          specials,
        };
        break;
      }
      default:
        corrupt("layout");
    }
    columns.push({ form: "flat", type, length: rows, validity, data });
  }
  if (pad8(at) > pad8(page.length)) corrupt("trailing bytes");
  return { rows, columns };
}

/** One side (build or probe) of one partition: its pages on spill. */
export class SpilledRun {
  readonly pages: SpillPageRef[] = [];
  rows = 0;
  bytes = 0;
  file: SpillFile | null = null;
}

/** The operator closed its spill files: a create or append that resolves later is refused. */
export class SpillClosedError extends Error {
  readonly code = "MINNOW_SPILL_CLOSED";
  constructor() {
    super("The operator's spill files are closed");
    this.name = "SpillClosedError";
  }
}

/**
 * Lazily created spill files for a partitioned operator: one file per run, closed together.
 *
 * Every operation that can still close a file (a creation that resolves after the close began,
 * a `release`, `closeAll` itself) is tracked in one set; `closed` settles only when all of them
 * have, and rejects with their failures (one error, or an `AggregateError`). So cleanup is never
 * reported complete while a close is in flight, and no close failure is dropped.
 */
export class SpillFiles {
  readonly #service: SpillService;
  readonly #queryId: number;
  readonly #budget: ByteCharge;
  readonly #open = new Set<SpillFile>();
  readonly #inFlight = new Set<Promise<void>>();
  readonly #failures: unknown[] = [];
  #closing = false;
  #closed: Promise<void> | null = null;
  /** Bytes written, for the profile. */
  written = 0;

  constructor(service: SpillService, queryId: number, budget: ByteCharge) {
    this.#service = service;
    this.#queryId = queryId;
    this.#budget = budget;
  }

  get isClosed(): boolean {
    return this.#closing;
  }

  /** Settles when `closeAll` finished every tracked close (rejects with their failures); null before. */
  get closed(): Promise<void> | null {
    return this.#closed;
  }

  /** Tracks an operation that may close a file; its failure is kept for `closed`. */
  #track(operation: Promise<unknown>): void {
    const tracked: Promise<void> = operation.then(
      () => undefined,
      (error: unknown) => {
        this.#failures.push(error);
      },
    );
    this.#inFlight.add(tracked);
    void tracked.then(() => this.#inFlight.delete(tracked));
  }

  async append(run: SpilledRun, page: Uint8Array, rows: number): Promise<void> {
    if (this.#closing) throw new SpillClosedError();
    if (run.file === null) {
      // The creation and, if the close began meanwhile, the late file's close are one tracked
      // operation, so `closed` waits for that close and sees its failure.
      const created = this.#service.create(this.#queryId, this.#budget).then(async (file) => {
        if (!this.#closing) return file;
        await file.close();
        return null;
      });
      this.#track(created);
      const file = await created;
      if (file === null || this.isClosed) {
        if (file !== null) this.#track(file.close());
        throw new SpillClosedError();
      }
      run.file = file;
      this.#open.add(file);
    }
    const ref = await run.file.append(page);
    if (this.isClosed) throw new SpillClosedError();
    run.pages.push(ref);
    run.rows += rows;
    run.bytes += page.length;
    this.written += page.length;
  }

  /** Closes a run's file (its pages are gone); the close is tracked until it settles. */
  async release(run: SpilledRun): Promise<void> {
    const file = run.file;
    run.file = null;
    run.pages.length = 0;
    if (file === null) return;
    this.#open.delete(file);
    const closing = file.close();
    this.#track(closing);
    await closing;
  }

  /** Closes every file, now and created later; idempotent (returns the same Promise). */
  closeAll(): Promise<void> {
    this.#closing = true;
    this.#closed ??= (async () => {
      // Drain until nothing is in flight: a late creation may add its own close meanwhile.
      for (;;) {
        for (const file of [...this.#open]) {
          this.#open.delete(file);
          this.#track(file.close());
        }
        if (this.#inFlight.size === 0) break;
        await Promise.all([...this.#inFlight]);
      }
      if (this.#failures.length === 1) throw this.#failures[0];
      if (this.#failures.length > 1)
        throw new AggregateError(
          this.#failures,
          `Spill files failed to close: ${String((this.#failures[0] as Error | undefined)?.message ?? this.#failures[0])} (and ${String(this.#failures.length - 1)} more)`,
        );
    })();
    return this.#closed;
  }
}
