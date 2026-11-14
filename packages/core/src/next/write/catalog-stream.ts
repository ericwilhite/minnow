/** MCAT logical stream, shared by native catalog pages and portable catalog frames.
 * One owned logical record at a time; native writers do not concatenate the entire catalog.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import {
  CATALOG_NATIVE_GRAMMAR as grammar,
  CATALOG_FTS_FEATURE,
  CATALOG_RECORD_KINDS as kinds,
} from "../durable-format.js";
import type { FtsDef } from "../access/fts-def.js";
import type { TableDef, SequenceDef, ViewDef, TypeDef } from "../access/catalog.js";
import type { CatalogCounters, LoggedChange } from "./catalog.js";
import type { BufferAllocator } from "./commit-buffers.js";
import { ByteReader, type ByteSink } from "./bytes.js";
import { StoreCorruptionError } from "../storage/contract.js";
import {
  encodeCatalogRecord,
  decodeCatalogRecord,
  readCatalogRecord,
  retainCatalogExtensions,
  UnsupportedCatalogError,
  type CatalogRecordKind,
  catalogRecordSize,
  writeCatalogRecord,
} from "./catalog-records.js";
export interface CatalogStreamData {
  readonly version: number;
  readonly counters: CatalogCounters;
  readonly tables: readonly TableDef[];
  readonly sequences: readonly SequenceDef[];
  readonly views: readonly ViewDef[];
  readonly types: readonly TypeDef[];
  readonly fts: readonly FtsDef[];
  /** Unknown display fields from the metadata record; never consulted for semantics. */
  readonly wireMetadata?: unknown;
}
interface Metadata {
  version: number;
  counters: CatalogCounters;
  requiredFeatures: readonly number[];
  sections: ReadonlyArray<{ kind: number; count: number }>;
}
const sections = [
  ["table", "tables"],
  ["sequence", "sequences"],
  ["view", "views"],
  ["namedType", "types"],
  ["fts", "fts"],
] as const;
const HEADER_BYTES = 16;
export const CATALOG_RECORD_BYTES = 4 * 1024 * 1024;
function check(ok: boolean, message: string): asserts ok {
  if (!ok) throw new StoreCorruptionError(message);
}
function header(magic: number, allocator?: BufferAllocator): { bytes: Uint8Array; close(): void } {
  const bytes = allocator?.create(HEADER_BYTES) ?? new Uint8Array(HEADER_BYTES),
    view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  view.setUint32(0, magic, true);
  view.setUint16(4, grammar.version, true);
  view.setUint16(6, 0, true);
  view.setUint32(8, HEADER_BYTES, true);
  view.setUint32(12, grammar.expressionDialect, true);
  let closed = false;
  return {
    bytes,
    close() {
      if (!closed) {
        closed = true;
        allocator?.release(bytes);
      }
    },
  };
}
function readHeader(input: ByteReader, magic: number): void {
  check(input.u32() === magic, "Not a catalog logical stream");
  const version = input.u16();
  if (version !== grammar.version)
    throw new UnsupportedCatalogError(`stream version ${String(version)}`);
  check(input.u16() === 0 && input.u32() === HEADER_BYTES, "Invalid catalog stream header");
  const dialect = input.u32();
  if (dialect !== grammar.expressionDialect)
    throw new UnsupportedCatalogError(`expression dialect ${String(dialect)}`);
}
function metadata(data: CatalogStreamData): Metadata {
  const result = {
    version: data.version,
    counters: data.counters,
    requiredFeatures: data.fts.length ? [CATALOG_FTS_FEATURE] : [],
    sections: sections.map(([kind, key]) => ({ kind: kinds[kind], count: data[key].length })),
  };
  retainCatalogExtensions(data.wireMetadata, result);
  return result;
}
function* ownedRecord(
  kind: CatalogRecordKind,
  value: unknown,
  allocator?: BufferAllocator,
): Generator<Uint8Array> {
  check(
    catalogRecordSize(kind, value) <= CATALOG_RECORD_BYTES,
    "Catalog record exceeds its4MiB limit",
  );
  const encoded = encodeCatalogRecord(kind, value, allocator);
  try {
    check(encoded.bytes.length <= CATALOG_RECORD_BYTES, "Catalog record exceeds its4MiB limit");
    yield encoded.bytes;
  } finally {
    encoded.close();
  }
}
export function* catalogStream(
  data: CatalogStreamData,
  allocator?: BufferAllocator,
): Generator<Uint8Array> {
  const first = header(grammar.streamMagic, allocator);
  try {
    yield first.bytes;
  } finally {
    first.close();
  }
  yield* ownedRecord("catalog", metadata(data), allocator);
  for (const [kind, key] of sections)
    for (const record of data[key]) yield* ownedRecord(kind, record, allocator);
}
/** Measure without allocating encoded record buffers or a whole-catalog string. */
export function catalogStreamByteLength(data: CatalogStreamData): number {
  let size = HEADER_BYTES + catalogRecordSize("catalog", metadata(data));
  for (const [kind, key] of sections)
    for (const record of data[key]) {
      const n = catalogRecordSize(kind, record);
      check(n <= CATALOG_RECORD_BYTES, "Catalog record exceeds its4MiB limit");
      size += n;
      check(Number.isSafeInteger(size), "Catalog stream length exceeds u53");
    }
  return size;
}
function concat(parts: Iterable<Uint8Array>): Uint8Array {
  // Compatibility/test convenience only. Native persistence consumes catalogStream directly.
  const copies: Uint8Array[] = [];
  let size = 0;
  for (const part of parts) {
    copies.push(part.slice());
    size += part.length;
  }
  const result = new Uint8Array(size);
  let at = 0;
  for (const part of copies) {
    result.set(part, at);
    at += part.length;
  }
  return result;
}
export function encodeCatalogStream(data: CatalogStreamData): Uint8Array {
  return concat(catalogStream(data));
}
function checkMetadata(value: unknown): Metadata {
  const meta = value as Metadata;
  let previousFeature = 0;
  for (const feature of meta.requiredFeatures) {
    check(feature > previousFeature, "Duplicate/out-of-order catalog required feature");
    previousFeature = feature;
  }
  if (meta.requiredFeatures.some((feature) => feature !== CATALOG_FTS_FEATURE))
    throw new UnsupportedCatalogError(`features ${meta.requiredFeatures.join(",")}`);
  check(meta.sections.length === sections.length, "Catalog section set is incomplete");
  for (const [i, [kind]] of sections.entries())
    check(meta.sections[i]?.kind === kinds[kind], "Duplicate/out-of-order catalog section");
  return meta;
}
export function decodeCatalogStream(bytes: Uint8Array): CatalogStreamData {
  const input = new ByteReader(bytes);
  readHeader(input, grammar.streamMagic);
  const meta = checkMetadata(readCatalogRecord("catalog", input));
  const groups: Record<string, unknown[]> = {
    tables: [],
    sequences: [],
    views: [],
    types: [],
    fts: [],
  };
  for (const [i, [kind, key]] of sections.entries()) {
    const count = meta.sections[i]?.count;
    check(
      count !== undefined && count <= Math.floor((input.bytes.length - input.offset) / 3),
      "Catalog count exceeds remaining bytes",
    );
    for (let j = 0; j < count; j++) {
      const at = input.offset,
        value = readCatalogRecord(kind, input);
      check(input.offset - at <= CATALOG_RECORD_BYTES, "Catalog record exceeds its4MiB limit");
      groups[key]?.push(value);
    }
  }
  input.end();
  return {
    version: meta.version,
    counters: meta.counters,
    tables: groups.tables as TableDef[],
    sequences: groups.sequences as SequenceDef[],
    views: groups.views as ViewDef[],
    types: groups.types as TypeDef[],
    fts: groups.fts as FtsDef[],
    wireMetadata: meta,
  };
}
/** Same MCHG wire in the bounded multipart commit path, with no record-sized allocation. */
export function catalogChangeByteLength(change: LoggedChange): number {
  const size = catalogRecordSize("change", change);
  check(size <= CATALOG_RECORD_BYTES, "Catalog change exceeds its4MiB limit");
  return HEADER_BYTES + size;
}
export function writeCatalogChange(out: ByteSink, change: LoggedChange): void {
  catalogChangeByteLength(change);
  out.u32(grammar.changeMagic);
  out.u16(grammar.version);
  out.u16(0);
  out.u32(HEADER_BYTES);
  out.u32(grammar.expressionDialect);
  writeCatalogRecord(out, "change", change);
}
export function encodeCatalogChange(change: LoggedChange, allocator?: BufferAllocator): Uint8Array {
  check(
    catalogRecordSize("change", change) <= CATALOG_RECORD_BYTES,
    "Catalog change exceeds its4MiB limit",
  );
  const first = header(grammar.changeMagic);
  try {
    // Transfer the exact allocation to the WAL caller; it releases through its allocator.
    return encodeCatalogRecord("change", change, allocator, first.bytes).bytes;
  } finally {
    first.close();
  }
}
export function decodeCatalogChange(bytes: Uint8Array): LoggedChange {
  const input = new ByteReader(bytes);
  readHeader(input, grammar.changeMagic);
  const result = readCatalogRecord("change", input) as LoggedChange;
  input.end();
  return result;
}

/** Streaming native/portable decoder. Only one encoded record, never the whole stream,
 * is held at once. Catalog definitions returned here remain owned by the caller. */
export async function decodeCatalogParts(
  parts: AsyncIterable<Uint8Array>,
  allocator: BufferAllocator,
  work?: TaskWork,
): Promise<CatalogStreamData> {
  const iterator = parts[Symbol.asyncIterator]();
  let current: Uint8Array | undefined,
    at = 0;
  async function nextPart(): Promise<boolean> {
    current = undefined;
    at = 0;
    for (;;) {
      await pauseIfDue(work);
      const next = await iterator.next();
      if (next.done) return false;
      if (next.value.length) {
        current = next.value;
        return true;
      }
    }
  }
  async function copy(into: Uint8Array, from: number, count: number): Promise<void> {
    while (count) {
      if (!current || at === current.length) check(await nextPart(), "Catalog stream ends early");
      const part = current;
      check(part !== undefined, "Missing catalog fragment");
      const n = Math.min(count, part.length - at);
      into.set(part.subarray(at, at + n), from);
      from += n;
      at += n;
      count -= n;
    }
  }
  async function one(): Promise<number> {
    if (!current || at === current.length) check(await nextPart(), "Catalog stream ends early");
    const byte = current?.[at++];
    check(byte !== undefined, "Missing catalog byte");
    return byte;
  }
  async function record(kind: CatalogRecordKind): Promise<unknown> {
    const prefix: number[] = [];
    async function uint(): Promise<number> {
      let value = 0,
        scale = 1;
      for (let i = 0; i < 8; i++) {
        const byte = await one();
        prefix.push(byte);
        value += (byte & 127) * scale;
        if (byte < 128) {
          check(
            Number.isSafeInteger(value) && (i === 0 || byte !== 0),
            "Noncanonical catalog record integer",
          );
          return value;
        }
        scale *= 128;
      }
      throw new StoreCorruptionError("Catalog record integer is too large");
    }
    const family = await uint(),
      version = await uint(),
      length = await uint();
    if (family !== kinds[kind] || version !== grammar.recordVersion)
      throw new UnsupportedCatalogError(`record ${String(family)} version ${String(version)}`);
    check(length + prefix.length <= CATALOG_RECORD_BYTES, "Catalog record exceeds its4MiB limit");
    const bytes = allocator.create(prefix.length + length);
    try {
      bytes.set(prefix);
      await copy(bytes, prefix.length, length);
      return decodeCatalogRecord(kind, bytes);
    } finally {
      allocator.release(bytes);
    }
  }
  try {
    const first = allocator.create(HEADER_BYTES);
    try {
      await copy(first, 0, HEADER_BYTES);
      readHeader(new ByteReader(first), grammar.streamMagic);
    } finally {
      allocator.release(first);
    }
    const meta = checkMetadata(await record("catalog"));
    const groups: Record<string, unknown[]> = {
      tables: [],
      sequences: [],
      views: [],
      types: [],
      fts: [],
    };
    for (const [i, [kind, key]] of sections.entries()) {
      const count = meta.sections[i]?.count;
      check(count !== undefined, "Missing catalog section");
      for (let j = 0; j < count; j++) {
        await pauseIfDue(work);
        groups[key]?.push(await record(kind));
      }
    }
    check(!current || at === current.length, "Catalog has trailing bytes");
    check(!(await nextPart()), "Catalog has trailing pages");
    return {
      version: meta.version,
      counters: meta.counters,
      tables: groups.tables as TableDef[],
      sequences: groups.sequences as SequenceDef[],
      views: groups.views as ViewDef[],
      types: groups.types as TypeDef[],
      fts: groups.fts as FtsDef[],
      wireMetadata: meta,
    };
  } finally {
    current = undefined;
    await iterator.return?.();
  }
}
