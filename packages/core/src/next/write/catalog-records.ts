/** MCAT1 schema-addressed records. Field names never appear on wire; field ids are registered.
 * This module is the logical record layer; native page and portable-frame wrappers own IO.
 */
import {
  CATALOG_RECORD_KINDS,
  CATALOG_RECORD_SCHEMAS,
  CATALOG_NATIVE_GRAMMAR as grammar,
  DURABLE_CODECS,
} from "../durable-format.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { ByteReader, ByteWriter, utf8Length, type ByteSink } from "./bytes.js";
import type { BufferAllocator } from "./commit-buffers.js";

export type CatalogRecordKind = keyof typeof CATALOG_RECORD_SCHEMAS;
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const opaque = Symbol("MCAT optional display fields");
interface Extension {
  id: number;
  bytes: Uint8Array;
}
type RecordValue = Record<string, unknown> & { [opaque]?: Extension[] };
export class UnsupportedCatalogError extends Error {
  readonly code = "MINNOW_CATALOG_UNSUPPORTED";
  constructor(readonly feature: string) {
    super(`Unsupported required catalog feature: ${feature}`);
    this.name = "UnsupportedCatalogError";
  }
}
function check(ok: boolean, message: string): asserts ok {
  if (!ok) throw new StoreCorruptionError(message);
}
function uint(reader: ByteReader): number {
  const from = reader.offset,
    value = reader.varuint();
  check(
    reader.offset - from === 1 || (reader.bytes[reader.offset - 1] ?? 0) !== 0,
    "Noncanonical catalog integer",
  );
  return value;
}
function bytes(out: ByteWriter): Uint8Array {
  // Return the charged backing buffer without finish()'s uncharged copy.
  return out.length === out.bytes.length ? out.bytes : out.bytes.subarray(0, out.length);
}
function free(out: ByteWriter): void {
  out.close();
}
function shape(kind: CatalogRecordKind) {
  return CATALOG_RECORD_SCHEMAS[kind] as readonly string[];
}
function field(spec: string) {
  const [name, raw] = spec.split(":");
  check(name !== undefined && raw !== undefined, "Invalid catalog schema");
  return { name, spec: raw.endsWith("~") ? raw.slice(0, -1) : raw, optional: raw.endsWith("~") };
}
const typed = [
  [DURABLE_CODECS.catalogTypedArray.uint8, Uint8Array],
  [DURABLE_CODECS.catalogTypedArray.uint16, Uint16Array],
  [DURABLE_CODECS.catalogTypedArray.int32, Int32Array],
  [DURABLE_CODECS.catalogTypedArray.uint32, Uint32Array],
  [DURABLE_CODECS.catalogTypedArray.float32, Float32Array],
  [DURABLE_CODECS.catalogTypedArray.float64, Float64Array],
  [DURABLE_CODECS.catalogTypedArray.int64, BigInt64Array],
] as const;
const storage = DURABLE_CODECS.catalogStorage;
const little = new Uint8Array(Uint16Array.of(1).buffer)[0] === 1;
function putTyped(out: ByteSink, value: unknown) {
  const tag = typed.find(([, Type]) => value instanceof Type)?.[0];
  check(tag !== undefined && ArrayBuffer.isView(value), "Invalid catalog typed array");
  const array = value as Exclude<ArrayBufferView, DataView> & {
    length: number;
    BYTES_PER_ELEMENT: number;
  };
  out.varuint(tag);
  out.varuint(array.length);
  const raw = new Uint8Array(array.buffer, array.byteOffset, array.byteLength),
    width = array.BYTES_PER_ELEMENT;
  if (little || width === 1) out.raw(raw);
  else
    for (let p = 0; p < raw.length; p += width)
      for (let i = width - 1; i >= 0; i--) out.u8(raw[p + i] ?? 0);
}
function getTyped(input: ByteReader) {
  const tag = uint(input),
    Type = typed.find(([kind]) => kind === tag)?.[1];
  check(Type !== undefined, "Unknown catalog typed array");
  const length = uint(input),
    width = Type.BYTES_PER_ELEMENT;
  check(
    length <= Math.floor((input.bytes.length - input.offset) / width),
    "Truncated catalog typed array",
  );
  const raw = input.raw(length * width).slice();
  if (!little && width > 1)
    for (let p = 0; p < raw.length; p += width) raw.subarray(p, p + width).reverse();
  return new Type(raw.buffer);
}
function put(
  out: ByteSink,
  spec: string,
  value: unknown,
  allocator: BufferAllocator | undefined,
  depth: number,
): void {
  check(depth <= 32, "Catalog nesting exceeds its cap");
  if (spec.endsWith("?")) {
    out.u8(value === null ? 0 : 1);
    if (value !== null) put(out, spec.slice(0, -1), value, allocator, depth + 1);
    return;
  }
  if (spec.startsWith("[")) {
    check(Array.isArray(value), "Invalid catalog array");
    out.varuint(value.length);
    for (const item of value) put(out, spec.slice(1, -1), item, allocator, depth + 1);
    return;
  }
  switch (spec) {
    case "s":
      check(typeof value === "string", "Invalid catalog string");
      for (let i = 0; i < value.length; i++) {
        const unit = value.charCodeAt(i);
        if (unit >= 0xd800 && unit <= 0xdbff) {
          const tail = value.charCodeAt(++i);
          check(tail >= 0xdc00 && tail <= 0xdfff, "Invalid catalog UTF16");
        } else check(unit < 0xdc00 || unit > 0xdfff, "Invalid catalog UTF16");
      }
      out.string(value);
      return;
    case "u":
      check(
        typeof value === "number" && Number.isSafeInteger(value) && value >= 0,
        "Invalid catalog id/count",
      );
      out.varuint(value);
      return;
    case "n":
      check(typeof value === "number" && Number.isSafeInteger(value), "Invalid catalog integer");
      out.f64(value);
      return;
    case "b":
      check(typeof value === "boolean", "Invalid catalog boolean");
      out.u8(value ? 1 : 0);
      return;
    case "i":
      check(typeof value === "bigint", "Invalid catalog bigint");
      out.bigint(value);
      return;
    case "typed":
      putTyped(out, value);
      return;
    case "vectorValues":
      if (ArrayBuffer.isView(value)) {
        out.u8(storage.typed);
        putTyped(out, value);
      } else {
        out.u8(storage.expanded);
        put(out, "integerValues", value, allocator, depth + 1);
      }
      return;
    case "integerArray":
      if (Array.isArray(value)) {
        out.u8(storage.expanded);
        put(out, "[i]", value, allocator, depth + 1);
      } else {
        out.u8(storage.typed);
        putTyped(out, value);
      }
      return;
  }
  check(Object.hasOwn(CATALOG_RECORD_SCHEMAS, spec), "Unknown catalog schema");
  writeRecord(out, spec as CatalogRecordKind, value, allocator, depth + 1);
}
function get(input: ByteReader, spec: string, depth: number): unknown {
  check(depth <= 32, "Catalog nesting exceeds its cap");
  if (spec.endsWith("?")) {
    const flag = input.u8();
    check(flag <= 1, "Invalid catalog null flag");
    return flag === 0 ? null : get(input, spec.slice(0, -1), depth + 1);
  }
  if (spec.startsWith("[")) {
    const count = uint(input);
    check(count <= input.bytes.length - input.offset, "Catalog array count exceeds bytes");
    const result: unknown[] = [];
    for (let i = 0; i < count; i++) result.push(get(input, spec.slice(1, -1), depth + 1));
    return result;
  }
  switch (spec) {
    case "s":
      try {
        return decoder.decode(input.raw(uint(input)));
      } catch (error) {
        throw new StoreCorruptionError(`Invalid catalog UTF8: ${String(error)}`);
      }
    case "u":
      return uint(input);
    case "n": {
      const n = input.f64();
      check(Number.isSafeInteger(n), "Invalid catalog integer");
      return n;
    }
    case "b": {
      const b = input.u8();
      check(b <= 1, "Invalid catalog boolean");
      return b === 1;
    }
    case "i": {
      const s = get(input, "s", depth + 1);
      check(
        typeof s === "string" && /^(?:0|-[1-9]\d*|[1-9]\d*)$/u.test(s),
        "Noncanonical catalog bigint",
      );
      return BigInt(s);
    }
    case "typed":
      return getTyped(input);
    case "vectorValues":
    case "integerArray": {
      const tag = input.u8();
      check(tag === storage.typed || tag === storage.expanded, "Unknown catalog vector storage");
      return tag === storage.typed
        ? getTyped(input)
        : get(input, spec === "vectorValues" ? "integerValues" : "[i]", depth + 1);
    }
  }
  check(Object.hasOwn(CATALOG_RECORD_SCHEMAS, spec), "Unknown catalog schema");
  return readRecord(input, spec as CatalogRecordKind, depth + 1);
}
function width(value: number): number {
  check(Number.isSafeInteger(value) && value >= 0, "Invalid catalog size");
  let size = 1;
  while (value >= 128) {
    value = Math.floor(value / 128);
    size++;
  }
  return size;
}
function typedSize(value: unknown): number {
  const tag = typed.find(([, Type]) => value instanceof Type)?.[0];
  check(tag !== undefined && ArrayBuffer.isView(value), "Invalid catalog typed array");
  const array = value as ArrayBufferView & { length: number };
  return width(tag) + width(array.length) + array.byteLength;
}
function valueSize(spec: string, value: unknown, depth: number): number {
  check(depth <= 32, "Catalog nesting exceeds its cap");
  if (spec.endsWith("?"))
    return 1 + (value === null ? 0 : valueSize(spec.slice(0, -1), value, depth + 1));
  if (spec.startsWith("[")) {
    check(Array.isArray(value), "Invalid catalog array");
    return (
      width(value.length) +
      value.reduce((sum: number, v: unknown) => sum + valueSize(spec.slice(1, -1), v, depth + 1), 0)
    );
  }
  switch (spec) {
    case "s":
      check(typeof value === "string", "Invalid catalog string");
      return width(utf8Length(value)) + utf8Length(value);
    case "u":
      check(typeof value === "number", "Invalid catalog id/count");
      return width(value);
    case "n":
      check(typeof value === "number" && Number.isSafeInteger(value), "Invalid catalog integer");
      return 8;
    case "b":
      check(typeof value === "boolean", "Invalid catalog boolean");
      return 1;
    case "i": {
      check(typeof value === "bigint", "Invalid catalog bigint");
      const n = value.toString().length;
      return width(n) + n;
    }
    case "typed":
      return typedSize(value);
    case "vectorValues":
      return (
        1 +
        (ArrayBuffer.isView(value)
          ? typedSize(value)
          : recordSize("integerValues", value, depth + 1))
      );
    case "integerArray":
      return 1 + (Array.isArray(value) ? valueSize("[i]", value, depth + 1) : typedSize(value));
  }
  check(Object.hasOwn(CATALOG_RECORD_SCHEMAS, spec), "Unknown catalog schema");
  return recordSize(spec as CatalogRecordKind, value, depth + 1);
}
function entries(
  kind: CatalogRecordKind,
  value: unknown,
  visit: (id: number, flags: number, spec: string | null, value: unknown) => void,
): void {
  check(
    typeof value === "object" && value !== null && !Array.isArray(value),
    "Invalid catalog record",
  );
  const record = value as RecordValue,
    definitions = shape(kind);
  for (const key of Object.keys(record))
    check(
      definitions.some((s) => field(s).name === key),
      `Unknown writer catalog field ${key}`,
    );
  for (let id = 1; id <= definitions.length; id++) {
    const definition = definitions[id - 1];
    check(definition !== undefined, "Missing catalog schema");
    const f = field(definition),
      v = record[f.name];
    if (v === undefined) {
      check(f.optional, `Missing catalog field ${f.name}`);
      continue;
    }
    visit(id, f.optional ? 0 : DURABLE_CODECS.catalogFieldFlags.required, f.spec, v);
  }
  let last = definitions.length;
  for (const extension of record[opaque] ?? []) {
    check(extension.id > last, "Invalid optional catalog extension order");
    last = extension.id;
    visit(extension.id, 0, null, extension.bytes);
  }
}
function bodySize(kind: CatalogRecordKind, value: unknown, depth: number): number {
  check(depth <= 32, "Catalog nesting exceeds its cap");
  let size = 0;
  entries(kind, value, (id, _flags, spec, v) => {
    const n = spec === null ? (v as Uint8Array).length : valueSize(spec, v, depth + 1);
    size += width(id) + 1 + width(n) + n;
  });
  return size;
}
function recordSize(kind: CatalogRecordKind, value: unknown, depth: number): number {
  const n = bodySize(kind, value, depth);
  return width(CATALOG_RECORD_KINDS[kind]) + width(grammar.recordVersion) + width(n) + n;
}
function writeRecord(
  out: ByteSink,
  kind: CatalogRecordKind,
  value: unknown,
  allocator: BufferAllocator | undefined,
  depth: number,
): void {
  out.varuint(CATALOG_RECORD_KINDS[kind]);
  out.varuint(grammar.recordVersion);
  out.varuint(bodySize(kind, value, depth));
  entries(kind, value, (id, flags, spec, v) => {
    const n = spec === null ? (v as Uint8Array).length : valueSize(spec, v, depth + 1);
    out.varuint(id);
    out.u8(flags);
    out.varuint(n);
    if (spec === null) out.raw(v as Uint8Array);
    else put(out, spec, v, allocator, depth + 1);
  });
}
/** The shared writer reserves eight bytes per varuint; an exact-size owner reserves its actual width. */
class RecordWriter extends ByteWriter {
  override varuint(value: number): void {
    this.ensure(width(value));
    while (value >= 128) {
      this.bytes[this.length++] = (value % 128) | 128;
      value = Math.floor(value / 128);
    }
    this.bytes[this.length++] = value;
  }
}
/** Preflight sizes before allocating once; nested records write directly into that owner. */
export function encodeCatalogRecord(
  kind: CatalogRecordKind,
  value: unknown,
  allocator?: BufferAllocator,
  prefix?: Uint8Array,
): { bytes: Uint8Array; close(): void } {
  const depth = 0;
  const size = recordSize(kind, value, depth) + (prefix?.length ?? 0),
    out = new RecordWriter(size, allocator, size);
  try {
    if (prefix) out.raw(prefix);
    writeRecord(out, kind, value, allocator, depth);
    check(out.length === size, "Catalog measured/emitted size differs");
    return { bytes: bytes(out), close: () => free(out) };
  } catch (error) {
    free(out);
    throw error;
  }
}
function readRecord(input: ByteReader, kind: CatalogRecordKind, depth: number): RecordValue {
  const id = uint(input),
    version = uint(input);
  if (id !== CATALOG_RECORD_KINDS[kind] || version !== grammar.recordVersion)
    throw new UnsupportedCatalogError(`record ${String(id)} version ${String(version)}`);
  const body = new ByteReader(input.raw(uint(input))),
    definitions = shape(kind),
    result: RecordValue = {},
    extensions: Extension[] = [];
  let previous = 0;
  while (!body.done) {
    const id = uint(body),
      flags = body.u8();
    check(
      (flags & ~DURABLE_CODECS.catalogFieldFlags.knownMask) === 0,
      "Reserved catalog field flags",
    );
    const required = (flags & DURABLE_CODECS.catalogFieldFlags.required) !== 0;
    check(id > previous, "Duplicate/out-of-order catalog field");
    previous = id;
    const payload = new ByteReader(body.raw(uint(body))),
      definition = definitions[id - 1];
    if (definition === undefined) {
      if (required) throw new UnsupportedCatalogError(`field ${String(id)} in ${kind}`);
      extensions.push({ id, bytes: payload.bytes.slice() });
      continue;
    }
    const f = field(definition);
    check(required !== f.optional, "Invalid catalog field requirement flag");
    result[f.name] = get(payload, f.spec, depth + 1);
    payload.end();
  }
  for (const definition of definitions) {
    const f = field(definition);
    check(f.optional || Object.hasOwn(result, f.name), `Missing catalog field ${f.name}`);
  }
  if (extensions.length) result[opaque] = extensions;
  return result;
}
export function decodeCatalogRecord(kind: CatalogRecordKind, source: Uint8Array): unknown {
  const reader = new ByteReader(source),
    value = readRecord(reader, kind, 0);
  reader.end();
  return value;
}

/** Preserve only opaque display fields when rebuilding a known record by identity. */
export function retainCatalogExtensions(source: unknown, target: object): void {
  if (typeof source === "object" && source !== null && opaque in source) {
    const extensions = (source as RecordValue)[opaque];
    if (extensions !== undefined) (target as RecordValue)[opaque] = extensions;
  }
}
/** Consume one record from a larger logical stream, without copying its enclosing bytes. */
export function readCatalogRecord(kind: CatalogRecordKind, input: ByteReader): unknown {
  return readRecord(input, kind, 0);
}

/** A schema walk measures the wire before the caller admits its output allocation. */
export function catalogRecordSize(kind: CatalogRecordKind, value: unknown): number {
  return recordSize(kind, value, 0);
}

/** Emit into a pre-admitted window without materializing a second catalog buffer. */
export function writeCatalogRecord(out: ByteSink, kind: CatalogRecordKind, value: unknown): void {
  writeRecord(out, kind, value, undefined, 0);
}
