/**
 * The complete registry of next/ durable wire identities. This is a leaf: all storage/format/
 * page/write decoders may import it. No supported next writer has shipped yet. From the first
 * released 1.0 writer, incompatible changes require a registered ordinary-open/load upgrade.
 *
 * A codec without its own version inherits the named parent's version. Epoch slots select
 * their layout by exact magic. Root slots carry a pre-fence reader/manifest barrier. An unknown,
 * checksummed slot is not an empty/torn slot. Reserved masks enumerate every permitted bit;
 * writers emit zero for the remainder, readers refuse it. Scratch/transport are not durable.
 */
/** Shared reference-table envelope; the manifest also carries its logical format version2. */
export const METADATA_ENVELOPE_VERSION = 1;
/** Raise before publishing any incompatible nested durable codec. Never fence before checking. */
export const DURABLE_READER_VERSION = 1;
export const MANIFEST_FORMAT_VERSION = 2;
export const ROOT_SLOT_HEADER_BYTES = 16;
export const ROOT_SLOT_OVERHEAD = ROOT_SLOT_HEADER_BYTES + 4;

export class DurableFormatError extends Error {
  readonly code = "MINNOW_FORMAT_UNSUPPORTED";
  constructor(message: string) {
    super(message);
    this.name = "DurableFormatError";
  }
}
export type FormatErrorType = new (message: string) => Error & { readonly code: string };
export interface FormatField {
  readonly name: string;
  readonly offset: number;
  readonly width: 1 | 2 | 4;
  readonly values?: readonly number[];
  /** Permitted bits; every other bit is reserved-zero. */
  readonly mask?: number;
}
/** Header selectors can name an outer envelope (manifest envelope1) plus its logical version2. */
interface Format {
  readonly magic: number | null;
  readonly version: number;
  readonly parent?: string;
  readonly versionIn: "header" | "magic" | "parent" | "platform";
  readonly fields: readonly FormatField[];
  readonly decoder: string;
  readonly state: "implemented" | "design";
}
const magic = (value: number): FormatField => ({
  name: "magic",
  offset: 0,
  width: 4,
  values: [value],
});
const version = (offset: number, width: 1 | 2 | 4 = 1): FormatField => ({
  name: "version",
  offset,
  width,
  values: [1],
});
const zero = (offset: number, width: 1 | 2 | 4): FormatField => ({
  name: "reserved",
  offset,
  width,
  mask: 0,
});
const envelope = (value: number, decoder: string) =>
  ({
    magic: value,
    version: 1,
    versionIn: "header",
    fields: [magic(value), version(4)],
    decoder,
    state: "implemented",
  }) as const satisfies Format;
const inherited = (parent: string, decoder: string, fields: readonly FormatField[] = []) =>
  ({
    magic: null,
    version: 1,
    versionIn: "parent",
    parent,
    fields,
    decoder,
    state: "implemented",
  }) as const satisfies Format;

export const DURABLE_CODECS = {
  checksum: {
    blob: "xxHash32",
    blobSeed: 0,
    nativeSlot: "xxHash32",
    snapshot: "CRC32/ISO-HDLC",
    reflectedPolynomial: 0xedb88320,
  },
  page: {
    plain: 1,
    constant: 2,
    bitmap: 3,
    frameOfReference: 4,
    dictionary: 5,
    scaledFloat: 6,
    gzip: 128,
  },
  value: {
    bool: 1,
    i32: 2,
    i64: 3,
    f64: 4,
    f32: 5,
    varlen: 6,
    fixed16: 7,
    interval: 8,
    dec64: 9,
    dec128: 10,
  },
  pageFlag: {
    validity: 1,
    allNull: 2,
    delta: 4,
    specials: 8,
    safe: 16,
    scales: 32,
    uniformScale: 64,
    large: 128,
  },
  frame: { commit: 1 },
  snapshot: {
    catalogBegin: 1,
    catalogPart: 2,
    catalogEnd: 3,
    tableBegin: 4,
    columnBegin: 5,
    columnPart: 6,
    columnEnd: 7,
    tableEnd: 8,
    footer: 255,
  },
  effect: {
    createTable: 1,
    alterTable: 2,
    dropTable: 3,
    catalog: 4,
    sequence: 5,
    rows: 6,
    publishedRows: 7,
  },
  column: {
    bool: 1,
    i32: 2,
    f64: 3,
    f32: 4,
    int64: 5,
    decimal: 6,
    varlen: 7,
    fixed16: 8,
    interval: 9,
  },
  arrayElement: {
    bool: 1,
    int16: 2,
    int32: 3,
    int64: 4,
    float32: 5,
    float64: 6,
    decimal: 7,
    date: 8,
    time: 9,
    timestamp: 10,
    timestamptz: 11,
    interval: 12,
    text: 13,
    bytes: 14,
    uuid: 15,
    enum: 16,
  },
  columnLayout: ["bool", "i32", "f64", "f32", "int64", "decimal", "varlen", "fixed16", "interval"],
  sketchPrecision: 10,
  arrayDimensions: 6,
  decimalSpecialTag: { finite: 0, nan: 1, positiveInfinity: 2, negativeInfinity: 3 },
  key: { nullFirst: 0, value: 1, nullLast: 2, escape: 255, terminator: 1 },
  decimalKey: {
    negativeInfinity: 1,
    negative: 2,
    zero: 3,
    positive: 4,
    positiveInfinity: 5,
    nan: 6,
  },
  physical: [
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
  ],
  statistic: [0, 1, 2, 3, 4, 5],
  boolean: [0, 1],
  jsonBoolean: [false, true],
  presentFlag: [true],
  int32Bits: [16, 32],
  simpleSql: ["boolean", "number", "string", "datetime"],
  sqlDomain: ["numeric", "json", "jsonb", "uuid", "date", "time", "interval", "array", "enum"],
  decimalSpecial: [0, 1, 2, 3],
  integerRepr: [0, 1],
  decimalRepr: [0, 1, 2],
  rowIds: [0, 1],
  sketchForm: [0, 1],
  catalogOperation: [
    "createTable",
    "dropTable",
    "createIndex",
    "dropIndex",
    "putSequence",
    "dropSequence",
    "createView",
    "dropView",
    "createType",
    "dropType",
  ],
  jsonNumber: ["NaN", "Infinity", "-Infinity", "-0"],
  indexKind: ["ordered", "equality"],
  indexRole: ["primary", "unique", "secondary", "foreignKey"],
  foreignKeyAction: ["restrict", "cascade", "setNull"],
  vectorForm: ["flat"],
  vectorIntegerRepr: ["safe", "int64"],
  vectorDecimalRepr: ["safe", "int64", "int128", "wide"],
} as const;

export const DURABLE_FORMATS = {
  indexedDb: {
    magic: null,
    version: 2,
    versionIn: "platform",
    fields: [],
    decoder: "openIndexedDb",
    state: "implemented",
  },
  rootSlot: {
    magic: 0x54524e4d,
    version: 1,
    versionIn: "header",
    fields: [
      magic(0x54524e4d),
      // The u53 sequence leaves11 high bits: minimum-reader-minus1 in bits21..23,
      // manifest-format-minus2 in bits24..31. Initial writer bytes remain unchanged.
      {
        name: "minimum reader",
        offset: 10,
        width: 1,
        values: Array.from({ length: 32 * DURABLE_READER_VERSION }, (_, i) => i),
      },
      { name: "manifest format", offset: 11, width: 1, values: [MANIFEST_FORMAT_VERSION - 2] },
    ],
    decoder: "decodeRootSlot",
    state: "implemented",
  },
  epochSlot: {
    magic: 0x50454e4d,
    version: 1,
    versionIn: "magic",
    fields: [
      magic(0x50454e4d),
      { name: "epoch high reserved", offset: 8, width: 4, mask: 0x1fffff },
    ],
    decoder: "decodeEpochSlot",
    state: "implemented",
  },
  logHeader: {
    ...envelope(0x484c4e4d, "decodeLogHeader"),
    fields: [
      magic(0x484c4e4d),
      version(4, 4),
      ...[16, 24, 36, 44].map((offset) => ({
        name: "u53 high reserved",
        offset,
        width: 4 as const,
        mask: 0x1fffff,
      })),
    ],
  },
  logFrame: {
    ...inherited("logHeader", "scanLog/scanLogIncremental"),
    magic: 0x464c4e4d,
    fields: [
      magic(0x464c4e4d),
      { name: "kind", offset: 20, width: 1, values: [1] },
      zero(21, 1),
      zero(26, 2),
    ],
  },
  manifest: { ...envelope(0x31524d4d, "decodeManifestRoot"), version: MANIFEST_FORMAT_VERSION },
  groupRoot: envelope(0x3152474d, "decodeGroupRoot/tryDecodeGroupRootView"),
  groupPage: envelope(0x3150474d, "decodeGroupPage"),
  deletionBitmap: inherited("groupPage", "decodeDeletionBitmap"),
  groupIndex: envelope(0x3149474d, "decodeGroupIndex"),
  tableStatistics: envelope(0x3154534d, "decodeTableStatistics"),
  extentList: {
    ...envelope(0x314c584d, "decodeListPage"),
    fields: [magic(0x314c584d), version(4, 2), zero(6, 2), zero(12, 4)],
  },
  pageDirectory: {
    ...envelope(0x3144504d, "parseDirectory"),
    fields: [
      magic(0x3144504d),
      version(4),
      { name: "chunk tag", offset: 5, width: 1, values: [1, 2, 3, 4, 5, 6, 7, 8, 9] },
      { name: "flags", offset: 6, width: 1, mask: 7 },
      zero(7, 1),
    ],
  },
  page: inherited("pageDirectory", "parsePage/parseDictionary", [
    { name: "encoding", offset: 0, width: 1, values: [1, 2, 3, 4, 5, 6] },
    { name: "value tag", offset: 1, width: 1, values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
    { name: "width", offset: 3, width: 1, values: [0, 1, 2, 4, 8, 16] },
    zero(12, 4),
  ]),
  largeValue: {
    magic: null,
    version: 1,
    versionIn: "header",
    fields: [
      version(0),
      zero(1, 1),
      { name: "offset high reserved", offset: 12, width: 4, mask: 0x1fffff },
    ],
    decoder: "decodePointer",
    state: "implemented",
  },
  sketch: inherited("pageDirectory", "DistinctSketch.parse", [
    { name: "precision", offset: 0, width: 1, values: [10] },
    { name: "form", offset: 1, width: 1, values: [0, 1] },
  ]),
  commitBody: {
    magic: null,
    version: 1,
    versionIn: "header",
    fields: [version(0)],
    decoder: "decodeBody",
    state: "implemented",
  },
  column: inherited("commitBody", "decodeColumn", [
    { name: "layout", offset: 0, width: 1, values: [1, 2, 3, 4, 5, 6, 7, 8, 9] },
  ]),
  catalog: inherited("manifest", "Catalog.decode"),
  catalogChange: inherited("commitBody", "decodeLogged"),
  runBlock: inherited("manifest", "blockEntries/RunReader.lookup/cursor"),
  runIndex: inherited("manifest", "RunReader.open:index"),
  runFences: inherited("manifest", "RunReader.open:fences"),
  runBloomDirectory: inherited("manifest", "RunReader.open:bloomDirectory"),
  runBloom: inherited("manifest", "RunReader.open:bloom", [
    { name: "xxHash32 double-hash probes", offset: 4, width: 1, values: [7] },
  ]),
  arrayValue: {
    magic: null,
    version: 1,
    versionIn: "header",
    fields: [
      version(0),
      {
        name: "element",
        offset: 1,
        width: 1,
        values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
      },
      { name: "dimensions", offset: 2, width: 1, values: [0, 1, 2, 3, 4, 5, 6] },
      { name: "flags", offset: 3, width: 1, mask: 1 },
    ],
    decoder: "decodeArrayValue",
    state: "implemented",
  },
  // Reservation only. There is no production next portable snapshot decoder yet.
  snapshot: {
    magic: 0x4e534e4d,
    version: 1,
    versionIn: "header",
    fields: [
      magic(0x4e534e4d),
      version(4, 2),
      zero(6, 2),
      { name: "header bytes", offset: 8, width: 4, values: [64] },
      { name: "codec set", offset: 12, width: 4, values: [1] },
      { name: "LSN high reserved", offset: 36, width: 4, mask: 0x1fffff },
      ...[40, 44, 48, 52, 56].map((offset) => zero(offset, 4)),
    ],
    decoder: "none: design only",
    state: "design",
  },
  snapshotFrame: {
    magic: 0x46534e4d,
    version: 1,
    versionIn: "parent",
    parent: "snapshot",
    fields: [
      magic(0x46534e4d),
      { name: "kind", offset: 4, width: 1, values: [1, 2, 3, 4, 5, 6, 7, 8, 255] },
      zero(5, 1),
      zero(6, 2),
      { name: "ordinal high reserved", offset: 12, width: 4, mask: 0x1fffff },
      { name: "object high reserved", offset: 20, width: 4, mask: 0x1fffff },
      zero(32, 4),
    ],
    decoder: "none: design only",
    state: "design",
  },
} as const satisfies Readonly<Record<string, Format>>;
export type DurableFormatName = keyof typeof DURABLE_FORMATS;

/** Persisted hints/scratch are swept/rebuilt, never recovery authorities or supported formats. */
export const PRIVATE_FORMATS = {
  metadataJournal: 0x504d4e4d,
  simulatedIdbWal: 0x4c41574d,
  spillPage: 0x50534e4d,
  scratchRanges: 0x524e534d,
} as const;
/** Canonical run v1 flags, full-key Bloom recipe and dictionary gzip bit. */
export const RESERVED_MASKS = {
  directoryFlags: 7,
  pageStatisticsFlags: 7,
  arrayFlags: 1,
  pageFlags: 255,
  booleanBitmapTail: 0,
  validityBitmapTail: 0,
  deletionBitmapTail: 0,
  arrayNullBitmapTail: 0,
  alignmentPadding: 0,
  scaledFloatPatchReserved: 0,
  runEntry: 1,
} as const;

export const PAGE_CODEC = {
  deltaRows: 128,
  scaledFloatMaxExponent: 15,
  mixedScale: 0xffffffff,
  statPrefixBytes: 32,
} as const;
export const LARGE_CODEC = {
  inlineLimit: 64 * 1024,
  valueLimit: 64 * 1024 * 1024,
  partBytes: 1024 * 1024,
} as const;
export const RUN_CODEC = {
  blockBytes: 4096,
  partitionBlocks: 256,
  bitsPerKey: 10,
  version: 1,
  flags: 1,
  hashes: 7,
  seed1: 0,
  seed2: 0x9747b28c,
} as const;

export function requireTag<T extends number | string | boolean>(
  value: T,
  allowed: readonly T[],
  what: string,
  ErrorType: FormatErrorType = DurableFormatError,
): void {
  if (!allowed.includes(value)) throw new ErrorType(`Unknown ${what}: ${String(value)}`);
}
export function requireBits(
  value: number,
  mask: number,
  what: string,
  ErrorType: FormatErrorType = DurableFormatError,
): void {
  if ((value & ~mask) !== 0) throw new ErrorType(`Nonzero reserved bits in ${what}`);
}
/** Caller has already verified a checksummed slot before distinguishing it from a torn slot. */
export function requireHeader(
  format: DurableFormatName,
  bytes: Uint8Array,
  ErrorType: FormatErrorType = DurableFormatError,
): void {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (const field of DURABLE_FORMATS[format].fields as readonly FormatField[]) {
    if (field.offset + field.width > bytes.length)
      throw new ErrorType(`Truncated ${format} ${field.name}`);
    const value =
      field.width === 1
        ? view.getUint8(field.offset)
        : field.width === 2
          ? view.getUint16(field.offset, true)
          : view.getUint32(field.offset, true);
    if (field.values !== undefined)
      requireTag(value, field.values, `${format} ${field.name}`, ErrorType);
    if (field.mask !== undefined)
      requireBits(value, field.mask, `${format} ${field.name}`, ErrorType);
  }
}
function freeze(value: object): void {
  for (const child of Object.values(value) as unknown[])
    if (typeof child === "object" && child !== null) freeze(child);
  Object.freeze(value);
}
freeze(DURABLE_FORMATS);
freeze(DURABLE_CODECS);
freeze(PRIVATE_FORMATS);
Object.freeze(RUN_CODEC);
Object.freeze(RESERVED_MASKS);
Object.freeze(PAGE_CODEC);
Object.freeze(LARGE_CODEC);
