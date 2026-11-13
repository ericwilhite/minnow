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
/** Shared reference-table envelope; the manifest also carries its logical format version3. */
export const METADATA_ENVELOPE_VERSION = 1;
/** Raise before publishing any incompatible nested durable codec. Never fence before checking. */
export const DURABLE_READER_VERSION = 2;
export const MANIFEST_FORMAT_VERSION = 3;
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
/** Header selectors can name an outer envelope (manifest envelope1) plus its logical version3. */
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
  catalogTypedArray: { uint8: 1, int32: 2, uint32: 3, float32: 4, float64: 5, int64: 6 },
  catalogStorage: { typed: 0, expanded: 1 },
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
  legacySnapshotPrefix: [0x4d, 0x49, 0x4e, 0x53, 0x4e, 0x41, 0x50, 0x31],
  snapshotValue: { null: 0, define: 1, reference: 2 },
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
    "invalidateIndex",
    "putSequence",
    "dropSequence",
    "createView",
    "dropView",
    "createType",
    "dropType",
  ],
  jsonNumber: ["NaN", "Infinity", "-Infinity", "-0"],
  indexKeyLimit: ["scalar", "tuple"],
  runBlockFlag: { extendedKey: 0x8000 },
  runEncoding: { inline: 1, overflow: 2 },
  keyDescriptor: { inline: 0, overflow: 1 },
  indexKind: ["ordered", "equality"],
  indexRole: ["primary", "unique", "secondary", "foreignKey"],
  foreignKeyAction: ["restrict", "cascade", "setNull"],
  vectorForm: ["flat"],
  vectorIntegerRepr: ["safe", "int64"],
  vectorDecimalRepr: ["safe", "int64", "int128", "wide"],
} as const;

/** MCAT1 schema positions are durable field ids (index+1); ~ means optional, ? nullable.
 * Record families20/21 are reserved for REQUIRED1.0 trigger/FTS implementation by B6/B7.
 * Do not reorder: append fields/families under the catalog reader capability gate. */
/** Lead-confirmed catalog1 allocation; selectors are isolated at stage1 integration. */
export const CATALOG_NATIVE_GRAMMAR = {
  version: 1,
  recordVersion: 1,
  expressionDialect: 1,
  rootMagic: 0x5452434d,
  pageMagic: 0x4750434d,
  streamMagic: 0x5441434d,
  changeMagic: 0x4748434d,
  pageBytes: 256 * 1024,
  fanOut: 64,
  maximumDepth: 8,
} as const;

export const CATALOG_RECORD_KINDS = {
  catalog: 1,
  counters: 2,
  table: 3,
  column: 4,
  physical: 5,
  declared: 6,
  domain: 7,
  length: 8,
  index: 9,
  foreignKey: 10,
  check: 11,
  sequence: 12,
  owned: 13,
  view: 14,
  namedType: 15,
  vector: 16,
  vectorData: 17,
  integerValues: 18,
  change: 19,
  trigger: 20,
  fts: 21,
  section: 22,
} as const;

export const CATALOG_RECORD_SCHEMAS = {
  catalog: ["version:u", "counters:counters", "requiredFeatures:[u]", "sections:[section]"],
  counters: [
    "nextTableId:u",
    "nextColumnId:u",
    "nextIndexId:u",
    "nextConstraintId:u",
    "nextSequenceId:u",
    "nextViewId:u",
    "nextTypeId:u",
  ],
  table: [
    "id:u",
    "name:s",
    "schemaVersion:u",
    "columns:[column]",
    "primaryKey:u?",
    "indexes:[index]",
    "foreignKeys:[foreignKey]",
    "checks:[check]",
  ],
  column: [
    "id:u",
    "name:s",
    "type:physical",
    "declared:declared",
    "nullable:b",
    "defaultSql:s?",
    "generatedSql:s?",
    "identitySequence:u?",
    "missing:vector?",
  ],
  physical: [
    "kind:s",
    "bits:u~",
    "precision:n?~",
    "scale:n?~",
    "zoned:b~",
    "maxLength:u~",
    "fixed:b~",
    "binary:b~",
    "name:s~",
    "labels:[s]~",
    "element:physical~",
    "tableId:u~",
  ],
  declared: [
    "type:s",
    "integer:b~",
    "sqlDomain:domain~",
    "withoutTimeZone:b~",
    "enumValues:[s]~",
    "declaredTypeName:s~",
    "declaredLength:length~",
  ],
  domain: ["kind:s", "precision:u~", "scale:n~", "element:s~", "name:s~", "values:[s]~"],
  length: ["length:u", "fixed:b"],
  index: [
    "id:u",
    "name:s",
    "tableId:u",
    "columnIds:[u]",
    "unique:b",
    "kind:s",
    "role:s",
    "invalid:b~",
    "keyLimit:s~",
  ],
  foreignKey: [
    "id:u",
    "name:s",
    "columnIds:[u]",
    "parentTableId:u",
    "parentIndexId:u",
    "supportIndexId:u?",
    "onDelete:s",
    "enforced:b",
  ],
  check: ["id:u", "name:s", "conditionSql:s", "columnIds:[u]"],
  sequence: [
    "id:u",
    "name:s",
    "increment:i",
    "min:i",
    "max:i",
    "start:i",
    "cycle:b",
    "ownedBy:owned?",
  ],
  owned: ["tableId:u", "columnId:u"],
  view: ["id:u", "name:s", "bodySql:s"],
  namedType: ["id:u", "name:s", "labels:[s]"],
  vector: ["form:s", "type:physical", "length:u", "validity:typed?", "data:vectorData"],
  vectorData: [
    "layout:s",
    "values:vectorValues~",
    "coefficients:integerValues~",
    "scale:n~",
    "scales:typed~",
    "specials:typed~",
    "offsets:typed~",
    "bytes:typed~",
    "months:typed~",
    "days:typed~",
    "micros:integerValues~",
  ],
  integerValues: ["repr:s", "values:integerArray"],
  change: [
    "op:s",
    "table:table~",
    "sequences:[sequence]~",
    "tableId:u~",
    "index:index~",
    "indexId:u~",
    "sequence:sequence~",
    "sequenceId:u~",
    "view:view~",
    "viewId:u~",
    "type:namedType~",
    "typeId:u~",
  ],
  section: ["kind:u", "count:u"],
} as const;

/** Conditional fields of a resolved WAL operation; all listed fields are required. */
export const CATALOG_CHANGE_FIELDS = {
  createTable: ["table", "sequences"],
  dropTable: ["tableId"],
  createIndex: ["index"],
  invalidateIndex: ["tableId", "indexId"],
  dropIndex: ["tableId", "indexId"],
  putSequence: ["sequence"],
  dropSequence: ["sequenceId"],
  createView: ["view"],
  dropView: ["viewId"],
  createType: ["type"],
  dropType: ["typeId"],
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
    fields: [{ ...version(0), values: [1] }],
    decoder: "decodeBody",
    state: "implemented",
  },
  column: inherited("commitBody", "decodeColumn", [
    { name: "layout", offset: 0, width: 1, values: [1, 2, 3, 4, 5, 6, 7, 8, 9] },
  ]),
  catalogRoot: {
    ...envelope(CATALOG_NATIVE_GRAMMAR.rootMagic, "decodeCatalogRoot"),
    fields: [magic(CATALOG_NATIVE_GRAMMAR.rootMagic), version(4), zero(5, 1), zero(6, 2)],
  },
  catalogPage: {
    ...envelope(CATALOG_NATIVE_GRAMMAR.pageMagic, "decodeCatalogPage"),
    fields: [magic(CATALOG_NATIVE_GRAMMAR.pageMagic), version(4), zero(5, 1), zero(6, 2)],
  },
  catalog: {
    ...envelope(CATALOG_NATIVE_GRAMMAR.streamMagic, "Catalog.decode/decodeCatalogParts"),
    fields: [
      magic(CATALOG_NATIVE_GRAMMAR.streamMagic),
      version(4, 2),
      zero(6, 2),
      { name: "header bytes", offset: 8, width: 4, values: [16] },
      {
        name: "expression dialect",
        offset: 12,
        width: 4,
        values: [CATALOG_NATIVE_GRAMMAR.expressionDialect],
      },
    ],
  },
  catalogChange: {
    ...envelope(CATALOG_NATIVE_GRAMMAR.changeMagic, "decodeLogged"),
    fields: [
      magic(CATALOG_NATIVE_GRAMMAR.changeMagic),
      version(4, 2),
      zero(6, 2),
      { name: "header bytes", offset: 8, width: 4, values: [16] },
      {
        name: "expression dialect",
        offset: 12,
        width: 4,
        values: [CATALOG_NATIVE_GRAMMAR.expressionDialect],
      },
    ],
  },
  runKeyPointer: inherited("manifest", "readKeyPointer", [
    { name: "kind", offset: 0, width: 1, values: [0, 1] },
  ]),
  runKeyDescriptor: inherited("runKeyDirectory", "readRunKey", [
    { name: "kind", offset: 0, width: 1, values: [0, 1] },
  ]),
  runKeyDirectory: envelope(0x31444b4d, "OverflowKeys.resolve"),
  // Raw bytes inherit the checked directory and full-key length; checksums are BlobRef checks.
  runKeyPayload: inherited("runKeyDirectory", "OverflowKeys.resolve:payload"),
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
  // Portable snapshot1 is independent of native storage layouts.
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
    decoder: "decodeSnapshotHeader",
    state: "implemented",
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
    decoder: "decodeSnapshotFrame",
    state: "implemented",
  },
  snapshotColumn: {
    magic: 0x43504e4d,
    version: 1,
    parent: "snapshot",
    versionIn: "header",
    fields: [
      magic(0x43504e4d),
      version(4),
      { name: "layout", offset: 5, width: 1, values: [1, 2, 3, 4, 5, 6, 7, 8, 9] },
      zero(6, 2),
      zero(12, 4),
    ],
    decoder: "decodeSnapshotColumnHeader",
    state: "implemented",
  },
  snapshotFooter: inherited("snapshot", "decodeSnapshotFooter", [
    zero(28, 4),
    zero(76, 4),
    ...[20, 36, 44, 52, 60, 68].map((offset) => ({
      name: "u53 high reserved",
      offset,
      width: 4 as const,
      mask: 0x1fffff,
    })),
  ]),
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
  extendedKeyBlock: DURABLE_CODECS.runBlockFlag.extendedKey,
  keyPrefixBytes: 32,
  keyChunkBytes: 64 * 1024,
  inlineKeyBytes: 2048,
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
