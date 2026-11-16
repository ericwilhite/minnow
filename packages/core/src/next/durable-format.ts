/**
 * The complete registry of next/ durable wire identities. This is a leaf: all storage/format/
 * page/write decoders may import it. No supported next writer has shipped yet. From the first
 * released 1.0 writer, incompatible changes require a registered ordinary-open/load upgrade.
 *
 * A codec without its own version inherits the named parent's version. Epoch slots select
 * their layout by exact magic. Root slots carry a pre-fence reader/writer/manifest barrier. An unknown,
 * checksummed slot is not an empty/torn slot. Reserved masks enumerate every permitted bit;
 * writers emit zero for the remainder, readers refuse it. Scratch/transport are not durable.
 *
 * PC2: compatibility belongs to the outer gates, not lazy nested decoders. A writer adding a
 * nested tag/encoding must raise minimum reader, and minimum writer if an older writer cannot
 * fully rewrite it, durably BEFORE feature bytes enter WAL/data. Under supported gates an
 * unknown nested tag is corruption: refuse before consuming it or publishing derived state.
 * Open validates everything it eagerly reads; it never scans all data pages just to open.
 * The native vocabulary/profile guard below catches an unassigned vocabulary extension.
 * The independent native writer ledger also guards read-optional fields before acquisition.
 */
/** Shared reference-table envelope; the manifest also carries its logical format version11. */
export const METADATA_ENVELOPE_VERSION = 1;
/** Root reader capability; incompatible nested bytes advance this or the exact manifest barrier. */
export const DURABLE_READER_VERSION = 8;
export const MANIFEST_FORMAT_VERSION = 11;
export const ROOT_SLOT_HEADER_BYTES = 16;
/** Profile8 escape: two independently checked u32 capability selectors precede the manifest. */
export const DURABLE_WRITER_VERSION = 8;
export const ROOT_SLOT_CAPABILITY_BYTES = 8;
export const ROOT_SLOT_EXTENSION_TAG = 7;
export const ROOT_SLOT_PAYLOAD_OFFSET = ROOT_SLOT_HEADER_BYTES + ROOT_SLOT_CAPABILITY_BYTES;
export const ROOT_SLOT_OVERHEAD = ROOT_SLOT_PAYLOAD_OFFSET + 4;

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
/** Header selectors can name an outer envelope (manifest envelope1) plus its logical version11. */
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
  ftsNode: { leaf: 0, branch: 1 },
  ftsState: { building: 0, ready: 1, invalid: 2 },
  catalogFieldFlags: { required: 1, knownMask: 1 },
  catalogDefaultSource: ["expression", "boolean", "number", "string", "datetime"],
  catalogTypedArray: { uint8: 1, int32: 2, uint32: 3, float32: 4, float64: 5, int64: 6, uint16: 7 },
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
    fts: 8,
    ftsMutation: 9,
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
    "replaceView",
    "dropView",
    "createType",
    "dropType",
    "createFts",
    "upsertFts",
    "dropFts",
    "alterTable",
    "createTrigger",
    "dropTrigger",
  ],
  jsonNumber: ["NaN", "Infinity", "-Infinity", "-0"],
  indexKeyLimit: ["scalar", "tuple"],
  runBlockFlag: { extendedKey: 0x8000 },
  runEncoding: { inline: 1, overflow: 2 },
  keyDescriptor: { inline: 0, overflow: 1 },
  indexKind: ["ordered", "equality"],
  indexRole: ["primary", "unique", "secondary", "foreignKey"],
  foreignKeyAction: ["restrict", "noAction", "cascade", "setNull", "setDefault"],
  foreignKeyUpdateAction: ["restrict", "noAction", "cascade", "setNull", "setDefault", "follow"],
  vectorForm: ["flat"],
  vectorIntegerRepr: ["safe", "int64"],
  vectorDecimalRepr: ["safe", "int64", "int128", "wide"],
} as const;

/** MCAT1 schema positions are durable field ids (index+1); ~ means absent is legal, ? nullable.
 * All known fields are semantic: ! requires understanding even when presence is conditional.
 * Family21/config26 implement portable FTS definitions; family20 remains trigger-owned.
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
  maximumEnumLabels: 65536,
  maximumEnumLabelBytes: 63,
} as const;

/** Physical page limits; long dictionaries/lists use trees, never a total-document limit. */
export const FTS_NATIVE_GRAMMAR = {
  pageBytes: 256 * 1024,
  fanOut: 64,
  maximumDepth: 8,
  leafRows: 4096,
  termEntries: 1024,
  deltaEntries: 64,
  termUnits: 32,
  ownerUnits: 128,
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
  expressionDependencies: 23,
  dependency: 24,
  viewOutput: 25,
  ftsConfig: 26,
  ftsCoverage: 27,
  ftsState: 28,
  viewSource: 29,
  triggerBody: 30,
  triggerBinding: 31,
  triggerColumn: 32,
} as const;

export const CATALOG_FTS_FEATURE = 1;
export const CATALOG_TRIGGER_FEATURE = 2;
/** Append-only semantic capabilities; IDs and their nonempty section must agree. */
export const CATALOG_REQUIRED_FEATURES = [
  { id: CATALOG_FTS_FEATURE, section: CATALOG_RECORD_KINDS.fts },
  { id: CATALOG_TRIGGER_FEATURE, section: CATALOG_RECORD_KINDS.trigger },
] as const;
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
    "nextFtsId:u",
    "nextTriggerId:u",
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
    "managed:b~!",
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
    "typeId:u~!",
    "allowedValues:[s]~!",
    "defaultDependencies:expressionDependencies~!",
    "generatedDependencies:expressionDependencies~!",
    "autoIncrement:b~!",
    "identityAlways:b~!",
    "defaultSource:s~!",
    "backfillNegativeZero:b~!",
  ],
  physical: [
    "kind:s",
    "bits:u~!",
    "precision:n?~!",
    "scale:n?~!",
    "zoned:b~!",
    "maxLength:u~!",
    "fixed:b~!",
    "binary:b~!",
    "name:s~!",
    "labels:[s]~!",
    "element:physical~!",
    "tableId:u~!",
  ],
  declared: [
    "type:s",
    "integer:b~!",
    "sqlDomain:domain~!",
    "withoutTimeZone:b~!",
    "enumValues:[s]~!",
    "declaredTypeName:s~!",
    "declaredLength:length~!",
  ],
  domain: ["kind:s", "precision:u~!", "scale:n?~!", "element:s~!", "name:s~!", "values:[s]~!"],
  length: ["length:u", "fixed:b"],
  index: [
    "id:u",
    "name:s",
    "tableId:u",
    "columnIds:[u]",
    "unique:b",
    "kind:s",
    "role:s",
    "invalid:b~!",
    "keyLimit:s~!",
    "directions:[s]~!",
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
    "onUpdate:s",
    "parentName:s~!",
    "parentColumnNames:[s]~!",
  ],
  check: [
    "id:u",
    "name:s",
    "conditionSql:s",
    "columnIds:[u]",
    "dependencies:expressionDependencies~!",
  ],
  expressionDependencies: ["dialectVersion:u", "columnIds:[u]", "sequenceIds:[u]", "typeIds:[u]~!"],
  dependency: ["kind:s", "objectId:u", "columnIds:[u]"],
  viewOutput: ["name:s", "type:physical", "declared:declared", "nullable:b"],
  viewSource: ["kind:s", "objectId:u", "columnIds:[u]", "outputCount:u"],
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
  view: [
    "id:u",
    "name:s",
    "bodySql:s",
    "managed:b~!",
    "outputs:[viewOutput]~!",
    "dependencies:[dependency]~!",
    "sourceSchemas:[viewSource]~!",
  ],
  namedType: ["id:u", "name:s", "labels:[s]"],
  vector: ["form:s", "type:physical", "length:u", "validity:typed?", "data:vectorData"],
  vectorData: [
    "layout:s",
    "values:vectorValues~!",
    "coefficients:integerValues~!",
    "scale:n?~!",
    "scales:typed?~!",
    "specials:typed?~!",
    "offsets:typed~!",
    "bytes:typed~!",
    "months:typed~!",
    "days:typed~!",
    "micros:integerValues~!",
  ],
  integerValues: ["repr:s", "values:integerArray~!", "limbs:typed~!"],
  change: [
    "op:s",
    "table:table~!",
    "sequences:[sequence]~!",
    "tableId:u~!",
    "index:index~!",
    "indexId:u~!",
    "sequence:sequence~!",
    "sequenceId:u~!",
    "view:view~!",
    "viewId:u~!",
    "type:namedType~!",
    "typeId:u~!",
    "expectedVersion:u",
    "version:u",
    "fts:fts~!",
    "ftsId:u~!",
    "trigger:trigger~!",
    "triggerId:u~!",
  ],
  trigger: [
    "id:u",
    "name:s",
    "tableId:u",
    "event:s",
    "timing:s",
    "dialectVersion:u",
    "statements:[triggerBody]",
  ],
  triggerBody: [
    "sql:s",
    "kind:s",
    "targetTableId:u",
    "targetName:s",
    "bindings:[triggerBinding]",
    "columns:[triggerColumn]",
    "insertColumnIds:[u]",
    "defaultColumnIds:[u]",
    "implicitArity:u?",
    "assignedColumnIds:[u]",
    "typeIds:[u]",
    "sequenceIds:[u]",
  ],
  triggerBinding: ["columnId:u", "name:s", "type:physical", "source:s"],
  triggerColumn: ["columnId:u", "name:s", "type:physical"],
  fts: [
    "id:u",
    "tableId:u",
    "columnId:u",
    "tokenizerVersion:u",
    "rendererVersion:u",
    "scorerVersion:u",
    "config:ftsConfig",
  ],
  ftsConfig: ["version:u"],
  section: ["kind:u", "count:u"],
} as const;

/** Reserved semantic fields are unsupported even when their wire flag says optional.
 * Never preserve these as opaque display extensions: their ids already have future semantics.
 * FTS implementation removes each reservation only while installing its checked live schema.
 */
export const CATALOG_RESERVED_FIELDS = {
  fts: [
    { id: 8, name: "future semantics 8", type: "u" },
    { id: 9, name: "future semantics 9", type: "u" },
    { id: 10, name: "future semantics 10", type: "u" },
    { id: 11, name: "future semantics 11", type: "u" },
    { id: 12, name: "future semantics 12", type: "u" },
    { id: 13, name: "future semantics 13", type: "u" },
    { id: 14, name: "future semantics 14", type: "u" },
    { id: 15, name: "future semantics 15", type: "u" },
  ],
} as const;
/** Coverage and build state are native, not portable catalog definitions. */
export const CATALOG_RESERVED_RECORD_SCHEMAS = {
  ftsCoverage: [],
  ftsState: [],
} as const;

/** Conditional fields of a resolved WAL operation; all listed fields are required. */
export const CATALOG_CHANGE_FIELDS = {
  createTable: ["table", "sequences"],
  alterTable: ["table", "sequences"],
  dropTable: ["tableId"],
  createIndex: ["index"],
  invalidateIndex: ["tableId", "indexId"],
  dropIndex: ["tableId", "indexId"],
  putSequence: ["sequence"],
  dropSequence: ["sequenceId"],
  createView: ["view"],
  replaceView: ["view"],
  dropView: ["viewId"],
  createType: ["type"],
  dropType: ["typeId"],
  createFts: ["fts"],
  upsertFts: ["fts"],
  dropFts: ["ftsId"],
  createTrigger: ["trigger"],
  dropTrigger: ["triggerId"],
} as const;

export const DURABLE_FORMATS = {
  ftsJournal: {
    ...envelope(0x4a54464d, "readFtsJournal"), // MFTJ
    fields: [
      magic(0x4a54464d),
      version(4),
      zero(5, 1),
      { name: "node kind", offset: 6, width: 1, values: [0, 1] },
      zero(7, 1),
    ],
  },
  ftsRoot: {
    ...envelope(0x5254464d, "decodeFts"), // MFTR
    fields: [
      magic(0x5254464d),
      version(4),
      zero(5, 1),
      { name: "node kind", offset: 6, width: 1, values: [0, 1] },
      zero(7, 1),
    ],
  },
  ftsTerms: {
    ...envelope(0x5454464d, "decodeFts"), // MFTT
    fields: [
      magic(0x5454464d),
      version(4),
      zero(5, 1),
      { name: "node kind", offset: 6, width: 1, values: [0, 1] },
      zero(7, 1),
    ],
  },
  ftsPostings: {
    ...envelope(0x5054464d, "decodeFts"), // MFTP
    fields: [
      magic(0x5054464d),
      version(4),
      zero(5, 1),
      { name: "node kind", offset: 6, width: 1, values: [0, 1] },
      zero(7, 1),
    ],
  },
  ftsDocuments: {
    ...envelope(0x4454464d, "decodeFts"), // MFTD
    fields: [
      magic(0x4454464d),
      version(4),
      zero(5, 1),
      { name: "node kind", offset: 6, width: 1, values: [0, 1] },
      zero(7, 1),
    ],
  },
  ftsDelta: {
    ...envelope(0x4c54464d, "decodeFts"), // MFTL
    fields: [
      magic(0x4c54464d),
      version(4),
      zero(5, 1),
      { name: "node kind", offset: 6, width: 1, values: [0, 1] },
      zero(7, 1),
    ],
  },
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
    version: 2,
    versionIn: "header",
    fields: [
      magic(0x54524e4d),
      // Preserve the recognizable16-byte checksummed frame. Old reader7 refuses escape8.
      {
        name: "capability envelope",
        offset: 10,
        width: 1,
        values: Array.from({ length: 32 }, (_, i) => (ROOT_SLOT_EXTENSION_TAG << 5) + i),
      },
      { name: "manifest format", offset: 11, width: 1, values: [MANIFEST_FORMAT_VERSION - 2] },
      // No earlier profile belongs to manifest11; this first baseline has no released predecessor.
      { name: "minimum reader", offset: 16, width: 4, values: [DURABLE_READER_VERSION] },
      { name: "minimum writer", offset: 20, width: 4, values: [DURABLE_WRITER_VERSION] },
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
  sketch: inherited("pageDirectory", "DistinctSketch.parse/validateSketch", [
    { name: "precision", offset: 0, width: 1, values: [10] },
    { name: "form", offset: 1, width: 1, values: [0, 1] },
  ]),
  commitBody: {
    magic: null,
    version: 8,
    versionIn: "header",
    fields: [{ ...version(0), values: [8] }],
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
  snapshotCatalog: {
    magic: 0x54414350,
    version: 1,
    versionIn: "header",
    fields: [magic(0x54414350), version(4, 2), zero(6, 2)],
    decoder: "write/snapshot-catalog.ts:decodeSnapshotCatalog",
    state: "implemented",
  },
  snapshotSequence: inherited(
    "snapshotCatalog",
    "write/snapshot-catalog.ts:decodeSnapshotSequence",
    [zero(12, 4)],
  ),
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

/** Conditional reserved fields cannot be unconditional header masks: aux is assigned by
 * varlen/dictionary/scaled-float/uniform-scale layouts; decimal alone owns directory bit2. */
export const CONDITIONAL_FORMAT = {
  unusedPageAux: 0,
  nondecimalDirectoryFlags: 3,
  sketchMaximumRank: 33 - DURABLE_CODECS.sketchPrecision,
  sparseSketchOrder: "strictly-increasing",
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
Object.freeze(CONDITIONAL_FORMAT);
Object.freeze(PAGE_CODEC);
Object.freeze(LARGE_CODEC);

/** An immutable build-time compatibility ledger, not another stored field. Portable snapshot
 * selectors have their own container/codecSet and do not change the native root profile.
 * When extending the native vocabulary, add a NEW reader profile: never rewrite an entry
 * belonging to a released writer. The independent generated test pins the known ledger too.
 * Profile7 is the initial UNRELEASED binding, aligned to main13c28d1b manifest10/body8;
 * older development bindings are not supported predecessors (AGENTS.md).
 */
export const NATIVE_READER_PROFILES: Readonly<
  Record<
    number,
    {
      readonly manifest: number;
      readonly vocabulary: number;
    }
  >
> = Object.freeze({
  7: Object.freeze({ manifest: 10, vocabulary: 3713904595 }),
  8: Object.freeze({ manifest: 11, vocabulary: 1164132746 }),
});

/** Required rewrite capability includes even fields that are optional for reading.
 * New fields/features must assign a new writer profile before any native owner can start. */
export const NATIVE_WRITER_PROFILES: typeof NATIVE_READER_PROFILES = Object.freeze({
  8: Object.freeze({ manifest: 11, vocabulary: 1164132746 }),
});

function canonicalVocabulary(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalVocabulary).join(",")}]`;
  if (typeof value === "object" && value !== null)
    return `{${Object.entries(value)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([key, child]) => `${JSON.stringify(key)}:${canonicalVocabulary(child)}`)
      .join(",")}}`;
  return value === undefined ? "null" : JSON.stringify(value);
}

/** FNV-1a is a source-vocabulary guard, NOT an on-disk checksum or an integrity boundary.
 * Required catalog field IDs/codecs participate. Read-optional fields need only a writer gate;
 * their source property names are not durable tags. B6 owns their capability classification. */
export function nativeVocabularyFingerprint(
  schemas: Readonly<Record<string, readonly string[]>> = CATALOG_RECORD_SCHEMAS,
  features: ReadonlyArray<{
    readonly id: number;
    readonly section: number;
  }> = CATALOG_REQUIRED_FEATURES,
  mode: "read" | "write" = "read",
): number {
  const formats = Object.fromEntries(
    Object.entries(DURABLE_FORMATS)
      .filter(([name]) => name !== "indexedDb" && !name.startsWith("snapshot"))
      .map(([name, format]) => [
        name,
        {
          magic: format.magic,
          version: format.version,
          versionIn: format.versionIn,
          parent: "parent" in format ? format.parent : null,
          fields:
            name === "rootSlot"
              ? format.fields.map((field) =>
                  field.name === "minimum reader" || field.name === "minimum writer"
                    ? { name: field.name, offset: field.offset, width: field.width }
                    : field,
                )
              : format.fields,
        },
      ]),
  );
  const codecs = Object.fromEntries(
    Object.entries(DURABLE_CODECS).filter(
      ([name]) => !["snapshot", "snapshotValue", "legacySnapshotPrefix", "checksum"].includes(name),
    ),
  );
  const text = canonicalVocabulary({
    envelope: METADATA_ENVELOPE_VERSION,
    formats,
    codecs,
    catalog: CATALOG_NATIVE_GRAMMAR,
    records: CATALOG_RECORD_KINDS,
    features,
    readerFields: Object.fromEntries(
      Object.entries(schemas).map(([name, fields]) => [
        name,
        fields.flatMap((field, index) =>
          mode === "read" && field.includes("~") && !field.endsWith("!")
            ? []
            : [{ id: index + 1, codec: field.slice(field.indexOf(":") + 1) }],
        ),
      ]),
    ),
    fts: FTS_NATIVE_GRAMMAR,
    reserved: RESERVED_MASKS,
    conditional: CONDITIONAL_FORMAT,
    page: PAGE_CODEC,
    large: LARGE_CODEC,
    run: RUN_CODEC,
    checksum: {
      blob: DURABLE_CODECS.checksum.blob,
      seed: DURABLE_CODECS.checksum.blobSeed,
      slot: DURABLE_CODECS.checksum.nativeSlot,
    },
  });
  let hash = 0x811c9dc5;
  for (let at = 0; at < text.length; at++) hash = Math.imul(hash ^ text.charCodeAt(at), 0x01000193);
  return hash >>> 0;
}

/** Called once at module initialization, before any native storage acquisition. No per-query,
 * page or row work. Parameters also let the generated law model an unassigned extension. */
export function requireNativeReaderProfile(
  reader: number,
  manifest: number,
  vocabulary: number,
): void {
  const profile = NATIVE_READER_PROFILES[reader];
  if (profile?.manifest !== manifest || profile.vocabulary !== vocabulary)
    throw new DurableFormatError(
      `Native vocabulary ${String(vocabulary)} requires an assigned outer reader profile (reader ${String(reader)}, manifest ${String(manifest)})`,
    );
}
requireNativeReaderProfile(
  DURABLE_READER_VERSION,
  MANIFEST_FORMAT_VERSION,
  nativeVocabularyFingerprint(),
);

export function requireNativeWriterProfile(
  writer: number,
  manifest: number,
  vocabulary: number,
): void {
  const profile = NATIVE_WRITER_PROFILES[writer];
  if (profile?.manifest !== manifest || profile.vocabulary !== vocabulary)
    throw new DurableFormatError(
      `Native rewrite vocabulary ${String(vocabulary)} requires an assigned outer writer profile (writer ${String(writer)}, manifest ${String(manifest)})`,
    );
}
requireNativeWriterProfile(
  DURABLE_WRITER_VERSION,
  MANIFEST_FORMAT_VERSION,
  nativeVocabularyFingerprint(CATALOG_RECORD_SCHEMAS, CATALOG_REQUIRED_FEATURES, "write"),
);
