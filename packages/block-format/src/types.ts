export const logicalTypes = ["boolean", "number", "string", "datetime"] as const;

export type LogicalType = (typeof logicalTypes)[number];
export type Compression = "raw" | "rle" | "gzip";

export interface ColumnValues {
  boolean: boolean;
  number: number;
  string: string;
  datetime: Date;
}

type ColumnInputByType = {
  [Type in LogicalType]: {
    type: Type;
    values: ReadonlyArray<ColumnValues[Type] | null>;
  };
};

type DecodedColumnByType = {
  [Type in LogicalType]: {
    type: Type;
    values: Array<ColumnValues[Type] | null>;
  };
};

export type ColumnInput<T extends LogicalType = LogicalType> = ColumnInputByType[T];
export type DecodedColumn<T extends LogicalType = LogicalType> = DecodedColumnByType[T];

export interface ZoneMap {
  min: number;
  max: number;
}

export interface BlockMetadata {
  zoneMap?: ZoneMap;
}

export interface BlockDescription {
  formatVersion: number;
  type: LogicalType;
  compression: Compression;
  rowCount: number;
  nullCount: number;
  encodedLength: number;
  storedLength: number;
  checksum: number;
  metadata: BlockMetadata;
}

export interface DecodedBlock<T extends LogicalType = LogicalType> {
  description: BlockDescription & { type: T };
  column: DecodedColumn<T>;
}

/**
 * An uncompressed physical column payload. `bytes` uses the version-zero
 * column encoding documented in `docs/BLOCK_FORMAT.md`.
 */
export interface PhysicalColumnPayload<T extends LogicalType = LogicalType> {
  type: T;
  rowCount: number;
  bytes: Uint8Array;
}

/** A physical payload whose structure and values have been checked. */
export interface ValidatedPhysicalColumn<
  T extends LogicalType = LogicalType,
> extends PhysicalColumnPayload<T> {
  nullCount: number;
  metadata: BlockMetadata;
}

/** A half-open row range to append to a new physical column. */
export interface PhysicalColumnRange<T extends LogicalType = LogicalType> {
  column: PhysicalColumnPayload<T>;
  start: number;
  end: number;
}

/**
 * Exact output sizing and canonical metadata for a set of physical row
 * ranges. Measuring does not allocate the output byte buffer.
 */
export interface PhysicalColumnMeasurement<T extends LogicalType = LogicalType> {
  type: T;
  rowCount: number;
  nullCount: number;
  /** Bytes occupied by the leading validity bitmap. */
  validityByteLength: number;
  /** All bytes after the validity bitmap, including string offsets and content. */
  valueByteLength: number;
  /** UTF-8 content bytes for strings; zero for every fixed-width type. */
  stringContentByteLength: number;
  /** The exact output allocation: validity plus value bytes. */
  encodedByteLength: number;
  metadata: BlockMetadata;
}

export interface DecodedPhysicalBlock<T extends LogicalType = LogicalType> {
  description: BlockDescription & { type: T };
  column: ValidatedPhysicalColumn<T>;
}
