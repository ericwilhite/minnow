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
