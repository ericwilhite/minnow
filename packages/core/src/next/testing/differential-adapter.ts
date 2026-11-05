/**
 * The new core as an `EngineUnderTest` (testing/differential-harness.ts), so every track can run
 * differential workloads against the current engine as soon as it has something to run.
 *
 * A track supplies a `NextCore`: whatever subset of the engine it has (a scan over a memory
 * table, the write path with a stub executor…). This adapter converts the core's columnar
 * results into the harness's row form.
 *
 * Values are converted to the **oracle's** representation, not the new public one: the current
 * engine is the reference, and the harness compares exact public values. Where the new core can
 * hold a value the oracle cannot (a timestamp with sub-millisecond digits, an int64 past 2^53),
 * the converted value differs and the comparison fails loudly; that is a known divergence to
 * record, never one to paper over here. Types the adapter does not convert yet throw
 * `UnsupportedOracleValueError` (a harness defect, so `replay` reports it as an error outcome
 * that cannot match the oracle).
 */
import type { CreateTableInput, DatabaseRow } from "../../engine/database.js";
import type { QueryParameter } from "../../engine/query.js";
import type { SqlDomain } from "../../storage/types.js";
import type {
  EngineFactory,
  EngineResult,
  EngineUnderTest,
  StoreKind,
} from "../../testing/differential-harness.js";
import type { OutputColumn } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import {
  PG_EPOCH_UNIX_MICROS,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  formatDate,
} from "../temporal.js";
import { int128At } from "../vector/int128.js";
import { int64At } from "../vector/int64.js";
import { isValid } from "../vector/validity.js";
import { decodeUtf8 } from "../vector/varlen.js";
import type { Batch, FlatVector, Vector } from "../vector/vector.js";

/** A columnar result: output columns and batches (the core may reuse nothing across them). */
export interface NextRows {
  readonly columns: readonly OutputColumn[];
  readonly batches: readonly Batch[];
}

export type NextStatementResult =
  | ({ readonly kind: "rows" } & NextRows)
  | {
      readonly kind: string;
      readonly rowCount?: number;
      readonly returning?: NextRows;
    };

/** What a track implements to join the differential harness. */
export interface NextCore {
  createTable(schema: CreateTableInput): Promise<void>;
  execute(sql: string, params: readonly QueryParameter[]): Promise<NextStatementResult>;
  writeBatch(
    kind: "insertBatch" | "upsertBatch",
    table: string,
    rows: readonly DatabaseRow[],
  ): Promise<Readonly<Record<string, number>>>;
  deleteBatch(
    table: string,
    keys: ReadonlyArray<string | number>,
  ): Promise<Readonly<Record<string, number>>>;
  /** Closes and reopens the same durable store (recovery path). */
  reopen(): Promise<void>;
  close(): Promise<void>;
}

export type NextCoreFactory = (store: StoreKind) => Promise<NextCore>;

export class UnsupportedOracleValueError extends Error {
  readonly code = "NEXT_ADAPTER_UNSUPPORTED";
  constructor(type: PhysicalType) {
    super(`The differential adapter cannot convert ${type.kind} values yet`);
    this.name = "UnsupportedOracleValueError";
  }
}

/** The current engine's `SqlDomain | null` for a physical type (result `domains`). */
export function oracleDomain(type: PhysicalType): SqlDomain | null {
  switch (type.kind) {
    case "decimal":
      return {
        kind: "numeric",
        ...(type.precision === null ? {} : { precision: type.precision }),
        ...(type.scale === null ? {} : { scale: type.scale }),
      };
    case "json":
      return { kind: type.binary ? "jsonb" : "json" };
    case "uuid":
    case "date":
    case "time":
    case "interval":
      return { kind: type.kind };
    case "enum":
      return { kind: "enum", name: type.name, values: [...type.labels] };
    default:
      return null;
  }
}

function decimalText(coefficient: bigint, scale: number, displayScale: number): string {
  const negative = coefficient < 0n;
  let magnitude = negative ? -coefficient : coefficient;
  if (displayScale < scale) magnitude /= 10n ** BigInt(scale - displayScale);
  const digits = magnitude.toString().padStart(displayScale + 1, "0");
  const split = digits.length - displayScale;
  const text = displayScale === 0 ? digits : `${digits.slice(0, split)}.${digits.slice(split)}`;
  return negative && magnitude !== 0n ? `-${text}` : text;
}

function flatValue(vector: FlatVector, row: number): unknown {
  if (!isValid(vector.validity, row)) return null;
  const data = vector.data;
  switch (data.layout) {
    case "bool":
      return data.values[row] === 1;
    case "i32":
      return vector.type.kind === "date" ? formatDate(data.values[row] ?? 0) : data.values[row];
    case "f64":
      if (vector.type.kind === "locator") throw new UnsupportedOracleValueError(vector.type);
      return data.values[row];
    case "f32":
      return data.values[row];
    case "int64": {
      if (vector.type.kind === "timestamp") {
        // The oracle returns Date (ms); D1's ISO text replaces it at cut-over.
        const micros = int64At(data.values, row);
        if (micros === TIMESTAMP_POS_INFINITY || micros === TIMESTAMP_NEG_INFINITY)
          return { infiniteTimestamp: String(micros > 0n) };
        const unix = micros + PG_EPOCH_UNIX_MICROS;
        if (unix % 1000n !== 0n) return { subMillisecondTimestamp: unix.toString() };
        return new Date(Number(unix / 1000n));
      }
      if (vector.type.kind !== "int64") throw new UnsupportedOracleValueError(vector.type);
      // The oracle returns safe numbers (D2's public bigint arrives at cut-over); a value past
      // 2^53 stays a bigint here, so it fails against the oracle loudly.
      const value = int64At(data.values, row);
      return value >= -BigInt(Number.MAX_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(value)
        : value;
    }
    case "decimal": {
      const special = data.specials?.[row] ?? 0;
      if (special !== 0) return special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity";
      const coefficients = data.coefficients;
      const coefficient =
        coefficients.repr === "wide"
          ? (coefficients.values[row] ?? 0n)
          : coefficients.repr === "int128"
            ? int128At(coefficients.limbs, row)
            : int64At(coefficients, row);
      return decimalText(coefficient, data.scale, data.displayScales?.[row] ?? data.scale);
    }
    case "varlen":
      if (vector.type.kind === "text" || vector.type.kind === "enum" || vector.type.kind === "json")
        return decodeUtf8(data, row);
      throw new UnsupportedOracleValueError(vector.type);
    case "fixed16": {
      let hex = "";
      for (let k = row * 16; k < row * 16 + 16; k++)
        hex += (data.bytes[k] ?? 0).toString(16).padStart(2, "0");
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    }
    case "interval":
      throw new UnsupportedOracleValueError(vector.type);
  }
}

/** One slot of any vector form, in the oracle's public representation. */
export function oracleValue(vector: Vector, row: number): unknown {
  switch (vector.form) {
    case "flat":
      return flatValue(vector, row);
    case "constant":
      return flatValue(vector.value, 0);
    case "dictionary":
      return isValid(vector.validity, row)
        ? flatValue(vector.dictionary, vector.codes[row] ?? 0)
        : null;
  }
}

/** Columnar rows → the harness's row objects. */
export function oracleRows(result: NextRows): Array<Record<string, unknown>> {
  const rows: Array<Record<string, unknown>> = [];
  for (const batch of result.batches) {
    const count = batch.selection === null ? batch.length : batch.selection.count;
    for (let index = 0; index < count; index++) {
      const row = batch.selection === null ? index : (batch.selection.rows[index] ?? 0);
      const record: Record<string, unknown> = {};
      for (const [column, output] of result.columns.entries()) {
        const vector = batch.columns[column];
        if (vector === undefined) throw new TypeError(`Batch has no column ${String(column)}`);
        record[output.name] = oracleValue(vector, row);
      }
      rows.push(record);
    }
  }
  return rows;
}

function rowsResult(kind: string, result: NextRows): EngineResult {
  return {
    kind,
    columns: result.columns.map((column) => column.name),
    rows: oracleRows(result),
    domains: result.columns.map((column) => oracleDomain(column.type)),
  };
}

/** Wraps a track's core factory as a harness `EngineFactory`. */
export function nextEngine(factory: NextCoreFactory): EngineFactory {
  return async (store): Promise<EngineUnderTest> => {
    const core = await factory(store);
    return {
      async run(operation): Promise<EngineResult> {
        switch (operation.kind) {
          case "reopen":
            await core.reopen();
            return { kind: "reopen" };
          case "schema":
            await core.createTable(operation.schema);
            return { kind: "schema" };
          case "insertBatch":
          case "upsertBatch":
            return {
              kind: operation.kind,
              counts: await core.writeBatch(operation.kind, operation.table, operation.rows),
            };
          case "deleteBatch":
            return {
              kind: operation.kind,
              counts: await core.deleteBatch(operation.table, operation.keys),
            };
          case "sql": {
            const result = await core.execute(operation.sql, operation.params ?? []);
            if (result.kind === "rows" && "batches" in result) return rowsResult("rows", result);
            const returning = "returning" in result ? result.returning : undefined;
            const rowCount = "rowCount" in result ? result.rowCount : undefined;
            return {
              kind: result.kind,
              ...(rowCount === undefined ? {} : { counts: { rowCount } }),
              ...(returning === undefined
                ? {}
                : {
                    columns: returning.columns.map((column) => column.name),
                    rows: oracleRows(returning),
                    domains: returning.columns.map((column) => oracleDomain(column.type)),
                  }),
            };
          }
        }
      },
      close: () => core.close(),
    };
  };
}
