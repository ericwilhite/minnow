/** Exact integer address bounds; sketches and observed prefixes never certify a query. */
import type { ColumnChunk } from "../format/manifest.js";

export interface Int32Domain {
  readonly column: number;
  readonly low: number;
  readonly high: number;
  readonly rows: number;
}

export class Int32DomainCertificate {
  #known: boolean;
  #low = Infinity;
  #high = -Infinity;
  constructor(
    readonly column: number | undefined,
    readonly rows: number | undefined,
    complete: boolean,
  ) {
    this.#known =
      column !== undefined && complete && Number.isSafeInteger(rows) && (rows ?? -1) >= 0;
  }
  unknown(): void {
    this.#known = false;
  }
  stored(rowCount: number, chunk: Pick<ColumnChunk, "type" | "statistics"> | undefined): void {
    if (!this.#known) return;
    if (chunk?.type.kind !== "int32") return this.unknown();
    const { min, max, nullCount } = chunk.statistics;
    if (nullCount === rowCount) return;
    if (
      typeof min !== "number" ||
      typeof max !== "number" ||
      !Number.isInteger(min) ||
      !Number.isInteger(max) ||
      min < -2147483648 ||
      max > 2147483647 ||
      min > max
    )
      return this.unknown();
    this.#low = Math.min(this.#low, min);
    this.#high = Math.max(this.#high, max);
  }
  result(): Int32Domain | undefined {
    if (
      !this.#known ||
      this.column === undefined ||
      this.rows === undefined ||
      this.#low > this.#high
    )
      return undefined;
    // Deleted values can widen this range. They cannot make a visible key escape it.
    return { column: this.column, low: this.#low, high: this.#high, rows: this.rows };
  }
}
