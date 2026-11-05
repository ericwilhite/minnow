/**
 * Exact SUM over decimal vectors with mixed scales: the reference for scale-partitioned execution
 * (vector/vector.ts "Decimal scale").
 *
 * Values are never rescaled one by one. Each distinct scale keeps its own accumulator: an f64 sum
 * while it is provably exact (`sumStaysSafe`), widening to BigInt only when it is not. Alignment
 * to the largest scale happens once per partition, at the end. So one value with a 1,000-digit
 * fraction costs one extra partition, not a 1,000-digit coefficient for every value.
 *
 * Specials follow PostgreSQL: any NaN gives NaN; +Infinity and −Infinity together give NaN;
 * otherwise an infinity wins. The result's scale is the largest input scale (PostgreSQL's SUM
 * display scale).
 */
import { int128At } from "../vector/int128.js";
import { sumStaysSafe } from "../vector/int64.js";
import type { Selection, Validity, VectorData } from "../vector/vector.js";

type DecimalData = Extract<VectorData, { layout: "decimal" }>;

interface Partition {
  safe: number;
  wide: bigint;
}

export type DecimalSumResult =
  | { readonly kind: "finite"; readonly coefficient: bigint; readonly scale: number }
  | { readonly kind: "special"; readonly value: "NaN" | "Infinity" | "-Infinity" }
  | { readonly kind: "empty" };

export class DecimalSum {
  readonly #partitions = new Map<number, Partition>();
  #nan = false;
  #positive = false;
  #negative = false;
  #count = 0;
  /** Values that needed a BigInt (int64/int128/wide tiers, or an f64 accumulator that overflowed). */
  bigintValues = 0;
  /** Alignments to the result scale: one per partition, in `result`. */
  rescales = 0;

  get partitions(): number {
    return this.#partitions.size;
  }

  #partition(scale: number): Partition {
    let partition = this.#partitions.get(scale);
    if (partition === undefined) {
      partition = { safe: 0, wide: 0n };
      this.#partitions.set(scale, partition);
    }
    return partition;
  }

  add(
    data: DecimalData,
    length: number,
    validity: Validity | null,
    selection: Selection | null,
  ): void {
    const count = selection === null ? length : selection.count;
    const coefficients = data.coefficients;
    for (let k = 0; k < count; k++) {
      const row = selection === null ? k : (selection.rows[k] ?? 0);
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      const special = data.specials?.[row] ?? 0;
      if (special !== 0) {
        if (special === 1) this.#nan = true;
        else if (special === 2) this.#positive = true;
        else this.#negative = true;
        continue;
      }
      this.#count += 1;
      const partition = this.#partition(data.scale ?? data.scales?.[row] ?? 0);
      if (coefficients.repr === "safe") {
        const value = coefficients.values[row] ?? 0;
        if (sumStaysSafe(partition.safe, Math.abs(value), 1)) partition.safe += value;
        else {
          partition.wide += BigInt(partition.safe) + BigInt(value);
          partition.safe = 0;
          this.bigintValues += 1;
        }
        continue;
      }
      this.bigintValues += 1;
      partition.wide +=
        coefficients.repr === "int128"
          ? int128At(coefficients.limbs, row)
          : (coefficients.values[row] ?? 0n);
    }
  }

  result(): DecimalSumResult {
    if (this.#nan || (this.#positive && this.#negative)) return { kind: "special", value: "NaN" };
    if (this.#positive) return { kind: "special", value: "Infinity" };
    if (this.#negative) return { kind: "special", value: "-Infinity" };
    if (this.#count === 0) return { kind: "empty" };
    let scale = 0;
    for (const partitionScale of this.#partitions.keys()) scale = Math.max(scale, partitionScale);
    let coefficient = 0n;
    for (const [partitionScale, partition] of this.#partitions) {
      this.rescales += 1;
      coefficient +=
        (partition.wide + BigInt(partition.safe)) * 10n ** BigInt(scale - partitionScale);
    }
    return { kind: "finite", coefficient, scale };
  }
}
