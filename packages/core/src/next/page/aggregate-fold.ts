/** Optional A1 producer state. Read the copied raw lanes, not pre-coercion input vectors.
 * Number lane sums stay exact: <=65536 u32 words sum below2^48. Only the signed top lane
 * has a sign; combine into a BigInt once per same-scale run. NULL payload is never read.
 * Mixed-scale arithmetic is funded before rescaling and bounded before constructing pow10.
 * Omission means the complete ordinary scan remains available; it never means SUM=0.
 */
import type { ByteBudget } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import { DURABLE_CODECS } from "../durable-format.js";
import {
  groupAggregateClass,
  groupAggregateTypeSupported,
  type GroupAggregate,
} from "../format/group-aggregate.js";

const STATE_BYTES = 1024;
// Includes old/new coefficient, power, products, addition and bounded result objects.
const WORK_BYTES = 128 * 1024;
const MAX_BITS = (DURABLE_CODECS.groupAggregate.payloadBytes - 12) * 8;
// floor(log10(2)*MAX_BITS); conservative support limit, never a correctness decision.
const MAX_SHIFT = Math.floor(MAX_BITS * Math.LOG10E * Math.LN2) - 1;
const MAX_ROWS = DURABLE_CODECS.groupAggregate.maxRows;
function validAt(validity: Uint32Array | null, row: number): boolean {
  return validity === null || (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) !== 0;
}

export class AggregateFold {
  #held = STATE_BYTES;
  #disabled = false;
  #rows = 0;
  #nonNull = 0;
  #finite = 0;
  #nan = 0;
  #positiveInfinity = 0;
  #negativeInfinity = 0;
  #scale = 0;
  #resultScale = 0;
  #coefficient = 0n;
  #laneScale = 0;
  #laneRows = 0;
  #low = 0;
  #second = 0;
  #third = 0;
  #top = 0;
  private constructor(
    readonly budget: ByteBudget,
    readonly kind: number,
  ) {}
  static create(type: PhysicalType, budget: ByteBudget): AggregateFold | null {
    const kind = groupAggregateClass(type);
    if (kind === undefined || !groupAggregateTypeSupported(type) || !budget.allocate(STATE_BYTES))
      return null;
    return new AggregateFold(budget, kind);
  }
  #omit(): void {
    this.#disabled = true;
    this.#coefficient = 0n;
    this.#laneRows = 0;
    this.#low = this.#second = this.#third = this.#top = 0;
    if (this.#held > STATE_BYTES) {
      this.budget.free(this.#held - STATE_BYTES);
      this.#held = STATE_BYTES;
    }
  }
  #fund(): boolean {
    if (this.#held > STATE_BYTES) return true;
    if (!this.budget.allocate(WORK_BYTES)) {
      this.#omit();
      return false;
    }
    this.#held += WORK_BYTES;
    return true;
  }
  #rowsAdded(rows: number): boolean {
    if (this.#disabled) return false;
    this.#rows += rows;
    if (!Number.isInteger(rows) || rows < 0 || this.#rows > MAX_ROWS) {
      this.#omit();
      return false;
    }
    return true;
  }
  #merge(value: bigint, scale: number): void {
    if (this.#disabled || value === 0n) return;
    if (this.#coefficient === 0n) {
      this.#coefficient = value;
      this.#scale = scale;
      return;
    }
    if (scale !== this.#scale) {
      const shift = Math.abs(scale - this.#scale);
      if (shift > MAX_SHIFT || !this.#fund()) {
        this.#omit();
        return;
      }
      const power = 10n ** BigInt(shift);
      if (scale > this.#scale) {
        value += this.#coefficient * power;
        this.#scale = scale;
      } else value = this.#coefficient + value * power;
    } else value += this.#coefficient;
    if (BigInt.asIntN(MAX_BITS, value) !== value) {
      this.#omit();
      return;
    }
    this.#coefficient = value;
  }
  #flushLanes(): void {
    if (this.#laneRows === 0 || this.#disabled) return;
    const coefficient =
      BigInt(this.#low) +
      (BigInt(this.#second) << 32n) +
      (BigInt(this.#third) << 64n) +
      (BigInt(this.#top) << 96n);
    this.#merge(coefficient, this.#laneScale);
    this.#laneRows = 0;
    this.#low = this.#second = this.#third = this.#top = 0;
  }
  #lane(low: number, second: number, third: number, top: number, scale: number): void {
    if (this.#laneRows !== 0 && scale !== this.#laneScale) this.#flushLanes();
    if (this.#disabled) return;
    this.#laneScale = scale;
    this.#laneRows++;
    this.#low += low >>> 0;
    this.#second += second >>> 0;
    this.#third += third >>> 0;
    this.#top += top | 0;
  }
  int32(values: Int32Array, validity: Uint32Array | null, rows: number, from = 0): void {
    if (!this.#rowsAdded(rows)) return;
    for (let row = from; row < from + rows; row++) {
      if (!validAt(validity, row)) continue;
      const value = values[row] ?? 0,
        top = value >> 31;
      // The low-level page writer also accepts physically i32 values under an int16 label.
      // Do not make such previously writable input fail because optional facts cannot prove
      // its logical domain; preserve the value bytes and omit the summary instead.
      if (
        this.kind === DURABLE_CODECS.groupAggregate.class.int16 &&
        (value < -32768 || value > 32767)
      ) {
        this.#omit();
        return;
      }
      this.#nonNull++;
      this.#finite++;
      this.#lane(value, top, top, top, 0);
    }
  }
  int64(words: Int32Array, validity: Uint32Array | null, rows: number, from = 0): void {
    if (!this.#rowsAdded(rows)) return;
    for (let row = from; row < from + rows; row++) {
      if (!validAt(validity, row)) continue;
      const high = words[row * 2 + 1] ?? 0,
        top = high >> 31;
      this.#nonNull++;
      this.#finite++;
      this.#lane(words[row * 2] ?? 0, high, top, top, 0);
    }
  }
  decimal(
    limbs: Uint32Array,
    specials: Uint8Array,
    scales: Uint16Array | null,
    fixedScale: number,
    validity: Uint32Array | null,
    rows: number,
    from = 0,
  ): void {
    if (!this.#rowsAdded(rows)) return;
    for (let row = from; row < from + rows; row++) {
      if (!validAt(validity, row)) continue;
      this.#nonNull++;
      const special = specials[row] ?? 0;
      if (special === 1) {
        this.#nan++;
        continue;
      }
      if (special === 2) {
        this.#positiveInfinity++;
        continue;
      }
      if (special === 3) {
        this.#negativeInfinity++;
        continue;
      }
      if (special !== 0) {
        this.#omit();
        return;
      }
      const scale = scales === null ? fixedScale : (scales[row] ?? 0);
      if (
        !Number.isInteger(scale) ||
        scale < 0 ||
        scale > DURABLE_CODECS.groupAggregate.maximumScale
      ) {
        this.#omit();
        return;
      }
      this.#finite++;
      this.#resultScale = Math.max(this.#resultScale, scale);
      this.#lane(
        limbs[row * 4] ?? 0,
        limbs[row * 4 + 1] ?? 0,
        limbs[row * 4 + 2] ?? 0,
        limbs[row * 4 + 3] ?? 0,
        scale,
      );
    }
  }
  /** Result coefficients stay under this object's charge through the final encode/append. */
  finish(): GroupAggregate | null {
    this.#flushLanes();
    if (this.#disabled || this.#held === 0) return null;
    return {
      nonNullCount: this.#nonNull,
      sum: {
        coefficient: this.#coefficient,
        coefficientScale: this.#coefficient === 0n ? 0 : this.#scale,
        resultScale: this.#finite === 0 ? 0 : this.#resultScale,
        nanCount: this.#nan,
        positiveInfinityCount: this.#positiveInfinity,
        negativeInfinityCount: this.#negativeInfinity,
      },
    };
  }
  /** Idempotent: mandatory writer growth may reclaim optional summary state at any point. */
  close(): void {
    if (this.#held === 0) return;
    this.#coefficient = 0n;
    this.#disabled = true;
    this.budget.free(this.#held);
    this.#held = 0;
  }
}
