/** PostgreSQL numeric_round/round_var: exact decimal rounding and PostgreSQL display scale. */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { MAX_SAFE } from "../../vector/int64.js";
import { bigintBytes } from "../../memory/governor.js";
import { NumericOutOfRangeError } from "../errors.js";
import { coefficientAt, scaleAt, specialAt } from "./numeric.js";
import {
  countIn,
  flatOf,
  isValidSlot,
  rowsIn,
  slotIndex,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

// NUMERIC_WEIGHT_MAX=32767, DEC_DIGITS=4; NUMERIC_DSCALE_MAX=16383 (PostgreSQL numeric.c).
const MAX_INTEGER_DIGITS = 131072;
const MAX_SCALE = 16383;
const MIN_ROUND_SCALE = -131073;
// One maximum-sized numeric operation's decimal/hex strings, coefficients, divisor and overlap.
const SCRATCH_BYTES = 2 * 1024 * 1024;
const SAFE_BIG = BigInt(MAX_SAFE);

function digitsOf(value: bigint): number {
  return value.toString().length - (value < 0n ? 1 : 0);
}

/** Exact safe-coefficient path: every retained integer operation stays inside the safe range. */
function safeRound(value: number, from: number, to: number): number | null {
  if (value === 0) return 0;
  if (to >= from) {
    if (to - from > 15) return null;
    const out = value * 10 ** (to - from);
    return Number.isSafeInteger(out) ? out : null;
  }
  const removed = from - to;
  if (removed > 16) return 0;
  const divisor = 10 ** removed;
  const whole = Math.trunc(value / divisor);
  const remainder = value - whole * divisor;
  const rounded = whole + (Math.abs(remainder) * 2 >= divisor ? (value < 0 ? -1 : 1) : 0);
  const out = to < 0 ? rounded * 10 ** -to : rounded;
  return Number.isSafeInteger(out) ? out + 0 : null;
}

class NumericRound implements Compiled {
  readonly #safe: Float64Array;
  readonly #wide: bigint[];
  readonly #wideSlots: Uint8Array;
  readonly #scales: Uint16Array;
  readonly #specials: Uint8Array;
  readonly #validity: Uint32Array;
  readonly #outputs: readonly FlatVector[];
  #retained = 0;
  constructor(
    readonly type: PhysicalType,
    readonly input: Compiled,
    readonly digits: Compiled,
    readonly arena: OutputArena,
  ) {
    const n = arena.capacity;
    // The wide array's references and small BigInts are fixed; larger coefficients charge
    // their size before computation and remain charged until the next evaluation or close.
    arena.take(SCRATCH_BYTES + n * (8 + 32 + 1 + 2 + 1));
    this.#safe = new Float64Array(n);
    this.#wide = new Array<bigint>(n).fill(0n);
    this.#wideSlots = new Uint8Array(n);
    this.#scales = new Uint16Array(n);
    this.#specials = new Uint8Array(n);
    this.#validity = arena.validity();
    this.#outputs = [
      { repr: "safe", values: this.#safe } as const,
      { repr: "wide", values: this.#wide } as const,
    ].map((coefficients): FlatVector => ({
      form: "flat",
      type,
      length: 0,
      validity: this.#validity,
      data: {
        layout: "decimal",
        scale: null,
        scales: this.#scales,
        coefficients,
        specials: this.#specials,
      },
    }));
  }
  evaluate(batch: Batch, selection: Selection | null): Vector {
    const input = this.input.evaluate(batch, selection),
      digits = this.digits.evaluate(batch, selection);
    const values = flatOf(input).data,
      scales = flatOf(digits).data;
    if (values.layout !== "decimal" || scales.layout !== "i32")
      throw new TypeError("ROUND needs numeric and int4");
    this.#wide.fill(0n);
    if (this.#retained > 0) this.arena.give(this.#retained);
    this.#retained = 0;
    this.#wideSlots.fill(0);
    this.#validity.fill(0);
    const active = rowsIn(selection),
      count = countIn(selection, batch.length);
    let anyWide = false;
    for (let k = 0; k < count; k++) {
      const row = active[k] ?? 0;
      this.#safe[row] = 0;
      this.#scales[row] = 0;
      this.#specials[row] = 0;
      if (!isValidSlot(input, row) || !isValidSlot(digits, row)) continue;
      this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
      const index = slotIndex(input, row),
        scaleIndex = slotIndex(digits, row);
      const special = specialAt(values, index);
      if (special !== 0) {
        this.#specials[row] = special;
        continue;
      }
      const from = scaleAt(values, index);
      if (from > MAX_SCALE) throw new NumericOutOfRangeError("value overflows numeric format");
      const to = Math.max(MIN_ROUND_SCALE, Math.min(MAX_SCALE, scales.values[scaleIndex] ?? 0));
      const display = Math.max(0, to);
      this.#scales[row] = display;
      if (values.coefficients.repr === "safe") {
        const result = safeRound(values.coefficients.values[index] ?? 0, from, to);
        if (result !== null) {
          this.#safe[row] = result;
          continue;
        }
      }
      const coefficient = coefficientAt(values, index);
      const precision = digitsOf(coefficient);
      if (precision - from > MAX_INTEGER_DIGITS)
        throw new NumericOutOfRangeError("value overflows numeric format");
      if (coefficient === 0n || from - to > precision) continue;
      const outDigits =
        to >= from ? precision + to - from : Math.max(1, precision - from + display + 1);
      // Four bits per decimal digit bounds the result before allocation; refund the slack
      // after computing its actual BigInt size. The scratch reservation covers intermediates.
      const bound = 16 + 8 * Math.ceil(outDigits / 16);
      this.arena.take(bound);
      let result = coefficient;
      if (to >= from) result *= 10n ** BigInt(to - from);
      else {
        const divisor = 10n ** BigInt(from - to);
        const whole = coefficient / divisor;
        const remainder = coefficient % divisor;
        result =
          whole +
          ((remainder < 0n ? -remainder : remainder) * 2n >= divisor
            ? coefficient < 0n
              ? -1n
              : 1n
            : 0n);
        if (to < 0 && result !== 0n) result *= 10n ** BigInt(-to);
      }
      if (digitsOf(result) - display > MAX_INTEGER_DIGITS) {
        this.arena.give(bound);
        throw new NumericOutOfRangeError("value overflows numeric format");
      }
      if (result >= -SAFE_BIG && result <= SAFE_BIG) {
        this.#safe[row] = Number(result);
        this.arena.give(bound);
      } else {
        const bytes = bigintBytes(result);
        this.arena.give(bound - bytes);
        this.#retained += bytes;
        this.#wide[row] = result;
        this.#wideSlots[row] = 1;
        anyWide = true;
      }
    }
    if (anyWide)
      for (let k = 0; k < count; k++) {
        const row = active[k] ?? 0;
        if (
          this.#wideSlots[row] === 0 &&
          ((this.#validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0
        )
          this.#wide[row] = BigInt(this.#safe[row] ?? 0);
      }
    const out = this.#outputs[anyWide ? 1 : 0];
    if (out === undefined) throw new TypeError("Missing ROUND output tier");
    out.length = batch.length;
    return out;
  }
}

export function compileNumericRound(
  type: PhysicalType,
  input: Compiled,
  digits: Compiled,
  arena: OutputArena,
): Compiled {
  if (type.kind !== "decimal" || input.type.kind !== "decimal" || digits.type.kind !== "int32")
    throw new TypeError("ROUND needs numeric and int4");
  return new NumericRound(type, input, digits, arena);
}
