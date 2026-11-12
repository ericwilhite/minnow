/** PostgreSQL float4/8 SUM and float8_accum transitions, including invisible AVG Sxx errors.
 * A sticky error lets prefetched prefixes defer 22003 until a frame actually consumes them.
 */
import { NumericOutOfRangeError } from "./errors.js";
export const WINDOW_FLOAT_STATE_BYTES = 128;
const infinite = (value: number): boolean => value === Infinity || value === -Infinity;
export class WindowFloat {
  sum: number;
  count = 0;
  squares = 0;
  bad = false;
  constructor(
    readonly average: boolean,
    readonly narrow = false,
  ) {
    this.sum = average ? 0 : -0;
  }
  reset(): void {
    this.sum = this.average ? 0 : -0;
    this.count = 0;
    this.squares = 0;
    this.bad = false;
  }
  add(value: number): void {
    const old = this.sum,
      before = this.count,
      count = before + 1;
    const sum = this.narrow ? Math.fround(old + value) : old + value;
    let squares = this.squares;
    if (this.average) {
      if (before > 0) {
        const delta = value * count - sum;
        squares += (delta * delta) / (count * before);
        if (infinite(sum) || infinite(squares)) {
          if (!infinite(old) && !infinite(value)) this.bad = true;
          squares = NaN;
        }
      } else if (!Number.isFinite(value)) squares = NaN;
    } else if (infinite(sum) && !infinite(old) && !infinite(value)) this.bad = true;
    this.sum = sum;
    this.count = count;
    this.squares = squares;
  }
  check(): void {
    if (this.bad) throw new NumericOutOfRangeError("value out of range: overflow");
  }
}
