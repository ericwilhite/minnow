/** Bounded, precharged ASCII output for PostgreSQL temporal casts to unbounded TEXT. */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { int64At } from "../../vector/int64.js";
import {
  DATE_MIN,
  DATE_MAX,
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  formatDate,
} from "../../temporal.js";
import { formatSqlTimestamp, formatSqlInterval, isInfiniteInterval } from "../../temporal-text.js";
import { DatetimeOverflowError, UnsupportedPlanError } from "../errors.js";
import {
  countIn,
  flatOf,
  isValidSlot,
  rowsIn,
  slotIndex,
  type Compiled,
  type OutputArena,
} from "./vectors.js";
class TemporalText implements Compiled {
  readonly #out: FlatVector;
  readonly #validity: Uint32Array;
  readonly #active: Uint8Array;
  constructor(
    readonly type: PhysicalType,
    readonly input: Compiled,
    arena: OutputArena,
  ) {
    // DATE/TIMESTAMP text fits 48 ASCII bytes; all finite INTERVAL fields fit 128. Strings,
    // calendar tuples and at most 65-bit BigInt intermediates use bounded per-row scratch.
    const width = input.type.kind === "interval" ? 128 : 48;
    const out = arena.flat(type, arena.capacity * width);
    this.#out = out.vector;
    this.#validity = out.validity;
    arena.take(arena.capacity + 4096);
    this.#active = new Uint8Array(arena.capacity);
  }
  evaluate(batch: Batch, selection: Selection | null): Vector {
    const input = this.input.evaluate(batch, selection),
      data = flatOf(input).data;
    const output = this.#out.data;
    if (output.layout !== "varlen") throw new TypeError("Expected text output");
    this.#active.fill(0);
    const rows = rowsIn(selection),
      count = countIn(selection, batch.length);
    for (let k = 0; k < count; k++) this.#active[rows[k] ?? 0] = 1;
    this.#validity.fill(0);
    output.offsets[0] = 0;
    let at = 0;
    // Offsets stay monotone even for a sparse or nonascending selection; inactive garbage is
    // never read. The existing output is reused on every evaluation.
    for (let row = 0; row < batch.length; row++) {
      if (this.#active[row] !== 0 && isValidSlot(input, row)) {
        const slot = slotIndex(input, row);
        let text: string;
        if (data.layout === "i32" && this.input.type.kind === "date") {
          const days = data.values[slot] ?? 0;
          if (
            days !== DATE_NEG_INFINITY &&
            days !== DATE_POS_INFINITY &&
            (days < DATE_MIN || days > DATE_MAX)
          )
            throw new DatetimeOverflowError("date out of range");
          text = formatDate(days);
        } else if (data.layout === "int64" && this.input.type.kind === "timestamp") {
          const micros = int64At(data.values, slot);
          if (
            micros !== TIMESTAMP_NEG_INFINITY &&
            micros !== TIMESTAMP_POS_INFINITY &&
            (micros < TIMESTAMP_MIN || micros >= TIMESTAMP_END)
          )
            throw new DatetimeOverflowError("timestamp out of range");
          text = formatSqlTimestamp(micros, this.input.type.zoned);
        } else if (data.layout === "interval") {
          const months = data.months[slot] ?? 0,
            days = data.days[slot] ?? 0,
            micros = int64At(data.micros, slot);
          if (isInfiniteInterval(months, days, micros))
            throw new UnsupportedPlanError("infinite INTERVAL text", { construct: "cast" });
          text = formatSqlInterval(months, days, micros);
        } else throw new TypeError("Expected a temporal cast input");
        if (at + text.length > output.bytes.length)
          throw new RangeError("Temporal text exceeded its fixed output bound");
        for (let i = 0; i < text.length; i++) output.bytes[at++] = text.charCodeAt(i);
        this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
      }
      output.offsets[row + 1] = at;
    }
    this.#out.length = batch.length;
    this.#out.validity = this.#validity;
    return this.#out;
  }
}
export function compileTemporalText(
  type: PhysicalType,
  input: Compiled,
  arena: OutputArena,
): Compiled {
  if (
    type.kind !== "text" ||
    type.maxLength !== undefined ||
    !(
      input.type.kind === "date" ||
      input.type.kind === "timestamp" ||
      input.type.kind === "interval"
    )
  )
    throw new UnsupportedPlanError("bounded or unsupported temporal text cast", {
      construct: "cast",
    });
  return new TemporalText(type, input, arena);
}
