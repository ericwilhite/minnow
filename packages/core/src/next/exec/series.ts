/** Streaming PostgreSQL GENERATE_SERIES, with fixed charged output buffers and shared task clock. */
import type { PhysicalType } from "../types.js";
import { INT64_MIN, INT64_MAX, samePhysicalType } from "../types.js";
import type { Batch, FlatVector, VectorData } from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import { TIMESTAMP_NEG_INFINITY, TIMESTAMP_POS_INFINITY } from "../temporal.js";
import { isInfiniteInterval } from "../temporal-text.js";
import { ExecutionError, UnsupportedPlanError } from "./errors.js";
import { addCalendar } from "./kernels/calendar-add.js";
import { copyFixedSlot, slotOf, isValidSlot, type OutputArena } from "./kernels/vectors.js";
import { Slice } from "./slices.js";
import {
  afterYield,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
export interface SeriesOptions {
  readonly signal?: AbortSignal;
  readonly stopped?: () => boolean;
  readonly rowsNeeded?: () => number;
}
function integer(data: VectorData, slot: number): bigint {
  if (data.layout === "i32") return BigInt(data.values[slot] ?? 0);
  if (data.layout === "int64") return int64At(data.values, slot);
  throw new TypeError("Expected integer/timestamp series value");
}
function vector(type: PhysicalType, arena: OutputArena): FlatVector {
  let data: VectorData;
  const capacity = arena.capacity;
  if (type.kind === "int32") {
    arena.take(128 + capacity * 4);
    data = { layout: "i32", values: new Int32Array(capacity) };
  } else if (type.kind === "int64" || type.kind === "timestamp") {
    arena.take(128 + capacity * 8);
    data = { layout: "int64", values: { repr: "int64", values: new BigInt64Array(capacity) } };
  } else if (type.kind === "interval") {
    arena.take(128 + capacity * 16);
    data = {
      layout: "interval",
      months: new Int32Array(capacity),
      days: new Int32Array(capacity),
      micros: { repr: "int64", values: new BigInt64Array(capacity) },
    };
  } else
    throw new UnsupportedPlanError("non-integer/timestamp series arguments", {
      construct: "series",
    });
  return { form: "flat", type, length: 0, validity: null, data };
}
export class SeriesOperator implements Operator {
  readonly #columns: readonly FlatVector[];
  readonly #capacity: number;
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  #pending: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  constructor(
    readonly kind: "integer" | "timestamp",
    types: readonly PhysicalType[],
    arena: OutputArena,
    readonly downstream: Operator,
    readonly options: SeriesOptions = {},
  ) {
    const [first, last, step] = types;
    if (
      types.length !== 3 ||
      first === undefined ||
      last === undefined ||
      step === undefined ||
      !samePhysicalType(first, last) ||
      (kind === "integer"
        ? (first.kind !== "int64" && (first.kind !== "int32" || first.bits !== 32)) ||
          !samePhysicalType(first, step)
        : first.kind !== "timestamp" || step.kind !== "interval")
    )
      throw new UnsupportedPlanError("non-integer/timestamp series signature", {
        construct: "series",
      });
    this.#capacity = arena.capacity;
    arena.take(4096);
    this.#columns = types.map((type) => vector(type, arena));
  }
  #check(): void {
    this.options.signal?.throwIfAborted();
    if (this.#aborted) throw this.#reason;
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#pending = Promise.resolve()
      .then(() => this.#run(batch))
      .finally(() => transfer?.release());
    return "blocked";
  }
  async #emit(rows: number): Promise<void> {
    for (const column of this.#columns) column.length = rows;
    if (
      this.downstream.push({ length: rows, columns: this.#columns, selection: null }) === "blocked"
    )
      await this.downstream.ready();
    this.#check();
  }
  async #run(batch: Batch): Promise<void> {
    const [av, bv, sv] = batch.columns;
    if (av === undefined || bv === undefined || sv === undefined)
      throw new TypeError("Missing series argument");
    const output = this.#columns[0];
    if (output === undefined) throw new TypeError("Missing series output");
    const count = batch.selection?.count ?? batch.length;
    for (let n = 0; n < count; n++) {
      this.#check();
      if (this.options.stopped?.()) return;
      const row = batch.selection?.rows[n] ?? n;
      // PostgreSQL's strict SRF does not inspect the zero step of a NULL-argument call.
      if (!isValidSlot(av, row) || !isValidSlot(bv, row) || !isValidSlot(sv, row)) continue;
      const [a, ai] = slotOf(av, row),
        [b, bi] = slotOf(bv, row),
        [s, si] = slotOf(sv, row);
      let current = integer(a.data, ai);
      const stop = integer(b.data, bi);
      let direction: bigint,
        step = 0n,
        months = 0,
        days = 0,
        micros = 0n;
      if (this.kind === "integer") {
        step = integer(s.data, si);
        direction = step;
      } else {
        if (s.data.layout !== "interval") throw new TypeError("Timestamp series needs an interval");
        months = s.data.months[si] ?? 0;
        days = s.data.days[si] ?? 0;
        micros = int64At(s.data.micros, si);
        if (
          current === TIMESTAMP_NEG_INFINITY ||
          current === TIMESTAMP_POS_INFINITY ||
          stop === TIMESTAMP_NEG_INFINITY ||
          stop === TIMESTAMP_POS_INFINITY ||
          isInfiniteInterval(months, days, micros)
        )
          throw new ExecutionError(
            "infinite GENERATE_SERIES argument",
            "MINNOW_SERIES_ARGUMENT",
            "22023",
          );
        direction = (BigInt(months) * 30n + BigInt(days)) * 86400000000n + micros;
      }
      if (direction === 0n)
        throw new ExecutionError("step size cannot equal zero", "MINNOW_SERIES_STEP", "22023");
      const type = av.type,
        min = type.kind === "int32" ? (type.bits === 16 ? -32768n : -2147483648n) : INT64_MIN,
        max = type.kind === "int32" ? (type.bits === 16 ? 32767n : 2147483647n) : INT64_MAX;
      let done = false,
        used = 0;
      while (!done && (direction > 0n ? current <= stop : current >= stop)) {
        this.#check();
        if (this.options.stopped?.()) return;
        const od = output.data;
        if (od.layout === "i32") od.values[used] = Number(current);
        else if (od.layout === "int64" && od.values.repr === "int64")
          od.values.values[used] = current;
        else throw new TypeError("Unexpected series output");
        for (let index = 1; index < 3; index++) {
          const target = this.#columns[index];
          if (target === undefined) throw new TypeError("Missing carried argument");
          copyFixedSlot(index === 1 ? b.data : s.data, index === 1 ? bi : si, target.data, used);
        }
        used++;
        if (this.kind === "integer") {
          const next = current + step;
          done = next < min || next > max;
          current = next;
        } else current = addCalendar(current, months, days, micros);
        const target = Math.max(
          1,
          Math.min(this.#capacity, this.options.rowsNeeded?.() ?? this.#capacity),
        );
        if (used >= target || this.#slice.due()) {
          await this.#emit(used);
          used = 0;
          if (this.#slice.due()) {
            await yieldTask();
            afterYield(this.#slice.clock);
          }
        }
      }
      if (used > 0) await this.#emit(used);
    }
  }
  ready(): Promise<void> {
    return this.#pending ?? Promise.resolve();
  }
  async finish(): Promise<void> {
    await this.#pending;
    this.#check();
    await this.downstream.finish();
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.downstream.abort(reason);
  }
  get closed(): Promise<void> {
    return this.#pending?.catch(() => undefined) ?? Promise.resolve();
  }
}
