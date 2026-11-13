/** One external group's transition state. DISTINCT follows PostgreSQL's ordered
 * value transitions, including floating SUM/AVG; the input ordinal breaks equal-value ties.
 */
import type { CheckpointCall } from "./aggregate.js";
import type { StateReader } from "./state-row.js";
import type { AggregateCall } from "../../ir/ir.js";
import type { Grant } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import { BATCH_ROWS, type Batch, type Selection, type Vector } from "../../vector/vector.js";
import { ColumnStore } from "../column-store.js";
import { Predicate } from "../filter.js";
import { compileExpr, type CompileContext } from "../kernels/compile.js";
import { isValidSlot, slotOf, IDENTITY, type Compiled } from "../kernels/vectors.js";
import type { Operator } from "../pipeline.js";
import { keysEqual, type SortColumn } from "../sort-keys.js";
import { SortOperator, sortMinimumBytes, type SortContext } from "../sort.js";
import { Charge, Pool } from "../spill-pool.js";
import { SortGrants } from "./sort-grants.js";
import { accumulatorFor, type Accumulator } from "./states.js";

import {
  numericCall,
  externalCallMinimum,
  externalCallDistinct,
  externalCallStepBytes,
} from "./external-call-layout.js";
export { externalCallMinimum, externalCallDistinct } from "./external-call-layout.js";

const ORDINAL: PhysicalType = { kind: "float64" };
const ZERO_GROUPS = new Int32Array(BATCH_ROWS); // Immutable: every row of this group updates slot 0.
const asc = (column: number): SortColumn => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
});
const VALUE_ORDER = [asc(0)];
const NUMERIC_SCRATCH = 384 * 1024;
/** Compiler buffers only: do not reserve unused hash tables in the external recipe. */
export function externalCallArguments(
  call: AggregateCall,
  context: CompileContext,
): {
  argument: Compiled | null;
  filter: Predicate | null;
} {
  const arg = call.args[0];
  return {
    argument: call.fn === "count_star" || arg === undefined ? null : compileExpr(arg, context),
    filter: call.filter === null ? null : new Predicate(call.filter, context),
  };
}

export interface ExternalCallHost {
  readonly grants: SortGrants;
  readonly context: CompileContext;
  check(): void;
  pace(): Promise<void>;
  sortContext(grant: Grant): SortContext;
}

/** All retained state and its next resize/emit overlap live in this call's own admitted lane.
 * The pool never waits on another execution's pinned minimum. The host gives every call and
 * each nested sort an independent progress owner as part of one atomic execution admission.
 */
export class ExternalCall {
  readonly #argument: Compiled | null;
  readonly #filter: Predicate | null;
  readonly #rows: Uint16Array;
  readonly #selection: Selection;
  readonly #grant: Grant;
  readonly #capacity: number;
  readonly #scratch: number;
  readonly #pool: Pool;
  readonly #charge: ByteCharge;
  readonly #previous: ColumnStore | null;
  readonly #valueType: PhysicalType | null;
  readonly #types: readonly PhysicalType[];
  readonly #valueGrant: Grant | null;
  #values: SortOperator | null = null;
  readonly #ordinalGrant: Grant | null;
  readonly #prefixOrdinal: Vector | null;
  #ordinals: SortOperator | null = null;
  #accumulator: Accumulator;
  #used = 0;
  #largest = 0;
  #emitted = 0;
  #output: Vector | null = null;
  #pending: Promise<void> | null = null;
  #cleanup: Promise<void> | null = null;
  readonly #sorts = new Set<SortOperator>();

  constructor(
    readonly call: AggregateCall,
    readonly host: ExternalCallHost,
    slot: number,
    readonly prefix?: CheckpointCall,
  ) {
    const compiled = externalCallArguments(call, host.context);
    this.#argument = compiled.argument;
    this.#filter = compiled.filter;
    this.#valueType = this.#argument?.type ?? null;
    const minimum = externalCallMinimum(call);
    this.#grant = host.grants.fork(minimum, slot);
    this.#capacity = host.grants.stepCapacity(this.#grant);
    this.#pool = new Pool(new Charge(this.#grant), minimum);
    this.#charge = {
      allocate: (bytes) => {
        if (!this.#pool.allocate(bytes)) return false;
        this.#used += bytes;
        return true;
      },
      free: (bytes) => {
        this.#used -= bytes;
        this.#pool.free(bytes);
      },
    };
    // At most 131072 integer +16383 fractional decimal digits, plus the safe
    // ordinal's <=53 count bits: a coefficient is <62KiB. Precharge alignment/
    // division temporaries and the <=295KiB UTF-16 range-check string BEFORE any
    // BigInt computation. The remaining 256KiB covers state, prepared/output
    // coefficients and metadata, including conservative ownership overlap.
    this.#scratch = numericCall(call) ? NUMERIC_SCRATCH : 0;
    if (!this.#charge.allocate(this.#scratch))
      throw new Error("Numeric aggregate scratch was not admitted");
    if (!this.#charge.allocate(2 * BATCH_ROWS))
      throw new Error("External selection was not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    this.#selection = { rows: this.#rows, count: 0 };
    this.#accumulator = this.#newAccumulator();
    this.#types = this.#valueType === null ? [] : [this.#valueType, ORDINAL];
    this.#previous =
      externalCallDistinct(call) && this.#valueType !== null
        ? new ColumnStore([this.#valueType], this.#charge, { chunkRows: 1, targetBytes: Infinity })
        : null;
    this.#valueGrant =
      this.#previous === null ? null : host.grants.fork(sortMinimumBytes(this.#types), slot + 1);
    this.#ordinalGrant =
      prefix && this.#previous !== null
        ? host.grants.fork(sortMinimumBytes(this.#types), slot + 2)
        : null;
    this.#prefixOrdinal =
      prefix && this.#previous !== null
        ? (() => {
            if (call.fn !== "count") throw new Error("Prefix DISTINCT restoration requires COUNT");
            if (!this.#charge.allocate(520)) throw new Error("Prefix ordinal was not admitted");
            return {
              form: "constant" as const,
              type: ORDINAL,
              length: BATCH_ROWS,
              value: {
                form: "flat" as const,
                type: ORDINAL,
                length: 1,
                validity: null,
                data: { layout: "f64" as const, values: Float64Array.of(-1) },
              },
            };
          })()
        : null;
    this.#restartSorts();
  }
  #newAccumulator(): Accumulator {
    const a = accumulatorFor(
      this.prefix?.stateFn ?? this.call.fn,
      this.prefix?.stateType ?? this.call.type,
      this.#valueType,
      this.#charge,
    );
    a.grow(1);
    return a;
  }
  #makeSort(grant: Grant, keys: readonly SortColumn[], sink: Operator): SortOperator {
    const sort = new SortOperator(
      { types: this.#types, keys, limit: null, offset: 0, withTies: false },
      this.host.sortContext(grant),
      sink,
    );
    this.#sorts.add(sort);
    return sort;
  }
  #restartSorts(): void {
    if (this.#valueGrant === null) return;
    const valuesSink: Operator = {
      push: (batch) => {
        this.#pending = this.#deduplicate(batch);
        return "blocked";
      },
      ready: () => this.#pending ?? Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    this.#values = this.#makeSort(this.#valueGrant, [asc(0), asc(1)], valuesSink);
    if (this.#ordinalGrant !== null) {
      this.#ordinals = this.#makeSort(this.#ordinalGrant, [asc(1)], {
        push: (batch) => {
          const value = batch.columns[0];
          if (!value) throw new Error("Missing restored DISTINCT candidate");
          this.#accumulator.update(
            value,
            ZERO_GROUPS,
            batch.selection?.rows ?? IDENTITY,
            batch.selection?.count ?? batch.length,
            true,
          );
          return "ready";
        },
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      });
    }
  }
  async restoreState(reader: StateReader, bytes: number): Promise<void> {
    if (!this.prefix || !this.#accumulator.restore) throw new Error("Missing exact prefix restore");
    let width = bytes;
    if (this.call.fn === "min" || this.call.fn === "max") {
      // The host owns the decoded page and its framing. Byte extrema copy only
      // their value field; use the same payload units as add() and admission.
      let payload: number | null = null;
      for (const column of reader.columns)
        if (column.data.layout === "varlen")
          payload = Math.max(
            payload ?? 0,
            (column.data.offsets[reader.row + 1] ?? 0) - (column.data.offsets[reader.row] ?? 0),
          );
      width = payload ?? bytes;
    }
    this.#largest = Math.max(this.#largest, width);
    await this.#room(externalCallStepBytes(externalCallMinimum(this.call), width));
    this.#pool.withStep(() => this.#accumulator.restore?.(0, reader));
    if (this.prefix.mode) reader.number();
  }
  async restoreMember(value: Vector): Promise<void> {
    if (!this.#values || !this.#prefixOrdinal) throw new Error("Missing prefix membership sort");
    if (
      isValidSlot(value, 0) &&
      this.#values.push({ length: 1, columns: [value, this.#prefixOrdinal], selection: null }) ===
        "blocked"
    )
      await this.#values.ready();
  }
  async #room(peak: number): Promise<void> {
    this.host.check();
    // Retained extrema can grow again before they are emitted. Take their whole
    // already-admitted lane once, rather than ask for a second loan while pinning
    // the first. Never turn optional later headroom into a larger capacity target.
    const target = peak > externalCallMinimum(this.call) ? Math.max(peak, this.#capacity) : peak;
    await this.#pool.need(
      Math.max(0, target - this.#used),
      this.#grant,
      this.host.sortContext(this.#grant).signal,
      this.#capacity > externalCallMinimum(this.call),
    );
    this.host.check();
  }
  #width(vector: Vector | null, rows: Uint16Array, count: number): number {
    let largest = 0;
    if (vector === null) return 0;
    for (let k = 0; k < count; k++) {
      const row = rows[k] ?? 0;
      if (!isValidSlot(vector, row)) continue;
      largest = Math.max(largest, this.#widthAt(vector, row));
    }
    return largest;
  }
  #widthAt(vector: Vector, row: number): number {
    const [flat, slot] = slotOf(vector, row);
    return flat.data.layout === "varlen"
      ? (flat.data.offsets[slot + 1] ?? 0) - (flat.data.offsets[slot] ?? 0)
      : 0;
  }
  async add(batch: Batch, from: number, count: number, ordinal: Vector): Promise<void> {
    this.host.check();
    for (let i = 0; i < count; i++) this.#rows[i] = from + i;
    this.#selection.count = count;
    const selected =
      this.#filter?.select({ ...batch, selection: this.#selection }) ?? this.#selection;
    const value = this.#argument?.evaluate(batch, selected) ?? null;
    if (this.#values !== null) {
      let kept = 0;
      for (let k = 0; k < selected.count; k++) {
        const row = selected.rows[k] ?? 0;
        if (value !== null && isValidSlot(value, row)) this.#rows[kept++] = row;
      }
      this.#selection.count = kept;
      if (
        kept !== 0 &&
        value !== null &&
        this.#values.push({
          length: batch.length,
          columns: [value, ordinal],
          selection: this.#selection,
        }) === "blocked"
      )
        await this.#values.ready();
    } else {
      // FILTER rows can be sparse; preserve their exact positions and order.
      this.#largest = Math.max(this.#largest, this.#width(value, selected.rows, selected.count));
      await this.#room(externalCallStepBytes(externalCallMinimum(this.call), this.#largest));
      await this.host.pace();
      this.#pool.withStep(() =>
        this.#accumulator.update(value, ZERO_GROUPS, selected.rows, selected.count, true),
      );
    }
    this.host.check();
  }
  async #deduplicate(batch: Batch): Promise<void> {
    const previous = this.#previous,
      value = batch.columns[0];
    if (previous === null || value === undefined)
      throw new Error("Missing external DISTINCT state");
    let kept = 0;
    for (let row = 0; row < batch.length; row++) {
      await this.host.pace();
      if (
        previous.rows > 0 &&
        keysEqual([value.type], VALUE_ORDER, previous.vectors(), 0, [value], row)
      )
        continue;
      previous.reset(true);
      this.#pool.settle();
      const size = this.#widthAt(value, row);
      await this.#room(externalCallStepBytes(externalCallMinimum(this.call), size));
      if (this.#pool.withStep(() => previous.append([value], null, row, 1)) !== 1)
        throw new Error("External DISTINCT capture exceeded its admitted step");
      if (this.prefix) {
        const ordinal = batch.columns[1];
        if (!ordinal) throw new Error("Missing prefix member ordinal");
        const [flat, at] = slotOf(ordinal, row);
        if (flat.data.layout !== "f64") throw new Error("Invalid prefix member ordinal");
        if ((flat.data.values[at] ?? -1) < 0) continue;
      }
      this.#rows[kept++] = row;
    }
    if (this.#ordinals) {
      if (
        kept &&
        this.#ordinals.push({ ...batch, selection: { rows: this.#rows, count: kept } }) ===
          "blocked"
      )
        await this.#ordinals.ready();
    } else
      this.#pool.withStep(() =>
        this.#accumulator.update(value, ZERO_GROUPS, this.#rows, kept, true),
      );
  }

  async emit(): Promise<Vector> {
    this.host.check();
    if (this.#values !== null) {
      await this.#values.finish();
      await this.#values.closed;
      if (this.#valueGrant !== null) this.host.grants.reset(this.#valueGrant);
      if (this.#ordinals) {
        await this.#ordinals.finish();
        await this.#ordinals.closed;
        if (this.#ordinalGrant) this.host.grants.reset(this.#ordinalGrant);
        this.#ordinals = null;
      }
      this.#previous?.reset(true);
      this.#pool.settle();
      this.#sorts.clear();
      this.#values = null;
    }
    const bytes = this.#accumulator.outputBytes(0, 1);
    await this.#room(this.#used + bytes);
    if (!this.#charge.allocate(bytes))
      throw new Error("External aggregate output was not admitted");
    this.#emitted = bytes;
    this.#output = this.#pool.withStep(() =>
      this.prefix && this.prefix.reads !== "own" && this.#accumulator.emitAs
        ? this.#accumulator.emitAs(this.prefix.reads === "average", this.call.type, 0, 1)
        : this.#accumulator.emit(0, 1),
    );
    return this.#output;
  }
  restart(): void {
    this.#output = null;
    this.#charge.free(this.#emitted);
    this.#emitted = 0;
    this.#accumulator.close();
    this.#pool.settle();
    this.#accumulator = this.#newAccumulator();
    this.#largest = 0;
    this.#restartSorts();
  }
  abort(reason: unknown): void {
    for (const sort of this.#sorts) sort.abort(reason);
  }
  close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      const results = await Promise.allSettled([...this.#sorts].map((s) => s.closed));
      await this.#pending?.catch(() => undefined);
      this.#output = null;
      this.#charge.free(this.#emitted);
      this.#emitted = 0;
      this.#accumulator.close();
      this.#previous?.close();
      this.#sorts.clear();
      this.#values = null;
      this.#ordinals = null;
      this.#pending = null;
      this.#charge.free(2 * BATCH_ROWS);
      this.#charge.free(this.#scratch);
      if (this.#prefixOrdinal) this.#charge.free(520);
      this.#pool.close();
      const errors = results
        .filter((r): r is PromiseRejectedResult => r.status === "rejected")
        .map((r) => r.reason as unknown);
      if (errors.length) throw new AggregateError(errors, "External DISTINCT cleanup failed");
    })());
  }
}
