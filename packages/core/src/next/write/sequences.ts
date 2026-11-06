/**
 * Sequences (access/write-path.ts `SequenceAccess`, ruling R4): values handed out from in-memory
 * blocks, each block reserved by a standalone sequence frame (a one-effect transaction) appended
 * to the log at once, outside any open transaction.
 *
 * - **State.** Per sequence: `next` (the next value to hand out) and `reservedThrough` (values
 *   strictly before it, in the sequence's direction, are covered by logged reservations). Replay
 *   sets both to the logged `reservedThrough`, so a crash skips the unused rest of a block.
 * - **Block size.** 1 for a fresh sequence; doubled (up to 1,024) when a block is used up within
 *   `ADAPT_MS` of its reservation; back to 1 after `ADAPT_MS` without a reservation.
 * - **Bounds.** A non-cycling sequence past its bound throws `SequenceExhaustedError` before
 *   consuming anything; a cycling one wraps to its start of range (PostgreSQL: min for ascending,
 *   max for descending).
 */
import { SequenceExhaustedError, type SequenceAccess } from "../access/write-path.js";
import type { SequenceDef } from "../access/catalog.js";
import { MAX_SAFE } from "../vector/int64.js";
import type { Int64Values } from "../vector/vector.js";
import { need } from "./bytes.js";

export const MAX_BLOCK = 1024;
export const ADAPT_MS = 1000;

interface SequenceState {
  def: SequenceDef;
  next: bigint;
  reservedThrough: bigint;
  /** LSN of the frame that reserved the current block (0: none in this process). */
  blockLsn: number;
  blockSize: number;
  reservedAtMs: number;
}

export interface SequenceHost {
  /** The definition visible to callers now (committed catalog, else the open transaction's). */
  definition(sequenceId: number): SequenceDef | undefined;
  /** Appends a standalone reservation frame; returns its LSN. */
  logReservation(sequenceId: number, reservedThrough: bigint): number;
  now(): number;
}

export class Sequences implements SequenceAccess {
  readonly #states = new Map<number, SequenceState>();
  /** Logged reservations per sequence (lsn, value), for root checkpoints at a global LSN. */
  readonly #history = new Map<number, Array<{ lsn: number; through: bigint }>>();

  constructor(readonly host: SequenceHost) {}

  /** A reserved-through value restored from a root or replayed from a frame. */
  restore(sequenceId: number, reservedThrough: bigint, lsn: number): void {
    const state = this.#states.get(sequenceId);
    if (state !== undefined) {
      state.next = reservedThrough;
      state.reservedThrough = reservedThrough;
    } else this.#pendingRestore.set(sequenceId, reservedThrough);
    this.#remember(sequenceId, lsn, reservedThrough);
  }

  readonly #pendingRestore = new Map<number, bigint>();

  #remember(sequenceId: number, lsn: number, through: bigint): void {
    let list = this.#history.get(sequenceId);
    if (list === undefined) {
      list = [];
      this.#history.set(sequenceId, list);
    }
    list.push({ lsn, through });
  }

  /** Reserved-through values as of `lsn` (for a root whose globalLsn is `lsn`), and prunes. */
  checkpoint(lsn: number): Array<{ sequenceId: number; reservedThrough: bigint }> {
    const out: Array<{ sequenceId: number; reservedThrough: bigint }> = [];
    for (const [sequenceId, list] of this.#history) {
      let last: { lsn: number; through: bigint } | undefined;
      let keepFrom = 0;
      for (let k = 0; k < list.length; k++) {
        const entry = list[k] as { lsn: number; through: bigint };
        if (entry.lsn <= lsn) {
          last = entry;
          keepFrom = k;
        }
      }
      if (last !== undefined) out.push({ sequenceId, reservedThrough: last.through });
      if (keepFrom > 0) list.splice(0, keepFrom);
    }
    return out;
  }

  /** Forgets a dropped sequence. */
  drop(sequenceId: number): void {
    this.#states.delete(sequenceId);
    this.#history.delete(sequenceId);
    this.#pendingRestore.delete(sequenceId);
  }

  #state(sequenceId: number): SequenceState {
    const def = this.host.definition(sequenceId);
    if (def === undefined) throw new RangeError(`Sequence ${String(sequenceId)} does not exist`);
    let state = this.#states.get(sequenceId);
    if (state === undefined) {
      const restored = this.#pendingRestore.get(sequenceId);
      this.#pendingRestore.delete(sequenceId);
      const start = restored ?? def.start;
      state = {
        def,
        next: start,
        reservedThrough: start,
        blockLsn: 0,
        blockSize: 1,
        reservedAtMs: 0,
      };
      this.#states.set(sequenceId, state);
    }
    state.def = def;
    return state;
  }

  /** The `count` values after `next`, wrapping for a cycling sequence; null if exhausted. */
  #plan(state: SequenceState, count: number): bigint[] | null {
    const { increment, min, max, cycle } = state.def;
    const values: bigint[] = [];
    let value = state.next;
    for (let k = 0; k < count; k++) {
      if (value > max || value < min) {
        if (!cycle) return null;
        value = increment > 0n ? min : max;
      }
      values.push(value);
      value += increment;
    }
    return values;
  }

  next(sequenceId: number, count: number, out: Int64Values, offset: number): number {
    const state = this.#state(sequenceId);
    const values = this.#plan(state, count);
    if (values === null) throw new SequenceExhaustedError(sequenceId);
    if (out.repr === "safe")
      for (const value of values)
        if (value > BigInt(MAX_SAFE) || value < -BigInt(MAX_SAFE))
          throw new RangeError("A sequence value is outside the f64-safe range; widen to int64");
    const increment = state.def.increment;
    const ascending = increment > 0n;
    let lsn = state.blockLsn;
    for (let k = 0; k < values.length; k++) {
      const value = need(values[k]);
      // A wrap starts a new reservation range.
      if (value !== state.next) state.reservedThrough = value;
      const covered = ascending ? value < state.reservedThrough : value > state.reservedThrough;
      if (!covered) lsn = this.#reserve(state, value);
      if (out.repr === "safe") out.values[offset + k] = Number(value);
      else out.values[offset + k] = value;
      state.next = value + increment;
    }
    return lsn;
  }

  #reserve(state: SequenceState, from: bigint): number {
    const now = this.host.now();
    if (state.reservedAtMs > 0 && now - state.reservedAtMs < ADAPT_MS)
      state.blockSize = Math.min(MAX_BLOCK, state.blockSize * 2);
    else if (now - state.reservedAtMs >= ADAPT_MS) state.blockSize = 1;
    const { increment, min, max } = state.def;
    let through = from + BigInt(state.blockSize) * increment;
    // Never reserve past the bound (the next value after it is exhaustion or a wrap).
    if (increment > 0n && through > max + increment) through = max + increment;
    if (increment < 0n && through < min + increment) through = min + increment;
    const lsn = this.host.logReservation(state.def.id, through);
    state.reservedThrough = through;
    state.blockLsn = lsn;
    state.reservedAtMs = now;
    this.#remember(state.def.id, lsn, through);
    return lsn;
  }

  set(sequenceId: number, value: bigint, isCalled: boolean): number {
    const state = this.#state(sequenceId);
    const { increment, min, max } = state.def;
    if (value < min || value > max)
      throw new RangeError(`setval: ${value.toString()} is outside the sequence's bounds`);
    const next = isCalled ? value + increment : value;
    state.next = next;
    const lsn = this.host.logReservation(state.def.id, next);
    state.reservedThrough = next;
    state.blockLsn = lsn;
    state.blockSize = 1;
    state.reservedAtMs = this.host.now();
    this.#remember(sequenceId, lsn, next);
    return lsn;
  }
}
