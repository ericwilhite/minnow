/**
 * Three-valued logic: AND, OR, NOT, IS [NOT] NULL, IS [NOT] TRUE/FALSE, and `[NOT] IN (list)`.
 *
 * - AND/OR evaluate each argument only on the rows its predecessors left undecided (a row already
 *   false for AND, or true for OR, is not computed again), so `x <> 0 AND 10 / x > 1` never divides
 *   by zero. SQL leaves the order of evaluation open; this order is the one the executor promises.
 * - AND: false if any argument is false, else null if any is null, else true. OR: the dual.
 * - IN: true if the argument equals an item; else null if the argument or an item is null; else
 *   false. NOT IN is its negation (so it is null whenever IN is).
 */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Validity, Vector } from "../../vector/vector.js";
import { slotOrder } from "./compare.js";
import { constantIsNull } from "./numeric.js";
import {
  nullConstant,
  countIn,
  rowsIn,
  setValidity,
  slotOf,
  validAt,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

const BOOL: PhysicalType = { kind: "bool" };
const TRUE = 1;
const FALSE = 0;
const NULL = 2;

/** Truth of bool vector `vector` at row `row`: TRUE, FALSE or NULL. */
export function truthAt(vector: Vector, row: number): number {
  switch (vector.form) {
    case "flat":
      if (!validAt(vector.validity, row)) return NULL;
      return (vector.data as { values: Uint8Array }).values[row] === 1 ? TRUE : FALSE;
    case "constant":
      if (!validAt(vector.value.validity, 0)) return NULL;
      return (vector.value.data as { values: Uint8Array }).values[0] === 1 ? TRUE : FALSE;
    case "dictionary": {
      if (!validAt(vector.validity, row)) return NULL;
      return (vector.dictionary.data as { values: Uint8Array }).values[vector.codes[row] ?? 0] === 1
        ? TRUE
        : FALSE;
    }
  }
}

/** A bool output: values and an always-present validity buffer, published as null when all valid. */
class BoolOutput {
  readonly vector: FlatVector;
  readonly values: Uint8Array;
  readonly validity: Validity;
  constructor(arena: OutputArena) {
    const output = arena.flat(BOOL);
    this.vector = output.vector;
    this.values = (output.vector.data as { values: Uint8Array }).values;
    this.validity = output.validity;
  }
  publish(length: number, anyNull: boolean): FlatVector {
    this.vector.length = length;
    this.vector.validity = anyNull ? this.validity : null;
    return this.vector;
  }
}

class JunctionNode implements Compiled {
  readonly type = BOOL;
  readonly #out: BoolOutput;
  readonly #pending: Selection;
  readonly #next: Selection;
  /** The value that decides a row: FALSE for AND, TRUE for OR. */
  readonly #decides: number;
  constructor(
    readonly and: boolean,
    readonly args: readonly Compiled[],
    arena: OutputArena,
  ) {
    this.#out = new BoolOutput(arena);
    arena.take(4 * arena.capacity);
    this.#pending = { rows: new Uint16Array(arena.capacity), count: 0 };
    this.#next = { rows: new Uint16Array(arena.capacity), count: 0 };
    this.#decides = and ? FALSE : TRUE;
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const out = this.#out;
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    const neutral = this.and ? TRUE : FALSE;
    for (let k = 0; k < count; k++) {
      const i = active[k] ?? 0;
      out.values[i] = neutral;
      setValidity(out.validity, i, true);
    }
    let pending = this.#pending;
    pending.rows.set(active.subarray(0, count));
    pending.count = count;
    let next = this.#next;
    let anyNull = false;
    for (const arg of this.args) {
      if (pending.count === 0) break;
      const value = arg.evaluate(batch, pending);
      next.count = 0;
      for (let k = 0; k < pending.count; k++) {
        const i = pending.rows[k] ?? 0;
        const truth = truthAt(value, i);
        if (truth === this.#decides) {
          out.values[i] = truth;
          setValidity(out.validity, i, true);
          continue;
        }
        if (truth === NULL) {
          setValidity(out.validity, i, false);
          anyNull = true;
        }
        next.rows[next.count++] = i;
      }
      [pending, next] = [next, pending];
    }
    return out.publish(batch.length, anyNull);
  }
}

class NotNode implements Compiled {
  readonly type = BOOL;
  readonly #out: BoolOutput;
  constructor(
    readonly arg: Compiled,
    arena: OutputArena,
  ) {
    this.#out = new BoolOutput(arena);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const value = this.arg.evaluate(batch, rows);
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    const out = this.#out;
    let anyNull = false;
    for (let k = 0; k < count; k++) {
      const i = active[k] ?? 0;
      const truth = truthAt(value, i);
      out.values[i] = truth === TRUE ? 0 : 1;
      const valid = truth !== NULL;
      setValidity(out.validity, i, valid);
      if (!valid) anyNull = true;
    }
    return out.publish(batch.length, anyNull);
  }
}

/** IS [NOT] NULL, IS [NOT] TRUE, IS [NOT] FALSE: never null. */
class TestNode implements Compiled {
  readonly type = BOOL;
  readonly #out: BoolOutput;
  constructor(
    readonly arg: Compiled,
    /** The truth values (TRUE, FALSE, NULL) for which the test holds. */
    readonly holds: readonly [boolean, boolean, boolean],
    arena: OutputArena,
  ) {
    this.#out = new BoolOutput(arena);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const value = this.arg.evaluate(batch, rows);
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    const values = this.#out.values;
    const [ifFalse, ifTrue, ifNull] = [this.holds[1], this.holds[0], this.holds[2]];
    if (this.arg.type.kind !== "bool") {
      // IS [NOT] NULL over any type: validity only.
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        const valid =
          value.form === "constant" ? validAt(value.value.validity, 0) : validAt(value.validity, i);
        values[i] = (valid ? ifTrue : ifNull) ? 1 : 0;
      }
    } else
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        const truth = truthAt(value, i);
        values[i] = (truth === TRUE ? ifTrue : truth === FALSE ? ifFalse : ifNull) ? 1 : 0;
      }
    return this.#out.publish(batch.length, false);
  }
}

class InListNode implements Compiled {
  readonly type = BOOL;
  readonly #out: BoolOutput;
  readonly #null = nullConstant(BOOL);
  #entryId = -1;
  #entries = new Uint8Array(0);
  readonly #items: Vector[] = [];
  /** The constant values the cached entries were computed against. */
  readonly #itemValues: Array<FlatVector | null> = [];
  constructor(
    readonly negated: boolean,
    readonly arg: Compiled,
    readonly list: readonly Compiled[],
    /** Every item is a constant: dictionary arguments are decided once per entry. */
    readonly constantList: boolean,
    arena: OutputArena,
  ) {
    this.#out = new BoolOutput(arena);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const value = this.arg.evaluate(batch, rows);
    if (constantIsNull(value)) {
      (this.#null as { length: number }).length = batch.length;
      return this.#null;
    }
    const items = this.#items;
    for (let k = 0; k < this.list.length; k++) {
      const item = this.list[k];
      if (item !== undefined) items[k] = item.evaluate(batch, rows);
    }
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    const out = this.#out;
    let anyNull = false;
    const hit = this.negated ? 0 : 1;
    if (value.form === "dictionary" && this.constantList) {
      // The per-entry answers depend on the dictionary and on the item values: a parameter
      // re-bound between executions (or a new constant) invalidates them.
      if (this.#entryId !== value.dictionaryId || !this.#sameItems(items)) {
        const dictionary = value.dictionary;
        if (this.#entries.length < dictionary.length)
          this.#entries = new Uint8Array(dictionary.length);
        for (let e = 0; e < dictionary.length; e++)
          this.#entries[e] = this.#truth(dictionary, e, items, 0);
        this.#entryId = value.dictionaryId;
        for (let k = 0; k < items.length; k++) {
          const item = items[k];
          if (item === undefined) continue;
          this.#itemValues[k] = item.form === "constant" ? item.value : null;
        }
      }
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        if (!validAt(value.validity, i)) {
          setValidity(out.validity, i, false);
          anyNull = true;
          continue;
        }
        const truth = this.#entries[value.codes[i] ?? 0] ?? FALSE;
        this.#write(i, truth, hit);
        if (truth === NULL) anyNull = true;
      }
      return out.publish(batch.length, anyNull);
    }
    for (let k = 0; k < count; k++) {
      const i = active[k] ?? 0;
      const [flat, slot] = slotOf(value, i);
      const valid = value.form === "constant" ? true : validAt(value.validity, i);
      const truth = valid ? this.#truth(flat, slot, items, i) : NULL;
      this.#write(i, truth, hit);
      if (truth === NULL) anyNull = true;
    }
    return out.publish(batch.length, anyNull);
  }
  #sameItems(items: readonly Vector[]): boolean {
    for (let k = 0; k < items.length; k++) {
      const item = items[k];
      if (item === undefined) continue;
      if (item.form !== "constant" || this.#itemValues[k] !== item.value) return false;
    }
    return true;
  }
  #write(row: number, truth: number, hit: number): void {
    const out = this.#out;
    if (truth === NULL) {
      setValidity(out.validity, row, false);
      return;
    }
    setValidity(out.validity, row, true);
    out.values[row] = truth === TRUE ? hit : 1 - hit;
  }
  /** IN's truth for value slot (flat, slot) against the items at row `row`. */
  #truth(flat: FlatVector, slot: number, items: readonly Vector[], row: number): number {
    let sawNull = false;
    for (const item of items) {
      const itemRow = item.form === "constant" ? 0 : row;
      const valid =
        item.form === "constant"
          ? validAt(item.value.validity, 0)
          : validAt(item.validity, itemRow);
      if (!valid) {
        sawNull = true;
        continue;
      }
      const [itemFlat, itemSlot] = slotOf(item, row);
      if (slotOrder(flat, slot, itemFlat, itemSlot) === 0) return TRUE;
    }
    return sawNull ? NULL : FALSE;
  }
}

export function compileJunction(
  and: boolean,
  args: readonly Compiled[],
  arena: OutputArena,
): Compiled {
  if (args.length === 1 && args[0] !== undefined) return args[0];
  return new JunctionNode(and, args, arena);
}

export function compileNot(arg: Compiled, arena: OutputArena): Compiled {
  return new NotNode(arg, arena);
}

/** `test` holds for [true, false, null]. */
export function compileTest(
  arg: Compiled,
  holds: readonly [boolean, boolean, boolean],
  arena: OutputArena,
): Compiled {
  return new TestNode(arg, holds, arena);
}

export function compileInList(
  negated: boolean,
  arg: Compiled,
  list: readonly Compiled[],
  constantList: boolean,
  arena: OutputArena,
): Compiled {
  return new InListNode(negated, arg, list, constantList, arena);
}
