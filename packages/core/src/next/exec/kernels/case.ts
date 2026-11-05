/**
 * CASE and COALESCE with masked branch evaluation.
 *
 * - CASE: each `when` runs on the rows no earlier branch took; its `then` runs only on the rows it
 *   took; `otherwise` (or null) on the rest. A branch never sees a row it does not produce, so
 *   `CASE WHEN x = 0 THEN 0 ELSE 1 / x END` never divides by zero.
 * - COALESCE: argument k runs only on the rows every earlier argument left null.
 * - Output: each part (a branch's value and the rows it owns) is copied into one output of the
 *   result type over its own row list: no per-row dispatch on which branch owns a row, so mixed
 *   rows cost no branch mispredictions. Varlen output is assembled in row order instead (its
 *   offsets must be monotone). When every part is all-valid and every computed row has a part,
 *   the output has no validity work at all.
 */
import type { PhysicalType } from "../../types.js";
import { layoutOf } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector, VectorData } from "../../vector/vector.js";
import { truthAt } from "./logic.js";
import { coefficientAt, scaleAt, specialAt, type DecimalData } from "./numeric.js";
import {
  IDENTITY,
  copyFixedSlot,
  flatOf,
  nullConstant,
  isValidSlot,
  slotOf,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

const TRUE = 1;
/** "No part" in the row-owner map (Uint16Array: more than 255 branches are fine). */
const NONE = 0xffff;

type VarlenData = Extract<VectorData, { layout: "varlen" }>;

/** A source vector and the rows (ascending) whose value it supplies; reused across batches. */
interface Part {
  vector: Vector;
  rows: Selection;
}

/** Part objects for every branch, allocated once (no object per batch). */
class Parts {
  readonly list: Part[] = [];
  readonly #pool: Part[];
  constructor(size: number, type: PhysicalType) {
    const empty = nullConstant(type);
    const rows: Selection = { rows: IDENTITY, count: 0 };
    this.#pool = Array.from({ length: size }, () => ({ vector: empty, rows }));
  }
  reset(): void {
    this.list.length = 0;
  }
  push(vector: Vector, rows: Selection): void {
    const part = this.#pool[this.list.length];
    if (part === undefined) throw new RangeError("more CASE parts than branches");
    part.vector = vector;
    part.rows = rows;
    this.list.push(part);
  }
}

function allValid(vector: Vector): boolean {
  switch (vector.form) {
    case "flat":
    case "dictionary":
      return vector.validity === null;
    case "constant":
      return vector.value.validity === null || ((vector.value.validity[0] ?? 0) & 1) === 1;
  }
}

/** The values of a flat or constant number-like vector, else null (stride: 1 flat, 0 constant). */
function numberArray(vector: Vector): ArrayLike<number> | null {
  if (vector.form === "dictionary") return null;
  const data = (vector.form === "flat" ? vector : vector.value).data;
  if (
    data.layout === "bool" ||
    data.layout === "i32" ||
    data.layout === "f32" ||
    data.layout === "f64"
  )
    return data.values;
  if (data.layout === "int64" && data.values.repr === "safe") return data.values.values;
  return null;
}

class Assembler {
  readonly #arena: OutputArena;
  readonly #layout: ReturnType<typeof layoutOf>;
  readonly #fixed: FlatVector | null = null;
  #int64: FlatVector | null = null;
  readonly #validity: Uint32Array;
  readonly #owner: Uint16Array;
  #bytes: Uint8Array = new Uint8Array(0);
  readonly #offsets: Int32Array | null = null;
  readonly #scales: Uint16Array | null = null;
  readonly #safe: Float64Array | null = null;
  readonly #wide: bigint[] | null = null;
  readonly #specials: Uint8Array | null = null;

  constructor(
    readonly type: PhysicalType,
    arena: OutputArena,
  ) {
    this.#arena = arena;
    this.#layout = layoutOf(type);
    const capacity = arena.capacity;
    this.#validity = arena.validity();
    arena.take(2 * capacity);
    this.#owner = new Uint16Array(capacity);
    if (this.#layout === "varlen") {
      arena.take(4 * (capacity + 1));
      this.#offsets = new Int32Array(capacity + 1);
    } else if (this.#layout === "decimal") {
      arena.take(2 * capacity + 8 * capacity + capacity + 32 * capacity);
      this.#scales = new Uint16Array(capacity);
      this.#safe = new Float64Array(capacity);
      this.#wide = new Array<bigint>(capacity).fill(0n);
      this.#specials = new Uint8Array(capacity);
    } else this.#fixed = arena.flat(type).vector;
  }

  /** The output over `parts`; `missing` rows (a CASE without ELSE, an all-null COALESCE) are null. */
  assemble(length: number, parts: readonly Part[], missing: Selection | null): FlatVector {
    const validity = this.#validityOf(parts, missing);
    switch (this.#layout) {
      case "varlen":
        return this.#varlen(length, parts, validity);
      case "decimal":
        return this.#decimal(length, parts, validity);
      default:
        return this.#fixedOutput(length, parts, validity);
    }
  }

  /** Null when every part is all-valid and no row is missing; else the computed rows' bits. */
  #validityOf(parts: readonly Part[], missing: Selection | null): Uint32Array | null {
    if (missing === null || missing.count === 0) {
      let valid = true;
      for (const part of parts) if (!allValid(part.vector)) valid = false;
      if (valid) return null;
    }
    const validity = this.#validity;
    for (const part of parts) {
      const { vector, rows } = part;
      for (let k = 0; k < rows.count; k++) {
        const i = rows.rows[k] ?? 0;
        const bit = isValidSlot(vector, i) ? 1 : 0;
        const mask = 1 << (i & 31);
        const word = i >>> 5;
        validity[word] = ((validity[word] ?? 0) & ~mask) | (-bit & mask);
      }
    }
    if (missing !== null)
      for (let k = 0; k < missing.count; k++) {
        const i = missing.rows[k] ?? 0;
        validity[i >>> 5] = (validity[i >>> 5] ?? 0) & ~(1 << (i & 31));
      }
    return validity;
  }

  #fixedOutput(length: number, parts: readonly Part[], validity: Uint32Array | null): FlatVector {
    const out = this.#fixed;
    if (out === null) throw new TypeError("no fixed output");
    // An exact int64 source (or interval µs) promotes the output to BigInt64Array for this batch.
    let promote = false;
    for (const part of parts) {
      const data = flatOf(part.vector).data;
      if (
        (data.layout === "int64" && data.values.repr === "int64") ||
        (data.layout === "interval" && data.micros.repr === "int64")
      )
        promote = true;
    }
    const target = promote ? this.#int64Target(out) : out;
    const data = target.data;
    const values =
      data.layout === "bool" ||
      data.layout === "i32" ||
      data.layout === "f32" ||
      data.layout === "f64"
        ? data.values
        : data.layout === "int64" && data.values.repr === "safe"
          ? data.values.values
          : null;
    for (const part of parts) {
      const source = values === null ? null : numberArray(part.vector);
      const rows = part.rows.rows;
      const count = part.rows.count;
      if (source !== null && values !== null) {
        const stride = part.vector.form === "flat" ? 1 : 0;
        if (stride === 0) {
          const v = source[0] ?? 0;
          for (let k = 0; k < count; k++) values[rows[k] ?? 0] = v;
        } else
          for (let k = 0; k < count; k++) {
            const i = rows[k] ?? 0;
            values[i] = source[i] ?? 0;
          }
        continue;
      }
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        if (!isValidSlot(part.vector, i)) continue;
        const [flat, slot] = slotOf(part.vector, i);
        copyFixedSlot(flat.data, slot, data, i);
      }
    }
    target.length = length;
    target.validity = validity;
    return target;
  }

  #int64Target(out: FlatVector): FlatVector {
    if (this.#int64 === null) {
      const capacity = this.#arena.capacity;
      this.#arena.take(8 * capacity);
      const micros = { repr: "int64", values: new BigInt64Array(capacity) } as const;
      this.#int64 = {
        form: "flat",
        type: out.type,
        length: 0,
        validity: null,
        data:
          out.data.layout === "interval"
            ? { layout: "interval", months: out.data.months, days: out.data.days, micros }
            : { layout: "int64", values: micros },
      };
    }
    return this.#int64;
  }

  /** Row-ordered: offsets must be monotone in slot order. */
  #varlen(length: number, parts: readonly Part[], validity: Uint32Array | null): FlatVector {
    const offsets = this.#offsets;
    if (offsets === null) throw new TypeError("no varlen output");
    const owner = this.#owner;
    owner.fill(NONE, 0, length);
    let total = 0;
    for (let p = 0; p < parts.length; p++) {
      const part = parts[p];
      if (part === undefined) continue;
      for (let k = 0; k < part.rows.count; k++) {
        const i = part.rows.rows[k] ?? 0;
        owner[i] = p;
        if (!isValidSlot(part.vector, i)) continue;
        const [flat, slot] = slotOf(part.vector, i);
        const data = flat.data as VarlenData;
        total += (data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0);
      }
    }
    if (this.#bytes.length < total) {
      const size = Math.max(total, 2 * this.#bytes.length);
      this.#arena.take(size);
      if (this.#bytes.length > 0) this.#arena.give(this.#bytes.length);
      this.#bytes = new Uint8Array(size);
    }
    const bytes = this.#bytes;
    let at = 0;
    offsets[0] = 0;
    for (let i = 0; i < length; i++) {
      const part = parts[owner[i] ?? NONE];
      if (part !== undefined && isValidSlot(part.vector, i)) {
        const [flat, slot] = slotOf(part.vector, i);
        const data = flat.data as VarlenData;
        const start = data.offsets[slot] ?? 0;
        const end = data.offsets[slot + 1] ?? 0;
        bytes.set(data.bytes.subarray(start, end), at);
        at += end - start;
      }
      offsets[i + 1] = at;
    }
    return {
      form: "flat",
      type: this.type,
      length,
      validity,
      data: { layout: "varlen", bytes, offsets },
    };
  }

  #decimal(length: number, parts: readonly Part[], validity: Uint32Array | null): FlatVector {
    const scales = this.#scales;
    const safe = this.#safe;
    const wide = this.#wide;
    const specials = this.#specials;
    if (scales === null || safe === null || wide === null || specials === null)
      throw new TypeError("no decimal output");
    let allSafe = true;
    let anySpecial = false;
    for (const part of parts) {
      const [flat] = slotOf(part.vector, 0);
      const data = flat.data as DecimalData;
      if (data.coefficients.repr !== "safe") allSafe = false;
      if (data.specials !== null) anySpecial = true;
    }
    for (const part of parts)
      for (let k = 0; k < part.rows.count; k++) {
        const i = part.rows.rows[k] ?? 0;
        specials[i] = 0;
        if (!isValidSlot(part.vector, i)) continue;
        const [flat, slot] = slotOf(part.vector, i);
        const data = flat.data as DecimalData;
        scales[i] = scaleAt(data, slot);
        specials[i] = specialAt(data, slot);
        if (allSafe) safe[i] = (data.coefficients as { values: Float64Array }).values[slot] ?? 0;
        else wide[i] = coefficientAt(data, slot);
      }
    return {
      form: "flat",
      type: this.type,
      length,
      validity,
      data: {
        layout: "decimal",
        scale: null,
        scales,
        coefficients: allSafe ? { repr: "safe", values: safe } : { repr: "wide", values: wide },
        specials: anySpecial ? specials : null,
      },
    };
  }
}

function selection(arena: OutputArena): Selection {
  arena.take(2 * arena.capacity);
  return { rows: new Uint16Array(arena.capacity), count: 0 };
}

/**
 * Splits the rows (`rows`, or all `length` rows) into those where `when` is true and the rest,
 * branch-free for a flat bool: each row is written to both lists; only one count advances.
 */
function split(
  when: Vector,
  rows: Selection | null,
  length: number,
  taken: Selection,
  rest: Selection,
): void {
  let t = 0;
  let r = 0;
  const source = rows === null ? IDENTITY : rows.rows;
  const count = rows === null ? length : rows.count;
  if (when.form === "flat") {
    const values = (when.data as { values: Uint8Array }).values;
    const validity = when.validity;
    const yes = taken.rows;
    const no = rest.rows;
    if (validity === null)
      for (let k = 0; k < count; k++) {
        const i = source[k] ?? 0;
        const hit = values[i] ?? 0;
        yes[t] = i;
        no[r] = i;
        t += hit;
        r += 1 - hit;
      }
    else
      for (let k = 0; k < count; k++) {
        const i = source[k] ?? 0;
        const hit = (values[i] ?? 0) & (((validity[i >>> 5] ?? 0) >>> (i & 31)) & 1);
        yes[t] = i;
        no[r] = i;
        t += hit;
        r += 1 - hit;
      }
  } else
    for (let k = 0; k < count; k++) {
      const i = source[k] ?? 0;
      if (truthAt(when, i) === TRUE) taken.rows[t++] = i;
      else rest.rows[r++] = i;
    }
  taken.count = t;
  rest.count = r;
}

class CaseNode implements Compiled {
  readonly #assembler: Assembler;
  readonly #taken: Selection[];
  /** Two alternating "still undecided" lists. */
  readonly #a: Selection;
  readonly #b: Selection;
  readonly #all: Selection = { rows: IDENTITY, count: 0 };
  readonly #parts: Parts;
  constructor(
    readonly type: PhysicalType,
    readonly branches: ReadonlyArray<{ readonly when: Compiled; readonly then: Compiled }>,
    readonly otherwise: Compiled | null,
    arena: OutputArena,
  ) {
    if (branches.length >= NONE) throw new TypeError("too many CASE branches");
    this.#parts = new Parts(branches.length + 1, type);
    this.#assembler = new Assembler(type, arena);
    this.#taken = branches.map(() => selection(arena));
    this.#a = selection(arena);
    this.#b = selection(arena);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const parts = this.#parts;
    parts.reset();
    // The first WHEN sees the caller's rows as they are (dense when null).
    let pending: Selection | null = rows;
    let rest = this.#a;
    // Index loops, not entries(): no iterator or pair object per batch.
    for (let b = 0; b < this.branches.length; b++) {
      const branch = this.branches[b];
      if (branch === undefined) break;
      if (pending !== null && pending.count === 0) break;
      const when = branch.when.evaluate(batch, pending);
      const taken = this.#taken[b];
      if (taken === undefined) break;
      split(when, pending, batch.length, taken, rest);
      if (taken.count > 0) parts.push(branch.then.evaluate(batch, taken), taken);
      pending = rest;
      rest = rest === this.#a ? this.#b : this.#a;
    }
    if (pending === null) {
      this.#all.count = batch.length;
      pending = this.#all;
    }
    let missing: Selection | null = null;
    if (pending.count > 0) {
      if (this.otherwise !== null) parts.push(this.otherwise.evaluate(batch, pending), pending);
      else missing = pending;
    }
    return this.#assembler.assemble(batch.length, parts.list, missing);
  }
}

class CoalesceNode implements Compiled {
  readonly #assembler: Assembler;
  readonly #kept: Selection[];
  readonly #a: Selection;
  readonly #b: Selection;
  readonly #parts: Parts;
  constructor(
    readonly type: PhysicalType,
    readonly args: readonly Compiled[],
    arena: OutputArena,
  ) {
    if (args.length >= NONE) throw new TypeError("too many COALESCE arguments");
    this.#parts = new Parts(args.length, type);
    this.#assembler = new Assembler(type, arena);
    this.#kept = args.map(() => selection(arena));
    this.#a = selection(arena);
    this.#b = selection(arena);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const parts = this.#parts;
    parts.reset();
    let pending: Selection | null = rows;
    let rest = this.#a;
    for (let a = 0; a < this.args.length; a++) {
      const arg = this.args[a];
      if (arg === undefined) break;
      if (pending !== null && pending.count === 0) break;
      const value = arg.evaluate(batch, pending);
      const kept = this.#kept[a];
      if (kept === undefined) break;
      const source = pending === null ? IDENTITY : pending.rows;
      const count = pending === null ? batch.length : pending.count;
      kept.count = 0;
      rest.count = 0;
      for (let k = 0; k < count; k++) {
        const i = source[k] ?? 0;
        if (isValidSlot(value, i)) kept.rows[kept.count++] = i;
        else rest.rows[rest.count++] = i;
      }
      if (kept.count > 0) parts.push(value, kept);
      pending = rest;
      rest = rest === this.#a ? this.#b : this.#a;
    }
    const missing = pending !== null && pending.count > 0 ? pending : null;
    return this.#assembler.assemble(batch.length, parts.list, missing);
  }
}

export function compileCase(
  type: PhysicalType,
  branches: ReadonlyArray<{ readonly when: Compiled; readonly then: Compiled }>,
  otherwise: Compiled | null,
  arena: OutputArena,
): Compiled {
  return new CaseNode(type, branches, otherwise, arena);
}

export function compileCoalesce(
  type: PhysicalType,
  args: readonly Compiled[],
  arena: OutputArena,
): Compiled {
  return new CoalesceNode(type, args, arena);
}
