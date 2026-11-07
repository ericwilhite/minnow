/** Bounded best-group ids; aggregate state stays put until only the winners are emitted. */
import type { Rel } from "../../ir/ir.js";
import { OBJECT_OVERHEAD_BYTES } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import { BATCH_ROWS } from "../../vector/vector.js";
import type { FlatVector } from "../../vector/vector.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { SortColumn } from "../sort-keys.js";
import { Charged } from "./groups.js";

export interface GroupTopNSpec {
  /** LIMIT + OFFSET. The downstream sorter still applies the actual offset and order. */
  readonly count: number;
  readonly keys: readonly SortColumn[];
}

/** Finalizers must be copy-only: omitting a losing group's fallible cast could hide an error. */
export function supportsGroupTopN(
  rel: Extract<Rel, { op: "aggregate" }>,
  input: readonly PhysicalType[],
  top: GroupTopNSpec,
): boolean {
  return (
    top.count > 0 &&
    top.count <= BATCH_ROWS &&
    Number.isSafeInteger(top.count) &&
    rel.groupBy.length === 1 &&
    input[rel.groupBy[0] ?? -1]?.kind === "int32" &&
    rel.aggregates.every(
      (a) =>
        a.fn === "count" ||
        a.fn === "count_star" ||
        (a.fn === "sum" && a.type.kind === "float64" && a.args[0]?.type.kind === "float64"),
    ) &&
    top.keys.length > 0 &&
    top.keys.length <= 4 &&
    top.keys.some((k) => k.column === 0) &&
    top.keys.every(
      (k) =>
        k.column === 0 || ["count", "count_star"].includes(rel.aggregates[k.column - 1]?.fn ?? ""),
    )
  );
}

/** Direct projections preserve ranking. HAVING, computed projections and other consumers stop it. */
export function groupTopNShape(
  sort: Extract<Rel, { op: "sort" }>,
): { aggregate: Extract<Rel, { op: "aggregate" }>; spec: GroupTopNSpec } | null {
  if (
    sort.withTies ||
    sort.limit?.kind !== "const" ||
    (sort.offset !== null && sort.offset.kind !== "const")
  )
    return null;
  const offset = sort.offset?.value ?? 0,
    count = sort.limit.value + offset;
  if (
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    sort.limit.value < 1 ||
    !Number.isSafeInteger(count)
  )
    return null;
  let aggregate = sort.input;
  let indexes = aggregate.output.map((_, i) => i);
  while (aggregate.op === "project") {
    const project = aggregate;
    if (project.exprs.some((e) => e.op !== "column")) return null;
    indexes = indexes.map((i) => {
      const e = project.exprs[i];
      return e?.op === "column" ? e.index : -1;
    });
    if (indexes.some((i) => i < 0)) return null;
    aggregate = project.input;
  }
  if (aggregate.op !== "aggregate") return null;
  const keys: SortColumn[] = [];
  for (const key of sort.keys) {
    if (key.expr.op !== "column") return null;
    const column = indexes[key.expr.index];
    if (column === undefined) return null;
    keys.push({ column, direction: key.direction, nulls: key.nulls, collation: key.collation });
  }
  const spec = { count, keys };
  return supportsGroupTopN(
    aggregate,
    aggregate.input.output.map((c) => c.type),
    spec,
  )
    ? { aggregate, spec }
    : null;
}

export class GroupTopN {
  readonly #memory: Charged;
  readonly #ids: Int32Array;
  readonly #values: Float64Array;
  #size = 0;
  constructor(
    readonly spec: GroupTopNSpec,
    charge: ByteCharge,
  ) {
    if (
      !Number.isSafeInteger(spec.count) ||
      spec.count < 1 ||
      spec.count > BATCH_ROWS ||
      spec.keys.length < 1 ||
      spec.keys.length > 4
    )
      throw new RangeError("Invalid group top-N bounds");
    this.#memory = new Charged(charge);
    try {
      // Heap, charge tracker and the two typed-array views.
      this.#memory.take(4 * OBJECT_OVERHEAD_BYTES);
      this.#ids = this.#memory.make(Int32Array, spec.count);
      this.#values = this.#memory.make(Float64Array, spec.count * spec.keys.length);
    } catch (error) {
      this.#memory.close();
      throw error;
    }
  }
  get size(): number {
    return this.#size;
  }
  group(index: number): number {
    return this.#ids[index] ?? 0;
  }
  /** Only int32 group keys and safe COUNT results enter here; NaN represents their NULL. */
  #value(column: FlatVector, row: number): number {
    if (column.validity !== null && ((column.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
      return NaN;
    const data = column.data;
    if (data.layout === "i32") return data.values[row] ?? 0;
    if (data.layout === "int64" && data.values.repr === "safe") return data.values.values[row] ?? 0;
    throw new TypeError("group top-N requires integer group keys or safe COUNT results");
  }
  #compare(a: number, b: number, key: SortColumn): number {
    const an = Number.isNaN(a),
      bn = Number.isNaN(b);
    if (an || bn) return an === bn ? 0 : an === (key.nulls === "first") ? -1 : 1;
    const order = a < b ? -1 : a > b ? 1 : 0;
    return key.direction === "asc" ? order : -order;
  }
  #slots(a: number, b: number): number {
    const { count, keys } = this.spec;
    for (let k = 0; k < keys.length; k++) {
      const key = keys[k];
      if (!key) throw new Error("group top-N key");
      const c = this.#compare(
        this.#values[k * count + a] ?? 0,
        this.#values[k * count + b] ?? 0,
        key,
      );
      if (c) return c;
    }
    return (this.#ids[a] ?? 0) - (this.#ids[b] ?? 0);
  }
  #swap(a: number, b: number): void {
    const id = this.#ids[a] ?? 0;
    this.#ids[a] = this.#ids[b] ?? 0;
    this.#ids[b] = id;
    for (let k = 0; k < this.spec.keys.length; k++) {
      const x = k * this.spec.count + a,
        y = k * this.spec.count + b,
        value = this.#values[x] ?? 0;
      this.#values[x] = this.#values[y] ?? 0;
      this.#values[y] = value;
    }
  }
  #repair(slot: number): void {
    if (slot !== 0) {
      while (slot > 0) {
        const parent = (slot - 1) >>> 1;
        if (this.#slots(slot, parent) <= 0) break;
        this.#swap(slot, parent);
        slot = parent;
      }
    } else
      for (;;) {
        const left = slot * 2 + 1;
        if (left >= this.#size) break;
        const right = left + 1,
          worst = right < this.#size && this.#slots(right, left) > 0 ? right : left;
        if (this.#slots(worst, slot) <= 0) break;
        this.#swap(slot, worst);
        slot = worst;
      }
  }
  /** Dense two-key batches avoid a type/validity check for every group and every comparison. */
  #dense(columns: readonly FlatVector[], first: number, length: number): boolean {
    if (
      this.spec.keys.length !== 2 ||
      columns.length !== 2 ||
      columns[0]?.validity !== null ||
      columns[1]?.validity !== null
    )
      return false;
    const a = columns[0].data,
      b = columns[1].data;
    const left =
      a.layout === "i32"
        ? a.values
        : a.layout === "int64" && a.values.repr === "safe"
          ? a.values.values
          : null;
    const right =
      b.layout === "i32"
        ? b.values
        : b.layout === "int64" && b.values.repr === "safe"
          ? b.values.values
          : null;
    const { count, keys } = this.spec;
    if (!left || !right || Number.isNaN(this.#values[0]) || Number.isNaN(this.#values[count]))
      return false;
    const ad = keys[0]?.direction === "asc" ? 1 : -1,
      bd = keys[1]?.direction === "asc" ? 1 : -1;
    for (let row = 0; row < length; row++) {
      const id = first + row,
        x = left[row] ?? 0,
        y = right[row] ?? 0;
      if (this.#size === count) {
        const primary = ad * (x - (this.#values[0] ?? 0));
        const compared = primary || bd * (y - (this.#values[count] ?? 0));
        if (compared > 0 || (compared === 0 && id >= (this.#ids[0] ?? 0))) continue;
      }
      const slot = this.#size < count ? this.#size++ : 0;
      this.#ids[slot] = id;
      this.#values[slot] = x;
      this.#values[count + slot] = y;
      this.#repair(slot);
    }
    return true;
  }
  offer(columns: readonly FlatVector[], first: number, length: number): void {
    if (columns.length !== this.spec.keys.length) throw new TypeError("group rank width");
    if (this.#dense(columns, first, length)) return;
    const { count, keys } = this.spec;
    for (let row = 0; row < length; row++) {
      const id = first + row;
      if (this.#size === count) {
        let compared = 0;
        for (let k = 0; k < keys.length; k++) {
          const column = columns[k],
            key = keys[k];
          if (!column || !key) throw new Error("group top-N width");
          compared = this.#compare(this.#value(column, row), this.#values[k * count] ?? 0, key);
          if (compared) break;
        }
        if (compared > 0 || (compared === 0 && id >= (this.#ids[0] ?? 0))) continue;
      }
      const slot = this.#size < count ? this.#size++ : 0;
      this.#ids[slot] = id;
      for (let k = 0; k < keys.length; k++) {
        const column = columns[k];
        if (!column) throw new Error("group top-N column");
        this.#values[k * count + slot] = this.#value(column, row);
      }
      this.#repair(slot);
    }
  }
  close(): void {
    this.#memory.close();
  }
}
