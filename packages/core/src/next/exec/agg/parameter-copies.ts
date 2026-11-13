/** Bounded row layouts for the lanes that actually borrow aggregate progress.
 * Conditional widths are maxima over reachable outputs, not a union of parameter slots.
 * These are sizing facts only: no expression is evaluated and no caller buffer is read.
 */
import type { Rel, ScalarExpr } from "../../ir/ir.js";
import { MAX_BLOB_BYTES } from "../../access/space.js";

type Width =
  | { readonly op: "bytes"; readonly bytes: number }
  | { readonly op: "param"; readonly slot: number }
  | { readonly op: "sum" | "max"; readonly args: readonly Width[] }
  | { readonly op: "unknown" };
export interface AggregateProgressLayout {
  readonly input: readonly Width[];
  readonly keys: readonly Width[];
  readonly calls: ReadonlyArray<{
    readonly width: Width;
    readonly variable: boolean;
    readonly distinct: boolean;
  }>;
}
const zero: Width = { op: "bytes", bytes: 0 };
const variable = (e: ScalarExpr) =>
  ["text", "bytes", "json", "array", "enum", "decimal"].includes(e.type.kind);
function constantNull(e: ScalarExpr): boolean | null {
  if (e.op === "cast") return constantNull(e.input);
  if (e.op === "call" && e.fn === "coalesce") {
    let unknown = false;
    for (const arg of e.args) {
      const value = constantNull(arg);
      if (value === false) return false;
      unknown ||= value === null;
    }
    return unknown ? null : true;
  }
  if (e.op === "call" && e.fn === "nullif" && e.args[0]) {
    const first = constantNull(e.args[0]);
    return first === true || (e.args[1] && constantNull(e.args[1]) === true) ? first : null;
  }
  if (e.op !== "literal") return null;
  return e.value.validity !== null && ((e.value.validity[0] ?? 0) & 1) === 0;
}
function constantBool(e: ScalarExpr): boolean | null | undefined {
  if (e.op !== "literal" || e.type.kind !== "bool") return undefined;
  if (constantNull(e)) return null;
  return e.value.data.layout === "bool" ? e.value.data.values[0] !== 0 : undefined;
}
export function aggregateProgressLayout(
  rel: Extract<Rel, { op: "aggregate" }>,
): AggregateProgressLayout {
  const column = (input: Rel, index: number): Width => {
    const c = input.output[index];
    if (!c || !["text", "bytes", "json", "array", "enum", "decimal"].includes(c.type.kind))
      return zero;
    if (input.op === "project") {
      const e = input.exprs[index];
      return e ? expr(e, input.input) : zero;
    }
    if (input.op === "values")
      return {
        op: "max",
        args: input.rows.map((row) => {
          const e = row[index];
          return e ? expr(e, input) : zero;
        }),
      };
    if (input.op === "join")
      return index < input.left.output.length
        ? column(input.left, index)
        : column(input.right, index - input.left.output.length);
    if (input.op === "aggregate") {
      const key = input.groupBy[index];
      return key === undefined ? { op: "unknown" } : column(input.input, key);
    }
    if (input.op === "setOp")
      return { op: "max", args: input.inputs.map((child) => column(child, index)) };
    if ("input" in input && index < input.input.output.length) return column(input.input, index);
    return { op: "unknown" };
  };
  const expr = (e: ScalarExpr, input: Rel): Width => {
    if (!variable(e)) return zero;
    if (e.op === "param") return { op: "param", slot: e.slot };
    if (e.op === "column") return column(input, e.index);
    if (e.op === "literal")
      return e.value.data.layout === "varlen"
        ? { op: "bytes", bytes: constantNull(e) ? 0 : e.value.data.bytes.byteLength }
        : { op: "unknown" };
    if (e.op === "cast")
      return e.input.type.kind === e.type.kind ? expr(e.input, input) : { op: "unknown" };
    if (e.op === "case") {
      const args: Width[] = [];
      let otherwise = true;
      for (const b of e.branches) {
        const condition = constantBool(b.when);
        if (condition === false || condition === null) continue;
        args.push(expr(b.then, input));
        if (condition === true) {
          otherwise = false;
          break;
        }
      }
      if (otherwise && e.otherwise) args.push(expr(e.otherwise, input));
      return { op: "max", args };
    }
    if (e.op === "call") {
      if (e.fn === "nullif") return e.args[0] ? expr(e.args[0], input) : zero;
      if (e.fn === "coalesce") {
        const args: Width[] = [];
        for (const a of e.args) {
          const isNull = constantNull(a);
          if (isNull !== true) args.push(expr(a, input));
          if (isNull === false) break;
        }
        return { op: "max", args };
      }
      if (e.fn === "textcat") return { op: "sum", args: e.args.map((a) => expr(a, input)) };
    }
    return { op: "unknown" };
  };
  return {
    input: rel.input.output.map((_, i) => column(rel.input, i)),
    keys: rel.groupBy.map((i) => column(rel.input, i)),
    calls: rel.aggregates.map((call) => ({
      width: call.args[0] ? expr(call.args[0], rel.input) : zero,
      variable: call.args[0] ? variable(call.args[0]) : false,
      distinct: call.distinct,
    })),
  };
}
function bytes(width: Width, slots: readonly number[] | undefined): number | null {
  switch (width.op) {
    case "bytes":
      return width.bytes;
    case "param":
      return slots?.[width.slot] ?? null;
    case "unknown":
      return null;
    case "sum":
    case "max": {
      let n = 0;
      for (const a of width.args) {
        const b = bytes(a, slots);
        if (b === null) return null;
        n = width.op === "sum" ? n + b : Math.max(n, b);
      }
      return n;
    }
  }
}
/** Sort.prepareMerge: two one-row pages and the larger of a page or output arena.
 * Page framing/alignment, 2048-row chunk offsets/validity, operand and view metadata
 * are bounded by 32KiB per column +64KiB. Oversized staging also fits: its
 * remaining-growth overlap is <=2*arena <=2*row+arena (arena<2*row).
 * Unknown producers keep the existing complete source envelope, never guessed widths.
 */
function rowStep(row: readonly Width[], slots: readonly number[] | undefined): number | null {
  let payload = 0,
    arena = 0;
  for (const field of row) {
    const n = bytes(field, slots);
    if (n === null) return null;
    payload += n;
    arena += n > 0 ? 2 ** Math.ceil(Math.log2(n)) : 0;
  }
  const framing = 64 * 1024 + (row.length + 8) * 32768;
  return 2 * payload + Math.max(payload + 4096, arena) + framing;
}
export function aggregateLaneBytes(
  layout: AggregateProgressLayout,
  kind: "hybrid" | "external" | "restore",
  slot: number | undefined,
  baseline: number,
  slots: readonly number[] | undefined,
): number {
  const full = () =>
    Math.max(
      MAX_BLOB_BYTES,
      rowStep(layout.input, slots) ?? baseline,
      ...layout.calls.map((call) => rowStep([...layout.keys, call.width], slots) ?? baseline),
      rowStep(layout.keys, slots) ?? baseline,
    );
  // The wrapper owns fixed compiler/native buffers; all borrows go through named helpers.
  if (slot === undefined || (kind === "hybrid" && slot === 0)) return 0;
  if (slot === 1 || (slot === 0 && kind === "restore")) return full();
  if (slot === 0) return Math.max(MAX_BLOB_BYTES, rowStep(layout.keys, slots) ?? baseline);
  let index = 2;
  for (const call of layout.calls) {
    const count = 1 + Number(call.distinct) * (kind === "restore" ? 2 : 1);
    if (slot < index + count)
      return call.variable ? Math.max(MAX_BLOB_BYTES, rowStep([call.width], slots) ?? baseline) : 0;
    index += count;
  }
  // Optional top-N over finished groups retains their keys and variable results.
  return full();
}
