/** Bounded row layouts for the lanes that actually borrow aggregate progress.
 * Conditional widths are maxima over reachable outputs, not a union of parameter slots.
 * These are sizing facts only: no expression is evaluated and no caller buffer is read.
 */
import type { PhysicalType } from "../../types.js";
import type { Rel, ScalarExpr } from "../../ir/ir.js";
import { BATCH_TARGET_BYTES } from "../../vector/vector.js";
import { MAX_BLOB_BYTES } from "../../access/space.js";
import { sortMergeWorkspaceBytes } from "../sort.js";
import {
  externalCallMinimum,
  externalCallDistinct,
  externalCallStepBytes,
} from "./external-call-layout.js";

type Width =
  | { readonly op: "bytes"; readonly bytes: number }
  | { readonly op: "param"; readonly slot: number }
  | { readonly op: "sum" | "max"; readonly args: readonly Width[] }
  | { readonly op: "unknown" };
export interface AggregateProgressLayout {
  readonly simple: boolean;
  readonly input: readonly Width[];
  readonly keys: readonly Width[];
  readonly calls: ReadonlyArray<{
    readonly width: Width;
    readonly variable: boolean;
    readonly distinct: boolean;
    readonly minimum: number;
  }>;
}
/** These layouts have no parsed operands or wide coefficient/object arrays. */
export const simpleAggregateType = (type: PhysicalType): boolean =>
  [
    "text",
    "bytes",
    "bool",
    "int32",
    "int64",
    "float32",
    "float64",
    "date",
    "time",
    "timestamp",
  ].includes(type.kind);
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
    simple: rel.input.output.every((c) => simpleAggregateType(c.type)),
    input: rel.input.output.map((_, i) => column(rel.input, i)),
    keys: rel.groupBy.map((i) => column(rel.input, i)),
    calls: rel.aggregates.map((call) => ({
      width: call.args[0] ? expr(call.args[0], rel.input) : zero,
      variable: call.args[0] ? variable(call.args[0]) : false,
      distinct: externalCallDistinct(call),
      minimum: externalCallMinimum(call),
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
/** One merge step holds two decoded pages and one output row/page, not a complete
 * merge per logical key. The physical input layout packs proven aliases first. Finite
 * sort arenas round a field up only through their target; larger fields are exact.
 * The saved group key has an infinite target and always rounds up.
 * Framing covers chunk masks/offsets, alignment, scalar views and fixed codec scratch.
 * Unknown producers retain the complete source envelope, never guessed cell widths.
 */
function rowStep(
  row: readonly Width[],
  slots: readonly number[] | undefined,
  target = Infinity,
): number | null {
  let payload = 0,
    arena = 0;
  for (const field of row) {
    const n = bytes(field, slots);
    if (n === null) return null;
    payload += n;
    arena += n > target ? n : n > 0 ? 2 ** Math.ceil(Math.log2(n)) : 0;
  }
  return sortMergeWorkspaceBytes(payload, arena, (row.length + 8) * 32768);
}
export function aggregateLaneBytes(
  layout: AggregateProgressLayout,
  kind: "hybrid" | "external" | "restore",
  slot: number | undefined,
  baseline: number,
  slots: readonly number[] | undefined,
): number {
  const fixed = layout.simple && !layout.calls.some((call) => call.variable);
  const step = (row: readonly Width[]) =>
    rowStep(row, slots, fixed ? BATCH_TARGET_BYTES : Infinity);
  const full = () =>
    Math.max(
      MAX_BLOB_BYTES,
      step(layout.input) ?? baseline,
      ...layout.calls.map((call) => step([...layout.keys, call.width]) ?? baseline),
      step(layout.keys) ?? baseline,
    );
  // The wrapper owns fixed compiler/native buffers; all borrows go through named helpers.
  if (slot === undefined || (kind === "hybrid" && slot === 0)) return 0;
  if (slot === 1) {
    if (kind === "hybrid" && layout.simple) {
      let row = payload(layout.input, slots);
      const key = payload(layout.keys, slots);
      if (row !== null && key !== null) {
        for (const call of layout.calls) {
          const value = bytes(call.width, slots);
          if (value === null) return full();
          row = Math.max(row, key + value);
        }
        // One read row plus old/new canonical keys: each key is <=2R after
        // escaping. Frozen lookup ends before partition encoding; raw staging
        // needs at most three rows. This is ONE step, not five rows per key.
        return Math.max(full(), 5 * row + framing(layout.input.length));
      }
    }
    return full();
  }
  if (slot === 0 && kind === "restore" && fixed) {
    const row = payload(layout.input, slots),
      keys = keyArena(layout.keys, slots);
    return row === null || keys === null
      ? full()
      : Math.max(MAX_BLOB_BYTES, Math.max(2 * row, keys) + framing(layout.input.length));
  }
  if (slot === 0 && kind === "restore") return full();
  if (slot === 0) return Math.max(MAX_BLOB_BYTES, step(layout.keys) ?? baseline);
  let index = 2;
  for (const call of layout.calls) {
    const count = 1 + Number(call.distinct) * (kind === "restore" ? 2 : 1);
    if (slot < index + count) {
      if (!call.variable) return 0;
      const width = bytes(call.width, slots);
      // The state helper and its DISTINCT sorts have different complete steps.
      // Match construction's helper predicate and order before assigning either bound.
      // State peak includes its fixed pool: only the deficit belongs to this lane.
      return Math.max(
        MAX_BLOB_BYTES,
        slot === index
          ? width === null
            ? baseline
            : externalCallStepBytes(call.minimum, width) - call.minimum
          : (rowStep([call.width], slots) ?? baseline),
      );
    }
    index += count;
  }
  // Optional top-N over finished groups retains their keys and variable results.
  return full();
}

function framing(columns: number): number {
  return 65536 + (columns + 8) * 32768;
}
function payload(row: readonly Width[], slots: readonly number[] | undefined): number | null {
  let total = 0;
  for (const field of row) {
    const n = bytes(field, slots);
    if (n === null) return null;
    total += n;
  }
  return total;
}
function keyArena(
  row: readonly Width[],
  slots: readonly number[] | undefined,
  target = Infinity,
): number | null {
  let total = 0;
  for (const field of row) {
    const n = bytes(field, slots);
    if (n === null) return null;
    total += n > target ? n : n > 0 ? 2 ** Math.ceil(Math.log2(n)) : 0;
  }
  return total;
}
/** Feed and merge are successive steps. Feed holds a decoded/aligned record plus
 * the sort writer; merge holds two input pages + output + ONE saved group key.
 * Give helpers local limits but back their actual allocations with this single peak.
 * They must trim unused feed room before merge/capture; no credit is used by hash state.
 * Variable transition helpers retain their own state/codec bounds concurrently.
 */
export function aggregateSharedBytes(
  layout: AggregateProgressLayout,
  kind: "hybrid" | "external" | "restore",
  slots: readonly number[] | undefined,
  lanes: readonly number[],
): number | null {
  if (!layout.simple || kind !== "restore" || layout.calls.some((call) => call.variable))
    return null;
  const input = payload(layout.input, slots),
    keys = keyArena(layout.keys, slots);
  if (input === null || keys === null) return null;
  let row = input,
    output = keyArena(layout.input, slots, BATCH_TARGET_BYTES) ?? 0;
  for (const call of layout.calls) {
    const value = bytes(call.width, slots),
      key = payload(layout.keys, slots);
    if (value === null || key === null) return null;
    row = Math.max(row, key + value);
    output = Math.max(
      output,
      keyArena([...layout.keys, call.width], slots, BATCH_TARGET_BYTES) ?? 0,
    );
  }
  const calls = lanes.slice(2).reduce((n, b) => n + b, 0);
  // R=max record payload, A=finite output arena, K=one saved-key arena.
  // Feed needs <=3R; merge needs 2R+A+K. A>=R, so the latter covers both.
  const peak =
    Math.max(MAX_BLOB_BYTES, 2 * row + output + keys + 2 * framing(layout.input.length)) + calls;
  // Both bounds include native fixed minima separately. Keeping the smaller proven
  // bound avoids growing admission for a narrow input with many fixed-width fields.
  const separate = lanes.reduce((n, b) => n + b, 0);
  return peak < separate ? peak : null;
}
