/** Immutable byte/array concatenation. Buffers are charged before allocation and
 * retired only after downstream readiness. Projection's ordinary denial path
 * supplies row splitting/checkpointing; no kernel waits while input is pinned. */
import { samePhysicalType, type PhysicalType } from "../../types.js";
import type { Vector } from "../../vector/vector.js";
import type { ByteCharge } from "../../vector/varlen.js";
import {
  ArrayElementBytes,
  arrayFunctionBounds,
  arrayElementScratch,
  arrayHeader,
  nativeArray,
  valueBytes,
  type NativeArray,
} from "../array-data.js";
import { MAX_ARRAY_DIMENSIONS, arrayElementTag } from "../../vector/array-value.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import { isValidSlot, type Compiled, type OutputArena } from "./vectors.js";
import { compileJson, type ByteRowWriter } from "./json.js";
interface Part {
  readonly count: number;
  readonly nulls: Uint8Array | null;
  readonly payload: Uint8Array;
  readonly scalar?: ArrayElementBytes;
}
interface ArrayPlan {
  readonly dimensions: readonly number[];
  readonly lowerBounds: readonly number[];
  readonly parts: readonly Part[];
}
const fail = (state = "2202E"): never => {
  throw new ExecutionError(
    "cannot concatenate incompatible arrays",
    "MINNOW_ARRAY_DIMENSIONS",
    state,
  );
};
function arrayPlan(
  fn: string,
  type: Extract<PhysicalType, { kind: "array" }>,
  left: Vector,
  right: Vector,
  row: number,
  charge: ByteCharge,
): ArrayPlan | null {
  const array = (v: Vector): NativeArray | null =>
    isValidSlot(v, row) ? nativeArray(type.element, valueBytes(v, row)) : null;
  let a: NativeArray | null, b: NativeArray | null;
  if (fn === "array_cat") {
    a = array(left);
    b = array(right);
    if (!a && !b) return null;
    if (!a?.count) {
      const only = b ?? a;
      return only
        ? { dimensions: only.dimensions, lowerBounds: only.lowerBounds, parts: [only] as Part[] }
        : null;
    }
    if (!b?.count)
      return { dimensions: a.dimensions, lowerBounds: a.lowerBounds, parts: [a] as Part[] };
    const ad = a.dimensions.length,
      bd = b.dimensions.length;
    const dimensions = [...(ad >= bd ? a.dimensions : b.dimensions)],
      lowerBounds = [...(ad >= bd ? a.lowerBounds : b.lowerBounds)];
    if (Math.abs(ad - bd) > 1) fail();
    if (ad === bd) {
      for (let d = 1; d < ad; d++)
        if (a.dimensions[d] !== b.dimensions[d] || a.lowerBounds[d] !== b.lowerBounds[d]) fail();
      dimensions[0] = (dimensions[0] ?? 0) + (b.dimensions[0] ?? 0);
    } else {
      const high = ad > bd ? a : b,
        low = ad > bd ? b : a;
      for (let d = 0; d < low.dimensions.length; d++)
        if (
          low.dimensions[d] !== high.dimensions[d + 1] ||
          low.lowerBounds[d] !== high.lowerBounds[d + 1]
        )
          fail();
      dimensions[0] = (dimensions[0] ?? 0) + 1;
    }
    arrayFunctionBounds(dimensions, lowerBounds);
    return { dimensions, lowerBounds, parts: [a, b] as Part[] };
  }
  const append = fn === "array_append",
    v = append ? right : left;
  a = array(append ? left : right);
  if (a && a.dimensions.length > 1) fail("22000");
  if (!append && a?.count && a.lowerBounds[0] === -0x80000000) fail("22003");
  const lower = a?.lowerBounds[0] ?? 1,
    count = a?.count ?? 0;
  if (append && lower + count > 0x7fffffff) fail("22003");
  arrayFunctionBounds([count + 1], [append || !count ? lower : lower - 1]);
  // PostgreSQL's prepend setter extends at lower-1, then restores the original
  // lower bound. This can produce an upper bound of INT_MAX; append cannot.

  const scalar = new ArrayElementBytes(v, row, charge);
  const part: Part = {
    count: 1,
    nulls: isValidSlot(v, row) ? null : Uint8Array.of(1),
    payload: new Uint8Array(0),
    scalar,
  };
  const parts = a?.count ? (append ? [a, part] : [part, a]) : [part];
  return { dimensions: [(a?.count ?? 0) + 1], lowerBounds: [a?.lowerBounds[0] ?? 1], parts };
}
export function compileConcat(
  fn: string,
  type: PhysicalType,
  args: readonly Compiled[],
  arena: OutputArena,
): Compiled {
  const [left, right] = args;
  if (!left || !right || args.length !== 2)
    throw new TypeError("Concatenation takes two arguments");
  if (fn !== "textcat" && type.kind !== "array")
    throw new UnsupportedPlanError("array concatenation requires array operands", {
      construct: "function",
    });
  if (type.kind === "array") {
    try {
      const tag = arrayElementTag(type.element);
      for (const arg of args) {
        const element = arg.type.kind === "array" ? arg.type.element : arg.type;
        if (
          arrayElementTag(element) !== tag ||
          (element.kind === "enum" && !samePhysicalType(element, type.element))
        )
          throw new Error("element cast required");
      }
    } catch {
      throw new UnsupportedPlanError("array concatenation element cast or element type", {
        construct: "function",
      });
    }
  }
  // Counting and copying share the existing expression runner. No synchronous
  // bitmap scan or full-width copy can bypass the worker task clock. Volatile
  // kernels remain refused, so the sizing pass cannot repeat observable effects.
  const write: ByteRowWriter = function* (vectors, row, output, memory) {
    const [a, b] = vectors;
    if (!a || !b) throw new TypeError("Missing concatenation operand");
    if (fn === "textcat") {
      if (!isValidSlot(a, row) || !isValidSlot(b, row)) return false;
      yield* output.copy(valueBytes(a, row));
      yield* output.copy(valueBytes(b, row));
      return true;
    }
    if (type.kind !== "array") throw new TypeError("Array result");
    const scratch =
      fn === "array_cat" ? 0 : arrayElementScratch(fn === "array_append" ? b : a, row);
    yield* memory.take(scratch);
    // This scalar writer is entirely inside the already-admitted row scratch.
    // The outer finally owns that charge even if bounds or encoding fail.
    const charge: ByteCharge = {
      allocate: (n) => n <= scratch,
      free: () => undefined,
    };
    let plan: ArrayPlan | null = null;
    try {
      plan = arrayPlan(fn, type, a, b, row, charge);
      if (!plan) return false;
      if (plan.dimensions.length > MAX_ARRAY_DIMENSIONS) fail();
      const n = plan.parts.reduce((s, x) => s + x.count, 0),
        nulls = plan.parts.some((x) => x.nulls !== null),
        header = arrayHeader(type.element, plan.dimensions, plan.lowerBounds, nulls);
      yield* output.copy(header);
      const nb = nulls ? Math.ceil(n / 8) : 0;
      if (output.bytes === null) output.position += nb;
      else {
        // Repack only the NULL bitmap; encoded element payloads stay borrowed.
        // A byte needs at most eight source-bit probes, independent of value width.
        let part = 0,
          position = 0;
        for (let at = 0; at < nb; at++) {
          let byte = 0;
          for (let bit = 0; bit < 8 && at * 8 + bit < n; bit++) {
            let x = plan.parts[part];
            while (position === x?.count) {
              x = plan.parts[++part];
              position = 0;
            }
            if (x?.nulls && (x.nulls[position >>> 3] ?? 0) & (1 << (position & 7)))
              byte |= 1 << bit;
            position++;
          }
          output.put(byte);
          if ((at & 1023) === 0 && output.slice.due()) yield;
        }
      }
      for (const x of plan.parts)
        for (const bytes of x.scalar?.parts ?? [x.payload]) yield* output.copy(bytes);
      return true;
    } finally {
      for (const x of plan?.parts ?? []) x.scalar?.close();
      memory.free(scratch);
    }
  };
  return compileJson(type, fn, args, arena, write);
}
