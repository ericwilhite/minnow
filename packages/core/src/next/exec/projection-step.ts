/** Pure sizing: never evaluates a predicate or function. Unknown producers are not certified. */
import { layoutOf } from "../types.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { Batch, Vector } from "../vector/vector.js";
import type { ParameterSlots } from "./kernels/compile.js";
import { isValidSlot, flatOf, slotIndex } from "./kernels/vectors.js";

type Bound = (batch: Batch, start: number, count: number) => number;
const zero: Bound = () => 0;
const variable = (e: ScalarExpr) => e.type.kind === "text" || e.type.kind === "bytes";
function width(vector: Vector, batch: Batch, start: number, count: number): number {
  let max = 0;
  for (let k = start; k < start + count; k++) {
    const row = batch.selection?.rows[k] ?? k;
    if (!isValidSlot(vector, row)) continue;
    const flat = flatOf(vector),
      slot = slotIndex(vector, row);
    if (flat.data.layout !== "varlen") throw new Error("Projection width is not varlen");
    max = Math.max(max, (flat.data.offsets[slot + 1] ?? 0) - (flat.data.offsets[slot] ?? 0));
  }
  return max;
}
/** Every varlen producer must be a selector or borrowed leaf. At one active row all
 * selectors borrow their sole surviving part, including nested selectors; null needs zero
 * bytes. Thus even a pinned competing execution cannot prevent this step from progressing.
 * Multi-row bounds cover every intermediate and old/new resize overlap, not just output.
 */
export function projectionStep(exprs: readonly ScalarExpr[], params: ParameterSlots): Bound | null {
  let selectors = 0;
  const materialized: Bound[] = [];
  const known = (e: ScalarExpr) =>
    e.op === "literal" ? e.value : e.op === "param" ? params.slots[e.slot] : undefined;
  const visit = (e: ScalarExpr): Bound | null => {
    if (layoutOf(e.type) === "varlen" && !variable(e)) return null;
    // These selectors may lazily promote a full fixed-width/coefficient buffer.
    // Their separate fixed-tier admission is not proved by a varlen-payload bound.
    if (
      (e.op === "case" || (e.op === "call" && ["coalesce", "nullif"].includes(e.fn))) &&
      ["int64", "decimal", "interval"].includes(layoutOf(e.type))
    )
      return null;
    let children: readonly ScalarExpr[];
    switch (e.op) {
      case "column":
        return variable(e)
          ? (batch, start, count) => {
              const vector = batch.columns[e.index];
              if (!vector) throw new Error("Missing projection input");
              return width(vector, batch, start, count);
            }
          : zero;
      case "param":
        return variable(e)
          ? () => {
              const value = params.slots[e.slot];
              if (!value) throw new Error("Missing projection parameter");
              if (!isValidSlot(value, 0)) return 0;
              if (value.data.layout !== "varlen")
                throw new Error("Projection parameter is not varlen");
              return (value.data.offsets[1] ?? 0) - (value.data.offsets[0] ?? 0);
            }
          : zero;
      case "literal":
        return variable(e)
          ? () =>
              isValidSlot(e.value, 0) && e.value.data.layout === "varlen"
                ? (e.value.data.offsets[1] ?? 0) - (e.value.data.offsets[0] ?? 0)
                : 0
          : zero;
      case "case": {
        const choices: Bound[] = [];
        let otherwise = true;
        for (const branch of e.branches) {
          if (visit(branch.when) === null) return null;
          const when = known(branch.when);
          if (
            when &&
            (!isValidSlot(when, 0) || (when.data.layout === "bool" && when.data.values[0] === 0))
          )
            continue;
          const value = visit(branch.then);
          if (value === null) return null;
          choices.push(value);
          if (when?.data.layout === "bool" && when.data.values[0] === 1) {
            otherwise = false;
            break;
          }
        }
        if (otherwise && e.otherwise) {
          const value = visit(e.otherwise);
          if (value === null) return null;
          choices.push(value);
        }
        return selector(e, choices);
      }
      case "call": {
        if (e.fn === "coalesce") {
          const choices: Bound[] = [];
          for (const arg of e.args) {
            const value = known(arg);
            if (value && !isValidSlot(value, 0)) continue;
            const bound = visit(arg);
            if (bound === null) return null;
            choices.push(bound);
            if (value) break;
          }
          return selector(e, choices);
        }
        children = e.args;
        break;
      }
      case "cast":
        children = [e.input];
        break;
      case "compare":
      case "distinctFrom":
        children = [e.left, e.right];
        break;
      case "and":
      case "or":
        children = e.args;
        break;
      case "not":
      case "isNull":
        children = [e.arg];
        break;
      case "like":
        children = [e.arg, e.pattern];
        break;
      case "inList":
        children = [e.arg, ...e.list];
        break;
    }
    const bounds: Bound[] = [];
    for (const child of children) {
      const bound = visit(child);
      if (bound === null) return null;
      bounds.push(bound);
    }
    if (!variable(e)) return zero;
    if (e.op === "cast") return e.type.kind === e.input.type.kind ? (bounds[0] ?? zero) : null;
    if (e.op !== "call" || e.fn !== "nullif") return null;
    return selector(e, bounds.slice(0, 1));
  };
  const selector = (e: ScalarExpr, choices: readonly Bound[]): Bound => {
    if (!variable(e)) return zero;
    selectors++;
    const bound: Bound = (batch, start, count) => {
      let max = 0;
      for (const choice of choices) max = Math.max(max, choice(batch, start, count));
      return max;
    };
    materialized.push(bound);
    return bound;
  };
  if (exprs.some((e) => visit(e) === null) || selectors === 0) return null;
  return (batch, start, count) => {
    if (count === 0) return 0;
    // No credit is reserved here. Kernels run synchronously immediately after this check,
    // so another execution cannot consume ordinary headroom between preflight and take().
    // If credit is insufficient the owner splits BEFORE evaluating any expression.
    let total = 0;
    for (const bound of materialized) total += 2 * count * bound(batch, start, count);
    return total;
  };
}
