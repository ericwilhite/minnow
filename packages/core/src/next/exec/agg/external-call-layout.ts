/** Pure layout facts shared by external helper construction and atomic admission. */
import type { AggregateCall } from "../../ir/ir.js";

export const numericCall = (call: AggregateCall) =>
  call.type.kind === "decimal" || call.args[0]?.type.kind === "decimal";
export const externalCallMinimum = (call: AggregateCall): number =>
  numericCall(call) ? 640 * 1024 : 128 * 1024;
export function externalCallDistinct(call: AggregateCall): boolean {
  if (!call.distinct || call.args.length === 0) return false;
  if (call.fn === "bool_and" || call.fn === "bool_or") return false;
  // Byte-equal extrema have the same display. Float signed zero and decimal scales
  // have distinct representations: retain DISTINCT's first representative there.
  if (call.fn === "min" || call.fn === "max")
    return !["text", "bytes", "uuid", "enum", "bool"].includes(call.args[0]?.type.kind ?? "");
  return true;
}
/** A transition/capture step holds retained value, old/new resize and output overlap.
 * DISTINCT's value/ordinal SORT helpers use the merge-row recipe instead. This is a
 * total pool peak (including the fixed minimum), not one reserve per input/key copy.
 */
export const externalCallStepBytes = (minimum: number, largestWidth: number): number =>
  Math.max(minimum, 32768 + 4 * largestWidth);
