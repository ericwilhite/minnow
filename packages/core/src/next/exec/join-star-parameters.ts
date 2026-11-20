/** Optional star admission reads only bounded plain TEXT bindings before domain proofs.
 * Unsupported/invalid inputs keep the ordinary binding and error-demand path. */
import type { PhysicalPlan } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { MaybePromise } from "../access/key-index.js";
import type { ParameterValue } from "./executor.js";
import { MemoryAdmissionError } from "../memory/governor.js";
import { prepareParameters, type ParameterBinding } from "./parameters.js";

export type StarParameterFootprint = Pick<ParameterBinding, "bytes" | "largestSlot" | "slotBytes">;
const EMPTY: StarParameterFootprint = { bytes: 0, largestSlot: 0, slotBytes: [] };
export function plainStarText(type: PhysicalType | undefined): boolean {
  return type?.kind === "text" && type.maxLength === undefined;
}
export function starParameterTypes(plan: PhysicalPlan): boolean {
  return (
    plan.parameters.length <= 4 &&
    plan.placeholderCount === plan.parameters.length &&
    plan.clockSlot === null &&
    plan.initPlans.length === 0 &&
    (plan.ftsParameters?.length ?? 0) === 0 &&
    (plan.ftsQueries?.length ?? 0) === 0 &&
    plan.hasRowLocking !== true &&
    plan.parameters.every(plainStarText)
  );
}
/** The shared binder computes exact input and per-slot progress bytes; no encoded
 * vectors or numeric workspaces are created here. No footprint authorizes a grant. */
export function starParameterFootprint(
  plan: PhysicalPlan,
  values: readonly ParameterValue[],
  nativeBinding: boolean,
  signal: AbortSignal,
  capacity: number,
): MaybePromise<StarParameterFootprint | null> {
  // Preserve the existing literal-only entry, including its normal bind errors.
  if (plan.parameters.length === 0) return EMPTY;
  if (
    nativeBinding ||
    !starParameterTypes(plan) ||
    values.length !== plan.placeholderCount ||
    values.some(
      (value) =>
        value !== null &&
        (typeof value !== "string" || value.length > 1024 || value.includes("\0")),
    )
  )
    return null;
  const failed = (error: unknown): null => {
    // The ordinary route reports its own complete input refusal in its old phase.
    if (error instanceof MemoryAdmissionError) return null;
    throw error;
  };
  try {
    const sized = prepareParameters(plan, values, signal, capacity);
    return sized instanceof Promise ? sized.catch(failed) : sized;
  } catch (error) {
    return failed(error);
  }
}
