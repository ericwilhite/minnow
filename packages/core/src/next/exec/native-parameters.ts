/** Borrow typed leading parameters inside an admitted, non-ending native producer.
 * The caller owns their scalar buffers through producer retirement. No JS value coercion,
 * payload copy or independent governor admission occurs here. */
import type { PhysicalPlan } from "../ir/ir.js";
import { dataBytes, type FlatVector, type Vector } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import { samePhysicalType } from "../types.js";
import { int64At } from "../vector/int64.js";
import { isValid } from "../vector/validity.js";
import { flatBytes } from "./agg/values.js";
import type { BoundParameters } from "./executor.js";
import type { ParameterBinding } from "./parameters.js";
import { clockParameter, nullParameter } from "./parameters.js";
import { ParameterBindError } from "./errors.js";

export function nativeParameterMinimumBytes(plan: PhysicalPlan): number {
  const bytes = 4096 + 512 * plan.parameters.length;
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native parameter metadata too large");
  return bytes;
}
function invalid(message: string, slot: number | null = null): never {
  throw new ParameterBindError(`Native parameters: ${message}`, slot);
}
function leadingSlot(value: Vector | undefined, slot: number): FlatVector {
  if (value?.form !== "flat" || value.length !== 1) invalid("leading slot shape/type", slot);
  return value;
}
/** Preparation arrays/borrowed references use the root metadata account. bind() charges only
 * newly owned clock/NULL buffers to its query share. Payloads already owned by the root do not
 * enter logicalBytes a second time; slotBytes still exposes their exact operand widths. */
export class NativeParameterBinding implements ParameterBinding {
  readonly bytes: number;
  readonly logicalBytes: number;
  readonly largestSlot: number;
  readonly slotBytes: readonly number[];
  readonly placeholderCount: number;
  #plan: PhysicalPlan | null;
  #leading: FlatVector[] = [];
  #slots: FlatVector[] | null = null;
  #charge: ByteCharge | null;
  readonly #metadata: number;
  constructor(
    plan: PhysicalPlan,
    provided: BoundParameters,
    charge: ByteCharge,
    readonly signal: AbortSignal,
  ) {
    signal.throwIfAborted();
    if (
      !Number.isSafeInteger(plan.placeholderCount) ||
      plan.placeholderCount < 0 ||
      plan.placeholderCount > plan.parameters.length ||
      provided.slots.length !== plan.placeholderCount
    )
      invalid("placeholder count");
    if (
      plan.clockSlot !== null &&
      (!Number.isInteger(plan.clockSlot) ||
        plan.clockSlot < plan.placeholderCount ||
        plan.parameters[plan.clockSlot]?.kind !== "timestamp")
    )
      invalid("statement clock slot");
    this.#metadata = nativeParameterMinimumBytes(plan);
    if (!charge.allocate(this.#metadata))
      throw new RangeError("Native parameter metadata must be admitted");
    this.#plan = plan;
    this.#charge = charge;
    this.placeholderCount = plan.placeholderCount;
    let bytes = 0,
      largest = 0;
    const sizes: number[] = [];
    try {
      for (const [i, type] of plan.parameters.entries()) {
        signal.throwIfAborted();
        const value = provided.slots[i];
        if (i < plan.placeholderCount) {
          const native = leadingSlot(value, i);
          if (!samePhysicalType(type, native.type)) invalid("leading slot shape/type", i);
          this.#leading.push(native);
          const size = flatBytes(native);
          sizes.push(size);
          largest = Math.max(largest, size);
        } else {
          const mapping = plan.ftsParameters?.find((mapping) => mapping.slot === i);
          if (mapping) {
            const source = provided.slots[mapping.source];
            if (
              type.kind !== "text" ||
              !Number.isInteger(mapping.source) ||
              mapping.source < 0 ||
              mapping.source >= plan.placeholderCount ||
              !source ||
              !(source.type.kind === "text" || source.type.kind === "enum")
            )
              invalid("FTS copy source", i);
            const size = flatBytes(source);
            sizes.push(size);
            largest = Math.max(largest, size);
          } else {
            const size = dataBytes(type, 1) + 4;
            bytes += size;
            sizes.push(size);
            largest = Math.max(largest, size);
          }
        }
      }
      for (const mapping of plan.ftsParameters ?? [])
        if (
          !Number.isInteger(mapping.slot) ||
          mapping.slot < plan.placeholderCount ||
          mapping.slot >= plan.parameters.length ||
          mapping.slot === plan.clockSlot ||
          (plan.ftsParameters?.filter((other) => other.slot === mapping.slot).length ?? 0) !== 1
        )
          invalid("FTS copy slot", mapping.slot);
      this.bytes = this.logicalBytes = bytes;
      this.largestSlot = largest;
      this.slotBytes = sizes;
    } catch (error) {
      this.#leading = [];
      this.#plan = null;
      this.#charge = null;
      charge.free(this.#metadata);
      throw error;
    }
  }
  matches(plan: PhysicalPlan): boolean {
    return this.#plan === plan;
  }
  private get plan(): PhysicalPlan {
    if (!this.#plan) throw new Error("Native parameter binding retired");
    this.signal.throwIfAborted();
    return this.#plan;
  }
  bind(charge: ByteCharge, statementTime = 0n): BoundParameters {
    const plan = this.plan;
    if (this.#slots) throw new Error("Native parameters already bound");
    if (!charge.allocate(this.bytes))
      throw new RangeError("Native parameter buffers must be admitted");
    const slots: FlatVector[] = [];
    try {
      for (const [i, type] of plan.parameters.entries()) {
        this.signal.throwIfAborted();
        if (i < plan.placeholderCount) {
          const leading = this.#leading[i];
          if (!leading) invalid("missing leading slot", i);
          slots.push(leading);
        } else if (i === plan.clockSlot) slots.push(clockParameter(type, statementTime));
        else {
          const mapping = plan.ftsParameters?.find((mapping) => mapping.slot === i);
          if (mapping) {
            const source = this.#leading[mapping.source];
            if (!source) invalid("missing FTS source", i);
            slots.push({ ...source, type });
          } else slots.push(nullParameter(type));
        }
      }
      this.#slots = slots;
      return { slots };
    } catch (error) {
      slots.length = 0;
      charge.free(this.bytes);
      throw error;
    }
  }
  /** Bounds are inspected before vector binding/reservation, directly from native scalars. */
  boundParameter(slot: number): number | bigint | null {
    const plan = this.plan,
      type = plan.parameters[slot],
      value = this.#leading[slot];
    if (!Number.isInteger(slot) || slot < 0 || slot >= plan.placeholderCount || !value)
      invalid("integer bound slot", slot);
    if (!isValid(value.validity, 0)) return null;
    if (type?.kind === "int32" && value.data.layout === "i32")
      return value.data.values[0] ?? invalid("integer lane", slot);
    if (type?.kind === "int64" && value.data.layout === "int64")
      return int64At(value.data.values, 0);
    invalid("integer bound type", slot);
  }
  /** Runtime operators/ParamNode caches must be fenced first. Caller-owned buffers survive;
   * the caller closes its parameter scope only after the producer has returned. */
  retire(): void {
    if (!this.#charge) return;
    if (this.#slots) this.#slots.length = 0;
    this.#slots = null;
    this.#leading = [];
    this.#plan = null;
    this.#charge.free(this.#metadata);
    this.#charge = null;
  }
}
