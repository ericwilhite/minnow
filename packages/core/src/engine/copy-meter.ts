/**
 * Optional metering of syntax-tree copies, for the private 1.0 rebuild's admitted trigger
 * preparation (next/owner/native-command.ts). The released engine never installs a meter; every
 * hook below returns at once for it, and its parse and compile give the same results.
 *
 * While a meter is installed (synchronously, around one parse/compile/lowering call), each copy
 * helper charges the node it is about to copy before allocating the copy: `cloneTreeValue` per
 * node, and whole-subtree helpers by `subtreeBytes`. Expression copy sites whose copies are then
 * lowered into retained IR (simple CASE, BETWEEN, row predicates, statistical aggregates, alias,
 * ordinal and merged-column references, grouping sets) run under `COPY_IR_FACTOR`, which covers
 * the copy plus the IR it becomes; so does every other copy, unless the helper marks it as a
 * whole-plan pass copy (`asTransientCopy`), which replaces the plan it copied. A meter may
 * throw to abort the call; callers then discard all of it and retry under a larger reservation.
 */
export interface CopyMeter {
  /** Charge `bytes` before they are allocated; throw to abort when they do not fit. */
  charge(bytes: number): void;
}

/** One copied object or array: header plus a slot per field. */
const NODE_BYTES = 64;
const SLOT_BYTES = 8;
/** A copied expression and the retained IR lowered from it, per source byte. Empirical, not
 * proved: calibrated by testing/native-preparation-bound.test.ts, whose generated bodies retain
 * at most about 0.6 of their metered charge across every copy site, and which fails loudly when
 * a site needs more. */
export const COPY_IR_FACTOR = 8;

let meter: CopyMeter | null = null;
let factor = 1;

/** Runs `run` with `next` installed; restores the previous meter on every exit. */
export function withCopyMeter<T>(next: CopyMeter, run: () => T): T {
  const previous = meter,
    previousFactor = factor;
  meter = next;
  // Any copy may be lowered again; only whole-plan pass copies are marked transient.
  factor = COPY_IR_FACTOR;
  try {
    return run();
  } finally {
    meter = previous;
    factor = previousFactor;
  }
}

export function copyMeterActive(): boolean {
  return meter !== null;
}

/** Runs a whole-plan pass copy, which replaces the plan it copies, at its own size only. */
export function asTransientCopy<T>(run: () => T): T {
  if (meter === null) return run();
  const previous = factor;
  factor = 1;
  try {
    return run();
  } finally {
    factor = previous;
  }
}

/** Runs one copy whose result is lowered into retained IR, at `COPY_IR_FACTOR`. */
export function asLoweredCopy<T>(run: () => T): T {
  if (meter === null) return run();
  const previous = factor;
  factor = COPY_IR_FACTOR;
  try {
    return run();
  } finally {
    factor = previous;
  }
}

/** Charges one node about to be copied: its header, field slots and string contents. */
export function chargeNode(value: object): void {
  if (meter === null) return;
  let bytes = NODE_BYTES;
  if (Array.isArray(value)) bytes += SLOT_BYTES * value.length;
  else
    for (const key of Object.keys(value)) {
      const field = (value as Record<string, unknown>)[key];
      bytes += SLOT_BYTES + (typeof field === "string" ? 24 + 2 * field.length : 0);
    }
  meter.charge(bytes * factor);
}

/** Charges a whole subtree about to be copied by a helper that does not visit nodes itself. */
export function chargeSubtree(value: unknown, scale = 1): void {
  if (meter === null) return;
  meter.charge(subtreeBytes(value) * factor * scale);
}

/** Charges bytes a caller measured exactly (for example, one more lowered copy of a default). */
export function chargeExactBytes(bytes: number): void {
  if (meter === null) return;
  meter.charge(bytes);
}

/** Charges arrays of references about to be built: one array of `outer` slots holding arrays of
 * the given lengths (for example, grouping sets). Exact; the referenced nodes are not copied. */
export function chargeSlotArrays(lengths: Iterable<number>): void {
  if (meter === null) return;
  let bytes = NODE_BYTES,
    outer = 0;
  for (const length of lengths) {
    bytes += NODE_BYTES + SLOT_BYTES * length;
    outer++;
  }
  meter.charge(bytes + SLOT_BYTES * outer);
}

/** The size of a tree as `chargeNode` counts it, iteratively (no recursion depth limit). */
export function subtreeBytes(value: unknown): number {
  let bytes = 0;
  const pending: unknown[] = [value];
  while (pending.length > 0) {
    const item = pending.pop();
    if (typeof item === "string") bytes += 24 + 2 * item.length;
    else if (typeof item === "bigint") bytes += 16 + Math.ceil(item.toString(16).length / 2);
    else if (typeof item === "number" || typeof item === "boolean") bytes += SLOT_BYTES;
    else if (typeof item === "object" && item !== null) {
      bytes += NODE_BYTES;
      const fields = Array.isArray(item)
        ? (item as unknown[])
        : Object.values(item as Record<string, unknown>);
      bytes += SLOT_BYTES * fields.length;
      for (const field of fields) pending.push(field);
    }
  }
  return bytes;
}
