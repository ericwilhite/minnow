/** Complete minimum admission through the governor's native group and idle-credit APIs. */
import type { Grant, MemoryGovernor, NativeGrantGroupOptions } from "./governor.js";
type ByteBudget = Pick<Grant, "allocate" | "free">;

/** R6 keeps an uncontended admission synchronous. No buffer exists during this split.
 * If any synchronous part cannot be obtained, return EVERYTHING before the FIFO group wait.
 * This transfers only idle ordinary credit; it never moves a lease or live allocation. */
export function admitWorkingSet(
  governor: MemoryGovernor,
  options: NativeGrantGroupOptions,
): readonly Grant[] | Promise<readonly Grant[]> {
  options.signal?.throwIfAborted();
  const parts = options.grants.map((part) => ({ ...part }));
  const total = parts.reduce((sum, part) => sum + part.minimumBytes, 0);
  const copied = { ...options, grants: parts };
  // Leave malformed group validation to its authority, the native group operation.
  if (
    !Number.isSafeInteger(total) ||
    parts.some((part) => !Number.isSafeInteger(part.minimumBytes) || part.minimumBytes < 0) ||
    new Set(parts.map((part) => part.label)).size !== parts.length
  )
    return governor.admitGroup(copied);
  if (options.includesProgress && total > governor.capBytes - governor.reserveBytes)
    return governor.admitGroup(copied);
  const whole = governor.tryAdmit({
    label: options.label,
    minimumBytes: total,
    spillable: false,
  });
  if (whole === null) return governor.admitGroup(copied);
  const installed: Grant[] = [];
  let complete = true;
  try {
    for (const part of parts) {
      // The synchronous total fits ordinary capacity, so its zero-sized parts fit too.
      const grant = governor.tryAdmit({ ...part, minimumBytes: 0 });
      if (grant === null) {
        complete = false;
        break;
      }
      installed.push(grant);
      if (!governor.transferCredit(whole, grant, part.minimumBytes)) {
        complete = false;
        break;
      }
    }
    if (complete && installed.length === parts.length && whole.held === 0) {
      options.signal?.throwIfAborted();
      return installed;
    }
    for (const grant of installed) grant.close();
    whole.close();
    return governor.admitGroup(copied);
  } catch (error) {
    for (const grant of installed) grant.close();
    throw error;
  } finally {
    whole.close();
  }
}

export class MinimumBudget implements ByteBudget {
  allocated = 0;
  constructor(
    readonly owner: ByteBudget,
    readonly capacity: number,
  ) {}
  allocate(bytes: number): boolean {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("nonnegative byte count required");
    if (this.allocated + bytes > this.capacity || !this.owner.allocate(bytes)) return false;
    this.allocated += bytes;
    return true;
  }
  free(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.allocated)
      throw new RangeError("minimum allocation underflow");
    this.owner.free(bytes);
    this.allocated -= bytes;
  }
}

export class WorkingSet {
  private constructor(readonly parts: readonly Grant[]) {}
  static async admit(
    governor: MemoryGovernor,
    sizes: readonly number[],
    signal?: AbortSignal,
  ): Promise<WorkingSet> {
    const grants = await admitWorkingSet(governor, {
      label: "execution/whole-minimum",
      includesProgress: true,
      grants: sizes.map((minimumBytes, index) => ({
        label: `part#${String(index)}`,
        minimumBytes,
        spillable: false,
      })),
      ...(signal ? { signal } : {}),
    });
    return new WorkingSet(grants);
  }
  close(): void {
    for (const grant of this.parts) grant.close();
  }
}
