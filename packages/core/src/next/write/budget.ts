/**
 * Write-path reservation sharing (governor.ts rules 4, 5 and 7).
 * `free` returns allocations to a grant, not to the governor. A wide batch's conservative
 * estimate must not leave its unused high-water holdings blocking merges or resident metadata.
 */
import type { Grant } from "../memory/governor.js";
import type { ByteBudget } from "../storage/extents.js";

/** Return only idle, non-loan holdings, retaining the admitted working minimum. */
export function releaseIdle(grant: Grant, minimum = 0): number {
  const bytes = grant.held - Math.max(minimum, grant.allocated + grant.loaned);
  if (bytes <= 0) return 0;
  grant.release(bytes);
  return bytes;
}

/**
 * Resident metadata shares the same cap with the writer. Its synchronous allocations first
 * use their own grant/cache reclamation, then shed idle writer reservations and retry. Keeping
 * this budget at every metadata allocation site also covers directories and RunReader.open,
 * which do not call the engine's root-charge helper. Live buffers and loans are never shed.
 */
export class MetadataBudget implements ByteBudget {
  constructor(
    readonly grant: Grant,
    readonly reclaimWriter: () => number,
  ) {}

  allocate(bytes: number): boolean {
    if (this.grant.allocate(bytes)) return true;
    if (this.reclaimWriter() === 0) return false;
    return this.grant.allocate(bytes);
  }

  free(bytes: number): void {
    this.grant.free(bytes);
  }
}
