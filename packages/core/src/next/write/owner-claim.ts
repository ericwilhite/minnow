/** One write engine per memory governor.
 *
 * Memtable rows leave memory only through a metadata publication, so write growth counts them
 * as required inputs instead of spilling them (credits.ts, memory/README.md). That is
 * deadlock-free only while a governor serves a single write engine: two engines on one governor
 * could each keep rows the other's complete step needs. Production opens one governor and one
 * write engine per owner (owner/native-boot.ts); this registry makes that invariant enforced.
 */
import type { MemoryGovernor } from "../memory/governor.js";

/** A second write engine was attached to a governor that already serves one. */
export class WriteOwnerConflictError extends Error {
  readonly code = "MINNOW_WRITE_OWNER_CONFLICT";
  constructor() {
    super("A memory governor serves one write engine; close the existing engine first");
    this.name = "WriteOwnerConflictError";
  }
}

const owners = new WeakMap<MemoryGovernor, object>();

/** Claims `governor` for one engine. The returned release is idempotent. */
export function claimWriteOwner(governor: MemoryGovernor): {
  readonly token: object;
  release(): void;
} {
  if (owners.has(governor)) throw new WriteOwnerConflictError();
  const token = {};
  owners.set(governor, token);
  return {
    token,
    release() {
      if (owners.get(governor) === token) owners.delete(governor);
    },
  };
}
