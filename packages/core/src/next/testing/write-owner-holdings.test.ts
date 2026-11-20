/** A native owner's permanent holdings belong to write growth's complete minimum: the engine
 * counts its non-evictable storage floor, and the owner registers its scratch control. */
import { expect, it, onTestFinished, vi } from "vitest";
import { openNativeOwner } from "../owner/native-boot.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import { FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import { WriteCredits } from "../write/credits.js";
import { WritePathImpl } from "../write/engine.js";

it("binds the storage floor and the owner's scratch control into write growth", async () => {
  const holdings: Array<() => number> = [];
  const owners: Array<() => number> = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
  const bind = WriteCredits.prototype.bindOwnerHoldings;
  const bound = vi.spyOn(WriteCredits.prototype, "bindOwnerHoldings").mockImplementation(function (
    this: WriteCredits,
    source,
  ) {
    holdings.push(source);
    bind.call(this, source);
  });
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
  const retain = WritePathImpl.prototype.retainOwnerHoldings;
  const retained = vi
    .spyOn(WritePathImpl.prototype, "retainOwnerHoldings")
    .mockImplementation(function (this: WritePathImpl, source) {
      owners.push(source);
      return retain.call(this, source);
    });
  onTestFinished(() => {
    bound.mockRestore();
    retained.mockRestore();
  });
  const owner = await openNativeOwner({
    backend: new SimBackend(new SimDisk()),
    memoryLimitBytes: 64 << 20,
  });
  try {
    expect(owners.map((source) => source())).toEqual([SCRATCH_CONTROL_BYTES]);
    const total = holdings.at(-1)?.() ?? 0;
    // Fixed slabs, the space's retained storage and the registered scratch control.
    expect(total).toBeGreaterThan(FIXED_CACHE_BYTES + SCRATCH_CONTROL_BYTES);
  } finally {
    await owner.close();
  }
});
