/**
 * MetadataBudget.retry under a reclaiming governor (review 9). A refused attempt is retried after
 * its credit grows. Two laws keep that finite and bounded:
 * - the credit grown for an attempt stays held through it: the owner's reclaim keeps
 *   `fundedBytes` (the write engine's working minimum includes it), so a queued request's reclaim
 *   between the growth and the attempt's allocation cannot take it back;
 * - credit the attempt itself moves out before refusing (a page read's charge handed to the cache)
 *   is funded again on the next target, so the retry does not fall short by the same bytes forever.
 * Each history is bounded: at most three attempts, and the target never exceeds what the attempt
 * needs plus what it moved out.
 */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MetadataBudget, MetadataPressure } from "./budget.js";

const NEED = 1 << 20;
const READ = 4096;

it("holds funded credit through the attempt and refunds what the attempt moved out", async () => {
  const governor = new Governor(32 << 20);
  const grant = await governor.admit({ label: "metadata", minimumBytes: READ, spillable: true });
  const cache = { evictableBytes: 0, evict: () => 0 };
  const handle = governor.registerCache(cache);
  let peak = grant.held,
    attempts = 0,
    moved = 0;
  const budget = new MetadataBudget(
    grant,
    () => 0,
    governor.capBytes,
    async (wanted) => {
      if (grant.held < wanted) expect(grant.tryGrow(wanted - grant.held)).toBe(true);
      peak = Math.max(peak, grant.held);
    },
  );
  // The owner's cooperative reclaim, as the write engine's: idle credit above its allocations
  // and the funded floor goes back to the governor.
  const reclaim = () => {
    const keep = Math.max(grant.allocated, budget.fundedBytes);
    if (grant.held > keep) grant.release(grant.held - keep);
  };
  const need = (bytes: number) => {
    if (grant.held - grant.allocated < bytes)
      throw new MetadataPressure(bytes, grant.allocated + bytes);
    expect(grant.allocate(bytes)).toBe(true);
  };
  const work = async () => {
    attempts++;
    if (attempts > 6) throw new Error("retry did not converge");
    // The attempt awaits (a lock, a writer) before allocating; a queued request's reclaim runs.
    await Promise.resolve();
    reclaim();
    // A page read: charged to the grant, then handed to the cache with its bytes.
    need(READ);
    expect(handle.takeFrom(grant, READ)).toBe(true);
    moved += READ;
    need(NEED);
    return "done";
  };
  try {
    expect(await budget.retry(work, () => undefined)).toBe("done");
    expect(attempts, "attempts").toBe(3);
    expect(peak, "the retry target stayed within the need and what the attempt moved out").toBe(
      NEED + READ,
    );
    expect(budget.fundedBytes).toBe(0);
  } finally {
    grant.free(grant.allocated);
    grant.close();
    handle.release(moved);
    handle.unregister();
  }
  expect(governor.usedBytes).toBe(0);
});
