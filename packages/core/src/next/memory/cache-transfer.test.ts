/** Governor rules 1/4/5: an ownership transfer keeps the charge and never captures a loan. */
import { afterAll, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { CACHE_TRANSFER_REACH, createReach } from "../../testing/reach.js";
import { Governor } from "./governor.js";

const MiB = 1 << 20;
const reach = createReach("atomic cache transfer", CACHE_TRANSFER_REACH);
for (const seed of seedsFor("cache-transfer", [1521, 1522, 1523, 1524]))
  it(`moves a charge at capacity, preserving queued work and loan repayment (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      governor = new Governor(4 * MiB);
    const ordinary = governor.capBytes - governor.reserveBytes;
    const source = await governor.admit({
      label: "startup",
      minimumBytes: ordinary,
      spillable: false,
    });
    const bytes = 64 * (1 + Math.floor(random() * 8000));
    expect(source.allocate(bytes)).toBe(true);
    let served = false;
    const waiting = governor
      .admit({ label: "queued", minimumBytes: 4096, spillable: false })
      .then((grant) => {
        served = true;
        return grant;
      });
    const cache = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
    // The old double-charge design cannot fit, even without a waiter.
    expect(cache.charge(bytes)).toBe(false);
    const peak = governor.peakBytes;
    expect(cache.takeFrom(source, bytes)).toBe(true);
    expect(source.allocated).toBe(0);
    expect(source.held).toBe(ordinary - bytes);
    expect(governor.usedBytes).toBe(ordinary);
    expect(governor.peakBytes).toBe(peak);
    await Promise.resolve();
    expect(served).toBe(false);
    reach.reach("at capacity");
    reach.reach("queued admission unchanged");

    const loan = await source.borrow(governor.reserveBytes);
    expect(source.allocate(source.held)).toBe(true);
    const state = [source.held, source.allocated, governor.usedBytes];
    expect(cache.takeFrom(source, ordinary - bytes + 1)).toBe(false);
    expect([source.held, source.allocated, governor.usedBytes]).toEqual(state);
    reach.reach("loan capture refused");
    // Transfer an ordinary subset while the loan remains with its original holder.
    const ordinaryPart = 64 * (1 + Math.floor(random() * 512));
    expect(cache.takeFrom(source, ordinaryPart)).toBe(true);
    expect(source.loaned).toBe(governor.reserveBytes);
    source.free(source.allocated);
    loan.repay();
    expect(source.loaned).toBe(0);
    expect(governor.usedBytes).toBe(ordinary);
    reach.reach("ordinary transfer beside loan");
    source.close();
    const next = await waiting;
    expect(governor.usedBytes).toBe(bytes + ordinaryPart + next.held);
    cache.release(bytes + ordinaryPart);
    next.close();
    cache.unregister();
    expect(governor.usedBytes).toBe(0);
    reach.reach("both owners close");
  });

it("rejects foreign, closed, malformed and unallocated transfers without changing usage", async () => {
  const a = new Governor(4 * MiB),
    b = new Governor(4 * MiB);
  const source = await a.admit({ label: "source", minimumBytes: 4096, spillable: false });
  const own = a.registerCache({ evictableBytes: 0, evict: () => 0 });
  const foreign = b.registerCache({ evictableBytes: 0, evict: () => 0 });
  expect(own.takeFrom(source, 1)).toBe(false);
  source.allocate(4096);
  expect(foreign.takeFrom(source, 4096)).toBe(false);
  expect(own.takeFrom({}, 1)).toBe(false);
  for (const bytes of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY])
    expect(() => own.takeFrom(source, bytes)).toThrow(RangeError);
  expect(a.usedBytes).toBe(4096);
  expect(b.usedBytes).toBe(0);
  own.unregister();
  expect(own.takeFrom(source, 4096)).toBe(false);
  source.close();
  expect(foreign.takeFrom(source, 0)).toBe(false);
  foreign.unregister();
  expect(a.usedBytes).toBe(0);
});

afterAll(() => reach.assertReached());
