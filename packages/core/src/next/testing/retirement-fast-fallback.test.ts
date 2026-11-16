/** A below-threshold resident optimization must fall back when its larger peak is refused. */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RETIREMENT_FAST_FALLBACK_REACH } from "../../testing/reach.js";
import { ExtentListKeeper, extentsOf } from "../storage/extent-list.js";
import { ReadDeniedError, type ExtentRef, type ByteBudget } from "../storage/extents.js";
import { DATA_ALIGN } from "../storage/contract.js";
import { fixture, openOwner, pressure } from "./retirement-test-support.js";
const reach = createReach("optional resident retirement admission", RETIREMENT_FAST_FALLBACK_REACH);
function released(first: number, count: number): readonly ExtentRef[] {
  return new Proxy([] as ExtentRef[], {
    get(target, key, receiver) {
      if (key === "length") return count;
      if (typeof key === "string" && /^(0|[1-9][0-9]*)$/u.test(key)) {
        const i = Number(key);
        return i < count ? { offset: first + i * DATA_ALIGN, length: 1 } : undefined;
      }
      return Reflect.get(target, key, receiver) as unknown;
    },
  });
}
for (const seed of seedsFor("bounded-retirement-fast-fallback", [0xb7fd, 0xb7fe]))
  it(`spools a refused small resident source ${String(seed)}`, async () => {
    reach.beginHistory();
    const owner = await openOwner(),
      count = seed & 1 ? 4095 : 4096,
      throws = (seed & 1) !== 0;
    let held: Awaited<ReturnType<typeof pressure>> | undefined;
    try {
      const { root, anchor } = await fixture(owner, count + 1, 64);
      held = await pressure(owner, 192 << 10);
      const grant = held.grant;
      let refused = 0,
        spools = 0,
        peak = grant.held;
      const budget: ByteBudget = {
        ordinarySource: () => grant,
        allocate(n) {
          const ok = grant.allocate(n);
          peak = Math.max(peak, grant.held);
          if (!ok) {
            refused++;
            if (throws) throw new ReadDeniedError(n);
          }
          return ok;
        },
        free: (n) => grant.free(n),
      };
      const original = owner.space.prepareRetirements.bind(owner.space);
      owner.space.prepareRetirements = async (...args) => {
        spools++;
        return original(...args);
      };
      const keeper = new ExtentListKeeper(root.list, {
          reader: owner.space,
          budget,
          pageExtents: 64,
        }),
        writer = owner.space.beginWrite("small refused release", { budget });
      try {
        const bytes = grant.held;
        const result = await keeper.prepare(2, {
          added: [],
          released: released(root.first, count),
          writer,
        });
        expect(await extentsOf(result.list, owner.space, budget)).toEqual([anchor.extent]);
        expect(refused).toBe(1);
        expect(spools).toBe(1);
        expect(peak).toBeLessThanOrEqual(bytes);
        keeper.abort(2);
        expect(grant.allocated).toBe(0);
        reach.reach(throws ? "throwing refusal" : "boolean refusal");
        reach.reach(count === 4096 ? "at threshold" : "below threshold");
        reach.reach("disk fits192KiB");
        reach.reach("exact survivor");
        reach.reach("no grant growth");
      } finally {
        keeper.close();
        await writer.abort();
      }
      // An unrelated failure must propagate, rather than being mistaken for pressure.
      const reason = new Error("observer failure"),
        before = spools,
        other: ByteBudget = {
          allocate(n) {
            if (n > 192 << 10) throw reason;
            return grant.allocate(n);
          },
          free: (n) => grant.free(n),
        },
        next = new ExtentListKeeper(root.list, {
          reader: owner.space,
          budget: other,
          pageExtents: 64,
        }),
        output = owner.space.beginWrite("foreign refusal", { budget: other });
      try {
        await expect(
          next.prepare(3, { added: [], released: released(root.first, count), writer: output }),
        ).rejects.toBe(reason);
        expect(spools).toBe(before);
        expect(grant.allocated).toBe(0);
        reach.reach("foreign error preserved");
      } finally {
        next.close();
        await output.abort();
      }
      await owner.space.audit();
      reach.reach("original root intact");
    } finally {
      held?.close();
      await owner.close();
    }
  });
afterAll(() => reach.assertReached());
