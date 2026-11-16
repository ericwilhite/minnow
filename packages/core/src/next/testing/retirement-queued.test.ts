/** A queued publication borrows synchronously, before it reaches the workspace or final barrier. */
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { DATA_ALIGN } from "../storage/contract.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import { descriptors, encodeRoot, fixture, openOwner } from "./retirement-test-support.js";
function gate() {
  let open!: () => void;
  return {
    wait: new Promise<void>((r) => {
      open = r;
    }),
    open: () => open(),
  };
}
for (const seed of seedsFor("bounded-retirement-queued", [0xb7f7, 0xb7f8]))
  it(`queued publication keeps its immutable claims after the caller closes them (${String(seed)})`, async () => {
    const owner = await openOwner(),
      enter = gate(),
      resume = gate();
    let first: Promise<number> | undefined,
      second: Promise<number> | undefined,
      release: Promise<void> | undefined;
    try {
      const { root } = await fixture(owner, 3),
        budget = owner.startup;
      const inventory = await owner.space.prepareRetirements(descriptors(root.first, 2), {
        budget,
      });
      const keeper = new ExtentListKeeper(root.list, {
          reader: owner.space,
          budget,
          pageExtents: 2,
        }),
        writer = owner.space.beginWrite("queued claims", { budget });
      try {
        const prepared = await keeper.prepare(2, {
          added: [],
          released: [],
          releasedInventory: inventory,
          writer,
        });
        const next = { first: root.first + 2 * DATA_ALIGN, count: 1, list: prepared.list };
        const native = owner.session.publish.bind(owner.session);
        let armed = true;
        owner.session.publish = async (bytes) => {
          if (armed) {
            armed = false;
            enter.open();
            await resume.wait;
          }
          await native(bytes);
        };
        first = owner.space.publish(encodeRoot(root), {
          writers: [],
          retired: [],
          list: root.list,
          budget,
        });
        await enter.wait;
        second = owner.space.publish(encodeRoot(next), {
          writers: [writer],
          retired: prepared.retire,
          retiredInventory: inventory,
          list: next.list,
          budget,
        });
        // Observe immediately, so an intentional mutation cannot leave an unhandled rejection.
        const outcome = second.then(
          (value) => ({ value }),
          (error: unknown) => ({ error }),
        );
        let done = false;
        release = inventory.release().then(() => {
          done = true;
        });
        await Promise.resolve();
        expect(done).toBe(false);
        resume.open();
        await first;
        const result = await outcome;
        expect(
          result,
          "accepted queued claim must stay readable until publication settles",
        ).not.toHaveProperty("error");
        keeper.commit(2, 2);
        await release;
        await owner.space.audit();
      } finally {
        resume.open();
        await first?.catch(() => undefined);
        await second?.catch(() => undefined);
        await release?.catch(() => undefined);
        keeper.close();
        if (writer.state === "open") await writer.abort();
        await inventory.release();
      }
    } finally {
      resume.open();
      await first?.catch(() => undefined);
      await second?.catch(() => undefined);
      await release?.catch(() => undefined);
      await owner.close();
    }
  });
