/** Every physical publication interruption reopens from durable R25 roots, never private files. */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, BOUNDED_RETIREMENT_CRASH_REACH } from "../../testing/reach.js";
import { DATA_ALIGN } from "../storage/contract.js";
import { ExtentListKeeper, extentsOf } from "../storage/extent-list.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import {
  decodeRoot,
  descriptors,
  encodeRoot,
  fixture,
  openOwner,
} from "./retirement-test-support.js";

const reach = createReach("retirement publication interruption", BOUNDED_RETIREMENT_CRASH_REACH);
async function history(seed: number, fence: "handle" | "epoch", crashAtStep?: number) {
  const disk = new SimDisk({ seed, ...(crashAtStep === undefined ? {} : { crashAtStep }) }),
    backend = new SimBackend(disk, fence);
  const owner = await openOwner(backend);
  let start: number, end: number, first: number;
  let acked = false;
  try {
    const { root } = await fixture(owner, 5);
    first = root.first;
    const budget = owner.startup,
      inventory = await owner.space.prepareRetirements(descriptors(first, 4, true), { budget });
    const keeper = new ExtentListKeeper(root.list, { reader: owner.space, budget, pageExtents: 2 }),
      writer = owner.space.beginWrite("crashable retirement", { budget });
    try {
      const prepared = await keeper.prepare(2, {
        added: [],
        released: [],
        releasedInventory: inventory,
        writer,
      });
      const next = { first: first + 4 * DATA_ALIGN, count: 1, list: prepared.list };
      start = disk.steps;
      try {
        await owner.space.publish(encodeRoot(next), {
          writers: [writer],
          retired: prepared.retire,
          retiredInventory: inventory,
          list: next.list,
          budget,
        });
        acked = true;
        keeper.commit(2, 2);
      } catch (error) {
        if (disk.crashes === 0) throw error;
      }
      end = disk.steps;
    } finally {
      keeper.close();
      await inventory.release().catch((error: unknown) => {
        if (disk.crashes === 0) throw error;
      });
      if (writer.state === "open")
        await writer.abort().catch((error: unknown) => {
          if (disk.crashes === 0) throw error;
        });
    }
  } finally {
    await owner.close();
  }
  if (disk.crashes !== 0) {
    const recovered = await openOwner(backend);
    try {
      const bytes = recovered.space.root;
      if (bytes === null) throw new Error("Lost acknowledged first root");
      const root = decodeRoot(bytes);
      reach.reach("durable exact root");
      expect([1, 5]).toContain(root.count);
      if (acked) expect(root.count).toBe(1);
      expect(root.first).toBe(root.count === 5 ? first : first + 4 * DATA_ALIGN);
      expect(await extentsOf(root.list, recovered.space, recovered.startup)).toEqual([
        ...descriptors(root.first, root.count),
      ]);
      // Retry through the real keeper after any possible root outcome, then drain both slots.
      const keeper = new ExtentListKeeper(root.list, {
          reader: recovered.space,
          budget: recovered.startup,
          pageExtents: 2,
        }),
        writer = recovered.space.beginWrite("retry after power loss", {
          budget: recovered.startup,
        });
      const inventory = await recovered.space.prepareRetirements(
        descriptors(root.first, root.count - 1),
        { budget: recovered.startup },
      );
      try {
        const prepared = await keeper.prepare(3, {
          added: [],
          released: [],
          releasedInventory: inventory,
          writer,
        });
        const next = { first: first + 4 * DATA_ALIGN, count: 1, list: prepared.list };
        await recovered.space.publish(encodeRoot(next), {
          writers: [writer],
          retired: prepared.retire,
          retiredInventory: inventory,
          list: next.list,
        });
        keeper.commit(3, 3);
        await recovered.space.publish(encodeRoot(next), {
          writers: [],
          retired: [],
          list: next.list,
        });
        await recovered.space.settled();
        await recovered.space.audit();
        reach.reach("retry then both roots drain");
      } finally {
        keeper.close();
        await inventory.release();
        if (writer.state === "open") await writer.abort();
      }
    } finally {
      await recovered.close();
    }
  }
  return { start, end, crashed: disk.crashes !== 0 };
}
for (const seed of seedsFor("bounded-retirement-crash", [0xb7f6]))
  for (const fence of ["handle", "epoch"] as const)
    it(
      `${fence}: every publication write/truncate/flush recovers and retries (${String(seed)})`,
      { timeout: 120_000 },
      async () => {
        reach.beginHistory();
        const control = await history(seed, fence);
        expect(control.end).toBeGreaterThan(control.start);
        let crashes = 0;
        for (let step = control.start; step < control.end; step++) {
          try {
            const result = await history(seed, fence, step);
            expect(result.crashed).toBe(true);
            crashes++;
          } catch (error) {
            throw new Error(
              `Retirement publication crash ${String(step - control.start)}/${String(control.end - control.start)}: ${String(error)}`,
              { cause: error },
            );
          }
        }
        expect(crashes).toBe(control.end - control.start);
        reach.reach(
          fence === "handle" ? "handle every publication step" : "epoch every publication step",
        );
      },
    );

afterAll(() => reach.assertReached());
