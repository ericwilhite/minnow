/**
 * The extent space under crashes, over the simulated disk (sim-disk.ts): a workload of writers
 * (with extends and relocations), publishes and two-generation reclaim, crashed at every physical
 * step with several seeds. After each crash a new owner opens a new space and must find the last
 * published (or in-flight) root and the root before it intact, an exact free map (`audit`), and
 * room to keep writing. A second case loses the newest root slot outright: the previous root's
 * extents must still read back, which is what two-generation reclaim is for.
 */
import { afterAll, describe, expect, it } from "vitest";
import { createReach, SPACE_CRASH_SWEEP_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { BlobRef, ExtentRef, ExtentSpace } from "./extents.js";
import type { StorageBackend } from "./contract.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimBackend, SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";
import { blobsOf, bytesOf, openOver, rootOf } from "./space-test-support.js";
import { DATA_ALIGN } from "./contract.js";

const SEEDS = seedsFor("next-extent-space-crash", [0x5ace, 41, 977, 3, 1009, 7717, 31337, 65521]);
const reach = createReach(
  "extent-space crash matrix",
  SPACE_CRASH_SWEEP_REACH.flatMap((mode) => SEEDS.map((seed) => `${mode}/seed=${String(seed)}`)),
);

interface Ack {
  readonly published: string[];
  inFlight: string | null;
}

const text = (bytes: Uint8Array | null) =>
  bytes === null ? null : new TextDecoder().decode(bytes);

/** Rounds of: one streamed (extended) chunk and one small blob; retire the previous round. */
async function workload(
  space: ExtentSpace,
  ack: Ack,
  rounds: number,
  first = 1,
  retireFirst: ExtentRef[] = [],
  forceRelocation = false,
): Promise<void> {
  let previous = retireFirst;
  for (let round = first; round < first + rounds; round++) {
    const writer = space.beginWrite(`round ${String(round)}`);
    const chunk = writer.open(DATA_ALIGN);
    // The reviewer's variant: a small extent right after the chunk blocks in-place growth.
    const early = forceRelocation
      ? await writer.write([bytesOf(300 + round, round * 10 + 9)])
      : null;
    for (let page = 0; page < 3; page++) {
      const part = bytesOf(1500 + round * 97 + page * 11, round * 10 + page);
      if (chunk.length + part.length > chunk.capacity) chunk.extend(part.length);
      await chunk.append([part]);
    }
    const sealed = await chunk.seal();
    const small = early ?? (await writer.write([bytesOf(300 + round, round * 10 + 9)]));
    if (sealed === null) throw new Error("sealed empty");
    const blobs = [...sealed.blobs, ...small.blobs];
    const root = rootOf(blobs);
    ack.inFlight = text(root);
    await space.publish(root, { writers: [writer], retired: previous });
    ack.published.push(text(root) ?? "");
    ack.inFlight = null;
    previous = [sealed.extent, small.extent];
  }
}

async function readAll(space: ExtentSpace, blobs: readonly BlobRef[]): Promise<void> {
  const budget = { allocate: () => true, free: () => undefined };
  const lease = await space.read(blobs, { budget });
  lease.release();
}

describe("extent space crash sweep", () => {
  // Every backend/placement/seed still sweeps every physical step. Give each seed its own
  // ordinary test deadline rather than putting eight complete sweeps inside one test.
  it.each(
    (
      [
        ["files", "growing in place", false],
        ["files", "relocating at seal (review B1-p2a)", true],
        ["IndexedDB", "growing in place", false],
        ["IndexedDB", "relocating at seal", true],
      ] as const
    ).flatMap(([store, placement, forceRelocation]) =>
      SEEDS.map((seed) => ({ store, placement, forceRelocation, seed })),
    ),
  )(
    "$store, $placement, seed $seed: reopens to an intact current and previous root, an exact free map, and keeps writing",
    async ({ store, forceRelocation, seed }) => {
      reach.beginHistory();
      const backendOf = (disk: SimDisk): StorageBackend =>
        store === "files"
          ? new SimBackend(disk)
          : createIndexedDbBackend({
              database: () => simIndexedDb(disk),
              name: "db",
              locks: new SimLocks(),
            });
      const clean = new SimDisk();
      const { space } = await openOver(backendOf(clean));
      await workload(space, { published: [], inFlight: null }, 5, 1, [], forceRelocation);
      if (forceRelocation) expect(space.stats.relocations).toBeGreaterThan(0);
      else expect(space.stats.relocations).toBe(0);
      const steps = clean.steps;
      let checked = 0;
      for (let step = 0; step < steps; step++) {
        const disk = new SimDisk({ crashAtStep: step, seed: seed + step * 7919 });
        const ack: Ack = { published: [], inFlight: null };
        try {
          const { space: first } = await openOver(backendOf(disk));
          await workload(first, ack, 5, 1, [], forceRelocation);
          disk.powerLoss();
        } catch (error) {
          // Once the disk has crashed the process is dead, whatever its calls then report (a
          // crash in background work, an IndexedDB release, fails the space; a call in flight
          // meets a dead handle). Before a crash, any error is a real failure.
          if (disk.crashes === 0) throw error;
        }
        const label = `crash at step ${String(step)}, seed ${String(seed)}`;
        const { space: reopened } = await openOver(backendOf(disk));
        const root = text(reopened.root);
        expect([ack.published.at(-1) ?? null, ack.inFlight], label).toContain(root);
        const history = [...ack.published, ...(ack.inFlight === null ? [] : [ack.inFlight])];
        if (root !== null) {
          const before = history[history.indexOf(root) - 1] ?? null;
          const previous = text(reopened.previousRoot);
          // The previous root is the one before, or lost with the slot an interrupted publish tore.
          if (previous !== null) expect(previous, label).toBe(before);
          await readAll(reopened, blobsOf(reopened.root ?? new Uint8Array(0)));
          if (reopened.previousRoot !== null)
            await readAll(reopened, blobsOf(reopened.previousRoot));
        }
        await reopened.audit();
        // The reopened space keeps going: two more rounds, the first retiring what its root reached.
        const reached = blobsOf(reopened.root ?? rootOf([]))
          .map((blob) => blob.extent)
          .filter(
            (extent, index, all) => all.findIndex((e) => e.offset === extent.offset) === index,
          );
        await workload(
          reopened,
          { published: [], inFlight: null },
          2,
          50,
          reached,
          forceRelocation,
        );
        await readAll(reopened, blobsOf(reopened.root ?? new Uint8Array(0)));
        await reopened.audit();
        checked += 1;
      }
      expect(checked).toBe(steps);
      // The workload reaches dozens of physical steps on files; on IndexedDB, batching leaves at
      // least a commit and a flush per publish (five publishes).
      expect(steps).toBeGreaterThan(store === "files" ? 30 : 2 * 5 - 1);
      reach.reach(`${store}/${forceRelocation ? "relocating" : "growing"}/seed=${String(seed)}`);
    },
  );

  it("falls back to the previous root intact when the newest root slot is lost", async () => {
    const disk = new SimDisk();
    const { space } = await openOver(new SimBackend(disk));
    const ack: Ack = { published: [], inFlight: null };
    await workload(space, ack, 2);
    // Generation 2 retired generation 1's extents. Write more data, as the next flush would.
    const writer = space.beginWrite("next");
    for (let index = 0; index < 4; index++) await writer.write([bytesOf(3000, 70 + index)]);
    // The owner dies, and generation 2's root slot is lost (a lying flush, or media damage): a
    // reopen falls back to generation 1.
    disk.powerLoss();
    const slot = disk.files.get("root-1");
    if (slot === undefined) throw new Error("no root slot");
    slot.current = new Uint8Array(slot.current.length);
    slot.durable = slot.current;
    const { space: reopened } = await openOver(new SimBackend(disk));
    expect(text(reopened.root)).toBe(ack.published[0]);
    // Generation 1's extents were never reused: they read back and verify.
    await readAll(reopened, blobsOf(reopened.root ?? new Uint8Array(0)));
    await reopened.audit();
  });
});

afterAll(() => reach.assertReached());
