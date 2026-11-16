/** Accepted preparation/read/publication owns its bytes through settlement, including cancel. */
import { afterAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, BOUNDED_RETIREMENT_FENCE_REACH } from "../../testing/reach.js";
import { DATA_ALIGN } from "../storage/contract.js";
import {
  RETIREMENT_CURSOR_BYTES,
  RETIREMENT_OWNER_BYTES,
  RetirementClosedError,
} from "../storage/retirement-inventory.js";
import { descriptors, encodeRoot, fixture, openOwner } from "./retirement-test-support.js";

const reach = createReach("retirement ownership fences", BOUNDED_RETIREMENT_FENCE_REACH);
function gate() {
  let open!: () => void;
  const wait = new Promise<void>((r) => {
    open = r;
  });
  return { wait, open };
}
const turn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
for (const seed of seedsFor("bounded-retirement-fences", [0xb7f3, 0xb7f4])) {
  it(`subtracts shared/packed retained extents, canonicalizes duplicates and replays twice (${String(seed)})`, async () => {
    reach.beginHistory();
    const owner = await openOwner();
    try {
      const random = mulberry32(seed),
        count = 5 + Math.floor(random() * 9),
        budget = owner.startup;
      const { root } = await fixture(owner, count);
      const before = budget.allocated;
      function* candidates() {
        for (const extent of descriptors(root.first, count, true)) {
          yield { extent, offset: 0, length: 1, checksum: 0 };
          yield extent;
        }
      }
      const inventory = await owner.space.prepareRetirements(candidates(), {
        budget,
        retained: descriptors(root.first, 2),
      });
      expect(inventory.count).toBe(count - 2);
      reach.reach("packed alias subtraction");
      reach.reach("exact duplicate union");
      try {
        for (let replay = 0; replay < 2; replay++) {
          const cursor = inventory.cursor({ budget });
          let index = 2;
          try {
            while (await cursor.next()) {
              expect(cursor.offset).toBe(root.first + index++ * DATA_ALIGN);
            }
          } finally {
            await cursor.close();
          }
          expect(index).toBe(count);
        }
        expect(budget.allocated).toBe(before + RETIREMENT_OWNER_BYTES);
      } finally {
        await inventory.release();
      }
      expect(budget.allocated).toBe(before);
      await expect(owner.space.prepareRetirements(candidates(), { budget })).rejects.toThrow(
        "retained references",
      );
      await expect(
        owner.space.prepareRetirements(
          [
            root.list.at(0)?.blob.extent ?? { offset: -1, length: 1 },
            root.list.at(0)?.blob.extent ?? { offset: -1, length: 1 },
          ],
          {
            budget,
            duplicates: "refuse",
          },
        ),
      ).rejects.toThrow(/Duplicate|duplicate|overlap/u);
      expect(budget.allocated).toBe(before);
      await owner.space.audit();
    } finally {
      await owner.close();
    }
  });
  it(`release and space close fence a gated native inventory read (${String(seed)})`, async () => {
    reach.beginHistory();
    for (const wholeSpace of [false, true]) {
      const owner = await openOwner(),
        enter = gate(),
        resume = gate();
      let pending: Promise<boolean> | undefined, closing: Promise<void> | undefined;
      try {
        const budget = owner.startup,
          { root } = await fixture(owner, 3),
          baseline = budget.allocated;
        const inventory = await owner.space.prepareRetirements(descriptors(root.first, 2), {
          budget,
        });
        const metadata = owner.session.metadata?.();
        if (metadata == null) throw new Error("Missing metadata store");
        const read = metadata.read.bind(metadata);
        let armed = true;
        metadata.read = async (page, target) => {
          if (armed) {
            armed = false;
            enter.open();
            await resume.wait;
          }
          await read(page, target);
        };
        const cursor = inventory.cursor({ budget });
        pending = Promise.resolve(cursor.next());
        const outcome = pending.catch((error: unknown) => error);
        await enter.wait;
        let done = false;
        closing = (wholeSpace ? owner.space.close() : inventory.release()).then(() => {
          done = true;
        });
        await turn();
        expect(done).toBe(false);
        reach.reach(wholeSpace ? "space-close read fence" : "release read fence");
        expect(budget.allocated).toBeGreaterThanOrEqual(
          baseline + RETIREMENT_OWNER_BYTES + RETIREMENT_CURSOR_BYTES,
        );
        resume.open();
        expect(await outcome).toBeInstanceOf(RetirementClosedError);
        await closing;
        expect(budget.allocated).toBe(baseline);
        await cursor.close();
        await inventory.release();
      } finally {
        resume.open();
        await pending?.catch(() => undefined);
        await closing?.catch(() => undefined);
        await owner.close();
      }
    }
  });
  it(`an abandoned producer with an immediately resolved return still drains next (${String(seed)})`, async () => {
    reach.beginHistory();
    const owner = await openOwner(),
      enter = gate(),
      resume = gate(),
      stop = new AbortController();
    let pending: Promise<unknown> | undefined, closing: Promise<void> | undefined;
    try {
      const budget = owner.startup,
        before = budget.allocated;
      let live = true;
      const input = {
        [Symbol.asyncIterator]() {
          return this;
        },
        async next() {
          enter.open();
          await resume.wait;
          live = false;
          return { done: false as const, value: { offset: 0, length: DATA_ALIGN } };
        },
        async return() {
          return { done: true as const, value: undefined };
        },
      };
      pending = owner.space.prepareRetirements(input, { budget, signal: stop.signal });
      const caught = pending.catch((error: unknown) => error);
      await enter.wait;
      stop.abort(new Error("stop producer"));
      let done = false;
      closing = owner.space.close().then(() => {
        done = true;
      });
      await turn();
      expect(done).toBe(false);
      expect(live).toBe(true);
      expect(budget.allocated).toBeGreaterThan(before);
      resume.open();
      expect(await caught).toBeInstanceOf(Error);
      await closing;
      expect(live).toBe(false);
      reach.reach("producer settlement fence");
      expect(budget.allocated).toBe(0);
    } finally {
      resume.open();
      await pending?.catch(() => undefined);
      await closing?.catch(() => undefined);
      await owner.close();
    }
  });
  it(`accepted publication borrows drain before releasing the descriptor inventory (${String(seed)})`, async () => {
    reach.beginHistory();
    const owner = await openOwner(),
      enter = gate(),
      resume = gate();
    let pending: Promise<number> | undefined, closing: Promise<void> | undefined;
    try {
      const budget = owner.startup,
        { root } = await fixture(owner, 4),
        inventory = await owner.space.prepareRetirements(descriptors(root.first, 3), { budget });
      // This root has a native exact walk, while its list is deliberately absent in this probe.
      // Prepare its surviving list through the real keeper before the final publication fence.
      const { ExtentListKeeper } = await import("../storage/extent-list.js");
      const keeper = new ExtentListKeeper(root.list, {
          reader: owner.space,
          budget,
          pageExtents: 64,
        }),
        writer = owner.space.beginWrite("borrow fence", { budget });
      try {
        const prepared = await keeper.prepare(2, {
          added: [],
          released: [],
          releasedInventory: inventory,
          writer,
        });
        const next = { first: root.first + 3 * DATA_ALIGN, count: 1, list: prepared.list };
        const publish = owner.session.publish.bind(owner.session);
        owner.session.publish = async (bytes) => {
          enter.open();
          await resume.wait;
          await publish(bytes);
        };
        pending = owner.space.publish(encodeRoot(next), {
          writers: [writer],
          retired: prepared.retire,
          retiredInventory: inventory,
          list: next.list,
          budget,
        });
        await enter.wait;
        const before = budget.allocated;
        let done = false;
        closing = inventory.release().then(() => {
          done = true;
        });
        await turn();
        expect(done).toBe(false);
        expect(budget.allocated).toBe(before);
        resume.open();
        await pending;
        keeper.commit(2, 2);
        await closing;
        expect(() => inventory.cursor({ budget })).toThrow(RetirementClosedError);
        reach.reach("accepted publication fence");
      } finally {
        keeper.close();
        if (writer.state === "open") await writer.abort();
        await inventory.release();
      }
    } finally {
      resume.open();
      await pending?.catch(() => undefined);
      await closing?.catch(() => undefined);
      await owner.close();
    }
  });
}

afterAll(() => reach.assertReached());
for (const seed of seedsFor("bounded-retirement-packed", [0xb7f9, 0xb7fa]))
  it(`real packed pages do not authorize retirement of a surviving alias (${String(seed)})`, async () => {
    const owner = await openOwner(),
      budget = owner.startup,
      writer = owner.space.beginWrite("packed pages", { budget });
    try {
      const packed = await writer.write([
        new Uint8Array([seed & 255]),
        new Uint8Array([7, 9]),
        new Uint8Array([3]),
      ]);
      const first = packed.blobs[0],
        kept = packed.blobs[1],
        last = packed.blobs[2];
      if (first === undefined || kept === undefined || last === undefined)
        throw new Error("Missing packed page");
      const baseline = budget.allocated;
      const inventory = await owner.space.prepareRetirements([first, last, first], {
        budget,
        retained: [kept],
      });
      try {
        expect(inventory.count).toBe(0);
        const cursor = inventory.cursor({ budget });
        try {
          expect(await cursor.next()).toBe(false);
        } finally {
          await cursor.close();
        }
      } finally {
        await inventory.release();
      }
      expect(budget.allocated).toBe(baseline);
      const read = await owner.space.read([first, kept, last], { budget });
      try {
        expect(read.bytes.map((b) => [...b])).toEqual([[seed & 255], [7, 9], [3]]);
      } finally {
        read.release();
      }
    } finally {
      await writer.abort();
      await owner.close();
    }
  });
