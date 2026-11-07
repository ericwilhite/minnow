/** Approved final-phase law: reservations may grow, but ownership transitions wait together. */
import { afterAll, expect, it } from "vitest";
import { createReach, STORAGE_MUTATION_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { beginTask, taskClock } from "../task.js";
import type { ReachableExtents } from "../storage/extents.js";
import { openExtentSpace } from "../storage/space.js";
import {
  countingBudget,
  countingMemory,
  jsonReachable,
  rootOf,
} from "../storage/space-test-support.js";

const reach = createReach("storage mutation boundary", STORAGE_MUTATION_REACH);
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
for (const seed of seedsFor("storage-mutation", [1551, 1552, 1553, 1554]))
  it(`holds abort/discard/seal ownership while reservations interleave with native publish (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      backend = new MemoryBackend(),
      lease = await backend.acquire();
    const session = await backend.open(lease),
      budget = countingBudget(64 << 20),
      memory = countingMemory(Infinity);
    const space = await openExtentSpace(session, {
      reachable: jsonReachable,
      budget,
      registerCache: () => memory,
    });
    const oldWriter = space.beginWrite("old"),
      old = await oldWriter.write([new Uint8Array([1])]);
    await space.publish(rootOf(old.blobs), { writers: [oldWriter], retired: [] });
    const aborter = space.beginWrite("abort"),
      discarded = space.beginWrite("discard"),
      sealer = space.beginWrite("seal"),
      reservations = space.beginWrite("reservations");
    await aborter.write([new Uint8Array([2])]);
    const abortedBuilder = aborter.open(8192);
    await abortedBuilder.append([new Uint8Array([3])]);
    const gone = await discarded.write([new Uint8Array([4])]);
    const sealingBuilder = sealer.open(8192);
    await sealingBuilder.append([new Uint8Array([5])]);
    const nextWriter = space.beginWrite("new root"),
      next = await nextWriter.write([new Uint8Array([6])]);
    const native = session.publish.bind(session);
    let enter!: () => void, resume!: () => void;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const held = new Promise<void>((resolve) => {
      resume = resolve;
    });
    session.publish = async (root) => {
      enter();
      await held;
      await native(root);
    };
    const publication = space.publish(rootOf(next.blobs), {
      writers: [nextWriter],
      retired: [old.extent],
    });
    const effects: Array<Promise<unknown>> = [],
      finished = [false, false, false];
    try {
      await entered;
      const actions = [
        () => aborter.abort(),
        () => discarded.discard(gone.extent),
        () => sealingBuilder.seal(),
      ];
      const order = [0, 1, 2];
      for (let index = order.length - 1; index > 0; index--) {
        const at = Math.floor(random() * (index + 1)),
          oldIndex = order[index] ?? 0;
        order[index] = order[at] ?? 0;
        order[at] = oldIndex;
      }
      const growing = reservations.open(4096);
      for (const index of order) {
        const action = actions[index];
        if (action === undefined) throw new Error("Missing action");
        const effect = action().then((value) => {
          finished[index] = true;
          return value;
        });
        void effect.catch(() => undefined); // observe even a deliberately broken ordering immediately
        effects.push(effect);
        // Only allocation/addition: no asynchronous placement and no gate wait.
        const before = growing.capacity;
        for (let step = 0; step < 1 + Math.floor(random() * 4); step++) growing.extend(4096);
        expect(growing.capacity).toBeGreaterThan(before);
        reservations.open(4096);
        await turn(); // Memory backend has no timer/I/O waits; drain all runnable microtasks.
        expect(finished).toEqual([false, false, false]);
        expect(aborter.state).toBe("open");
        expect(aborter.extents).toHaveLength(1);
        expect(discarded.extents).toHaveLength(1);
        expect(sealer.extents).toHaveLength(0);
      }
      reach.reach("reservations during publish");
      reach.reach("abort held");
      reach.reach("discard held");
      reach.reach("seal held");
      resume();
      await publication;
      await Promise.all(effects);
      expect(finished).toEqual([true, true, true]);
      reach.reach("transitions drain");
      const read = await space.read(next.blobs, { budget });
      try {
        expect(read.bytes[0]).toEqual(new Uint8Array([6]));
      } finally {
        read.release();
      }
      await discarded.abort();
      await sealer.abort();
      await reservations.abort();
      await space.audit();
      reach.reach("audit after interleaving");
    } finally {
      resume();
      await Promise.allSettled([publication, ...effects]);
      session.publish = native;
      for (const writer of [aborter, discarded, sealer, reservations])
        if (writer.state === "open") await writer.abort();
      await space.close();
      await session.close();
      await lease.release();
    }
    expect(memory.used).toBe(0);
    expect(budget.used).toBe(0);
  });
afterAll(() => reach.assertReached());

for (const fail of [false, true])
  it(`close drains accepted publication before returning its memory (native failure ${String(fail)})`, async () => {
    const backend = new MemoryBackend(),
      lease = await backend.acquire(),
      session = await backend.open(lease),
      budget = countingBudget(64 << 20),
      memory = countingMemory(Infinity);
    const space = await openExtentSpace(session, {
      reachable: jsonReachable,
      budget,
      registerCache: () => memory,
    });
    const writer = space.beginWrite("closing"),
      written = await writer.write([new Uint8Array([7])]);
    let enter!: () => void, resume!: () => void;
    const entered = new Promise<void>((resolve) => {
        enter = resolve;
      }),
      held = new Promise<void>((resolve) => {
        resume = resolve;
      });
    const native = session.publish.bind(session),
      failure = new Error("scripted native publication failure");
    session.publish = async (root) => {
      enter();
      await held;
      if (fail) throw failure;
      await native(root);
    };
    const publication = space.publish(rootOf(written.blobs), { writers: [writer], retired: [] });
    // Attach the observer immediately, including the scripted failure case.
    const outcome = publication.then(
      () => null,
      (error: unknown) => error,
    );
    let closed = false;
    let closing: Promise<void> | null = null;
    try {
      await entered;
      closing = space.close().then(() => {
        closed = true;
      });
      await turn();
      expect(closed).toBe(false);
      expect(memory.used).toBeGreaterThan(0);
      expect(() => space.beginWrite("after close")).toThrow();
      await expect(space.publish(rootOf([]), { writers: [], retired: [] })).rejects.toThrow();
      resume();
      expect(await outcome).toBe(fail ? failure : null);
      await closing;
      expect(closed).toBe(true);
    } finally {
      resume();
      await outcome;
      await closing;
      await space.close();
      await session.close();
      await lease.release();
    }
    expect(memory.used).toBe(0);
    expect(budget.used).toBe(0);
  });

it("restarts an audit whose captured previous root was reclaimed by a concurrent generation", async () => {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease),
    budget = countingBudget(64 << 20),
    memory = countingMemory(Infinity);
  let enter!: () => void,
    resume!: () => void,
    blockedOffset = -1,
    calls = 0;
  const entered = new Promise<void>((resolve) => {
      enter = resolve;
    }),
    held = new Promise<void>((resolve) => {
      resume = resolve;
    });
  const reachable: ReachableExtents = async function* (root, reader, budget, work) {
    calls++;
    for await (const ref of jsonReachable(root, reader, budget, work)) {
      yield ref;
      const extent = "extent" in ref ? ref.extent : ref;
      if (extent.offset === blockedOffset) {
        blockedOffset = -1;
        enter();
        await held;
      }
    }
  };
  const space = await openExtentSpace(session, { reachable, budget, registerCache: () => memory });
  const now = taskClock.now;
  let audit: Promise<void> | null = null,
    publishing: Promise<number> | null = null;
  try {
    const a = space.beginWrite("A"),
      first = await a.write([new Uint8Array([1])]);
    await space.publish(rootOf(first.blobs), { writers: [a], retired: [] });
    const b = space.beginWrite("B"),
      second = await b.write([new Uint8Array([2])]);
    await space.publish(rootOf(second.blobs), { writers: [b], retired: [first.extent] });
    // Explicit scheduler: all backend promises resolve immediately and the task clock is held
    // constant. One task drains runnable microtasks; no elapsed-time/timer assumption is used.
    taskClock.now = () => 0;
    beginTask();
    let released = false;
    const nativeRelease = session.releaseData.bind(session);
    session.releaseData = async (ranges) => {
      await nativeRelease(ranges);
      if (
        ranges.some(
          (range) =>
            range.offset <= first.extent.offset &&
            first.extent.offset < range.offset + range.length,
        )
      )
        released = true;
    };
    calls = 0; // only this captured generation and its concurrent publication count
    blockedOffset = first.extent.offset;
    audit = space.audit();
    await entered;
    let published = false;
    publishing = space
      .publish(rootOf(second.blobs), { writers: [], retired: [] })
      .then((generation) => {
        published = true;
        return generation;
      });
    await turn();
    expect(published, "read-only audit must allow publication to advance").toBe(true);
    await publishing;
    await turn();
    expect(released, "the captured previous root reached native release").toBe(true);
    resume();
    await audit;
    expect(calls).toBeGreaterThanOrEqual(5); // two captured roots, one publish input, two restarted roots
  } finally {
    resume();
    await Promise.allSettled([audit, publishing]);
    taskClock.now = now;
    beginTask();
    await space.close();
    await session.close();
    await lease.release();
  }
  expect(memory.used).toBe(0);
  expect(budget.used).toBe(0);
});

it("finishes an accepted discard before a later abort of the same writer", async () => {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease),
    budget = countingBudget(64 << 20),
    memory = countingMemory(Infinity);
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget,
    registerCache: () => memory,
  });
  const writer = space.beginWrite("discard then abort"),
    written = await writer.write([new Uint8Array([1])]);
  let enter!: () => void, resume!: () => void;
  const entered = new Promise<void>((resolve) => {
      enter = resolve;
    }),
    held = new Promise<void>((resolve) => {
      resume = resolve;
    });
  const native = session.publish.bind(session);
  session.publish = async (root) => {
    enter();
    await held;
    await native(root);
  };
  const publication = space.publish(rootOf([]), { writers: [], retired: [] });
  let discard: Promise<void> | null = null,
    abort: Promise<void> | null = null;
  try {
    await entered;
    discard = writer.discard(written.extent);
    abort = writer.abort();
    resume();
    await publication;
    await discard;
    await abort;
    expect(writer.state).toBe("aborted");
    await space.audit();
  } finally {
    resume();
    await Promise.allSettled([publication, discard, abort]);
    await space.close();
    await session.close();
    await lease.release();
  }
  expect(memory.used).toBe(0);
  expect(budget.used).toBe(0);
});
