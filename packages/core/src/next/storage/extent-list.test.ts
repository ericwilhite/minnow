/**
 * Ruling R25 at the storage layer (extent-list.ts): the page codec, the keeper (the write path's
 * extent-delta sink) and startup from the two roots' lists, over both simulated stores.
 *
 * The publisher here plays the write path: each round seals new blobs (some packed several to an
 * extent), drops some old ones (whole extents, `released`), asks the keeper to prepare the list,
 * publishes a root naming the blobs and the list, and commits (or aborts) the attempt. Startup
 * reads the lists only (`listedExtents`); the full walk (the root's blobs and list pages) is the
 * cross-check, at open (`verifyAtOpen`) and in every audit.
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { StoreCorruptionError, type StorageBackend } from "./contract.js";
import {
  decodeListPage,
  encodeListPage,
  extentsOf,
  ExtentListKeeper,
  listedExtents,
  type ExtentList,
} from "./extent-list.js";
import type { BlobRef, ExtentRef, ExtentSpace, ReachableExtents } from "./extents.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { createOpfsBackend, type OpfsDirectoryLike } from "./opfs.js";
import { SimDisk, SimLocks, SimulatedCrash } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";
import { openExtentSpace } from "./space.js";
import { bytesOf, countingBudget, countingMemory } from "./space-test-support.js";

// ── Roots that name blobs and a list ──────────────────────────────────────────────────────────

interface ListRoot {
  readonly round: number;
  readonly blobs: readonly BlobRef[];
  readonly list: ExtentList;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const encodeRoot = (root: ListRoot) => encoder.encode(JSON.stringify(root));
const decodeRoot = (bytes: Uint8Array) => JSON.parse(decoder.decode(bytes)) as ListRoot;

/** Startup's source: the root's list only. */
const fromList: ReachableExtents = (root, reader, budget) =>
  listedExtents(decodeRoot(root).list, reader, budget);

/** The full walk: every blob the root names, and its list pages. */
const fullWalk: ReachableExtents = function* (root) {
  const parsed = decodeRoot(root);
  yield* parsed.blobs;
  for (const page of parsed.list) yield page.blob;
};

const directoryOf = (disk: SimDisk): OpfsDirectoryLike => ({
  getFileHandle: async (name) => ({
    createSyncAccessHandle: async () => disk.open(name, true),
  }),
});

type Kind = "opfs" | "indexeddb";

function backendOver(kind: Kind, disk: SimDisk, locks = new SimLocks()): StorageBackend {
  return kind === "opfs"
    ? createOpfsBackend({ directory: directoryOf(disk), name: "db", locks })
    : createIndexedDbBackend({ database: () => simIndexedDb(disk), name: "db", locks });
}

/** One owner: the space opened from the lists, and the keeper built from the durable root. */
async function openOwner(
  backend: StorageBackend,
  options: { steal?: boolean; pageExtents?: number; skipPrevious?: boolean } = {},
) {
  const lease = await backend.acquire({ steal: options.steal === true, timeoutMs: 2_000 });
  const session = await backend.open(lease);
  const budget = countingBudget(Infinity);
  const space = await openExtentSpace(session, {
    reachable: fromList,
    walk: fullWalk,
    verifyAtOpen: true,
    budget,
    registerCache: () => countingMemory(Infinity),
  });
  const root = space.root === null ? null : decodeRoot(space.root);
  const keeper = new ExtentListKeeper(root?.list ?? [], {
    reader: space,
    budget,
    pageExtents: options.pageExtents ?? 4,
  });
  return { lease, session, space, keeper, root, budget };
}

/** The publisher's state: the blobs the current root names, grouped by extent. */
interface Published {
  round: number;
  /** Blobs by extent offset (an extent is dropped whole). */
  extents: Map<number, { extent: ExtentRef; blobs: BlobRef[] }>;
}

const publishedOf = (root: ListRoot | null): Published => {
  const extents = new Map<number, { extent: ExtentRef; blobs: BlobRef[] }>();
  for (const blob of root?.blobs ?? []) {
    const entry = extents.get(blob.extent.offset) ?? { extent: blob.extent, blobs: [] };
    entry.blobs.push(blob);
    extents.set(blob.extent.offset, entry);
  }
  return { round: root?.round ?? 0, extents };
};

let attempts = 0;

/**
 * One publication round: seal 1–3 extents of 1–4 blobs each, drop up to two old extents, prepare,
 * publish, commit. `inFlight` is told the round before its publish starts; `between` runs after
 * the list pages are written and before the publish (a power-loss point).
 */
async function round(
  owner: { space: ExtentSpace; keeper: ExtentListKeeper },
  state: Published,
  random: () => number,
  hooks: { inFlight?: (round: number) => void; between?: () => void } = {},
): Promise<void> {
  const { space, keeper } = owner;
  const writer = space.beginWrite("round");
  const next = state.round + 1;
  const sealed: Array<{ extent: ExtentRef; blobs: BlobRef[] }> = [];
  for (let extents = 1 + Math.floor(random() * 3); extents > 0; extents--) {
    const parts = Array.from({ length: 1 + Math.floor(random() * 4) }, (_, index) =>
      bytesOf(1 + Math.floor(random() * 9_000), next * 31 + index),
    );
    const written = await writer.write(parts);
    sealed.push({ extent: written.extent, blobs: [...written.blobs] });
  }
  const old = [...state.extents.values()];
  const dropped: Array<{ extent: ExtentRef; blobs: BlobRef[] }> = [];
  for (let drop = Math.floor(random() * 3); drop > 0 && old.length > 0; drop--)
    dropped.push(...old.splice(Math.floor(random() * old.length), 1));
  const attempt = (attempts += 1);
  const prepared = await keeper.prepare(attempt, {
    added: [...writer.extents],
    released: dropped.map((entry) => entry.extent),
    writer,
  });
  const blobs = [...old, ...sealed].flatMap((entry) => entry.blobs);
  hooks.between?.();
  hooks.inFlight?.(next);
  try {
    await space.publish(encodeRoot({ round: next, blobs, list: prepared.list }), {
      writers: [writer],
      retired: [...dropped.map((entry) => entry.extent), ...prepared.retire],
    });
  } catch (error) {
    keeper.abort(attempt);
    throw error;
  }
  keeper.commit(attempt, next);
  state.round = next;
  for (const entry of dropped) state.extents.delete(entry.extent.offset);
  for (const entry of sealed) state.extents.set(entry.extent.offset, entry);
}

/** Every blob of `root` reads back (verified), and the space audits exactly. */
async function verify(space: ExtentSpace, root: ListRoot | null): Promise<void> {
  if (root !== null && root.blobs.length > 0) {
    const lease = await space.read(root.blobs, { budget: countingBudget(Infinity) });
    lease.release();
  }
  await space.audit();
}

// ── The page codec ───────────────────────────────────────────────────────────────────────────

describe("extent list pages", () => {
  const extents = [
    { offset: 0, length: 100 },
    { offset: 4096, length: 8192 },
    { offset: 16_384, length: 1 },
  ];
  const page = (bytes: Uint8Array) => ({
    blob: {
      extent: { offset: 0, length: bytes.length },
      offset: 0,
      length: bytes.length,
      checksum: 0,
    },
    first: 0,
    count: 3,
  });

  it("round-trip", () => {
    const bytes = encodeListPage(extents);
    expect(decodeListPage(bytes, page(bytes))).toEqual(extents);
  });

  it("refuse a wrong count, a wrong first offset, disorder, overlap and foreign bytes", () => {
    const bytes = encodeListPage(extents);
    expect(() => decodeListPage(bytes, { ...page(bytes), count: 2 })).toThrow(StoreCorruptionError);
    expect(() => decodeListPage(bytes, { ...page(bytes), first: 4096 })).toThrow(
      StoreCorruptionError,
    );
    const disordered = encodeListPage([extents[1], extents[0]] as ExtentRef[]);
    expect(() =>
      decodeListPage(disordered, { ...page(disordered), first: 4096, count: 2 }),
    ).toThrow(StoreCorruptionError);
    const overlapping = encodeListPage([
      { offset: 0, length: 5_000 },
      { offset: 4096, length: 10 },
    ]);
    expect(() => decodeListPage(overlapping, { ...page(overlapping), count: 2 })).toThrow(
      StoreCorruptionError,
    );
    const foreign = new Uint8Array(bytes);
    foreign[0] = 0;
    expect(() => decodeListPage(foreign, page(foreign))).toThrow(StoreCorruptionError);
  });
});

// ── The keeper and startup ───────────────────────────────────────────────────────────────────

const SEEDS = seedsFor("next-extent-list", [0x25a1, 0x25a2, 0x25a3]);

describe.each(["opfs", "indexeddb"] as const)("%s: lists kept by the publications", (kind) => {
  it.each(SEEDS)(
    "each list names exactly the root's extents, shares untouched pages, and reopens (seed %i)",
    { timeout: 60_000 },
    async (seed) => {
      const random = mulberry32(seed);
      const disk = new SimDisk({ seed });
      const backend = backendOver(kind, disk);
      let owner = await openOwner(backend);
      const state = publishedOf(null);
      let shared = 0;
      for (let at = 0; at < 30; at++) {
        const before = owner.keeper.list;
        await round(owner, state, random);
        const after = owner.keeper.list;
        shared += after.filter((page) => before.includes(page)).length;
        // Exactly the extents of the root's blobs (not its own pages).
        const expected = [...state.extents.values()]
          .map((entry) => entry.extent)
          .sort((a, b) => a.offset - b.offset);
        expect(await extentsOf(after, owner.space, owner.budget)).toEqual(expected);
        expect(after.every((page) => page.count <= 4)).toBe(true);
        if (at % 10 === 9) {
          // A new process: startup from the lists, cross-checked against the walk.
          await owner.space.close();
          await owner.session.close();
          await owner.lease.release();
          owner = await openOwner(backend);
          expect(owner.keeper.list).toEqual(after);
        }
        await verify(
          owner.space,
          owner.root === null ? null : decodeRoot(owner.space.root ?? new Uint8Array()),
        );
      }
      // Untouched pages are carried over by reference, not rewritten.
      expect(shared).toBeGreaterThan(30);
      await owner.space.close();
      await owner.lease.release();
    },
  );

  it("an aborted attempt leaves the committed list, and its pages are freed", async () => {
    const disk = new SimDisk({ seed: 7 });
    const backend = backendOver(kind, disk);
    const owner = await openOwner(backend);
    const state = publishedOf(null);
    const random = mulberry32(7);
    for (let at = 0; at < 5; at++) await round(owner, state, random);
    const committed = owner.keeper.list;
    const writer = owner.space.beginWrite("aborted");
    const written = await writer.write([bytesOf(5_000, 1)]);
    await owner.keeper.prepare((attempts += 1), {
      added: [written.extent],
      released: [],
      writer,
    });
    owner.keeper.abort(attempts);
    await writer.abort();
    expect(owner.keeper.list).toBe(committed);
    await owner.space.audit();
    await round(owner, state, random);
    await owner.space.audit();
    await owner.space.close();
    await owner.lease.release();
  });
});

// ── Power loss ────────────────────────────────────────────────────────────────────────────────

/** Runs rounds until the disk's planned crash; returns what was acknowledged and in flight. */
async function runUntilCrash(kind: Kind, disk: SimDisk, seed: number, rounds: number) {
  const backend = backendOver(kind, disk);
  const random = mulberry32(seed);
  const state = publishedOf(null);
  let acked = 0;
  let inFlight: number | null = null;
  try {
    const owner = await openOwner(backend);
    for (let at = 0; at < rounds; at++) {
      await round(owner, state, random, {
        inFlight: (next) => {
          inFlight = next;
        },
      });
      acked = state.round;
      inFlight = null;
    }
    await owner.space.close();
    await owner.lease.release();
    return { crashed: false, acked, inFlight };
  } catch (error) {
    if (disk.crashes === 0) throw error;
    return { crashed: true, acked, inFlight };
  }
}

/** A new process over the crashed disk: startup from the lists must hold, and work goes on. */
async function recoverAndContinue(
  kind: Kind,
  disk: SimDisk,
  seed: number,
  expected: { acked: number; inFlight: number | null },
): Promise<void> {
  const owner = await openOwner(backendOver(kind, disk));
  const round_ = owner.root?.round ?? 0;
  expect([expected.acked, expected.inFlight]).toContain(round_);
  await verify(owner.space, owner.root);
  const state = publishedOf(owner.root);
  const random = mulberry32(seed ^ 0x5eed);
  for (let at = 0; at < 3; at++) {
    await round(owner, state, random);
    await verify(owner.space, decodeRoot(owner.space.root ?? new Uint8Array()));
  }
  await owner.space.close();
  await owner.lease.release();
}

describe.each(["opfs", "indexeddb"] as const)("%s: power loss at every physical step", (kind) => {
  it(
    "every crash point of eight list publications recovers from the lists",
    { timeout: 300_000 },
    async () => {
      const seed = 0x25c0;
      const steps = (() => {
        const disk = new SimDisk({ seed });
        return runUntilCrash(kind, disk, seed, 8).then(() => disk.steps);
      })();
      const total = await steps;
      let crashes = 0;
      for (let crashAtStep = 0; crashAtStep < total; crashAtStep++) {
        const disk = new SimDisk({ seed: seed + crashAtStep, crashAtStep });
        const outcome = await runUntilCrash(kind, disk, seed, 8);
        if (!outcome.crashed) continue;
        crashes += 1;
        try {
          await recoverAndContinue(kind, disk, seed, outcome);
        } catch (error) {
          throw new Error(
            `crash at step ${String(crashAtStep)} of ${String(total)} (acked ${String(outcome.acked)}, in flight ${String(outcome.inFlight)}): ${String(error)}`,
            { cause: error },
          );
        }
      }
      expect(crashes).toBeGreaterThan(total / 2);
    },
  );

  it("power lost after the list pages are written and before the root: the previous root's list holds", async () => {
    const disk = new SimDisk({ seed: 11 });
    const backend = backendOver(kind, disk);
    const owner = await openOwner(backend);
    const state = publishedOf(null);
    const random = mulberry32(11);
    for (let at = 0; at < 6; at++) await round(owner, state, random);
    const acked = state.round;
    await expect(
      round(owner, state, random, {
        between: () => {
          disk.powerLoss();
          throw new SimulatedCrash();
        },
      }),
    ).rejects.toThrow();
    await recoverAndContinue(kind, disk, 11, { acked, inFlight: null });
  });

  it("generated rounds under seeded crash points (seeds)", { timeout: 120_000 }, async () => {
    for (const seed of SEEDS) {
      const random = mulberry32(seed);
      for (let trial = 0; trial < 12; trial++) {
        const disk = new SimDisk({
          seed: seed + trial,
          crashAtStep: 20 + Math.floor(random() * 400),
        });
        const outcome = await runUntilCrash(kind, disk, seed + trial, 20);
        if (outcome.crashed) await recoverAndContinue(kind, disk, seed + trial, outcome);
      }
    }
  });
});

// ── The previous root, and concurrent openers ─────────────────────────────────────────────────

describe.each(["opfs", "indexeddb"] as const)("%s: the previous root's list", (kind) => {
  it("startup keeps what only the previous root reaches, so a rollback to it finds its blobs", async () => {
    const disk = new SimDisk({ seed: 21 });
    const backend = backendOver(kind, disk);
    let owner = await openOwner(backend);
    const state = publishedOf(null);
    const random = mulberry32(21);
    for (let at = 0; at < 6; at++) await round(owner, state, random);
    // The next round drops extents the current root still reaches (the previous root after it).
    const before = decodeRoot(owner.space.root ?? new Uint8Array());
    await round(owner, state, random);
    await owner.space.close();
    await owner.lease.release();
    // A new process: the previous root's extents must stay allocated, so new writes land elsewhere.
    owner = await openOwner(backend);
    const writer = owner.space.beginWrite("fill");
    for (let at = 0; at < 40; at++) await writer.write([bytesOf(4096, at)]);
    await writer.abort();
    await owner.space.audit();
    // Everything the previous root names still reads back (no blob was overwritten).
    const lease = await owner.space.read(before.blobs, { budget: countingBudget(Infinity) });
    lease.release();
    await owner.space.close();
    await owner.lease.release();
  });
});

describe.each(["opfs", "indexeddb"] as const)("%s: concurrent openers", (kind) => {
  it("a stealing opener starts from the lists while the old owner's attempt is prepared", async () => {
    const disk = new SimDisk({ seed: 31 });
    const locks = new SimLocks();
    const first = await openOwner(backendOver(kind, disk, locks));
    const state = publishedOf(null);
    const random = mulberry32(31);
    for (let at = 0; at < 5; at++) await round(first, state, random);
    // A new opener steals while the old owner is in the middle of a round.
    const stealing = openOwner(backendOver(kind, disk, locks), { steal: true });
    const failed = round(first, { ...state, extents: new Map(state.extents) }, random).then(
      () => "published",
      () => "fenced",
    );
    const second = await stealing;
    expect(await failed).toBe("fenced");
    await verify(second.space, second.root);
    const next = publishedOf(second.root);
    for (let at = 0; at < 3; at++) await round(second, next, random);
    await verify(second.space, decodeRoot(second.space.root ?? new Uint8Array()));
    await second.space.close();
    await second.lease.release();
    await first.lease.release();
  });
});
