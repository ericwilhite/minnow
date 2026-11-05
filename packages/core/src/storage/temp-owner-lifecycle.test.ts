/**
 * Generated spill-owner lifecycles, checked against a shadow model on every store.
 *
 * A spilling query creates a temp owner, writes pages under it, renews it while it runs, and
 * removes it in `finally`. A query that dies (killed worker, closed tab, timeout) leaves the owner
 * to expire, and something else reclaims it: spill cleanup (`removeTempOwnerIfExpired`) or the
 * next owner's creation, which sweeps expired owners (storage/types.ts, `TempSpillStore`). The
 * law, from the same contract: an owner and its pages go together — a page is readable only while
 * its owner record exists, removal of either kind takes the pages with it — and whatever state
 * these calls reach survives a close, a crash, and a reopen (BlockStore atomicity; RecordCore.load
 * restores what dump captured). Core 0.19.x broke it: a sweeping create removed the expired
 * owner's record and kept its pages, and the next OPFS open refused the database.
 *
 * Each seed plans one history of creates, page writes (some to owners already gone, some failing
 * mid-write), renewals, clock moves, run and owner removals, cleanup, checkpoints, graceful
 * reopens and crashes. Stores differ in whether a create sweeps (the IndexedDB adapter does not),
 * so the model observes which expired owners a create removed and then requires their pages gone.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { createReach } from "../testing/reach.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
  type TempOwnerRecord,
} from "./index.js";

const SEEDS = seedsFor("temp-owner-lifecycle", [0x7e40, 0x7e41, 0x7e42, 0x7e43, 0x7e44, 0x7e45]);
const STEPS = 60;

const REACH_STATES = [
  "create swept an owner holding pages",
  "reopened after a sweep",
  "checkpointed after a sweep",
  "crashed and reopened",
  "write refused for a gone owner",
  "write failed mid-way",
  "renewed owner reopened",
  "cleanup reclaimed an owner holding pages",
  "owner removed holding pages",
] as const;
type ReachState = (typeof REACH_STATES)[number];

interface Harness {
  store: BlockStore;
  /** Persistent stores only: close and reopen; `crash` skips the graceful close. */
  reopen?: (crash: boolean) => Promise<BlockStore>;
  /** OPFS only: makes the next temp-page file write fail, optionally crashing the store first. */
  failNextPageWrite?: (crash: boolean) => void;
  /** OPFS only: whether the page's file is on disk. */
  pageFileExists?: (ownerId: string, runId: string, pageIndex: number) => boolean;
  checkpoints: boolean;
}

interface Target {
  name: string;
  /** The declared states this store can enter; the campaign must reach each of them. */
  reach: readonly ReachState[];
  create: (rng: () => number) => Promise<Harness>;
}

const TARGETS: Target[] = [
  {
    name: "memory",
    // Memory never reopens and has no file to fail.
    reach: [
      "create swept an owner holding pages",
      "write refused for a gone owner",
      "cleanup reclaimed an owner holding pages",
      "owner removed holding pages",
    ],
    create: () => Promise.resolve({ store: new MemoryBlockStore(), checkpoints: false }),
  },
  {
    name: "indexeddb",
    // This adapter's create does not sweep, and it has no checkpoint or file to fail.
    reach: [
      "crashed and reopened",
      "write refused for a gone owner",
      "renewed owner reopened",
      "cleanup reclaimed an owner holding pages",
      "owner removed holding pages",
    ],
    create: async () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      const harness: Harness = {
        store: await IndexedDbBlockStore.open({ name, indexedDB }),
        checkpoints: false,
        reopen: async () => {
          harness.store.close();
          harness.store = await IndexedDbBlockStore.open({ name, indexedDB });
          return harness.store;
        },
      };
      return harness;
    },
  },
  {
    name: "opfs",
    reach: REACH_STATES,
    create: async (rng) => {
      const shim = new MemoryOpfs();
      const name = crypto.randomUUID();
      // Frequent checkpoints put sweeps into checkpoints; rare ones keep them in the log.
      const checkpointEntries = [1, 3, 1_000_000][Math.floor(rng() * 3)] ?? 1;
      const open = () => OpfsBlockStore.open({ name, root: shim.root, checkpointEntries });
      const opfs = await open();
      let failNext: boolean | undefined;
      shim.setWriteFault((path, phase) => {
        if (failNext === undefined || phase !== "write" || !path.includes("/temp/")) return;
        const crash = failNext;
        failNext = undefined;
        if (crash) (harness.store as OpfsBlockStore)._crashForTests();
        throw new Error("simulated page write failure");
      });
      const harness: Harness = {
        store: opfs,
        checkpoints: checkpointEntries <= 3,
        reopen: async (crash) => {
          const current = harness.store as OpfsBlockStore;
          if (crash) current._crashForTests();
          else current.close();
          harness.store = await open();
          return harness.store;
        },
        failNextPageWrite: (crash) => {
          failNext = crash;
        },
        pageFileExists: (ownerId, runId, pageIndex) =>
          shim.readFileBytes(`minnowdb/${name}/temp/${ownerId}/${runId}/${String(pageIndex)}`) !==
          undefined,
      };
      return harness;
    },
  },
];

interface ModelOwner {
  record: TempOwnerRecord;
  /** `${runId}/${pageIndex}` → fill byte. */
  pages: Map<string, number>;
}

const iso = (ms: number): string => new Date(ms).toISOString();
const bytesOf = (fill: number): Uint8Array => new Uint8Array(8).fill(fill);

async function runHistory(target: Target, seed: number, reach: (state: ReachState) => void) {
  const rng = mulberry32(seed);
  const pick = <T>(items: readonly T[]): T => {
    const item = items[Math.floor(rng() * items.length)];
    if (item === undefined) throw new Error("pick from an empty list");
    return item;
  };
  const harness = await target.create(rng);
  let nowMs = Date.parse("2026-01-01T00:00:00.000Z");
  let ownerOrdinal = 0;
  let fill = 0;
  const live = new Map<string, ModelOwner>();
  /** Owners removed or swept: their pages must never be readable again. */
  const gone = new Map<string, Set<string>>();
  let sweptSinceReopen = false;
  let renewedSinceReopen = false;
  const log: string[] = [];

  const forget = (ownerId: string): void => {
    const owner = live.get(ownerId);
    const pages = gone.get(ownerId) ?? new Set<string>();
    for (const key of owner?.pages.keys() ?? []) pages.add(key);
    gone.set(ownerId, pages);
    live.delete(ownerId);
  };

  const check = async (where: string): Promise<void> => {
    const store = harness.store;
    // A page write that failed part-way may leave an unreserved file ("temp-page-orphan"): debris
    // the next recovery deletes. Every other issue, an ownerless page above all, is a defect.
    const integrity = await store.checkIntegrity?.({ mode: "metadata", maxIssues: 1_000 });
    expect(
      integrity?.issues.filter(({ code }) => code !== "temp-page-orphan") ?? [],
      `${where}: integrity`,
    ).toEqual([]);
    for (const [ownerId, owner] of live) {
      expect(await store.getTempOwner(ownerId), `${where}: owner ${ownerId}`).toEqual(owner.record);
      for (const [key, pageFill] of owner.pages) {
        const [runId = "", index = ""] = key.split("/");
        expect(
          await store.getTempRunPage(ownerId, runId, Number(index)),
          `${where}: page ${ownerId}/${key}`,
        ).toEqual(bytesOf(pageFill));
      }
    }
    for (const [ownerId, pages] of gone) {
      expect(await store.getTempOwner(ownerId), `${where}: gone owner ${ownerId}`).toBeUndefined();
      for (const key of pages) {
        const [runId = "", index = ""] = key.split("/");
        expect(
          await store.getTempRunPage(ownerId, runId, Number(index)),
          `${where}: page ${ownerId}/${key} outlived its owner`,
        ).toBeUndefined();
        // Removal reclaims the bytes too, not only the ledger entry.
        expect(
          harness.pageFileExists?.(ownerId, runId, Number(index)) ?? false,
          `${where}: file of page ${ownerId}/${key} outlived its owner`,
        ).toBe(false);
      }
    }
  };

  const expired = (owner: ModelOwner): boolean => Date.parse(owner.record.expiresAt) <= nowMs;

  for (let step = 0; step < STEPS; step += 1) {
    const roll = rng();
    const owners = [...live.values()];
    const unexpired = owners.filter((owner) => !expired(owner));
    if (roll < 0.17 || owners.length === 0) {
      const ownerId = `query-${String(seed)}-${String((ownerOrdinal += 1))}`;
      const record: TempOwnerRecord = {
        ownerId,
        createdAt: iso(nowMs),
        expiresAt: iso(nowMs + pick([30_000, 60_000])),
        revision: 0,
      };
      log.push(`create ${ownerId}`);
      await harness.store.createTempOwner(record);
      for (const owner of owners.filter(expired)) {
        if ((await harness.store.getTempOwner(owner.record.ownerId)) !== undefined) continue;
        if (owner.pages.size > 0) reach("create swept an owner holding pages");
        sweptSinceReopen = true;
        log.push(`  swept ${owner.record.ownerId}`);
        forget(owner.record.ownerId);
      }
      live.set(ownerId, { record, pages: new Map() });
    } else if (roll < 0.42 && unexpired.length > 0) {
      const owner = pick(unexpired);
      const fault = harness.failNextPageWrite !== undefined && rng() < 0.12;
      const crash = fault && rng() < 0.5;
      // A failing batch writes fresh pages, so "none of the batch exists" is the whole outcome.
      const runId = fault ? `run-fault-${String(step)}` : `run-${String(Math.floor(rng() * 2))}`;
      const count = 1 + Math.floor(rng() * 3);
      const start = Math.floor(rng() * 3);
      const pages = Array.from({ length: count }, (_, offset) => ({
        ownerId: owner.record.ownerId,
        runId,
        pageIndex: start + offset,
        bytes: bytesOf((fill = (fill % 250) + 1)),
      }));
      const putPages = () => {
        const store = harness.store;
        if (store.putTempRunPages === undefined) throw new Error(`${target.name} has no batch put`);
        return store.putTempRunPages(pages);
      };
      log.push(`put ${owner.record.ownerId} ${runId} ${String(start)}+${String(count)}`);
      if (fault) {
        log.push(`  failing mid-write${crash ? " and crashing" : ""}`);
        harness.failNextPageWrite?.(crash);
        await expect(putPages()).rejects.toThrow();
        reach("write failed mid-way");
        if (crash) {
          await harness.reopen?.(true);
          reach("crashed and reopened");
          sweptSinceReopen = false;
          renewedSinceReopen = false;
        }
        // A failed batch is all-or-nothing: none of its pages exist.
        for (const page of pages) {
          expect(
            await harness.store.getTempRunPage(page.ownerId, page.runId, page.pageIndex),
          ).toBeUndefined();
        }
      } else {
        await putPages();
        for (const page of pages) {
          owner.pages.set(`${page.runId}/${String(page.pageIndex)}`, page.bytes[0] ?? 0);
        }
      }
    } else if (roll < 0.47 && gone.size > 0) {
      // A page write landing after its owner was removed or swept.
      const ownerId = pick([...gone.keys()]);
      log.push(`late put ${ownerId}`);
      await expect(
        harness.store.putTempRunPage({
          ownerId,
          runId: "run-late",
          pageIndex: 0,
          bytes: bytesOf(9),
        }),
      ).rejects.toThrow();
      reach("write refused for a gone owner");
      gone.get(ownerId)?.add("run-late/0");
    } else if (roll < 0.57 && unexpired.length > 0) {
      const owner = pick(unexpired);
      const expiresAt = iso(nowMs + pick([30_000, 60_000]));
      log.push(`renew ${owner.record.ownerId}`);
      owner.record = await harness.store.renewTempOwner({
        ownerId: owner.record.ownerId,
        expectedRevision: owner.record.revision,
        expiresAtCutoff: iso(nowMs),
        expiresAt,
      });
      expect(owner.record.expiresAt).toBe(expiresAt);
      renewedSinceReopen = true;
    } else if (roll < 0.7) {
      const advance = pick([1_000, 10_000, 25_000, 45_000, 90_000]);
      log.push(`advance ${String(advance)}`);
      nowMs += advance;
    } else if (roll < 0.75) {
      const owner = pick(owners);
      const runId = `run-${String(Math.floor(rng() * 2))}`;
      log.push(`remove run ${owner.record.ownerId} ${runId}`);
      await harness.store.removeTempRun(owner.record.ownerId, runId);
      for (const key of [...owner.pages.keys()]) {
        if (!key.startsWith(`${runId}/`)) continue;
        owner.pages.delete(key);
        const [, index = ""] = key.split("/");
        expect(
          await harness.store.getTempRunPage(owner.record.ownerId, runId, Number(index)),
          `removed run page ${owner.record.ownerId}/${key}`,
        ).toBeUndefined();
      }
    } else if (roll < 0.8) {
      // The query's `finally`.
      const owner = pick(owners);
      log.push(`remove owner ${owner.record.ownerId}`);
      if (owner.pages.size > 0) reach("owner removed holding pages");
      await harness.store.removeTempOwner(owner.record.ownerId);
      forget(owner.record.ownerId);
    } else if (roll < 0.87) {
      // Spill cleanup at a cutoff of now: every expired or ownerless owner goes, pages included.
      log.push("cleanup");
      const cutoff = iso(nowMs);
      const listed = (await harness.store.listTempOwnerIdsPage(null, 64)).records;
      for (const owner of owners) expect(listed).toContain(owner.record.ownerId);
      for (const ownerId of listed) {
        const owner = live.get(ownerId);
        const removed = await harness.store.removeTempOwnerIfExpired(ownerId, cutoff);
        expect(removed, `cleanup of ${ownerId}`).toBe(owner === undefined || expired(owner));
        if (!removed) continue;
        if ((owner?.pages.size ?? 0) > 0) reach("cleanup reclaimed an owner holding pages");
        forget(ownerId);
      }
    } else if (harness.reopen !== undefined) {
      const crash = rng() < 0.5;
      log.push(crash ? "crash and reopen" : "reopen");
      if (sweptSinceReopen) reach("reopened after a sweep");
      if (sweptSinceReopen && harness.checkpoints) reach("checkpointed after a sweep");
      if (renewedSinceReopen) reach("renewed owner reopened");
      await harness.reopen(crash);
      if (crash) reach("crashed and reopened");
      sweptSinceReopen = false;
      renewedSinceReopen = false;
    }
    await check(`step ${String(step)} (${log.at(-1) ?? ""})`);
  }

  // Everything left is reclaimable once it expires, and the store still opens afterwards.
  nowMs += 3_600_001;
  for (const ownerId of (await harness.store.listTempOwnerIdsPage(null, 64)).records) {
    expect(await harness.store.removeTempOwnerIfExpired(ownerId, iso(nowMs))).toBe(true);
    forget(ownerId);
  }
  expect((await harness.store.listTempOwnerIdsPage(null, 64)).records).toEqual([]);
  await harness.reopen?.(false);
  await check("final reopen");
  expect((await harness.store.listTempOwnerIdsPage(null, 64)).records).toEqual([]);
  harness.store.close();
}

describe("generated spill-owner lifecycles", () => {
  for (const target of TARGETS) {
    describe(target.name, () => {
      const campaign = createReach(`temp-owner-lifecycle/${target.name}`, target.reach);
      it.each(SEEDS)(`keeps owners and pages together across reopens (seed %i)`, async (seed) => {
        campaign.beginHistory();
        await runHistory(target, seed, (state) => {
          if (target.reach.includes(state)) campaign.reach(state);
        });
      });
      it("reaches every declared lifecycle state", () => {
        campaign.assertReached();
      });
    });
  }
});
