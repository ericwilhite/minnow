/**
 * Many tabs, one database, a randomized schedule — and a check that what survived could have
 * happened.
 *
 * Cross-tab concurrency is the defining hazard of a browser database. Every other test of it here
 * is a hand-written interleaving: two writers, a known order, an asserted winner. Those pin the
 * cases somebody thought of, and interleavings are exactly the thing nobody thinks of enough of.
 *
 * This runs N independent `MinnowDatabase` instances over one shared store — the same arrangement
 * as N browser tabs over one IndexedDB — issuing a seeded random schedule of reads and writes,
 * with the tabs' operations interleaved rather than serialized. The seed comes from the registry,
 * so a failure replays with `MINNOW_SEED` and can be committed as a permanent case.
 *
 * What it checks is not "everything succeeded", because under contention not everything can (see
 * write-contention.test.ts for that ceiling). It checks that the outcome is *explicable*:
 *
 *   - every row present is one some writer actually wrote, with the values that writer wrote
 *   - every write reported as accepted is present; every write reported as rejected is absent
 *   - no key appears twice, however many tabs raced to insert it
 *   - every tab, reading afterwards, sees the same database
 *
 * That last one is the cross-tab guarantee proper: tabs may disagree about who won, but they may
 * not disagree about what the database now contains.
 *
 * Scope, honestly stated. These tabs share one process and one store, and the store serializes
 * commits through its own queue — so this cannot manufacture the kind of lost update that only
 * two real threads produce. Disabling the IndexedDB compare-and-set outright does *not* fail this
 * suite, because the queue keeps commits ordered anyway; `storage.test.ts` is what covers that
 * check, and does catch it. What this suite does catch, verified by injecting it, is an
 * acknowledged write that never lands: the ordering check below reports the exact key, the
 * sequence number, and both writes.
 *
 * Twelve tabs, deliberately: the commit ceiling on IndexedDB is `maxCommitRetries + 1`, so fewer
 * writers than that would never actually lose a race and the "a rejected write is absent" checks
 * would pass without ever being exercised. Measured on this fixture, the memory store admits all
 * 244 writes and IndexedDB rejects 9 of 244 -- which is the asymmetry write-contention.test.ts
 * characterizes, and the reason both stores run here.
 */
import { describe, expect, it, onTestFinished } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";

const TABS = 12;
const ROUNDS = 30;
const KEY_SPACE = 40;

interface Written {
  tab: number;
  key: number;
  amount: number;
  /**
   * Global order of acknowledgement, stamped when the write's promise resolved. If the store told
   * A it had committed and *then* told B the same, B committed later — so B's values are the ones
   * that must be visible. This is what makes the outcome checkable rather than merely plausible.
   */
  sequence: number;
}

/** What one tab did and what it was told. */
interface TabOutcome {
  accepted: Written[];
  rejected: Written[];
  /** Rejections that were not conflicts are a failure of the run, not a normal outcome. */
  unexpected: string[];
}

async function openTab(store: BlockStore): Promise<MinnowDatabase> {
  // Each tab is its own MinnowDatabase over the shared store, which is exactly the shape of two
  // browser tabs on one origin: separate engines, separate caches, one set of bytes.
  return new MinnowDatabase(store, { rowsPerBlock: 16, autoCompact: false });
}

async function simulate(
  store: BlockStore,
  seed: number,
  options: { tabs?: number; rounds?: number; compactEvery?: number | null } = {},
): Promise<{ tabs: MinnowDatabase[]; outcomes: TabOutcome[]; setup: MinnowDatabase }> {
  const tabCount = options.tabs ?? TABS;
  const rounds = options.rounds ?? ROUNDS;
  const compactEvery = options.compactEvery === undefined ? 5 : options.compactEvery;
  const setup = await openTab(store);
  await setup.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "tab", type: "number" },
      { name: "amount", type: "number" },
    ],
  });

  const tabs = await Promise.all(Array.from({ length: tabCount }, () => openTab(store)));
  const outcomes: TabOutcome[] = tabs.map(() => ({ accepted: [], rejected: [], unexpected: [] }));
  let sequence = 0;

  for (let round = 0; round < rounds; round += 1) {
    // Every tab acts in the same round without awaiting the others: the schedule is whatever the
    // event loop and the store produce, which is the point.
    await Promise.all(
      tabs.map(async (database, tab) => {
        // A per-tab stream, so each tab's choices are its own but the whole run is reproducible.
        const random = mulberry32(seed + tab * 7919 + round * 104_729);
        const key = Math.floor(random() * KEY_SPACE) + 1;
        const amount = Math.floor(random() * 1_000);
        const outcome = outcomes[tab];
        if (outcome === undefined) return;

        if (random() < 0.3) {
          // A read, which must never fail however much writing is going on around it.
          try {
            await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false });
          } catch (error) {
            outcome.unexpected.push(`read failed: ${String(error)}`);
          }
          return;
        }

        try {
          // Upsert rather than insert: two tabs racing on the same key is the interesting case,
          // and a plain insert would make "already exists" the dominant outcome rather than a
          // genuine commit race.
          await database.upsertBatch("items", {
            columns: { id: [key], tab: [tab], amount: [amount] },
          });
          // Stamped after the await resolves, so it records when the store acknowledged the
          // commit rather than when the call was issued.
          sequence += 1;
          outcome.accepted.push({ tab, key, amount, sequence });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          // Losing a commit race is the expected way to fail here. Anything else is a bug.
          sequence += 1;
          if (/Manifest changed|conflict/i.test(message)) {
            outcome.rejected.push({ tab, key, amount, sequence });
          } else outcome.unexpected.push(`write failed unexpectedly: ${message}`);
        }
      }),
    );
    // This suite is about cross-tab commit ordering, not the deliberately pathological cost of
    // retaining one L0 segment per mutation forever. Fold at deterministic quiescent points so
    // the same 244 racing writes run while query preparation remains bounded. Compaction itself
    // is part of the concurrency contract: it must preserve every accepted winner exactly.
    if (compactEvery !== null && (round + 1) % compactEvery === 0) {
      await setup.compactTable("items");
    }
  }

  return { tabs, outcomes, setup };
}

const stores: Array<{ name: string; create: () => Promise<BlockStore> }> = [
  { name: "memory", create: async () => new MemoryBlockStore() },
  {
    name: "opfs",
    create: async () =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
  {
    name: "indexeddb",
    create: () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

it("keeps a small uncompacted IndexedDB multi-tab conflict path exact", async () => {
  const store = await IndexedDbBlockStore.open({
    name: crypto.randomUUID(),
    indexedDB: new IDBFactory(),
  });
  const { tabs, outcomes, setup } = await simulate(store, 0x51c1, {
    tabs: 4,
    rounds: 4,
    compactEvery: null,
  });
  onTestFinished(async () => {
    await Promise.allSettled([...tabs, setup].map((database) => database.close()));
    store.close();
  });
  expect(outcomes.flatMap((outcome) => outcome.unexpected)).toEqual([]);
  const canonical = JSON.stringify(
    (await tabs[0]?.query("SELECT id, tab, amount FROM items ORDER BY id", { memoize: false }))
      ?.rows ?? [],
  );
  for (const tab of tabs) {
    expect(
      JSON.stringify(
        (await tab.query("SELECT id, tab, amount FROM items ORDER BY id", { memoize: false })).rows,
      ),
    ).toBe(canonical);
  }
  expect(outcomes.reduce((total, outcome) => total + outcome.accepted.length, 0)).toBeGreaterThan(
    0,
  );
});

describe.each(stores)("many tabs over one store ($name)", ({ create }) => {
  for (const seed of seedsFor("concurrency-simulation", [0x51c1])) {
    it(`leaves a database every tab agrees on (seed ${String(seed)})`, async () => {
      const store = await create();
      const { tabs, outcomes, setup } = await simulate(store, seed);
      const databases = [...tabs, setup];
      onTestFinished(async () => {
        await Promise.allSettled(databases.map((database) => database.close()));
        store.close();
      });

      // Nothing failed for a reason this simulation does not model.
      for (const [tab, outcome] of outcomes.entries()) {
        expect(outcome.unexpected, `tab ${String(tab)}, seed ${String(seed)}`).toEqual([]);
      }

      const reader = tabs[0];
      expect(reader).toBeDefined();
      if (reader === undefined) return;
      const rows = (
        await reader.query("SELECT id, tab, amount FROM items ORDER BY id", { memoize: false })
      ).rows as Array<{ id: number; tab: number; amount: number }>;

      // No key twice, however many tabs raced for it.
      const ids = rows.map((row) => row.id);
      expect(new Set(ids).size, `seed ${String(seed)}: duplicate keys`).toBe(ids.length);

      // Every surviving row is one some tab actually wrote, with that tab's values. A row that is
      // not is the signature of two writes torn together.
      const everyWrite = outcomes.flatMap((outcome) => [...outcome.accepted, ...outcome.rejected]);
      for (const row of rows) {
        const match = everyWrite.some(
          (write) => write.key === row.id && write.tab === row.tab && write.amount === row.amount,
        );
        expect(
          match,
          `seed ${String(seed)}: row ${JSON.stringify(row)} was never written by any tab`,
        ).toBe(true);
      }

      // The heart of it. For each key, the write the store acknowledged *last* is the one whose
      // values must be visible. An earlier write showing through is a lost update: the store told
      // two writers it had committed them and then kept the wrong one.
      const winnerByKey = new Map<number, Written>();
      for (const outcome of outcomes) {
        for (const write of outcome.accepted) {
          const current = winnerByKey.get(write.key);
          if (current === undefined || write.sequence > current.sequence) {
            winnerByKey.set(write.key, write);
          }
        }
      }
      const visible = new Map(rows.map((row) => [row.id, row]));
      for (const [key, winner] of winnerByKey) {
        const row = visible.get(key);
        expect(
          row,
          `seed ${String(seed)}: key ${String(key)} was accepted but is absent`,
        ).toBeDefined();
        if (row === undefined) continue;
        expect(
          { tab: row.tab, amount: row.amount },
          `seed ${String(seed)}: key ${String(key)} shows an earlier write than the one the store ` +
            `acknowledged last (sequence ${String(winner.sequence)}, tab ${String(winner.tab)})`,
        ).toEqual({ tab: winner.tab, amount: winner.amount });
      }

      // Every tab sees the same database. Tabs may disagree about who won a race; they may not
      // disagree about the result.
      const canonical = JSON.stringify(rows);
      for (const [index, tab] of tabs.entries()) {
        const seen = (
          await tab.query("SELECT id, tab, amount FROM items ORDER BY id", { memoize: false })
        ).rows;
        expect(
          JSON.stringify(seen),
          `seed ${String(seed)}: tab ${String(index)} sees a different database`,
        ).toBe(canonical);
      }

      // The exact invariant this workload allows. Every write is an upsert and nothing deletes,
      // so a key is visible if and only if some tab was told its write succeeded. A lost write --
      // acknowledged but absent -- or a phantom one shows up here as a set difference.
      const acceptedKeys = new Set(
        outcomes.flatMap((outcome) => outcome.accepted.map((write) => write.key)),
      );
      expect(
        [...new Set(ids)].sort((left, right) => left - right),
        `seed ${String(seed)}: visible keys do not match the writes that were acknowledged`,
      ).toEqual([...acceptedKeys].sort((left, right) => left - right));

      // And the run has to have been a real race, or none of the above proves anything.
      const accepted = outcomes.reduce((total, outcome) => total + outcome.accepted.length, 0);
      expect(accepted, `seed ${String(seed)}: no writes landed at all`).toBeGreaterThan(50);

      // Idle per-tab reader leases correctly pin their snapshots. Close those tabs before the
      // explicit collection pass, then prove the physical history collapses to live state.
      await Promise.all([...tabs, setup].map((database) => database.close()));
      const cleanup = await openTab(store);
      databases.push(cleanup);
      // One pass prunes old manifests; the next can reclaim artifacts they had rooted. The third
      // proves the fixed point and keeps this explicit rather than relying on background timing.
      for (let pass = 0; pass < 3; pass += 1) {
        await cleanup.collectGarbage({ maxItemsPerStep: 256, retainRecentVersions: 1 });
      }
      // Manifest tombstones are removed by a separately bounded adapter cursor so foreground
      // collection never monopolizes the event loop. Drive that cursor to its fixed point here.
      for (let page = 0; page < 4; page += 1) {
        await store.removePrunedManifestRecords(256);
      }
      const storage = await cleanup.storageStats();
      expect(
        storage.segmentCount,
        `seed ${String(seed)}: segment history stayed unbounded`,
      ).toBeLessThanOrEqual(8);
      expect(
        storage.transactionCount,
        `seed ${String(seed)}: transaction history stayed unbounded`,
      ).toBeLessThanOrEqual(16);
      expect(
        storage.manifestCount,
        `seed ${String(seed)}: manifest history stayed unbounded`,
      ).toBeLessThanOrEqual(2);
    }, 300_000);
  }
});
