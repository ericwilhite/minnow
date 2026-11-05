/**
 * A database must reopen after a query dies mid-spill.
 *
 * A spilling query registers a temp owner and renews it while it runs (`#leasedSpillStore`), so
 * a long spill leaves an owner past revision zero, and possibly past one TTL from its creation.
 * When the worker running it is killed, or the tab closes, nothing removes that owner: it stays
 * until it expires and spill cleanup reclaims it (storage/types.ts, `TempOwnerRecord`). The next
 * open must load that state, not refuse it as corruption.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, OpfsBlockStore, type BlockStore } from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";

interface Target {
  name: string;
  create: () => Promise<{ store: BlockStore; reopen: () => Promise<BlockStore> }>;
}

const targets: Target[] = [
  {
    name: "opfs",
    create: async () => {
      const shim = new MemoryOpfs();
      const name = crypto.randomUUID();
      let store: BlockStore = await OpfsBlockStore.open({ name, root: shim.root });
      return {
        store,
        reopen: async () => {
          store.close();
          store = await OpfsBlockStore.open({ name, root: shim.root });
          return store;
        },
      };
    },
  },
  {
    name: "indexeddb",
    create: async () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      let store: BlockStore = await IndexedDbBlockStore.open({ name, indexedDB });
      return {
        store,
        reopen: async () => {
          store.close();
          store = await IndexedDbBlockStore.open({ name, indexedDB });
          return store;
        },
      };
    },
  },
];

class SimulatedKill extends Error {
  override readonly name = "SimulatedKill";
}

/**
 * Forwards to a real store until the spill owner has been renewed `renewals` times. From then
 * on every temp write and removal fails, as if the worker had been killed: the owner and its
 * pages stay exactly as the last durable call left them.
 */
function killAfterRenewals(store: BlockStore, renewals: number): BlockStore {
  let seen = 0;
  const dead = (): boolean => seen >= renewals;
  return new Proxy(store, {
    get(target, property, receiver) {
      const value: unknown = Reflect.get(target, property, receiver);
      if (typeof value !== "function") return value;
      const method = value as (...args: unknown[]) => unknown;
      return (...args: unknown[]) => {
        if (
          dead() &&
          (property === "putTempRunPage" ||
            property === "putTempRunPages" ||
            property === "getTempRunPage" ||
            property === "removeTempRun" ||
            property === "removeTempOwner" ||
            property === "renewTempOwner")
        ) {
          return Promise.reject(new SimulatedKill("worker killed mid-spill"));
        }
        const result = method.apply(target, args);
        if (property === "renewTempOwner") {
          return (result as Promise<unknown>).then((renewed) => {
            seen += 1;
            return renewed;
          });
        }
        return result;
      };
    },
  });
}

describe("a query killed mid-spill", () => {
  // Killed after 3 renewals: a short spill, still inside its first TTL. After 150 renewals of
  // a 60 s lease: 75 minutes, past one TTL from the owner's creation.
  for (const target of targets)
    for (const renewals of [3, 150]) {
      it(`${target.name} reopens after ${String(renewals)} renewals and reclaims the owner after expiry`, async () => {
        const { store, reopen } = await target.create();
        const start = Date.parse("2026-01-01T00:00:00.000Z");
        let nowMs = start;
        const leaseMs = 60_000;
        // During the spilling query each clock read moves 2 s, so the 60 s owner lease renews
        // every few pages. Setup and the later checks read a clock that only moves by 1 ms.
        let stepMs = 1;
        const now = (): Date => new Date((nowMs += stepMs));
        // Small blocks so the sort below spills, as in database.test.ts's spill-lease test.
        const options = {
          rowsPerBlock: 256,
          compression: "raw",
          spillOwnerLeaseMs: leaseMs,
        } as const;
        const seed = new MinnowDatabase(store, { ...options, now });
        await seed.createTable({
          name: "spilled",
          columns: [
            { name: "id", type: "number" },
            { name: "bucket", type: "number" },
          ],
        });
        const rowCount = 20_000;
        await seed.insertBatch("spilled", {
          columns: {
            id: Array.from({ length: rowCount }, (_, index) => index),
            bucket: Array.from({ length: rowCount }, (_, index) => index % 11),
          },
        });

        stepMs = 2_000;
        const database = new MinnowDatabase(killAfterRenewals(store, renewals), {
          ...options,
          now,
        });
        await expect(
          database.query("SELECT id, bucket FROM spilled ORDER BY bucket, id DESC", {
            executionMemoryBudgetBytes: 150_000,
            spillPageRows: 64,
            memoize: false,
          }),
        ).rejects.toBeInstanceOf(SimulatedKill);
        stepMs = 1;
        const [orphan] = (await store.listTempOwnerIdsPage(null, 16)).records;
        if (orphan === undefined) throw new Error("the killed query left no spill owner");
        const left = await store.getTempOwner(orphan);
        expect(left?.revision).toBe(renewals);
        if (renewals === 150) {
          expect(
            Date.parse(left?.expiresAt ?? "") - Date.parse(left?.createdAt ?? ""),
          ).toBeGreaterThan(3_600_000);
        }

        // The tab closes and a new one opens the same database.
        const reopened = await reopen();
        expect(await reopened.getTempOwner(orphan)).toEqual(left);
        const next = new MinnowDatabase(reopened, { now });
        expect((await next.query("SELECT COUNT(*) AS n FROM spilled")).rows).toEqual([
          { n: rowCount },
        ]);

        // Still live: cleanup keeps it. Past its expiration: cleanup reclaims owner and pages.
        expect(await next.cleanupQuerySpill()).toMatchObject({
          ownersReclaimed: 0,
          ownersRetained: 1,
        });
        nowMs = Date.parse(left?.expiresAt ?? "") + 1;
        expect(await next.cleanupQuerySpill()).toMatchObject({ ownersReclaimed: 1 });
        expect((await reopened.listTempOwnerIdsPage(null, 16)).records).toEqual([]);

        // And the store still reopens after the cleanup.
        const again = await reopen();
        expect((await again.listTempOwnerIdsPage(null, 16)).records).toEqual([]);
        again.close();
      });
    }
});

describe("the next spilling query after a killed one", () => {
  // The browser sequence behind 0.19.x's "Temp page has no owner": a spilling query dies, its
  // owner expires, and the next spilling query's owner creation sweeps the expired owner. That
  // sweep must take the dead owner's pages too, or the next open refuses the database.
  for (const target of targets)
    it(`${target.name} sweeps the dead query's owner with its pages and reopens`, async () => {
      const { store, reopen } = await target.create();
      const start = Date.parse("2026-01-01T00:00:00.000Z");
      let nowMs = start;
      let stepMs = 1;
      const now = (): Date => new Date((nowMs += stepMs));
      const options = { rowsPerBlock: 256, compression: "raw", spillOwnerLeaseMs: 60_000 } as const;
      const seed = new MinnowDatabase(store, { ...options, now });
      await seed.createTable({
        name: "spilled",
        columns: [
          { name: "id", type: "number" },
          { name: "bucket", type: "number" },
        ],
      });
      const rowCount = 20_000;
      await seed.insertBatch("spilled", {
        columns: {
          id: Array.from({ length: rowCount }, (_, index) => index),
          bucket: Array.from({ length: rowCount }, (_, index) => index % 11),
        },
      });
      const spill = {
        executionMemoryBudgetBytes: 150_000,
        spillPageRows: 64,
        memoize: false,
      } as const;
      const sql = "SELECT id, bucket FROM spilled ORDER BY bucket, id DESC";

      stepMs = 2_000;
      const killed = new MinnowDatabase(killAfterRenewals(store, 1), { ...options, now });
      await expect(killed.query(sql, spill)).rejects.toBeInstanceOf(SimulatedKill);
      stepMs = 1;
      const [dead] = (await store.listTempOwnerIdsPage(null, 16)).records;
      if (dead === undefined) throw new Error("the killed query left no spill owner");
      const left = await store.getTempOwner(dead);
      nowMs = Date.parse(left?.expiresAt ?? "") + 1;

      const next = new MinnowDatabase(store, { ...options, now });
      expect((await next.query(sql, spill)).rows).toHaveLength(rowCount);
      const swept = (await store.getTempOwner(dead)) === undefined;

      // The tab closes and a new one opens the same database.
      const reopened = await reopen();
      // A store whose create swept the dead owner took its pages too: nothing lists it.
      if (swept) expect((await reopened.listTempOwnerIdsPage(null, 16)).records).toEqual([]);
      const after = new MinnowDatabase(reopened, { now });
      expect((await after.query("SELECT COUNT(*) AS n FROM spilled")).rows).toEqual([
        { n: rowCount },
      ]);
      expect(await after.cleanupQuerySpill()).toMatchObject({ ownersRetained: 0 });
      expect((await reopened.listTempOwnerIdsPage(null, 16)).records).toEqual([]);
      (await reopen()).close();
    });
});
