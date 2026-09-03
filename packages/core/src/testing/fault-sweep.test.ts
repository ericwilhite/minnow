/**
 * The fault sweep: a fixed write workload, run once per fault point, with the fault moved one
 * operation further along each time.
 *
 * This is the shape of SQLite's I/O-error and OOM testing. A hand-written fault test proves the
 * engine survives a failure *somewhere*; it cannot prove there is no single operation whose
 * interruption corrupts the database, because nobody writes out that list by hand. The sweep
 * does: it counts the operations a clean run performs, then fails the Nth one for every N, and
 * after each interruption reopens the store and asks whether what survived is coherent.
 *
 * The invariant under test is atomicity, not durability. A write interrupted mid-flight may be
 * present or absent -- both are correct, and which one depends on where the fault landed. What is
 * never correct is a torn state: a row that is half-written, a table that reads back a value
 * nobody inserted, a committed batch that arrived in pieces. So every check below is written as
 * "the surviving state is one of the states a caller could have observed", never "the write
 * landed".
 */
import { describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "./opfs-shim.js";
import { MinnowDatabase } from "../engine/database.js";
import { FaultInjectingBlockStore, faultPoints, type FaultPoint } from "./index.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/** Every row the workload could possibly have written, keyed by id. */
const REGIONS = ["west", "east", "north", "south"] as const;
const ROWS = Array.from({ length: 12 }, (_, index) => ({
  id: index + 1,
  region: REGIONS[index % REGIONS.length] ?? "west",
  amount: (index + 1) * 10,
}));
const BY_ID = new Map(ROWS.map((row) => [row.id, row]));

/**
 * The fault points this workload actually reaches, pinned.
 *
 * Manifest publication is absent on purpose: `MinnowDatabase` commits transactions rather than
 * publishing manifests directly, so those two points are unreachable through the public write
 * API and the transaction suite owns them instead. Pinning the set is what keeps that an
 * observation rather than an assumption -- if a change starts or stops routing writes through a
 * point, this fails and someone decides whether the sweep should follow.
 */
const EXPECTED_POINTS: readonly FaultPoint[] = [
  "beforeBlockWrite",
  "afterBlockWrite",
  "beforeBlockRead",
  "afterBlockRead",
  "beforeTransactionCommit",
  "afterTransactionCommit",
];

/** The amount an update sets, so a half-applied update is distinguishable from either end state. */
const UPDATED_AMOUNT = 999;

async function createSchema(database: MinnowDatabase): Promise<void> {
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
}

/**
 * The workload. Deliberately several separate writes rather than one: the interesting faults are
 * the ones that land between two operations that a reader might expect to see together.
 */
async function workload(database: MinnowDatabase): Promise<void> {
  await database.insertBatch("items", ROWS.slice(0, 6));
  await database.insertBatch("items", ROWS.slice(6));
  // The queries are not incidental: reads are how the block-read fault points are reached at
  // all, and a fault during a read is the case where a torn block would surface as a wrong
  // answer rather than an error.
  await database.query("SELECT COUNT(*) AS n FROM items");
  await database.updateBatch("items", { keys: [1], changes: { amount: [UPDATED_AMOUNT] } });
  await database.query("SELECT id FROM items WHERE amount > 50 ORDER BY id");
  await database.deleteBatch("items", { keys: [4] });
}

interface StoreKind {
  readonly name: string;
  readonly create: () => Promise<{ store: BlockStore; reopen: () => Promise<BlockStore> }>;
}

const storeKinds: StoreKind[] = [
  {
    name: "memory",
    create: async () => {
      const store = new MemoryBlockStore();
      // A memory store *is* its own persistence: reopening returns the same instance, which is
      // still the right question -- does the state left behind read back coherently?
      return { store, reopen: () => Promise.resolve(store) };
    },
  },
  {
    name: "indexeddb",
    create: async () => {
      // One factory shared across open and reopen, so the reopen sees the bytes the interrupted
      // run actually left on disk rather than a fresh database.
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      const store = await IndexedDbBlockStore.open({ name, indexedDB });
      return {
        store,
        reopen: async () => {
          store.close();
          return IndexedDbBlockStore.open({ name, indexedDB });
        },
      };
    },
  },
  {
    name: "opfs",
    create: async () => {
      // One shim shared across open and reopen, so the reopen replays the command log the
      // interrupted run actually left behind.
      const shim = new MemoryOpfs();
      const name = crypto.randomUUID();
      const store = await OpfsBlockStore.open({ name, root: shim.root });
      return {
        store,
        reopen: async () => {
          store.close();
          return OpfsBlockStore.open({ name, root: shim.root });
        },
      };
    },
  },
];

/** Counts how many times each fault point is reached by an uninterrupted run. */
async function countOperations(kind: StoreKind): Promise<Map<FaultPoint, number>> {
  const counts = new Map<FaultPoint, number>(faultPoints.map((point) => [point, 0]));
  const { store } = await kind.create();
  const counting = new FaultInjectingBlockStore(store, (point) => {
    counts.set(point, (counts.get(point) ?? 0) + 1);
  });
  const database = new MinnowDatabase(counting, { rowsPerBlock: 2 });
  await createSchema(database);
  await workload(database);
  return counts;
}

/**
 * Reads the table back and asserts the state is one a caller could have observed.
 *
 * Returns the surviving row count so the caller can report coverage; throws through `expect` if
 * the state is torn. A read that fails outright is also a failure: an interrupted write may lose
 * data, but it may not leave the database unopenable.
 */
async function assertCoherent(store: BlockStore, label: string): Promise<number> {
  const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
  const result = await database.query("SELECT id, region, amount FROM items ORDER BY id");
  const rows = result.rows as Array<{ id: number; region: string; amount: number }>;

  const ids = rows.map((row) => row.id);
  expect(new Set(ids).size, `${label}: duplicate ids survived ${JSON.stringify(ids)}`).toBe(
    ids.length,
  );

  for (const row of rows) {
    const original = BY_ID.get(row.id);
    // Every surviving row must be one this workload could have written. A row that is not is the
    // signature of a torn block: fields recombined across writes.
    expect(original, `${label}: unknown id ${String(row.id)}`).toBeDefined();
    if (original === undefined) continue;
    expect(row.region, `${label}: id ${String(row.id)} has a region nobody wrote`).toBe(
      original.region,
    );
    const allowed = row.id === 1 ? [original.amount, UPDATED_AMOUNT] : [original.amount];
    expect(
      allowed,
      `${label}: id ${String(row.id)} has amount ${String(row.amount)}, not one of ${JSON.stringify(allowed)}`,
    ).toContain(row.amount);
  }
  return rows.length;
}

describe("fault sweep", () => {
  for (const kind of storeKinds) {
    it(`leaves a coherent database when any single operation fails (${kind.name})`, async () => {
      const counts = await countOperations(kind);
      // A workload that never reaches a fault point would make this suite pass by doing
      // nothing, which is the failure mode a sweep is most likely to rot into.
      const reachable = faultPoints.filter((point) => (counts.get(point) ?? 0) > 0);
      expect(reachable).toEqual(EXPECTED_POINTS);

      const observed = new Set<number>();
      let injections = 0;
      for (const point of reachable) {
        const total = counts.get(point) ?? 0;
        for (let nth = 1; nth <= total; nth += 1) {
          const { store, reopen } = await kind.create();
          let seen = 0;
          const failing = new FaultInjectingBlockStore(store, (at) => {
            if (at !== point) return;
            seen += 1;
            if (seen === nth) throw new Error(`injected ${point} #${String(nth)}`);
          });
          const database = new MinnowDatabase(failing, { rowsPerBlock: 2 });
          // The schema itself must exist for the read-back to mean anything. If creating it is
          // what got interrupted, there is nothing to check and nothing wrong with that.
          try {
            await createSchema(database);
          } catch {
            continue;
          }
          // The workload is expected to fail here -- that is the point. What happens after it
          // is what this test is about.
          await workload(database).catch(() => undefined);
          injections += 1;

          const label = `${kind.name} ${point} #${String(nth)}`;
          observed.add(await assertCoherent(await reopen(), label));
        }
      }

      // The sweep is only meaningful if it actually injected faults across the workload, and
      // only interesting if those faults produced more than one outcome: a run where every
      // injection leaves an identical database is one where the faults are landing somewhere
      // that does not matter.
      expect(injections, "the sweep injected nothing").toBeGreaterThan(20);
      expect(
        observed.size,
        `every injection left the same ${String([...observed])} rows -- faults are not landing on writes`,
      ).toBeGreaterThan(1);
    });
  }
});
