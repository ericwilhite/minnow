/**
 * The interaction-plan simulator with primary-key changes, retired keys, and self-references,
 * over every block store. Split from interaction-simulator.test.ts so its seeds run in parallel
 * with the other suites; replay a seed with MINNOW_SEED.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, OpfsBlockStore } from "../storage/index.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import {
  createDatabaseDriver,
  generateInteractionPlan,
  runInteractionPlan,
  type DriverStoreSource,
} from "./interaction-simulator.js";
import { MemoryOpfs } from "./opfs-shim.js";
import { seedsFor } from "./seeds.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/**
 * Memory has no durable identity to share, so its connections share one instance. IndexedDB and
 * OPFS open a separate adapter instance per connection over one database, as separate tabs do.
 * (The same list as interaction-simulator.test.ts: kept per file so the suites run in parallel.)
 */
const stores: ReadonlyArray<{ name: string; source: () => DriverStoreSource }> = [
  { name: "memory", source: () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    source: () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      return () => IndexedDbBlockStore.open({ name, indexedDB });
    },
  },
  {
    name: "opfs",
    source: () => {
      const root = new MemoryOpfs().root;
      const name = crypto.randomUUID();
      return () => OpfsBlockStore.open({ name, root });
    },
  },
];

describe.each(stores)("interaction simulator with rekeying over $name", ({ source }) => {
  it.each(seedsFor("interaction-simulator-rekeying", [0x4e4b, 0xbeef]))("seed %i", async (seed) => {
    const store = source();
    try {
      const plan = generateInteractionPlan(seed, {
        length: 200,
        connections: 3,
        tables: 2,
        keySpace: 16,
        rekeying: true,
      });
      expect(plan.interactions.some((interaction) => interaction.kind === "rekey")).toBe(true);
      const result = await runInteractionPlan(
        plan,
        createDatabaseDriver(store, { databaseOptions: { rowsPerBlock: 8 } }),
      );
      expect(result.interactions).toBe(plan.interactions.length);
      expect(result.acceptedWrites).toBeGreaterThan(20);
      expect(result.rejectedConflicts).toBe(0);
    } finally {
      if (typeof store !== "function") store.close();
    }
  });
});
