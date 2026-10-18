/**
 * The interaction-plan simulator with UNIQUE and foreign-key constraints, schema changes on
 * populated tables, full-text search, and commit bursts, over every block store. Split from
 * interaction-simulator.test.ts so its seeds run in parallel with the other suites; replay a seed
 * with MINNOW_SEED.
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

/**
 * fake-indexeddb runs these plans several times slower than the other stores, and Vitest runs one
 * file's tests in sequence, so IndexedDB's seeds alternate with
 * interaction-simulator-schema-indexeddb.test.ts: this file runs the even-indexed ones. A seed
 * chosen with MINNOW_SEED is index 0, so it runs here on every store.
 */
const schemaSeeds = seedsFor("interaction-simulator-schema", [0x5c4e, 0xd00d]);
const seedsOver = (name: string): number[] =>
  name === "indexeddb" ? schemaSeeds.filter((_, index) => index % 2 === 0) : schemaSeeds;

describe.each(stores)(
  "interaction simulator with constraints and schema changes over $name",
  ({ name, source }) => {
    it.each(seedsOver(name))("seed %i", async (seed) => {
      const store = source();
      try {
        const plan = generateInteractionPlan(seed, {
          length: 200,
          connections: 3,
          tables: 3,
          keySpace: 16,
          constraints: true,
          schemaChanges: true,
          fullText: true,
          burstCommits: 60,
        });
        const result = await runInteractionPlan(
          plan,
          createDatabaseDriver(store, {
            databaseOptions: { rowsPerBlock: 8, ftsAutoIndexRows: 8 },
          }),
        );
        expect(result.interactions).toBe(plan.interactions.length);
        expect(result.acceptedWrites).toBeGreaterThan(20);
        expect(result.expectedFailures).toBeGreaterThan(0);
        expect(result.rejectedConflicts).toBe(0);
        expect(result.burstCommits).toBeGreaterThanOrEqual(30);
        // The lookups are shaped for the secondary-index path; some must have taken it.
        expect(result.indexPathQueries).toBeGreaterThan(0);
      } finally {
        if (typeof store !== "function") store.close();
      }
    });
  },
);
