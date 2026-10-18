/**
 * The odd-indexed seeds of the constraints-and-schema simulator suite over IndexedDB. They run in
 * a file of their own so Vitest runs them beside interaction-simulator-schema.test.ts, which runs
 * the even-indexed ones and the other stores; the plan, the driver, and the assertions are the
 * same there. Replay a seed with MINNOW_SEED (it then runs in the main schema file).
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { IndexedDbBlockStore } from "../storage/index.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import {
  createDatabaseDriver,
  generateInteractionPlan,
  runInteractionPlan,
  type DriverStoreSource,
} from "./interaction-simulator.js";
import { seedsFor } from "./seeds.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/** A separate adapter instance per connection over one database, as separate tabs do. */
function indexedDbSource(): DriverStoreSource {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  return () => IndexedDbBlockStore.open({ name, indexedDB });
}

// The defaults must match interaction-simulator-schema.test.ts.
const suite = "interaction-simulator-schema";
const oddSeeds = seedsFor(suite, [0x5c4e, 0xd00d]).filter((_, index) => index % 2 === 1);

describe("interaction simulator with constraints and schema changes over 'indexeddb' (odd seeds)", () => {
  if (oddSeeds.length === 0) {
    it.skip("has no odd-indexed seed when MINNOW_SEED picks one", () => undefined);
    return;
  }
  it.each(oddSeeds)("seed %i", async (seed) => {
    const store = indexedDbSource();
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
});
