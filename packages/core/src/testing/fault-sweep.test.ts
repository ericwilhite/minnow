import { describe, expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { IndexedDbBlockStore, MemoryBlockStore, OpfsBlockStore } from "../storage/index.js";
import { MemoryOpfs } from "./opfs-shim.js";
import { assertFaultSweepState, runFaultSweep, type FaultSweepStore } from "./fault-sweep.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";

const factories: Record<string, () => Promise<FaultSweepStore>> = {
  memory: () => {
    const store = new MemoryBlockStore();
    return Promise.resolve({
      store,
      reopen: () => Promise.resolve(store),
      cleanup: () => Promise.resolve(),
    });
  },
  indexeddb: async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    return {
      store,
      reopen: () => {
        store.close();
        return IndexedDbBlockStore.open({ name, indexedDB });
      },
      cleanup: () => Promise.resolve(),
    };
  },
  opfs: async () => {
    const root = new MemoryOpfs().root;
    const name = crypto.randomUUID();
    const store = await OpfsBlockStore.open({ name, root });
    return {
      store,
      reopen: () => {
        store.close();
        return OpfsBlockStore.open({ name, root });
      },
      cleanup: () => Promise.resolve(),
    };
  },
};

describe("fault sweep", () => {
  for (const [kind, create] of Object.entries(factories)) {
    it(
      `preserves acknowledged writes and statement atomicity (${kind})`,
      async () => {
        const result = await runFaultSweep(create);
        expect(result.injections).toBe(Object.values(result.counts).reduce((a, b) => a + b, 0));
        expect(result.injections).toBeGreaterThan(20);
        expect(result.outcomes.length).toBeGreaterThan(1);
      },
      heavyTestTimeout(120_000),
    );
  }

  it("rejects partial batches, lost acknowledgements, and failures of read-only statements that change data", () => {
    const first = { id: 1, region: "west", amount: 10 };
    expect(() => assertFaultSweepState([first], 0, true)).toThrow("Invalid durable state");
    expect(() => assertFaultSweepState([], 1, true)).toThrow("Invalid durable state");
    expect(() => assertFaultSweepState([], 2, true)).toThrow("Invalid durable state");
    expect(() => assertFaultSweepState([], 6, false)).toThrow("Invalid durable state");
  });
});
