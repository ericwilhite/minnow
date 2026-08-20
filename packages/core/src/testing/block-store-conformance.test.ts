/**
 * The conformance kit run against every first-party adapter — which is both the proof that
 * the adapters honor the published contract and the guard that keeps the kit itself honest:
 * a kit case no store can pass, or one that stops compiling against the interface, fails
 * here before it fails a third-party implementor.
 */
import { describe, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { IndexedDbBlockStore, MemoryBlockStore, OpfsBlockStore } from "../storage/index.js";
import { MemoryOpfs } from "./opfs-shim.js";
import { blockStoreConformanceCases, type BlockStoreConformanceTarget } from "./index.js";

const targets: Array<{ name: string; target: BlockStoreConformanceTarget }> = [
  {
    name: "memory",
    target: {
      create: async () => new MemoryBlockStore(),
      // A memory store is its own persistence; handing the same instance back is honest.
      reopen: async (store) => store,
    },
  },
  {
    name: "indexeddb",
    target: (() => {
      const factories = new Map<IndexedDbBlockStore, { indexedDB: IDBFactory; name: string }>();
      return {
        create: async () => {
          const indexedDB = new IDBFactory();
          const name = crypto.randomUUID();
          const store = await IndexedDbBlockStore.open({ name, indexedDB });
          factories.set(store, { indexedDB, name });
          return store;
        },
        reopen: async (store) => {
          const opened = factories.get(store as IndexedDbBlockStore);
          if (opened === undefined) throw new Error("unknown store");
          store.close();
          return IndexedDbBlockStore.open(opened);
        },
      } satisfies BlockStoreConformanceTarget;
    })(),
  },
  {
    name: "opfs",
    target: (() => {
      const roots = new Map<OpfsBlockStore, { root: FileSystemDirectoryHandle; name: string }>();
      return {
        create: async () => {
          const shim = new MemoryOpfs();
          const name = crypto.randomUUID();
          const store = await OpfsBlockStore.open({ name, root: shim.root });
          roots.set(store, { root: shim.root, name });
          return store;
        },
        reopen: async (store) => {
          const opened = roots.get(store as OpfsBlockStore);
          if (opened === undefined) throw new Error("unknown store");
          store.close();
          return OpfsBlockStore.open(opened);
        },
      } satisfies BlockStoreConformanceTarget;
    })(),
  },
];

for (const { name, target } of targets) {
  describe(`block store conformance (${name})`, () => {
    for (const conformanceCase of blockStoreConformanceCases()) {
      it(conformanceCase.name, () => conformanceCase.run(target));
    }
  });
}
