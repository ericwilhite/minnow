const fenceCounters = (bytes: Uint8Array): { epoch: number; generation: number } => {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return {
    epoch: view.getUint32(4, true) + view.getUint32(8, true) * 2 ** 32,
    generation: view.getUint32(16, true) + view.getUint32(20, true) * 2 ** 32,
  };
};
import { IDBFactory } from "fake-indexeddb";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { IDB_STORES, openIndexedDb } from "../storage/idb-like.js";
import type { LockManagerLike } from "../storage/fence.js";
export const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

export async function fixture(
  kind: "IDB" | "OPFS",
  locks: LockManagerLike,
  beforeFence?: () => Promise<void>,
) {
  if (kind === "OPFS") {
    const shim = new MemoryOpfs();
    return {
      backend: () =>
        createOpfsBackend({
          name: "gate-race",
          locks,
          // Native Chromium/Firefox permit Blob snapshots while a SAH is held. The strict
          // cross-browser shim refuses them, so model that independently probed native API here.
          directory: {
            keys: () => shim.root.keys(),
            getFileHandle: async (name, options) => {
              const handle = await shim.root.getFileHandle(name, options);
              return {
                createSyncAccessHandle: async () => {
                  await beforeFence?.();
                  return handle.createSyncAccessHandle();
                },
                getFile: async () =>
                  new Blob([new Uint8Array(shim.readFileBytes(name) ?? []).buffer]),
              };
            },
          },
        }),
      counters: async () => {
        const bytes = shim.readFileBytes("epoch");
        if (bytes === undefined) throw new Error("missing fence");
        const first = fenceCounters(bytes.subarray(0, 40));
        const second = bytes.length >= 552 ? fenceCounters(bytes.subarray(512, 552)) : first;
        return first.generation >= second.generation ? first : second;
      },
      image: async () => {
        const entries: Array<readonly [string, Uint8Array | undefined]> = [];
        for await (const name of shim.root.keys()) entries.push([name, shim.readFileBytes(name)]);
        return entries.sort(([a], [b]) => a.localeCompare(b));
      },
    };
  }
  const factory = new IDBFactory();
  return {
    backend: () =>
      createIndexedDbBackend({
        name: "gate-race",
        locks,
        database: async () => {
          const db = await openIndexedDb(factory, "gate-race");
          if (beforeFence === undefined) return db;
          return {
            close: () => db.close(),
            transaction: async (stores, mode, durability, body) => {
              if (mode === "readwrite" && stores.includes("roots") && stores.includes("meta"))
                await beforeFence();
              await db.transaction(stores, mode, durability, body);
            },
          } satisfies import("../storage/idb-like.js").IdbDatabaseLike;
        },
      }),
    counters: async () => {
      const db = await openIndexedDb(factory, "gate-race");
      let counters: { epoch: number; generation: number } | undefined;
      try {
        await db.transaction(["meta"], "readonly", "relaxed", (tx) =>
          tx.store("meta").get("epoch", (value) => {
            counters = fenceCounters(new Uint8Array(value as ArrayBuffer));
          }),
        );
      } finally {
        db.close();
      }
      if (counters === undefined) throw new Error("missing fence");
      return counters;
    },
    image: async () => {
      const db = await openIndexedDb(factory, "gate-race");
      const image: Record<string, unknown> = {};
      try {
        await db.transaction(IDB_STORES, "readonly", "relaxed", (tx) => {
          for (const name of IDB_STORES) {
            tx.store(name).getAllKeys({}, (keys) => {
              image[`${name}:keys`] = keys;
            });
            tx.store(name).getAll({}, (values) => {
              image[`${name}:values`] = values;
            });
          }
        });
        return image;
      } finally {
        db.close();
      }
    },
  };
}
