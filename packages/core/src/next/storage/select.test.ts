import { describe, expect, it } from "vitest";
import { chooseBackend, type StoragePlatform } from "./select.js";
import { SimLocks } from "./sim-disk.js";

const named = (name: string) => Object.assign(new Error(name), { name });
const directory = {
  getDirectoryHandle: async () => directory,
  getFileHandle: async () => ({ createSyncAccessHandle: async () => ({}) }),
};
class SyncCapableHandle {
  createSyncAccessHandle(): string {
    return "handle";
  }
}

describe("backend choice", () => {
  it("takes OPFS where a dedicated worker can open sync handles", async () => {
    const platform: StoragePlatform = {
      navigator: { storage: { getDirectory: async () => directory } },
      FileSystemFileHandle: SyncCapableHandle,
      indexedDB: {} as IDBFactory,
    };
    const chosen = await chooseBackend("db", platform, new SimLocks());
    expect(chosen.kind).toBe("opfs");
    expect(chosen.backend.fence).toBe("handle");
  });

  it("falls back to IndexedDB when OPFS is refused (Firefox and Safari private browsing)", async () => {
    for (const refusal of ["SecurityError", "UnknownError"]) {
      const chosen = await chooseBackend(
        "db",
        {
          navigator: {
            storage: {
              getDirectory: () => Promise.reject(named(refusal)),
            },
          },
          FileSystemFileHandle: SyncCapableHandle,
          indexedDB: {} as IDBFactory,
        },
        new SimLocks(),
      );
      expect(chosen).toMatchObject({ kind: "indexeddb", opfsRefused: refusal });
      expect(chosen.backend.fence).toBe("epoch");
    }
    const noSyncHandles = await chooseBackend(
      "db",
      {
        navigator: { storage: { getDirectory: async () => directory } },
        FileSystemFileHandle: { prototype: {} },
        indexedDB: {} as IDBFactory,
      },
      new SimLocks(),
    );
    expect(noSyncHandles).toMatchObject({ kind: "indexeddb", opfsRefused: "unsupported" });
  });

  it("is memory only where neither exists (Safari Lockdown Mode)", async () => {
    const chosen = await chooseBackend("db", {}, new SimLocks());
    expect(chosen.kind).toBe("memory");
    const lease = await chosen.backend.acquire();
    await lease.release();
  });
});
