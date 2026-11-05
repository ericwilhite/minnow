import { describe, expect, it } from "vitest";
import { encodeSegment } from "../../storage/opfs/files.js";
import {
  LegacyDataError,
  StoreConflictError,
  chooseBackend,
  encodeLegacySegment,
  type StoragePlatform,
} from "./select.js";
import { SimLocks } from "./sim-disk.js";

const named = (name: string) => Object.assign(new Error(name), { name });

/** An OPFS directory tree: lookups without `create` throw NotFoundError, as the platform. */
interface FakeDirectory {
  readonly children: Map<string, FakeDirectory>;
  getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<FakeDirectory>;
  getFileHandle(): Promise<{ createSyncAccessHandle(): Promise<object> }>;
}
function directory(): FakeDirectory {
  const children = new Map<string, FakeDirectory>();
  return {
    children,
    getDirectoryHandle: async (name, options) => {
      const found = children.get(name);
      if (found !== undefined) return found;
      if (options?.create !== true) throw named("NotFoundError");
      const created = directory();
      children.set(name, created);
      return created;
    },
    getFileHandle: async () => ({ createSyncAccessHandle: async () => ({}) }),
  };
}

/** An IDBFactory whose `open` fires upgradeneeded for a missing database and honours its abort. */
function factory(databases: Map<string, string[]>): IDBFactory {
  return {
    open(name: string) {
      const request: Record<string, unknown> = {};
      queueMicrotask(() => {
        const stores = databases.get(name);
        if (stores !== undefined) {
          request.result = { objectStoreNames: stores, close: () => undefined };
          (request.onsuccess as () => void)();
          return;
        }
        const upgrade = { aborted: false };
        request.transaction = { abort: () => (upgrade.aborted = true) };
        (request.onupgradeneeded as () => void)();
        if (upgrade.aborted) {
          request.error = named("AbortError");
          (request.onerror as (event: { preventDefault(): void }) => void)({
            preventDefault: () => undefined,
          });
        } else {
          databases.set(name, []);
          request.result = { objectStoreNames: [], close: () => undefined };
          (request.onsuccess as () => void)();
        }
      });
      return request;
    },
  } as unknown as IDBFactory;
}

class SyncCapableHandle {
  createSyncAccessHandle(): string {
    return "handle";
  }
}

const withOpfs = (
  root: FakeDirectory,
  databases = new Map<string, string[]>(),
): StoragePlatform => ({
  navigator: { storage: { getDirectory: async () => root } },
  FileSystemFileHandle: SyncCapableHandle,
  indexedDB: factory(databases),
});

describe("backend choice", () => {
  it("takes OPFS where a dedicated worker can open sync handles", async () => {
    const root = directory();
    const chosen = await chooseBackend("db", withOpfs(root), new SimLocks());
    expect(chosen.kind).toBe("opfs");
    expect(chosen.backend.fence).toBe("handle");
    expect([...root.children.keys()]).toEqual(["minnow-core"]);
  });

  it("falls back to IndexedDB only on the private modes' refusals, or with no OPFS API", async () => {
    for (const refusal of ["SecurityError", "UnknownError", "NotSupportedError"]) {
      const chosen = await chooseBackend(
        "db",
        {
          navigator: { storage: { getDirectory: () => Promise.reject(named(refusal)) } },
          FileSystemFileHandle: SyncCapableHandle,
          indexedDB: factory(new Map()),
        },
        new SimLocks(),
      );
      expect(chosen).toMatchObject({ kind: "indexeddb", opfsRefused: refusal });
    }
    const noSyncHandles = await chooseBackend(
      "db",
      { ...withOpfs(directory()), FileSystemFileHandle: { prototype: {} } },
      new SimLocks(),
    );
    expect(noSyncHandles).toMatchObject({ kind: "indexeddb", opfsRefused: "unsupported" });
  });

  it("propagates quota and unexpected OPFS errors instead of opening an empty store (finding 2)", async () => {
    for (const failure of ["QuotaExceededError", "InvalidStateError", "TypeError"])
      await expect(
        chooseBackend(
          "db",
          {
            navigator: { storage: { getDirectory: () => Promise.reject(named(failure)) } },
            FileSystemFileHandle: SyncCapableHandle,
            indexedDB: factory(new Map()),
          },
          new SimLocks(),
        ),
        failure,
      ).rejects.toThrow(failure);
    const root = directory();
    root.getDirectoryHandle = () => Promise.reject(named("QuotaExceededError"));
    await expect(chooseBackend("db", withOpfs(root), new SimLocks())).rejects.toThrow(
      "QuotaExceededError",
    );
  });

  it("refuses the pre-reset engine's data, in OPFS or IndexedDB, before creating anything (finding 3)", async () => {
    const name = "orders é/1";
    const root = directory();
    await (
      await root.getDirectoryHandle("minnowdb", { create: true })
    ).getDirectoryHandle(encodeSegment(name), { create: true });
    const opfs = chooseBackend(name, withOpfs(root), new SimLocks());
    await expect(opfs).rejects.toThrow(LegacyDataError);
    await expect(opfs).rejects.toMatchObject({
      store: "opfs",
      location: `minnowdb/${encodeSegment(name)}`,
    });
    expect([...root.children.keys()]).toEqual(["minnowdb"]);

    const databases = new Map([[name, ["catalog", "manifests", "segments", "blocks", "temp"]]]);
    const idb = chooseBackend(name, withOpfs(directory(), databases), new SimLocks());
    await expect(idb).rejects.toMatchObject({
      name: "LegacyDataError",
      store: "indexeddb",
      location: name,
    });
    expect([...databases.keys()]).toEqual([name]);
    // An unrelated database of the same name is not the old engine's.
    const unrelated = new Map([[name, ["notes"]]]);
    expect((await chooseBackend(name, withOpfs(directory(), unrelated), new SimLocks())).kind).toBe(
      "opfs",
    );
  });

  it("never opens OPFS over this database's IndexedDB store (finding 2)", async () => {
    const databases = new Map([["minnow-core:db", ["meta", "roots", "data", "log", "spill"]]]);
    await expect(
      chooseBackend("db", withOpfs(directory(), databases), new SimLocks()),
    ).rejects.toThrow(StoreConflictError);
  });

  it("encodes legacy names exactly as the pre-reset engine did", () => {
    for (const name of ["db", ".hidden", "a.b", "spaces and/slashes", "é", "日本", "x%y", "-_."])
      expect(encodeLegacySegment(name), name).toBe(encodeSegment(name));
  });

  it("is memory only where neither exists (Safari Lockdown Mode)", async () => {
    const chosen = await chooseBackend("db", {}, new SimLocks());
    expect(chosen.kind).toBe("memory");
    const lease = await chosen.backend.acquire();
    await lease.release();
  });
});
