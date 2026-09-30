import { afterEach, expect, it, vi } from "vitest";
import type { DatasetRecord, EngineMaterialization } from "./protocol";
const fake = vi.hoisted(() => ({ open: vi.fn(), close: vi.fn(), pool: vi.fn() }));
vi.mock("./engines/vendored", () => ({
  loadSqlite: async () => async () => ({
    oo1: {
      OpfsDb: class {
        constructor(path: string) {
          fake.open(path);
        }
        isOpen() {
          return true;
        }
        close() {
          fake.close();
        }
      },
    },
    installOpfsSAHPoolVfs: fake.pool,
  }),
}));
const { sqliteDriver } = await import("./engines/sqlite");
afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});
it("selects the public OpfsDb constructor without the removed private opfs property", async () => {
  const session = await sqliteDriver.openSession({
    id: "audit",
    engines: { sqlite: { vfs: "opfs" } },
  } as DatasetRecord);
  expect(fake.open).toHaveBeenCalledWith("/mdb-dataset-audit.sqlite3");
  expect(fake.pool).not.toHaveBeenCalled();
  await session.close();
  expect(fake.close).toHaveBeenCalledOnce();
});
it("removes the database and sidecars through public OPFS and reports deletion failures", async () => {
  const removeEntry = vi.fn(async (name: string) => {
    void name;
  });
  vi.stubGlobal("navigator", { storage: { getDirectory: async () => ({ removeEntry }) } });
  const materialization = {
    storageName: "/mdb-dataset-audit.sqlite3",
    vfs: "opfs",
  } as EngineMaterialization;
  await sqliteDriver.deleteDataset(materialization);
  expect(removeEntry.mock.calls.map(([name]) => name)).toEqual([
    "mdb-dataset-audit.sqlite3",
    "mdb-dataset-audit.sqlite3-wal",
    "mdb-dataset-audit.sqlite3-shm",
  ]);
  removeEntry.mockRejectedValueOnce(new DOMException("not present", "NotFoundError"));
  await sqliteDriver.deleteDataset(materialization);
  removeEntry.mockRejectedValueOnce(new DOMException("denied", "NotAllowedError"));
  await expect(sqliteDriver.deleteDataset(materialization)).rejects.toThrow("denied");
});
