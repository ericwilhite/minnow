/**
 * `{ kind: "auto" }` with nothing remembered for a name: a database that already exists under it
 * — created by an explicit descriptor, or whose memory was lost — decides the store, so the
 * switch to `auto` never reopens it, empty, elsewhere. And a reservation another connection has
 * already opened successfully survives this connection's failed open.
 */
import { IDBFactory } from "fake-indexeddb";
import { afterEach, expect, it } from "vitest";
import { autoStoreTestHooks, openAutoStore, resolveAutoStoreKind } from "./auto-store.js";
import { DatabaseStoreUnavailableError } from "./errors.js";
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { MinnowDatabase } from "./database.js";
import { settled } from "./client-audit-harness.js";

afterEach(() => {
  delete autoStoreTestHooks.opfsAvailable;
  delete autoStoreTestHooks.opfsDatabaseExists;
  delete autoStoreTestHooks.indexedDB;
});

it("opens an existing IndexedDB database of that name when a descriptor switches to auto", async () => {
  const indexedDB = new IDBFactory();
  autoStoreTestHooks.indexedDB = indexedDB;
  autoStoreTestHooks.opfsDatabaseExists = async () => false;
  // Release N: { kind: "indexeddb", name: "app" } — real data lands in IndexedDB.
  const store = await IndexedDbBlockStore.open({ name: "app", indexedDB });
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "people",
    columns: [{ name: "id", type: "number" }],
    uniqueKey: "id",
  });
  await database.insert("people", { id: 1 });
  await database.close();
  expect((await indexedDB.databases()).map((d) => d.name)).toContain("app");
  // Release N+1: { kind: "auto", name: "app" } in a worker with OPFS.
  autoStoreTestHooks.opfsAvailable = async () => true;
  expect(await resolveAutoStoreKind("app")).toEqual({ kind: "indexeddb", reserved: true });
  // The choice is now remembered: the probe is not consulted again.
  expect(await resolveAutoStoreKind("app")).toEqual({ kind: "indexeddb", reserved: false });
});

it("opens an existing OPFS database of that name, and refuses where OPFS cannot open", async () => {
  autoStoreTestHooks.indexedDB = new IDBFactory();
  autoStoreTestHooks.opfsDatabaseExists = async (name) => name === "app";
  // A context without OPFS must not reserve IndexedDB for a database that lives on OPFS.
  autoStoreTestHooks.opfsAvailable = async () => false;
  await expect(resolveAutoStoreKind("app")).rejects.toBeInstanceOf(DatabaseStoreUnavailableError);
  // Nothing was reserved by the refusal: an OPFS-capable context still finds the data.
  autoStoreTestHooks.opfsAvailable = async () => true;
  expect(await resolveAutoStoreKind("app")).toEqual({ kind: "opfs", reserved: true });
  // Another name with no database anywhere still follows the probe.
  autoStoreTestHooks.opfsAvailable = async () => false;
  expect(await resolveAutoStoreKind("fresh")).toEqual({ kind: "indexeddb", reserved: true });
});

it("finds a database by its OPFS directory again after the remembered choice is lost", async () => {
  autoStoreTestHooks.indexedDB = new IDBFactory();
  autoStoreTestHooks.opfsAvailable = async () => true;
  let opfsHolds = false;
  autoStoreTestHooks.opfsDatabaseExists = async () => opfsHolds;
  expect((await resolveAutoStoreKind("db")).kind).toBe("opfs");
  opfsHolds = true;
  autoStoreTestHooks.indexedDB = new IDBFactory(); // the choice database is gone
  expect(await resolveAutoStoreKind("db")).toEqual({ kind: "opfs", reserved: true });
  // Memory gone AND OPFS gone in this context: refused, never silently IndexedDB and empty.
  autoStoreTestHooks.indexedDB = new IDBFactory();
  autoStoreTestHooks.opfsAvailable = async () => false;
  await expect(resolveAutoStoreKind("db")).rejects.toBeInstanceOf(DatabaseStoreUnavailableError);
});

it("keeps a reservation another connection opened when this connection's open fails", async () => {
  autoStoreTestHooks.indexedDB = new IDBFactory();
  autoStoreTestHooks.opfsAvailable = async () => true;
  let opfsHolds = false;
  autoStoreTestHooks.opfsDatabaseExists = async () => opfsHolds;
  // Tab A resolves first and reserves OPFS, then its open fails transiently.
  const a = openAutoStore("db", async () => {
    await settled(40);
    throw new Error("transient OPFS open failure");
  });
  await settled(10);
  // Tab B reads A's reservation and opens on OPFS successfully; it now holds data there.
  const b = await openAutoStore("db", async (kind) => {
    opfsHolds = true;
    return kind;
  });
  expect(b.kind).toBe("opfs");
  await expect(a).rejects.toThrow("transient OPFS open failure");
  // A's failure did not delete the record B relies on: the choice stands...
  expect(await resolveAutoStoreKind("db")).toEqual({ kind: "opfs", reserved: false });
  // ...and a context that cannot open OPFS is refused rather than reserving IndexedDB.
  autoStoreTestHooks.opfsAvailable = async () => false;
  await expect(resolveAutoStoreKind("db")).rejects.toBeInstanceOf(DatabaseStoreUnavailableError);
});

it("releases a reservation whose failed open left no database behind", async () => {
  autoStoreTestHooks.indexedDB = new IDBFactory();
  autoStoreTestHooks.opfsAvailable = async () => true;
  autoStoreTestHooks.opfsDatabaseExists = async () => false;
  await expect(
    openAutoStore("db", async () => {
      throw new Error("open failed");
    }),
  ).rejects.toThrow("open failed");
  // Nothing held data, so the name is free to be chosen afresh — here by a context without OPFS.
  autoStoreTestHooks.opfsAvailable = async () => false;
  expect(await resolveAutoStoreKind("db")).toEqual({ kind: "indexeddb", reserved: true });
});
