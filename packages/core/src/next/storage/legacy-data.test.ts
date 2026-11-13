/* eslint-disable no-restricted-imports -- Node-only law reads the released writers' frozen artifacts; never shipped. */
/** The 1.0 reset refuses 0.x data before creating anything (next/README.md). */
import { readFileSync, readdirSync } from "node:fs";
import { serialize } from "node:v8";
import { gunzipSync } from "node:zlib";
import fc from "fast-check";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { IndexedDbBlockStore } from "../../storage/indexeddb.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import {
  createReach,
  LEGACY_REFUSAL_REACH,
  LEGACY_PARTIAL_TREE_REACH,
  LEGACY_HELD_WRITER_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { openIndexedDb } from "./idb-like.js";
import { createOpfsBackend } from "./opfs.js";
import { chooseBackend, encodeLegacySegment, LegacyDataError } from "./select.js";
import { SimLocks } from "./sim-disk.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;
const directory = new URL("../../../format-fixtures/", import.meta.url);
const files = readdirSync(directory);
const snapshots = files.filter((name) => name.endsWith(".minnow"));
const frozen = files
  .filter((name) => /^opfs-layout\d+\.json$/u.test(name))
  .map((name) => {
    const fixture = JSON.parse(readFileSync(new URL(name, directory), "utf8")) as {
      layoutFormatVersion: number;
      files: Record<string, string>;
    };
    return fixture;
  });
class SyncHandle {
  createSyncAccessHandle(): void {
    /* Capability only; the shim supplies the real handles. */
  }
}
const names = fc
  .array(fc.constantFrom("a", ".", "%", "/", "é", "😀"), { minLength: 1, maxLength: 8 })
  .map((parts) => parts.join(""));

async function refused(action: () => Promise<unknown>, store: "opfs" | "indexeddb"): Promise<void> {
  let failure: unknown;
  try {
    await action();
  } catch (error) {
    failure = error;
  }
  expect(failure).toBeInstanceOf(LegacyDataError);
  expect(failure).toMatchObject({ name: "LegacyDataError", code: "MINNOW_LEGACY_DATA", store });
  expect((failure as Error).message).toContain("Minnow 0.x");
  expect((failure as Error).message).toContain("1.0 does not open");
}

/** Includes empty directories and every file byte, so even namespace creation is observable. */
async function tree(root: FileSystemDirectoryHandle, prefix = ""): Promise<unknown[]> {
  const result: unknown[] = [];
  for await (const [name, handle] of root.entries()) {
    const path = `${prefix}/${name}`;
    if (handle.kind === "directory")
      result.push([path, await tree(handle as FileSystemDirectoryHandle, path)]);
    else
      result.push([
        path,
        Buffer.from(
          await (await (handle as FileSystemFileHandle).getFile()).arrayBuffer(),
        ).toString("base64"),
      ]);
  }
  return result;
}

async function openNext(
  name: string,
  shim: MemoryOpfs | null,
  sync: boolean,
  factory: IDBFactory,
): Promise<void> {
  const { backend } = await chooseBackend(
    name,
    {
      indexedDB: factory,
      ...(shim === null
        ? {}
        : {
            navigator: { storage: { getDirectory: async () => shim.root } },
            FileSystemFileHandle: sync ? SyncHandle : { prototype: {} },
          }),
    },
    new SimLocks(),
  );
  const lease = await backend.acquire();
  try {
    const session = await backend.open(lease);
    try {
      await session.readRoot();
    } finally {
      await session.close();
    }
  } finally {
    await lease.release();
  }
}

async function native(
  factory: IDBFactory,
  name: string,
  version?: number,
  create?: (db: IDBDatabase) => void,
): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = factory.open(name, version);
    request.onupgradeneeded = () => create?.(request.result);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("Cannot open fixture"));
  });
}
async function image(factory: IDBFactory, name: string): Promise<unknown> {
  const db = await native(factory, name);
  try {
    const stores = Array.from(db.objectStoreNames);
    const records = await Promise.all(
      stores.map(
        (store) =>
          new Promise((resolve, reject) => {
            const tx = db.transaction(store, "readonly");
            const values = tx.objectStore(store).getAll();
            const keys = tx.objectStore(store).getAllKeys();
            tx.oncomplete = () => resolve([store, keys.result, values.result]);
            tx.onabort = () => reject(tx.error ?? new Error("Cannot read fixture"));
          }),
      ),
    );
    return serialize([db.version, records]).toString("hex");
  } finally {
    db.close();
  }
}

for (const seed of seedsFor("next-legacy-refusal", [0xb201, 0xb202])) {
  it(`refuses frozen OPFS trees, all O5–O13 markers and legacy layout without mutation seed ${String(seed)}`, async () => {
    const reach = createReach("legacy OPFS refusal", LEGACY_REFUSAL_REACH.opfs);
    await fc.assert(
      fc.asyncProperty(names, fc.boolean(), async (name, sync) => {
        reach.beginHistory();
        reach.reach(sync ? "sync" : "no sync");
        for (let marker = 5; marker <= 13; marker++) {
          // O5 and O11 have no frozen artifact: synthesized markers, explicitly NOT older-writer fixtures.
          const fixture = frozen.find((item) => item.layoutFormatVersion === marker) ?? frozen[0];
          if (fixture === undefined) throw new Error("Missing frozen OPFS fixture");
          const shim = new MemoryOpfs();
          const prefix = `minnowdb/${encodeLegacySegment(name)}`;
          for (const [path, base64] of Object.entries(fixture.files)) {
            shim.writeFileBytes(
              path.replace("minnowdb/native-fixture", prefix),
              Uint8Array.from(atob(base64), (char) => char.charCodeAt(0)),
            );
          }
          if (fixture.layoutFormatVersion !== marker)
            shim.writeFileBytes(
              `${prefix}/format.json`,
              new TextEncoder().encode(JSON.stringify({ formatVersion: marker })),
            );
          const before = await tree(shim.root);
          let writes = 0;
          shim.setWriteFault(() => {
            writes++;
            throw new Error("refusal must not write");
          });
          shim.setDeleteFault(() => {
            writes++;
            throw new Error("refusal must not delete");
          });
          const factory = new IDBFactory();
          await refused(() => openNext(name, shim, sync, factory), "opfs");
          await Promise.all(
            [0, 1].map(() => refused(() => openNext(name, shim, sync, factory), "opfs")),
          );
          expect(await tree(shim.root)).toEqual(before);
          expect(await factory.databases()).toEqual([]);
          expect(writes).toBe(0);
          reach.reach(`O${String(marker)}` as (typeof LEGACY_REFUSAL_REACH.opfs)[number]);
          // Protect the direct platform-directory entry point too, before it opens epoch/data files.
          const old = await (
            await shim.root.getDirectoryHandle("minnowdb")
          ).getDirectoryHandle(encodeLegacySegment(name));
          await refused(
            () => createOpfsBackend({ name, directory: old, locks: new SimLocks() }).acquire(),
            "opfs",
          );
          expect(await tree(shim.root)).toEqual(before);
          expect(writes).toBe(0);
        }
        // A pre-marker legacy layout must also refuse; no attempt to parse or repair it.
        const shim = new MemoryOpfs();
        shim.writeFileBytes(`minnowdb/${encodeLegacySegment(name)}/wal`, new Uint8Array([0, 1, 2]));
        await refused(() => openNext(name, shim, sync, new IDBFactory()), "opfs");
        reach.reach("pre-marker");
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });

  it(`refuses I1–I8 on both named and direct next open without upgrading seed ${String(seed)}`, async () => {
    const reach = createReach("legacy IndexedDB refusal", LEGACY_REFUSAL_REACH.idb);
    await fc.assert(
      fc.asyncProperty(
        names,
        fc.integer({ min: 0, max: snapshots.length - 1 }),
        async (name, artifact) => {
          reach.beginHistory();
          const source = snapshots[artifact];
          if (source === undefined) throw new Error("Missing frozen snapshot");
          const bytes = new Uint8Array(readFileSync(new URL(source, directory)));
          for (let version = 1; version <= 8; version++) {
            const factory = new IDBFactory();
            // Synthetic native schema wrappers carry genuine frozen snapshot bytes. Native I1–I7
            // images are not in the corpus; the separate test below uses the actual shipped I8 writer.
            const db = await native(factory, name, version, (database) => {
              for (const store of ["catalog", "manifests", "blocks"])
                database
                  .createObjectStore(store)
                  .put({ bytes, source: snapshots[artifact] }, "frozen-artifact");
            });
            db.close();
            const before = await image(factory, name);
            await refused(() => openNext(name, null, false, factory), "indexeddb");
            await refused(async () => {
              const opened = await openIndexedDb(factory, name);
              opened.close();
            }, "indexeddb");
            expect(await image(factory, name)).toEqual(before);
            expect(await factory.databases()).toEqual([{ name, version }]);
            reach.reach(`I${String(version)}` as (typeof LEGACY_REFUSAL_REACH.idb)[number]);
          }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  });
}

it("refuses actual shipped I8 databases loaded from every frozen block/snapshot artifact", async () => {
  for (const artifact of snapshots) {
    const factory = new IDBFactory();
    const name = `frozen-${artifact}`;
    const store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
    const old = new MinnowDatabase(store);
    await old.importSnapshot(new Uint8Array(readFileSync(new URL(artifact, directory))));
    await old.close();
    const before = await image(factory, name);
    await refused(() => openNext(name, null, false, factory), "indexeddb");
    expect(await image(factory, name)).toEqual(before);
    expect(await factory.databases()).toEqual([{ name, version: 8 }]);
  }
});

it("opens fresh stores and an unrelated bare-name IDB, without leaving probe databases", async () => {
  const factory = new IDBFactory();
  const db = await native(factory, "notes", 1, (database) => {
    database.createObjectStore("notes");
  });
  db.close();
  const before = await image(factory, "notes");
  await openNext("notes", null, false, factory);
  await openNext("fresh", new MemoryOpfs(), true, factory);
  expect(await image(factory, "notes")).toEqual(before);
  expect((await factory.databases()).map((item) => item.name).sort()).toEqual([
    "minnow-core:notes",
    "notes",
  ]);
});

it.each([1, 2, 8])(
  "refuses an I%i writer racing the native schema open before any mutation",
  async (version) => {
    const inner = new IDBFactory();
    const name = "racing-old-writer";
    let raced = false;
    const factory = {
      open(databaseName: string, target?: number) {
        if (target === 2 && !raced) {
          raced = true;
          const old = inner.open(databaseName, version);
          old.onupgradeneeded = () => {
            for (const store of ["catalog", "manifests", "blocks"])
              old.result.createObjectStore(store).put(new Uint8Array([4, 5, 6]), "preserved");
          };
          old.onsuccess = () => old.result.close();
        }
        return inner.open(databaseName, target);
      },
    } as IDBFactory;
    await refused(async () => {
      const opened = await openIndexedDb(factory, name);
      opened.close();
    }, "indexeddb");
    expect(raced).toBe(true);
    const db = await native(inner, name);
    expect(db.version).toBe(version);
    expect(Array.from(db.objectStoreNames)).toEqual(["blocks", "catalog", "manifests"]);
    db.close();
    expect(await image(inner, name)).toContain("040506");
  },
);

it("refuses old data even if a next namespace already exists and propagates unreadable paths", async () => {
  const shim = new MemoryOpfs();
  shim.writeFileBytes("minnowdb/db/format.json", new Uint8Array([255])); // Corrupt old marker, never repaired.
  shim.writeFileBytes("minnow-core/db/root-0", new Uint8Array([1, 2]));
  const before = await tree(shim.root);
  await refused(() => openNext("db", shim, true, new IDBFactory()), "opfs");
  expect(await tree(shim.root)).toEqual(before);
  const factory = new IDBFactory();
  await expect(
    chooseBackend(
      "db",
      {
        indexedDB: factory,
        navigator: {
          storage: {
            getDirectory: async () => ({
              getDirectoryHandle: async () => {
                throw new DOMException("unreadable", "TypeMismatchError");
              },
            }),
          },
        },
      },
      new SimLocks(),
    ),
  ).rejects.toMatchObject({ name: "TypeMismatchError" });
  expect(await factory.databases()).toEqual([]);
});

it("refuses the frozen orphan tree and binary fold-ceiling fixture unchanged", async () => {
  const orphan = JSON.parse(
    readFileSync(new URL("opfs-orphan-temp-pages.json", directory), "utf8"),
  ) as { files: Record<string, string> };
  const packed = gunzipSync(readFileSync(new URL("opfs-layout10-fold-ceiling.json.gz", directory)));
  const end = packed.indexOf(10);
  expect(end).toBeGreaterThan(0);
  const header = JSON.parse(packed.subarray(0, end).toString("utf8")) as {
    files: Record<string, { offset: number; length: number }>;
  };
  const images = [
    Object.entries(orphan.files).map(
      ([path, bytes]) =>
        [path, Uint8Array.from(atob(bytes), (char) => char.charCodeAt(0))] as const,
    ),
    Object.entries(header.files).map(
      ([path, { offset, length }]) =>
        [path, packed.subarray(end + 1 + offset, end + 1 + offset + length)] as const,
    ),
  ];
  for (const files of images) {
    const shim = new MemoryOpfs();
    for (const [path, bytes] of files) shim.writeFileBytes(path, bytes);
    const name = files[0]?.[0].split("/")[1];
    if (name === undefined) throw new Error("Missing frozen native name");
    const before = await tree(shim.root);
    let writes = 0;
    shim.setWriteFault(() => {
      writes++;
      throw new Error("must not write old fixture");
    });
    shim.setDeleteFault(() => {
      writes++;
      throw new Error("must not delete old fixture");
    });
    for (const sync of [false, true]) {
      const factory = new IDBFactory();
      await refused(() => openNext(name, shim, sync, factory), "opfs");
      expect(await factory.databases()).toEqual([]);
    }
    expect(await tree(shim.root)).toEqual(before);
    expect(writes).toBe(0);
  }
});

for (const seed of seedsFor("next-legacy-refusal-partial", [0xb211, 0xb212])) {
  it(`refuses generated nonempty subsets of every frozen OPFS tree seed ${String(seed)}`, async () => {
    const reach = createReach("partial legacy trees", LEGACY_PARTIAL_TREE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.boolean(), { minLength: 1, maxLength: 64 }),
        fc.nat(),
        async (mask, first) => {
          reach.beginHistory();
          for (const fixture of frozen) {
            const entries = Object.entries(fixture.files);
            const selected = entries.filter(
              (_, index) => index === first % entries.length || mask[index % mask.length],
            );
            const shim = new MemoryOpfs();
            for (const [path, bytes] of selected)
              shim.writeFileBytes(path, Buffer.from(bytes, "base64"));
            const before = await tree(shim.root);
            let writes = 0;
            shim.setWriteFault(() => {
              writes++;
              throw new Error("partial tree must not be written");
            });
            shim.setDeleteFault(() => {
              writes++;
              throw new Error("partial tree must not be removed");
            });
            const directory = await (
              await shim.root.getDirectoryHandle("minnowdb")
            ).getDirectoryHandle("native-fixture");
            await refused(
              () =>
                createOpfsBackend({
                  name: "native-fixture",
                  directory,
                  locks: new SimLocks(),
                }).acquire(),
              "opfs",
            );
            expect(await tree(shim.root)).toEqual(before);
            expect(writes).toBe(0);
            if (selected.length < entries.length) reach.reach("partial");
            if (!selected.some(([path]) => path.endsWith("/format.json"))) reach.reach("no marker");
          }
          // Reviewer pin, inside every generated history: only the genuine O13 acknowledgement bytes survive.
          const newest = frozen.find((fixture) => fixture.layoutFormatVersion === 13);
          const witness = newest?.files["minnowdb/native-fixture/wal-acknowledgements"];
          if (witness === undefined) throw new Error("Missing frozen acknowledgement witness");
          const shim = new MemoryOpfs();
          shim.writeFileBytes("db/wal-acknowledgements", Buffer.from(witness, "base64"));
          const before = await tree(shim.root);
          const directory = await shim.root.getDirectoryHandle("db");
          await refused(
            () => createOpfsBackend({ name: "db", directory, locks: new SimLocks() }).acquire(),
            "opfs",
          );
          expect(await tree(shim.root)).toEqual(before);
          reach.reach("ack only");
          for (const kind of [
            "unknown file",
            "unknown directory",
            "native name directory",
          ] as const) {
            const shim = new MemoryOpfs();
            if (kind === "unknown file")
              shim.writeFileBytes("db/unrecognized", new Uint8Array([1]));
            else
              await (
                await shim.root.getDirectoryHandle("db", { create: true })
              ).getDirectoryHandle(kind === "native name directory" ? "data" : "unrecognized", {
                create: true,
              });
            const before = await tree(shim.root);
            const directory = await shim.root.getDirectoryHandle("db");
            await expect(
              createOpfsBackend({ name: "db", directory, locks: new SimLocks() }).acquire(),
            ).rejects.toMatchObject({ code: "MINNOW_STORE_CORRUPT" });
            expect(await tree(shim.root)).toEqual(before);
            reach.reach(kind);
          }
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });
  it(`keeps typed refusal while a racing old IDB writer retains its connection seed ${String(seed)}`, async () => {
    const reach = createReach("held legacy writer", LEGACY_HELD_WRITER_REACH);
    await fc.assert(
      fc.asyncProperty(names, fc.boolean(), async (name, held) => {
        reach.beginHistory();
        reach.reach(held ? "held" : "closed");
        for (const version of [1, 2, 8]) {
          const inner = new IDBFactory();
          let old: IDBDatabase | undefined;
          let raced = false;
          const factory = {
            open(databaseName: string, target?: number) {
              if (target === 2 && !raced) {
                raced = true;
                const request = inner.open(databaseName, version);
                request.onupgradeneeded = () => {
                  for (const store of ["catalog", "manifests", "blocks"])
                    request.result
                      .createObjectStore(store)
                      .put(new Uint8Array([7, 8, 9]), "preserved");
                };
                request.onsuccess = () => {
                  old = request.result;
                  if (!held) old.close();
                };
              }
              return inner.open(databaseName, target);
            },
          } as IDBFactory;
          try {
            await refused(async () => {
              const db = await openIndexedDb(factory, name);
              db.close();
            }, "indexeddb");
          } finally {
            old?.close();
          }
          expect(await inner.databases()).toEqual([{ name, version }]);
          const db = await native(inner, name);
          expect(Array.from(db.objectStoreNames)).toEqual(["blocks", "catalog", "manifests"]);
          db.close();
          if (version === 1 && held) reach.reach("I1 blocked");
          if (version === 2) reach.reach("I2");
          if (version === 8) reach.reach("I8");
        }
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });
}
