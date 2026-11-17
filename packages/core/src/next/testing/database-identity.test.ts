/** Replacement changes database identity, never ordinary checkpoint/recovery. Real native
 * adapters over their platform shims; all mutations use the ordinary write/load paths.
 */
import fc from "fast-check";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DATABASE_IDENTITY_NATIVE_REACH } from "../../testing/reach.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";
import { exportSnapshot } from "../write/snapshot-capture.js";
import { loadSnapshot } from "../write/snapshot-load.js";
import { snapshotBlob } from "../write/snapshot-source.js";
import { snapshotTestTarget } from "../write/snapshot-test-support.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "../write/test-support.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

function native(kind: "OPFS" | "IndexedDB"): Harness {
  const h = new Harness(),
    locks = new SimLocks();
  const factory = new IDBFactory(),
    disk = new MemoryOpfs();
  Object.assign(h, {
    backend:
      kind === "OPFS"
        ? createOpfsBackend({ name: "identity", directory: disk.root, locks })
        : createIndexedDbBackend({
            name: "identity",
            locks,
            database: () => openIndexedDb(factory, "identity"),
          }),
  });
  return h;
}
function identity(h: Harness): bigint {
  const bytes = h.space.root;
  if (bytes === null) throw new Error("No native manifest");
  let held = 0;
  const parsed = decodeManifestRoot(bytes, {
    allocate(n) {
      held += n;
      return true;
    },
    free(n) {
      held -= n;
    },
  });
  // Kept structural so this law also runs on the parent (RED is an assertion, not an import).
  const value = (parsed.value as unknown as { databaseId?: bigint }).databaseId;
  held -= parsed.charge;
  expect(held).toBe(0);
  expect(typeof value, "ordinary root needs a durable database identity").toBe("bigint");
  if (value === undefined) throw new Error("Missing durable identity");
  return value;
}
async function archive(
  value: number,
  mode: "empty database" | "empty table" | "populated",
): Promise<Blob> {
  const h = new Harness("strict", "memory"),
    path = await h.open();
  const grant = await h.governor.admit({
    label: "identity fixture",
    minimumBytes: 8 << 20,
    spillable: false,
  });
  try {
    if (mode !== "empty database") {
      await autocommit(path, (s) =>
        s.changeCatalog({ kind: "createTable", table: tableInput("loaded", [column("v", INT)]) }),
      );
      if (mode === "populated")
        await autocommit(path, (s) => s.table(1).insert(batch(ints([value])), null));
    }
    const parts: Array<Uint8Array<ArrayBuffer>> = [];
    await exportSnapshot(
      path,
      new Uint8Array(16).fill(7),
      {
        write: (bytes) => {
          parts.push(bytes.slice());
        },
      },
      grant,
    );
    return new Blob(parts);
  } finally {
    grant.close();
    await h.close();
    expect(h.governor.usedBytes).toBe(0);
  }
}
for (const seed of seedsFor("next-database-identity-native", [0xb2dd, 0xb2de]))
  it(`native identity survives maintenance and changes on repeated replacement ${String(seed)}`, async () => {
    const reach = createReach("native database identity", DATABASE_IDENTITY_NATIVE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 1000 }),
        fc.constantFrom("empty database", "empty table", "populated"),
        async (value, mode) => {
          reach.beginHistory();
          const source = await archive(value, mode);
          reach.reach(mode);
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const h = native(kind);
            let opened = false;
            try {
              let path = await h.open();
              opened = true;
              let id = identity(h);
              expect(path.databaseId).toBe(id);
              reach.reach(kind);
              expect(id).toBeGreaterThan(0n);
              await autocommit(path, (s) =>
                s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("old", [column("n", INT)]),
                }),
              );
              await autocommit(path, (s) => s.table(1).insert(batch(ints([-value])), null));
              await path.recycleLog();
              await path.quiesce();
              expect(identity(h)).toBe(id);
              reach.reach("ordinary publication");
              path = await h.reopen();
              expect(identity(h)).toBe(id);
              reach.reach("ordinary reopen");
              for (let turn = 0; turn < 2; turn++) {
                const lsn = path.appliedLsn;
                await h.close();
                opened = false;
                const prepaid = await h.governor.admit({
                  label: "identity target startup",
                  minimumBytes: 8 << 20,
                  spillable: true,
                });
                try {
                  await loadSnapshot(
                    snapshotBlob(source),
                    () => snapshotTestTarget(h, lsn, false, undefined, prepaid),
                    h.governor,
                    { replace: true },
                  );
                } finally {
                  prepaid.close();
                }
                expect(h.governor.usedBytes).toBe(0);
                path = await h.open();
                opened = true;
                expect(identity(h)).toBe(id + 1n);
                id = identity(h);
                expect(path.catalog.table(1)?.name).toBe(
                  mode === "empty database" ? undefined : "loaded",
                );
                if (mode !== "empty database") {
                  const pin = path.pin();
                  try {
                    expect(await rowsOf(pin, 1)).toEqual(mode === "populated" ? [[value]] : []);
                  } finally {
                    pin.release();
                  }
                }
                reach.reach(turn === 0 ? "replacement" : "same archive again");
              }
              path = await h.reopen();
              expect(identity(h)).toBe(id);
              reach.reach("replaced reopen");
            } finally {
              if (opened) await h.close();
              expect(h.governor.usedBytes).toBe(0);
            }
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, "empty database"],
          [2, "empty table"],
          [3, "populated"],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
