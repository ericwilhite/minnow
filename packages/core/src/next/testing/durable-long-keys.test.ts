/** Registry integration: logged invalidation survives WAL-only recovery, and reader1 cannot fence. */
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { build } from "esbuild";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createReach, DURABLE_LONG_KEY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { SimLocks } from "../storage/sim-disk.js";
import { createOpfsBackend, encodeEpochSlot } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { IDBFactory } from "fake-indexeddb";
import { openIndexedDb } from "../storage/idb-like.js";
import { encodeRootSlot } from "../storage/root-slot.js";
import { DURABLE_READER_VERSION, MANIFEST_FORMAT_VERSION } from "../durable-format.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  TEXT,
  batch,
  ints,
  texts,
  rowsOf,
} from "../write/test-support.js";
import { need, UNCHARGED } from "../write/bytes.js";
const reach = createReach("durable long-key integration", DURABLE_LONG_KEY_REACH);
let dir = "",
  older: {
    createOpfsBackend: typeof createOpfsBackend;
    createIndexedDbBackend: typeof createIndexedDbBackend;
  };
// A codec-compatible capability-1 build isolates the minimum-reader gate from the
// manifest-version gate. It is not a supported predecessor fixture or an upgrade test.
beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "minnow-reader1-"));
  const result = await build({
    stdin: {
      contents: `export { createOpfsBackend } from "./storage/opfs.js"; export { createIndexedDbBackend } from "./storage/indexeddb.js";`,
      resolveDir: join(import.meta.dirname, ".."),
    },
    bundle: true,
    platform: "node",
    format: "esm",
    write: false,
    plugins: [
      {
        name: "older-reader-capability",
        setup(api) {
          api.onLoad({ filter: /durable-format\.ts$/ }, async ({ path }) => ({
            contents: (await readFile(path, "utf8")).replace(
              /export const DURABLE_READER_VERSION = \d+;/,
              "export const DURABLE_READER_VERSION = 1;",
            ),
            loader: "ts",
          }));
        },
      },
    ],
  });
  const file = join(dir, "reader1.mjs");
  await writeFile(file, need(result.outputFiles[0]).contents);
  older = (await import(pathToFileURL(file).href)) as typeof older;
});
afterAll(async () => {
  if (dir) await rm(dir, { recursive: true, force: true });
  reach.assertReached();
});
for (const seed of seedsFor("next-durable-long-key-recovery", [0xb531]))
  it(`replays committed nonunique invalidation at the tuple boundary (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(13107, 16384),
        fc.constantFrom("memory" as const, "sim" as const),
        async (units, store) => {
          reach.beginHistory();
          const h = new Harness("strict", store, 1024, undefined, { merges: { mode: "off" } });
          await h.open();
          try {
            let table = 0,
              index = 0;
            await autocommit(h.path, async (s) => {
              table = need(
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false), column("k", TEXT, false)]),
                  })
                ).id,
              );
              index = need(
                (
                  await s.changeCatalog({
                    kind: "createIndex",
                    index: {
                      tableId: table,
                      name: "k",
                      columnIds: [need(s.catalog.table(table)?.columns[1]).id],
                      unique: false,
                      role: "secondary",
                    },
                  })
                ).id,
              );
            });
            await h.path.quiesce();
            await autocommit(h.path, (s) =>
              s.table(table).insert(batch(ints([1]), texts(["x".repeat(units)])), null),
            );
            expect(h.path.catalog.index(index)?.invalid).toBe(true);
            expect(
              decodeManifestRoot(need(h.space.root), UNCHARGED).value.catalogVersion,
            ).toBeLessThan(h.path.catalog.version);
            reach.reach("WAL-only invalidation");
            await h.reopen();
            expect(h.path.catalog.index(index)?.invalid).toBe(true);
            const view = h.path.pin();
            try {
              expect(await rowsOf(view, table)).toEqual([[1, "x".repeat(units)]]);
            } finally {
              view.release();
            }
            reach.reach("replayed invalidation");
            reach.reach(store);
            if (units === 13107) reach.reach("13107-unit boundary");
          } finally {
            await h.close();
            expect(h.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [13107, "memory"],
          [13107, "sim"],
        ],
      },
    );
  });
for (const seed of seedsFor("next-durable-older-long-key-reader", [0xb532]))
  it(`reader1 refuses the emitted minimum before any native write (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), async (second, complete) => {
        reach.beginHistory();
        expect(DURABLE_READER_VERSION).toBeGreaterThan(1);
        expect(MANIFEST_FORMAT_VERSION).toBeGreaterThan(2);
        const bytes = encodeRootSlot(2, Uint8Array.of(1, 2, 3));
        expect((bytes[10] ?? 0) >>> 5).toBe(DURABLE_READER_VERSION - 1);
        expect(bytes[11]).toBe(MANIFEST_FORMAT_VERSION - 2);
        const shim = new MemoryOpfs(),
          slot = second ? "root-1" : "root-0";
        shim.writeFileBytes(slot, bytes);
        shim.writeFileBytes("epoch", encodeEpochSlot(1));
        const image = new Map([
          [slot, bytes],
          ["epoch", encodeEpochSlot(1)],
        ]);
        if (complete)
          for (const name of ["log", "data", "spill", "space-meta"]) {
            const b = Uint8Array.of(7, 9);
            shim.writeFileBytes(name, b);
            image.set(name, b);
          }
        let writes = 0;
        shim.setWriteFault(() => {
          writes++;
        });
        await expect(
          older
            .createOpfsBackend({ name: "older", directory: shim.root, locks: new SimLocks() })
            .acquire(),
        ).rejects.toMatchObject({ code: "MINNOW_FORMAT_UNSUPPORTED" });
        expect(writes).toBe(0);
        const after = new Map();
        for await (const name of shim.root.keys()) after.set(name, shim.readFileBytes(name));
        expect(after).toEqual(image);
        reach.reach("older OPFS zero writes");
        const factory = new IDBFactory(),
          raw = await openIndexedDb(factory, "older");
        await raw.transaction(["roots", "meta"], "readwrite", "strict", (tx) => {
          tx.store("roots").put("slot-0", bytes.slice().buffer);
          tx.store("meta").put("epoch", 1);
        });
        let idbWrites = 0;
        const backend = older.createIndexedDbBackend({
          name: "older",
          locks: new SimLocks(),
          database: () => ({
            close: () => raw.close(),
            transaction: (stores, mode, durability, body) => {
              return raw.transaction(stores, mode, durability, (tx) =>
                body({
                  abort: (e) => tx.abort(e),
                  store: (name) => {
                    const store = tx.store(name);
                    return {
                      ...store,
                      put: (key, value) => {
                        idbWrites++;
                        store.put(key, value);
                      },
                      delete: (key) => {
                        idbWrites++;
                        store.delete(key);
                      },
                      clear: () => {
                        idbWrites++;
                        store.clear();
                      },
                    };
                  },
                }),
              );
            },
          }),
        });
        await expect(backend.acquire()).rejects.toMatchObject({
          code: "MINNOW_FORMAT_UNSUPPORTED",
        });
        expect(idbWrites).toBe(0);
        raw.close();
        reach.reach("older IDB zero writes");
        reach.reach(second ? "second root slot" : "first root slot");
        reach.reach(complete ? "complete native tree" : "partial native tree");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, false],
          [true, true],
        ],
      },
    );
  });
