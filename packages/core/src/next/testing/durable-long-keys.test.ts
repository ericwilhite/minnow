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
import { decodeRootSlot, encodeRootSlot } from "../storage/root-slot.js";
import {
  DURABLE_READER_VERSION,
  MANIFEST_FORMAT_VERSION,
  NATIVE_READER_PROFILES,
  nativeVocabularyFingerprint,
} from "../durable-format.js";
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
    encodeRootSlot: typeof encodeRootSlot;
    decodeRootSlot: typeof decodeRootSlot;
    NATIVE_READER_PROFILES: typeof NATIVE_READER_PROFILES;
    nativeVocabularyFingerprint: typeof nativeVocabularyFingerprint;
    DURABLE_READER_VERSION: number;
    MANIFEST_FORMAT_VERSION: number;
  };
// A codec-compatible capability-1 build isolates the minimum-reader gate from the
// manifest-version gate. Like a real build, it needs a profile for its OWN vocabulary:
// changing only the selector makes the module internally inconsistent. The synthetic
// entry is local to this bundled fixture; production's immutable ledger and initializer
// stay strict. This is not a historical predecessor fixture or an upgrade test.
async function readerBuild(coherent: boolean): Promise<typeof older> {
  const result = await build({
    stdin: {
      contents: `export { createOpfsBackend } from "./storage/opfs.js";
        export { createIndexedDbBackend } from "./storage/indexeddb.js";
        export { encodeRootSlot, decodeRootSlot } from "./storage/root-slot.js";
        export { DURABLE_READER_VERSION, MANIFEST_FORMAT_VERSION, NATIVE_READER_PROFILES,
          nativeVocabularyFingerprint } from "./durable-format.js";`,
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
          api.onLoad({ filter: /durable-format\.ts$/ }, async ({ path }) => {
            const source = await readFile(path, "utf8"),
              selector = /export const DURABLE_READER_VERSION = \d+;/,
              ledger = /(export const NATIVE_READER_PROFILES:[\s\S]*?= Object\.freeze\(\{)/;
            expect(source).toMatch(selector);
            expect(source).toMatch(ledger);
            const selected = source.replace(selector, "export const DURABLE_READER_VERSION = 1;");
            return {
              contents: coherent
                ? selected.replace(
                    ledger,
                    "$1\n  1: Object.freeze({ manifest: MANIFEST_FORMAT_VERSION, vocabulary: nativeVocabularyFingerprint() }),",
                  )
                : selected,
              loader: "ts",
            };
          });
        },
      },
    ],
  });
  const file = join(dir, coherent ? "reader1.mjs" : "unassigned-reader1.mjs");
  await writeFile(file, need(result.outputFiles[0]).contents);
  return (await import(pathToFileURL(file).href)) as typeof older;
}
beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "minnow-reader1-"));
  older = await readerBuild(true);
});
it("keeps the module-initialization guard for an incoherent synthetic build", async () => {
  await expect(readerBuild(false)).rejects.toMatchObject({ code: "MINNOW_FORMAT_UNSUPPORTED" });
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
        expect(older.DURABLE_READER_VERSION).toBe(1);
        expect(older.MANIFEST_FORMAT_VERSION).toBe(MANIFEST_FORMAT_VERSION);
        expect(older.NATIVE_READER_PROFILES[1]).toEqual({
          manifest: MANIFEST_FORMAT_VERSION,
          vocabulary: older.nativeVocabularyFingerprint(),
        });
        reach.reach("coherent older profile loaded");
        const supported = older.encodeRootSlot(2, Uint8Array.of(1, 2, 3));
        expect(older.decodeRootSlot(supported)).toEqual({
          sequence: 2,
          root: Uint8Array.of(1, 2, 3),
        });
        reach.reach("older own floor accepted");
        expect(DURABLE_READER_VERSION).toBeGreaterThan(1);
        expect(MANIFEST_FORMAT_VERSION).toBeGreaterThan(2);
        const bytes = encodeRootSlot(2, Uint8Array.of(1, 2, 3));
        expect((bytes[10] ?? 0) >>> 5).toBe(DURABLE_READER_VERSION - 1);
        expect(bytes[11]).toBe(MANIFEST_FORMAT_VERSION - 2);
        expect(() => older.decodeRootSlot(bytes)).toThrow(
          expect.objectContaining({
            code: "MINNOW_FORMAT_UNSUPPORTED",
          }),
        );
        expect(() => older.decodeRootSlot(bytes)).toThrow("rootSlot minimum reader");
        reach.reach("same manifest reader refusal");
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
        const idbSlot = second ? "slot-1" : "slot-0";
        await raw.transaction(["roots", "meta"], "readwrite", "strict", (tx) => {
          tx.store("roots").put(idbSlot, bytes.slice().buffer);
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
        const inspect = await openIndexedDb(factory, "older");
        try {
          await inspect.transaction(["roots", "meta"], "readonly", "relaxed", (tx) => {
            tx.store("roots").get(idbSlot, (value) => expect(value).toEqual(bytes.slice().buffer));
            tx.store("meta").get("epoch", (value) => expect(value).toBe(1));
          });
        } finally {
          inspect.close();
        }
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
