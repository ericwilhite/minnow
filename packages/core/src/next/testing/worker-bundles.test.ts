/** Execute production-minified browser worker entry bundles, not unbundled imports. */
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { build } from "esbuild";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_WORKER_BUNDLE_REACH } from "../../testing/reach.js";
import { connectNativeWorkerClient } from "../owner/client-worker-connect.js";
import type * as Memory from "../owner/entries/worker/memory.js";
import type * as IndexedDb from "../owner/entries/worker/indexeddb.js";
import type * as Opfs from "../owner/entries/worker/opfs.js";
import type * as Auto from "../owner/entries/worker/auto.js";
import type { NativeWorkerScope } from "../owner/native-worker.js";
import type { OwnerHostCommand, OwnerHostStatus } from "../owner/host.js";
import type { ComposeOptions } from "../owner/compose.js";
import type { StorageBackend } from "../storage/contract.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { FakeLockRegistry } from "./fake-locks.js";
(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;
type Entry = typeof Memory & typeof IndexedDb & typeof Opfs & typeof Auto;
for (const kind of ["memory", "indexeddb", "opfs", "auto"] as const)
  it(`executes dynamic window SQL through bundled ${kind} worker`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "minnow-worker-bundle-")),
      pg = new PGlite();
    try {
      const bundled = await build({
        entryPoints: [
          fileURLToPath(new URL(`../owner/entries/worker/${kind}.ts`, import.meta.url)),
        ],
        bundle: true,
        platform: "browser",
        format: "esm",
        target: "es2022",
        minify: true,
        treeShaking: true,
        write: false,
      });
      const output = bundled.outputFiles[0];
      if (output === undefined) throw new Error("No worker bundle");
      const file = join(directory, "worker.mjs");
      await writeFile(file, output.contents);
      const entry = (await import(pathToFileURL(file).href)) as Entry;
      for (const seed of seedsFor("next-worker-entry-bundles", [46960, 46961])) {
        const reach = createReach(`bundled ${kind}`, NATIVE_WORKER_BUNDLE_REACH);
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 2, max: 6 }),
            fc.integer({ min: -2, max: 2 }),
            fc.integer({ min: -99, max: 99 }),
            async (rows, step, fallback) => {
              reach.beginHistory();
              const registry = new FakeLockRegistry(),
                locks = new SimLocks(),
                disk = new SimDisk(),
                factory = new IDBFactory();
              const backend: StorageBackend =
                kind === "memory"
                  ? new entry.MemoryBackend({ locks })
                  : kind === "indexeddb"
                    ? entry.createIndexedDbBackend({
                        name: "bundled",
                        locks,
                        database: () => entry.openIndexedDb(factory, "bundled"),
                      })
                    : kind === "opfs"
                      ? entry.createOpfsBackend({
                          name: "bundled",
                          locks,
                          directory: {
                            getFileHandle: async (name) => ({
                              createSyncAccessHandle: async () => disk.open(name, true),
                            }),
                          },
                        })
                      : (await entry.chooseBackend("bundled", {}, locks)).backend;
              let listener: ((event: { data: OwnerHostStatus }) => void) | null = null;
              let terminated = 0;
              const scope: NativeWorkerScope = {
                onmessage: null,
                postMessage: (data) => queueMicrotask(() => listener?.({ data })),
              };
              const service = entry.installNativeOwnerWorker(
                scope,
                () => backend,
                { memoryLimitBytes: 64 << 20 },
                { locks: registry.context() },
              );
              const worker = {
                get onmessage() {
                  return listener;
                },
                set onmessage(value: typeof listener) {
                  listener = value;
                },
                onerror: null,
                postMessage(data: OwnerHostCommand, transfer: Transferable[] = []) {
                  const copy = structuredClone(data, { transfer });
                  queueMicrotask(() => scope.onmessage?.({ data: copy }));
                },
                terminate() {
                  terminated++;
                },
              } as unknown as Worker;
              const db = connectNativeWorkerClient({
                database: `bundle-${kind}`,
                locks: registry.context(),
                openBroker: null,
                createOwnerWorker: () => worker,
                platform: { mobile: false },
                context: () => ({
                  kind: "page",
                  platform: { mobile: false },
                  visible: true,
                  superseded: false,
                }),
                announcements: false,
              });
              try {
                const cases = [
                  [
                    "variable lag",
                    "SELECT lag(i,i,99) OVER(ORDER BY i) AS v FROM generate_series(1,3) s(i)",
                  ],
                  [
                    "variable lead",
                    "SELECT lead(i,i,99) OVER(ORDER BY i) AS v FROM generate_series(1,3) s(i)",
                  ],
                  [
                    "lazy default",
                    `SELECT lag(i,i-1,1/(i-i)) OVER(ORDER BY i) AS v FROM generate_series(1,${String(rows)}) s(i)`,
                  ],
                  [
                    "signed row offset",
                    `SELECT lead(i,i*${String(step)},${String(fallback)}+i) OVER(ORDER BY i) AS v FROM generate_series(1,${String(rows)}) s(i)`,
                  ],
                ] as const;
                for (const [label, sql] of cases) {
                  const expected = await pg.query<{ v: number }>(sql);
                  expect((await db.query(sql)).tuples()).toEqual(
                    expected.rows.map((row) => [row.v]),
                  );
                  reach.reach(label);
                }
                expect((await db.query(cases[0][1])).tuples()).toEqual([[99], [99], [99]]);
                reach.reach("minified real entry SQL");
              } finally {
                await db.close();
                await service.close();
                expect(terminated).toBe(1);
                const host = await service.ready;
                const engine = host.engine as typeof host.engine & {
                  readonly options: ComposeOptions;
                };
                expect(engine.options.governor.usedBytes).toBe(0);
                expect(engine.options.spill.bytesInUse).toBe(0);
                expect(registry.query()).toEqual({ held: [], pending: [] });
                reach.reach("native cleanup");
              }
            },
          ),
          {
            seed,
            numRuns: 6,
            examples: [
              [3, -1, 99],
              [3, 0, 99],
              [3, 1, 99],
            ],
          },
        );
        reach.assertReached();
      }
    } finally {
      await pg.close();
      await rm(directory, { recursive: true, force: true });
    }
  }, 60000);
