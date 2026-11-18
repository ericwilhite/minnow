/** Actual native store/installer/worker connector, with only browser primitives simulated. */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_STORE_ENTRY_REACH } from "../../testing/reach.js";
import { connectNativeWorkerClient } from "../owner/entries/client.js";
import { MemoryBackend, installNativeOwnerWorker } from "../owner/entries/worker/memory.js";
import { createIndexedDbBackend, openIndexedDb } from "../owner/entries/storage/indexeddb.js";
import { createOpfsBackend } from "../owner/entries/storage/opfs.js";
import { chooseBackend } from "../owner/entries/storage/auto.js";
import type { StorageBackend } from "../owner/entries/storage/contracts.js";
import type { ComposeOptions } from "../owner/compose.js";
import type { NativeWorkerScope } from "../owner/native-worker.js";
import type { OwnerHostCommand, OwnerHostStatus } from "../owner/host.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { FakeLockRegistry } from "./fake-locks.js";
(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

for (const seed of seedsFor("next-public-store-entries", [0xb75c, 0xb75d]))
  it(`composes store-specific native workers and retires all ownership (${String(seed)})`, async () => {
    const reach = createReach("native store entries", PUBLIC_STORE_ENTRY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("memory", "indexeddb", "opfs"),
        fc.bigInt({ min: -(1n << 55n), max: 1n << 55n }),
        fc.string({ maxLength: 24 }).map((value) => value.replaceAll("\u0000", "�")),
        async (kind, value, text) => {
          reach.beginHistory();
          const registry = new FakeLockRegistry(),
            locks = new SimLocks(),
            disk = new SimDisk(),
            factory = new IDBFactory();
          const backend: StorageBackend =
            kind === "memory"
              ? new MemoryBackend({ locks })
              : kind === "indexeddb"
                ? createIndexedDbBackend({
                    name: "entries",
                    locks,
                    database: () => openIndexedDb(factory, "entries"),
                  })
                : createOpfsBackend({
                    name: "entries",
                    locks,
                    directory: {
                      getFileHandle: async (name) => ({
                        createSyncAccessHandle: async () => disk.open(name, true),
                      }),
                    },
                  });
          let onmessage: ((event: { data: OwnerHostStatus }) => void) | null = null,
            terminated = 0;
          const commands: OwnerHostCommand[] = [];
          const scope: NativeWorkerScope = {
            onmessage: null,
            postMessage: (data) => queueMicrotask(() => onmessage?.({ data })),
          };
          const service = installNativeOwnerWorker(
            scope,
            () => backend,
            { memoryLimitBytes: 32 << 20 },
            { locks: registry.context() },
          );
          const worker = {
            get onmessage() {
              return onmessage;
            },
            set onmessage(value: typeof onmessage) {
              onmessage = value;
            },
            onerror: null,
            postMessage(data: OwnerHostCommand, transfer: Transferable[] = []) {
              commands.push(data);
              const copied = structuredClone(data, { transfer });
              queueMicrotask(() => scope.onmessage?.({ data: copied }));
            },
            terminate() {
              terminated++;
            },
          } as unknown as Worker;
          const platform = { mobile: false };
          const document = Object.assign(new EventTarget(), { visibilityState: "hidden" });
          const lifecycleTarget = Object.assign(new EventTarget(), { document });
          const db = connectNativeWorkerClient({
            database: `entries-${String(seed)}`,
            locks: registry.context(),
            openBroker: null,
            createOwnerWorker: () => worker,
            platform,
            context: () => ({
              kind: "page",
              platform: { mobile: false },
              visible: true,
              superseded: false,
            }),
            lifecycleTarget: lifecycleTarget as unknown as Window & typeof globalThis,
            announcements: false,
          });
          platform.mobile = true;
          try {
            await db.execute("CREATE TABLE t(id INT PRIMARY KEY,n BIGINT,v TEXT)");
            await db.execute("INSERT INTO t VALUES(1,$1,$2)", [value, text]);
            expect((await db.query("SELECT n,v FROM t")).tuples()).toEqual([[value, text]]);
            reach.reach(`${kind} native worker`);
            // All lifecycle relays use the captured policy, not the caller's later mutation.
            document.dispatchEvent(new Event("visibilitychange"));
            expect(commands.filter((c) => c.t === "minnow:lifecycle")).toEqual([
              { t: "minnow:lifecycle", event: "hidden", platform: { mobile: false } },
            ]);
            reach.reach("captured lifecycle policy");
            reach.reach("exact worker rows");
          } finally {
            await db.close();
            await service.close();
            const host = await service.ready;
            const engine = host.engine as typeof host.engine & { readonly options: ComposeOptions };
            expect(engine.options.governor.usedBytes).toBe(0);
            expect(engine.options.spill.bytesInUse).toBe(0);
            expect(registry.query()).toEqual({ held: [], pending: [] });
            expect(terminated).toBe(1);
            const lease = await backend.acquire();
            await lease.release();
            reach.reach("worker lease and charge cleanup");
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          ["memory", 9007199254740993n, "水"],
          ["indexeddb", -9007199254740993n, ""],
          ["opfs", 0n, "🙂"],
        ],
      },
    );
    reach.assertReached();
  }, 60000);

it("keeps auto's explicit no-durable-platform memory selection", async () => {
  const chosen = await chooseBackend("entries", {}, new SimLocks());
  expect(chosen.kind).toBe("memory");
  expect(chosen.opfsRefused).toBe("unsupported");
  const lease = await chosen.backend.acquire();
  await lease.release();
});
