/** IdbSession's strict publication barrier captures earlier writes in call order,
 * even while its compatibility coordination gate waits. Later overwrites cannot pass it.
 */
import "fake-indexeddb/auto";
import fc from "fast-check";
import { expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { createReach, ROOT_PUBLICATION_ORDER_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb, type ByteRecord } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";

for (const seed of seedsFor("root-publication-order", [0xb2cb, 0xb2cc]))
  it(`keeps queued publication ahead of later same-extent overwrites seed ${String(seed)}`, async () => {
    const reach = createReach("root publication ordering", ROOT_PUBLICATION_ORDER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 127 }),
        fc.integer({ min: 128, max: 255 }),
        async (first, second) => {
          reach.beginHistory();
          for (const batching of [true, false]) {
            const locks = new SimLocks(),
              factory = new IDBFactory();
            const trace: Array<{ tx: number; kind: "data" | "root"; value: number }> = [];
            let serial = 0;
            const backend = createIndexedDbBackend({
              name: "publication-order",
              locks,
              batching,
              database: async () => {
                const db = await openIndexedDb(factory, "publication-order");
                return {
                  close: () => {
                    db.close();
                  },
                  transaction: (stores, mode, durability, body) => {
                    const txId = ++serial;
                    return db.transaction(stores, mode, durability, (tx) =>
                      body({
                        abort: (error) => {
                          tx.abort(error);
                        },
                        store: (name) => {
                          const store = tx.store(name);
                          return {
                            ...store,
                            put: (key, value) => {
                              if (name === "data")
                                trace.push({
                                  tx: txId,
                                  kind: "data",
                                  value: new Uint8Array((value as ByteRecord).bytes)[0] ?? -1,
                                });
                              if (name === "roots")
                                trace.push({ tx: txId, kind: "root", value: 0 });
                              store.put(key, value);
                            },
                          };
                        },
                      }),
                    );
                  },
                };
              },
            });
            const owner = await backend.acquire(),
              session = await backend.open(owner);
            const gate = await locks.request("minnow-gate:minnow:publication-order", {});
            try {
              await session.writeData(0, Uint8Array.of(first));
              const publishing = session.publish(Uint8Array.of(3));
              const overwritten = session.writeData(0, Uint8Array.of(second));
              const flushed = session.flushLog();
              // No sleeps or assumptions about how many microtasks locking takes.
              gate.release();
              await Promise.all([publishing, overwritten, flushed]);
              expect(trace.map((event) => (event.kind === "root" ? "root" : event.value))).toEqual([
                first,
                "root",
                second,
              ]);
              const initial = trace[0],
                root = trace[1],
                later = trace[2];
              expect(initial?.tx === root?.tx).toBe(batching);
              expect(later?.tx).not.toBe(root?.tx);
              reach.reach(
                batching
                  ? "batched earlier write and root atomic"
                  : "unbatched earlier write precedes root",
              );
              reach.reach("same extent overwrite after root");
              expect(await session.readRoot()).toEqual(Uint8Array.of(3));
            } finally {
              gate.release();
              await session.close();
              await owner.release();
            }
          }
        },
      ),
      { seed, numRuns: 4, endOnFailure: true },
    );
    reach.assertReached();
  });
