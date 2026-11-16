/** The compatibility probe must precede even stealing a live newer owner's Web Lock. */
import {
  DURABLE_READER_VERSION,
  DURABLE_WRITER_VERSION,
  MANIFEST_FORMAT_VERSION,
} from "../durable-format.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  ROOT_NATIVE_DUAL_GATE_REACH,
  ROOT_WRITE_GATE_REACH,
} from "../../testing/reach.js";
import { createOpfsBackend, encodeEpochSlot } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import type { IdbDatabaseLike, IdbStoreLike } from "../storage/idb-like.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { identityOwnerName } from "../storage/owner-identity.js";
import { SimLocks } from "../storage/sim-disk.js";
import { encodeRootSlot } from "../storage/root-slot.js";
import { Harness, autocommit, batch, texts } from "../write/test-support.js";
import { setupFts, lastFtsCommit } from "./fts-transaction.test-support.js";
import { xxHash32 } from "../hash/xxhash32.js";

const ownerName = identityOwnerName("future", {
  epoch: 7,
  generation: 7,
  reader: DURABLE_READER_VERSION,
  writer: DURABLE_WRITER_VERSION,
});
const futureCapability = fc.integer({ min: DURABLE_READER_VERSION + 1, max: 0xffffffff });

for (const seed of seedsFor("next-root-dual-native", [0xb2c3, 0xb2c4]))
  it(`refuses future read or rewrite gates without fencing a live owner seed ${String(seed)}`, async () => {
    const reach = createReach("dual native pre-authority gates", ROOT_NATIVE_DUAL_GATE_REACH);
    await fc.assert(
      fc.asyncProperty(futureCapability, async (future) => {
        reach.beginHistory();
        for (const field of ["minimum reader", "minimum writer"] as const)
          for (const slot of ["root-0", "root-1"] as const)
            for (const complete of [false, true]) {
              reach.reach(field);
              reach.reach(slot);
              reach.reach(complete ? "complete tree" : "partial tree");
              const bytes = encodeRootSlot(5, new Uint8Array(32)),
                view = new DataView(bytes.buffer);
              view.setUint32(field === "minimum reader" ? 16 : 20, future, true);
              view.setUint32(bytes.length - 4, xxHash32(bytes, 0, 0, bytes.length - 4), true);
              const other = encodeRootSlot(6, new Uint8Array(32).fill(11)); // valid current cannot conceal future previous
              const locks = new SimLocks(),
                holder = await locks.request(ownerName, {});
              let lockCalls = 0;
              const observedLocks = {
                request: (...args: Parameters<SimLocks["request"]>) => {
                  if (args[0].startsWith("minnow-owner:")) lockCalls++;
                  return locks.request(...args);
                },
              };
              const shim = new MemoryOpfs(),
                image = new Map<string, Uint8Array>([
                  [slot, bytes],
                  [slot === "root-0" ? "root-1" : "root-0", other],
                  ["epoch", encodeEpochSlot(7)],
                  ["data", new Uint8Array([7, 13, 23])],
                ]);
              if (complete)
                for (const name of ["log", "spill", "space-meta", "space-meta-index", "spill-meta"])
                  image.set(name, new Uint8Array(0));
              for (const [name, value] of image) shim.writeFileBytes(name, value);
              let writes = 0;
              shim.setWriteFault(() => {
                writes++;
              });
              const opfs = createOpfsBackend({
                name: "future",
                directory: shim.root,
                locks: observedLocks,
              });
              try {
                const outcomes = await Promise.allSettled([
                  opfs.acquire({ steal: true, timeoutMs: 200 }),
                  opfs.acquire({ steal: true, timeoutMs: 200 }),
                ]);
                for (const outcome of outcomes) {
                  if (outcome.status === "fulfilled") {
                    await outcome.value.release();
                    throw new Error("future OPFS owner acquired");
                  }
                  expect(outcome.reason).toMatchObject({ code: "MINNOW_FORMAT_UNSUPPORTED" });
                }
                expect(lockCalls).toBe(0);
                expect(holder.lost.aborted).toBe(false);
                expect(writes).toBe(0);
                const after = new Map<string, Uint8Array | undefined>();
                for await (const name of shim.root.keys())
                  after.set(name, shim.readFileBytes(name));
                expect(after).toEqual(image);
                reach.reach("OPFS unchanged");
              } finally {
                holder.release();
              }
              const factory = new IDBFactory(),
                raw = await openIndexedDb(factory, "future"),
                witness = new Uint8Array([9, 17, 31]).buffer,
                futureSlot = slot === "root-0" ? "slot-0" : "slot-1",
                supportedSlot = slot === "root-0" ? "slot-1" : "slot-0";
              await raw.transaction(
                ["meta", "roots", "log", "data"],
                "readwrite",
                "strict",
                (tx) => {
                  tx.store("meta").put("epoch", encodeEpochSlot(7).buffer);
                  tx.store("roots").put(futureSlot, bytes.slice().buffer);
                  tx.store("roots").put(supportedSlot, other.slice().buffer);
                  for (const name of ["meta", "log", "data"]) tx.store(name).put(0, witness);
                },
              );
              const idbLocks = new SimLocks(),
                idbHolder = await idbLocks.request(ownerName, {});
              let idbLockCalls = 0,
                idbWrites = 0,
                opened = 0,
                closed = 0;
              const idb = createIndexedDbBackend({
                name: "future",
                locks: {
                  request: (...args) => {
                    if (args[0].startsWith("minnow-owner:")) idbLockCalls++;
                    return idbLocks.request(...args);
                  },
                },
                database: async () => {
                  const db = await openIndexedDb(factory, "future");
                  opened++;
                  return {
                    close: () => {
                      closed++;
                      db.close();
                    },
                    transaction: (stores, mode, durability, body) =>
                      db.transaction(stores, mode, durability, (tx) =>
                        body({
                          abort: (error) => tx.abort(error),
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
                            };
                          },
                        }),
                      ),
                  };
                },
              });
              try {
                const outcomes = await Promise.allSettled([
                  idb.acquire({ steal: true, timeoutMs: 200 }),
                  idb.acquire({ steal: true, timeoutMs: 200 }),
                ]);
                for (const outcome of outcomes) {
                  if (outcome.status === "fulfilled") {
                    await outcome.value.release();
                    throw new Error("future IDB owner acquired");
                  }
                  expect(outcome.reason).toMatchObject({ code: "MINNOW_FORMAT_UNSUPPORTED" });
                }
                expect(idbLockCalls).toBe(0);
                expect(idbHolder.lost.aborted).toBe(false);
                expect(idbWrites).toBe(0);
                expect(opened).toBe(2);
                expect(closed).toBe(2);
                await raw.transaction(
                  ["meta", "roots", "log", "data"],
                  "readonly",
                  "relaxed",
                  (tx) => {
                    tx.store("meta").get("epoch", (value) =>
                      expect(value).toEqual(encodeEpochSlot(7).buffer),
                    );
                    tx.store("roots").get(futureSlot, (value) =>
                      expect(new Uint8Array(value as ArrayBuffer)).toEqual(bytes),
                    );
                    tx.store("roots").get(supportedSlot, (value) =>
                      expect(new Uint8Array(value as ArrayBuffer)).toEqual(other),
                    );
                    for (const name of ["meta", "log", "data"])
                      tx.store(name).get(0, (value) => expect(value).toEqual(witness));
                  },
                );
                reach.reach("IDB unchanged");
                reach.reach("concurrent refusals");
                reach.reach("live lock untouched");
                reach.reach("zero writes");
              } finally {
                idbHolder.release();
                raw.close();
              }
            }
      }),
      { seed, numRuns: 4, endOnFailure: true },
    );
    reach.assertReached();
  }, 60000);

it("closes an IDB preflight connection arriving after the acquisition deadline", async () => {
  const factory = new IDBFactory(),
    raw = await openIndexedDb(factory, "late-probe");
  const coordination = new SimLocks();
  let deliver!: (db: IdbDatabaseLike) => void,
    notify!: () => void,
    locks = 0,
    closes = 0;
  const connection = new Promise<IdbDatabaseLike>((resolve) => {
      deliver = resolve;
    }),
    closed = new Promise<void>((resolve) => {
      notify = resolve;
    });
  const backend = createIndexedDbBackend({
    name: "late-probe",
    database: () => connection,
    locks: {
      request(name, options) {
        if (name.startsWith("minnow-gate:")) return coordination.request(name, options);
        locks++;
        return Promise.reject(new Error("must not acquire owner lock"));
      },
    },
  });
  await expect(backend.acquire({ timeoutMs: 5, steal: true })).rejects.toMatchObject({
    code: "MINNOW_OWNER_UNRESPONSIVE",
  });
  deliver({
    close() {
      closes++;
      raw.close();
      notify();
    },
    transaction: raw.transaction.bind(raw),
  });
  await closed;
  expect(closes).toBe(1);
  expect(locks).toBe(0);
});

for (const seed of seedsFor("next-root-gate-before-wal", [0xb2c7, 0xb2c8]))
  it(`persists both gates before ordinary catalog and FTS WAL seed ${String(seed)}`, async () => {
    const reach = createReach("root gates before WAL", ROOT_WRITE_GATE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 3 }), async (rows) => {
        reach.beginHistory();
        const h = new Harness(),
          open = h.backend.open.bind(h.backend);
        let appends = 0;
        h.backend.open = async (...args) => {
          const session = await open(...args),
            append = session.appendLog.bind(session);
          session.appendLog = async (bytes) => {
            if (session.readRootSlot === undefined)
              throw new Error("missing native root-slot read");
            let roots = 0;
            for (const slot of [0, 1] as const) {
              const frame = await session.readRootSlot(slot);
              if (frame === null) continue;
              const header = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
              expect(header.getUint32(16, true)).toBe(DURABLE_READER_VERSION);
              expect(header.getUint32(20, true)).toBe(DURABLE_WRITER_VERSION);
              expect(header.getUint8(11)).toBe(MANIFEST_FORMAT_VERSION - 2);
              roots++;
            }
            expect(roots).toBeGreaterThan(0);
            appends++;
            reach.reach("durable reader gate before append");
            reach.reach("durable writer gate before append");
            return append(bytes);
          };
          return session;
        };
        try {
          const path = await setupFts(h);
          await autocommit(path, (s) =>
            s
              .table(1)
              .insert(
                batch(texts(Array.from({ length: rows }, (_, i) => `alpha beta ${String(i)}`))),
                null,
              ),
          );
          const commit = await lastFtsCommit(h);
          expect(commit.body.effects.some((effect) => effect.kind === "fts")).toBe(true);
          expect(appends).toBeGreaterThan(1);
          reach.reach("FTS claims after gate");
          await h.reopen();
          await h.space.audit();
        } finally {
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
        reach.reach("zero charges");
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  });

it("aborts a stalled readonly preflight before closing without taking the owner lock", async () => {
  const events: string[] = [];
  const coordination = new SimLocks();
  let locks = 0;
  const unexpected = () => {
    throw new Error("unexpected preflight operation");
  };
  const never: IdbStoreLike = {
    get: () => undefined,
    getAll: unexpected,
    getAllKeys: unexpected,
    last: unexpected,
    put: unexpected,
    delete: unexpected,
    deleteRange: unexpected,
    clear: unexpected,
  };
  const database: IdbDatabaseLike = {
    close: () => {
      events.push("close");
    },
    transaction: (_stores, mode, _durability, body) => {
      expect(mode).toBe("readonly");
      return new Promise<void>((_resolve, reject) =>
        body({
          store: () => never,
          abort: (reason) => {
            events.push("abort");
            reject(reason);
          },
        }),
      );
    },
  };
  const backend = createIndexedDbBackend({
    name: "stalled-probe",
    database: () => database,
    releaseSettleMs: 50,
    locks: {
      request(name, options) {
        if (name.startsWith("minnow-gate:")) return coordination.request(name, options);
        locks++;
        return Promise.reject(new Error("must not acquire owner lock"));
      },
    },
  });
  await expect(backend.acquire({ timeoutMs: 5, steal: true })).rejects.toMatchObject({
    code: "MINNOW_OWNER_UNRESPONSIVE",
  });
  expect(events).toEqual(["abort", "close"]);
  expect(locks).toBe(0);
});
