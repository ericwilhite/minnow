/** Native binding law: external stores install committed decoded rows, fence stale work,
 * retain finite charged state, and drain owned work before close resolves. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  LIVE_STORE_BINDING_REACH,
  LIVE_STORE_DISPOSAL_REACH,
} from "../../testing/reach.js";
import { NativeLiveStore, type NativeLiveOptions } from "../owner/live-store.js";
import type { Param } from "../owner/api.js";
import { captureLiveParams } from "../owner/live-params.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { setup, until, publicationTurn } from "./live-runtime.test-support.js";

for (const seed of seedsFor("next-live-store-binding", [8890, 8891]))
  it(`preserves native store snapshots and retirement seed=${String(seed)}`, async () => {
    const reach = createReach("native live binding", LIVE_STORE_BINDING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: 1, max: 100 }), { minLength: 1, maxLength: 4 }),
        fc.boolean(),
        async (values, failDecode) => {
          reach.beginHistory();
          const f = await setup();
          try {
            const db = new MinnowDatabaseClient(f.client, { ready: Promise.resolve() });
            await db.execute("CREATE TABLE t(id INT PRIMARY KEY, n BIGINT, hidden INT)");
            await db.execute("INSERT INTO t VALUES(1,9007199254740993,0)");
            let rejectDecode = false;
            const sourceOptions = {
              decode: (result) => {
                if (rejectDecode) throw new Error("adapter decode rejected");
                return result.rows();
              },
              key: "id",
              table: "t",
              memoryBytes: 4 * 1048576,
            } satisfies NativeLiveOptions<{ id: number; n: bigint }>;
            const store = db.liveQuery<{ id: number; n: bigint }>(
              "SELECT id,n FROM t ORDER BY id",
              [],
              sourceOptions,
            );
            if (failDecode) {
              sourceOptions.table = "not_the_captured_table";
              reach.reach("captured options");
            }
            let rekeyNotices = 0;
            const stop = store.subscribe(() => {
              const snapshot = store.getSnapshot();
              if (snapshot.status === "ready") rekeyNotices += snapshot.keyChanges?.length ?? 0;
            });
            try {
              const first = store.refresh(),
                shared = store.refresh();
              expect(first).toBe(shared);
              await first;
              const snapshot = store.getSnapshot();
              expect(snapshot.status).toBe("ready");
              expect(snapshot.rows).toEqual([{ id: 1, n: 9007199254740993n }]);
              expect(store.getSnapshot()).toBe(snapshot);
              reach.reach("ready");
              const iterator = store[Symbol.asyncIterator]();
              expect((await iterator.next()).value).toBe(store.getSnapshot());
              const pendingNext = iterator.next();
              await expect(iterator.next()).rejects.toThrow(/one live iterator/);
              await iterator.return?.();
              expect((await pendingNext).done).toBe(true);
              const coalescing = store[Symbol.asyncIterator]();
              expect((await coalescing.next()).value).toMatchObject({
                status: "ready",
                reset: true,
              });
              reach.reach("iterator start reset");
              if (failDecode) {
                rejectDecode = true;
                await db.execute("UPDATE t SET n=42 WHERE id=1");
                await until(() => store.getSnapshot().status === "error");
                expect(store.getSnapshot().rows).toBe(snapshot.rows);
                reach.reach("decode error preserves rows");
                rejectDecode = false;
                await store.refresh();
                expect(store.getSnapshot().rows[0]?.n).toBe(42n);
                reach.reach("error recovery");
              }
              for (const n of values) {
                await db.execute("UPDATE t SET n=$1 WHERE id=1", [BigInt(n)]);
                await publicationTurn();
                await until(() => store.getSnapshot().rows[0]?.n === BigInt(n));
                expect(store.getSnapshot().rows).toEqual(
                  (await db.query("SELECT id,n FROM t ORDER BY id")).rows(),
                );
                reach.reach("update");
              }
              expect(store.getSnapshot().patch).toBeDefined();
              reach.reach("keyed patch");
              await db.execute("UPDATE t SET id=2 WHERE id=1");
              await publicationTurn();
              await until(() => store.getSnapshot().rows[0]?.id === 2);
              expect(store.getSnapshot().changes).toEqual(
                expect.arrayContaining([
                  expect.objectContaining({ type: "rekey", from: 1, key: 2 }),
                ]),
              );
              const beforeUnchanged = store.getSnapshot();
              await db.execute("UPDATE t SET hidden=hidden+1 WHERE id=2");
              await publicationTurn();
              await until(() => store.getSnapshot() !== beforeUnchanged);
              expect(store.getSnapshot()).toMatchObject({
                status: "ready",
                reset: false,
                changes: [],
                keyChanges: [],
                patch: { type: "patch", changedRows: [], retained: Int32Array.of(0) },
              });
              expect(rekeyNotices).toBe(1);
              reach.reach("unchanged delivery clears notices");
              const afterGap = (await coalescing.next()).value;
              expect(afterGap).toMatchObject({
                status: "ready",
                reset: true,
                keyChanges: [],
                changes: [],
                patch: { type: "reset" },
              });
              expect(afterGap?.rows).toBe(store.getSnapshot().rows);
              await coalescing.return?.();
              reach.reach("iterator gap reset");
              const late = store[Symbol.asyncIterator]();
              expect((await late.next()).value).toMatchObject({
                status: "ready",
                reset: true,
                keyChanges: [],
                changes: [],
                patch: { type: "reset" },
              });
              await late.return?.();
              reach.reach("iterator late reset");
              await f.changeOwner();
              await store.refresh();
              await until(
                () => store.getSnapshot().status === "ready" && store.getSnapshot() !== snapshot,
              );
              expect(store.getSnapshot().status).toBe("ready");
              expect(store.getSnapshot()).toMatchObject({ reset: true });
              expect(store.getSnapshot().rows[0]?.n).toBe(BigInt(values.at(-1) ?? 0));
              reach.reach("reconnect reset");
            } finally {
              stop();
              await store.close();
            }
            expect(store.usedBytes).toBe(0);
            let calls = 0,
              resumed = false,
              refreshSettled = false;
            let releaseRefresh: (() => void) | undefined;
            let refreshing: Promise<void> | undefined;
            const reentrant = new NativeLiveStore(f.client, "SELECT id FROM t", [], {
              decode: async (result) => {
                calls++;
                if (calls === 1 && failDecode) throw new Error("first decode rejected");
                if (calls === 2) {
                  resumed = true;
                  await new Promise<void>((resolve) => {
                    releaseRefresh = resolve;
                  });
                }
                return result.rows();
              },
            });
            const stopReentrant = reentrant.subscribe(() => {
              if (
                calls === 1 &&
                reentrant.getSnapshot().status === (failDecode ? "error" : "ready")
              ) {
                refreshing = reentrant.refresh().then(() => {
                  refreshSettled = true;
                });
              }
            });
            try {
              await until(() => resumed);
              expect(refreshSettled).toBe(false);
              releaseRefresh?.();
              await refreshing;
              expect(reentrant.getSnapshot().status).toBe("ready");
              reach.reach(failDecode ? "refresh reentry error" : "refresh reentry ready");
            } finally {
              releaseRefresh?.();
              stopReentrant();
              await reentrant.close();
            }
            const gate: { release?: () => void } = {};
            let entered = false;
            let decoders = 0;
            const slow = new NativeLiveStore<{ id: number }>(f.client, "SELECT id FROM t", [], {
              decode: async (result) => {
                entered = true;
                decoders++;
                await new Promise<void>((resolve) => {
                  gate.release = resolve;
                });
                return result.rows();
              },
            });
            const unsubscribe = slow.subscribe(() => undefined);
            let resubscribed: (() => void) | undefined;
            try {
              await until(() => entered);
              unsubscribe();
              resubscribed = slow.subscribe(() => undefined);
              await publicationTurn();
              expect(decoders).toBe(1);
              reach.reach("resubscribe drains decoder");
              let drained = false;
              const closing = slow.close().then(() => {
                drained = true;
              });
              await Promise.resolve();
              expect(drained).toBe(false);
              gate.release?.();
              await closing;
              expect(slow.getSnapshot().status).toBe("loading");
              expect(slow.usedBytes).toBe(0);
              reach.reach("decode retirement");
              reach.reach("zero charge");
            } finally {
              gate.release?.();
              unsubscribe();
              resubscribed?.();
              await slow.close();
            }
          } finally {
            await f.close();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [[1], true],
          [[1], false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-live-store-disposal", [8892, 8893]))
  it(`fences native store disposal and decode refusal seed=${String(seed)}`, async () => {
    const reach = createReach("native store disposal", LIVE_STORE_DISPOSAL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("loading", "ready", "decode", "client", "activity", "capture"),
        fc.constantFrom(65536, 131072, 2097152),
        async (phase, memoryBytes) => {
          reach.beginHistory();
          const f = await setup();
          const db = new MinnowDatabaseClient(f.client, { ready: Promise.resolve() });
          let closed: Promise<void> | null = null;
          if (phase === "capture") {
            const escaped: { store?: NativeLiveStore } = {};
            try {
              expect(() => {
                escaped.store = db.liveQuery("SELECT 1", [], {
                  get table() {
                    closed = db.close();
                    return "t";
                  },
                });
              }).toThrow(/closed/i);
              expect(closed).toBeInstanceOf(Promise);
              await Promise.resolve(closed);
              expect(NativeLiveStore.clientUsedBytes(f.client)).toBe(0);
              reach.reach("capture teardown");
              reach.reach("zero charge");
            } finally {
              await escaped.store?.close();
              await db.close();
              await f.close();
            }
            return;
          }
          let deliveries = 0;
          let release: (() => void) | undefined;
          let entered = false;
          const store = db.liveQuery<{ v: string }>("SELECT '" + "x".repeat(1000) + "' v", [], {
            memoryBytes,
            decode: async (result) => {
              if (phase === "decode" || phase === "client") {
                reach.reach("decode");
                entered = true;
                await new Promise<void>((resolve) => {
                  release = resolve;
                });
              }
              return result.rows();
            },
          });
          const stop = store.subscribe(() => {
            const status = store.getSnapshot().status;
            if (status === "ready") {
              deliveries++;
              if (phase === "ready") {
                reach.reach("ready");
                closed ??= store.close();
              }
            }
            if (status === "loading" && phase === "loading") {
              reach.reach("loading");
              closed ??= store.close();
            }
            if (status === "error") reach.reach("refusal");
          });
          let postClose = 0;
          const stopSecond = store.subscribe(() => {
            if (closed !== null) postClose++;
          });
          try {
            if (phase === "decode" || phase === "client") {
              await until(() => entered || store.getSnapshot().status === "error");
              if (memoryBytes > 65536) expect(entered, "admitted decoder is reached").toBe(true);
              if (phase === "client") reach.reach("client teardown");
              closed = phase === "client" ? db.close() : store.close();
              release?.();
            } else {
              await until(() => store.getSnapshot().status !== "loading" || closed !== null);
              if (memoryBytes > 65536 && phase !== "ready")
                expect(store.getSnapshot().status).toBe("ready");
              if (phase === "loading" && store.getSnapshot().status === "ready") {
                const a = store.refresh(),
                  b = store.refresh();
                // A reentrant close may settle a immediately; neither refresh may restart a closed store.
                await Promise.allSettled([a, b]);
              }
              if (phase === "activity") {
                f.client.onActivity = () => {
                  reach.reach("activity teardown");
                  closed ??= store.close();
                };
              }
              closed ??= store.close();
            }
            await closed;
            const prior = deliveries;
            await publicationTurn();
            expect(deliveries).toBe(prior);
            expect(postClose).toBe(0);
            reach.reach("no later listener after close");
            expect(store.usedBytes).toBe(0);
            expect(f.client.liveCount).toBe(0);
            reach.reach("zero charge");
          } finally {
            release?.();
            stop();
            stopSecond();
            await store.close();
            await f.close();
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ["loading", 2097152],
          ["ready", 2097152],
          ["decode", 2097152],
          ["client", 2097152],
          ["activity", 2097152],
          ["capture", 2097152],
          ["ready", 65536],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-live-store-account", [8896, 8897]))
  it(`bounds the shared cold-store account seed=${String(seed)}`, async () => {
    const { ProtocolClient } = await import("../owner/client.js");
    const { LIVE_STORE_ACCOUNT_REACH } = await import("../../testing/reach.js");
    const reach = createReach("shared native client account", LIVE_STORE_ACCOUNT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(8, 32, 128), async (width) => {
        reach.beginHistory();
        const client = new ProtocolClient();
        const stores: NativeLiveStore[] = [];
        let refused = false;
        try {
          const escaped: { store?: NativeLiveStore } = {};
          const view = new Uint8Array(new ArrayBuffer(16 * 1048576), 0, 1);
          try {
            expect(() => {
              escaped.store = new NativeLiveStore(client, "SELECT $1 v", [view]);
            }).toThrow(/live source/);
          } finally {
            await escaped.store?.close();
          }
          expect(NativeLiveStore.clientUsedBytes(client)).toBe(0);
          reach.reach("wide source view refused");
          const shadow = new Uint8Array(new ArrayBuffer(16 * 1048576), 0, 1);
          Object.defineProperty(shadow, "buffer", { value: new ArrayBuffer(1) });
          try {
            expect(() => {
              escaped.store = new NativeLiveStore(client, "SELECT $1", [shadow]);
            }).toThrow(/live source/);
          } finally {
            await escaped.store?.close();
          }
          expect(NativeLiveStore.clientUsedBytes(client)).toBe(0);
          reach.reach("shadowed source view refused");
          const metadata: { close?: () => Promise<void> } = {};
          for (const kind of ["table", "key"] as const) {
            try {
              expect(() => {
                const store = new NativeLiveStore<Record<string, number>>(client, "SELECT 1", [], {
                  [kind]: "x".repeat(16 * 1048576),
                });
                metadata.close = () => store.close();
              }).toThrow(/live source/);
            } finally {
              await metadata.close?.();
              delete metadata.close;
            }
            expect(NativeLiveStore.clientUsedBytes(client)).toBe(0);
          }
          reach.reach("wide source metadata refused");
          for (let i = 0; i < 4096; i++) {
            try {
              stores.push(new NativeLiveStore(client, "SELECT $1 v", ["x".repeat(width)]));
            } catch (error) {
              expect(error).toMatchObject({ code: "MINNOW_MEMORY_ADMISSION" });
              refused = true;
              break;
            }
          }
          expect(stores.reduce((sum, store) => sum + store.usedBytes, 0)).toBeLessThanOrEqual(
            64 * 1048576,
          );
          expect(stores.reduce((sum, store) => sum + store.usedBytes, 0)).toBeGreaterThan(
            32 * 1048576,
          );
          expect(refused).toBe(true);
          expect(client.liveCount).toBe(0);
          reach.reach("cold stores bounded");
          reach.reach("shared refusal");
        } finally {
          await Promise.all(stores.map((store) => store.close()));
          client.close();
        }
        expect(stores.every((store) => store.usedBytes === 0)).toBe(true);
        expect(NativeLiveStore.clientUsedBytes(client)).toBe(0);
        reach.reach("all credit returned");
      }),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-live-store-capture", [8898, 8899]))
  it(`captures native source values once seed=${String(seed)}`, async () => {
    const { LIVE_STORE_CAPTURE_REACH } = await import("../../testing/reach.js");
    const reach = createReach("native source capture", LIVE_STORE_CAPTURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -100, max: 100 }),
        fc.constantFrom("scalar", "bytes", "date"),
        fc.boolean(),
        async (n, kind, shadow) => {
          reach.beginHistory();
          const bytes = new Uint8Array(new ArrayBuffer(8192), 128, 1);
          bytes[0] = n & 255;
          if (shadow) Object.defineProperty(bytes, "buffer", { value: new ArrayBuffer(1) });
          const instant = Date.UTC(2000, 0, 1, 0, 0, 0, 123) + n * 1000;
          const date = new Date(instant);
          let reads = 0,
            scalar = n,
            paid = 0;
          const params: Param[] = [];
          Object.defineProperty(params, "0", {
            enumerable: true,
            get() {
              reads++;
              return kind === "scalar" ? scalar++ : kind === "bytes" ? bytes : date;
            },
          });
          const clone = globalThis.structuredClone;
          const spy = vi.spyOn(globalThis, "structuredClone").mockImplementation((value) => {
            expect(paid).toBeGreaterThanOrEqual(8192);
            return clone(value);
          });
          let captured: readonly Param[];
          try {
            captured = captureLiveParams(params, (count) => {
              paid += count;
            });
          } finally {
            spy.mockRestore();
          }
          expect(reads).toBe(1);
          scalar = n + 7;
          bytes[0] = (n + 1) & 255;
          date.setTime(instant + 5000);
          if (kind === "bytes") {
            expect(captured[0]).toEqual(Uint8Array.of(n & 255));
            expect((captured[0] as Uint8Array).buffer.byteLength).toBe(8192);
            expect(paid).toBeGreaterThanOrEqual(8192);
            reach.reach("admitted before clone");
            if (shadow) reach.reach("shadowed buffer");
          } else if (kind === "date") expect((captured[0] as Date).getTime()).toBe(instant);
          else {
            expect(captured).toEqual([n]);
            const f = await setup();
            const store = new NativeLiveStore<{ v: number }>(
              f.client,
              "SELECT $1::integer v",
              captured,
            );
            try {
              await store.refresh();
              expect(store.getSnapshot().rows).toEqual([{ v: n }]);
            } finally {
              await store.close();
              await f.close();
            }
            expect(store.usedBytes).toBe(0);
            reach.reach("zero charge");
          }
          expect(reads).toBe(1);
          reach.reach(kind);
          reach.reach("getter once");
          reach.reach("captured value");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, "scalar", false],
          [1, "bytes", false],
          [1, "bytes", true],
          [1, "date", false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
