/** Acknowledged bytes survive failed flushes; only committed transactions publish native bytes. */
import { afterAll, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, IDB_METADATA_FAILURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { METADATA_PAGE_BYTES, METADATA_SMALL_BYTES } from "../storage/contract.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import type { IdbDatabaseLike } from "../storage/idb-like.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { expectBytes } from "../storage/space-test-support.js";

const reach = createReach("private metadata transaction failures", IDB_METADATA_FAILURE_REACH);

for (const seed of seedsFor("idb-private-metadata-failures", [1775, 1776, 1777, 1778]))
  it(`preserves acknowledged pages across consecutive failed transactions (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      inner = simIndexedDb(new SimDisk()),
      governor = new Governor(64 * 1024 * 1024),
      acknowledged = new Map<number, Uint8Array>(),
      persisted = new Map<number, Uint8Array>();
    let allowed = true,
      failAt = 0,
      failureKind: "abort" | "request error" = "abort",
      failure = new Error("No failure armed");
    const database: IdbDatabaseLike = {
      close: () => inner.close(),
      transaction(stores, mode, durability, body) {
        const inject =
          failAt > 0 &&
          stores.length === 1 &&
          stores[0] === "meta" &&
          mode === "readwrite" &&
          durability === "relaxed" &&
          --failAt === 0;
        return inner.transaction(stores, mode, durability, (tx) => {
          body({
            abort: (reason) => tx.abort(reason),
            store: (name) => {
              const store = tx.store(name);
              return {
                get: (key, next) => store.get(key, next),
                getAll: (range, next, count) => store.getAll(range, next, count),
                getAllKeys: (range, next, count) => store.getAllKeys(range, next, count),
                last: (range, next) => store.last(range, next),
                put: (key, value) => {
                  store.put(key, value);
                  // Throw after issuing the put from the epoch callback. The transaction
                  // must roll it back just as it rolls back an explicit abort.
                  if (
                    inject &&
                    failureKind === "request error" &&
                    typeof key === "number" &&
                    key < 0
                  )
                    throw failure;
                },
                delete: (key) => store.delete(key),
                deleteRange: (range) => store.deleteRange(range),
                clear: () => store.clear(),
              };
            },
          });
          if (inject && failureKind === "abort") tx.abort(failure);
        });
      },
    };
    const backend = createIndexedDbBackend({
        database: () => database,
        locks: new SimLocks(),
        name: "metadata-failure-history",
        registerCache: (cache) => {
          const memory = governor.registerCache(cache);
          return { ...memory, charge: (bytes) => allowed && memory.charge(bytes) };
        },
      }),
      lease = await backend.acquire(),
      target = new Uint8Array(METADATA_PAGE_BYTES);
    let session = await backend.open(lease),
      metadata = session.metadata?.();
    if (metadata == null) throw Error("Missing metadata store");

    const nativeAgrees = async (model: ReadonlyMap<number, Uint8Array>) => {
      const keys: number[] = [],
        values: Uint8Array[] = [];
      await inner.transaction(["meta"], "readonly", "relaxed", (tx) => {
        tx.store("meta").getAllKeys({ upper: -1 }, (found) => {
          for (const key of found) {
            if (typeof key !== "number") throw Error("Non-numeric private key");
            keys.push(-key - 1);
          }
        });
        tx.store("meta").getAll({ upper: -1 }, (found) => {
          for (const value of found) {
            if (!(value instanceof ArrayBuffer)) throw Error("Malformed private page");
            values.push(new Uint8Array(value));
          }
        });
      });
      expect([...keys].sort((a, b) => a - b)).toEqual([...model.keys()].sort((a, b) => a - b));
      expect(values).toHaveLength(keys.length);
      for (let index = 0; index < keys.length; index++) {
        const page = keys[index],
          bytes = values[index],
          expected = page === undefined ? undefined : model.get(page);
        if (bytes === undefined || expected === undefined) throw Error("Missing native model page");
        expectBytes(bytes, expected, `native page ${String(page)}`);
      }
    };
    const agrees = async () => {
      if (metadata == null) throw Error("Missing metadata store");
      for (const [page, bytes] of acknowledged) {
        target.fill(255);
        await metadata.read(page, target);
        expectBytes(target.subarray(0, bytes.length), bytes, `acknowledged page ${String(page)}`);
        expect(target.subarray(bytes.length).every((byte) => byte === 0)).toBe(true);
      }
      // Dirty writeback is intentionally ahead of native storage. The native oracle only
      // advances at a known successful transaction; aborted requests publish nothing.
      await nativeAgrees(persisted);
    };
    try {
      await metadata.reset();
      await agrees();
      for (let round = 0; round < 8; round++) {
        const page = round % 3,
          length = round % 2 === 0 ? METADATA_PAGE_BYTES : METADATA_SMALL_BYTES,
          original = new Uint8Array(length).fill(1 + Math.floor(random() * 126)),
          replacement = new Uint8Array(
            length === METADATA_PAGE_BYTES ? METADATA_SMALL_BYTES : METADATA_PAGE_BYTES,
          ).fill(128 + Math.floor(random() * 127)),
          failures = 2 + Math.floor(random() * 3);
        allowed = true;
        await metadata.write(page, original);
        acknowledged.set(page, original.slice());
        await agrees();
        reach.reach("dirty acknowledged page ahead of storage");
        allowed = false;
        failureKind = round % 4 < 2 ? "abort" : "request error";
        for (let attempt = 0; attempt < failures; attempt++) {
          // Some histories first commit the old flush and fail the replacement; subsequent
          // attempts fail their first transaction. Never reset or rewrite the dirty original
          // between consecutive failures: doing so masks premature clean publication.
          const nth = round >= 4 && attempt === 0 ? 2 : 1;
          failAt = nth;
          failure = new Error(`metadata ${failureKind}: ${String(round)}/${String(attempt)}`);
          await expect(metadata.write(page, replacement)).rejects.toThrow(failure.message);
          expect(failAt).toBe(0);
          if (nth === 2) persisted.set(page, original.slice());
          await agrees();
          reach.reach(
            nth === 2
              ? "old flush committed before failed replacement"
              : "failed first transaction",
          );
          if (attempt === 1)
            reach.reach(
              failureKind === "abort" ? "consecutive aborts" : "consecutive callback failures",
            );
        }
        // A successful native transaction on another page must not repair or discard the
        // original page's pending state. Credit remains refused, so this fresh page writes through.
        const otherPage = 3 + round,
          other = new Uint8Array(random() < 0.5 ? METADATA_SMALL_BYTES : METADATA_PAGE_BYTES).fill(
            71,
          );
        await metadata.write(otherPage, other);
        acknowledged.set(otherPage, other.slice());
        persisted.set(otherPage, other.slice());
        await agrees();
        reach.reach("successful transactions between failed bursts");
        await metadata.write(page, replacement);
        acknowledged.set(page, replacement.slice());
        persisted.set(page, replacement.slice());
        await agrees();
        reach.reach(
          length === METADATA_PAGE_BYTES
            ? "full to compact replacement"
            : "compact to full replacement",
        );
        reach.reach("both publication oracles after every action");
        await session.close();
        expect(governor.usedBytes).toBe(0);
        await nativeAgrees(acknowledged);
        session = await backend.open(lease);
        metadata = session.metadata?.();
        if (metadata == null) throw Error("Missing metadata store");
        await agrees();
        reach.reach("close and reopen agreement");
      }
    } finally {
      await session.close();
      await lease.release();
    }
    expect(governor.usedBytes).toBe(0);
  }, 60_000);

afterAll(() => reach.assertReached());
