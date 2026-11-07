/** R25.5: normal startup is lazy; full native index integrity remains in audit/test open. */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb, type IdbDatabaseLike, type IdbStoreLike } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { openExtentSpace } from "../storage/space.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import fixture from "../format/root-r25.fixture.json";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

function tracked(db: IdbDatabaseLike, bulk: () => void): IdbDatabaseLike {
  const wrap = (store: IdbStoreLike): IdbStoreLike => ({
    get: store.get.bind(store),
    last: store.last.bind(store),
    put: store.put.bind(store),
    delete: store.delete.bind(store),
    deleteRange: store.deleteRange.bind(store),
    clear: store.clear.bind(store),
    getAll(range, then, count) {
      bulk();
      store.getAll(range, then, count);
    },
    getAllKeys(range, then, count) {
      bulk();
      store.getAllKeys(range, then, count);
    },
  });
  return {
    close: () => db.close(),
    transaction: (stores, mode, durability, body) =>
      db.transaction(stores, mode, durability, (tx) =>
        body({
          store: (name) => wrap(tx.store(name)),
          abort: (reason) => tx.abort(reason),
        }),
      ),
  };
}

it.each([17, 8193])(
  "opens %i physical records without bulk enumeration and continues writing",
  async (count) => {
    const factory = new IDBFactory();
    const db = await openIndexedDb(factory, "lazy");
    await db.transaction(
      ["data", "data-index", "log", "log-index"],
      "readwrite",
      "strict",
      (tx) => {
        for (const [name, rows] of [
          ["data", count],
          ["log", 3],
        ] as const)
          for (let index = 0; index < rows; index++) {
            const offset = index * 4096;
            tx.store(name).put(offset, { offset, bytes: new Uint8Array([7]).buffer });
            tx.store(`${name}-index`).put(offset, { offset, length: 1 });
          }
      },
    );
    db.close();
    let bulkCalls = 0;
    const backend = createIndexedDbBackend({
      name: "lazy",
      locks: new SimLocks(),
      database: async () => tracked(await openIndexedDb(factory, "lazy"), () => bulkCalls++),
    });
    const lease = await backend.acquire();
    try {
      const session = await backend.open(lease);
      expect(bulkCalls).toBe(0);
      expect(await session.dataLength()).toBe((count - 1) * 4096 + 1);
      expect(session.logLength()).toBe(8193);
      expect((await session.readData([{ offset: (count - 1) * 4096, length: 1 }]))[0]).toEqual(
        new Uint8Array([7]),
      );
      await session.writeData(0, new Uint8Array([9]));
      await session.publish(new Uint8Array([1]));
      expect((await session.readData([{ offset: 0, length: 1 }]))[0]).toEqual(new Uint8Array([9]));
      await session.releaseData([{ offset: (count - 1) * 4096, length: 4096 }]);
      expect(await session.dataLength()).toBe((count - 2) * 4096 + 1);
      expect(typeof session.audit).toBe("function");
      const beforeAudit = bulkCalls;
      await session.audit?.();
      expect(bulkCalls).toBeGreaterThan(beforeAudit);
      await session.close();
    } finally {
      await lease.release();
    }
    const next = await backend.acquire();
    try {
      const before = bulkCalls;
      const session = await backend.open(next);
      expect(bulkCalls).toBe(before);
      expect(await session.dataLength()).toBe((count - 2) * 4096 + 1);
      expect((await session.readData([{ offset: 0, length: 1 }]))[0]).toEqual(new Uint8Array([9]));
      await session.close();
    } finally {
      await next.release();
    }
  },
);

it("retains whole-store orphan refusal in ordinary audit and test-build open", async () => {
  const factory = new IDBFactory();
  const backend = createIndexedDbBackend({
    name: "audit",
    locks: new SimLocks(),
    database: () => openIndexedDb(factory, "audit"),
  });
  const hex = (value: string) => Uint8Array.from(Buffer.from(value, "hex"));
  const options = {
    reachable: listedRootExtents(),
    walk: reachableExtents(),
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(Infinity),
  };
  const seed = await backend.acquire();
  try {
    const session = await backend.open(seed);
    for (const payload of fixture.payloads)
      await session.writeData(payload.offset, hex(payload.hex));
    await session.publish(hex(fixture.rootHex));
    await session.close();
  } finally {
    await seed.release();
  }
  const raw = await openIndexedDb(factory, "audit");
  await raw.transaction(["data"], "readwrite", "strict", (tx) => {
    tx.store("data").put(2 ** 29, { offset: 2 ** 29, bytes: new ArrayBuffer(1) });
  });
  raw.close();
  const normal = await backend.acquire();
  try {
    const session = await backend.open(normal);
    const space = await openExtentSpace(session, options);
    await expect(space.audit()).rejects.toBeInstanceOf(StoreCorruptionError);
    await space.close();
    await session.close();
  } finally {
    await normal.release();
  }
  for (let attempt = 0; attempt < 2; attempt++) {
    const lease = await backend.acquire();
    try {
      const session = await backend.open(lease);
      await expect(
        openExtentSpace(session, { ...options, verifyAtOpen: true }),
      ).rejects.toBeInstanceOf(StoreCorruptionError);
      await session.close();
    } finally {
      await lease.release();
    }
  }
});

it("restores the lazy end and boundary records after an interrupted replacement", async () => {
  const { ByteStore } = await import("../storage/idb-bytes.js");
  const factory = new IDBFactory();
  const db = await openIndexedDb(factory, "lazy-rollback");
  const store = await ByteStore.openLazy(db, "data");
  const initial = new Uint8Array(16_384).fill(7);
  try {
    await db.transaction(["data", "data-index"], "readwrite", "strict", (tx) =>
      store.write(tx, 0, initial, () => undefined),
    );
    store.commit();
    const failure = new Error("interrupted replacement");
    await expect(
      db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
        store.write(tx, 4096, new Uint8Array(4096).fill(9), () => {
          store.clear(tx, 12_288, Number.MAX_SAFE_INTEGER, () => tx.abort(failure));
        });
      }),
    ).rejects.toBe(failure);
    store.rollback();
    expect(store.end).toBe(initial.length);
    const restored = new Uint8Array(initial.length);
    await db.transaction(["data", "data-index"], "readonly", "relaxed", (tx) =>
      store.read(tx, 0, restored, () => undefined),
    );
    expect(restored).toEqual(initial);
    await ByteStore.audit(db, "data");
    const reopened = await ByteStore.openLazy(db, "data");
    expect(reopened.end).toBe(initial.length);
    reopened.close();
  } finally {
    store.close();
    db.close();
  }
});
