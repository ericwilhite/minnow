/**
 * What happens when the browser refuses a write because the origin is out of space.
 *
 * This is the characteristic way a browser database fails. A server database runs out of disk on
 * a machine somebody monitors; a browser database runs out of quota on a stranger's laptop, at an
 * arbitrary moment, with no warning and no operator. It is also the failure an application most
 * needs to handle deliberately — "your data did not save, free some space" is a very different
 * message from "something went wrong" — so the contract has to be worth relying on.
 *
 * Four things are pinned here, and each is something an application would build on:
 *
 *   1. the write fails rather than half-succeeding
 *   2. the error keeps its identity, so `error.name === "QuotaExceededError"` still works after
 *      it has crossed the storage layer — this is what lets an app tell "out of space" apart
 *      from a conflict or a corruption
 *   3. everything committed before the failure is still there and still readable
 *   4. the same write succeeds once space is available, with no reopen and no repair step
 *
 * Scope: the refusal is injected at the block store, which is the layer the engine talks to and
 * the layer whose behaviour this project controls. IndexedDB's own transaction-abort semantics
 * underneath are the browser's, and provoking a genuine quota error would mean actually filling
 * a disk. What is tested here is everything above that line.
 */
import { describe, expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { MinnowDatabase } from "../engine/database.js";
import { FaultInjectingBlockStore } from "../testing/index.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { MemoryBlockStore } from "./memory.js";
import type { BlockStore } from "./types.js";

/**
 * The error a browser raises when an origin is over quota. `DOMException` exists in Node 24, and
 * using the real thing rather than a stand-in is the point: the assertions below are about the
 * identity surviving, and a plain `Error` would prove nothing about that.
 */
function quotaExceeded(): DOMException {
  return new DOMException("The quota has been exceeded.", "QuotaExceededError");
}

const ROWS = Array.from({ length: 120 }, (_, index) => ({
  id: index + 1,
  region: index % 2 === 0 ? "west" : "east",
  amount: index,
}));

async function seeded(
  inner: BlockStore,
): Promise<{ database: MinnowDatabase; refuseWrites: (on: boolean) => void }> {
  let refusing = false;
  const store = new FaultInjectingBlockStore(inner, (point) => {
    // A real quota failure lands when bytes are handed to storage, which is the block write.
    if (refusing && point === "beforeBlockWrite") throw quotaExceeded();
  });
  const database = new MinnowDatabase(store, { rowsPerBlock: 16, autoCompact: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("items", ROWS);
  return {
    database,
    refuseWrites: (on: boolean) => {
      refusing = on;
    },
  };
}

const stores: Array<{ name: string; create: () => Promise<BlockStore> }> = [
  { name: "memory", create: async () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

describe.each(stores)("running out of storage quota ($name)", ({ create }) => {
  it("refuses the write instead of half-applying it", async () => {
    const { database, refuseWrites } = await seeded(await create());
    refuseWrites(true);

    await expect(
      database.insertBatch("items", [{ id: 5_000, region: "north", amount: 1 }]),
    ).rejects.toThrow();

    refuseWrites(false);
    // Not one row of the refused batch may be visible. A partially applied write is worse than a
    // failed one: the application has been told it failed and would write it again.
    expect(
      (await database.query("SELECT id FROM items WHERE id = 5000", { memoize: false })).rows,
    ).toEqual([]);
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
    ).toEqual([{ n: ROWS.length }]);
  });

  it("keeps the error identifiable as a quota failure", async () => {
    const { database, refuseWrites } = await seeded(await create());
    refuseWrites(true);

    // The whole point: an application catches this and shows "free up space", which it can only
    // do if the name survives the trip out through the storage layer and the engine.
    const error = await database
      .insertBatch("items", [{ id: 5_001, region: "north", amount: 1 }])
      .then(
        () => undefined,
        (reason: unknown) => reason,
      );
    expect(error, "the write should have failed").toBeDefined();
    // Both halves matter. The name is what an application branches on; `instanceof` is what
    // proves the engine handed the browser's own exception back rather than re-wrapping it in
    // something that merely copied the string across.
    expect(error).toBeInstanceOf(DOMException);
    expect((error as DOMException).name).toBe("QuotaExceededError");
  });

  it("leaves everything committed before the failure readable", async () => {
    const { database, refuseWrites } = await seeded(await create());
    refuseWrites(true);
    await expect(
      database.insertBatch("items", [{ id: 5_002, region: "north", amount: 1 }]),
    ).rejects.toThrow();
    refuseWrites(false);

    // Reads must not need a repair step, a reopen, or a compaction to work again.
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
    ).toEqual([{ n: ROWS.length }]);
    expect(
      (await database.query("SELECT amount FROM items WHERE id = 1", { memoize: false })).rows,
    ).toEqual([{ amount: 0 }]);
    expect(
      (
        await database.query("SELECT region, COUNT(*) AS n FROM items GROUP BY region", {
          memoize: false,
        })
      ).rows.length,
    ).toBe(2);
  });

  it("accepts the same write once there is room again", async () => {
    const { database, refuseWrites } = await seeded(await create());
    refuseWrites(true);
    await expect(
      database.insertBatch("items", [{ id: 5_003, region: "north", amount: 7 }]),
    ).rejects.toThrow();

    // Freeing space is something the user does, outside the database. What matters here is that
    // the database needs nothing done to it: the retry is just the write again.
    refuseWrites(false);
    await database.insertBatch("items", [{ id: 5_003, region: "north", amount: 7 }]);
    expect(
      (await database.query("SELECT amount FROM items WHERE id = 5003", { memoize: false })).rows,
    ).toEqual([{ amount: 7 }]);
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
    ).toEqual([{ n: ROWS.length + 1 }]);
  });

  it("survives a refusal in the middle of a run of writes", async () => {
    // One failure among many, which is how quota actually arrives: writes succeed until the
    // moment they do not, and the application keeps going afterwards.
    const { database, refuseWrites } = await seeded(await create());
    let refused = 0;
    for (let index = 0; index < 12; index += 1) {
      const failing = index % 4 === 1;
      refuseWrites(failing);
      const outcome = await database
        .insertBatch("items", [{ id: 6_000 + index, region: "south", amount: index }])
        .then(
          () => "ok",
          () => "failed",
        );
      if (failing) {
        refused += 1;
        expect(outcome).toBe("failed");
      } else {
        expect(outcome).toBe("ok");
      }
    }
    refuseWrites(false);
    expect(refused).toBeGreaterThan(0);

    // Exactly the writes that were not refused are present -- no gaps, no duplicates, no rows
    // from a refused batch that arrived anyway.
    const survivors = (
      await database.query("SELECT id FROM items WHERE id >= 6000 ORDER BY id", { memoize: false })
    ).rows as Array<{ id: number }>;
    const expected = Array.from({ length: 12 }, (_, index) => 6_000 + index).filter(
      (_, index) => index % 4 !== 1,
    );
    expect(survivors.map((row) => row.id)).toEqual(expected);
  });
});
