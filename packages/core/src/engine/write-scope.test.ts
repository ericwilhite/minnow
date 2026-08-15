import { IDBFactory } from "fake-indexeddb";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";

/**
 * write(): every mutation in the scope publishes as one atomic commit — all of it or none
 * of it, in every tab — across keyed and keyless tables, with AFTER triggers firing per
 * staged operation exactly as they do for standalone writes.
 */

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

async function bank(store: BlockStore): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
  await database.createTable({
    name: "accounts",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "balance", type: "number" },
    ],
  });
  await database.insertBatch("accounts", { columns: { id: [1, 2], balance: [500, 100] } });
  return database;
}

describe("atomic write scopes", () => {
  for (const implementation of implementations) {
    it(`${implementation.name} publishes a multi-table transfer as one commit`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.createTable({
        name: "transfers",
        columns: [
          { name: "from_id", type: "number" },
          { name: "to_id", type: "number" },
          { name: "amount", type: "number" },
        ],
      });
      const before = await database.query("SELECT id, balance FROM accounts ORDER BY id");
      const { version } = await database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [400] } });
        await tx.updateBatch("accounts", { keys: [2], changes: { balance: [200] } });
        await tx.insertBatch("transfers", {
          columns: { from_id: [1], to_id: [2], amount: [100] },
        });
      });
      expect(version).not.toBeNull();
      // After: everything moved together.
      expect((await database.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
        { id: 1, balance: 400 },
        { id: 2, balance: 200 },
      ]);
      expect((await database.query("SELECT COUNT(*) AS n FROM transfers")).rows).toEqual([
        { n: 1 },
      ]);
      // One version earlier: none of it happened.
      const previous = (version ?? 1) - 1;
      expect(
        (
          await database.query("SELECT id, balance FROM accounts ORDER BY id", {
            version: previous,
          })
        ).rows,
      ).toEqual(before.rows);
      expect(
        (await database.query("SELECT COUNT(*) AS n FROM transfers", { version: previous })).rows,
      ).toEqual([{ n: 0 }]);
    });

    it(`${implementation.name} publishes nothing when the scope fails mid-way`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await expect(
        database.write(async (tx) => {
          await tx.updateBatch("accounts", { keys: [1], changes: { balance: [400] } });
          // The second update targets a missing key: the whole scope must vanish.
          await tx.updateBatch("accounts", { keys: [999], changes: { balance: [1] } });
        }),
      ).rejects.toThrow();
      expect((await database.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
        { id: 1, balance: 500 },
        { id: 2, balance: 100 },
      ]);
      // An error thrown by the callback itself also aborts everything staged so far.
      await expect(
        database.write(async (tx) => {
          await tx.deleteBatch("accounts", { keys: [2] });
          throw new Error("caller changed its mind");
        }),
      ).rejects.toThrow("caller changed its mind");
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 2 }]);
    });

    it(`${implementation.name} enforces unique keys across and inside scopes`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      // In-scope conflict: inserting one key twice in the same scope.
      await expect(
        database.write(async (tx) => {
          await tx.insertBatch("accounts", { columns: { id: [7], balance: [1] } });
          await tx.insertBatch("accounts", { columns: { id: [7], balance: [2] } });
        }),
      ).rejects.toThrow();
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 2 }]);
      // Insert + delete of independent keys across two keyed tables in one commit.
      await database.createTable({
        name: "labels",
        uniqueKey: "name",
        columns: [
          { name: "name", type: "string" },
          { name: "note", type: "string" },
        ],
      });
      await database.insertBatch("labels", { columns: { name: ["old"], note: ["x"] } });
      const { version } = await database.write(async (tx) => {
        await tx.insertBatch("accounts", { columns: { id: [3], balance: [50] } });
        await tx.insertBatch("labels", { columns: { name: ["fresh"], note: ["y"] } });
        await tx.deleteBatch("labels", { keys: ["old"] });
      });
      expect(version).not.toBeNull();
      expect((await database.query("SELECT name FROM labels")).rows).toEqual([{ name: "fresh" }]);
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
      // Key membership survived the multi-entry commit: re-inserting fresh conflicts,
      // re-inserting old succeeds.
      await expect(
        database.insertBatch("labels", { columns: { name: ["fresh"], note: ["z"] } }),
      ).rejects.toThrow();
      await database.insertBatch("labels", { columns: { name: ["old"], note: ["z"] } });
    });

    it(`${implementation.name} fires AFTER triggers inside the scope's single commit`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.createTable({
        name: "audit",
        columns: [
          { name: "account_id", type: "number" },
          { name: "balance", type: "number" },
        ],
      });
      await database.execute(
        "CREATE TRIGGER account_audit AFTER UPDATE ON accounts BEGIN " +
          "INSERT INTO audit (account_id, balance) VALUES (NEW.id, NEW.balance); END",
      );
      const { version } = await database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [450] } });
        await tx.updateBatch("accounts", { keys: [2], changes: { balance: [150] } });
      });
      const audit = await database.query(
        "SELECT account_id, balance FROM audit ORDER BY account_id",
      );
      expect(audit.rows).toEqual([
        { account_id: 1, balance: 450 },
        { account_id: 2, balance: 150 },
      ]);
      // The audit rows share the scope's version: nothing at version - 1.
      expect(
        (
          await database.query("SELECT COUNT(*) AS n FROM audit", {
            version: (version ?? 1) - 1,
          })
        ).rows,
      ).toEqual([{ n: 0 }]);
    });
  }

  for (const implementation of implementations) {
    it(`${implementation.name} reads its own staged writes inside the scope`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.write(async (tx) => {
        // Outside the scope: committed state only. Inside: staged rows overlay it.
        await tx.insertBatch("accounts", { columns: { id: [3], balance: [50] } });
        expect((await tx.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
        expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([
          { n: 2 },
        ]);
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [400] } });
        expect((await tx.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
          { balance: 400 },
        ]);
        await tx.deleteBatch("accounts", { keys: [2] });
        expect((await tx.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
          { id: 1, balance: 400 },
          { id: 3, balance: 50 },
        ]);
        // Aggregates and params run through the same doctored read.
        expect(
          (
            await tx.query("SELECT SUM(balance) AS total FROM accounts WHERE balance >= ?", {
              params: [50],
            })
          ).rows,
        ).toEqual([{ total: 450 }]);
      });
      // After commit, the outside view converges on what the scope saw.
      expect((await database.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
        { id: 1, balance: 400 },
        { id: 3, balance: 50 },
      ]);
    });

    it(`${implementation.name} updates and deletes rows staged in the same scope`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.write(async (tx) => {
        await tx.insertBatch("accounts", { columns: { id: [10], balance: [5] } });
        // The staged key is updatable in-scope even though it is not committed yet.
        await tx.updateBatch("accounts", { keys: [10], changes: { balance: [7] } });
        expect((await tx.query("SELECT balance FROM accounts WHERE id = 10")).rows).toEqual([
          { balance: 7 },
        ]);
        // A key deleted in-scope is gone for later statements in the same scope.
        await tx.deleteBatch("accounts", { keys: [10] });
        await expect(
          tx.updateBatch("accounts", { keys: [10], changes: { balance: [9] } }),
        ).rejects.toThrow();
      });
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 2 }]);
    });
  }

  it("gives trigger pre-images the scope's view of staged rows", async () => {
    const database = await bank(new MemoryBlockStore());
    await database.createTable({
      name: "audit",
      columns: [
        { name: "account_id", type: "number" },
        { name: "old_balance", type: "number" },
      ],
    });
    await database.execute(
      "CREATE TRIGGER account_update_audit AFTER UPDATE ON accounts BEGIN " +
        "INSERT INTO audit (account_id, old_balance) VALUES (OLD.id, OLD.balance); END",
    );
    await database.write(async (tx) => {
      await tx.insertBatch("accounts", { columns: { id: [30], balance: [11] } });
      // OLD for the staged row must read the staged value, not "missing".
      await tx.updateBatch("accounts", { keys: [30], changes: { balance: [12] } });
    });
    expect((await database.query("SELECT account_id, old_balance FROM audit")).rows).toEqual([
      { account_id: 30, old_balance: 11 },
    ]);
  });

  it("surfaces concurrent commits as explicit conflicts; a retry succeeds", async () => {
    const store = new MemoryBlockStore();
    const database = await bank(store);
    const other = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await expect(
      database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [401] } });
        // A commit from another instance lands while the scope is open.
        await other.insertBatch("accounts", { columns: { id: [50], balance: [1] } });
      }),
    ).rejects.toThrow();
    // The losing scope published nothing; the interloper's row is there.
    expect((await database.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
      { balance: 500 },
    ]);
    expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
    // Retrying the scope succeeds against the new state.
    const { version } = await database.write(async (tx) => {
      await tx.updateBatch("accounts", { keys: [1], changes: { balance: [401] } });
    });
    expect(version).not.toBeNull();
    expect((await database.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
      { balance: 401 },
    ]);
  });

  it("delivers one live notification and one fresh memo per scope", async () => {
    const database = await bank(new MemoryBlockStore());
    const live = database.liveQueries();
    const changes: unknown[] = [];
    await live.subscribe("SELECT SUM(balance) AS total FROM accounts", {
      onChange: (result) => changes.push(result.rows),
    });
    expect(changes).toEqual([[{ total: 600 }]]);
    const sql = "SELECT SUM(balance) AS total FROM accounts";
    expect((await database.query(sql)).rows).toEqual([{ total: 600 }]);
    await database.write(async (tx) => {
      await tx.updateBatch("accounts", { keys: [1], changes: { balance: [450] } });
      await tx.updateBatch("accounts", { keys: [2], changes: { balance: [151] } });
    });
    await live.refresh();
    // One scope, one re-run, one notification — never an intermediate state.
    expect(changes).toEqual([[{ total: 600 }], [{ total: 601 }]]);
    expect(live.stats.reruns).toBe(1);
    // The memo cannot serve the pre-scope answer: the scope moved the epoch.
    expect((await database.query(sql)).rows).toEqual([{ total: 601 }]);
    await database.write(async (tx) => {
      await tx.deleteBatch("accounts", { keys: [2] });
    });
    expect((await database.query(sql)).rows).toEqual([{ total: 450 }]);
    live.close();
  });

  it("stages nothing, publishes nothing", async () => {
    const database = await bank(new MemoryBlockStore());
    const versionBefore = (await database.query("SELECT COUNT(*) AS n FROM accounts")).rows;
    const { result, version } = await database.write(async () => "noop");
    expect(result).toBe("noop");
    expect(version).toBe(0);
    expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual(
      versionBefore,
    );
  });

  it("rejects session use after the scope ends", async () => {
    const database = await bank(new MemoryBlockStore());
    let leaked: Parameters<Parameters<MinnowDatabase["write"]>[0]>[0] | undefined;
    await database.write(async (tx) => {
      leaked = tx;
    });
    await expect(
      leaked?.insertBatch("accounts", { columns: { id: [9], balance: [1] } }),
    ).rejects.toThrow("The write scope has ended");
  });
});
