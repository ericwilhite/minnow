import { MemoryBlockStore } from "../storage/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";

/**
 * AFTER triggers: catalog-persisted, fired by the committing writer inside the same
 * transaction as the triggering write, so the write and its derivations publish atomically
 * and cross-tab visibility follows the catalog epoch like any other DDL.
 */

async function seeded(store = new MemoryBlockStore()): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
  await database.createTable({
    name: "accounts",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "balance", type: "number" },
      { name: "owner", type: "string" },
    ],
  });
  await database.createTable({
    name: "audit",
    columns: [
      { name: "action", type: "string" },
      { name: "account_id", type: "number", nullable: true },
      { name: "amount", type: "number", nullable: true },
    ],
  });
  return database;
}

describe("AFTER triggers", () => {
  it("fires AFTER INSERT atomically with the triggering write", async () => {
    const database = await seeded();
    // An earlier commit pins version 0, so "one version before the trigger fired" exists.
    await database.insertBatch("accounts", {
      columns: { id: [99], balance: [0], owner: ["seed"] },
    });
    await database.execute(
      "CREATE TRIGGER account_insert_audit AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('insert', NEW.id, NEW.balance); END",
    );
    const result = await database.insertBatch("accounts", {
      columns: { id: [1, 2], balance: [100, 250], owner: ["ada", "grace"] },
    });
    const audit = await database.query(
      "SELECT action, account_id, amount FROM audit ORDER BY account_id",
    );
    expect(audit.rows).toEqual([
      { action: "insert", account_id: 1, amount: 100 },
      { action: "insert", account_id: 2, amount: 250 },
    ]);
    // Atomicity: at the insert's version both are visible; one version earlier, neither is.
    const atCommit = await database.query("SELECT COUNT(*) AS n FROM audit", {
      version: result.version,
    });
    expect(atCommit.rows).toEqual([{ n: 2 }]);
    const before = await database.query("SELECT COUNT(*) AS n FROM audit", {
      version: result.version - 1,
    });
    expect(before.rows).toEqual([{ n: 0 }]);
  });

  it("fires AFTER UPDATE with OLD and NEW values and AFTER DELETE with OLD", async () => {
    const database = await seeded();
    await database.insertBatch("accounts", {
      columns: { id: [1], balance: [100], owner: ["ada"] },
    });
    await database.execute(
      "CREATE TRIGGER account_update_audit AFTER UPDATE ON accounts FOR EACH ROW BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('old', OLD.id, OLD.balance); " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('new', NEW.id, NEW.balance); END",
    );
    await database.execute(
      "CREATE TRIGGER account_delete_audit AFTER DELETE ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('delete', OLD.id, OLD.balance); END",
    );
    await database.updateBatch("accounts", { keys: [1], changes: { balance: [175] } });
    // NEW carries the changed value, OLD the pre-image; unchanged columns read from OLD.
    expect((await database.query("SELECT action, amount FROM audit ORDER BY action")).rows).toEqual(
      [
        { action: "new", amount: 175 },
        { action: "old", amount: 100 },
      ],
    );
    await database.deleteBatch("accounts", { keys: [1] });
    expect((await database.query("SELECT amount FROM audit WHERE action = 'delete'")).rows).toEqual(
      [{ amount: 175 }],
    );
  });

  it("keeps the primary write out when a trigger body fails", async () => {
    const database = await seeded();
    // The body inserts a string into a number column: firing must fail the whole write.
    await database.execute(
      "CREATE TRIGGER bad_audit AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('insert', NEW.owner, NEW.balance); END",
    );
    await expect(
      database.insertBatch("accounts", {
        columns: { id: [7], balance: [10], owner: ["x"] },
      }),
    ).rejects.toThrow();
    expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 0 }]);
    expect((await database.query("SELECT COUNT(*) AS n FROM audit")).rows).toEqual([{ n: 0 }]);
  });

  it("stops firing after DROP TRIGGER and is visible across instances via the epoch", async () => {
    const store = new MemoryBlockStore();
    const writer = await seeded(store);
    const other = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await writer.execute(
      "CREATE TRIGGER account_insert_audit AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('insert', NEW.id, NEW.balance); END",
    );
    // A different instance sharing the store fires the trigger on its very next write.
    await other.insertBatch("accounts", {
      columns: { id: [1], balance: [5], owner: ["ada"] },
    });
    expect((await writer.query("SELECT COUNT(*) AS n FROM audit")).rows).toEqual([{ n: 1 }]);
    await writer.execute("DROP TRIGGER account_insert_audit");
    await other.insertBatch("accounts", {
      columns: { id: [2], balance: [6], owner: ["grace"] },
    });
    expect((await writer.query("SELECT COUNT(*) AS n FROM audit")).rows).toEqual([{ n: 1 }]);
    await expect(writer.execute("DROP TRIGGER account_insert_audit")).rejects.toThrow(
      "Trigger not found",
    );
  });

  it("rejects invalid trigger definitions at CREATE time", async () => {
    const database = await seeded();
    await expect(
      database.execute(
        "CREATE TRIGGER bad AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (action) VALUES (OLD.owner); END",
      ),
    ).rejects.toThrow("AFTER INSERT triggers have no OLD row");
    await expect(
      database.execute(
        "CREATE TRIGGER bad AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (action) VALUES (NEW.missing); END",
      ),
    ).rejects.toThrow("Unknown trigger column: missing");
    // Keyed body targets are rejected: the commit's unique-key channel belongs to the
    // primary write.
    await expect(
      database.execute(
        "CREATE TRIGGER bad AFTER INSERT ON audit BEGIN " +
          "INSERT INTO accounts (id, balance, owner) VALUES (1, 2, 'x'); END",
      ),
    ).rejects.toThrow("keyless tables only");
    // Cascades are rejected, not silently skipped.
    await database.createTable({
      name: "mirror",
      columns: [{ name: "action", type: "string" }],
    });
    await database.execute(
      "CREATE TRIGGER audit_mirror AFTER INSERT ON audit BEGIN " +
        "INSERT INTO mirror (action) VALUES (NEW.action); END",
    );
    await expect(
      database.execute(
        "CREATE TRIGGER cascade_attempt AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (action) VALUES ('x'); END",
      ),
    ).rejects.toThrow("Trigger cascades are not supported");
    await expect(
      database.execute(
        "CREATE TRIGGER audit_mirror AFTER INSERT ON mirror BEGIN " +
          "INSERT INTO audit (action) VALUES ('x'); END",
      ),
    ).rejects.toThrow("Trigger already exists");
    await expect(
      database.execute("CREATE TRIGGER b BEFORE INSERT ON accounts BEGIN INSERT INTO x; END"),
    ).rejects.toThrow("AFTER triggers only");
  });
});
