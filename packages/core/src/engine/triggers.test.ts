import {
  MemoryBlockStore,
  TableRecordConflictError,
  type BlockStore,
  type TableRecord,
  type TableRecordUpdate,
} from "../storage/index.js";
import { FaultInjectingBlockStore } from "../testing/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { column, schema, table } from "./schema.js";

/**
 * AFTER triggers: catalog-persisted, fired by the committing writer inside the same
 * transaction as the triggering write, so the write and its derivations publish atomically
 * and cross-tab visibility follows the catalog epoch like any other DDL.
 */

async function seeded(store: BlockStore = new MemoryBlockStore()): Promise<MinnowDatabase> {
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

/** Moves a trigger after DROP has read it but before its owner CAS reaches storage. */
class TriggerDropAbaStore extends MemoryBlockStore {
  #replacementOwnerId: string | undefined;

  armReplacement(ownerId: string): void {
    this.#replacementOwnerId = ownerId;
  }

  override async updateTable(
    id: string,
    expectedRevision: number,
    update: TableRecordUpdate,
  ): Promise<TableRecord> {
    const replacementOwnerId = this.#replacementOwnerId;
    const current = await super.getTable(id);
    const removed = current?.triggers?.find(
      (trigger) => !(update.triggers ?? []).some((candidate) => candidate.id === trigger.id),
    );
    if (replacementOwnerId === undefined || removed === undefined) {
      return super.updateTable(id, expectedRevision, update);
    }
    this.#replacementOwnerId = undefined;
    await super.updateTable(id, expectedRevision, {
      triggers: (current?.triggers ?? []).filter((trigger) => trigger.id !== removed.id),
    });
    const replacementOwner = await super.getTable(replacementOwnerId);
    if (replacementOwner === undefined) throw new Error("Replacement trigger owner disappeared");
    await super.updateTable(replacementOwner.id, replacementOwner.revision, {
      triggers: [
        ...(replacementOwner.triggers ?? []),
        {
          ...removed,
          id: "replacement-trigger-id",
          statements: [{ sql: "INSERT INTO accounts (id) VALUES (1)", bindings: [] }],
        },
      ],
    });
    return super.updateTable(id, expectedRevision, update);
  }
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

  it("supports BEFORE timing with identical atomicity", async () => {
    const database = await seeded();
    await database.execute(
      "CREATE TRIGGER before_audit BEFORE INSERT ON accounts FOR EACH ROW BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('before', NEW.id, NEW.balance); END",
    );
    const result = await database.insertBatch("accounts", {
      columns: { id: [1], balance: [10], owner: ["ada"] },
    });
    const audit = await database.query("SELECT action, amount FROM audit", {
      version: result.version,
    });
    expect(audit.rows).toEqual([{ action: "before", amount: 10 }]);
  });

  it("runs UPDATE and DELETE trigger bodies against keyed targets", async () => {
    const database = await seeded();
    await database.createTable({
      name: "owner_stats",
      uniqueKey: "owner",
      columns: [
        { name: "owner", type: "string" },
        { name: "accounts", type: "number" },
      ],
    });
    await database.insertBatch("owner_stats", {
      columns: { owner: ["ada", "grace"], accounts: [0, 0] },
    });
    await database.execute(
      "CREATE TRIGGER count_up AFTER INSERT ON accounts BEGIN " +
        "UPDATE owner_stats SET accounts = accounts + 1 WHERE owner = NEW.owner; END",
    );
    await database.insertBatch("accounts", {
      columns: { id: [1], balance: [10], owner: ["ada"] },
    });
    await database.insertBatch("accounts", {
      columns: { id: [2], balance: [20], owner: ["ada"] },
    });
    // Two separate commits compound: the body reads current state each firing.
    expect(
      (await database.query("SELECT owner, accounts FROM owner_stats ORDER BY owner")).rows,
    ).toEqual([
      { owner: "ada", accounts: 2 },
      { owner: "grace", accounts: 0 },
    ]);
    // DELETE bodies prune keyed targets.
    await database.execute(
      "CREATE TRIGGER drop_stats AFTER DELETE ON accounts BEGIN " +
        "DELETE FROM owner_stats WHERE owner = OLD.owner; END",
    );
    await database.deleteBatch("accounts", { keys: [1] });
    expect((await database.query("SELECT owner FROM owner_stats ORDER BY owner")).rows).toEqual([
      { owner: "grace" },
    ]);
    // The same target row touched twice in one firing cannot compound and is rejected.
    await expect(
      database.insertBatch("accounts", {
        columns: { id: [10, 11], balance: [1, 2], owner: ["grace", "grace"] },
      }),
    ).rejects.toThrow("touched the same owner_stats row twice");
    expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 1 }]);
  });

  it("trigger body updates compound across firings inside one write scope", async () => {
    const database = await seeded();
    await database.createTable({
      name: "owner_stats",
      uniqueKey: "owner",
      columns: [
        { name: "owner", type: "string" },
        { name: "accounts", type: "number" },
      ],
    });
    await database.insertBatch("owner_stats", { columns: { owner: ["ada"], accounts: [0] } });
    await database.execute(
      "CREATE TRIGGER count_up AFTER INSERT ON accounts BEGIN " +
        "UPDATE owner_stats SET accounts = accounts + 1 WHERE owner = NEW.owner; END",
    );
    // Two firings in one scope: the second body read must see the first's staged update.
    await database.write(async (tx) => {
      await tx.insertBatch("accounts", { columns: { id: [1], balance: [10], owner: ["ada"] } });
      await tx.insertBatch("accounts", { columns: { id: [2], balance: [20], owner: ["ada"] } });
    });
    expect((await database.query("SELECT accounts FROM owner_stats")).rows).toEqual([
      { accounts: 2 },
    ]);
  });

  it("trigger body updates compound with the scope's own staged update", async () => {
    const database = await seeded();
    await database.insertBatch("accounts", {
      columns: { id: [1], balance: [110], owner: ["ada"] },
    });
    await database.createTable({
      name: "events",
      columns: [{ name: "amount", type: "number" }],
    });
    await database.execute(
      "CREATE TRIGGER apply AFTER INSERT ON events BEGIN " +
        "UPDATE accounts SET balance = balance + NEW.amount WHERE id = 1; END",
    );
    await database.write(async (tx) => {
      await tx.updateBatch("accounts", { keys: [1], changes: { balance: [500] } });
      await tx.insertBatch("events", { columns: { amount: [10] } });
    });
    // The body reads the staged 500, not the committed 110.
    expect((await database.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
      { balance: 510 },
    ]);
  });

  it("trigger bodies match rows the scope staged earlier", async () => {
    const database = await seeded();
    await database.createTable({
      name: "events",
      columns: [{ name: "amount", type: "number" }],
    });
    await database.execute(
      "CREATE TRIGGER apply AFTER INSERT ON events BEGIN " +
        "UPDATE accounts SET balance = balance + NEW.amount WHERE id = 7; END",
    );
    // The target row exists only in the scope's staged state.
    await database.write(async (tx) => {
      await tx.insertBatch("accounts", { columns: { id: [7], balance: [0], owner: ["ada"] } });
      await tx.insertBatch("events", { columns: { amount: [25] } });
    });
    expect((await database.query("SELECT balance FROM accounts WHERE id = 7")).rows).toEqual([
      { balance: 25 },
    ]);
  });

  it("fires DELETE triggers only for rows that actually existed", async () => {
    const database = await seeded();
    await database.insertBatch("accounts", {
      columns: { id: [1], balance: [100], owner: ["ada"] },
    });
    await database.execute(
      "CREATE TRIGGER account_delete_audit AFTER DELETE ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('delete', OLD.id, OLD.balance); END",
    );
    const result = await database.deleteBatch("accounts", { keys: [1, 2, 3] });
    expect(result.deletedRowCount).toBe(1);
    // One audit row for the one real deletion — no phantom all-null OLD images for 2 and 3.
    expect((await database.query("SELECT account_id, amount FROM audit")).rows).toEqual([
      { account_id: 1, amount: 100 },
    ]);
  });

  it("upserts fire INSERT triggers for fresh keys and UPDATE triggers for replaced rows", async () => {
    const database = await seeded();
    await database.insertBatch("accounts", {
      columns: { id: [1], balance: [100], owner: ["ada"] },
    });
    await database.execute(
      "CREATE TRIGGER ins_audit AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('ins', NEW.id, NEW.balance); END",
    );
    await database.execute(
      "CREATE TRIGGER upd_audit AFTER UPDATE ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('upd-old', OLD.id, OLD.balance); " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('upd-new', NEW.id, NEW.balance); END",
    );
    await database.upsertBatch("accounts", {
      columns: { id: [1, 2], balance: [150, 50], owner: ["ada", "bo"] },
    });
    expect(
      (await database.query("SELECT action, account_id, amount FROM audit ORDER BY action")).rows,
    ).toEqual([
      { action: "ins", account_id: 2, amount: 50 },
      { action: "upd-new", account_id: 1, amount: 150 },
      { action: "upd-old", account_id: 1, amount: 100 },
    ]);
    // The SQL upsert routes identically whether or not the SET lists every column.
    await database.execute(
      "INSERT INTO accounts (id, balance, owner) VALUES (2, 75, 'bo') " +
        "ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance, owner = EXCLUDED.owner",
    );
    expect(
      (await database.query("SELECT amount FROM audit WHERE action = 'upd-new' AND account_id = 2"))
        .rows,
    ).toEqual([{ amount: 75 }]);
  });

  it("re-runs trigger bodies from fresh state when the commit hits a conflict", async () => {
    const inner = new MemoryBlockStore();
    let fired = false;
    const interloper: { run?: () => Promise<void> } = {};
    const store = new FaultInjectingBlockStore(inner, async (point) => {
      if (point === "beforeTransactionCommit" && !fired && interloper.run !== undefined) {
        fired = true;
        await interloper.run();
      }
    });
    const database = await seeded(store);
    const other = new MinnowDatabase(inner, { rowsPerBlock: 8, compression: "raw" });
    await database.createTable({
      name: "owner_stats",
      uniqueKey: "owner",
      columns: [
        { name: "owner", type: "string" },
        { name: "accounts", type: "number" },
      ],
    });
    await database.insertBatch("owner_stats", { columns: { owner: ["ada"], accounts: [0] } });
    await database.execute(
      "CREATE TRIGGER count_up AFTER INSERT ON accounts BEGIN " +
        "UPDATE owner_stats SET accounts = accounts + 1 WHERE owner = NEW.owner; END",
    );
    // A competing commit (which itself fires the trigger) lands between this write's staging
    // and its commit: the conflicted attempt must re-run the body, not republish accounts=1.
    interloper.run = async () => {
      await other.insertBatch("accounts", { columns: { id: [2], balance: [1], owner: ["ada"] } });
    };
    await database.insertBatch("accounts", {
      columns: { id: [1], balance: [1], owner: ["ada"] },
    });
    expect((await database.query("SELECT accounts FROM owner_stats")).rows).toEqual([
      { accounts: 2 },
    ]);
  });

  it("pads unlisted nullable body-INSERT columns and rejects impossible bodies at CREATE", async () => {
    const database = await seeded();
    // audit(action, account_id nullable, amount nullable): listing only action is legal.
    await database.execute(
      "CREATE TRIGGER thin_audit AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action) VALUES ('touched'); END",
    );
    await database.insertBatch("accounts", {
      columns: { id: [1], balance: [10], owner: ["ada"] },
    });
    expect((await database.query("SELECT action, account_id FROM audit")).rows).toEqual([
      { action: "touched", account_id: null },
    ]);
    // A body omitting a non-nullable column without a default can never fire: CREATE fails.
    await database.createTable({
      name: "strict",
      columns: [
        { name: "a", type: "string" },
        { name: "b", type: "number" },
      ],
    });
    await expect(
      database.execute(
        "CREATE TRIGGER impossible AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO strict (a) VALUES ('x'); END",
      ),
    ).rejects.toThrow("omits a non-nullable column");
    await expect(
      database.execute(
        "CREATE TRIGGER unknown_column AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (nope) VALUES ('x'); END",
      ),
    ).rejects.toThrow("column does not exist");
  });

  it("refuses later schema changes that would invalidate stored INSERT bodies", async () => {
    const defaultDatabase = new MinnowDatabase(new MemoryBlockStore());
    await defaultDatabase.migrate(
      schema([
        table("source", { value: column.string() }),
        table("sink", {
          value: column.string(),
          required: column.string().default("generated"),
        }),
      ]),
    );
    await defaultDatabase.execute(
      "CREATE TRIGGER preserve_default AFTER INSERT ON source BEGIN " +
        "INSERT INTO sink (value, required) VALUES (NEW.value, DEFAULT); END",
    );
    await expect(
      defaultDatabase.migrate(
        schema([
          table("source", { value: column.string() }),
          table("sink", { value: column.string(), required: column.string() }),
        ]),
      ),
    ).rejects.toThrow("trigger preserve_default would become invalid");
    expect(
      (await defaultDatabase.introspect()).tables
        .find((candidate) => candidate.name === "sink")
        ?.columns.find((candidate) => candidate.name === "required")?.defaultValue,
    ).toBeDefined();

    const requiredDatabase = new MinnowDatabase(new MemoryBlockStore());
    await requiredDatabase.migrate(
      schema([
        table("source", { value: column.string() }),
        table("sink", { value: column.string() }),
      ]),
    );
    await requiredDatabase.execute(
      "CREATE TRIGGER preserve_required_shape AFTER INSERT ON source BEGIN " +
        "INSERT INTO sink (value) VALUES (NEW.value); END",
    );
    await expect(
      requiredDatabase.migrate(
        schema([
          table("source", { value: column.string() }),
          table("sink", {
            value: column.string(),
            required: column.string().backfill("legacy"),
          }),
        ]),
      ),
    ).rejects.toThrow("trigger preserve_required_shape would become invalid");
    expect(
      (await requiredDatabase.introspect()).tables.find((candidate) => candidate.name === "sink")
        ?.columns,
    ).toHaveLength(1);

    const implicitDatabase = new MinnowDatabase(new MemoryBlockStore());
    await implicitDatabase.migrate(
      schema([
        table("source", { value: column.string() }),
        table("sink", { value: column.string() }),
      ]),
    );
    await implicitDatabase.execute(
      "CREATE TRIGGER preserve_implicit_arity AFTER INSERT ON source BEGIN " +
        "INSERT INTO sink VALUES (NEW.value); END",
    );
    await expect(
      implicitDatabase.migrate(
        schema([
          table("source", { value: column.string() }),
          table("sink", { value: column.string(), extra: column.string().nullable() }),
        ]),
      ),
    ).rejects.toThrow("trigger preserve_implicit_arity would become invalid");
  });

  it("rejects renaming a column a trigger references", async () => {
    const database = await seeded();
    await database.execute(
      "CREATE TRIGGER account_insert_audit AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('insert', NEW.id, NEW.balance); END",
    );
    // NEW.balance binds the trigger's own table column; renaming it would bind NULL forever.
    await expect(
      database.migrate(
        schema([
          table("accounts", {
            id: column.number().unique(),
            cash: column.number().renamedFrom("balance"),
            owner: column.string(),
          }),
        ]),
      ),
    ).rejects.toThrow("trigger account_insert_audit references it");
    // The body's target column is protected too: renaming audit.amount would fail every firing.
    await expect(
      database.migrate(
        schema([
          table("audit", {
            action: column.string(),
            account_id: column.number().nullable(),
            value: column.number().nullable().renamedFrom("amount"),
          }),
        ]),
      ),
    ).rejects.toThrow("trigger account_insert_audit references it");
  });

  it("keeps trigger names unique when two instances create the same name at once", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const other = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await database.createTable({
      name: "mirror",
      columns: [{ name: "action", type: "string" }],
    });
    // Same trigger name, different tables: the pre-check can't see the other in flight, so
    // exactly one must survive the post-write settle and the other must report the conflict.
    const outcomes = await Promise.allSettled([
      database.execute(
        "CREATE TRIGGER dup AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (action, account_id, amount) VALUES ('a', NEW.id, NEW.balance); END",
      ),
      other.execute(
        "CREATE TRIGGER dup AFTER INSERT ON audit BEGIN " +
          "INSERT INTO mirror (action) VALUES (NEW.action); END",
      ),
    ]);
    expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
    const named = (await store.listTables()).flatMap((record) =>
      (record.triggers ?? []).filter((trigger) => trigger.name === "dup"),
    );
    expect(named).toHaveLength(1);
    expect(named[0]?.id).toMatch(/\S/);
    // The survivor is intact and droppable by name.
    await database.execute("DROP TRIGGER dup");
    expect(
      (await store.listTables()).flatMap((record) =>
        (record.triggers ?? []).filter((trigger) => trigger.name === "dup"),
      ),
    ).toHaveLength(0);
  });

  it("never drops a replacement that reused the name during a DROP ABA race", async () => {
    const store = new TriggerDropAbaStore();
    const database = await seeded(store);
    await database.execute(
      "CREATE TRIGGER aba AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('a', NEW.id, NEW.balance); END",
    );
    const original = (await store.listTables())
      .flatMap((record) => record.triggers ?? [])
      .find((trigger) => trigger.name === "aba");
    const replacementOwner = (await store.listTables()).find((record) => record.name === "audit");
    if (original === undefined || replacementOwner === undefined) {
      throw new Error("Trigger race fixture was not installed");
    }
    store.armReplacement(replacementOwner.id);

    await expect(database.execute("DROP TRIGGER aba")).rejects.toBeInstanceOf(
      TableRecordConflictError,
    );
    const survivors = (await store.listTables()).flatMap((record) =>
      (record.triggers ?? []).map((trigger) => ({ tableId: record.id, trigger })),
    );
    expect(survivors).toHaveLength(1);
    expect(survivors[0]).toMatchObject({
      tableId: replacementOwner.id,
      trigger: { id: "replacement-trigger-id", name: "aba" },
    });
    expect(survivors[0]?.trigger.id).not.toBe(original.id);

    // A new DROP invocation pins the replacement's identity and may remove it normally.
    await database.execute("DROP TRIGGER aba");
    expect((await store.listTables()).flatMap((record) => record.triggers ?? [])).toHaveLength(0);
  });

  it("allows one cascade level and errors loudly on deeper chains", async () => {
    const database = await seeded();
    await database.createTable({
      name: "mirror",
      columns: [{ name: "action", type: "string" }],
    });
    await database.execute(
      "CREATE TRIGGER audit_mirror AFTER INSERT ON audit BEGIN " +
        "INSERT INTO mirror (action) VALUES (NEW.action); END",
    );
    await database.execute(
      "CREATE TRIGGER account_audit AFTER INSERT ON accounts BEGIN " +
        "INSERT INTO audit (action, account_id, amount) VALUES ('insert', NEW.id, NEW.balance); END",
    );
    const result = await database.insertBatch("accounts", {
      columns: { id: [1], balance: [10], owner: ["ada"] },
    });
    // Primary -> audit (trigger) -> mirror (one cascade level), all in one commit.
    expect(
      (await database.query("SELECT action FROM mirror", { version: result.version })).rows,
    ).toEqual([{ action: "insert" }]);
    // A third level errors at write time and the whole commit vanishes.
    await database.createTable({
      name: "deep",
      columns: [{ name: "action", type: "string" }],
    });
    await database.execute(
      "CREATE TRIGGER mirror_deep AFTER INSERT ON mirror BEGIN " +
        "INSERT INTO deep (action) VALUES (NEW.action); END",
    );
    await expect(
      database.insertBatch("accounts", {
        columns: { id: [2], balance: [20], owner: ["grace"] },
      }),
    ).rejects.toThrow("Trigger cascade depth exceeded");
    expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 1 }]);
    expect((await database.query("SELECT COUNT(*) AS n FROM mirror")).rows).toEqual([{ n: 1 }]);
  });

  it("rejects invalid trigger definitions at CREATE time", async () => {
    const database = await seeded();
    await expect(
      database.execute(
        "CREATE TRIGGER bad AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (action) VALUES (OLD.owner); END",
      ),
    ).rejects.toThrow("INSERT triggers have no OLD row");
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
    await expect(
      database.execute(
        "CREATE TRIGGER bad_assignment AFTER INSERT ON audit BEGIN " +
          "UPDATE accounts SET missing = 1 WHERE id = 1; END",
      ),
    ).rejects.toThrow("assignment column does not exist: missing");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_expression AFTER INSERT ON audit BEGIN " +
          "UPDATE accounts SET balance = missing + 1 WHERE id = 1; END",
      ),
    ).rejects.toThrow("UPDATE column does not exist: missing");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_key_update AFTER INSERT ON audit BEGIN " +
          "UPDATE accounts SET id = 2 WHERE id = 1; END",
      ),
    ).rejects.toThrow("cannot update a key column: id");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_update_predicate AFTER INSERT ON audit BEGIN " +
          "UPDATE accounts SET balance = 1 WHERE abs(missing) = 1; END",
      ),
    ).rejects.toThrow("UPDATE column does not exist: missing");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_delete_predicate AFTER INSERT ON audit BEGIN " +
          "DELETE FROM accounts WHERE abs(missing) = 1; END",
      ),
    ).rejects.toThrow("DELETE column does not exist: missing");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_qualifier AFTER INSERT ON audit BEGIN " +
          "DELETE FROM accounts WHERE audit.action = 'x'; END",
      ),
    ).rejects.toThrow("DELETE column does not exist: audit.action");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_arity AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit VALUES ('only one'); END",
      ),
    ).rejects.toThrow("1 values for 3 target columns");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_default_values AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit DEFAULT VALUES; END",
      ),
    ).rejects.toThrow("omits a non-nullable column");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_explicit_default AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (action) VALUES (DEFAULT); END",
      ),
    ).rejects.toThrow("uses DEFAULT for a non-nullable column");
    await expect(
      database.execute(
        "CREATE TRIGGER bad_returning AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (action) VALUES ('x') RETURNING action; END",
      ),
    ).rejects.toThrow("cannot carry RETURNING");
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
        "CREATE TRIGGER audit_mirror AFTER INSERT ON mirror BEGIN " +
          "INSERT INTO audit (action) VALUES ('x'); END",
      ),
    ).rejects.toThrow("Trigger already exists");
    // Body UPDATE/DELETE statements need keyed targets; body INSERTs need keyless ones.
    await expect(
      database.execute(
        "CREATE TRIGGER bad AFTER INSERT ON accounts BEGIN " +
          "UPDATE audit SET action = 'x' WHERE action = 'y'; END",
      ),
    ).rejects.toThrow("need a keyed table");
  });
});
