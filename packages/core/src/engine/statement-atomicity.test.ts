import { describe, expect, it } from "vitest";
import {
  MemoryBlockStore,
  SchemaConflictError,
  type CommitTransactionInput,
  type ManifestSummary,
  type SqlDomain,
  type StageTransactionArtifactsInput,
  type WriteTransactionInput,
} from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { compileStatement } from "./query.js";

class CommitRaceStore extends MemoryBlockStore {
  beforeCommit: (() => Promise<void>) | undefined;
  beforeWrite: (() => Promise<void>) | undefined;
  commitAttempts = 0;

  override async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    this.commitAttempts += 1;
    const race = this.beforeCommit;
    this.beforeCommit = undefined;
    if (race !== undefined) await race();
    return super.commitTransaction(input);
  }

  override async writeTransaction(input: WriteTransactionInput): Promise<ManifestSummary> {
    const race = this.beforeWrite;
    this.beforeWrite = undefined;
    if (race !== undefined) await race();
    return super.writeTransaction(input);
  }
}

class CtasObservingStore extends MemoryBlockStore {
  artifactStages = 0;
  failCommit = false;

  override async stageTransactionArtifacts(input: StageTransactionArtifactsInput) {
    this.artifactStages += 1;
    return super.stageTransactionArtifacts(input);
  }

  override async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    if (this.failCommit) {
      this.failCommit = false;
      throw new DOMException("injected CTAS commit failure", "QuotaExceededError");
    }
    return super.commitTransaction(input);
  }
}

function database(store: MemoryBlockStore): MinnowDatabase {
  return new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
}

describe("SQL statement atomicity", () => {
  it("rejects malformed table constraints before admitting any partial catalog object", async () => {
    const store = new MemoryBlockStore();
    const db = database(store);
    await db.createTable({
      name: "unkeyed_parent",
      columns: [{ name: "id", type: "number" }],
    });
    await db.createTable({
      name: "composite_parent",
      compositePrimaryKey: ["tenant", "id"],
      columns: [
        { name: "tenant", type: "number", integer: true },
        { name: "id", type: "number", integer: true },
      ],
    });
    await db.createTable({
      name: "integer_parent",
      uniqueKey: "id",
      columns: [{ name: "id", type: "number", integer: true }],
    });

    const invalid = [
      db.createTable({
        name: "unsupported_type",
        columns: [{ name: "value", type: "bytes" as "number" }],
      }),
      db.createTable({
        name: "two_primary_shapes",
        uniqueKey: "id",
        compositePrimaryKey: ["id", "tenant"],
        columns: [
          { name: "id", type: "number" },
          { name: "tenant", type: "number" },
        ],
      }),
      db.createTable({
        name: "missing_primary_column",
        compositePrimaryKey: ["id", "missing"],
        columns: [{ name: "id", type: "number" }],
      }),
      db.createTable({
        name: "duplicate_primary_column",
        compositePrimaryKey: ["id", "id"],
        columns: [{ name: "id", type: "number" }],
      }),
      db.createTable({
        name: "missing_unique_column",
        uniqueKey: "missing",
        columns: [{ name: "id", type: "number" }],
      }),
      db.createTable({
        name: "missing_child_column",
        columns: [{ name: "id", type: "number" }],
        foreignKeys: [
          {
            name: "missing_child_fk",
            column: "missing",
            parentTable: "integer_parent",
            parentColumn: "id",
            onDelete: "restrict",
          },
        ],
      }),
      db.createTable({
        name: "missing_parent",
        columns: [{ name: "id", type: "number" }],
        foreignKeys: [
          {
            name: "missing_parent_fk",
            column: "id",
            parentTable: "does_not_exist",
            parentColumn: "id",
            onDelete: "restrict",
          },
        ],
      }),
      db.createTable({
        name: "parent_without_key",
        columns: [{ name: "id", type: "number" }],
        foreignKeys: [
          {
            name: "unkeyed_parent_fk",
            column: "id",
            parentTable: "unkeyed_parent",
            parentColumn: "id",
            onDelete: "restrict",
          },
        ],
      }),
      db.createTable({
        name: "wrong_parent_key",
        columns: [
          { name: "tenant", type: "number", integer: true },
          { name: "id", type: "number", integer: true },
        ],
        foreignKeys: [
          {
            name: "wrong_parent_key_fk",
            column: "tenant",
            columns: ["tenant", "id"],
            parentTable: "composite_parent",
            parentColumn: "id",
            parentColumns: ["id", "tenant"],
            onDelete: "restrict",
          },
        ],
      }),
      db.createTable({
        name: "wrong_child_arity",
        columns: [{ name: "tenant", type: "number", integer: true }],
        foreignKeys: [
          {
            name: "wrong_child_arity_fk",
            column: "tenant",
            columns: ["tenant"],
            parentTable: "composite_parent",
            parentColumn: "tenant",
            parentColumns: ["tenant", "id"],
            onDelete: "restrict",
          },
        ],
      }),
      db.createTable({
        name: "wrong_child_type",
        columns: [{ name: "id", type: "string" }],
        foreignKeys: [
          {
            name: "wrong_child_type_fk",
            column: "id",
            parentTable: "integer_parent",
            parentColumn: "id",
            onDelete: "restrict",
          },
        ],
      }),
      db.createTable({
        name: "wrong_integer_domain",
        columns: [{ name: "id", type: "number" }],
        foreignKeys: [
          {
            name: "wrong_integer_domain_fk",
            column: "id",
            parentTable: "integer_parent",
            parentColumn: "id",
            onDelete: "restrict",
          },
        ],
      }),
      db.createTable({
        name: "invalid_set_null",
        columns: [{ name: "id", type: "number", integer: true }],
        foreignKeys: [
          {
            name: "invalid_set_null_fk",
            column: "id",
            parentTable: "integer_parent",
            parentColumn: "id",
            onDelete: "set null",
          },
        ],
      }),
      db.createTable({
        name: "unique_unknown",
        columns: [{ name: "id", type: "number" }],
        uniqueConstraints: [{ name: "unique_unknown_constraint", columns: ["missing"] }],
      }),
      db.createTable({
        name: "unique_empty",
        columns: [{ name: "id", type: "number" }],
        uniqueConstraints: [{ name: "unique_empty_constraint", columns: [] }],
      }),
      db.createTable({
        name: "unique_duplicate",
        columns: [{ name: "id", type: "number" }],
        uniqueConstraints: [{ name: "unique_duplicate_constraint", columns: ["id", "id"] }],
      }),
    ];

    const settled = await Promise.allSettled(invalid);
    expect(settled).toHaveLength(invalid.length);
    expect(settled.every((result) => result.status === "rejected")).toBe(true);
    expect((await db.listTables()).map(({ name }) => name).sort()).toEqual([
      "composite_parent",
      "integer_parent",
      "unkeyed_parent",
    ]);
    await db.close();
  });

  it("validates sequence rewrites and statement-only DDL failures without side effects", async () => {
    const store = new MemoryBlockStore();
    const db = database(store);
    await db.execute("CREATE TYPE mood AS ENUM ('calm', 'busy')");
    await db.execute("CREATE SEQUENCE serial_values");
    await expect(db.execute("CREATE TYPE mood AS ENUM ('other')")).rejects.toThrow(
      "Type already exists",
    );
    await expect(db.execute("CREATE SEQUENCE serial_values")).rejects.toThrow(
      "Sequence already exists",
    );

    const nested = await db.query(
      "SELECT NEXTVAL('serial_values') + 1 AS binary_value, " +
        "ABS(NEXTVAL('serial_values')) AS call_value, " +
        "CASE WHEN NEXTVAL('serial_values') > 0 THEN CURRVAL('serial_values') " +
        "ELSE NEXTVAL('serial_values') END AS case_value, " +
        "9 IN (NEXTVAL('serial_values'), 9) AS list_value, " +
        "NOT (NEXTVAL('serial_values') = 0) AS not_value",
    );
    expect(nested.rows).toEqual([
      expect.objectContaining({ list_value: true, not_value: true }) as unknown,
    ]);
    await expect(
      db.execute("SELECT NEXTVAL('serial_values') FROM (SELECT 1 AS n) source"),
    ).rejects.toThrow(/without FROM/);
    await expect(db.execute("SELECT NEXTVAL(unknown_name)")).rejects.toThrow(/constant sequence/);
    await expect(db.execute("SELECT NEXTVAL('missing_sequence')")).rejects.toThrow(
      /Sequence does not exist/,
    );
    expect(await db.execute("CREATE TABLE IF NOT EXISTS stable (id INTEGER PRIMARY KEY)")).toEqual({
      kind: "create-table",
      table: "stable",
    });
    expect(await db.execute("CREATE TABLE IF NOT EXISTS stable (id INTEGER PRIMARY KEY)")).toEqual({
      kind: "create-table",
      table: "stable",
    });
    await db.close();
  });

  it("covers defensive backfill, DDL, trigger, and mutation validation paths atomically", async () => {
    const store = new MemoryBlockStore();
    const db = new MinnowDatabase(store, {
      autoCollect: false,
      autoCompact: false,
      compression: "raw",
      rowsPerBlock: 3,
    });
    const colorDomain: SqlDomain = {
      kind: "enum",
      name: "color",
      values: ["red", "blue"],
    };
    await db.createTable({
      name: "domain_parent",
      uniqueKey: "color",
      columns: [
        { name: "color", type: "string", sqlDomain: colorDomain, backfill: "red" },
        { name: "seen_at", type: "datetime", backfill: new Date(0) },
        { name: "state", type: "string", enumValues: ["open", "closed"], backfill: "open" },
      ],
    });
    await expect(
      db.createTable({
        name: "null_domain_backfill",
        columns: [
          { name: "color", type: "string", sqlDomain: colorDomain, backfill: null as never },
        ],
      }),
    ).rejects.toThrow("Backfill cannot be NULL");
    await expect(
      db.createTable({
        name: "wrong_number_backfill",
        columns: [{ name: "value", type: "number", backfill: "wrong" }],
      }),
    ).rejects.toThrow("Backfill does not fit column");
    await expect(
      db.createTable({
        name: "wrong_enum_backfill",
        columns: [
          { name: "state", type: "string", enumValues: ["open", "closed"], backfill: "other" },
        ],
      }),
    ).rejects.toThrow("Backfill must be one of the enum values");
    await expect(
      db.createTable({
        name: "domain_child",
        columns: [
          {
            name: "color",
            type: "string",
            sqlDomain: { kind: "enum", name: "other_color", values: ["red", "blue"] },
          },
        ],
        foreignKeys: [
          {
            name: "domain_mismatch",
            column: "color",
            parentTable: "domain_parent",
            parentColumn: "color",
            onDelete: "restrict",
          },
        ],
      }),
    ).rejects.toThrow("compares different SQL value domains");

    await db.execute("CREATE TABLE drop_source (id INTEGER PRIMARY KEY, note TEXT)");
    await db.createView("drop_view", "SELECT id FROM drop_source");
    await expect(db.dropColumn("drop_view", "id")).rejects.toThrow("is a view, not a table");
    await expect(db.dropColumn("missing_table", "id", { ifExists: true })).resolves.toBe(false);
    await expect(db.dropColumn("missing_table", "id")).rejects.toThrow("Table not found");
    await expect(db.dropColumn("drop_source", "missing")).rejects.toThrow("Column not found");

    await db.createTable({
      name: "keyless_rows",
      columns: [{ name: "value", type: "number" }],
    });
    await expect(db.deleteBatch("keyless_rows", { keys: [1] })).rejects.toThrow(
      "needs a unique key",
    );
    await db.insert("domain_parent", { color: "red", seen_at: new Date(0), state: "open" });
    await expect(db.deleteBatch("domain_parent", { keys: ["red", "red"] })).rejects.toThrow(
      "Duplicate key in delete batch",
    );
    expect((await db.deleteBatch("domain_parent", { keys: ["blue"] })).deletedRowCount).toBe(0);

    await db.execute(
      "CREATE TABLE checked_rows (id INTEGER PRIMARY KEY, score INTEGER CHECK (score >= 0))",
    );
    await db.execute("INSERT INTO checked_rows VALUES (1, 10)");
    expect(
      (
        await db.updateBatch("checked_rows", {
          keys: [1],
          changes: { score: [20] },
        })
      ).updatedRowCount,
    ).toBe(1);
    expect((await db.query("SELECT score FROM checked_rows")).rows).toEqual([{ score: 20 }]);

    await db.execute("CREATE TABLE trigger_log (value INTEGER)");
    await expect(
      db.execute(
        "CREATE TRIGGER delete_new AFTER DELETE ON drop_source BEGIN " +
          "INSERT INTO trigger_log (value) VALUES (NEW.id); END",
      ),
    ).rejects.toThrow("DELETE triggers have no NEW row");
    await expect(
      db.execute("CREATE TRIGGER select_body AFTER INSERT ON drop_source BEGIN SELECT 1; END"),
    ).rejects.toThrow("Trigger bodies support INSERT, UPDATE, and DELETE");
    await expect(
      db.execute(
        "CREATE TRIGGER self_write AFTER INSERT ON drop_source BEGIN " +
          "INSERT INTO drop_source (id, note) VALUES (NEW.id, NEW.note); END",
      ),
    ).rejects.toThrow("cannot write to its own table");

    await expect(
      db.execute(
        "INSERT INTO checked_rows VALUES (1, 30) " +
          "ON CONFLICT (score) DO UPDATE SET score = EXCLUDED.score",
      ),
    ).rejects.toThrow("cannot reassign the conflict key");
    await expect(
      db.execute(
        "INSERT INTO checked_rows VALUES (1, 30) " +
          "ON CONFLICT (id) DO UPDATE SET score = stranger.score",
      ),
    ).rejects.toThrow("Unknown ON CONFLICT row alias");
    await expect(
      db.execute(
        "INSERT INTO checked_rows VALUES (1, 30) " +
          "ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.missing",
      ),
    ).rejects.toThrow("ON CONFLICT column does not exist");
    await expect(
      db.execute(
        "INSERT INTO checked_rows VALUES (1, 30) " +
          "ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score RETURNING missing",
      ),
    ).rejects.toThrow("RETURNING column does not exist");
    await expect(
      db.execute(
        "MERGE INTO keyless_rows target USING (SELECT 1 AS value) source " +
          "ON target.value = source.value WHEN MATCHED THEN DELETE",
      ),
    ).rejects.toThrow("MERGE requires a table with a unique key");

    expect(await db.runStatement(compileStatement("SELECT 7 AS value"))).toMatchObject({
      kind: "rows",
      result: { rows: [{ value: 7 }] },
    });
    await db.createTable({
      name: "projection_rows",
      columns: [
        { name: "id", type: "number" },
        { name: "score", type: "number" },
      ],
    });
    await db.insertBatch("projection_rows", [
      { id: 1, score: 20 },
      { id: 2, score: 10 },
      { id: 3, score: 50 },
      { id: 4, score: 40 },
      { id: 5, score: 30 },
    ]);
    let peak = 0;
    expect(
      (
        await db.query("SELECT id FROM projection_rows ORDER BY score", {
          onStats: (stats) => {
            peak = stats.peakMemoryBytes;
          },
        })
      ).rows,
    ).toEqual([{ id: 2 }, { id: 1 }, { id: 5 }, { id: 4 }, { id: 3 }]);
    expect(peak).toBeGreaterThan(0);
    const batches = [];
    for await (const batch of db.queryCursor("SELECT id FROM projection_rows ORDER BY score", {
      batchRows: 2,
    })) {
      batches.push(...batch.rows);
    }
    expect(batches).toEqual([{ id: 2 }, { id: 1 }, { id: 5 }, { id: 4 }, { id: 3 }]);

    for (let value = 0; value <= 128; value += 1) {
      await db.execute(`SELECT ${String(value)} AS cache_value`);
    }
    await expect(db.query("SELECT 1 UNION SELECT 1, 2")).rejects.toThrow(
      "UNION members must select the same number of columns",
    );
    await db.close();
  });

  it("restarts a standalone insert after ADD COLUMN and applies the new default", async () => {
    const store = new CommitRaceStore();
    const first = database(store);
    const rival = database(store);
    await first.execute("CREATE TABLE records (id INTEGER PRIMARY KEY)");
    store.beforeWrite = async () => {
      await rival.execute("ALTER TABLE records ADD COLUMN source TEXT NULL DEFAULT 'after-ddl'");
    };

    await first.insert("records", { id: 1 });

    expect((await first.query("SELECT id, source FROM records")).rows).toEqual([
      { id: 1, source: "after-ddl" },
    ]);
    await first.close();
    await rival.close();
  });

  it("restarts a standalone insert after CREATE TRIGGER and runs the new trigger", async () => {
    const store = new CommitRaceStore();
    const first = database(store);
    const rival = database(store);
    await first.execute("CREATE TABLE records (id INTEGER PRIMARY KEY)");
    await first.execute("CREATE TABLE audit (record_id INTEGER NOT NULL)");
    store.beforeWrite = async () => {
      await rival.execute(
        "CREATE TRIGGER records_audit AFTER INSERT ON records BEGIN " +
          "INSERT INTO audit (record_id) VALUES (NEW.id); END",
      );
    };

    await first.insert("records", { id: 7 });

    expect((await first.query("SELECT record_id FROM audit")).rows).toEqual([{ record_id: 7 }]);
    await first.close();
    await rival.close();
  });

  it("fails an explicit write scope after structural DDL without publishing partial rows", async () => {
    const store = new CommitRaceStore();
    const first = database(store);
    const rival = database(store);
    await first.execute("CREATE TABLE records (id INTEGER PRIMARY KEY)");

    await expect(
      first.write(async (session) => {
        await session.insertBatch("records", [{ id: 1 }]);
        await rival.execute("ALTER TABLE records ADD COLUMN note TEXT");
      }),
    ).rejects.toBeInstanceOf(SchemaConflictError);

    expect((await first.query("SELECT id, note FROM records")).rows).toEqual([]);
    await first.close();
    await rival.close();
  });

  it.each(["UPDATE", "DELETE"] as const)(
    "re-evaluates a standalone %s predicate after a real commit conflict",
    async (kind) => {
      const store = new CommitRaceStore();
      const first = database(store);
      const rival = database(store);
      await first.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)");
      await first.execute("INSERT INTO records VALUES (1, 10)");

      // This single-segment statement publishes through the atomic write boundary. Race there,
      // not only through the older two-step commit hook, so the retry proves the real fast path.
      store.beforeWrite = async () => {
        await rival.updateBatch("records", { keys: [1], changes: { score: [20] } });
      };
      const result = await first.execute(
        kind === "UPDATE"
          ? "UPDATE records SET score = score + 1 WHERE score = 10"
          : "DELETE FROM records WHERE score = 10",
      );

      expect(result).toMatchObject({ kind: kind.toLowerCase(), rowCount: 0 });
      expect((await first.query("SELECT id, score FROM records")).rows).toEqual([
        { id: 1, score: 20 },
      ]);
      await first.close();
      await rival.close();
    },
  );

  it.each(["INSERT", "UPSERT", "UPDATE"] as const)(
    "re-proves a foreign key after a concurrent parent delete during %s",
    async (kind) => {
      const store = new CommitRaceStore();
      const first = database(store);
      const rival = database(store);
      await first.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)");
      await first.execute(
        "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))",
      );
      await first.execute("INSERT INTO parents VALUES (1), (2)");
      if (kind !== "INSERT") await first.execute("INSERT INTO children VALUES (7, 1)");

      store.beforeWrite = async () => {
        await rival.deleteBatch("parents", { keys: [2] });
      };
      const write =
        kind === "INSERT"
          ? first.insert("children", { id: 7, parent_id: 2 })
          : kind === "UPSERT"
            ? first.upsert("children", { id: 7, parent_id: 2 })
            : first.updateBatch("children", {
                keys: [7],
                changes: { parent_id: [2] },
              });

      await expect(write).rejects.toThrow("FOREIGN KEY");
      expect((await first.query("SELECT id FROM parents ORDER BY id")).rows).toEqual([{ id: 1 }]);
      expect((await first.query("SELECT id, parent_id FROM children")).rows).toEqual(
        kind === "INSERT" ? [] : [{ id: 7, parent_id: 1 }],
      );
      await first.close();
      await rival.close();
    },
  );

  it.each([
    "INSERT INTO returning_rows VALUES (1, 10) RETURNING missing",
    "INSERT INTO returning_rows VALUES (1, 10) ON CONFLICT (id) DO REPLACE RETURNING missing",
    "UPDATE returning_rows SET score = 20 WHERE id = 999 RETURNING missing",
    "DELETE FROM returning_rows WHERE id = 999 RETURNING missing",
  ])("validates RETURNING before any write or zero-row shortcut: %s", async (sql) => {
    const store = new MemoryBlockStore();
    const db = database(store);
    await db.execute("CREATE TABLE returning_rows (id INTEGER PRIMARY KEY, score INTEGER)");
    const before = await store.getCurrentManifestVersion();

    await expect(db.execute(sql)).rejects.toThrow(/missing/);

    expect(await store.getCurrentManifestVersion()).toBe(before);
    expect((await db.query("SELECT * FROM returning_rows")).rows).toEqual([]);
    await db.close();
  });

  it("rolls back an invalid RETURNING statement without poisoning the surrounding transaction", async () => {
    const store = new MemoryBlockStore();
    const db = database(store);
    await db.execute("CREATE TABLE returning_scope (id INTEGER PRIMARY KEY)");
    await db.execute("BEGIN");
    await expect(
      db.execute("INSERT INTO returning_scope VALUES (1) RETURNING missing"),
    ).rejects.toThrow("RETURNING column does not exist: missing");
    await db.execute("INSERT INTO returning_scope VALUES (2)");
    await db.execute("COMMIT");
    expect((await db.query("SELECT id FROM returning_scope")).rows).toEqual([{ id: 2 }]);
    await db.close();
  });

  it("streams CTAS through multiple bounded artifact stages and publishes all rows atomically", async () => {
    const store = new CtasObservingStore();
    const db = new MinnowDatabase(store, {
      autoCollect: false,
      autoCompact: false,
      compression: "raw",
      rowsPerBlock: 1,
    });
    await db.execute("CREATE TABLE source_rows (id INTEGER PRIMARY KEY)");
    await db.insertBatch(
      "source_rows",
      Array.from({ length: 65 }, (_, index) => ({ id: index + 1 })),
    );
    const beforeStages = store.artifactStages;

    await db.execute("CREATE TABLE copied_rows AS SELECT id FROM source_rows");

    expect(store.artifactStages - beforeStages).toBeGreaterThan(1);
    expect((await db.query("SELECT COUNT(*) AS count FROM copied_rows")).rows).toEqual([
      { count: 65 },
    ]);
    await db.close();
  });

  it("restarts CTAS after structural DDL and rebuilds its pending table from the new schema", async () => {
    const store = new CommitRaceStore();
    const first = database(store);
    const rival = database(store);
    await first.execute("CREATE TABLE source_rows (id INTEGER PRIMARY KEY)");
    await first.execute("INSERT INTO source_rows VALUES (1), (2)");
    const beforeAttempts = store.commitAttempts;
    store.beforeCommit = async () => {
      await rival.execute("ALTER TABLE source_rows ADD COLUMN note TEXT NULL DEFAULT 'after-ddl'");
    };

    await first.execute("CREATE TABLE copied_rows AS SELECT id FROM source_rows");

    expect(store.commitAttempts - beforeAttempts).toBe(2);
    expect((await first.query("SELECT id FROM copied_rows ORDER BY id")).rows).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    await first.close();
    await rival.close();
  });

  it("publishes neither CTAS catalog nor rows when the final atomic commit fails", async () => {
    const store = new CtasObservingStore();
    const db = database(store);
    await db.execute("CREATE TABLE source_rows (id INTEGER PRIMARY KEY)");
    await db.execute("INSERT INTO source_rows VALUES (1), (2)");
    store.failCommit = true;

    await expect(
      db.execute("CREATE TABLE failed_copy AS SELECT id FROM source_rows"),
    ).rejects.toMatchObject({ name: "QuotaExceededError" });

    expect(await store.getTableByName("failed_copy")).toBeUndefined();
    expect((await db.query("SELECT id FROM source_rows ORDER BY id")).rows).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    await db.close();
  });

  it("refuses an over-budget CTAS before staging or publishing its target", async () => {
    const store = new CtasObservingStore();
    const db = new MinnowDatabase(store, {
      autoCollect: false,
      autoCompact: false,
      executionMemoryBudgetBytes: 1_024,
    });
    await db.execute("CREATE TABLE source_rows (id INTEGER PRIMARY KEY, note TEXT NOT NULL)");
    await db.insertBatch(
      "source_rows",
      Array.from({ length: 32 }, (_, index) => ({ id: index + 1, note: "x".repeat(256) })),
    );
    const beforeStages = store.artifactStages;

    await expect(
      db.execute("CREATE TABLE too_large AS SELECT id, note FROM source_rows"),
    ).rejects.toThrow("bytes");

    expect(store.artifactStages).toBe(beforeStages);
    expect(await store.getTableByName("too_large")).toBeUndefined();
    await db.close();
  });
});
