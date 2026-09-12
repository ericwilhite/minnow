/**
 * The keyed constant UPDATE / DELETE path — a `WHERE key = constant` or `key IN (...)` statement
 * that joins the scope's write set without reading the table — commits exactly what the general
 * read-first path commits. Each script runs twice in a scope: once in its keyed form and once
 * with RETURNING appended, which forces the general path. Row counts, error classes, resulting
 * state, and trigger audit rows must match, on both stores (generated columns included).
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

const DDL = [
  "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL DEFAULT 1, label TEXT, flag BOOLEAN NOT NULL DEFAULT FALSE, doubled INTEGER GENERATED ALWAYS AS (amount * 2) STORED, code INTEGER)",
  "CREATE UNIQUE INDEX items_code ON items (code)",
  "CREATE TABLE audit (action TEXT NOT NULL, item_id INTEGER NOT NULL, amount INTEGER NOT NULL)",
  "INSERT INTO items (id, amount, label, code) VALUES (1, 10, 'a', 1), (2, 20, 'b', 2), (3, 30, 'c', 3)",
];

async function fixture(
  create: () => Promise<BlockStore>,
  trigger = false,
): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(await create(), {});
  for (const sql of DDL) await db.execute(sql);
  if (trigger) {
    await db.execute(
      "CREATE TRIGGER items_upd AFTER UPDATE ON items BEGIN INSERT INTO audit (action, item_id, amount) VALUES ('upd', NEW.id, NEW.amount); END",
    );
    await db.execute(
      "CREATE TRIGGER items_del AFTER DELETE ON items BEGIN INSERT INTO audit (action, item_id, amount) VALUES ('del', OLD.id, OLD.amount); END",
    );
  }
  return db;
}

const STATE = "SELECT id, amount, label, flag, doubled, code FROM items ORDER BY id";
const AUDIT = "SELECT action, item_id, amount FROM audit ORDER BY item_id, action, amount";

interface Outcome {
  counts: Array<number | string>;
  state: unknown;
  audit: unknown;
}

/** Runs the statements in a scope; `general` appends RETURNING id to force the read path. */
async function outcome(
  create: () => Promise<BlockStore>,
  statements: string[],
  general: boolean,
  trigger = false,
): Promise<Outcome> {
  const db = await fixture(create, trigger);
  const counts: Array<number | string> = [];
  await db.write(async (tx) => {
    for (const sql of statements) {
      try {
        const result = await tx.execute(
          general && /^(UPDATE|DELETE)/.test(sql) ? `${sql} RETURNING id` : sql,
        );
        counts.push("rowCount" in result ? result.rowCount : -1);
      } catch (error) {
        counts.push((error as Error).constructor.name);
      }
    }
  });
  const state = (await db.query(STATE)).rows;
  const audit = (await db.query(AUDIT)).rows;
  await db.close();
  return { counts, state, audit };
}

describe.each(implementations)("keyed constant mutations on $name", ({ create }) => {
  const scripts: Array<[string, string[]]> = [
    [
      "IN list with missing, deleted, and inserted keys",
      [
        "INSERT INTO items (id, amount, label, code) VALUES (4, 40, 'd', 4)",
        "DELETE FROM items WHERE id = 2",
        "UPDATE items SET amount = 7 WHERE id IN (1, 2, 4, 99)",
        "UPDATE items SET label = 'z' WHERE id IN (2, 2, 3)",
        "DELETE FROM items WHERE id IN (3, 4, 2, 77)",
        "UPDATE items SET amount = 8 WHERE id = 3",
        "DELETE FROM items WHERE id = 99",
      ],
    ],
    [
      "reversed operands, aliases, booleans, NULL, and defaults",
      [
        "UPDATE items SET flag = TRUE WHERE 1 = id",
        "UPDATE items AS x SET amount = 5 WHERE x.id = 2",
        "UPDATE items SET label = NULL WHERE id = 3",
        "UPDATE items SET label = 'k', amount = 9 WHERE id = 1",
        "INSERT INTO items (id) VALUES (5)",
        "UPDATE items SET amount = 50 WHERE id = 5",
        "UPDATE items SET code = 55 WHERE id = 5",
        "DELETE FROM items AS y WHERE y.id = 2",
      ],
    ],
    [
      "generated column recomputed from the folded row",
      [
        "INSERT INTO items (id, amount, label, code) VALUES (6, 1, 'g', 6)",
        "UPDATE items SET amount = 2 WHERE id = 6",
        "UPDATE items SET amount = 3 WHERE id = 6",
        "UPDATE items SET amount = 11 WHERE id = 1",
        "UPDATE items SET label = 'x' WHERE id = 1",
      ],
    ],
    [
      "refusals leave the scope usable",
      [
        "UPDATE items SET amount = NULL WHERE id = 1",
        "UPDATE items SET amount = 12 WHERE id = 1",
        "UPDATE items SET amount = 'abc' WHERE id = 2",
        "UPDATE items SET doubled = 5 WHERE id = 3",
        "UPDATE items SET id = 9 WHERE id = 3",
        "DELETE FROM items WHERE id = 3",
      ],
    ],
    [
      "unique index terms retire through keyed updates and deletes",
      [
        "UPDATE items SET code = 9 WHERE id = 1",
        "INSERT INTO items (id, amount, code) VALUES (7, 1, 1)",
        "DELETE FROM items WHERE id = 2",
        "INSERT INTO items (id, amount, code) VALUES (8, 1, 2)",
        "UPDATE items SET code = NULL WHERE id = 3",
        "INSERT INTO items (id, amount, code) VALUES (9, 1, 3)",
        "UPDATE items SET code = 3 WHERE id = 9",
        "INSERT INTO items (id, amount, label, code) VALUES (3, 3, 'c', 30) ON CONFLICT (id) DO UPDATE SET code = EXCLUDED.code",
        "INSERT INTO items (id, amount, code) VALUES (10, 1, 9) ON CONFLICT (id) DO NOTHING",
      ],
    ],
  ];
  it.each(scripts)(
    "%s: the keyed path commits what the general path commits",
    async (_name, statements) => {
      const keyed = await outcome(create, statements, false);
      const general = await outcome(create, statements, true);
      expect(keyed.state).toEqual(general.state);
      expect(keyed.counts).toEqual(general.counts);
    },
  );

  it("with triggers: per-statement staging fires the same audit rows either way", async () => {
    const statements = [
      "INSERT INTO items (id, amount, label, code) VALUES (4, 40, 'd', 4)",
      "UPDATE items SET amount = 41 WHERE id = 4",
      "UPDATE items SET amount = 7 WHERE id IN (1, 4, 99)",
      "DELETE FROM items WHERE id IN (2, 4, 77)",
      "UPDATE items SET label = 'q' WHERE id = 2",
      "DELETE FROM items WHERE id = 99",
    ];
    const keyed = await outcome(create, statements, false, true);
    const general = await outcome(create, statements, true, true);
    expect(keyed.state).toEqual(general.state);
    expect(keyed.audit).toEqual(general.audit);
    expect(keyed.counts).toEqual(general.counts);
    // A missing key fires nothing.
    expect(keyed.audit).not.toContainEqual({ action: "upd", item_id: 99, amount: 7 });
  });

  it("commit-time unique enforcement matches what the statements accepted", async () => {
    // Term 1 is retired by the keyed update of id 1 before id 7 takes it; the commit must
    // accept exactly that, on every store.
    const db = await fixture(create);
    await db.write(async (tx) => {
      await tx.execute("UPDATE items SET code = 9 WHERE id = 1");
      await tx.execute("INSERT INTO items (id, amount, code) VALUES (7, 1, 1)");
      await tx.execute("DELETE FROM items WHERE id = 7");
      await tx.execute("INSERT INTO items (id, amount, code) VALUES (8, 1, 1)");
      await tx.execute("UPDATE items SET code = 1 WHERE id = 8");
    });
    expect((await db.query("SELECT id FROM items WHERE code = 1")).rows).toEqual([{ id: 8 }]);
    expect((await db.query("SELECT id FROM items WHERE code = 9")).rows).toEqual([{ id: 1 }]);
    await db.close();
  });
});
