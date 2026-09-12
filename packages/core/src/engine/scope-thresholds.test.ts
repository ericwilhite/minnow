/**
 * The write set's encoding triggers — a block's worth of rows (rowsPerBlock), the per-scope
 * byte budget (mirror drop), the 4,096-row direct-stage threshold, savepoints, and a statement
 * that fails part-way — never change what commits.
 *
 * Self-differential: the same seeded script runs under several threshold settings, skipping
 * exactly the statements the default configuration refused, and every configuration must
 * commit the same state.
 */
import { IDBFactory } from "fake-indexeddb";
import { afterEach, describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { mulberry32 } from "../testing/seeds.js";
import { MinnowDatabase, type WriteSession } from "./database.js";
import { MissingKeyError, UniqueConstraintError } from "./errors.js";
import { scopeWriteSetTestHooks } from "./scope-write-set.js";

afterEach(() => {
  scopeWriteSetTestHooks.budgetBytes = scopeWriteSetTestHooks.defaultBudgetBytes;
});

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

interface Row {
  id: number;
  amount: number;
  code: number | null;
}
type Op =
  | { kind: "insert"; rows: Row[] }
  | { kind: "upsert"; rows: Row[]; guard?: number }
  | { kind: "update"; keys: number[]; amount: number }
  | { kind: "delete"; keys: number[] }
  | { kind: "sql"; sql: string; params: unknown[] }
  | { kind: "read" }
  | { kind: "savepoint" }
  | { kind: "rollback" };

function gen(seed: number, steps: number, keys: number): Op[] {
  const rng = mulberry32(seed);
  const key = (): number => 1 + Math.floor(rng() * keys);
  const amount = (): number => Math.floor(rng() * 100);
  // Codes are per key (id * 10 + k) so the UNIQUE index only ever sees its own key's terms
  // retire and return; the script exercises encoding, not cross-key unique refusals.
  const code = (id: number): number | null =>
    rng() < 0.3 ? null : id * 10 + Math.floor(rng() * 3);
  const rows = (n: number): Row[] => {
    const seen = new Set<number>();
    const out: Row[] = [];
    while (out.length < n) {
      const id = key();
      if (seen.has(id)) continue;
      seen.add(id);
      out.push({ id, amount: amount(), code: code(id) });
    }
    return out;
  };
  const ops: Op[] = [];
  let open = 0;
  for (let i = 0; i < steps; i += 1) {
    const p = rng();
    if (p < 0.2) ops.push({ kind: "insert", rows: rows(1 + Math.floor(rng() * 5)) });
    else if (p < 0.35) ops.push({ kind: "upsert", rows: rows(1 + Math.floor(rng() * 3)) });
    else if (p < 0.42) ops.push({ kind: "upsert", rows: rows(2), guard: amount() });
    else if (p < 0.57)
      ops.push({ kind: "update", keys: [...new Set([key(), key(), key()])], amount: amount() });
    else if (p < 0.7) ops.push({ kind: "delete", keys: [...new Set([key(), key()])] });
    else if (p < 0.78)
      ops.push({
        kind: "sql",
        sql: "UPDATE items SET amount = $1 WHERE id IN ($2, $3)",
        params: [amount(), key(), key()],
      });
    else if (p < 0.84)
      ops.push({
        kind: "sql",
        sql: "UPDATE items SET amount = amount + 1 WHERE id = $1",
        params: [key()],
      });
    else if (p < 0.9) ops.push({ kind: "read" });
    else if (p < 0.95 && open < 2) {
      ops.push({ kind: "savepoint" });
      open += 1;
    } else if (open > 0) {
      ops.push({ kind: "rollback" });
      open -= 1;
    } else ops.push({ kind: "read" });
  }
  return ops;
}

type SqlRunner = Pick<WriteSession, "query" | "execute">;

const values = (rows: Row[]): string =>
  rows
    .map(
      (r) => `(${String(r.id)}, ${String(r.amount)}, ${r.code === null ? "NULL" : String(r.code)})`,
    )
    .join(", ");

/** The batch ops spelled as SQL so the SQL-transaction form (with savepoints) can run them. */
async function apply(session: SqlRunner, op: Op): Promise<void> {
  switch (op.kind) {
    case "insert":
      await session.execute(`INSERT INTO items (id, amount, code) VALUES ${values(op.rows)}`);
      return;
    case "upsert": {
      const guard = op.guard === undefined ? "" : ` WHERE items.amount < ${String(op.guard)}`;
      await session.execute(
        `INSERT INTO items (id, amount, code) VALUES ${values(op.rows)} ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount, code = EXCLUDED.code${guard}`,
      );
      return;
    }
    case "update": {
      // updateBatch semantics: every key must exist. Emulate by checking first.
      const present = (
        await session.query(`SELECT COUNT(*) AS n FROM items WHERE id IN (${op.keys.join(", ")})`)
      ).rows[0]?.n;
      if (present !== op.keys.length) throw new MissingKeyError("items", "id", op.keys[0] ?? 0);
      await session.execute(
        `UPDATE items SET amount = ${String(op.amount)} WHERE id IN (${op.keys.join(", ")})`,
      );
      return;
    }
    case "delete":
      await session.execute(`DELETE FROM items WHERE id IN (${op.keys.join(", ")})`);
      return;
    case "sql":
      await session.execute(op.sql, op.params as never);
      return;
    case "read":
      await session.query("SELECT COUNT(*) AS n FROM items WHERE amount > 50");
      return;
    case "savepoint":
    case "rollback":
      return;
  }
}

/** Errors a statement may raise and leave the scope usable. */
function tolerable(error: unknown): boolean {
  return (
    error instanceof UniqueConstraintError ||
    error instanceof MissingKeyError ||
    /duplicate|missing|does not exist/i.test((error as Error).message)
  );
}

async function fixture(store: BlockStore, rowsPerBlock: number): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(store, { rowsPerBlock });
  await db.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, code INTEGER)",
  );
  await db.execute("CREATE UNIQUE INDEX items_code ON items (code)");
  await db.execute(
    "INSERT INTO items (id, amount, code) VALUES (1, 10, 1), (2, 20, 2), (3, 30, 3)",
  );
  return db;
}

const STATE = "SELECT id, amount, code FROM items ORDER BY id";

/**
 * Runs the script in one SQL transaction (so savepoints are available) with the given
 * thresholds. Statements that fail with a tolerable error are skipped, and the skip decisions
 * are recorded so another configuration can replay exactly the accepted statements.
 */
async function scoped(
  store: BlockStore,
  ops: Op[],
  rowsPerBlock: number,
  budget: number,
  accepted?: boolean[],
): Promise<{ state: unknown; accepted: boolean[] }> {
  scopeWriteSetTestHooks.budgetBytes = budget;
  const db = await fixture(store, rowsPerBlock);
  const decisions: boolean[] = [];
  await db.execute("BEGIN");
  let depth = 0;
  const session: SqlRunner = {
    query: (sql, options) => db.query(sql, options),
    execute: (sql, params) => db.execute(sql, params),
  };
  for (const [index, op] of ops.entries()) {
    if (op.kind === "savepoint") {
      await db.execute(`SAVEPOINT s${String(depth)}`);
      depth += 1;
      decisions.push(true);
      continue;
    }
    if (op.kind === "rollback") {
      depth -= 1;
      await db.execute(`ROLLBACK TO SAVEPOINT s${String(depth)}`);
      await db.execute(`RELEASE SAVEPOINT s${String(depth)}`);
      decisions.push(true);
      continue;
    }
    if (accepted?.[index] === false) {
      decisions.push(false);
      continue;
    }
    try {
      await apply(session, op);
      decisions.push(true);
    } catch (error) {
      if (!tolerable(error)) throw error;
      decisions.push(false);
    }
  }
  await db.execute("COMMIT");
  const state = (await db.query(STATE)).rows;
  await db.close();
  return { state, accepted: decisions };
}

describe.each(implementations)(
  "encoding thresholds never change the commit on $name",
  ({ create }) => {
    it.each([21, 22, 23, 24])(
      "seed %s: rowsPerBlock 4 / budget 2 KiB / both agree with the defaults",
      async (seed) => {
        const ops = gen(seed, 80, 24);
        const defaults = scopeWriteSetTestHooks.defaultBudgetBytes;
        const baseline = await scoped(await create(), ops, 65_536, defaults);
        const smallBlocks = await scoped(await create(), ops, 4, defaults, baseline.accepted);
        const tinyBudget = await scoped(await create(), ops, 65_536, 2_048, baseline.accepted);
        const both = await scoped(await create(), ops, 4, 512, baseline.accepted);
        expect(smallBlocks.accepted).toEqual(baseline.accepted);
        expect(tinyBudget.accepted).toEqual(baseline.accepted);
        expect(both.accepted).toEqual(baseline.accepted);
        expect(smallBlocks.state).toEqual(baseline.state);
        expect(tinyBudget.state).toEqual(baseline.state);
        expect(both.state).toEqual(baseline.state);
      },
    );

    it("a statement failing on its third row leaves the set as it was", async () => {
      const db = await fixture(await create(), 8);
      await db.write(async (tx) => {
        await tx.insertBatch("items", [{ id: 4, amount: 4, code: null }]);
        await expect(
          tx.insertBatch("items", [
            { id: 5, amount: 5, code: null },
            { id: 6, amount: 6, code: null },
            { id: 4, amount: 44, code: null },
            { id: 7, amount: 7, code: null },
            { id: 8, amount: 8, code: null },
          ]),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
        await expect(
          tx.updateBatch("items", { keys: [1, 2, 99, 3], changes: { amount: [0, 0, 0, 0] } }),
        ).rejects.toBeInstanceOf(MissingKeyError);
        await expect(
          tx.execute(
            "INSERT INTO items (id, amount, code) VALUES (5, 5, NULL), (6, 6, NULL), (4, 44, NULL), (7, 7, NULL)",
          ),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
        expect((await tx.query(STATE)).rows).toEqual([
          { id: 1, amount: 10, code: 1 },
          { id: 2, amount: 20, code: 2 },
          { id: 3, amount: 30, code: 3 },
          { id: 4, amount: 4, code: null },
        ]);
        await tx.insertBatch("items", [{ id: 5, amount: 5, code: null }]);
      });
      expect((await db.query("SELECT id FROM items ORDER BY id")).rows.map((r) => r.id)).toEqual([
        1, 2, 3, 4, 5,
      ]);
      await db.close();
    });

    it("a 4,096-row statement stages after the pending set and before what follows", async () => {
      const db = await fixture(await create(), 65_536);
      const big = Array.from({ length: 4_096 }, (_, i) => ({ id: 100 + i, amount: i, code: null }));
      await db.write(async (tx) => {
        await tx.updateBatch("items", { keys: [1], changes: { amount: [111] } });
        await tx.insertBatch("items", [{ id: 100, amount: -1, code: null }]);
        await tx.deleteBatch("items", { keys: [100] });
        await tx.upsertBatch("items", [...big, { id: 1, amount: 1_000, code: 1 }]);
        await tx.updateBatch("items", { keys: [1, 100], changes: { amount: [2_000, 2_000] } });
        await tx.deleteBatch("items", { keys: [101] });
        await tx.insertBatch("items", [{ id: 101, amount: 5, code: null }]);
      });
      const rows = (
        await db.query(
          "SELECT id, amount FROM items WHERE id IN (1, 100, 101, 102, 4195) ORDER BY id",
        )
      ).rows;
      expect(rows).toEqual([
        { id: 1, amount: 2_000 },
        { id: 100, amount: 2_000 },
        { id: 101, amount: 5 },
        { id: 102, amount: 2 },
        { id: 4195, amount: 4_095 },
      ]);
      expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows[0]?.n).toBe(3 + 4_096);
      await db.close();
    });

    it("savepoints checkpoint pending entries and rollback restores them exactly", async () => {
      const db = await fixture(await create(), 65_536);
      await db.execute("BEGIN");
      await db.execute("INSERT INTO items (id, amount, code) VALUES (4, 4, NULL)");
      await db.execute("UPDATE items SET amount = 11 WHERE id = 1");
      await db.execute("SAVEPOINT a");
      await db.execute("UPDATE items SET amount = 44 WHERE id = 4");
      await db.execute("DELETE FROM items WHERE id = 1");
      await db.execute("INSERT INTO items (id, amount, code) VALUES (5, 5, NULL)");
      await db.execute("SAVEPOINT b");
      await db.execute("DELETE FROM items WHERE id = 4");
      await db.execute("ROLLBACK TO SAVEPOINT b");
      expect((await db.query(STATE)).rows).toEqual([
        { id: 2, amount: 20, code: 2 },
        { id: 3, amount: 30, code: 3 },
        { id: 4, amount: 44, code: null },
        { id: 5, amount: 5, code: null },
      ]);
      await db.execute("ROLLBACK TO SAVEPOINT a");
      expect((await db.query(STATE)).rows).toEqual([
        { id: 1, amount: 11, code: 1 },
        { id: 2, amount: 20, code: 2 },
        { id: 3, amount: 30, code: 3 },
        { id: 4, amount: 4, code: null },
      ]);
      // Keys freed by the rollback are usable again; keys kept are still taken.
      await db.execute("INSERT INTO items (id, amount, code) VALUES (5, 55, NULL)");
      await expect(
        db.execute("INSERT INTO items (id, amount, code) VALUES (4, 0, NULL)"),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await db.execute("UPDATE items SET amount = 45 WHERE id = 4");
      await db.execute("DELETE FROM items WHERE id = 1");
      await db.execute("INSERT INTO items (id, amount, code) VALUES (1, 111, 1)");
      await db.execute("COMMIT");
      expect((await db.query(STATE)).rows).toEqual([
        { id: 1, amount: 111, code: 1 },
        { id: 2, amount: 20, code: 2 },
        { id: 3, amount: 30, code: 3 },
        { id: 4, amount: 45, code: null },
        { id: 5, amount: 55, code: null },
      ]);
      await db.close();
    });
  },
);
