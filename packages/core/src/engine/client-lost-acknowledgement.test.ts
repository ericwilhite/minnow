/**
 * The transaction layer's recovery of a lost durable acknowledgement. The store performs the
 * operation and then throws OpfsUncertainOutcomeError, exactly what an OPFS follower sees when
 * the leader died after applying the frame (or withheld a > 64 KiB result). The engine reads
 * the record back, recognises its own write, and carries on: nothing is staged twice, the scope
 * commits its rows, and an abort still finds the record to mark.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { scopeWriteSetTestHooks } from "./scope-write-set.js";
import {
  OpfsUncertainOutcomeError,
  type StageTransactionArtifactsInput,
  type TransactionRecord,
} from "../storage/types.js";
import { faultyStore } from "./client-audit-harness.js";

function lostAckStore(target: string, hide: string[] = [], times = 1) {
  let armed = false;
  let remaining = times;
  const { store, calls } = faultyStore(
    async (method, _args, run) => {
      if (armed && method === target && remaining > 0) {
        remaining -= 1;
        await run();
        throw new OpfsUncertainOutcomeError(method);
      }
      return run();
    },
    { hide },
  );
  const database = new MinnowDatabase(store);
  return {
    database,
    calls,
    store,
    arm: () => {
      armed = true;
    },
  };
}

async function seed(database: MinnowDatabase): Promise<void> {
  await database.createTable({
    name: "t",
    columns: [
      { name: "id", type: "number" },
      { name: "v", type: "string" },
    ],
    uniqueKey: "id",
  });
}

interface Scope {
  insertBatch(table: "t", rows: Array<{ id: number; v: string }>): Promise<unknown>;
  query(sql: string): Promise<{ rows: Array<Record<string, unknown>> }>;
}

/**
 * Inserts more statements than the transaction defers in memory. With the scope budget at one
 * byte every statement flushes to its own block, and the transaction journals its deferred
 * artifacts to the store once they pass MAX_TRANSACTION_STAGE_BLOCKS — so the durable
 * create/stage calls happen here, inside the callback. The count read afterwards proves the
 * scope still answers reads after adopting a record whose acknowledgement was lost.
 */
async function insertManyAndCount(s: Scope, count = 80): Promise<unknown> {
  for (let i = 0; i < count; i += 1) await s.insertBatch("t", [{ id: i, v: "x".repeat(16) }]);
  const { rows } = await s.query("SELECT COUNT(*) AS n FROM t");
  return rows[0]?.n;
}

describe("lost commit acknowledgement", () => {
  it("recovers a single-shot writeTransaction and commits once", async () => {
    const { database, calls, arm } = lostAckStore("writeTransaction");
    await seed(database);
    arm();
    const result = await database.insert("t", { id: 1, v: "x" });
    expect(result).toBeDefined();
    expect((await database.readTable("t")).length).toBe(1);
    expect(calls.filter((c) => c.method === "writeTransaction").length).toBe(1);
  });

  it("recovers a two-step commitTransaction inside write() and commits once", async () => {
    const { database, calls, arm } = lostAckStore("commitTransaction", ["writeTransaction"]);
    await seed(database);
    arm();
    const { version } = await database.write(async (s) => {
      await s.insertBatch("t", [{ id: 1, v: "x" }]);
    });
    expect(version).not.toBeNull();
    expect((await database.readTable("t")).length).toBe(1);
    expect(calls.filter((c) => c.method === "commitTransaction").length).toBe(1);
  });
});

describe("lost staging acknowledgement", () => {
  beforeEach(() => {
    scopeWriteSetTestHooks.budgetBytes = 1;
  });
  afterEach(() => {
    scopeWriteSetTestHooks.budgetBytes = scopeWriteSetTestHooks.defaultBudgetBytes;
  });

  it("adopts the staged record, stages nothing twice, and commits the scope", async () => {
    const { database, calls, store, arm } = lostAckStore("stageTransactionArtifacts", [
      "writeTransaction",
    ]);
    await seed(database);
    arm();
    const { version, result } = await database.write(async (s) => insertManyAndCount(s));
    expect(version).not.toBeNull();
    expect(result, "count read inside the scope after the adopted stage").toBe(80);
    expect((await database.readTable("t")).length).toBe(80);
    const stages = calls
      .filter((c) => c.method === "stageTransactionArtifacts")
      .map((c) => c.args[0] as StageTransactionArtifactsInput);
    expect(stages.length).toBeGreaterThanOrEqual(1);
    // The lost-ack stage was adopted, not repeated: no block id was staged twice.
    const stagedBlockIds = stages.flatMap((stage) => stage.blocks.map((block) => block.id));
    expect(new Set(stagedBlockIds).size).toBe(stagedBlockIds.length);
    const txId = stages[0]?.transactionId ?? "";
    const record = await store.getTransaction(txId);
    expect(record?.status).toBe("committed");
    expect(new Set(record?.pendingBlockIds).size).toBe(record?.pendingBlockIds.length);
  });

  it("adopts a record whose createTransaction acknowledgement was lost and commits the scope", async () => {
    const { database, calls, store, arm } = lostAckStore("createTransaction", ["writeTransaction"]);
    await seed(database);
    arm();
    const { version, result } = await database.write(async (s) => insertManyAndCount(s));
    expect(version).not.toBeNull();
    expect(result, "count read inside the scope after the adopted record").toBe(80);
    expect((await database.readTable("t")).length).toBe(80);
    const created = calls.filter((c) => c.method === "createTransaction");
    expect(created.length).toBe(1);
    const txId = (created[0]?.args[0] as TransactionRecord).id;
    expect((await store.getTransaction(txId))?.status).toBe("committed");
  });

  it("marks an adopted record aborted when the scope fails after the lost acknowledgement", async () => {
    const { database, calls, store, arm } = lostAckStore("createTransaction", ["writeTransaction"]);
    await seed(database);
    arm();
    const error = await database
      .write(async (s) => {
        expect(await insertManyAndCount(s)).toBe(80);
        throw new Error("application rollback");
      })
      .catch((e: unknown) => e);
    expect((error as Error).message).toBe("application rollback");
    expect((await database.readTable("t")).length).toBe(0);
    const created = calls.filter((c) => c.method === "createTransaction");
    expect(created.length).toBe(1);
    const txId = (created[0]?.args[0] as TransactionRecord).id;
    // The engine learned the record exists, so the rollback marked it instead of leaving it
    // active — pin held — until it expired.
    expect((await store.getTransaction(txId))?.status).toBe("aborted");
  });
});
