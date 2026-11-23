/**
 * A read inside a writable transaction is served or refused typed: it never waits for memory its
 * own transaction keeps.
 *
 * A writable transaction holds the writer turn from BEGIN to its end, and while it does, the
 * write owner keeps its rows and credit (`LentHoldings.retained`). A query of that transaction
 * ran with `statement: null` and a read-only statement's view, whose `writerHoldings` was null,
 * so its admissions carried only its own grants: at 32 MiB, after two 2.5 MB rows, `BEGIN;
 * SELECT v FROM t WHERE id = 1` queued 27.46 MB of fixed buffers beside the writer's 8.55 MB
 * forever (GovernorStallError in test builds). Every statement of a write transaction now lends
 * the writer's holdings (write/transaction.ts `writerHoldings`), and the owner passes them with
 * the query (`ExecuteRequest.writerHoldings`).
 *
 * Law (GOVERNOR-WAIT-POLICY point 5, next/memory/README.md): a request that could never be served
 * beside what its statement keeps while it waits, including the writer's holdings while it holds
 * the turn, is refused with 53200 before it waits; PostgreSQL: an error aborts the transaction
 * (25P02 until ROLLBACK), which then leaves no trace.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { Governor } from "../memory/governor.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { createReach, WRITABLE_TRANSACTION_READ_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;

afterEach(() => {
  Governor.stallAfterMs = null;
});

async function open(capMiB: number) {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(capMiB * MiB) });
  await core.open();
  const engine = composeEngine({
    planner: core.lowerer,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "tx-read-wait", epoch: 1, build: "test" });
  const { port1, port2 } = new MessageChannel();
  server.accept(port1);
  const client = new ProtocolClient();
  await client.connect(port2);
  const close = async () => {
    client.close();
    port1.close();
    port2.close();
    const closing = server.quiesce("closing");
    await closing.drained;
    closing.finish();
    await engine.close();
    await core.close();
  };
  return { core, client, close };
}

const READS = ["SELECT v FROM t WHERE id = 1", "SELECT id, v FROM t ORDER BY id"] as const;

async function history(
  capMiB: number,
  width: number,
  read: (typeof READS)[number],
  writeFirst: boolean,
  reach?: ReturnType<typeof createReach>,
): Promise<void> {
  const { core, client, close } = await open(capMiB);
  const context = `${String(capMiB)} MiB, ${String(width)}-byte rows, ${read}${writeFirst ? " after a write" : ""}`;
  try {
    await client.query("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await client.query("INSERT INTO t VALUES (1, 'x'), (2, 'y')");
    await client.query("UPDATE t SET v = $1", ["a".repeat(width)]);
    await client.query("BEGIN");
    if (writeFirst) {
      await client.query("UPDATE t SET v = 'z' WHERE id = 2");
      reach?.reach("after a write in the transaction");
    }
    const outcome: unknown = await client.query(read).then(
      () => "served",
      (error: unknown) => error,
    );
    if (outcome === "served") reach?.reach("served");
    else {
      expect(outcome, `${context}: served or refused typed, never a stall`).toMatchObject({
        sqlState: "53200",
      });
      reach?.reach("refused typed");
    }
    await client.query("ROLLBACK");
    // The transaction left nothing, and the engine is usable.
    const rows = (await client.query("SELECT id FROM t WHERE v = 'z'")).rows();
    expect(rows, `${context}: rolled back`).toEqual([]);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    await close();
  }
}

// P1's history. With a read by key sized to the row it fetches (#52) it is served; on main it was
// sized 27.46 MB and refused. Either way it must not wait.
it("serves or refuses typed a read inside BEGIN rather than wait for the writer's own rows", async () => {
  await history(32, 2_500_000, "SELECT v FROM t WHERE id = 1", false);
}, 60_000);

for (const seed of seedsFor("next-writable-transaction-read", [0x6301, 0x6302]))
  it(`a read in a writable transaction never waits for its writer (seed ${String(seed)})`, async () => {
    const reach = createReach("writable transaction read", WRITABLE_TRANSACTION_READ_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 36, 48, 64),
        // 3,000,000-byte rows read in order at 32 MiB are refused typed beside the writer's
        // holdings; wider rows make the history's own post-rollback check refused at these caps.
        fc.constantFrom(1_000_000, 2_000_000, 2_500_000, 3_000_000),
        fc.constantFrom(...READS),
        fc.boolean(),
        async (cap, width, read, writeFirst) => {
          reach.beginHistory();
          await history(cap, width, read, writeFirst, reach);
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [32, 2_500_000, READS[0], false],
          [32, 3_000_000, READS[1], false],
        ],
      },
    );
    reach.assertReached();
  }, 600_000);
