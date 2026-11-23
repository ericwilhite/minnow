/**
 * The owner offers a query a fresh snapshot only when the snapshot is the query's to choose.
 *
 * Law: PostgreSQL docs, Transaction Isolation (13.2). An autocommit statement takes its own
 * snapshot; a REPEATABLE READ transaction "sees a snapshot as of the start of the first
 * non-transaction-control statement" and every later statement sees that one. A statement that
 * locks rows has observed them. Lead decisions (2026-11-23): observing is delivering rows, taking
 * row locks, calling nextval, setval or currval, or writing. So:
 * - an autocommit query is offered a re-pin (owner/compose.ts `#queryPin`);
 * - the statement that took a read-only transaction's snapshot is offered one, and taking it
 *   replaces the transaction's snapshot for every later statement;
 * - a later statement of that transaction is refused one (the offer returns null);
 * - a query in a writable transaction is offered a fresh view of its statement (the writer turn
 *   keeps every other commit out, so it shows the same rows);
 * - a query that locks rows is offered none.
 * next/ refuses sequence functions in queries (0A000), so that exclusion (`usesSequences`) has no
 * SQL history yet.
 */
import { expect, it } from "vitest";
import type { Snapshot } from "../access/snapshot.js";
import { chunkReads } from "../exec/scan.js";
import { createExecutor } from "../exec/create.js";
import type { ExecuteRequest, Executor, PreparedPlan } from "../exec/executor.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { StorageCore } from "./storage-core.js";

interface Seen {
  readonly offered: boolean;
  /** What the offer returned when the test took it before the query ran. */
  readonly took: "fresh" | "declined" | null;
}

async function open(take: (index: number) => boolean) {
  const core = await new StorageCore().open();
  const seen: Seen[] = [];
  const inner = createExecutor();
  // Records each query's offer; takes it before the query runs when `take` says so, and runs the
  // query on what it returned, as the execution would after a refusal.
  const executor: Executor = {
    prepare(plan) {
      const prepared = inner.prepare(plan);
      return {
        ...prepared,
        bind: prepared.bind.bind(prepared),
        execute(request: ExecuteRequest) {
          if (request.statement !== null) return prepared.execute(request);
          const index = seen.length;
          const offer = request.repin;
          if (offer === undefined || !take(index)) {
            seen.push({ offered: offer !== undefined, took: null });
            return prepared.execute(request);
          }
          const fresh: Snapshot | null = offer(plan.catalogVersion);
          seen.push({ offered: true, took: fresh === null ? "declined" : "fresh" });
          return prepared.execute(fresh === null ? request : { ...request, snapshot: fresh });
        },
      } satisfies PreparedPlan;
    },
  };
  const engine = composeEngine({
    planner: core.lowerer,
    executor,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "repin-offer", epoch: 1, build: "test" });
  const ports: MessagePort[] = [];
  const clients: ProtocolClient[] = [];
  const connect = async () => {
    const { port1, port2 } = new MessageChannel();
    ports.push(port1, port2);
    server.accept(port1);
    const client = new ProtocolClient();
    clients.push(client);
    await client.connect(port2);
    return client;
  };
  const close = async () => {
    for (const client of clients) client.close();
    for (const port of ports) port.close();
    const closing = server.quiesce("closing");
    await closing.drained;
    closing.finish();
    await engine.close();
    await core.close();
  };
  const a = await connect();
  await a.query("CREATE TABLE t(id INT PRIMARY KEY, v INT)");
  await a.query("INSERT INTO t VALUES (1, 1), (2, 2)");
  seen.length = 0;
  return { core, seen, connect, close, a };
}

const count = async (client: ProtocolClient) =>
  Number((await client.query("SELECT count(*) FROM t")).rows()[0]?.[0]);

it("offers an autocommit query a fresh snapshot, and none to one that locks rows", async () => {
  const { seen, close, a } = await open(() => true);
  try {
    expect((await a.query("SELECT v FROM t WHERE id = 1")).rows()).toEqual([[1]]);
    expect(await count(a)).toBe(2);
    expect((await a.query("SELECT v FROM t WHERE id = 2 FOR UPDATE")).rows()).toEqual([[2]]);
    expect(seen).toEqual([
      { offered: true, took: "fresh" },
      { offered: true, took: "fresh" },
      { offered: false, took: null },
    ]);
  } finally {
    await close();
  }
}, 30_000);

it("moves a read-only transaction's snapshot only from the statement that took it", async () => {
  const { seen, connect, close, a } = await open(() => true);
  try {
    const b = await connect();
    await a.query("BEGIN READ ONLY");
    expect(await count(a), "the first statement").toBe(2);
    // Taken by the first statement: later statements see the snapshot it moved to.
    await b.query("INSERT INTO t VALUES (3, 3)");
    const later = await count(a).catch((error: unknown) => error);
    expect(later, "a later statement keeps the transaction's snapshot").toBe(2);
    expect((await a.query("SELECT v FROM t WHERE id = 3")).rows()).toEqual([]);
    await a.query("COMMIT");
    expect(await count(a)).toBe(3);
    expect(seen.slice(0, 3)).toEqual([
      { offered: true, took: "fresh" },
      { offered: true, took: "declined" },
      { offered: true, took: "declined" },
    ]);
  } finally {
    await close();
  }
}, 30_000);

it("offers a query in a writable transaction a fresh view of the same rows", async () => {
  const { seen, close, a } = await open(() => true);
  try {
    await a.query("BEGIN");
    await a.query("INSERT INTO t VALUES (3, 3)");
    expect(await count(a), "its own write, through the fresh view").toBe(3);
    expect((await a.query("SELECT v FROM t WHERE id = 3")).rows()).toEqual([[3]]);
    await a.query("ROLLBACK");
    expect(await count(a)).toBe(2);
    expect(seen.slice(0, 2)).toEqual([
      { offered: true, took: "fresh" },
      { offered: true, took: "fresh" },
    ]);
  } finally {
    await close();
  }
}, 30_000);
