/**
 * A read-only transaction takes its snapshot at its first statement, not at BEGIN.
 *
 * next/ pinned a READ ONLY transaction's snapshot when BEGIN ran (owner/compose.ts `begin`), so
 * a row another connection committed between `BEGIN READ ONLY` and the first SELECT was not seen.
 * The pin is now taken by the first query or data statement (`#guard`, `#transactionPin`), even
 * one refused as a write, and by `SET TRANSACTION READ ONLY` only after a statement already ran.
 *
 * Law (PostgreSQL, "Transaction Isolation", 13.2): a REPEATABLE READ transaction "sees a snapshot
 * as of the start of the first non-transaction-control statement in the transaction", and every
 * later statement sees that snapshot. Transaction control (BEGIN, SAVEPOINT, RELEASE, ROLLBACK TO,
 * SET TRANSACTION) and SHOW take none. PostgreSQL takes the snapshot before the executor's
 * read-only check, so a write refused with 25006 fixes it too; ROLLBACK TO keeps it.
 *
 * PGlite runs one backend, so a second session cannot commit inside the first one's transaction
 * there: the shadow model below is the rule above, and the history runs on the real engine.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { createReach, READ_ONLY_SNAPSHOT_TIMING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";

type Entry = "begin read only" | "start transaction read only" | "set read only" | "switch after";
type First = "scan" | "point" | "constant" | "refused write";
type Control = "savepoint" | "show" | "set isolation" | "set read only";

interface History {
  readonly entry: Entry;
  readonly controls: readonly Control[];
  readonly first: First;
  /** Gaps where the other connection commits a row: 0 before BEGIN, 1 after the entry, then one
   * after each control statement, then after the first statement and after the second read. */
  readonly commits: readonly boolean[];
}

async function open() {
  const core = await new StorageCore().open();
  await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)");
  await core.execute("INSERT INTO t VALUES (1, 1)");
  const engine = composeEngine({
    planner: core.lowerer,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "read-only-timing", epoch: 1, build: "test" });
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
  return { core, connect, close };
}

const count = async (client: ProtocolClient) =>
  Number((await client.query("SELECT count(*) FROM t")).rows()[0]?.[0]);

async function run(history: History, reach?: ReturnType<typeof createReach>): Promise<void> {
  const { core, connect, close } = await open();
  try {
    const a = await connect();
    const b = await connect();
    const context = JSON.stringify(history);
    let rows = 1;
    let gap = 0;
    let visible = 1;
    let taken = false;
    // A read-write transaction holds the writer turn: the other connection commits only while
    // this one is read-only or outside a transaction.
    let writable = false;
    // The other connection commits one row at this gap if the history says so. A row committed
    // before the transaction's snapshot is seen; after it, never.
    const gapEnds = async () => {
      if (history.commits[gap++] !== true || writable) return;
      await b.query(`INSERT INTO t VALUES (${String(100 + rows)}, 0)`);
      rows += 1;
      if (taken) reach?.reach("commit after the snapshot unseen");
      else {
        visible += 1;
        reach?.reach("commit before the snapshot seen");
      }
    };
    await gapEnds();
    // Every entry is REPEATABLE READ, the level the law describes (next/ runs every level on
    // the transaction's snapshot; READ COMMITTED's per-statement snapshot is a documented
    // divergence, sql/dml.mdx).
    if (history.entry === "begin read only")
      await a.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    else if (history.entry === "start transaction read only")
      await a.query("START TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ");
    else {
      await a.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
      writable = true;
      if (history.entry === "set read only") {
        await a.query("SET TRANSACTION READ ONLY");
        writable = false;
      }
    }
    await gapEnds();
    let savepoints = 0;
    for (const control of history.controls) {
      if (control === "savepoint") await a.query(`SAVEPOINT s${String(savepoints++)}`);
      else if (control === "show") await a.query("SHOW transaction_isolation");
      else if (control === "set isolation")
        await a.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
      else {
        await a.query("SET TRANSACTION READ ONLY");
        writable = false;
      }
      await gapEnds();
    }
    // The first statement takes the snapshot.
    const expected = visible;
    if (history.first === "scan") {
      expect(await count(a), `${context}: the first read`).toBe(expected);
    } else if (history.first === "point") {
      expect((await a.query("SELECT v FROM t WHERE id = 1")).rows(), context).toEqual([[1]]);
      reach?.reach("point read first");
    } else if (history.first === "constant") {
      await a.query("SELECT 1");
    } else {
      // Refused as a write, inside a savepoint so the transaction continues.
      await a.query("SAVEPOINT w");
      const refused = await a.query("INSERT INTO t VALUES (1000, 0)").then(
        () => null,
        (error: unknown) => (error as { sqlState?: string }).sqlState ?? String(error),
      );
      if (writable) expect(refused, `${context}: written while read-write`).toBeNull();
      else {
        expect(refused, `${context}: refused in a read-only transaction`).toBe("25006");
        reach?.reach("refused write first");
      }
      await a.query("ROLLBACK TO w");
    }
    taken = true;
    if (writable) {
      await a.query("SET TRANSACTION READ ONLY");
      writable = false;
      reach?.reach("read only after the first statement");
    }
    await gapEnds();
    expect(await count(a), `${context}: a later read sees the first statement's snapshot`).toBe(
      expected,
    );
    await gapEnds();
    expect(await count(a), `${context}: and so does the next`).toBe(expected);
    await a.query("COMMIT");
    expect(await count(a), `${context}: every commit, after the transaction`).toBe(rows);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    await close();
  }
}

it("sees a row committed between BEGIN READ ONLY and the first SELECT", async () => {
  const { connect, close } = await open();
  try {
    const a = await connect();
    const b = await connect();
    await a.query("BEGIN READ ONLY");
    await b.query("INSERT INTO t VALUES (2, 2)");
    expect(await count(a), "the first SELECT sees the commit made after BEGIN").toBe(2);
    await a.query("COMMIT");
  } finally {
    await close();
  }
}, 30_000);

it("keeps the snapshot a refused write took, through ROLLBACK TO", async () => {
  await run({
    entry: "begin read only",
    controls: ["savepoint"],
    first: "refused write",
    commits: [false, true, true, true, true],
  });
}, 30_000);

const entry = fc.constantFrom<Entry>(
  "begin read only",
  "start transaction read only",
  "set read only",
  "switch after",
);
const first = fc.constantFrom<First>("scan", "point", "constant", "refused write");
const control = fc.constantFrom<Control>("savepoint", "show", "set isolation", "set read only");

for (const seed of seedsFor("next-read-only-snapshot-timing", [0x6101, 0x6102]))
  it(`a read-only transaction's snapshot is its first statement's (seed ${String(seed)})`, async () => {
    const reach = createReach("read-only snapshot timing", READ_ONLY_SNAPSHOT_TIMING_REACH);
    await fc.assert(
      fc.asyncProperty(
        entry,
        fc.array(control, { maxLength: 3 }),
        first,
        fc.array(fc.boolean(), { minLength: 7, maxLength: 7 }),
        async (entry, controls, first, commits) => {
          reach.beginHistory();
          await run({ entry, controls, first, commits }, reach);
        },
      ),
      { seed, numRuns: 30 },
    );
    reach.assertReached();
  }, 300_000);
