/**
 * The next statement of a transaction that holds credit is holder growth: it never queues as a
 * new admission behind a request that waits on that very credit.
 *
 * Found probing #65's follow-up (2026-11-23), pure SQL on main 97023c2b. Session A: BEGIN, two
 * 2 MB rows (uncommitted: the writer keeps them until A ends). Session B: `SELECT id, v FROM big`
 * (8 rows x 3 MB) needs 18.98 MB of fixed buffers, more than is free beside A's rows: it waits,
 * queued as a new admission. Session A: one more INSERT, whose fixed buffers (7.42 MB) fit in the
 * 17.9 MB free, queued behind B in FIFO order. B waits for A's transaction to end; A's
 * transaction waits for that INSERT; both stall (GovernorStallError in test builds, a hang in
 * production). Also at 32 MiB (6 x 4 MB, 5 uncommitted rows).
 *
 * Law: GOVERNOR-WAIT-POLICY point 2 (next/memory/README.md): a request whose statement or
 * transaction holds credit never queues as a new admission behind requests that may wait on it;
 * its growth is holder growth, served before new admissions, still refused typed (53200) when it
 * could never be served beside what it keeps (point 5). B waiting on an open transaction stays a
 * cancellable wait (an application-level wait; lead decision, 2026-11-23). Point 6: the
 * statement's holder growth joins the writer's holder chain, the one chain of the transaction
 * that holds the writer turn.
 */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { createLowerer } from "../lower/create.js";
import { Governor, type HolderChain } from "../memory/governor.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { createReach, TRANSACTION_HOLDER_GROWTH_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Harness } from "../write/test-support.js";

const MiB = 1 << 20;

afterEach(() => {
  Governor.stallAfterMs = null;
  vi.restoreAllMocks();
});

type Next = "insert" | "update" | "select" | "create index" | "idle";
type Other = "wide select" | "write";

interface History {
  readonly capMiB: number;
  readonly bigRows: number;
  readonly bigWidth: number;
  /** A's uncommitted rows before its next statement. */
  readonly txRows: number;
  readonly next: Next;
  readonly other: Other;
  /** A's next statement inside a savepoint, rolled back to after it. */
  readonly savepoint: boolean;
  readonly end: "commit" | "rollback";
  /** B is cancelled while it waits (after A's next statement settled). */
  readonly cancelOther: boolean;
}

/** What A's next statement queued in the governor while B waited (the invariant test). */
interface Queued {
  readonly label: string;
  readonly chain: HolderChain | undefined;
}

const describe = (error: unknown) =>
  error instanceof Error
    ? `${error.name} ${(error as { sqlState?: string }).sqlState ?? ""} ${error.message}`
    : String(error);

async function run(history: History, reach?: ReturnType<typeof createReach>, queued?: Queued[]) {
  Governor.stallAfterMs = 5_000;
  const h = new Harness("strict", "memory");
  const governor = new Governor(history.capMiB * MiB);
  Object.defineProperty(h, "governor", { value: governor });
  await h.open();
  const engine = composeEngine({
    planner: createLowerer(),
    writePath: h.path,
    governor,
    spill: new MemorySpillService(),
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "holder-growth", epoch: 1, build: "test" });
  const clients: ProtocolClient[] = [];
  const ports: MessagePort[] = [];
  for (let k = 0; k < 2; k++) {
    const { port1, port2 } = new MessageChannel();
    ports.push(port1, port2);
    server.accept(port1);
    const client = new ProtocolClient();
    await client.connect(port2);
    clients.push(client);
  }
  const [a, b] = clients as [ProtocolClient, ProtocolClient];
  const context = JSON.stringify(history);
  try {
    await a.query("CREATE TABLE big (id integer PRIMARY KEY, v text)");
    await a.query("CREATE TABLE t (id integer PRIMARY KEY, k integer, v text)");
    for (let k = 1; k <= history.bigRows; k++)
      await a.query("INSERT INTO big VALUES ($1, $2)", [k, "b".repeat(history.bigWidth)]);
    await h.path.quiesce();
    const baseline = governor.usedBytes;
    await a.query("BEGIN");
    for (let k = 1; k <= history.txRows; k++)
      await a.query("INSERT INTO t VALUES ($1, $1, $2)", [k, "a".repeat(2_000_000)]);
    // B queues behind A's uncommitted rows.
    const cancel = new AbortController();
    const other =
      history.other === "wide select"
        ? b.query("SELECT id, v FROM big", [], { signal: cancel.signal })
        : b.query("INSERT INTO big VALUES (1000, 'w')", [], { signal: cancel.signal });
    const outcome = other.then(
      () => "served",
      (error: unknown) => error,
    );
    await new Promise((resolve) => setTimeout(resolve, 200));
    if (history.savepoint) await a.query("SAVEPOINT s");
    const statements: Record<Exclude<Next, "idle">, readonly [string, unknown[]]> = {
      insert: ["INSERT INTO t VALUES ($1, 0, $2)", [history.txRows + 1, "a".repeat(2_000_000)]],
      update: ["UPDATE t SET k = k + 1 WHERE id = 1", []],
      select: ["SELECT id, v FROM t", []],
      "create index": ["CREATE INDEX t_k ON t (k)", []],
    };
    let watching = false;
    if (queued !== undefined) {
      const admit = governor.admit.bind(governor);
      const admitGroup = governor.admitGroup.bind(governor);
      vi.spyOn(governor, "admit").mockImplementation((options) => {
        if (watching) queued.push({ label: options.label, chain: options.holdsCredit });
        return admit(options);
      });
      vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
        if (watching) queued.push({ label: options.label, chain: options.holdsCredit });
        return admitGroup(options);
      });
    }
    watching = true;
    if (history.next !== "idle") {
      // A's next statement holds the writer turn and A's rows: it is served (or refused typed),
      // never left behind B.
      const [sql, params] = statements[history.next];
      const result = await a.query(sql, params as never).then(
        () => "done",
        (error: unknown) => error,
      );
      // A refused read of a write's own rows is a ReadDeniedError without a SQLSTATE until #65
      // (fix/lp-read-denied-delete) types it 53200 at the statement boundary; tighten then.
      if (result !== "done")
        expect(describe(result), `${context}: A's next statement`).toMatch(
          /^(?:MemoryAdmissionError 53200|ReadDeniedError )/u,
        );
      // Served, or refused typed: never left waiting behind B (point 2). An early refusal of a
      // row step is the limit stated in the commit (funded credit not yet protected until use).
      reach?.reach("A answered beside a waiting reader");
    } else reach?.reach("A idle");
    watching = false;
    if (history.savepoint) {
      await a.query("ROLLBACK TO s");
      reach?.reach("savepoint");
    }
    if (history.cancelOther) {
      cancel.abort(new Error("cancelled by the test"));
      reach?.reach("B cancelled");
    }
    await a
      .query(history.end === "commit" ? "COMMIT" : "ROLLBACK")
      .catch(() => a.query("ROLLBACK"));
    reach?.reach(history.end);
    const settled = await outcome;
    if (settled === "served") reach?.reach("B served after A ended");
    else if (history.cancelOther) expect(describe(settled), context).not.toMatch(/Stall/u);
    else expect(describe(settled), `${context}: B`).toMatch(/^MemoryAdmissionError 53200/u);
    // Nothing of A or B stays charged once both ended and maintenance is quiet.
    await h.path.quiesce();
    expect(h.path.failure, context).toBeNull();
    if (history.end === "rollback" || history.next !== "insert")
      expect(governor.usedBytes, `${context}: governor back to its baseline`).toBeLessThanOrEqual(
        baseline + 16 * MiB,
      );
    reach?.reach(history.next);
    reach?.reach(history.other);
  } finally {
    for (const client of clients) client.close();
    const stop = server.quiesce("closing");
    await stop.drained;
    stop.finish();
    for (const port of ports) port.close();
    await engine.close();
    await h.close();
  }
}

/** The reported two-way stalls (main 97023c2b). */
const REPORTED: readonly History[] = [
  {
    capMiB: 28,
    bigRows: 8,
    bigWidth: 3_000_000,
    txRows: 2,
    next: "insert",
    other: "wide select",
    savepoint: false,
    end: "commit",
    cancelOther: false,
  },
  {
    capMiB: 32,
    bigRows: 6,
    bigWidth: 4_000_000,
    txRows: 5,
    next: "insert",
    other: "wide select",
    savepoint: false,
    end: "commit",
    cancelOther: false,
  },
];

for (const history of REPORTED)
  it(`serves A's next statement beside B's waiting read at ${String(history.capMiB)} MiB`, async () => {
    await run(history);
  }, 60_000);

it("grows A's next statement from the writer's holder chain, never as a new admission", async () => {
  // GOVERNOR-WAIT-POLICY point 6: holder growth names an open registered chain, and the
  // statement's is the writer's own (its transaction holds the writer turn), never a new chain.
  const queued: Queued[] = [];
  const [first] = REPORTED;
  if (first === undefined) throw new Error("no reported history");
  await run(first, undefined, queued);
  const mine = queued.filter(
    (entry) => entry.label.startsWith("query#") || entry.label === "write path memtable",
  );
  expect(mine.length, "A's statement queued admissions while B waited").toBeGreaterThan(0);
  for (const entry of mine) {
    expect(entry.chain?.kind, `${entry.label}: holder growth from the writer chain`).toBe("writer");
  }
  expect(new Set(mine.map((entry) => entry.chain)).size, "one chain, the writer's").toBe(1);
}, 60_000);

for (const seed of seedsFor("next-transaction-holder-growth", [0x6d01, 0x6d02]))
  it(`a transaction's next statement never waits behind its own waiter (seed ${String(seed)})`, async () => {
    const reach = createReach("transaction holder growth", TRANSACTION_HOLDER_GROWTH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          // The table B reads, loaded before A begins: 4 MB rows only where the load fits (at
          // 28 MiB the load's own fixed buffers are refused typed beside the rows before it).
          table: fc.constantFrom(
            { capMiB: 28, bigRows: 8, bigWidth: 3_000_000 },
            { capMiB: 30, bigRows: 6, bigWidth: 3_000_000 },
            { capMiB: 30, bigRows: 8, bigWidth: 3_000_000 },
            { capMiB: 32, bigRows: 6, bigWidth: 4_000_000 },
            { capMiB: 32, bigRows: 8, bigWidth: 3_000_000 },
          ),
          txRows: fc.integer({ min: 1, max: 5 }),
          next: fc.constantFrom<Next>("insert", "update", "select", "create index", "idle"),
          other: fc.constantFrom<Other>("wide select", "write"),
          savepoint: fc.boolean(),
          end: fc.constantFrom<"commit" | "rollback">("commit", "rollback"),
          cancelOther: fc.boolean(),
        }),
        async ({ table, ...rest }) => {
          // Left out, not passed: B's write waiting for the writer turn while A's transaction
          // creates an index is answered PlanStaleError 0A000 at any cap (the owner should
          // re-lower it, exec/errors.ts). A separate defect, reported; not memory admission.
          fc.pre(!(rest.next === "create index" && rest.other === "write"));
          reach.beginHistory();
          await run({ ...table, ...rest }, reach);
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: REPORTED.map(({ capMiB, bigRows, bigWidth, ...rest }) => [
          { table: { capMiB, bigRows, bigWidth }, ...rest },
        ]),
      },
    );
    reach.assertReached();
  }, 900_000);
