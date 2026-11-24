/**
 * A write statement whose own reads its budget cannot hold completes or is refused typed (53200);
 * the reader's internal error (`ReadDeniedError`, `PageBudgetError`, `ParseBudgetError`) never
 * reaches the caller, and no statement hangs.
 *
 * Found while probing #59 (2026-11-23): at a 27 MiB cap, after three rows were updated to 2.5 MB
 * values, `DELETE FROM t WHERE id > 1` threw `ReadDeniedError` ("A read needs 2500000 bytes the
 * budget does not have", no SQLSTATE) out of write/rows.ts. Neon's review of #65 found the same
 * class at every write-path read on the write path's own budget: old rows, index runs (ON
 * CONFLICT, unique and foreign-key checks), deletion bitmaps, group roots, an upsert's
 * conflicting row, CREATE INDEX's backfill. Funding those reads (#65's first attempt) waited as a
 * new admission while holding credit, a hold-and-wait the lead rejected. Interim (lead decision):
 * the statement boundary types the refusal (page/read-refusal.ts, write/bytes.ts
 * `memoryRefusal`); no funding, no new wait.
 *
 * Law (GOVERNOR-WAIT-POLICY, next/memory/README.md): a statement completes, waits cancellably for
 * finite holders, or is refused typed with SQLSTATE 53200 before its effect; never untyped, never
 * forever. A refused statement changes nothing (statement atomicity, PostgreSQL); a completed one
 * leaves PostgreSQL's rows, which the model below states.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach, WRITE_PATH_READ_REFUSAL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows, type NextRows, type NextStatementResult } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
const TYPED = { name: "MemoryAdmissionError", code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" };

afterEach(() => {
  Governor.stallAfterMs = null;
});

const range = (rows: number) => Array.from({ length: rows }, (_, k) => k + 1);

/** The statements that read rows on the write path's own budget, and the ids each leaves. */
const STATEMENTS = {
  "delete range": { sql: "DELETE FROM t WHERE id > 1", ids: () => [1] },
  "delete one": {
    sql: "DELETE FROM t WHERE id = 2",
    ids: (rows: number) => range(rows).filter((id) => id !== 2),
  },
  rekey: {
    sql: "UPDATE t SET id = id + 10 WHERE id > 1",
    ids: (rows: number) => range(rows).map((id) => (id > 1 ? id + 10 : id)),
  },
  shrink: { sql: "UPDATE t SET v = 's' WHERE id > 1", ids: range },
  upsert: {
    sql: "INSERT INTO t VALUES (2, 0, 'u') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
    ids: range,
  },
  "create index": { sql: "CREATE INDEX t_k ON t (k)", ids: range },
  "delete cascade": { sql: "DELETE FROM t WHERE id > 1", ids: () => [1] },
} as const;
type Kind = keyof typeof STATEMENTS;

interface History {
  readonly capMiB: number;
  readonly rows: number;
  readonly width: number;
  readonly kind: Kind;
  /** Rows per memtable: small ones flush the wide rows into several groups. */
  readonly memtableRows: number | undefined;
  /** The wide rows' flush held at its publication for the statement, or settled first. */
  readonly flush: "held" | "settled";
  /** Another connection's read of the wide rows, held undelivered while the statement runs. */
  readonly read: "held" | "none";
}

const error = (cause: unknown) =>
  cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause);

function rowsOf(result: NextStatementResult): Array<Record<string, unknown>> {
  if (result.kind !== "rows") throw new Error(`rows expected, got ${result.kind}`);
  return oracleRows(result as NextRows);
}

/** The history: its setup, the statement beside the held flush and read, then the rows left. */
async function run(history: History, reach?: ReturnType<typeof createReach>) {
  Governor.stallAfterMs = 5_000;
  const gate = { entered: false };
  let release: () => void = () => undefined;
  const flushHeld = new Promise<void>((resolve) => {
    release = resolve;
  });
  const core = new StorageCore(
    history.memtableRows,
    history.flush === "held"
      ? {
          // Held as a finite holder: it publishes when the statement settles, or after 2 s (a
          // flush the setup itself waits for, with small memtables, must not wait on the test).
          beforeFlushPublish: () => {
            if (gate.entered) return Promise.resolve();
            gate.entered = true;
            return Promise.race([
              flushHeld,
              new Promise<void>((resolve) => setTimeout(resolve, 2_000)),
            ]);
          },
        }
      : undefined,
  );
  Object.defineProperty(core.harness, "governor", {
    value: new Governor(Math.round(history.capMiB * MiB)),
  });
  await core.open();
  const path = core.harness.path;
  const context = JSON.stringify(history);
  let endRead: (() => Promise<void>) | null = null;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, k INT, v TEXT)");
    await core.execute(
      `INSERT INTO t SELECT g, g, 'x' FROM generate_series(1, ${String(history.rows)}) g`,
    );
    if (history.kind === "delete cascade") {
      await core.execute(
        "CREATE TABLE c(id INT PRIMARY KEY, p INT REFERENCES t(id) ON DELETE CASCADE)",
      );
      await core.execute(
        `INSERT INTO c SELECT g, g FROM generate_series(1, ${String(history.rows)}) g`,
      );
    }
    // The setup may itself be refused at a tight cap: then there are no wide rows to read.
    const widened = await core.execute("UPDATE t SET v = $1", ["a".repeat(history.width)]).then(
      () => true,
      (cause: unknown) => {
        expect(cause, `${context}: the setup`).toMatchObject(TYPED);
        return false;
      },
    );
    if (!widened) {
      reach?.reach("setup refused");
      return;
    }
    if (history.flush === "settled") await path.quiesce();
    else
      for (let wait = 0; wait < 50 && !gate.entered; wait++)
        await new Promise((resolve) => setTimeout(resolve, 10));
    if (gate.entered) reach?.reach("flush held");
    const ids = async () =>
      rowsOf(await core.execute("SELECT id FROM t ORDER BY id")).map((row) => Number(row.id));
    const before = await ids();
    if (history.read === "held") {
      const lowered = core.lowerer.lower("SELECT id, v FROM t", core.context());
      if (lowered.kind !== "query") throw new Error("query plan required");
      const other = core.start(core.executor.prepare(lowered.plan));
      await new Promise((resolve) => setTimeout(resolve, 50));
      endRead = async () => {
        endRead = null;
        for (let frame = await other.execution.results.next(); frame !== null;) {
          frame.release();
          frame = await other.execution.results.next();
        }
        await other.execution.delivered.catch(() => undefined);
        other.release();
      };
      reach?.reach("read held");
    }
    const statement = STATEMENTS[history.kind];
    const running = core.execute(statement.sql).then(
      () => "completed" as const,
      (cause: unknown) => cause,
    );
    // A statement may wait for the held read or flush: both end after 2 s at most (finite
    // holders), and it is served or refused then. A hang is the stall guard's error.
    const PENDING = Symbol("pending");
    const first = await Promise.race([
      running,
      new Promise<typeof PENDING>((resolve) => setTimeout(() => resolve(PENDING), 2_000)),
    ]);
    void first;
    release();
    await endRead?.();
    const outcome = await running;
    if (outcome === "completed") {
      reach?.reach("completed");
      expect(await ids(), `${context}: the rows it left`).toEqual(statement.ids(history.rows));
    } else {
      expect(error(outcome), `${context}: refused typed, never untyped`).toMatch(
        /^MemoryAdmissionError/u,
      );
      expect(outcome, context).toMatchObject(TYPED);
      reach?.reach("refused typed");
      if ((outcome as Error).message.startsWith("write path read"))
        reach?.reach("refused at a write-path read");
      expect(await ids(), `${context}: a refused statement changes nothing`).toEqual(before);
    }
    reach?.reach(history.kind);
    expect(path.failure, context).toBeNull();
  } finally {
    release();
    await endRead?.();
    await core.close();
  }
}

/** The reported history: a DELETE beside the held flush of its wide rows (ReadDeniedError 8 of
 * 12 runs on main 1cf5b1406; 6 of 6 with the flush held). */
const REPORTED: History = {
  capMiB: 27,
  rows: 3,
  width: 2_500_000,
  kind: "delete range",
  memtableRows: undefined,
  flush: "held",
  read: "none",
};

it("refuses the reported DELETE typed or completes it, never with the reader's error", async () => {
  for (let attempt = 0; attempt < 4; attempt++) await run(REPORTED);
}, 120_000);

for (const seed of seedsFor("next-write-path-read-refusal", [0x6c01, 0x6c02]))
  it(`completes or refuses typed, beside held flushes and reads (seed ${String(seed)})`, async () => {
    const reach = createReach("write-path read refusal", WRITE_PATH_READ_REFUSAL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          capMiB: fc.integer({ min: 24 * 4, max: 40 * 4 }).map((quarters) => quarters / 4),
          rows: fc.integer({ min: 2, max: 4 }),
          width: fc.constantFrom(2_000_000, 2_500_000, 3_000_000),
          kind: fc.constantFrom<Kind>(
            "delete range",
            "delete one",
            "rekey",
            "shrink",
            "upsert",
            "create index",
            "delete cascade",
          ),
          memtableRows: fc.constantFrom<number | undefined>(undefined, 1, 2),
          flush: fc.constantFrom<"held" | "settled">("held", "settled"),
          read: fc.constantFrom<"held" | "none">("held", "none"),
        }),
        async (history) => {
          reach.beginHistory();
          await run(history, reach);
        },
      ),
      { seed, numRuns: 64, examples: [[REPORTED]] },
    );
    reach.assertReached();
  }, 1_200_000);
