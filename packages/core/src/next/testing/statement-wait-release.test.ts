/**
 * A statement whose need fits once another holder releases waits for that release and is then
 * served: it is never refused (GOVERNOR-WAIT-POLICY point 5 refuses only what could never be
 * served beside the statement's own holdings and its writer's).
 *
 * The other holders here are real: another connection's large read, paused with its result
 * undelivered, and a background flush held before it publishes. Beside the read, each statement
 * is written at once or after the read ends, never refused (its result progress lane covers the
 * row its spool copies: C1 found it refused, `result/progress needs …`, beside a smaller read).
 * Behind the held flush, the waiter is shown to wait, then to complete with its row written.
 */
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
const CAP = 32 * MiB;
const PENDING = Symbol("pending");
const within = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);

afterEach(() => {
  Governor.stallAfterMs = null;
});

async function ids(core: StorageCore): Promise<number[]> {
  const rows = await core.execute("SELECT id FROM t ORDER BY id");
  if (!("batches" in rows)) throw new Error("rows");
  return oracleRows(rows).map((row) => Number(row.id));
}

/** The other connection's table: its rows' widths (C1's variants: a 1.5 MB row, two 2 MiB). */
const READERS: ReadonlyArray<readonly number[]> = [[2 * MiB], [1_500_000], [2 * MiB, 2 * MiB]];

for (const reader of READERS)
  for (const [sql, width] of [
    ["INSERT INTO t VALUES (10, $1)", 1_500_000],
    ["INSERT INTO t VALUES (10, $1)", 3_000_000],
    ["INSERT INTO t SELECT 10, $1 || $1", 500_000],
  ] as const)
    it(`is served beside another connection's read of ${reader.map((w) => String(w)).join(" + ")} bytes, at once or once it ends: ${sql} (${String(width)})`, async () => {
      Governor.stallAfterMs = 5_000;
      const core = new StorageCore();
      const governor = new Governor(CAP);
      Object.defineProperty(core.harness, "governor", { value: governor });
      await core.open();
      try {
        await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
        for (const [at, bytes] of reader.entries())
          await core.execute(`INSERT INTO t SELECT ${String(at + 1)}, $1`, ["b".repeat(bytes)]);
        const idle = governor.usedBytes;
        // The other connection reads the table and holds its result undelivered.
        const lowered = core.lowerer.lower("SELECT id, v FROM t", core.context());
        if (lowered.kind !== "query") throw new Error("query plan required");
        const other = core.start(core.executor.prepare(lowered.plan));
        for (let i = 0; i < 200 && governor.usedBytes - idle < 4 * MiB; i++)
          await new Promise((resolve) => setTimeout(resolve, 5));
        expect(governor.usedBytes - idle, "the other connection holds its read").toBeGreaterThan(
          4 * MiB,
        );
        const statement = core.execute(sql, ["w".repeat(width)]).then(
          () => "written" as const,
          (error: unknown) => error,
        );
        // Never refused: written beside the read if it fits, else it waits for the read.
        const early = await within(statement, 500);
        if (early !== PENDING) expect(early).toBe("written");
        for (let frame = await other.execution.results.next(); frame !== null;) {
          frame.release();
          frame = await other.execution.results.next();
        }
        await other.execution.delivered;
        other.release();
        expect(await statement).toBe("written");
        expect(await ids(core)).toEqual([...reader.map((_, at) => at + 1), 10]);
        expect(core.harness.path.failure).toBeNull();
      } finally {
        await core.close();
      }
    }, 60_000);

it("waits for a background flush to publish, then writes", async () => {
  Governor.stallAfterMs = 5_000;
  let hold = false;
  let held = 0;
  let resume: (() => void) | null = null;
  // Memtables of two rows: the fourth wide row freezes one and starts its flush.
  const core = new StorageCore(2, {
    beforeFlushPublish: () => {
      if (!hold) return Promise.resolve();
      held++;
      return new Promise<void>((resolve) => {
        resume = resolve;
      });
    },
  });
  Object.defineProperty(core.harness, "governor", { value: new Governor(CAP) });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    hold = true;
    for (let id = 1; id <= 4; id++)
      await core.execute(`INSERT INTO t SELECT ${String(id)}, $1`, ["q".repeat(MiB)]);
    for (let i = 0; i < 200 && held === 0; i++)
      await new Promise((resolve) => setTimeout(resolve, 5));
    expect(held, "a flush is held before it publishes").toBe(1);
    // Wide enough not to fit beside the held flush's rows (a 1.5 MB row now fits at once, since
    // the memtable charges what a step allocates rather than twice it).
    const statement = core.execute("INSERT INTO t VALUES (10, $1)", ["w".repeat(2_500_000)]);
    expect(await within(statement, 500), "it waits while the flush holds its rows").toBe(PENDING);
    hold = false;
    (resume as (() => void) | null)?.();
    await statement;
    expect(await ids(core)).toEqual([1, 2, 3, 4, 10]);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    hold = false;
    (resume as (() => void) | null)?.();
    await core.close();
  }
}, 60_000);
