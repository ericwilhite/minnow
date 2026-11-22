/**
 * A statement whose need fits once another holder releases waits for that release and is then
 * served: it is never refused (GOVERNOR-WAIT-POLICY point 5 refuses only what could never be
 * served beside the statement's own holdings and its writer's).
 *
 * The other holders here are real: another connection's large read, paused with its result
 * undelivered, and a background flush held before it publishes. Each waiter is shown to wait
 * (it does not fit while the other holds), and then to complete with its row written.
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

for (const [sql, width] of [
  ["INSERT INTO t VALUES (10, $1)", 1_500_000],
  ["INSERT INTO t VALUES (10, $1)", 3_000_000],
  ["INSERT INTO t SELECT 10, $1 || $1", 500_000],
] as const)
  it(`waits for another connection's statement to end, then writes: ${sql} (${String(width)})`, async () => {
    Governor.stallAfterMs = 5_000;
    const core = new StorageCore();
    const governor = new Governor(CAP);
    Object.defineProperty(core.harness, "governor", { value: governor });
    await core.open();
    try {
      await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
      await core.execute("INSERT INTO t SELECT 1, $1", ["b".repeat(2 * MiB)]);
      const idle = governor.usedBytes;
      // The other connection reads the 2 MiB row and holds its result undelivered.
      const lowered = core.lowerer.lower("SELECT id, v FROM t", core.context());
      if (lowered.kind !== "query") throw new Error("query plan required");
      const other = core.start(core.executor.prepare(lowered.plan));
      for (let i = 0; i < 200 && governor.usedBytes - idle < 8 * MiB; i++)
        await new Promise((resolve) => setTimeout(resolve, 5));
      expect(governor.usedBytes - idle, "the other connection holds its read").toBeGreaterThan(
        8 * MiB,
      );
      const statement = core.execute(sql, ["w".repeat(width)]);
      expect(await within(statement, 500), "it waits while the other holds").toBe(PENDING);
      for (let frame = await other.execution.results.next(); frame !== null;) {
        frame.release();
        frame = await other.execution.results.next();
      }
      await other.execution.delivered;
      other.release();
      // Served after the release, never refused.
      await statement;
      expect(await ids(core)).toEqual([1, 10]);
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
    const statement = core.execute("INSERT INTO t VALUES (10, $1)", ["w".repeat(1_500_000)]);
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
