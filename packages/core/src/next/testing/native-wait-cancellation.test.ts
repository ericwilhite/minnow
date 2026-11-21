/**
 * A native write that needs more memory than another holder leaves waits, and cancelling the
 * statement ends that wait (GOVERNOR-WAIT-POLICY points 3 and 5: grant, cancellable wait, or spill;
 * never a refusal below the cap). The competitor never releases: it stands for an application that
 * holds a result open while it awaits its own write, which only cancellation (or a statement
 * timeout) can end, as a lock wait in PostgreSQL. The write updates a 6 MiB value whose AFTER
 * trigger copies it: more than the 1 MiB the competitor leaves plus the reserve a native root may use.
 *
 * Contract (owner/client.ts: cancellation is acknowledged by the owner's terminal reply; docs
 * engine/errors.mdx: the `cancelled` class is "an AbortError from the caller's own signal", which
 * has not published): the call rejects with the caller's abort reason, and nothing of the write is
 * visible. Laws checked:
 * - cancelling through the statement's signal ends it promptly with that AbortError;
 * - nothing leaks: every grant the statement took is returned with its rollback;
 * - the row and the trigger's copy are unchanged;
 * - the competitor and the engine stay usable: the competitor is untouched, and once it is gone
 *   the same statement runs;
 * - in test builds the stall guard reports the same wait, naming the waiter and its holder.
 */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

const WIDTH = 6 << 20;
const STATEMENT = "UPDATE t SET v = v || 'b'";

/** A database whose write needs more than a competitor holding all but 1 MiB leaves. */
async function pressed() {
  const owner = await publicOwner({ capBytes: 64 << 20 });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  for (const sql of [
    "CREATE TABLE t(id INT PRIMARY KEY, v TEXT)",
    "CREATE TABLE copy(id INT, v TEXT)",
    "CREATE TRIGGER keep AFTER UPDATE ON t BEGIN INSERT INTO copy VALUES(NEW.id, NEW.v); END",
  ])
    await db.execute(sql);
  await db.execute("INSERT INTO t VALUES(1, $1)", ["a".repeat(WIDTH)]);
  const governor = owner.core.harness.governor;
  const competitor = await governor.admit({
    label: "application result held open",
    minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes - (1 << 20),
    spillable: false,
  });
  expect(competitor.allocate(competitor.held)).toBe(true);
  return { owner, db, governor, competitor };
}

async function unchanged(db: MinnowDatabaseClient): Promise<void> {
  expect((await db.query("SELECT id FROM t WHERE v = $1", ["a".repeat(WIDTH)])).rows()).toEqual([
    { id: 1 },
  ]);
  expect((await db.query("SELECT count(*) AS n FROM copy")).rows()).toEqual([{ n: 0 }]);
}

it("ends a native write waiting behind a non-releasing holder when the statement is cancelled", async () => {
  const { owner, db, governor, competitor } = await pressed();
  try {
    const before = governor.usedBytes;
    const held = competitor.held;
    const stop = new AbortController();
    let settled = false;
    const run = db.execute(STATEMENT, [], { signal: stop.signal }).then(
      () => "completed",
      (error: unknown) => error,
    );
    void run.then(() => (settled = true));
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(settled, "the write waits behind the competitor").toBe(false);
    const cancelledAt = performance.now();
    stop.abort();
    const outcome = await run;
    expect(performance.now() - cancelledAt, "cancellation ends the wait promptly").toBeLessThan(
      2_000,
    );
    // The caller's own abort reason: the `cancelled` class, which did not publish.
    expect(outcome).toBe(stop.signal.reason);
    expect(outcome).toMatchObject({ name: "AbortError" });
    // Nothing leaks: the statement's grants are back, the competitor is untouched.
    expect(governor.usedBytes).toBeLessThanOrEqual(before);
    expect(competitor.held).toBe(held);
    expect(competitor.allocated).toBe(held);
    // The application releases its result; the write left nothing behind and now runs.
    competitor.close();
    await unchanged(db);
    await db.execute(STATEMENT);
    expect(
      (await db.query("SELECT id FROM t WHERE v = $1", [`${"a".repeat(WIDTH)}b`])).rows(),
    ).toEqual([{ id: 1 }]);
    expect((await db.query("SELECT count(*) AS n FROM copy")).rows()).toEqual([{ n: 1 }]);
  } finally {
    competitor.close();
    await db.close();
    await owner.close();
  }
  expect(governor.usedBytes).toBe(0);
}, 120_000);

it("reports the same wait through the stall guard in test builds", async () => {
  const previous = Governor.stallAfterMs;
  Governor.stallAfterMs = 1_000;
  const { owner, db, governor, competitor } = await pressed();
  try {
    const outcome = await db.execute(STATEMENT).then(
      () => "completed",
      (error: unknown) => error,
    );
    expect(outcome).toMatchObject({ code: "MINNOW_GOVERNOR_STALL" });
    const message = (outcome as Error).message;
    expect(message, "the holder is named").toContain("application result held open");
    expect(message, "the waiter is named").toMatch(/"[^"]+" waited \d+ ms for \d+B/u);
    competitor.close();
    await unchanged(db);
    await db.execute(STATEMENT);
    expect((await db.query("SELECT count(*) AS n FROM copy")).rows()).toEqual([{ n: 1 }]);
  } finally {
    Governor.stallAfterMs = previous;
    competitor.close();
    await db.close();
    await owner.close();
  }
  expect(governor.usedBytes).toBe(0);
}, 120_000);
