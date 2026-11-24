/**
 * Replaying a log tail admits the memory its rows take before it stores them.
 *
 * Law: the governor contract (memory/governor.ts rules 2/4/5/7, PROPOSAL "One memory governor"):
 * below the cap, work completes, waits (cancellably) for temporary holdings, or is refused with a
 * typed 53200, never an untyped internal failure. Replay stores the tail's rows in memtables it
 * cannot flush until recovery ends; it stored them with no admission, so a temporary holder made
 * an open fail with `MemtableMemoryError` (no SQLSTATE). It now admits each step as an insert
 * does (write/step-charge.ts): it waits for a temporary holder, and when the rows could never fit
 * at this cap it refuses the open with `ReplayMemoryError` (53200) and leaves the log as it was,
 * so a larger cap opens it.
 *
 * The tail is thirty-two 1 MiB rows written with their flushes held (`beforeFlushPublish`), so every
 * row is replayed after the crash.
 */
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { need } from "./bytes.js";
import { ReplayMemoryError } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
  texts,
} from "./test-support.js";

const ROWS = 32;
const payload = (k: number) => String(k).padEnd(1 << 20, "x");
const GROWTH = "complete grow write path";
/** Room for the write owner's minimum and its startup, not for the replayed rows beside it. */
const SMALL_CAP = 30 * 1024 * 1024;

/** Tables of `PER_TABLE` 1 MiB rows each: no memtable reaches its byte limit and freezes. */
const TABLES = 8;
const PER_TABLE = ROWS / TABLES;

/**
 * A harness with `ROWS` 1 MiB rows only in its log: no memtable freezes, the log is not recycled,
 * and any flush is held until the next open starts (`onSpace`, after the crashed path is
 * abandoned). `h.crash()` then replays them.
 */
async function tail(): Promise<{ h: Harness; tables: number[] }> {
  let holding = true;
  const held = new Promise<void>(() => undefined);
  const h = new Harness("strict", "memory", 64, undefined, {
    beforeFlushPublish: () => (holding ? held : Promise.resolve()),
    logRecycleBytes: 1 << 30,
  });
  h.onSpace = () => {
    holding = false;
  };
  const path = await h.open();
  const tables: number[] = [];
  for (let t = 0; t < TABLES; t++)
    await autocommit(path, async (s) => {
      tables.push(
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput(`t${String(t)}`, [
              column("id", INT, false),
              column("v", { kind: "text" }),
            ]),
          })
        ).id ?? 0,
      );
    });
  for (let k = 0; k < ROWS; k++)
    await autocommit(path, (s) =>
      s.table(need(tables[k % TABLES])).insert(batch(ints([k]), texts([payload(k)])), null),
    );
  return { h, tables };
}

/** The governor every later open uses. The crashed path is abandoned, not closed: like a dead
 * process's, its grants stay on the governor it had, so each reopen gets its own. */
function freshGovernor(h: Harness, cap = 256 * 1024 * 1024): Governor {
  const governor = new Governor(cap);
  Object.defineProperty(h, "governor", { value: governor, configurable: true });
  return governor;
}

async function allRows(h: Harness, tables: readonly number[]): Promise<void> {
  const view = h.path.pin();
  try {
    for (const [t, table] of tables.entries()) {
      const rows = await rowsOf(view, table);
      const ids = Array.from({ length: PER_TABLE }, (_, k) => k * TABLES + t);
      expect(rows.map((row) => row[0])).toEqual(ids);
      expect(rows.map((row) => row[1])).toEqual(ids.map(payload));
    }
  } finally {
    view.release();
  }
}

/** Observes the replay's row growth: queued, and still queued once the code that queued it ran. */
function watchGrowth(governor: Governor, onQueued: () => void) {
  const counts = { queued: 0, blocked: 0 };
  const admitGroup = governor.admitGroup.bind(governor);
  governor.admitGroup = (options) => {
    if (options.label !== GROWTH) return admitGroup(options);
    const seen = { served: false, done: false };
    const stop = governor.observeGrants((event) => {
      if (event.label === GROWTH) seen.served = true;
    });
    const admitted = admitGroup(options);
    const settle = () => {
      seen.done = true;
      stop();
    };
    admitted.then(settle, settle);
    queueMicrotask(() => {
      if (seen.served || seen.done) return;
      counts.queued++;
      counts.blocked++;
      onQueued();
    });
    return admitted;
  };
  return counts;
}

/** Once the open's owner minimum is granted, a temporary reader takes every byte left. */
function fillAfterOwnerMinimum(governor: Governor): { reader: () => Grant | null } {
  let reader: Grant | null = null;
  const stop = governor.observeGrants((event) => {
    if (event.label !== "write owner minimum" || reader !== null) return;
    queueMicrotask(() => {
      reader = governor.tryAdmit({
        label: "temporary reader",
        minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
        spillable: false,
      });
      stop();
    });
  });
  return { reader: () => reader };
}

it("waits for a temporary holder while it replays, then opens with every row", async () => {
  const { h, tables } = await tail();
  const governor = freshGovernor(h);
  const filled = fillAfterOwnerMinimum(governor);
  const growth = watchGrowth(governor, () => {
    filled.reader()?.close();
  });
  const failed = await h.crash().then(
    () => null,
    (error: unknown) => error,
  );
  expect(failed, "the open completes").toBeNull();
  try {
    expect(filled.reader(), "a temporary reader held the room").not.toBeNull();
    expect(growth.blocked, "the replay waited for it").toBeGreaterThan(0);
    await allRows(h, tables);
  } finally {
    await h.close();
  }
  expect(governor.usedBytes).toBe(0);
}, 60_000);

it("refuses an open whose replay can never fit the cap (53200), then opens at a larger cap", async () => {
  const { h, tables } = await tail();
  // Room for the write owner's minimum, not for thirty-two 1 MiB rows beside it.
  const small = freshGovernor(h, SMALL_CAP);
  const refused = await h.crash().then(
    () => null,
    (error: unknown) => error,
  );
  expect(refused).toBeInstanceOf(ReplayMemoryError);
  expect(refused).toMatchObject({ sqlState: "53200", code: "MINNOW_MEMORY_ADMISSION" });
  expect((refused as Error).message).toMatch(
    /^Opening this database needs at least [\d.]+ MiB to replay its recently written rows, more than its memory cap allows for them \([\d.]+ MiB\)\. The data is intact; open it with a larger memory cap\.$/,
  );
  // What it needs is more than what it was measured against (review of #70: it said less).
  const { bytes, capacity } = refused as ReplayMemoryError;
  expect(bytes).toBeGreaterThan(capacity);
  expect(small.usedBytes, "the refused open holds nothing").toBe(0);
  const large = freshGovernor(h);
  try {
    await h.open();
    await allRows(h, tables);
  } finally {
    await h.close();
  }
  expect(large.usedBytes).toBe(0);
}, 60_000);

it("cancels an open waiting in replay; the next open replays every row", async () => {
  const { h, tables } = await tail();
  const governor = freshGovernor(h);
  const controller = new AbortController();
  h.signal = controller.signal;
  const filled = fillAfterOwnerMinimum(governor);
  const growth = watchGrowth(governor, () => {
    controller.abort(new Error("open cancelled while replaying"));
  });
  const cancelled = await h.crash().then(
    () => null,
    (error: unknown) => error,
  );
  expect(growth.blocked, "the replay was waiting").toBeGreaterThan(0);
  expect(cancelled).toBeInstanceOf(Error);
  filled.reader()?.close();
  expect(governor.usedBytes, "the cancelled open holds nothing").toBe(0);
  h.signal = undefined;
  try {
    await h.open();
    await allRows(h, tables);
  } finally {
    await h.close();
  }
  expect(governor.usedBytes).toBe(0);
}, 60_000);
