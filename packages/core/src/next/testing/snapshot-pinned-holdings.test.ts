/**
 * A statement's holdings include what only its own snapshot keeps alive, and nothing another
 * live snapshot keeps.
 *
 * A flush publishes a memtable while a reader older than the publication can still read it: its
 * borrowed chunks and key deltas stay until every such reader closes (write/state.ts
 * `ReaderRegistry.holdUntilOlderClose`). A query waiting for memory while its own snapshot is the
 * only one keeping them waited for itself: `SELECT v FROM t WHERE id = 1` at 32 MiB, after two
 * 2.5 MB rows, queued 27.46 MB of fixed buffers forever while 8.55 MB of flushed rows stayed for
 * its own snapshot (statement-sizing PR 2, found by keyed-update-sizing.test.ts).
 *
 * Law (GOVERNOR-WAIT-POLICY point 5, next/memory/README.md): a request is refused (typed 53200)
 * before it waits when it could never be served beside what its own statement keeps held while
 * it waits; what other holders keep is finite, and is waited for.
 * - Only this snapshot: counted (`Snapshot.exclusiveBytes`, `WaitingHoldings.pinned`).
 * - Also another live, older snapshot: not counted; the request waits for that one to close.
 */
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
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
const TYPED = { name: "MemoryAdmissionError", code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" };

afterEach(() => {
  Governor.stallAfterMs = null;
});

it("refuses a read typed when only its own snapshot keeps the rows it waits for", async () => {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(32 * MiB) });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')");
    await core.execute("UPDATE t SET v = $1", ["a".repeat(2_500_000)]);
    // Refused or written: either way the memtable holds the wide rows.
    await core
      .execute("UPDATE t SET v = $1 WHERE id = 1", ["b".repeat(4 * MiB)])
      .catch((error: unknown) => {
        expect(error).toMatchObject(TYPED);
      });
    // The read's reclaim makes the idle writer spill and flush; the flushed rows stay for the
    // read's own snapshot. Decided, never a stall: served, or refused typed.
    const outcome: unknown = await core.execute("SELECT v FROM t WHERE id = 1").then(
      () => null,
      (error: unknown) => error,
    );
    if (outcome !== null) expect(outcome).toMatchObject(TYPED);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    await core.close();
  }
}, 60_000);

it("waits for an older snapshot that alone keeps flushed rows, then serves the read", async () => {
  Governor.stallAfterMs = 5_000;
  // Memtables of two rows: the third row freezes the first two and starts their flush.
  const core = new StorageCore(2);
  const governor = new Governor(32 * MiB);
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    // 3 MB rows: a read by key is sized to the row it fetches (#52), so the new read below needs
    // more than is free beside what the older snapshot keeps only with rows this wide.
    await core.execute("INSERT INTO t VALUES (1, $1)", ["a".repeat(3_000_000)]);
    await core.execute("INSERT INTO t VALUES (2, $1)", ["b".repeat(3_000_000)]);
    // Another connection reads both rows and holds its result undelivered: its snapshot is
    // older than the flush below, and keeps the published memtable's borrowed chunks.
    const lowered = core.lowerer.lower("SELECT id, v FROM t", core.context());
    if (lowered.kind !== "query") throw new Error("query plan required");
    const other = core.start(core.executor.prepare(lowered.plan));
    await new Promise((resolve) => setTimeout(resolve, 100));
    await core.execute("INSERT INTO t VALUES (3, 'z')");
    await core.harness.path.quiesce();
    // A new read (its snapshot after the publication) needs memory the older one keeps: it
    // waits, never refused on that account.
    const read = core.execute("SELECT v FROM t WHERE id = 1").then(
      (rows) => rows,
      (error: unknown) => error,
    );
    expect(await within(read, 500), "it waits while the older snapshot is open").toBe(PENDING);
    for (let frame = await other.execution.results.next(); frame !== null;) {
      frame.release();
      frame = await other.execution.results.next();
    }
    await other.execution.delivered;
    other.release();
    const rows = await read;
    if (!(typeof rows === "object" && rows !== null && "batches" in rows))
      throw new Error(`the read is served once the older snapshot closes: ${String(rows)}`);
    expect(
      oracleRows(rows as Parameters<typeof oracleRows>[0])[0]?.v === "a".repeat(3_000_000),
    ).toBe(true);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    await core.close();
  }
}, 60_000);

it("counts a snapshot's own pin of a superseded state (Astra, review of #56)", async () => {
  // The snapshot pins the state after `t`; `u` supersedes it. Its root, its space pin and the
  // group directories only it keeps stay until it closes: a request of what it reported
  // servable waited forever when they counted nothing.
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  const governor = new Governor(32 * MiB);
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  const cancel = new AbortController();
  let snapshot: ReturnType<typeof core.harness.path.pin> | null = null;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY)");
    snapshot = core.harness.path.pin();
    await core.execute("CREATE TABLE u(id INT PRIMARY KEY)");
    await core.harness.path.quiesce();
    const pinned = snapshot;
    const holdings = () => ({ grants: [], pinned: () => pinned.exclusiveBytes() });
    const servable = governor.servableNow(holdings());
    const admit = (bytes: number) =>
      governor
        .admitGroup({
          label: "snapshot request",
          grants: [{ label: "snapshot request", minimumBytes: bytes, spillable: false }],
          holdings,
          signal: cancel.signal,
        })
        .then(
          (grants) => {
            for (const grant of grants) grant.close();
            return "served" as const;
          },
          (error: unknown) => error,
        );
    expect(await within(admit(servable), 1_500), "what it reports is served").toBe("served");
    expect(await within(admit(servable + 1), 1_500), "one byte more is refused").toMatchObject(
      TYPED,
    );
    expect(pinned.exclusiveBytes(), "its superseded state is its own").toBeGreaterThan(0);
  } finally {
    cancel.abort();
    snapshot?.release();
    await core.close();
  }
}, 60_000);

it("leaves out a superseded state another snapshot also pins, until that one closes", async () => {
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(32 * MiB) });
  await core.open();
  const pins: Array<ReturnType<typeof core.harness.path.pin>> = [];
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY)");
    pins.push(core.harness.path.pin(), core.harness.path.pin());
    await core.execute("CREATE TABLE u(id INT PRIMARY KEY)");
    await core.harness.path.quiesce();
    const [first, second] = pins;
    if (first === undefined || second === undefined) throw new Error("two pins");
    expect(first.exclusiveBytes(), "shared: the other keeps it too").toBe(0);
    expect(second.exclusiveBytes()).toBe(0);
    second.release();
    expect(first.exclusiveBytes(), "now its own").toBeGreaterThan(0);
  } finally {
    for (const pin of pins) pin.release();
    await core.close();
  }
}, 60_000);
