/**
 * Write index range cursor admission classes (GOVERNOR-WAIT-POLICY point 6, the holder-chain
 * invariant; reviews of #41). Laws:
 * - a read snapshot's cursor never queues: a cursor cannot see its caller's grants, so a caller
 *   funds it atomically with its own admission (the ordered scan does); unfunded, it takes only
 *   credit granted at once and otherwise raises before anything queues;
 * - a statement view's cursor waits as holder growth in the writer chain;
 * - holder priority has no aging: a new admission waiting behind the writer's holder growth is
 *   served once the writer's holdings become reclaimable (its rows freed, its transaction
 *   ended), not merely when its statement ends.
 */
import { expect, it, vi } from "vitest";
import {
  HolderChainInvariantError,
  type AdmitOptions,
  type Grant,
  type NativeGrantGroupOptions,
} from "../memory/governor.js";
import { useDiagnosticOrderedScan } from "./key-order-diagnostic.test-support.js";
import { StorageCore } from "./storage-core.js";
import { yieldTask } from "../exec/pipeline.js";
import { WriteCredits } from "../write/credits.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
} from "../write/test-support.js";
import type { WritePathImpl } from "../write/engine.js";

const ALL = { lower: null, upper: null };
const MiB = 1 << 20;

/** `promise`'s value, or `"stuck"` after `ms`: a bounded wait fails by assertion, not timeout. */
function within<T>(promise: Promise<T>, ms: number): Promise<T | "stuck"> {
  return Promise.race([
    promise,
    new Promise<"stuck">((resolve) => setTimeout(() => resolve("stuck"), ms)),
  ]);
}

/** A database with one table of rows, its write credits, and no reclaimable credit left. */
async function opened() {
  const h = new Harness("strict", "memory");
  let credits: WriteCredits | undefined;
  const open = WriteCredits.open.bind(WriteCredits);
  const spy = vi.spyOn(WriteCredits, "open").mockImplementation(async (...args) => {
    credits = await open(...args);
    return credits;
  });
  const p = await h.open();
  spy.mockRestore();
  if (credits === undefined) throw new Error("Missing write credits");
  let t = 0;
  await autocommit(p, async (st) => {
    t =
      (
        await st.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  await autocommit(p, (st) => st.table(t).insert(batch(ints([1, 2, 3])), null));
  await p.quiesce();
  return { h, p, t, credits };
}

/** Takes every byte the governor can grant now, after draining reclaimable credit. */
async function pressure(h: Harness, p: WritePathImpl): Promise<Grant> {
  for (let round = 0; round < 2; round++) {
    const reclaim = new AbortController();
    const drained = h.governor
      .admit({
        label: "drain reclaimable credit",
        minimumBytes: h.governor.usableCapacity(),
        spillable: false,
        signal: reclaim.signal,
      })
      .then(
        (grant) => {
          grant.close();
        },
        () => undefined,
      );
    await yieldTask();
    reclaim.abort();
    await drained;
    await p.quiesce();
  }
  const held = await h.governor.admit({
    label: "foreign pressure",
    minimumBytes: h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes,
    spillable: false,
  });
  expect(held.allocate(held.held)).toBe(true);
  return held;
}

it("never queues a read cursor; a statement view's cursor waits as writer-chain growth", async () => {
  const { h, p, t, credits } = await opened();
  const admissions: AdmitOptions[] = [];
  const admit = h.governor.admit.bind(h.governor);
  const spy = vi.spyOn(h.governor, "admit").mockImplementation((options) => {
    if (options.label === "write index range") admissions.push(options);
    return admit(options);
  });
  const pk = p.catalog.table(t)?.primaryKey ?? 0;
  const out = new Float64Array(1);
  try {
    // A read snapshot, outside any transaction: nothing is free, and it raises without queueing.
    let held = await pressure(h, p);
    const view = p.pin();
    const reader = view.index(t, pk).range(ALL, "asc");
    const read = await within(
      Promise.resolve()
        .then(() => reader.next(out))
        .then(
          () => "served",
          (error: unknown) => error,
        ),
      5_000,
    );
    expect(read, "raised before queueing").toBeInstanceOf(HolderChainInvariantError);
    expect(admissions, "the read cursor never queued").toHaveLength(0);
    reader.close();
    held.close();
    // With credit free at once, the same cursor reads.
    const again = view.index(t, pk).range(ALL, "asc");
    expect(await again.next(out)).toBe(1);
    again.close();
    view.release();
    // A statement view, inside a writing transaction.
    const tx = await p.begin();
    const statement = tx.statement({ rules });
    held = await pressure(h, p);
    const cursor = statement.view.index(t, pk).range(ALL, "asc");
    const inside = Promise.resolve(cursor.next(out));
    await yieldTask();
    expect(admissions, "the statement view's cursor waits at admission").toHaveLength(1);
    expect(admissions[0]?.holdsCredit, "writer-chain holder growth").toBe(credits.holderChain);
    expect(credits.holderChain.kind).toBe("writer");
    expect(credits.holderChain.transactionOpen).toBe(true);
    held.close();
    expect(await inside).toBe(1);
    cursor.close();
    await statement.end();
    tx.rollback();
  } finally {
    spy.mockRestore();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it("serves a waiting new admission once the writer's holdings become reclaimable, not at statement end", async () => {
  const { h, p, credits } = await opened();
  const holders: number[] = [];
  const admitGroup = h.governor.admitGroup.bind(h.governor);
  const spy = vi
    .spyOn(h.governor, "admitGroup")
    .mockImplementation((options: NativeGrantGroupOptions) => {
      if (options.holdsCredit === credits.holderChain)
        holders.push(options.grants.reduce((sum, part) => sum + part.minimumBytes, 0));
      return admitGroup(options);
    });
  const tx = await p.begin();
  const statement = tx.statement({ rules });
  // No dormant forecasts: a growth that gave one back before queueing would serve the reader
  // early (forecasts are revocable), which is allowed but would not exercise holder priority.
  credits.reclaimIdleForecasts();
  const held = await pressure(h, p);
  // A reader's admission (a query holding nothing yet): a new admission, behind holder growth.
  const stop = new AbortController();
  let settled = false;
  const read = h.governor
    .admit({
      label: "reader admission",
      minimumBytes: 64 << 10,
      spillable: false,
      signal: stop.signal,
    })
    .then((grant) => {
      grant.close();
      return 1;
    })
    .finally(() => {
      settled = true;
    });
  let grown = 0;
  try {
    await yieldTask();
    expect(settled, "the reader waits: nothing is free").toBe(false);
    // The writer's statement keeps growing as a holder. Each growth queues ahead of the reader;
    // exactly its bytes come back from the foreign pressure, so holder priority serves the
    // writer and leaves the reader waiting.
    for (let round = 0; round < 3; round++) {
      const before = holders.length;
      const growth = credits.growTo(credits.rows, credits.rows.held + MiB);
      await yieldTask();
      expect(holders.length, "the writer's growth queued as holder growth").toBe(before + 1);
      const need = holders.at(-1) ?? 0;
      held.free(need);
      held.release(need);
      expect(await within(growth, 5_000), "the writer's growth is served").not.toBe("stuck");
      // The statement's rows: live, so no reclaim can hand them to the reader.
      const live = credits.rows.held - credits.rows.allocated;
      expect(credits.rows.allocate(live)).toBe(true);
      grown += live;
      expect(settled, "holder growth is served ahead of the waiting reader").toBe(false);
    }
    // The statement ends while its rows are retained (the transaction is open): holder priority
    // has no aging, so the reader still waits. Pinned behaviour, not a law of progress.
    await statement.end();
    expect(await within(read, 500), "statement end alone, rows retained").toBe("stuck");
    // The writer's holdings become reclaimable: its rows go and the transaction ends, and
    // nothing else is released.
    credits.rows.free(grown);
    grown = 0;
    tx.rollback();
    expect(await within(read, 5_000), "the reader is served from what the writer returned").toBe(1);
  } finally {
    if (grown > 0) credits.rows.free(grown);
    stop.abort();
    await read.catch(() => undefined);
    held.close();
    spy.mockRestore();
    if (tx.state === "open") tx.rollback();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it("funds an ordered scan's cursor with the query, so it never waits mid-query", async () => {
  // The diagnostic ordered-scan plan (review of #41, Astra): an explicit full-range index scan.
  const core = await new StorageCore().open();
  const governor = core.harness.governor;
  await core.execute("CREATE TABLE t(id INT PRIMARY KEY, n INT)");
  await core.execute("INSERT INTO t SELECT g, g FROM generate_series(1, 2305) g");
  await core.harness.path.quiesce();
  useDiagnosticOrderedScan(core);
  const stop = new AbortController();
  let earlier: Promise<Grant | null> | undefined;
  const cursorAdmissions: string[] = [];
  // Once the query holds its admission, a large request queues: it fits only after the query
  // releases. A cursor that admitted itself then would wait behind it, holding the query.
  const queueEarlier = () => {
    earlier ??= governor
      .admit({
        label: "earlier admission",
        minimumBytes: governor.usableCapacity() - (1 << 20),
        spillable: false,
        signal: stop.signal,
      })
      .catch(() => null);
  };
  const tryAdmit = governor.tryAdmit.bind(governor);
  const spyTry = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
    if (options.label === "write index range") cursorAdmissions.push("tryAdmit");
    const granted = tryAdmit(options);
    if (granted !== null && options.label.startsWith("query#")) queueMicrotask(queueEarlier);
    return granted;
  });
  const admitGroup = governor.admitGroup.bind(governor);
  const spyGroup = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
    const granted = admitGroup(options);
    if (options.label.startsWith("query#")) void granted.then(queueEarlier, () => undefined);
    return granted;
  });
  try {
    const query = core.execute("SELECT id, n FROM t ORDER BY id");
    expect(await within(query, 5_000), "the query completes beside the queued admission").not.toBe(
      "stuck",
    );
    expect(earlier, "the large admission queued while the query held its grants").toBeDefined();
    expect(cursorAdmissions, "the cursor never admitted itself").toEqual([]);
  } finally {
    stop.abort();
    (await earlier)?.close();
    spyTry.mockRestore();
    spyGroup.mockRestore();
    await core.close();
  }
});

it("raises at once for a bare read cursor whose caller holds execution credit", async () => {
  // Reviews of #41 (Astra): the caller retains a 2 MiB execution grant (or a first cursor's), and
  // a request that fits only once that grant is released queues. A bare cursor that waited behind
  // it would never be served; it raises instead, before anything queues.
  const { h, p, t } = await opened();
  (await pressure(h, p)).close();
  const view = p.pin(),
    pk = p.catalog.table(t)?.primaryKey ?? 0;
  const caller = await h.governor.admit({
    label: "caller execution grant",
    minimumBytes: 2 * MiB,
    spillable: false,
  });
  expect(caller.allocate(caller.held)).toBe(true);
  const first = view.index(t, pk).range(ALL, "asc");
  const second = view.index(t, pk).range(ALL, "asc");
  const out = new Float64Array(1),
    stop = new AbortController();
  expect(await first.next(out), "credit free at once").toBe(1);
  const head = h.governor
    .admit({
      label: "earlier admission",
      minimumBytes: h.governor.usableCapacity() - (1 << 20),
      spillable: false,
      signal: stop.signal,
    })
    .catch(() => null);
  try {
    await yieldTask();
    const outcome = await within(
      Promise.resolve()
        .then(() => second.next(out))
        .then(
          () => "served",
          (error: unknown) => error,
        ),
      5_000,
    );
    expect(outcome, "raised before waiting").toBeInstanceOf(HolderChainInvariantError);
    expect((outcome as Error).message).toContain("fund the cursor with the caller's admission");
  } finally {
    stop.abort();
    (await head)?.close();
    caller.close();
    first.close();
    second.close();
    view.release();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});
