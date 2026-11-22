/**
 * Write index range cursor admission classes (GOVERNOR-WAIT-POLICY point 6, the holder-chain
 * invariant). Laws:
 * - a read snapshot's cursor waits as a new admission, and only while neither it nor its caller
 *   holds governor credit (readers are not holders); a caller that holds credit funds its cursor
 *   with its own admission, as the ordered scan does, so its cursor never waits mid-query;
 * - a second cursor of a view whose first cursor owns a grant raises the invariant rather than
 *   waiting behind a request only that grant's release could serve;
 * - a statement view's cursor waits as holder growth in the writer chain;
 * - holder priority has no aging: a waiting reader is served once the writer's holdings become
 *   reclaimable (its rows freed, its transaction ended), not merely when its statement ends.
 */
import { expect, it, vi } from "vitest";
import {
  HolderChainInvariantError,
  type AdmitOptions,
  type Grant,
  type NativeGrantGroupOptions,
} from "../memory/governor.js";
import { createExecutor } from "../exec/create.js";
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

it("admits a read cursor as a new admission and a statement view's as writer-chain growth", async () => {
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
    // A read snapshot, outside any transaction.
    let held = await pressure(h, p);
    const view = p.pin();
    const reader = view.index(t, pk).range(ALL, "asc");
    const read = Promise.resolve(reader.next(out));
    await yieldTask();
    expect(admissions, "the read cursor waits at admission").toHaveLength(1);
    expect(admissions[0]?.holdsCredit, "a reader is not a holder").toBeUndefined();
    held.close();
    expect(await read).toBe(1);
    reader.close();
    view.release();
    // A statement view, inside a writing transaction.
    const tx = await p.begin();
    const statement = tx.statement({ rules });
    held = await pressure(h, p);
    const cursor = statement.view.index(t, pk).range(ALL, "asc");
    const inside = Promise.resolve(cursor.next(out));
    await yieldTask();
    expect(admissions, "the statement view's cursor waits at admission").toHaveLength(2);
    expect(admissions[1]?.holdsCredit, "writer-chain holder growth").toBe(credits.holderChain);
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

it("serves a waiting read cursor once the writer's holdings become reclaimable, not at statement end", async () => {
  const { h, p, t, credits } = await opened();
  const holders: number[] = [];
  const admitGroup = h.governor.admitGroup.bind(h.governor);
  const spy = vi
    .spyOn(h.governor, "admitGroup")
    .mockImplementation((options: NativeGrantGroupOptions) => {
      if (options.holdsCredit === credits.holderChain)
        holders.push(options.grants.reduce((sum, part) => sum + part.minimumBytes, 0));
      return admitGroup(options);
    });
  const pk = p.catalog.table(t)?.primaryKey ?? 0;
  const out = new Float64Array(1);
  const tx = await p.begin();
  const statement = tx.statement({ rules });
  // No dormant forecasts: a growth that gave one back before queueing would serve the reader
  // early (forecasts are revocable), which is allowed but would not exercise holder priority.
  credits.reclaimIdleForecasts();
  const held = await pressure(h, p);
  const view = p.pin();
  const reader = view.index(t, pk).range(ALL, "asc");
  let settled = false;
  const read = Promise.resolve(reader.next(out)).finally(() => {
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
    reader.close();
    view.release();
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
  const diagnostic = createExecutor({ keyOrder: true });
  await core.execute("CREATE TABLE t(id INT PRIMARY KEY, n INT)");
  await core.execute("INSERT INTO t SELECT g, g FROM generate_series(1, 2305) g");
  await core.harness.path.quiesce();
  type PhysicalPlan = Parameters<typeof diagnostic.prepare>[0];
  core.executor.prepare = (plan: PhysicalPlan) => {
    interface Node {
      op: string;
      input: Node;
      id: number;
      tableId: number;
      columns: number[];
    }
    const root = plan.root as unknown as Node;
    const project = root.input,
      scan = project.input;
    if (root.op !== "sort" || project.op !== "project" || scan.op !== "scan")
      throw new Error(`unexpected shape ${root.op}`);
    const indexId = core.harness.path.catalog.table(scan.tableId)?.primaryKey ?? 0;
    const range = {
      id: scan.id,
      op: "indexRange",
      output: (scan as unknown as { output: unknown }).output,
      tableId: scan.tableId,
      indexId,
      lower: null,
      upper: null,
      descending: false,
      columns: scan.columns,
      emitLocator: false,
    };
    return diagnostic.prepare({
      ...plan,
      root: { ...root, input: { ...project, input: range } },
    } as unknown as PhysicalPlan);
  };
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

it("refuses a second read cursor's wait while its view's first cursor owns a grant", async () => {
  // Review of #41 (Astra): the first cursor's grant is the caller's credit. A second cursor that
  // waited as a new admission behind a request only that grant's release could serve would never
  // be served; it raises the invariant instead, before anything queues.
  const { h, p, t } = await opened();
  (await pressure(h, p)).close();
  const view = p.pin(),
    pk = p.catalog.table(t)?.primaryKey ?? 0;
  const first = view.index(t, pk).range(ALL, "asc");
  const second = view.index(t, pk).range(ALL, "asc");
  const out = new Float64Array(1),
    stop = new AbortController();
  expect(await first.next(out)).toBe(1);
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
    expect((outcome as Error).message).toContain("its caller holds governor credit");
  } finally {
    stop.abort();
    (await head)?.close();
    first.close();
    second.close();
    view.release();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});
