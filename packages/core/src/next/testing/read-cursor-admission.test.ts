/**
 * Write index range cursor admission classes (GOVERNOR-WAIT-POLICY point 6, the holder-chain
 * invariant). Laws:
 * - a read snapshot's cursor waits as a new admission, holding nothing (readers are not holders);
 * - a statement view's cursor waits as holder growth in the writer chain;
 * - holder priority never starves a reader: a read cursor waiting at admission while the writer
 *   keeps growing as a holder is served once the writer's statement ends, from what the writer
 *   returns, with no other memory released.
 */
import { expect, it, vi } from "vitest";
import type { AdmitOptions, Grant, NativeGrantGroupOptions } from "../memory/governor.js";
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

it("serves a waiting read cursor once the writer's statement ends, however long it grows", async () => {
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
    // The statement ends: its rows go, and nothing else is released.
    credits.rows.free(grown);
    grown = 0;
    await statement.end();
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
