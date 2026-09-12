import { expect, it } from "vitest";
import { OpfsUncertainOutcomeError, type TableRecord } from "../types.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore, type OpfsBlockStoreOptions } from "./store.js";

/**
 * Leadership handover, keepalives, and declines: the paths where a follower's write used to
 * surface as an uncertain outcome without ever having been in doubt.
 */

function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(condition: () => Promise<boolean> | boolean, what: string): Promise<void> {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    if (await condition()) return;
    await sleep(5);
  }
  throw new Error(`Timed out waiting for ${what}`);
}

function opener(
  shim: MemoryOpfs,
  name: string,
  overrides: Partial<OpfsBlockStoreOptions> = {},
): () => Promise<OpfsBlockStore> {
  return () => OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 200, ...overrides });
}

async function outcome(work: Promise<unknown>): Promise<string> {
  return work.then(
    () => "ok",
    (error: unknown) => (error instanceof Error ? error.name : String(error)),
  );
}

it("serves every follower's write during a foreground handover, none of them uncertain", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "handover");
  const background = await open();
  const foreground = await open();
  const third = await open();
  try {
    for (let index = 0; index < 200; index += 1)
      await background.addTable(table(`seed${String(index)}`));
    await third.getCurrentManifestVersion();
    foreground.setForeground(true);
    const writes: Array<Promise<string>> = [];
    for (let index = 0; index < 5; index += 1) {
      writes.push(outcome(third.addTable(table(`during${String(index)}`))));
      await sleep(2);
    }
    expect(await Promise.all(writes)).toEqual(["ok", "ok", "ok", "ok", "ok"]);
    await waitFor(() => foreground._isLeaderForTests(), "the foreground bidder to take over");
    expect(background._isLeaderForTests()).toBe(false);
    const names = (await foreground.listTables()).map((record) => record.name);
    expect(names.filter((name) => name.startsWith("during")).sort()).toEqual([
      "during0",
      "during1",
      "during2",
      "during3",
      "during4",
    ]);
  } finally {
    background.close();
    foreground.close();
    third.close();
  }
});

it("hands leadership to a tab that is still visible when the leader goes hidden", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "hidden");
  const first = await open();
  const second = await open();
  try {
    await first.addTable(table("seed"));
    first.setForeground(true);
    second.setForeground(true);
    await sleep(50);
    expect(first._isLeaderForTests()).toBe(true);
    first.setForeground(false);
    await waitFor(() => second._isLeaderForTests(), "the visible tab to take over");
    expect(first._isLeaderForTests()).toBe(false);
    await expect(first.listTables()).resolves.toHaveLength(1);
  } finally {
    first.close();
    second.close();
  }
});

it("keeps a follower's slow write alive with keepalives instead of timing it out", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "slow");
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("seed"));
    const release = leader._holdServedMutationsForTests();
    const write = outcome(follower.addTable(table("slow")));
    // Longer than the follower's whole patience: without keepalives this is an uncertain outcome.
    await sleep(1_500);
    release();
    expect(await write).toBe("ok");
    expect(await follower.getTableByName("slow")).toBeDefined();
  } finally {
    leader.close();
    follower.close();
  }
});

it("asks a silent leader whether it is alive before giving a write up as uncertain", async () => {
  const shim = new MemoryOpfs();
  // A short search budget: the second half measures a leader that never answers again.
  const open = opener(shim, "patience", { dispatchBudgetMs: 1_500 });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("seed"));
    leader._suppressKeepaliveForTests();
    const release = leader._holdServedMutationsForTests();
    const write = outcome(follower.addTable(table("patient")));
    // Two full timeouts of silence: the leader answers the pings, so the write waits.
    await sleep(450);
    release();
    expect(await write).toBe("ok");

    // A leader that neither answers nor pings back is gone, and its handles are still held,
    // so nobody else can lead either: bounded patience and a bounded search, then uncertain.
    const hold = leader._holdServedMutationsForTests();
    const doomed = outcome(follower.addTable(table("doomed")));
    await sleep(50);
    const resume = leader._pauseCoordinationForTests();
    const started = Date.now();
    expect(await doomed).toBe(OpfsUncertainOutcomeError.name);
    expect(Date.now() - started).toBeLessThan(8_000);
    resume();
    hold();
  } finally {
    leader.close();
    follower.close();
  }
});

it("waits out an announced hold through a leader's long synchronous step", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "hold");
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("seed"));
    leader._suppressKeepaliveForTests();
    const release = leader._holdServedMutationsForTests();
    const write = outcome(follower.addTable(table("held")));
    await waitFor(
      () => follower._oldestPendingRequestIdForTests() !== undefined,
      "the follower to send its request",
    );
    // What the leader posts right before a checkpoint, from the test's hand.
    const inbox = new BroadcastChannel(`minnowdb-store:hold:${follower._instanceIdForTests()}`);
    const pendingId = follower._oldestPendingRequestIdForTests();
    inbox.postMessage({ kind: "hold", requestId: pendingId, ms: 3_000 });
    inbox.close();
    await sleep(20);
    // The leader is now silent to everything, including pings, for longer than its patience.
    const resume = leader._pauseCoordinationForTests();
    await sleep(1_000);
    resume();
    release();
    expect(await write).toBe("ok");
  } finally {
    leader.close();
    follower.close();
  }
});

it("defers a bid that lands inside the yield cooldown and honors it when the cooldown ends", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "cooldown", { yieldCooldownMs: 600, handoverGraceMs: 30 });
  const background = await open();
  const first = await open();
  const second = await open();
  try {
    await background.addTable(table("seed"));
    first.setForeground(true);
    await waitFor(() => first._isLeaderForTests(), "the first bidder to take over");
    first.close();
    // The background tab is the only connection left that can lead; it does, inside its cooldown.
    await waitFor(async () => {
      await background.listTables();
      return background._isLeaderForTests();
    }, "the background tab to lead again");
    second.setForeground(true);
    await sleep(150);
    expect(background._isLeaderForTests()).toBe(true);
    expect(second._isLeaderForTests()).toBe(false);
    await waitFor(() => second._isLeaderForTests(), "the deferred bid to be honored");
    expect(background._isLeaderForTests()).toBe(false);
  } finally {
    background.close();
    second.close();
  }
});

it("lets a hidden idle leader go so an awake tab can lead", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "idle", { hiddenIdleReleaseMs: 100 });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("seed"));
    await follower.listTables();
    leader.setForeground(false);
    await waitFor(() => !leader._isLeaderForTests(), "the hidden idle leader to release");
    await follower.addTable(table("after"));
    expect(follower._isLeaderForTests()).toBe(true);
    // Its own next operation elects again when nobody else took the handles.
    follower.close();
    await waitFor(async () => {
      await leader.listTables();
      return leader._isLeaderForTests();
    }, "the hidden tab to lead again on demand");
    expect((await leader.listTables()).map((record) => record.name)).toEqual(["after", "seed"]);
  } finally {
    leader.close();
  }
});

it("declines a write that reaches a closing leader so it runs once on the next one", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "closing");
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("seed"));
    const release = leader._holdServedMutationsForTests();
    const write = outcome(follower.addTable(table("moved")));
    await waitFor(
      () => leader._residentStateForTests().inFlightMutations === 1,
      "the leader to admit the follower's write",
    );
    leader.close();
    release();
    expect(await write).toBe("ok");
    expect(follower._isLeaderForTests()).toBe(true);
    expect((await follower.listTables()).map((record) => record.name)).toEqual(["moved", "seed"]);
  } finally {
    follower.close();
  }
});

it("reports a failed background checkpoint through onDiagnostic", async () => {
  const shim = new MemoryOpfs();
  const reports: Array<{ error: unknown; context: string }> = [];
  const store = await OpfsBlockStore.open({
    name: "checkpoint",
    root: shim.root,
    checkpointEntries: 2,
    onDiagnostic: (error, context) => reports.push({ error, context }),
  });
  try {
    await store.addTable(table("one"));
    shim.setWriteFault((path) => {
      if (path.includes("checkpoint-")) throw new DOMException("no room", "QuotaExceededError");
    });
    await store.addTable(table("two"));
    await store.addTable(table("three"));
    await waitFor(() => reports.length > 0, "the checkpoint failure to be reported");
    expect(reports[0]?.context).toBe("opfs checkpoint");
    expect(reports[0]?.error).toBeInstanceOf(DOMException);
    shim.setWriteFault(null);
    expect((await store.listTables()).map((record) => record.name)).toEqual([
      "one",
      "three",
      "two",
    ]);
  } finally {
    store.close();
  }
});

it("carries a served request through a checkpoint and answers its re-send after a crash", async () => {
  const shim = new MemoryOpfs();
  const open = (): Promise<OpfsBlockStore> =>
    OpfsBlockStore.open({
      name: "ledger",
      root: shim.root,
      rpcTimeoutMs: 200,
      checkpointEntries: 1,
    });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("counter"));
    leader._dropNextRpcResultForTests();
    const pending = outcome(follower.reserveRowIds("table-counter", 5));
    await waitFor(
      () => leader._residentStateForTests().inFlightMutations === 0,
      "the leader to serve the request",
    );
    // A checkpoint every entry: the served request now lives in the checkpoint's ledger.
    await sleep(100);
    leader._crashForTests();
    const recovered = await open();
    expect(await pending).toBe("ok");
    expect(await recovered.reserveRowIds("table-counter", 5)).toEqual({
      start: 6n,
      endExclusive: 11n,
    });
    recovered.close();
  } finally {
    follower.close();
  }
});

it("withholds a served result too large to retain, so its re-send is uncertain but never repeated", async () => {
  const shim = new MemoryOpfs();
  const open = (): Promise<OpfsBlockStore> =>
    OpfsBlockStore.open({
      name: "withheld",
      root: shim.root,
      rpcTimeoutMs: 200,
      servedLedgerResultBytes: 1,
    });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("counter"));
    leader._dropNextRpcResultForTests();
    const pending = outcome(follower.reserveRowIds("table-counter", 5));
    // Served, and remembered by the leader that served it: the crash must come after that.
    await waitFor(
      () => leader._residentStateForTests().dedupeEntries === 1,
      "the leader to serve the request",
    );
    leader._crashForTests();
    const recovered = await open();
    // The log proves the reservation happened, but not what it answered.
    expect(await pending).toBe(OpfsUncertainOutcomeError.name);
    expect(await recovered.reserveRowIds("table-counter", 5)).toEqual({
      start: 6n,
      endExclusive: 11n,
    });
    recovered.close();
  } finally {
    follower.close();
  }
});

it("answers its own withheld request as uncertain when the requester becomes the leader", async () => {
  const shim = new MemoryOpfs();
  const open = (): Promise<OpfsBlockStore> =>
    OpfsBlockStore.open({
      name: "withheld-self",
      root: shim.root,
      rpcTimeoutMs: 200,
      servedLedgerResultBytes: 1,
    });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("counter"));
    leader._dropNextRpcResultForTests();
    const pending = outcome(follower.reserveRowIds("table-counter", 5));
    await waitFor(
      () => leader._residentStateForTests().dedupeEntries === 1,
      "the leader to serve the request",
    );
    // Nobody else opens the database: the follower elects itself and consults the log it
    // recovered for its own outstanding request.
    leader._crashForTests();
    expect(await pending).toBe(OpfsUncertainOutcomeError.name);
    expect(follower._isLeaderForTests()).toBe(true);
    expect(await follower.reserveRowIds("table-counter", 5)).toEqual({
      start: 6n,
      endExclusive: 11n,
    });
  } finally {
    follower.close();
  }
});

it("keeps a withheld outcome withheld through a checkpoint", async () => {
  const shim = new MemoryOpfs();
  const open = (): Promise<OpfsBlockStore> =>
    OpfsBlockStore.open({
      name: "withheld-checkpoint",
      root: shim.root,
      rpcTimeoutMs: 200,
      checkpointEntries: 1,
      servedLedgerResultBytes: 1,
    });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("counter"));
    leader._dropNextRpcResultForTests();
    const pending = outcome(follower.reserveRowIds("table-counter", 5));
    await waitFor(
      () => leader._residentStateForTests().dedupeEntries === 1,
      "the leader to serve the request",
    );
    await sleep(100);
    leader._crashForTests();
    const recovered = await open();
    expect(await pending).toBe(OpfsUncertainOutcomeError.name);
    expect(await recovered.reserveRowIds("table-counter", 5)).toEqual({
      start: 6n,
      endExclusive: 11n,
    });
    recovered.close();
  } finally {
    follower.close();
  }
});

it("attaches a re-sent request to the execution still in flight rather than the log", async () => {
  const shim = new MemoryOpfs();
  const open = (): Promise<OpfsBlockStore> =>
    OpfsBlockStore.open({ name: "in-flight", root: shim.root, rpcTimeoutMs: 100 });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("counter"));
    const release = leader._holdServedMutationsForTests();
    const pending = outcome(follower.reserveRowIds("table-counter", 5));
    // Well past the follower's patience: it re-sends the same request while the first
    // delivery is still queued behind the hold.
    await sleep(350);
    release();
    expect(await pending).toBe("ok");
    expect(await leader.reserveRowIds("table-counter", 1)).toEqual({
      start: 6n,
      endExclusive: 7n,
    });
  } finally {
    follower.close();
    leader.close();
  }
});

it("reports an outcome as uncertain only when the ledger no longer covers the request", async () => {
  const shim = new MemoryOpfs();
  const open = (): Promise<OpfsBlockStore> =>
    OpfsBlockStore.open({
      name: "coverage",
      root: shim.root,
      rpcTimeoutMs: 200,
      servedLedgerAgeMs: 1,
    });
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("counter"));
    leader._dropNextRpcResultForTests();
    const pending = outcome(follower.reserveRowIds("table-counter", 5));
    await waitFor(
      () => leader._residentStateForTests().inFlightMutations === 0,
      "the leader to serve the first request",
    );
    // Later served requests age the first one out of the ledger: its coverage moves past it.
    await sleep(5);
    await leader.reserveRowIds("table-counter", 1);
    const other = await open();
    await other.reserveRowIds("table-counter", 1);
    await waitFor(
      () => leader._residentStateForTests().inFlightMutations === 0,
      "the second served request",
    );
    leader._crashForTests();
    const recovered = await open();
    expect(await pending).toBe(OpfsUncertainOutcomeError.name);
    expect((await recovered.reserveRowIds("table-counter", 1)).start).toBeGreaterThanOrEqual(8n);
    other.close();
    recovered.close();
  } finally {
    follower.close();
  }
});
