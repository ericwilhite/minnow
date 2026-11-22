/**
 * GOVERNOR-WAIT-POLICY point 6, checked: holder priority stays deadlock-free because no holder
 * outside the writer chain grows while a writing transaction is open. The governor enforces it
 * (`HolderChain`, `HolderChainInvariantError`), raising before anything queues.
 *
 * Boundary pinned here, from the review of writer floors steps 2-3: a native INSERT with an AFTER
 * trigger reaches its commit, and a foreign holder then asks for growth that fits only once the
 * writer's rows are released. Under FIFO among holders, a commit funding its window queued behind
 * that request, which waited for the writer's rows, and neither moved until cancellation. No real
 * path issues such a request (the policy's audit); the synthetic one is refused as an invariant
 * error, so the cycle cannot form, and the commit runs on. Autocommit and a two-statement
 * transaction (the foreign request arrives at its commit).
 *
 * Also checked: holder growth naming no open chain of the governor is refused; an owner chain
 * grows while no writing transaction is open (startup, reopen, import); a second writer chain on
 * one governor is refused.
 */
import { expect, it, vi } from "vitest";
import {
  Governor,
  holderChain,
  HolderChainInvariantError,
  type Grant,
  type HolderChain,
} from "../memory/governor.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { WriteCredits } from "../write/credits.js";
import { publicOwner } from "./public-owner.test.fixture.js";

/** `promise`'s value, or `"stuck"` after `ms`: a bounded wait fails by assertion, not timeout. */
function within<T>(promise: Promise<T>, ms: number): Promise<T | "stuck"> {
  return Promise.race([
    promise,
    new Promise<"stuck">((resolve) => setTimeout(() => resolve("stuck"), ms)),
  ]);
}

it.each([
  ["autocommit", false],
  ["two-statement transaction", true],
] as const)(
  "refuses foreign holder growth while a writing transaction is open (%s)",
  async (_name, multi) => {
    const owner = await publicOwner({ capBytes: 32 << 20 });
    const governor = owner.core.harness.governor;
    const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
    const stop = new AbortController();
    let foreign: Promise<unknown> | null = null;
    let chain: HolderChain | null = null;
    // The test's own holdings, released first in cleanup whatever failed: a stalled request
    // must never keep the commit (or the close) waiting on them.
    let idle: Grant | null = null;
    let run: Promise<void> | null = null;
    // eslint-disable-next-line @typescript-eslint/unbound-method -- called with its receiver
    const original = WriteCredits.prototype.prepareCommit;
    const prepareCommit = vi.spyOn(WriteCredits.prototype, "prepareCommit");
    try {
      for (const sql of [
        "CREATE TABLE t(id INT PRIMARY KEY, n INT)",
        "CREATE TABLE log(id INT, n INT)",
        "CREATE TRIGGER r AFTER INSERT ON t BEGIN INSERT INTO log VALUES(NEW.id, NEW.n); END",
      ])
        await db.execute(sql);
      // Outside any writing transaction an owner chain grows as a holder (startup, import).
      chain = holderChain(governor, "foreign holder");
      idle = await governor.admit({
        label: "owner growth between transactions",
        minimumBytes: 1 << 20,
        spillable: false,
        holdsCredit: chain,
      });
      let armed = !multi;
      prepareCommit.mockImplementation(function (this: WriteCredits, signal) {
        if (armed && foreign === null && chain !== null) {
          // The review's request: fits beside the foreign holder's own 1 MiB only once the
          // writer's rows (counted, not releasable before the commit) are released.
          const want = governor.usableCapacity() - (idle?.held ?? 0) - 1024;
          foreign = governor
            .admitGroup({
              label: "earlier holder",
              holdsCredit: chain,
              grants: [{ label: "growth", minimumBytes: want, spillable: false }],
              signal: stop.signal,
            })
            .then(
              (grants) => {
                for (const grant of grants) grant.close();
                return "served";
              },
              (error: unknown) => error,
            );
        }
        return original.call(this, signal);
      });
      const sql = "INSERT INTO t SELECT g, g FROM generate_series(1, 100) g";
      let done = false;
      run = (
        multi
          ? db.transaction(async (tx) => {
              await tx.execute(sql);
              await tx.execute("INSERT INTO t VALUES(101, 101)");
              armed = true;
            })
          : db.execute(sql)
      ).then(() => {
        done = true;
      });
      await within(run, 5_000);
      expect(done, "the commit completed: no foreign holder queued ahead of it").toBe(true);
      await run;
      // The foreign request was refused before it queued: the invariant, not a wait.
      const outcome: unknown = await within(
        (foreign as Promise<unknown> | null) ?? Promise.resolve("never made"),
        5_000,
      );
      expect(outcome).toBeInstanceOf(HolderChainInvariantError);
      expect(outcome).toMatchObject({ code: "MINNOW_HOLDER_CHAIN_INVARIANT", sqlState: "XX000" });
      expect((outcome as Error).message).toContain('while the writer chain "write engine"');
      // The commit ran on, and its rows and the trigger's copies are all there.
      expect((await db.query("SELECT count(*) AS n FROM log")).rows()).toEqual([
        { n: multi ? 101 : 100 },
      ]);
      // With the transaction ended, the same owner chain grows again.
      const after = await governor.admit({
        label: "owner growth after the transaction",
        minimumBytes: 1 << 20,
        spillable: false,
        holdsCredit: chain,
      });
      after.close();
    } finally {
      idle?.close();
      stop.abort(new Error("test cleanup"));
      await within((foreign as Promise<unknown> | null) ?? Promise.resolve(), 5_000);
      await within(run ?? Promise.resolve(), 10_000);
      chain?.close();
      prepareCommit.mockRestore();
      await db.close();
      await owner.close();
    }
    expect(governor.usedBytes).toBe(0);
  },
  30_000,
);

it("refuses holder growth that names no open chain of the governor", async () => {
  const governor = new Governor(8 << 20);
  const other = new Governor(8 << 20);
  const foreign = holderChain(other, "another governor's chain");
  const closed = holderChain(governor, "closed chain");
  closed.close();
  const forged: HolderChain = {
    label: "forged",
    kind: "writer",
    transactionOpen: false,
    close: () => undefined,
  };
  for (const chain of [foreign, closed, forged]) {
    await expect(
      governor.admit({ label: "holder", minimumBytes: 1, spillable: false, holdsCredit: chain }),
    ).rejects.toMatchObject({ code: "MINNOW_HOLDER_CHAIN_INVARIANT" });
    await expect(
      governor.admitGroup({
        label: "holder group",
        holdsCredit: chain,
        grants: [{ label: "part", minimumBytes: 1, spillable: false }],
      }),
    ).rejects.toThrow(/names no open holder chain/u);
  }
  foreign.close();
  expect(governor.usedBytes).toBe(0);
});

it("opens at most one writer chain per governor", () => {
  const governor = new Governor(8 << 20);
  const writer = governor.openHolderChain("write engine", "writer");
  expect(() => governor.openHolderChain("second writer", "writer")).toThrow(
    HolderChainInvariantError,
  );
  writer.close();
  governor.openHolderChain("next writer", "writer").close();
});
