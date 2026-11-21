/**
 * A writing transaction's commit never waits while its rows are counted (GOVERNOR-WAIT-POLICY: no
 * hold-and-wait, including counted held bytes under FIFO). Reproducer adopted from the review of
 * writer floors steps 2-3: a native INSERT with an AFTER trigger reaches its commit; an earlier
 * holder then asks for growth that fits only once the writer's rows are released. A commit that
 * funded its window at commit time queued behind that holder, which waited for the writer's
 * rows: neither moved until cancellation. The commit window is funded with the transaction's
 * first write, while it holds nothing, and kept until the transaction ends; the commit then never
 * waits. Autocommit and a two-statement transaction (the holder arrives at its commit).
 *
 * Not checked here (both also on main, reported separately): the earlier holder is not served
 * after the commit, because the committed rows stay in the memtable and are not flushed for a
 * holder's growth; and a holder that arrives between the two statements leaves the second one a
 * ParseBudgetError instead of a wait or 53200.
 */
import { expect, it, vi } from "vitest";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { WriteCredits } from "../write/credits.js";
import { publicOwner } from "./public-owner.test.fixture.js";

it.each([
  ["autocommit", false],
  ["two-statement transaction", true],
] as const)(
  "commits ahead of an earlier holder that needs the transaction's rows (%s)",
  async (_name, multi) => {
    const owner = await publicOwner({ capBytes: 32 << 20 });
    const governor = owner.core.harness.governor;
    const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
    const stop = new AbortController();
    let earlier: Promise<unknown> | null = null;
    let base: Awaited<ReturnType<typeof governor.admit>> | null = null;
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
      base = await governor.admit({
        label: "foreign holder",
        minimumBytes: 1 << 20,
        spillable: false,
      });
      const held = base;
      let armed = !multi;
      let reached!: () => void;
      const atCommit = new Promise<void>((resolve) => (reached = resolve));
      prepareCommit.mockImplementation(function (this: WriteCredits, signal) {
        if (armed && earlier === null) {
          // Fits the usable capacity beside the holder's own 1 MiB only once the writer's rows
          // (counted, not releasable before the commit) are released.
          const want = governor.usableCapacity() - held.held - 1024;
          earlier = governor
            .admitGroup({
              label: "earlier holder",
              holdsCredit: true,
              signal: stop.signal,
              grants: [{ label: "growth", minimumBytes: want, spillable: false }],
            })
            .then(
              (grants) => {
                for (const grant of grants) grant.close();
                return "served";
              },
              (error: unknown) => error,
            );
          reached();
        }
        return original.call(this, signal);
      });
      let done = false;
      const sql = "INSERT INTO t SELECT g, g FROM generate_series(1, 100) g";
      const run = (
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
      await atCommit;
      // The commit runs on: it neither waits for the earlier holder nor needs cancelling.
      await Promise.race([run, new Promise((resolve) => setTimeout(resolve, 2_000))]);
      expect(done, "the commit completed while the earlier holder waited").toBe(true);
      await run;
      // The holder is not served here (see above); cancel it before reading the rows.
      stop.abort(new Error("earlier holder cancelled"));
      await earlier;
      expect((await db.query("SELECT count(*) AS n FROM log")).rows()).toEqual([
        { n: multi ? 101 : 100 },
      ]);
    } finally {
      stop.abort(new Error("test cleanup"));
      await earlier;
      prepareCommit.mockRestore();
      base?.close();
      await db.close();
      await owner.close();
    }
    expect(governor.usedBytes).toBe(0);
  },
  30_000,
);
