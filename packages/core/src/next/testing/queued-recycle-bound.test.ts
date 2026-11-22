/**
 * Work queued behind a statement's writer turn frees nothing for that statement, so it does not
 * suspend the statement's bound (GOVERNOR-WAIT-POLICY point 5; `LentHoldings.retained`).
 *
 * Review #51 (Astra), on its head: sequence writes start an automatic log recycle while a
 * statement holds the turn; the recycle waits for the writer slot, which the statement holds.
 * Counted as the writer's own work, it made the writer's holdings count nothing, so a request
 * that can never fit beside them waited forever (the recycle waits for the statement, the
 * statement for memory its own writer keeps). A recycle counts only once it holds the slot.
 */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import {
  autocommit,
  batch,
  BIG,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
} from "../write/test-support.js";

it("refuses a request that never fits beside the writer while a recycle waits for the turn", async () => {
  const h = new Harness("strict", "memory", 16, undefined, { logRecycleBytes: 1024 });
  const governor = new Governor(32 << 20);
  Object.defineProperty(h, "governor", { value: governor });
  const path = await h.open();
  let recycle: Promise<void> | null = null;
  try {
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1])), null));
    await autocommit(path, async (s) => {
      await s.changeCatalog({
        kind: "createTable",
        table: tableInput("seq", [
          { ...column("id", BIG, false), identity: { start: 1n, increment: 1n } },
        ]),
      });
    });
    await path.quiesce();
    const sequenceId =
      path.catalog.tables.find((table) => table.name === "seq")?.columns[0]?.identitySequence ?? 0;
    const tx = await path.begin();
    const statement = tx.statement({ rules });
    try {
      const lent = statement.writerHoldings();
      if (lent === null) throw new Error("a writable statement holds the writer turn");
      const holdings = { grants: [], lent };
      // Sequence frames reach the recycle bound: a recycle starts and waits for the turn.
      for (let i = 0; i < 100 && !path.maintenanceState.recycling; i++)
        await path.sequences.set(sequenceId, BigInt(i + 1), true);
      expect(path.maintenanceState.recycling).toBe(true);
      recycle = path.recycleLog();
      await new Promise((resolve) => setTimeout(resolve, 30));
      // Queued behind the turn, it does not suspend what the writer keeps: its holdings still
      // count, so the bound stays below the usable capacity.
      expect(lent.retained()).toBeGreaterThan(0);
      const bound = governor.servableNow(holdings, true);
      expect(bound).toBeLessThan(governor.usableCapacity(true));
      const cancel = new AbortController();
      const outcome = governor
        .admitGroup({
          label: "statement request",
          includesProgress: true,
          signal: cancel.signal,
          holdings: () => holdings,
          grants: [{ label: "statement request", minimumBytes: bound + 1, spillable: false }],
        })
        .then(
          (grants) => {
            for (const grant of grants) grant.close();
            return "served";
          },
          (error: unknown) => error,
        );
      const decided = await Promise.race([
        outcome,
        new Promise((resolve) => setTimeout(() => resolve("still waiting"), 300)),
      ]);
      cancel.abort(new Error("done"));
      await outcome;
      expect(decided).toMatchObject({ name: "MemoryAdmissionError", sqlState: "53200" });
    } finally {
      statement.rollback();
      tx.rollback();
    }
  } finally {
    await recycle;
    await h.close();
  }
}, 60_000);
