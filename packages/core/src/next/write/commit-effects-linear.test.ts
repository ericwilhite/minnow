/**
 * Commit encoding reads the undo log a bounded number of times per entry (write/stream-body.ts
 * `effects`). A trigger statement logs one undo entry per root row and then one per body row, so
 * "was this memtable (or table) already emitted?" answered by rescanning earlier entries made a
 * commit quadratic: 42% of a 20,000-row trigger INSERT's CPU. Law: undo reads per entry stay
 * bounded as the log grows, and the committed rows are exactly the transaction's.
 */
import { expect, it } from "vitest";
import { createReach, COMMIT_EFFECTS_LINEAR_REACH } from "../../testing/reach.js";
import type { Undo } from "./transaction.js";
import { probe } from "./crash-sweep.test-support.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rules,
  tableInput,
} from "./test-support.js";

/** Undo reads during one commit of about `entries` single-row writes over two tables. */
async function commitReads(entries: number): Promise<{ reads: number; undo: number }> {
  const h = new Harness("strict", "memory");
  let path = await h.open();
  try {
    const ids: number[] = [];
    for (const name of ["a", "b"])
      await autocommit(path, async (s) => {
        const created = await s.changeCatalog({
          kind: "createTable",
          table: tableInput(name, [column("id", INT, false), column("v", INT)]),
        });
        ids.push(created.id ?? -1);
      });
    const [a, b] = ids;
    if (a === undefined || b === undefined) throw new Error("missing tables");
    const half = entries / 4;
    // Rows the measured transaction deletes, committed first.
    for (const t of [a, b])
      await autocommit(path, (s) =>
        s
          .table(t)
          .insert(
            batch(
              ints(Array.from({ length: half }, (_, k) => k)),
              ints(Array.from({ length: half }, (_, k) => k)),
            ),
            null,
          ),
      );
    const located = new Map<number, number[]>();
    for (const t of [a, b])
      located.set(
        t,
        await probe(
          path,
          t,
          path.catalog.table(t)?.primaryKey ?? 0,
          Array.from({ length: half }, (_, k) => k),
        ),
      );
    const tx = await path.begin();
    const statement = tx.statement({ rules });
    // One undo entry per write, in a trigger statement's order: every write of the root table,
    // then every write its AFTER bodies make to the other table. Each body entry is preceded by
    // all the root's entries, which a rescan for "already emitted" walks past.
    for (const t of [a, b]) {
      for (let k = 0; k < half; k++)
        await statement.table(t).delete(batch(locators([located.get(t)?.[k] ?? -1], t)), null);
      for (let k = 0; k < half; k++)
        await statement.table(t).insert(batch(ints([half + k]), ints([k])), null);
    }
    await statement.end();
    const undo = (tx as unknown as { undo: Undo[] }).undo;
    const logged = undo.length;
    let reads = 0;
    Object.defineProperty(tx, "undo", {
      value: new Proxy(undo, {
        get(target, key, receiver) {
          if (typeof key === "string" && /^\d+$/.test(key)) reads++;
          return Reflect.get(target, key, receiver) as unknown;
        },
      }),
    });
    await (
      await tx.commit(null)
    ).durable;
    path = await h.reopen();
    // Exactly the transaction's rows survive: the deleted keys are gone, the inserted ones remain.
    for (const t of [a, b]) {
      const gone = await probe(path, t, path.catalog.table(t)?.primaryKey ?? 0, [0, half - 1]);
      expect(gone.filter((locator) => locator >= 0)).toEqual([]);
      const kept = await probe(path, t, path.catalog.table(t)?.primaryKey ?? 0, [
        half,
        2 * half - 1,
      ]);
      expect(kept.filter((locator) => locator >= 0)).toHaveLength(2);
    }
    return { reads, undo: logged };
  } finally {
    await h.close();
  }
}

it("encodes a commit with undo reads linear in its undo entries", async () => {
  const reach = createReach("commit effects linear", COMMIT_EFFECTS_LINEAR_REACH);
  reach.beginHistory();
  const small = await commitReads(1_000);
  const large = await commitReads(8_000);
  reach.reach("streamed commit");
  reach.reach("root then body inserts");
  reach.reach("root then body deletes");
  if (large.undo >= 8_000) reach.reach("several thousand undo entries");
  const perEntry = (m: { reads: number; undo: number }) => m.reads / m.undo;
  // A constant number of passes (about 37 reads per entry: the commit's own passes and one
  // forward pass per distinct memtable and table); a rescan per entry reads about undo / 2.
  expect(perEntry(small), JSON.stringify(small)).toBeLessThan(64);
  expect(perEntry(large), JSON.stringify(large)).toBeLessThan(64);
  expect(perEntry(large)).toBeLessThan(perEntry(small) * 1.25 + 1);
  reach.assertReached();
}, 300_000);
