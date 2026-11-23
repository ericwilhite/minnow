/**
 * A rollback never fails for memory: undoing a memtable freeze (or a table the transaction
 * created) republishes the writer's state, whose new pin it charged at rollback time; with the
 * metadata grant full and nothing free, `MetadataBudget.allocate` threw `MetadataPressure` out
 * of the rollback, leaving its undo half applied (found by a merge probe in #56's review).
 * The transaction now reserves that pin when it records the undo (`reserveRollback`), and the
 * rollback republishes on the reserve (`#withRollbackReserve`).
 *
 * Law: PostgreSQL ROLLBACK and ROLLBACK TO SAVEPOINT always complete and restore the state
 * before the undone work; next/memory/README.md: a write path refusal is a typed 53200 before
 * the step it refuses, never a failure while undoing one.
 */
import { expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";
import type { WritePathImpl } from "./engine.js";

const MiB = 1 << 20;

/**
 * No memory for anything new: every idle byte of the writer's accounts is allocated, and the
 * governor grows no grant (whatever reclaim returns). A rollback that needs one more byte fails.
 * Returns the undo.
 */
function fill(path: WritePathImpl): () => void {
  const undo: Array<() => void> = [];
  for (const grant of path.writerHoldings().grants) {
    const idle = grant.held - grant.allocated - grant.loaned;
    if (idle > 0 && grant.allocate(idle)) undo.push(() => grant.free(idle));
  }
  const grow = vi.spyOn(Governor.prototype, "grow").mockReturnValue(false);
  return () => {
    grow.mockRestore();
    for (const step of undo) step();
  };
}

for (const pinned of [false, true])
  for (const mode of ["statement", "savepoint", "transaction"] as const)
    it(`completes a ${mode} rollback that undoes a memtable freeze with no memory free${pinned ? ", a snapshot keeping the replaced state" : ""}`, async () => {
      // Memtables of two rows: the statement's third row freezes one and creates the next.
      const h = new Harness("strict", "memory", 2);
      const governor = new Governor(32 * MiB);
      Object.defineProperty(h, "governor", { value: governor });
      const path = await h.open();
      try {
        let t = 0;
        await autocommit(path, async (s) => {
          t =
            (
              await s.changeCatalog({
                kind: "createTable",
                table: tableInput("t", [column("id", INT, false), column("v", INT)]),
              })
            ).id ?? 0;
        });
        await autocommit(path, (s) => s.table(t).insert(batch(ints([0]), ints([0])), null));
        const tx = await path.begin();
        let release: (() => void) | null = null;
        try {
          if (mode === "savepoint") {
            const first = tx.statement({ rules });
            await first.table(t).insert(batch(ints([1]), ints([1])), null);
            await first.end();
          }
          const savepoint = tx.savepoint();
          const statement = tx.statement({ rules });
          await statement.table(t).insert(batch(ints([2, 3, 4]), ints([2, 3, 4])), null);
          expect(
            tx.undo.some((entry) => entry.kind === "freeze"),
            "a freeze to undo",
          ).toBe(true);
          if (mode !== "statement") await statement.end();
          // A live snapshot keeps the state each republication replaces, so that pin stays
          // (Astra, re-check of #56): every undo needs its own.
          const snapshot = pinned ? path.pin() : null;
          release = fill(path);
          const rollback = () => {
            if (mode === "statement") statement.rollback();
            else if (mode === "savepoint") tx.rollbackTo(savepoint);
            else tx.rollback();
          };
          expect(rollback, "the rollback completes").not.toThrow();
          release();
          release = null;
          snapshot?.release();
          if (mode !== "transaction") {
            const check = tx.statement({ rules });
            expect(await rowsOf(check.view, t), "the state before the undone work").toEqual(
              mode === "savepoint"
                ? [
                    [0, 0],
                    [1, 1],
                  ]
                : [[0, 0]],
            );
            await check.end();
            tx.rollback();
          }
        } finally {
          release?.();
          tx.rollback();
        }
        // The engine stays usable, and the rolled-back rows are gone.
        await autocommit(path, (s) => s.table(t).insert(batch(ints([9]), ints([9])), null));
        const view = path.pin();
        try {
          expect(await rowsOf(view, t)).toEqual([
            [0, 0],
            [9, 9],
          ]);
        } finally {
          view.release();
        }
        expect(path.failure).toBeNull();
      } finally {
        await h.close();
      }
    }, 60_000);

for (const pinned of [false, true])
  it(`completes a statement rollback that undoes a table it created, with no memory free${pinned ? ", a snapshot keeping the replaced state" : ""}`, async () => {
    const h = new Harness("strict", "memory", 2);
    Object.defineProperty(h, "governor", { value: new Governor(32 * MiB) });
    const path = await h.open();
    try {
      const tx = await path.begin();
      let release: (() => void) | null = null;
      try {
        const statement = tx.statement({ rules });
        await statement.changeCatalog({
          kind: "createTable",
          table: tableInput("made", [column("id", INT, false)]),
        });
        const snapshot = pinned ? path.pin() : null;
        release = fill(path);
        expect(() => {
          statement.rollback();
        }, "the rollback completes").not.toThrow();
        release();
        release = null;
        snapshot?.release();
        expect(tx.catalog.tableByName("made"), "the table it created is gone").toBeUndefined();
      } finally {
        release?.();
        tx.rollback();
      }
      // The engine stays usable: the name is free again.
      await autocommit(path, async (s) => {
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("made", [column("id", INT, false)]),
        });
      });
      expect(path.failure).toBeNull();
    } finally {
      await h.close();
    }
  }, 60_000);
