/** A rollback must not revive a memtable whose committed prefix is being published (R16.3). */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FLUSH_ROLLBACK_REACH } from "../../testing/reach.js";
import { UniqueViolationError } from "../access/modify.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  ints,
  INT,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";
const reach = createReach("rollback during flush publication", FLUSH_ROLLBACK_REACH);
for (const seed of seedsFor("flush-rollback-publication", [1621, 1622, 1623, 1624]))
  it(`keeps the flushing prefix immutable after rollback (${String(seed)})`, async () => {
    reach.beginHistory();
    let armed = false,
      enter!: () => void,
      resume!: () => void;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const held = new Promise<void>((resolve) => {
      resume = resolve;
    });
    const capacity = 16 + (seed % 8);
    const h = new Harness("strict", "memory", capacity, undefined, {
      beforeFlushPublish: async () => {
        if (armed) {
          armed = false;
          enter();
          await held;
        }
      },
    });
    try {
      const p = await h.open();
      let table = 0;
      await autocommit(p, async (s) => {
        table =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false)]),
            })
          ).id ?? 0;
      });
      const original = Array.from({ length: capacity - 2 }, (_, k) => k);
      await autocommit(p, (s) => s.table(table).insert(batch(ints(original)), null));
      armed = true;
      const tx = await p.begin(),
        s = tx.statement({ rules });
      await s.table(table).insert(batch(ints([capacity, 0, capacity + 1])), null);
      await entered;
      reach.reach("flush held before publication");
      await expect(s.end()).rejects.toThrow(UniqueViolationError);
      tx.rollback();
      reach.reach("rollback removes the new tail");
      await autocommit(p, (stmt) => stmt.table(table).insert(batch(ints([capacity + 2])), null));
      resume();
      await p.quiesce();
      expect(p.failure).toBeNull();
      for (let open = 0; open < 2; open++) {
        const path = open === 0 ? p : await h.reopen(),
          view = path.pin();
        try {
          expect(
            (await rowsOf(view, table)).map((row) => row[0]).sort((a, b) => Number(a) - Number(b)),
          ).toEqual([...original, capacity + 2]);
        } finally {
          view.release();
        }
      }
      reach.reach("later committed row survives publication and reopen");
      await h.space.audit();
    } finally {
      resume();
      await h.close();
    }
  });
afterAll(() => reach.assertReached());
