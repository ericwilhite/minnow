/** Complete contiguous append ranges preserve statement rollback and catalog WAL boundaries. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_ROW_UNDO_REACH } from "../../testing/reach.js";
import { StatementImpl } from "./transaction.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  batch,
  ints,
  rules,
  rowsOf,
} from "./test-support.js";
it.each(seedsFor("next-row-undo-ranges", [0xb5ca]))(
  "coalesces single-row writes without crossing statement/catalog boundaries (seed %i)",
  async (seed) => {
    const reach = createReach("row undo ranges", WRITE_ROW_UNDO_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 18 }),
        fc.boolean(),
        fc.constantFrom("commit", "statement rollback", "transaction rollback"),
        async (n, boundary, mode) => {
          reach.beginHistory();
          const h = new Harness("strict", "sim", 4);
          let p = await h.open();
          let table = 0;
          try {
            await autocommit(p, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            await autocommit(p, (s) => s.table(table).insert(batch(ints([-1])), null));
            const tx = await p.begin(),
              statement = tx.statement({ rules });
            if (!(statement instanceof StatementImpl)) throw new Error("native statement");
            const writer = statement.table(table);
            for (let k = 0; k < n; k++) {
              if (boundary && k === 2)
                await statement.changeCatalog({
                  kind: "createTable",
                  table: tableInput("other", [column("id", INT, false)]),
                });
              await writer.insert(batch(ints([k + 1])), null);
            }
            const ranges = statement.tx.undo.filter(
              (e) => e.kind === "rows" && e.statement === statement.ordinal,
            );
            const memtables = new Set(ranges.map((e) => (e.kind === "rows" ? e.memtable : null)));
            expect(ranges.length).toBeLessThanOrEqual(memtables.size + (boundary ? 1 : 0));
            if (boundary) {
              expect(ranges.length).toBe(memtables.size + 1);
              reach.reach("catalog boundary");
            }
            reach.reach("coalesced append");
            if (n > 4) reach.reach("frozen memtable");
            const descriptor = Object.getOwnPropertyDescriptor(statement.tx, "undo");
            if (!descriptor) throw new Error("native undo descriptor");
            Object.defineProperty(statement.tx, "undo", {
              configurable: true,
              get() {
                throw new Error("existing row scanned undo");
              },
            });
            try {
              expect(statement.inserted(table, 1)).toBe(false);
            } finally {
              Object.defineProperty(statement.tx, "undo", descriptor);
            }
            expect(statement.inserted(table, n + 1)).toBe(true);
            reach.reach("visible row shortcut");
            reach.reach("new row classified");
            if (mode === "statement rollback") {
              statement.rollback();
              const good = tx.statement({ rules });
              await good.table(table).insert(batch(ints([999])), null);
              await good.end();
              const receipt = await tx.commit(null);
              await receipt.durable;
            } else if (mode === "transaction rollback") {
              await statement.end();
              tx.rollback();
            } else {
              await statement.end();
              const receipt = await tx.commit(null);
              await receipt.durable;
            }
            reach.reach(mode);
            p = await h.reopen();
            const view = p.pin();
            try {
              expect(
                (await rowsOf(view, table))
                  .map((row) => row[0])
                  .sort((a, b) => Number(a) - Number(b)),
              ).toEqual(
                mode === "commit"
                  ? [-1, ...Array.from({ length: n }, (_, k) => k + 1)]
                  : mode === "statement rollback"
                    ? [-1, 999]
                    : [-1],
              );
            } finally {
              view.release();
            }
            reach.reach("reopen");
          } finally {
            await h.close();
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [8, true, "commit"],
          [8, true, "statement rollback"],
          [8, false, "transaction rollback"],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
