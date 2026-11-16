/** inputView must remain live at every locator use, including after repeat-tracker I/O. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_PREFIX_TARGET_REACH } from "../../testing/reach.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { batch, ints, locators, rules } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
it.each(seedsFor("next-trigger-prefix-targets", [0xb5cc]))(
  "refuses expired pins after async repeat tracking (seed %i)",
  async (seed) => {
    const reach = createReach("prefix repeat wait", TRIGGER_PREFIX_TARGET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 8 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (n, published, remove, final) => {
          reach.beginHistory();
          const core = await new StorageCore(published ? 1 : undefined).open();
          try {
            await core.execute("CREATE TABLE pin_target(id INTEGER PRIMARY KEY,v INTEGER)");
            const table = core.harness.path.catalog.tableByName("pin_target");
            if (table?.primaryKey == null) throw new Error("table");
            const ids = Array.from({ length: n }, (_, k) => k + 1);
            await core.write((s) => s.table(table.id).insert(batch(ints(ids), ints(ids)), null));
            if (published) {
              await core.harness.path.recycleLog();
              await core.reopen();
            }
            let resume!: () => void,
              entered!: () => void,
              calls = 0;
            const gate = new Promise<void>((resolve) => {
                resume = resolve;
              }),
              started = new Promise<void>((resolve) => {
                entered = resolve;
              });
            const tx = await core.harness.path.begin();
            const statement = tx.statement({
              rules,
              targets: {
                testAndSet: () => {
                  if (calls++ === (final ? n - 1 : 0)) {
                    entered();
                    return gate.then(() => false);
                  }
                  return false;
                },
              },
            });
            const pin = statement.readOwn(),
              keys = new KeyBatch({ allocate: () => true, free: () => undefined });
            try {
              encodeKeyBatch(
                "equality",
                [{ vector: ints(ids), direction: "asc", nulls: "first" }],
                { length: n, selection: null },
                keys,
              );
              const out = { probes: new Int32Array(n), locators: new Float64Array(n), count: 0 };
              await pin.index(table.id, table.primaryKey).probe(keys, out);
              expect(out.count).toBe(n);
              const values = Array.from(out.locators);
              const writer = statement.table(table.id, {
                inputView: pin,
                assigned: remove ? [] : [table.columns[1]?.id ?? 0],
              });
              const pending = remove
                ? writer.delete(batch(locators(values, table.id)), null)
                : writer.update(
                    batch(locators(values, table.id), ints(ids.map((id) => id + 10))),
                    null,
                  );
              if (calls === 0) await pending;
              expect(calls, "admitted tracker participates before primary writes").toBe(
                final ? n : 1,
              );
              await started;
              pin.release();
              resume();
              await expect(pending).rejects.toThrow(/released/);
              reach.reach("expired during wait");
              if (n === 1) reach.reach("one-row batch");
              reach.reach(
                final || n === 1 ? "suspended on final row" : "suspended before final row",
              );
              reach.reach(remove ? "DELETE" : "UPDATE");
              reach.reach(published ? "published" : "resident");
            } finally {
              resume();
              pin.release();
              keys.close();
              statement.rollback();
              tx.rollback();
            }
            const rows = await core.execute("SELECT * FROM pin_target ORDER BY id");
            if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("rows");
            expect(oracleRows(rows)).toEqual(ids.map((id) => ({ id, v: id })));
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("no primary effect");
          } finally {
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [1, false, false, true],
          [1, true, true, false],
          [2, false, false, true],
          [3, true, true, true],
          [2, false, true, false],
          [3, true, false, false],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
