/** A committed DELETE survives reopen and a later staged transaction's rollback. */
import { expect, it } from "vitest";
import { prefixRange } from "../access/key-index.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  ints,
  INT,
  locators,
  rules,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
for (const backend of ["memory", "sim"] as const)
  it(`keeps only the live key 9 after delete, reopen and staged rollback (${backend})`, async () => {
    const h = new Harness("strict", backend, 4);
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([9]), direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      keys,
    );
    let p = await h.open(),
      table = 0;
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
      await autocommit(p, (s) => s.table(table).insert(batch(ints([8, 9, 10, 11])), null));
      const prime = await p.begin();
      await prime
        .statement({ rules })
        .table(table)
        .insert(batch(ints([12])), null);
      prime.rollback();
      await p.quiesce();
      await autocommit(p, async (s) => {
        const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
        await s.view.index(table, s.catalog.table(table)?.primaryKey ?? 0).probe(keys, out);
        expect(out.count).toBe(1);
        await s.table(table).delete(batch(locators([out.locators[0] ?? 0], table)), null);
      });
      p = await h.reopen();
      const tx = await p.begin();
      const s = tx.statement({ rules });
      for (let k = 0; k < 3; k++)
        await s
          .table(table)
          .insert(batch(ints([100 + k * 4, 101 + k * 4, 102 + k * 4, 103 + k * 4])), null);
      expect(p.maintenance.stagedResidualEntries).toBeGreaterThan(0);
      tx.rollback();
      await autocommit(p, (s) => s.table(table).insert(batch(ints([9])), null));
      for (let attempt = 0; attempt < 2; attempt++) {
        if (attempt) {
          await p.quiesce();
          p = await h.reopen();
        }
        const view = p.pin();
        try {
          const index = view.index(table, view.catalog.table(table)?.primaryKey ?? 0);
          const out = { probes: new Int32Array(3), locators: new Float64Array(3), count: 0 };
          await index.probe(keys, out);
          expect(out.count).toBe(1);
          const cursor = index.range(prefixRange(keys.key(0)), "asc");
          try {
            const found = new Float64Array(3);
            expect(await cursor.next(found)).toBe(1);
            expect(found[0]).toBe(out.locators[0]);
            expect(await cursor.next(found)).toBe(0);
          } finally {
            cursor.close();
          }
        } finally {
          view.release();
        }
      }
    } finally {
      keys.close();
      await h.close();
    }
  });
