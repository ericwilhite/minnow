/** readRows must own/borrow each resident source until a mixed disk read finishes or fails. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_ROW_BORROW_STATES } from "../../testing/reach.js";
import type { TableSnapshotImpl } from "./view.js";
import { ArenaPool, Memtable } from "./memtable.js";
import { readRows } from "./rows.js";
import { Harness, autocommit, tableInput, column, TEXT, texts } from "./test-support.js";
const reach = createReach("mixed row-source borrows", WRITE_ROW_BORROW_STATES);
for (const seed of seedsFor("next-write-row-borrow", [0xb300]))
  it(`protects resident rows across disk reads (${String(seed)})`, async () => {
    const h = new Harness();
    const path = await h.open();
    let id = 0;
    await autocommit(path, async (s) => {
      id =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("v", TEXT)], { primaryKey: null }),
          })
        ).id ?? 0;
    });
    const columns = path.catalog.table(id)?.columns;
    if (!columns) throw new Error("missing columns");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            value: fc.integer({ min: -100000, max: 100000 }),
            residentFirst: fc.boolean(),
            mode: fc.constantFrom("success", "async error", "sync error"),
          }),
          async (s) => {
            reach.beginHistory();
            reach.reach(s.residentFirst ? "resident first" : "stored first");
            reach.reach(s.mode);
            let charged = 0;
            const charge = {
              allocate: (n: number) => {
                charged += n;
                return true;
              },
              free: (n: number) => {
                charged -= n;
              },
            };
            const pool = new ArenaPool(charge, 65536);
            const m = new Memtable(id, 0, 1, columns, [], charge, 2048, pool);
            const next = new Memtable(id, 1, 1, columns, [], charge, 2048, pool);
            const error = new Error("disk read failed");
            const table = {
              columns,
              locator: (row: number) => (row === 0 ? 0 : 65536),
              segments: [
                { kind: "memory", memtable: m },
                {
                  kind: "stored",
                  root: () => {
                    if (s.mode === "sync error") throw error;
                    if (s.mode === "async error") return Promise.reject(error);
                    // A group predating this column supplies NULL; its async decoder still suspends.
                    return { rowCount: 1, rowIds: { kind: "dense" }, columns: [] };
                  },
                },
              ],
              snapshot: { shared: {} },
            } as unknown as TableSnapshotImpl;
            try {
              const value = String(s.value).padStart(4096, "x");
              m.append([texts([value])], null, 0, 1);
              const rows = s.residentFirst ? [0, 1] : [1, 0];
              if (s.mode === "sync error") {
                expect(() => readRows(table, rows)).toThrow(error);
                m.release();
                expect(m.borrowedBytes).toBe(0);
              } else {
                const pending = readRows(table, rows);
                m.release();
                expect(pool.bytes, "an unfinished mixed read retains its resident source").toBe(0);
                next.append([texts([String(s.value + 1).padStart(4096, "x")])], null, 0, 1);
                if (s.mode === "async error") await expect(pending).rejects.toBe(error);
                else {
                  const vector = (await pending)[0];
                  if (vector?.data.layout !== "varlen") throw new Error("expected varlen");
                  const slot = s.residentFirst ? 0 : 1;
                  expect(
                    new TextDecoder().decode(
                      vector.data.bytes.subarray(
                        vector.data.offsets[slot],
                        vector.data.offsets[slot + 1],
                      ),
                    ),
                  ).toBe(value);
                  expect((vector.validity?.[0] ?? 0) & (1 << (1 - slot))).toBe(0);
                }
                expect(m.borrowedBytes).toBe(0);
                reach.reach("released after suspension");
              }
            } finally {
              m.releaseAll();
              next.releaseAll();
              pool.close();
            }
            expect(charged).toBe(0);
          },
        ),
        { seed, numRuns: 48 },
      );
    } finally {
      await h.close();
    }
  });
it("reaches mixed row read lifetimes", () => reach.assertReached());
