import { it, expect } from "vitest";
import { Memtable } from "./memtable.js";
import { Harness, autocommit, tableInput, column, INT, ints } from "./test-support.js";
it.each([2, 5])("returns all charges after %i borrowed generations", async (generations) => {
  const h = new Harness();
  const p = await h.open();
  let t = 0;
  await autocommit(p, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  const def = p.catalog.table(t);
  if (!def) throw new Error("missing table");
  let charged = 0;
  const charge = {
    allocate: (n: number) => {
      charged += n;
      return true;
    },
    free: (n: number) => {
      charged -= n;
      expect(charged).toBeGreaterThanOrEqual(0);
    },
  };
  const m = new Memtable(t, 0, def.schemaVersion, def.columns, [], charge);
  for (let i = 1; i <= generations; i++) {
    m.append([ints([i])], null, 0, 1);
    m.mark(i);
    if (i === 1) m.borrow(0, -1);
    m.truncate(0, i);
  }
  m.unborrow(0);
  m.releaseAll();
  await h.close();
  expect(charged).toBe(0);
});
