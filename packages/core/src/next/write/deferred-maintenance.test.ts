/** close must retain every accepted flush, including work before the ready acknowledgement. */
import { expect, it, vi } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { WritePathImpl } from "./engine.js";
import { Harness, autocommit, batch, column, ints, INT, tableInput } from "./test-support.js";
const reach = createReach("deferred-maintenance", [
  "held-flush",
  "started",
  "close-waits",
  "settled",
]);
for (const seed of seedsFor("deferred-maintenance", [761, 762, 763, 764]))
  it(`joins a held flush before close (seed ${String(seed)})`, async () => {
    reach.beginHistory();
    const width = 2 + Math.floor(mulberry32(seed)() * 5);
    const original = WritePathImpl.open.bind(WritePathImpl);
    const open = vi
      .spyOn(WritePathImpl, "open")
      .mockImplementation((input) => original({ ...input, deferInitialMaintenance: true }));
    let entered!: () => void;
    let release!: () => void;
    const at = new Promise<void>((r) => {
      entered = r;
    });
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let armed = false;
    const h = new Harness("strict", "sim", width, undefined, {
      merges: { mode: "off" },
      beforeFlushPublish: async () => {
        if (armed) {
          armed = false;
          entered();
          await gate;
        }
      },
    });
    let closing: Promise<void> | undefined;
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
      await p.quiesce();
      armed = true;
      await autocommit(p, (s) =>
        s.table(table).insert(batch(ints(Array.from({ length: width }, (_, i) => i))), null),
      );
      await autocommit(p, (s) => s.table(table).insert(batch(ints([width])), null));
      await at;
      expect(p.maintenanceState.flushes).toBe(1);
      reach.reach("held-flush");
      vi.useFakeTimers();
      p.startInitialMaintenance();
      p.startInitialMaintenance();
      reach.reach("started");
      let closed = false;
      closing = p.close().then(() => {
        closed = true;
      });
      await vi.runAllTimersAsync();
      expect(closed, "close must wait until the held publication settles").toBe(false);
      reach.reach("close-waits");
      release();
      await vi.runAllTimersAsync();
      await closing;
      expect(p.maintenanceState.flushes).toBe(0);
      expect(p.maintenanceState.merging).toBe(false);
      expect(p.failure).toBeNull();
      reach.reach("settled");
    } finally {
      release();
      vi.useRealTimers();
      await closing;
      await h.close();
      open.mockRestore();
    }
  });
it("reaches every deferred-maintenance state", () => reach.assertReached());
