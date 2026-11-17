/** Real root close/cancel fences a sealed RI continuation before writer/registry retirement. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_RI_LIFETIME_REACH } from "../../testing/reach.js";
import { referentialFixture } from "./referential.test-support.js";
import { yieldTask } from "../task.js";
import { valueAt } from "../exec/kernels/test-support.js";
for (const seed of seedsFor("next-native-ri-lifetime", [0xb649, 0xb64a]))
  it(`fences native RI after full child capture ${String(seed)}`, async () => {
    const reach = createReach("native RI lifetime", NATIVE_RI_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 2, max: 17 }),
        async (published, close, count) => {
          reach.beginHistory();
          const f = await referentialFixture(
              [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE,v INT)",
                "INSERT INTO p VALUES(1,1)",
                `INSERT INTO c VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},1,1)`).join(",")}`,
              ],
              published,
            ),
            child = f.statement.catalog.tableByName("c"),
            realWrite = f.rows.write.bind(f.rows),
            realCapture = f.prefix.capture.bind(f.prefix);
          if (!child) throw new Error("Missing native child");
          let captured = 0,
            arrive!: () => void,
            resume!: () => void,
            closed = false;
          const inside = new Promise<void>((resolve) => {
              arrive = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              resume = resolve;
            }),
            source = vi.spyOn(f.prefix, "capture").mockImplementation(async (...args) => {
              const result = await realCapture(...args);
              if (args[0] === child.id) captured++;
              return result;
            }),
            write = vi.spyOn(f.rows, "write").mockImplementation(async (...args) => {
              if (args[1] === child.id) {
                arrive();
                await gate;
                args[6].throwIfAborted();
              }
              return realWrite(...args);
            });
          try {
            const input = await f.capture("p", [0]),
              row = input.refs[0];
            if (!row) throw new Error("Missing parent");
            await f.rows.write(
              f.controller.root,
              input.tableId,
              "delete",
              row.image,
              row.id,
              f.controller,
              f.stop.signal,
            );
            const ending = f.end().then(
              () => null,
              (error: unknown) => error,
            );
            await inside;
            expect(captured, "whole child input precedes mutation").toBe(count);
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            expect(f.charge.loaned).toBe(0);
            const closing = close ? f.statement.closeNativeCommands() : Promise.resolve();
            if (close)
              void closing.then(
                () => {
                  closed = true;
                },
                () => {
                  closed = true;
                },
              );
            else f.stop.abort(new Error("caller native RI cancellation"));
            await yieldTask();
            if (close) expect(closed, "close fences suspended continuation").toBe(false);
            expect(f.charge.allocated).toBeGreaterThan(0);
            resume();
            expect(await ending).toBeInstanceOf(Error);
            await closing;
            await f.rollback();
            await f.retire();
            const result = await f.core.execute("SELECT id,pid FROM c ORDER BY id");
            if (!("batches" in result)) throw new Error("Missing rollback rows");
            expect(
              result.batches.flatMap((batch) =>
                Array.from({ length: batch.length }, (_, i) =>
                  batch.columns.map((v) => valueAt(v, i)),
                ),
              ),
            ).toEqual(Array.from({ length: count }, (_, i) => [i + 1, 1]));
            reach.reach(published ? "published" : "resident");
            reach.reach(close ? "close fence" : "caller cancel");
            reach.reach("complete child input");
            reach.reach("no borrow across continuation");
            reach.reach("rollback");
          } finally {
            resume();
            source.mockRestore();
            write.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [false, false, 4],
          [true, true, 7],
        ],
      },
    );
    reach.assertReached();
  });
