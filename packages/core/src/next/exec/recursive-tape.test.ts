/** The recursive work table's FIFO contract survives binary directory carries with fixed metadata. */
import { expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RECURSIVE_TAPE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { SpillCorruptionError } from "../spill/contract.js";
import { MemorySpillService, MemorySpillFile } from "../spill/memory-spill.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import { RecursiveTape } from "./recursive-tape.js";
import { I32, numbers, valueAt } from "./kernels/test-support.js";
it.each(seedsFor("next-recursive-tape", [0xb66a, 0xb66b]))(
  "reads original page/row order with fixed charged directory state (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Recursive FIFO tape", RECURSIVE_TAPE_REACH);
    const examples: Array<[number, number, boolean]> = [
      [1, 1, false],
      [2, 2, false],
      [3, 1, false],
      [8, 4, false],
      [33, 3, false],
      [64, 2, true],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 80 }),
        fc.integer({ min: 1, max: 5 }),
        fc.boolean(),
        async (pages, rows, corrupt) => {
          reach.beginHistory();
          const governor = new Governor(8 * 1024 * 1024),
            grant = await governor.admit({
              label: "recursive-tape",
              minimumBytes: 4 * 1024 * 1024,
              spillable: true,
            });
          const charge = new Charge(grant),
            pool = new Pool(charge, 3 * 1024 * 1024),
            service = new MemorySpillService(),
            files = new SpillFiles(service, 1, charge),
            writer = new PageWriter(pool, 16384);
          const io = new SpillIo(pool, files, writer, { grant, queryId: 1 }, () => undefined),
            tape = new RecursiveTape(io, 0);
          const expected: number[] = [];
          const fixed = grant.allocated;
          try {
            for (let page = 0; page < pages; page++) {
              const values = Array.from({ length: rows }, (_, row) => page * rows + row);
              expected.push(...values);
              await tape.append([numbers(I32, values)], rows);
              expect(tape.run.pages.length).toBe(0); // No row/page-proportional ref list.
            }
            const allocated = grant.allocated;
            const actual: unknown[] = [];
            const room = pool.room;
            if (corrupt) {
              const file = tape.run.file;
              if (!(file instanceof MemorySpillFile)) throw new Error("Expected memory file");
              const read = file.read.bind(file);
              let injected = false;
              file.read = async (ref) => {
                if (!injected) {
                  injected = true;
                  file.corrupt(ref, 0);
                }
                return read(ref);
              };
              await expect(tape.each([I32], async () => true)).rejects.toBeInstanceOf(
                SpillCorruptionError,
              );
              reach.reach("corruption refusal");
            } else {
              await tape.each([I32], async (columns, n) => {
                const vector = columns[0];
                if (vector === undefined) throw new Error("Missing page column");
                for (let row = 0; row < n; row++) actual.push(valueAt(vector, row));
                return true;
              });
              expect(actual).toEqual(expected);
              reach.reach("FIFO rows");
            }
            expect(pool.room).toBe(room);
            expect(grant.allocated).toBe(allocated);
            expect(allocated - fixed).toBeLessThan(256 * 1024);
            reach.reach(
              pages === 1
                ? "one page"
                : (pages & (pages - 1)) === 0
                  ? "power-of-two carry"
                  : "ragged roots",
            );
            reach.reach("bounded metadata");
          } finally {
            await tape.close();
            await files.closeAll();
            writer.close();
            pool.close();
            grant.close();
          }
          expect(service.bytesInUse).toBe(0);
          expect(governor.usedBytes).toBe(0);
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          reach.reach("returned bytes");
        },
      ),
      { seed, numRuns: 35 + examples.length, examples },
    );
    reach.assertReached();
  },
);
