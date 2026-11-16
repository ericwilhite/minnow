/** Native publications must not turn a workload-sized fragment list into call arguments.
 * The stored row-id high-water mark survives staging, folding, rollback and reopen. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, STAGED_FRAGMENT_MAXIMA_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";

for (const seed of seedsFor("next-staged-fragment-maxima", [0xb5fc, 0xb5fd]))
  it(`publishes arbitrary fragment counts without variadic maxima seed ${String(seed)}`, async () => {
    const reach = createReach("staged fragment maxima", STAGED_FRAGMENT_MAXIMA_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("stage", "flush"),
        fc.integer({ min: 1, max: 96 }),
        fc.constantFrom(1, 2),
        fc.boolean(),
        async (phase, fragments, capacity, rollback) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", capacity, undefined, {
            merges: { mode: "off" },
          });
          let path = await h.open();
          let table = 0;
          await autocommit(path, async (s) => {
            table =
              (
                await s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("fragmented", [column("id", INT, false)]),
                })
              ).id ?? 0;
          });
          // A deterministic argument ceiling catches the wrong design at small native
          // histories. It does not pretend the VM's actual stack threshold is portable.
          const maximum = Math.max;
          const guard = vi.spyOn(Math, "max").mockImplementation((...values) => {
            if (values.length > 8) {
              const stack = new Error().stack ?? "";
              const method = phase === "stage" ? "#publishStaging" : "#writeFlush";
              if (stack.includes(method))
                throw new Error(`unbounded ${phase} maximum: ${String(values.length)} arguments`);
            }
            return maximum(...values);
          });
          let tx: Awaited<ReturnType<typeof path.begin>> | null = null;
          try {
            tx = await path.begin();
            const count = (fragments + 2) * capacity;
            for (let from = 0; from < count; from += capacity) {
              const s = tx.statement({ rules });
              await s
                .table(table)
                .insert(batch(ints(Array.from({ length: capacity }, (_, k) => from + k))), null);
              await s.end();
            }
            expect(path.maintenance.stages).toBeGreaterThan(0);
            const root = path.state().root.tables.find((t) => t.tableId === table);
            expect(root?.staged).toHaveLength(1);
            expect(root?.staged[0]?.rowCount).toBeGreaterThanOrEqual(fragments * capacity);
            expect(root?.nextRowId).toBeGreaterThanOrEqual(root?.staged[0]?.rowCount ?? 0);
            const before = path.pin();
            try {
              expect(await rowsOf(before, table)).toEqual([]);
            } finally {
              before.release();
            }
            if (rollback) tx.rollback();
            else await (await tx.commit(null)).durable;
            tx = null;
            await path.quiesce();
            if (!rollback) {
              expect(path.maintenance.stagedFolds).toBeGreaterThan(0);
              expect(
                path.state().root.tables.find((t) => t.tableId === table)?.nextRowId,
              ).toBeGreaterThanOrEqual(root?.nextRowId ?? 0);
            }
            const expected = rollback ? [] : Array.from({ length: count }, (_, k) => [k]);
            path = await h.reopen();
            // Appending after reopen detects a lost row-id maximum as well as bad data.
            await autocommit(path, (s) => s.table(table).insert(batch(ints([10000, 10001])), null));
            const after = path.pin();
            try {
              expect(
                (await rowsOf(after, table)).sort((a, b) => Number(a[0]) - Number(b[0])),
              ).toEqual([...expected, [10000], [10001]]);
            } finally {
              after.release();
            }
            await h.space.audit();
            reach.reach(phase);
            reach.reach(fragments === 1 ? "one fragment" : "multiple fragments");
            if (fragments > 64) reach.reach("over 64 fragments");
            reach.reach(rollback ? "rollback" : "fold committed rows");
            reach.reach("hidden staging");
            reach.reach("append after reopen");
          } finally {
            guard.mockRestore();
            tx?.rollback();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero charge");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["stage", 1, 1, false],
          ["stage", 65, 2, false],
          ["flush", 65, 1, false],
          ["flush", 9, 2, true],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  }, 120000);
