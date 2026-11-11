/** Regression for ordinary SQL repeating one wide payload across output aliases.
 * The known fanout under-admission must refuse with a structured memory error,
 * before emitting a frame. This tests the refusal, not the pending sizing fix. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, SORT_ADMISSION_ERROR_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { SortOperator } from "../exec/sort.js";
import { Governor, MemoryAdmissionError } from "../memory/governor.js";
import { StepLoans } from "../memory/step-loans.js";
import { StorageCore } from "./storage-core.js";

const reach = createReach("sort admission error", SORT_ADMISSION_ERROR_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-sort-admission-error", [0xb761, 0xb762]))
  it(`reports an ordinary SQL fanout refusal without leaking (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          aliases: fc.integer({ min: 5, max: 6 }),
          chars: fc.integer({ min: 360000, max: 360010 }),
          desc: fc.boolean(),
        }),
        async ({ aliases, chars, desc }) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          const governor = new Governor(128 * 1024 * 1024);
          const resident = await governor.admit({
            label: "busy resident",
            minimumBytes: 0,
            spillable: true,
          });
          const scopes: Array<{ mockRestore(): void }> = [];
          const requests: Array<{ bytes: number; capacity: number; live: number }> = [];
          let pushed = false;
          try {
            await core.execute(
              "CREATE TABLE wide_text(id INTEGER PRIMARY KEY, v0 TEXT, v1 TEXT, v2 TEXT, v3 TEXT, v4 TEXT)",
            );
            await core.writeBatch("insertBatch", "wide_text", [{ id: 1, v0: "汉".repeat(chars) }]);
            const prepare = core.executor.prepare.bind(core.executor);
            scopes.push(
              vi.spyOn(core.executor, "prepare").mockImplementation((input) => {
                const prepared = prepare(input);
                return {
                  ...prepared,
                  execute: (request) => prepared.execute({ ...request, governor }),
                };
              }),
            );
            // eslint-disable-next-line @typescript-eslint/unbound-method -- applied with the original receiver.
            const push = SortOperator.prototype.push;
            scopes.push(
              vi.spyOn(SortOperator.prototype, "push").mockImplementation(function (
                this: SortOperator,
                batch,
                transfer,
              ) {
                if (!pushed) {
                  pushed = true;
                  expect(
                    resident.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                  reach.reach("ordinary room exhausted");
                }
                return push.call(this, batch, transfer);
              }),
            );
            // eslint-disable-next-line @typescript-eslint/unbound-method -- applied with the original receiver.
            const borrow = StepLoans.prototype.borrow;
            scopes.push(
              vi.spyOn(StepLoans.prototype, "borrow").mockImplementation(function (
                this: StepLoans,
                bytes,
                signal,
              ) {
                if (this.label.includes("/sort#"))
                  requests.push({ bytes, capacity: this.capacity, live: this.budget.allocated });
                return borrow.call(this, bytes, signal);
              }),
            );
            const sql = `SELECT ${Array.from({ length: aliases }, (_, i) => `v0 AS v${String(i)}`).join(",")} FROM wide_text ORDER BY v0 ${desc ? "DESC" : "ASC"}`;
            const error: unknown = await core.execute(sql).then(
              () => null,
              (failure: unknown) => failure,
            );
            expect(error).toBeInstanceOf(MemoryAdmissionError);
            const refused = requests.at(-1);
            expect(refused).toBeDefined();
            if (!refused) throw new Error("sort did not request progress");
            expect(error).toMatchObject({
              name: "MemoryAdmissionError",
              code: "MINNOW_MEMORY_ADMISSION",
              sqlState: "53200",
              bytes: refused.bytes,
              capacity: refused.capacity,
              cause: { requestedBytes: refused.bytes, admittedBytes: refused.capacity },
            });
            expect(refused.bytes).toBeGreaterThan(refused.capacity);
            expect(requests.every((request) => request.live === 0)).toBe(true);
            reach.reach("repeated SQL projection");
            if (aliases === 5) {
              expect(requests.some((request) => request.bytes <= request.capacity)).toBe(true);
              reach.reach("completed earlier step");
            } else {
              expect(requests).toHaveLength(1);
              reach.reach("initial step refusal");
            }
          } finally {
            for (const scope of scopes.reverse()) scope.mockRestore();
            resident.close();
            await core.close();
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("cleanup returns all resources");
          }
        },
      ),
      {
        seed,
        numRuns: 5,
        examples: [
          [{ aliases: 5, chars: 360000, desc: false }],
          [{ aliases: 6, chars: 360000, desc: true }],
        ],
      },
    );
  }, 120000);
