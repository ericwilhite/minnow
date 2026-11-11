/** Governor rules 2/4/5: an under-admitted step refuses structurally, without
 * allocating, queuing, or poisoning the next correctly sized step. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, STEP_LOANS_REFUSAL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { GrantClosedError, MemoryAdmissionError } from "./governor.js";
import { StepLoans } from "./step-loans.js";

const reach = createReach("step loan refusal", STEP_LOANS_REFUSAL_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-step-loans-refusal", [0xb751, 0xb752]))
  it(`reports the requested and admitted progress bytes (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 16 * 1024 * 1024 }),
        fc.integer({ min: 1, max: 16 * 1024 * 1024 }),
        async (capacity, excess) => {
          reach.beginHistory();
          let live = 0;
          const owner = {
            allocate(bytes: number) {
              live += bytes;
              return true;
            },
            free(bytes: number) {
              live -= bytes;
            },
          };
          const loans = new StepLoans(owner, capacity, "query#1/sort#3");
          try {
            const requested = capacity + excess;
            await expect(loans.borrow(requested)).rejects.toBeInstanceOf(MemoryAdmissionError);
            await expect(loans.borrow(requested)).rejects.toMatchObject({
              name: "MemoryAdmissionError",
              code: "MINNOW_MEMORY_ADMISSION",
              sqlState: "53200",
              label: "query#1/sort#3",
              bytes: requested,
              capacity,
              cause: { requestedBytes: requested, admittedBytes: capacity },
            });
            expect(live).toBe(0);
            expect(loans.budget.allocated).toBe(0);
            reach.reach("oversized refused without allocation");
            const loan = await loans.borrow(capacity);
            expect(loan.bytes).toBe(capacity);
            expect(loans.budget.allocate(capacity)).toBe(true);
            expect(live).toBe(capacity);
            await expect(loans.borrow(requested)).rejects.toThrow("must repay its step loan");
            reach.reach("ownership violation stays distinct");
            loans.budget.free(capacity);
            loan.repay();
            const again = await loans.borrow(capacity);
            again.repay();
            reach.reach("capacity boundary and retry succeed");
            const reason = new Error("cancelled before refusal");
            const signal = AbortSignal.abort(reason);
            await expect(loans.borrow(requested, signal)).rejects.toBe(reason);
            reach.reach("abort has precedence");
            loans.close();
            await expect(loans.borrow(requested)).rejects.toBeInstanceOf(GrantClosedError);
            reach.reach("closed has precedence");
          } finally {
            loans.close();
          }
          expect(live).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, 1],
          [4320064, 10478448],
        ],
      },
    );
  });
