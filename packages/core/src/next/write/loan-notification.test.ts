/** A phase notification may not release a byte owned by a live progress loan. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_LOAN_NOTIFICATION_REACH } from "../../testing/reach.js";
import { Governor, type Grant, type Loan } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "./credits.js";

const loanStates = ["row loan", "metadata loan", "commit loan", "merge loan"] as const;

for (const seed of seedsFor("next-write-loan-notification", [47241, 47242]))
  it(`notifies queued readers while another account owns loan-backed bytes (seed ${String(seed)})`, async () => {
    const reach = createReach("loan-safe phase notification", WRITE_LOAN_NOTIFICATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 16 }),
        fc.integer({ min: 0, max: 3 }),
        async (units, index) => {
          reach.beginHistory();
          const governor = new Governor(8 << 20);
          const credits = await WriteCredits.open(governor, {
            rows: 1 << 20,
            metadata: 1 << 20,
            commits: 1 << 20,
            merge: 1 << 20,
          });
          const accounts = [credits.rows, credits.metadata, credits.commits, credits.merge];
          const owner = accounts[index];
          if (!owner) throw new Error("Missing loan owner");
          const stop = new AbortController();
          const buffers = new Map<Grant, Uint8Array>();
          const state = { busy: true, settled: false };
          let loan: Loan | undefined,
            reader: Grant | undefined,
            pending: Promise<readonly Grant[]> | undefined;
          try {
            loan = await owner.borrow(units * 1024);
            expect(owner.allocate(owner.held)).toBe(true);
            buffers.set(owner, new Uint8Array(owner.allocated).fill(37));
            expect(owner.held - owner.allocated - owner.loaned).toBeLessThan(0);
            const loanState = loanStates[index];
            if (!loanState) throw new Error("Missing loan state");
            reach.reach(loanState);
            credits.rows.onReclaimRequest(() => (state.busy ? 0 : credits.reclaimIdleForecasts()));
            pending = governor.admitGroup({
              label: "reader after loan-backed phase",
              grants: [{ label: "reader", minimumBytes: 6 << 20, spillable: false }],
              includesProgress: true,
              signal: stop.signal,
            });
            void pending.then(
              (group) => {
                reader = group[0];
                state.settled = true;
              },
              () => undefined,
            );
            await yieldTask();
            expect(state.settled).toBe(false);
            reach.reach("reader queued during phase");
            state.busy = false;
            credits.notifyReclaimable();
            for (let turn = 0; turn < 16 && !state.settled; turn++) await yieldTask();
            const stalled = !state.settled;
            if (stalled) credits.reclaimIdleForecasts(); // Cleanup, not a progress waiver.
            await pending;
            expect(stalled, "phase notification must wake an already fitting reader").toBe(false);
            expect(owner.loaned).toBe(loan.bytes);
            expect(owner.allocated).toBe(buffers.get(owner)?.byteLength);
            expect(buffers.get(owner)?.at(-1)).toBe(37);
            reach.reach("loan and real buffer retained");
            reach.reach("queued reader progresses");
          } finally {
            stop.abort(new Error("loan notification history ended"));
            await Promise.allSettled(pending ? [pending] : []);
            reader?.close();
            for (const [account, buffer] of buffers) {
              const bytes = buffer.byteLength;
              expect(buffer.at(-1)).toBe(37);
              buffers.delete(account);
              account.free(bytes);
            }
            loan?.repay();
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 20,
        examples: [
          [1, 0],
          [16, 1],
          [8, 2],
          [4, 3],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
