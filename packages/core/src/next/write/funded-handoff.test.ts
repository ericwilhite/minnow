/** A funded step owns its prepayment until its waiting continuation can consume it. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_FUNDED_HANDOFF_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "./credits.js";

for (const seed of seedsFor("next-write-funded-handoff", [47245, 47246]))
  it(`consumes funded commit and merge steps without trading the same credit forever (seed ${String(seed)})`, async () => {
    const reach = createReach("funded native handoff", WRITE_FUNDED_HANDOFF_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 16 }), fc.boolean(), async (units, mergeFirst) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20),
          step = units * 64 * 1024;
        const credits = await WriteCredits.open(governor, {
          rows: step,
          metadata: 32 * 1024,
          commits: step,
          merge: step,
        });
        const buffers = new Map<Grant, Uint8Array>();
        const handoff = { commit: false, merge: false };
        const observeFunding = (account: Grant) => {
          credits.reclaimIdleForecasts();
          expect(account.held, "funded handoff survives reclaim").toBeGreaterThanOrEqual(step);
          expect(
            credits.metadata.held,
            "compound metadata survives reclaim",
          ).toBeGreaterThanOrEqual(9 * 1024);
          expect(credits.rows.held).toBe(0);
          reach.reach("compound metadata funded");
          reach.reach("idle forecasts returned");
        };
        let moves = 0;
        const transfer = governor.transferCredit.bind(governor);
        governor.transferCredit = (source, target, bytes) => {
          if (bytes > 0 && source === credits.commits && target === credits.merge && handoff.commit)
            throw new Error("Funded commit handoff was reused before its consumer resumed");
          if (bytes > 0 && source === credits.merge && target === credits.commits && handoff.merge)
            throw new Error("Funded merge handoff was reused before its consumer resumed");
          if (
            bytes > 0 &&
            ((source === credits.commits && target === credits.merge) ||
              (source === credits.merge && target === credits.commits)) &&
            ++moves > 32
          )
            throw new Error("Funded phases traded the same credit instead of progressing");
          return transfer(source, target, bytes);
        };
        const commit = async () => {
          for (let attempt = 0; ; attempt++) {
            expect(attempt, "commit prepayment reaches a usable continuation").toBeLessThan(32);
            const ready = credits.prepareCommit();
            if (!(ready instanceof Promise)) break;
            handoff.commit = credits.commits.held >= step;
            if (handoff.commit) observeFunding(credits.commits);
            await ready;
            handoff.commit = false;
          }
          expect(credits.commits.allocate(step)).toBe(true);
          buffers.set(credits.commits, new Uint8Array(step).fill(41));
          try {
            await yieldTask();
            expect(buffers.get(credits.commits)?.at(-1)).toBe(41);
          } finally {
            buffers.delete(credits.commits);
            credits.commits.free(step);
            credits.notifyReclaimable();
          }
        };
        const merge = async () => {
          const pending = credits.acquireMerge("funded test merge", step);
          handoff.merge = credits.merge.held >= step;
          let grant: Grant | undefined;
          try {
            if (handoff.merge) observeFunding(credits.merge);
            grant = await pending;
            handoff.merge = false;
            expect(grant.allocate(step)).toBe(true);
            buffers.set(grant, new Uint8Array(step).fill(43));
            await yieldTask();
            expect(buffers.get(grant)?.at(-1)).toBe(43);
          } finally {
            // An observation may fail before acquireMerge's already accepted
            // handoff resumes. Drain it before closing the owner, even in RED.
            grant ??= await pending.catch(() => undefined);
            handoff.merge = false;
            if (grant) {
              const buffer = buffers.get(grant);
              if (buffer) {
                buffers.delete(grant);
                grant.free(buffer.byteLength);
              }
              credits.releaseMerge(grant);
            }
          }
        };
        try {
          expect(credits.metadata.allocate(1024)).toBe(true);
          buffers.set(credits.metadata, new Uint8Array(1024).fill(47));
          await credits.prepareMetadata(8 * 1024);
          // Both next steps fit together; retain only a row forecast from which
          // their initial funding can start, rather than two warm target grants.
          credits.commits.release(credits.commits.held);
          credits.merge.release(credits.merge.held);
          reach.reach(mergeFirst ? "merge first" : "commit first");
          const results = await Promise.allSettled(
            mergeFirst ? [merge(), commit()] : [commit(), merge()],
          );
          for (const result of results) if (result.status === "rejected") throw result.reason;
          expect(moves).toBeLessThanOrEqual(32);
          expect(governor.peakBytes).toBeLessThanOrEqual(3 * step + 32 * 1024);
          expect(buffers.get(credits.metadata)?.at(-1)).toBe(47);
          expect(credits.metadata.allocated).toBe(1024);
          reach.reach("finite funding work");
          reach.reach("funded handoffs consumed");
          reach.reach("real metadata buffer retained");
        } finally {
          for (const [account, buffer] of buffers) {
            buffers.delete(account);
            account.free(buffer.byteLength);
          }
          credits.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      {
        seed,
        numRuns: 14,
        examples: [
          [1, false],
          [16, true],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
