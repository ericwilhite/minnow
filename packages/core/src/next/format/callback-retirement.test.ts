/** Final callback fences keep COMMIT replayable and bound session end-marker retention. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CALLBACK_LEDGER_RETIRE_REACH } from "../../testing/reach.js";
import { RequestLedger, type RequestIdentity } from "./ledger.js";
for (const seed of seedsFor("next-callback-ledger-retirement", [0xb766, 0xb767]))
  it(`reclaims only acknowledged final-session markers (${String(seed)})`, () => {
    const reach = createReach("callback ledger retirement", CALLBACK_LEDGER_RETIRE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 20, max: 80 }),
        fc.integer({ min: 1, max: 8 }),
        (rounds, width) => {
          reach.beginHistory();
          const ledger = new RequestLedger();
          let sequence = 0,
            through = 0;
          const identity = (
            sessionId: string,
            scope: "transaction" | "standalone",
            seq = ++sequence,
          ): RequestIdentity => ({
            clientId: "callback",
            sessionId,
            scope,
            sequence: seq,
            generation: 1,
            sentAtMs: 0,
            resolvedThrough: through,
          });
          for (let round = 0; round < rounds; round++) {
            const old: RequestIdentity[] = [];
            for (let n = 0; n < width; n++) {
              const session = String(round * width + n + 1),
                begin = identity(session, "transaction");
              expect(ledger.admit(begin, 0)).toEqual({ kind: "execute" });
              ledger.record("callback", begin.sequence, {
                kind: "statement",
                status: "ok",
                counts: {},
                recordedAtMs: 0,
              });
              const commit = identity(session, "standalone");
              expect(ledger.admit(commit, 0)).toEqual({ kind: "execute" });
              const outcome = {
                kind: "committed",
                lsn: commit.sequence,
                counts: { t: 1 },
                recordedAtMs: 0,
              } as const;
              ledger.record("callback", commit.sequence, outcome);
              ledger.endSession("callback", session, 1, 0, commit.sequence);
              expect(ledger.admit(begin, 0)).toEqual({ kind: "sessionLost" });
              expect(ledger.admit(commit, 0)).toEqual({ kind: "duplicate", outcome });
              old.push(begin);
            }
            expect(ledger.endedSessionCount).toBe(width);
            through = sequence;
            const ack = identity("0", "standalone");
            expect(ledger.admit(ack, 0)).toEqual({ kind: "execute" });
            ledger.settle("callback", ack.sequence);
            expect(ledger.endedSessionCount).toBe(0);
            for (const request of old)
              expect(ledger.admit({ ...request, resolvedThrough: through }, 0)).toEqual({
                kind: "stale",
              });
            through = sequence;
          }
          reach.reach(width === 1 ? "single final marker" : "concurrent final markers");
          reach.reach("transaction retry refused before ACK");
          reach.reach("COMMIT replay retained before ACK");
          reach.reach("ACK frees callback tombstones");
          reach.reach("old requests stale after ACK");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [20, 1],
          [80, 8],
        ],
      },
    );
    reach.assertReached();
  });
