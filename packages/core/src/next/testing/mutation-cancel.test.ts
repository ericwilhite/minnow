/** R9/R16: only the owner's terminal reply decides whether cancellation beat commit. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MUTATION_CANCEL_REACH } from "../../testing/reach.js";
import { CancelledError } from "../owner/errors.js";
import { DEFAULT_RESULT_WINDOW_BYTES } from "../owner/protocol.js";
import { mutationOwner } from "./mutation-owner.test-support.js";
for (const seed of seedsFor("next-mutation-cancel-native", [0xb701, 0xb702]))
  it(`binds cancellation to actual native mutation outcome seed ${String(seed)}`, async () => {
    const reach = createReach("native mutation cancellation", MUTATION_CANCEL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("direct" as const, "channel" as const),
        fc.constantFrom(
          "before-send" as const,
          "end" as const,
          "commit" as const,
          "after" as const,
        ),
        fc.integer({ min: 1, max: 8 }),
        fc.integer({ min: 1, max: 9 }),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 2 }),
        async (transport, inputPhase, count, delta, returning, primitive, signalMode) => {
          reach.beginHistory();
          const signalInput = signalMode !== 2,
            phase = !signalInput && inputPhase === "before-send" ? "end" : inputPhase,
            f = await mutationOwner(transport, count),
            stop = new AbortController(),
            reason = primitive
              ? "local abort reason"
              : new Error("local abort", { cause: new Error("abort cause") });
          const removed = vi.spyOn(stop.signal, "removeEventListener");
          try {
            if (phase !== "before-send") await f.connect();
            const gate = phase === "end" || phase === "commit" ? f.arm(phase) : null;
            const options = { session: 0, signal: stop.signal, mutation: true };
            const sql = `UPDATE t SET v=v+$1${returning ? " RETURNING id,v" : ""}`,
              wire =
                signalMode === 0
                  ? null
                  : f.client.send(
                      {
                        t: "statement",
                        session: 0,
                        text: sql,
                        prepared: null,
                        knownColumnsVersion: -1,
                        params: [delta],
                        result: {
                          windowBytes: DEFAULT_RESULT_WINDOW_BYTES,
                          frameRows: null,
                          wantRows: true,
                        },
                      },
                      { mutation: true },
                    );
            if (signalMode === 1 && wire) f.client.attachSignal(wire.seq, stop.signal);
            let callerSettled = false;
            const result = (wire?.promise ?? f.client.query(sql, [delta], options)).then(
              (value) => {
                callerSettled = true;
                return { value, error: null };
              },
              (error: unknown) => {
                callerSettled = true;
                return { value: null, error };
              },
            );
            if (gate) await gate.entered;
            if (gate && returning) {
              expect(
                f.packets.filter((p) => (p.t === "rows" || p.t === "done") && p.frames.length > 0),
              ).toEqual([]);
              reach.reach("RETURNING withheld before commit");
            }
            if (phase === "after") await result;
            if (!signalInput && wire) f.client.cancel(wire.seq);
            else stop.abort(reason);
            if (phase === "before-send") await f.connect();
            else await f.fence();
            if (gate) {
              expect(callerSettled).toBe(false);
              reach.reach("cancellation awaits owner terminal");
            }
            gate?.release();
            const actual = await result,
              committed = phase === "commit" || phase === "after";
            if (committed) {
              expect(actual.error).toBeNull();
              expect(actual.value?.summary.counts).toEqual({ t: count });
              expect(actual.value?.rows()).toEqual(
                returning ? Array.from({ length: count }, (_, i) => [i + 1, 1 + delta]) : [],
              );
              reach.reach("committed");
            } else {
              if (signalInput) expect(actual.error).toBe(reason);
              else {
                expect(actual.error).toBeInstanceOf(CancelledError);
                expect(actual.error).toMatchObject({ code: "MINNOW_CANCELLED", sqlState: "57014" });
                reach.reach("wire cancellation identity");
              }
              expect(actual.value).toBeNull();
              reach.reach("rolled back");
            }
            expect(await f.rows()).toEqual(
              Array.from({ length: count }, (_, i) => [i + 1, committed ? 1 + delta : 1]),
            );
            if (phase === "before-send") {
              expect(f.clientTrace.some((p) => p.t === "statement")).toBe(false);
              reach.reach("cancelled input never posted");
            }
            expect(f.client.pendingCount).toBe(0);
            expect(removed.mock.calls.filter(([type]) => type === "abort")).toHaveLength(
              signalInput ? 1 : 0,
            );
            reach.reach("listener retired");
            reach.reach(signalInput ? "signal" : "wire");
            if (signalInput) reach.reach(signalMode === 0 ? "options signal" : "attached signal");
            reach.reach(transport);
            reach.reach(phase);
            reach.reach(returning ? "RETURNING" : "no RETURNING");
            if (signalInput) reach.reach(primitive ? "primitive reason" : "Error reason");
          } finally {
            removed.mockRestore();
            await f.close();
          }
          reach.reach("zero native charge");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          ["direct", "before-send", 2, 1, true, false, 0],
          ["channel", "end", 3, 2, true, true, 0],
          ["direct", "commit", 2, 3, false, false, 0],
          ["channel", "after", 1, 1, true, false, 0],
          ["direct", "end", 3, 2, true, false, 2],
          ["channel", "end", 3, 2, false, false, 2],
          ["direct", "before-send", 2, 1, true, true, 1],
          ["channel", "end", 2, 1, true, false, 1],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
