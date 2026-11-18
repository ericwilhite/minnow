/** R16/R18: lost delivery never changes the original native mutation's identity or effect. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MUTATION_OUTCOMES_REACH } from "../../testing/reach.js";
import { CLIENT_RETRY_WINDOW_MS } from "../format/ledger.js";
import {
  CommittedWithoutResultError,
  OutcomeUnknownError,
  OwnerUnresponsiveError,
  OwnerUnreachableError,
  RequestExpiredError,
} from "../owner/errors.js";
import { StreamDictionaries } from "../owner/frame.js";
import { DEFAULT_RESULT_WINDOW_BYTES } from "../owner/protocol.js";
import { latch, mutationOwner } from "./mutation-owner.test-support.js";

for (const seed of seedsFor("next-mutation-outcomes-native", [0xb70b, 0xb70c]))
  it(`keeps native terminal outcomes and lost RETURNING distinct seed ${String(seed)}`, async () => {
    const reach = createReach("native mutation outcomes", MUTATION_OUTCOMES_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("direct" as const, "channel" as const),
        fc.integer({ min: 0, max: 5 }),
        fc.integer({ min: 1, max: 8 }),
        fc.integer({ min: 1, max: 5 }),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: 0, max: 2 }),
        async (transport, route, count, delta, returning, cancel, commitMode, frameRows) => {
          reach.beginHistory();
          const f = await mutationOwner(transport, count),
            stop = new AbortController(),
            cause = new OwnerUnresponsiveError(137),
            terminal = latch(),
            reason = new Error("local abort after lost delivery"),
            decode = vi.spyOn(StreamDictionaries.prototype, "apply"),
            // Prepared reconnect/re-prepare belongs to the facade. Native lost-reply
            // control coverage here uses typed and SQL COMMIT; all three forms cover unknown.
            explicit =
              commitMode > 0 && (route === 1 || ((route === 2 || route === 3) && commitMode !== 3)),
            sql =
              route === 5
                ? "SELECT id,v+$1 AS v FROM t"
                : `UPDATE t SET v=v+$1${returning ? " RETURNING id,v" : ""}`;
          let committed = false;
          try {
            if (route !== 4) await f.connect();
            if (explicit) {
              await f.client.begin(9);
              const provisional = await f.client.query(sql, [delta], {
                session: 9,
                mutation: true,
              });
              expect(provisional.summary).toMatchObject({ lsn: null, transaction: "open" });
              expect(provisional.rows()).toEqual(
                returning ? Array.from({ length: count }, (_, i) => [i + 1, 1 + delta]) : [],
              );
              decode.mockClear();
              reach.reach(
                transport === "direct" ? "explicit direct commit" : "explicit channel commit",
              );
              if (returning) reach.reach("provisional RETURNING");
            }
            const prepared = explicit && commitMode === 3 ? await f.client.prepare("COMMIT") : null,
              settled = explicit ? (prepared ? 3 : 2) : 0;
            const gate = route < 2 ? f.arm(route === 0 ? "end" : "commit") : null;
            f.observe((message) => {
              if (message.t === "done" || message.t === "error") terminal.release();
            });
            if (route === 2 || route === 3 || route === 5)
              f.drop(
                (message) => message.t === "rows" || message.t === "done" || message.t === "error",
              );
            const task = explicit
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
                      frameRows: frameRows || null,
                      wantRows: true,
                    },
                  },
                  { mutation: route !== 5, signal: stop.signal },
                );
            let inputRetired = false;
            if (task?.retired)
              void task.retired.then(() => {
                inputRetired = true;
              });
            const pending = (
              explicit
                ? commitMode === 1
                  ? f.client.commit(9, { signal: stop.signal })
                  : commitMode === 2
                    ? f.client.query("COMMIT", [], { session: 9, signal: stop.signal })
                    : f.client.execute(prepared?.id ?? -1, [], 9, -1, { signal: stop.signal })
                : (task?.promise ?? Promise.reject(new Error("missing native task")))
            ).then(
              (value) => ({ value, error: null }),
              (error: unknown) => ({ value: null, error }),
            );
            if (gate) {
              await gate.entered;
              f.client.giveUp("native mutation stopped answering", cause);
              const result = await pending;
              expect(result.value).toBeNull();
              expect(result.error).toBeInstanceOf(OutcomeUnknownError);
              expect(result.error).toMatchObject({ reason: "unresponsive", cause });
              expect(f.client.pendingCount).toBe(1);
              expect(f.client.resolvedThrough).toBe(settled);
              if (task) {
                expect(inputRetired).toBe(false);
                reach.reach("input retained after unknown");
              }
              if (cancel) stop.abort(reason);
              await f.fence();
              gate.release();
              await terminal.promise;
              await f.fence();
              committed = route === 1 || !cancel;
              expect(decode.mock.calls).toHaveLength(0);
              reach.reach("unknown result not decoded");
              reach.reach(route === 0 ? "unknown before commit" : "unknown after commit call");
              if (cancel) reach.reach(committed ? "late cancel ignored" : "late cancel rolls back");
              reach.reach("late terminal collected");
              if (explicit)
                reach.reach(
                  commitMode === 1
                    ? "typed COMMIT"
                    : commitMode === 2
                      ? "SQL COMMIT"
                      : "prepared COMMIT",
                );
            } else if (route === 4) {
              f.client.giveUp("native request was never sent", cause);
              const result = await pending;
              expect(result.error).toBeInstanceOf(OwnerUnreachableError);
              expect(result.error).toMatchObject({ effect: "none" });
              await f.connect();
              reach.reach("never sent none");
            } else if (route === 5) {
              await terminal.promise;
              expect(f.packets.find((m) => m.t === "done")?.summary).toMatchObject({
                command: "SELECT",
                rowCount: count,
              });
              f.client.giveUp("read reply was lost", cause);
              const result = await pending;
              expect(result.error).toBeInstanceOf(OwnerUnreachableError);
              expect(result.error).toMatchObject({ effect: "unknown" });
              reach.reach("read retains reachability error");
            } else {
              await terminal.promise; // Real write completed; only its reply is withheld.
              if (cancel) stop.abort(reason);
              if (route === 3) f.advance(CLIENT_RETRY_WINDOW_MS + 1);
              f.drop(null);
              await f.reconnect();
              const result = await pending;
              if (cancel) {
                const resend = f.clientTrace.find(
                  (p) => p.connection === 2 && p.sequence === settled + 1,
                );
                expect(resend).toBeDefined();
                if (!resend?.terminalAtReturn) {
                  expect(
                    f.clientTrace.some(
                      (p) => p.connection === 2 && p.t === "cancel" && p.target === settled + 1,
                    ),
                  ).toBe(true);
                  reach.reach("cancel intent resent");
                }
              }
              committed = true;
              if (route === 3) {
                expect(result.value).toBeNull();
                expect(result.error).toBeInstanceOf(OutcomeUnknownError);
                expect(result.error).toMatchObject({ reason: "expired" });
                expect((result.error as OutcomeUnknownError).cause).toBeInstanceOf(
                  RequestExpiredError,
                );
                reach.reach("expired unknown");
              } else if (returning && !explicit) {
                expect(result.value).toBeNull();
                expect(result.error).toBeInstanceOf(CommittedWithoutResultError);
                expect(result.error).toMatchObject({ counts: { t: count } });
                reach.reach("committed without result");
              } else {
                expect(result.error).toBeNull();
                expect(result.value?.summary).toMatchObject({
                  replayed: true,
                  counts: { t: count },
                });
                expect(result.value?.rows()).toEqual([]);
                reach.reach("committed replay counts");
              }
              if (explicit) {
                expect(f.client.lostSessions.has(9)).toBe(route === 3);
                reach.reach(route === 3 ? "explicit expiry" : "explicit committed replay");
              }
            }
            if (
              f.packets.some((packet) => packet.t === "rows" && packet.frames.length > 0) &&
              returning &&
              route !== 5
            )
              reach.reach("RETURNING split before terminal");
            expect(f.client.pendingCount).toBe(0);
            expect(f.client.resolvedThrough).toBe(settled + 1);
            expect(f.client.hasPins).toBe(false);
            if (task) {
              expect(inputRetired).toBe(true);
              reach.reach("input retired after collection");
            }
            expect(await f.rows()).toEqual(
              Array.from({ length: count }, (_, i) => [i + 1, committed ? 1 + delta : 1]),
            );
            reach.reach(transport);
            reach.reach(returning ? "RETURNING" : "no RETURNING");
          } finally {
            decode.mockRestore();
            await f.close();
          }
          reach.reach("zero native charge");
        },
      ),
      {
        seed,
        numRuns: 44,
        examples: [
          ["direct", 0, 2, 1, true, true, 0, 1],
          ["channel", 1, 3, 2, true, true, 0, 2],
          ["direct", 2, 2, 3, true, true, 0, 1],
          ["channel", 2, 1, 1, false, false, 0, 2],
          ["direct", 3, 2, 1, true, true, 0, 1],
          ["channel", 4, 1, 1, false, false, 0, 2],
          ["direct", 1, 3, 2, true, true, 1, 1],
          ["channel", 1, 3, 2, false, true, 1, 2],
          ["direct", 1, 2, 1, true, true, 2, 1],
          ["channel", 1, 2, 1, false, false, 2, 2],
          ["direct", 1, 2, 1, false, true, 3, 1],
          ["channel", 1, 2, 1, true, false, 3, 2],
          ["direct", 5, 2, 1, false, false, 0, 1],
          ["channel", 5, 2, 1, false, false, 0, 2],
          ["channel", 2, 2, 1, true, true, 0, 1],
          ["channel", 3, 2, 1, false, true, 0, 2],
          ["direct", 2, 2, 1, true, true, 1, 1],
          ["channel", 2, 2, 1, true, true, 2, 2],
          ["channel", 3, 2, 1, false, true, 1, 1],
          ["direct", 3, 2, 1, true, true, 2, 2],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
