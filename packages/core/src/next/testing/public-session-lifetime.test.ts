/** Callback sessions are ephemeral; closing never waits for a user callback awaiting close. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  PUBLIC_SESSION_RETIRE_REACH,
  PUBLIC_REENTRANT_CLOSE_REACH,
} from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { ProtocolClient } from "../owner/client.js";
import { IdleTransactionTimer, type EngineSession } from "../owner/engine.js";
import type { Cursor, SqlExecutor } from "../owner/api.js";
import { publicOwner } from "./public-owner.test.fixture.js";
function gate() {
  let resolve: () => void = () => undefined,
    reject: (reason: unknown) => void = () => undefined;
  const promise = new Promise<void>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}

for (const seed of seedsFor("next-public-session-retirement", [0xb762, 0xb763]))
  it(`bounds sequential/concurrent callback sessions and credit (${String(seed)})`, async () => {
    const reach = createReach("callback session retirement", PUBLIC_SESSION_RETIRE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 20, max: 28 }),
        fc.integer({ min: 1, max: 4 }),
        fc.boolean(),
        fc.boolean(),
        async (rounds, concurrent, fail, prepare) => {
          reach.beginHistory();
          const owner = await publicOwner({ planCacheBytes: 0 });
          const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
          const active = new Set<EngineSession>(),
            open = owner.engine.openSession.bind(owner.engine);
          let completed = 0,
            baseline: number | undefined;
          const opened = vi.spyOn(owner.engine, "openSession").mockImplementation((...args) => {
            const session = open(...args),
              close = session.close.bind(session);
            active.add(session);
            session.close = () => {
              try {
                close();
              } finally {
                active.delete(session);
              }
            };
            return session;
          });
          const dispose = vi.spyOn(IdleTransactionTimer.prototype, "dispose");
          try {
            for (let round = 0; round < rounds; round++) {
              await Promise.all(
                Array.from({ length: concurrent }, async (_, n) => {
                  const task = db.transaction(
                    async (tx) => {
                      if (prepare) {
                        const statement = await tx.prepare("SELECT $1::integer n");
                        expect((await statement.query([n])).values(0)).toEqual([n]);
                      } else
                        expect((await tx.query("SELECT $1::integer n", [n])).values(0)).toEqual([
                          n,
                        ]);
                      if (fail && n % 2 === 0) throw new Error("body rollback");
                    },
                    { readOnly: true },
                  );
                  if (fail && n % 2 === 0) await expect(task).rejects.toThrow("body rollback");
                  else await task;
                  completed++;
                }),
              );
              await owner.client.send({ t: "admin", op: "catalog", args: [] }).promise;
              expect(active.size).toBe(0);
              expect(owner.server.sessionCount).toBe(1); // Only the stateless admin/default slot.
              expect(owner.client.retainedSessionCount).toBe(0);
              expect(owner.client.retainedPreparedCount).toBe(0);
              expect(owner.core.harness.path.ledger.endedSessionCount).toBe(0);
              expect(dispose.mock.calls.length).toBe(completed);
              baseline ??= owner.core.harness.governor.usedBytes;
              expect(owner.core.harness.governor.usedBytes).toBe(baseline);
            }
            reach.reach(concurrent === 1 ? "sequential callbacks" : "concurrent callbacks");
            reach.reach(fail ? "rollback retires session" : "commit retires session");
            reach.reach(prepare ? "scoped preparations retired" : "unprepared callbacks");
            reach.reach("twenty or more callback rounds");
            reach.reach("zero retained callback credit");
          } finally {
            await db.close();
            await owner.close();
            opened.mockRestore();
            dispose.mockRestore();
          }
          expect(owner.core.harness.governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [20, 1, false, false],
          [24, 4, true, true],
          [20, 2, false, true],
          [20, 1, true, false],
        ],
      },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-public-reentrant-close", [0xb764, 0xb765]))
  it(`closes from callbacks/savepoints/cursor reads without self-wait (${String(seed)})`, async () => {
    const reach = createReach("reentrant facade close", PUBLIC_REENTRANT_CLOSE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("body", "savepoint", "cursor"),
        fc.integer({ min: 0, max: 3 }),
        async (kind, reads) => {
          reach.beginHistory();
          const owner = await publicOwner({ planCacheBytes: 0 }),
            observer = new ProtocolClient(),
            wire = new MessageChannel();
          owner.server.accept(wire.port2);
          await observer.connect(wire.port1);
          const entered = gate(),
            resume = gate();
          let teardown = false,
            closing: Promise<void> | undefined,
            scope: SqlExecutor | undefined,
            cursor: Cursor | undefined,
            closedQuery: Promise<unknown> | undefined;
          const db = new MinnowDatabaseClient(owner.client, {
            ready: Promise.resolve(),
            close: async () => {
              teardown = true;
            },
          });
          const running = db.transaction(
            async (tx) => {
              scope = tx;
              if (kind === "savepoint") await tx.execute("SAVEPOINT inside_callback");
              for (let i = 0; i < reads; i++) await tx.query("SELECT 1");
              if (kind === "cursor") {
                cursor = tx.cursor("SELECT i FROM generate_series(1,9) s(i)", [], {
                  frameRows: 1,
                  prefetchBytes: 1,
                });
                expect((await cursor.next()).done).toBe(false);
              }
              closing = db.close();
              expect(db.status).toBe("closed");
              closedQuery = tx.query("SELECT 1").then(
                () => false,
                (error: unknown) => error,
              );
              entered.resolve();
              // Cleanup can break a mutant's actual callback->close->callback cycle.
              await Promise.race([closing, resume.promise]);
              await closing;
            },
            { readOnly: true },
          );
          const settled = running.then(
            () => null,
            (error: unknown) => error,
          );
          try {
            await entered.promise;
            // Independent protocol barriers prove progress without a wall-clock expiry oracle.
            const teardownComplete = (): boolean => teardown;
            for (let i = 0; i < 16 && !teardownComplete(); i++)
              await observer.send({ t: "admin", op: "catalog", args: [] }).promise;
            expect(teardown).toBe(true);
            expect(await closedQuery).toMatchObject({ code: "MINNOW_CLIENT_CLOSED" });
            expect(db.status).toBe("closed");
            if (!scope) throw new Error("No callback scope");
            const closed = { code: "MINNOW_CLIENT_CLOSED" };
            await expect(scope.query("SELECT 1")).rejects.toMatchObject(closed);
            await expect(scope.execute("SELECT 1")).rejects.toMatchObject(closed);
            await expect(scope.prepare("SELECT 1")).rejects.toMatchObject(closed);
            await expect(scope.insert("t", [])).rejects.toMatchObject(closed);
            await expect(scope.upsert("t", [])).rejects.toMatchObject(closed);
            await expect(scope.update("t", [])).rejects.toMatchObject(closed);
            await expect(scope.delete("t", [])).rejects.toMatchObject(closed);
            expect(() => scope?.cursor("SELECT 1")).toThrow(expect.objectContaining(closed));
            if (cursor) await expect(cursor.next()).rejects.toMatchObject(closed);
            resume.resolve();
            expect(await settled).toMatchObject(closed);
            expect(db.close()).toBe(closing);
            reach.reach(`${kind} closes without waiting on itself`);
            reach.reach("closed scope rejects every entry");
          } finally {
            resume.reject(new Error("test cleanup"));
            await settled;
            await db.close();
            observer.close();
            wire.port1.close();
            wire.port2.close();
            await owner.close();
          }
          expect(owner.core.harness.governor.usedBytes).toBe(0);
          reach.reach("reentrant teardown drains native state");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          ["body", 0],
          ["savepoint", 1],
          ["cursor", 3],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
