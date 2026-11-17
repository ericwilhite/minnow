/** Deterministic owner-boundary gates: abort does not guess whether a write committed. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_FACADE_OUTCOME_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
function deferred() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
for (const seed of seedsFor("next-public-facade-outcomes", [0xb73c, 0xb73d]))
  it(`keeps terminal authority over abort, lost replies and cursor cancellation (${String(seed)})`, async () => {
    const reach = createReach("public facade outcomes", PUBLIC_FACADE_OUTCOME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 9999 }),
        fc.boolean(),
        async (value, returning) => {
          reach.beginHistory();
          const owner = await publicOwner();
          const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
          const cleanups: Array<() => void> = [];
          try {
            await db.execute("CREATE TABLE outcome(id INT PRIMARY KEY)");
            // Hold the actual writer slot. The next native begin is observed before cancelling;
            // no wall-clock sleep can allow it to cross the commit boundary by accident.
            const blocker = await owner.core.harness.path.begin();
            const entered = deferred();
            const begin = owner.core.harness.path.begin.bind(owner.core.harness.path);
            const before = vi
              .spyOn(owner.core.harness.path, "begin")
              .mockImplementation((options) => {
                entered.resolve();
                return begin(options);
              });
            const controller = new AbortController(),
              reason = { kind: "before commit" };
            const aborted = db.execute("INSERT INTO outcome VALUES($1)", [value], {
              signal: controller.signal,
            });
            const rejected = expect(aborted).rejects.toBe(reason);
            try {
              await entered.promise;
              controller.abort(reason);
              await rejected;
            } finally {
              before.mockRestore();
              blocker.rollback();
            }
            expect((await db.query("SELECT count(*) FROM outcome")).values(0)).toEqual([0]);
            reach.reach("abort before commit is no effect");

            const won = new AbortController();
            const commitSpies: Array<() => void> = [];
            const around = vi
              .spyOn(owner.core.harness.path, "begin")
              .mockImplementation(async (options) => {
                const tx = await begin(options),
                  commit = tx.commit.bind(tx);
                const spy = vi.spyOn(tx, "commit").mockImplementation((request) => {
                  // compose already set reachedCommit before invoking this native method.
                  won.abort({ kind: "too late" });
                  return commit(request);
                });
                commitSpies.push(() => spy.mockRestore());
                return tx;
              });
            try {
              const committed = await db.execute("INSERT INTO outcome VALUES($1)", [value], {
                signal: won.signal,
              });
              expect(committed.command).toBe("INSERT");
              expect(committed.affectedRows).toBe(1);
            } finally {
              around.mockRestore();
              for (const restore of commitSpies) restore();
            }
            reach.reach("commit wins over abort");

            const dropped = deferred(),
              post = owner.channel.port2.postMessage.bind(owner.channel.port2);
            let drop = true;
            owner.channel.port2.postMessage = (
              message: unknown,
              options?: Transferable[] | StructuredSerializeOptions,
            ) => {
              const reply = message as { t?: string; summary?: { command?: string } };
              if (drop && reply.t === "done" && reply.summary?.command === "INSERT") {
                drop = false;
                dropped.resolve();
                return;
              }
              if (Array.isArray(options)) post(message, options);
              else post(message, options);
            };
            cleanups.push(() => {
              owner.channel.port2.postMessage = post;
            });
            const retry = db.execute(
              `INSERT INTO outcome VALUES($1)${returning ? " RETURNING id" : ""}`,
              [value + 1],
            );
            // Attach rejection observation before reconnect can settle it.
            const settled = retry.then(
              (result) => ({ result }),
              (error: unknown) => ({ error }),
            );
            await dropped.promise;
            const channel = new MessageChannel();
            cleanups.push(() => {
              channel.port1.close();
              channel.port2.close();
            });
            owner.server.accept(channel.port2);
            await owner.client.reconnect(channel.port1);
            const outcome = await settled;
            if (returning) {
              expect(outcome).toMatchObject({ error: { code: "MINNOW_COMMITTED_WITHOUT_RESULT" } });
              reach.reach("lost RETURNING stays committed without result");
            } else {
              expect(outcome).toMatchObject({ result: { replayed: true, affectedRows: 1 } });
              reach.reach("lost reply ledger replay");
            }
            expect((await db.query("SELECT id FROM outcome ORDER BY id")).values(0)).toEqual([
              value,
              value + 1,
            ]);
            const cursorAbort = new AbortController(),
              cursorReason = { kind: "cursor" };
            const cursor = db.cursor("SELECT i FROM generate_series(1,9) s(i)", [], {
              frameRows: 1,
              prefetchBytes: 1,
              signal: cursorAbort.signal,
            });
            const first = await cursor.next();
            expect(first.done).toBe(false);
            cursorAbort.abort(cursorReason);
            let caught: unknown;
            try {
              while (!(await cursor.next()).done) {
                /* Drain already posted prefix before error. */
              }
            } catch (error) {
              caught = error;
            }
            expect(caught).toBe(cursorReason);
            await cursor.close();
            expect(first.done ? null : first.value.values(0)).toEqual([1]);
            expect(owner.client.hasPins).toBe(false);
            reach.reach("cursor abort preserves prefix and terminal reason");
          } finally {
            await db.close();
            for (const close of cleanups.reverse()) close();
            await owner.close();
            expect(owner.core.harness.governor.usedBytes).toBe(0);
            expect(owner.core.spill.bytesInUse).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, false],
          [2, true],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
