/** A deterministic native-root gate lets the real protocol cancel after accepted rows. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_SQL_CANCEL_REACH } from "../../testing/reach.js";
import * as commands from "../owner/native-command.js";
import { NativeCommandRoot } from "../owner/command-root.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
import { orderingScript } from "./trigger-ordering.test-support.js";
const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;
const queries = [
  "SELECT * FROM p ORDER BY id",
  "SELECT * FROM c ORDER BY id",
  "SELECT label,n FROM log ORDER BY label,n",
  "SELECT label,n FROM audit ORDER BY label,n",
  "SELECT * FROM trace",
];
for (const seed of seedsFor("next-native-trigger-sql-cancel", [46984, 46985]))
  it(`cancels and drains funded trigger SQL before commit (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("funded SQL cancellation", NATIVE_TRIGGER_SQL_CANCEL_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 4 }),
          fc.boolean(),
          fc.boolean(),
          async (rows, update, published) => {
            reach.beginHistory();
            const owner = await publicOwner({ capBytes }),
              db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
            let started: () => void = () => undefined;
            const entered = new Promise<void>((resolve) => {
                started = resolve;
              }),
              stop = new AbortController(),
              reason = { kind: "cancel accepted native rows" };
            const h = orderingScript(
              {
                rows,
                event: update ? "update" : "delete",
                beforeNames: ["B"],
                afterNames: ["zz_A"],
                twoBodies: true,
                reverseCreation: false,
                published,
              },
              "accumulator",
            );
            try {
              for (const sql of [...h.nativeSetup, ...h.seed]) await db.execute(sql);
              await pg.exec("DROP TABLE IF EXISTS c,p,log,audit,trace CASCADE");
              for (const sql of [...h.postgresSetup, ...h.seed]) await pg.exec(sql);
              if (published) {
                await owner.core.harness.path.recycleLog();
                await owner.core.harness.path.quiesce();
              }
              const execute = commands.executeNativeCommand;
              // Deliberately capture the original: the spy calls it with end.call(this) below.
              // eslint-disable-next-line @typescript-eslint/unbound-method
              const end = NativeCommandRoot.prototype.end;
              let signal: AbortSignal | null = null,
                blocked = false;
              vi.spyOn(commands, "executeNativeCommand").mockImplementation((request) => {
                signal = request.signal;
                return execute(request);
              });
              vi.spyOn(NativeCommandRoot.prototype, "end").mockImplementation(async function (
                this: NativeCommandRoot,
              ) {
                if (!blocked) {
                  blocked = true;
                  const active = signal;
                  if (!active) throw new Error("Missing native request signal");
                  started();
                  await new Promise<void>((resolve) => {
                    if (active.aborted) resolve();
                    else active.addEventListener("abort", () => resolve(), { once: true });
                  });
                  active.throwIfAborted();
                }
                await end.call(this);
              });
              const pending = db.execute(h.mutation, [], { signal: stop.signal }),
                observed = pending.then(
                  (value) => ({ value }),
                  (error: unknown) => ({ error }),
                );
              await Promise.race([
                entered,
                observed.then((outcome) => {
                  if ("error" in outcome) throw outcome.error;
                  throw new Error("Native root completed without reaching the cancellation gate");
                }),
              ]);
              reach.reach("cancel after earlier rows");
              stop.abort(reason);
              expect(await observed).toEqual({ error: reason });
              // The next request queues behind cancellation retirement, not a sleep/timing guess.
              for (const sql of queries)
                expect((await db.query(sql)).rows()).toEqual((await pg.query(sql)).rows);
              reach.reach("no committed effects");
              vi.restoreAllMocks();

              expect((await db.execute(h.mutation)).affectedRows).toBe(rows);
              await pg.exec(h.mutation);
              for (const sql of queries)
                expect((await db.query(sql)).rows()).toEqual((await pg.query(sql)).rows);
              reach.reach("transaction continues");
              reach.reach("exact PG rows");
              reach.reach(update ? "update" : "delete");
              reach.reach(published ? "published" : "resident");
            } finally {
              stop.abort(reason);
              vi.restoreAllMocks();
              await db.close();
              await owner.close();
              expect(owner.core.harness.governor.usedBytes).toBe(0);
              expect(owner.core.spill.bytesInUse).toBe(0);
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [2, true, false],
            [3, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
