/** Governor rules2/3/7: idle write forecasts cannot indefinitely block a fitting reader. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_QUERY_RECLAIM_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { WriteCredits } from "../write/credits.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { MinnowDatabase } from "../owner/database.js";
import { yieldTask } from "../task.js";

const MiB = 1 << 20;
// B8's existing SQL route asks this much. If B9 reduces that estimate, real reader-owned
// padding preserves the admission law without pinning the executor's oversized estimate.
const readerMinimum = 44_170_564;
for (const seed of seedsFor("next-write-query-reclaim", [47225, 47226]))
  it(`admits a fitting reader after native writes retire their forecasts (seed ${String(seed)})`, async () => {
    const reach = createReach("native reader after write", WRITE_QUERY_RECLAIM_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(52, 56, 60, 64),
        fc.integer({ min: 0, max: 4 }),
        fc.constantFrom("after", "changed", "水🙂", ""),
        fc.boolean(),
        async (cap, extra, value, transaction) => {
          reach.beginHistory();
          reach.reach(
            cap === 52 ? "52 MiB" : cap === 56 ? "56 MiB" : cap === 60 ? "60 MiB" : "64 MiB",
          );
          const stop = new AbortController(),
            state = { active: true, observe: false, blocked: 0 };
          let db: Awaited<ReturnType<typeof MinnowDatabase.open>> | undefined;
          let credits: WriteCredits | undefined;
          let pending: Promise<unknown> | undefined, closing: Promise<void> | undefined;
          const padding: Array<{ grant: Grant; buffer?: Uint8Array }> = [];
          const releasePadding = () => {
            for (const held of padding.splice(0)) {
              if (held.buffer && held.buffer.length > 0) expect(held.buffer.at(-1)).toBe(23);
              delete held.buffer;
              held.grant.free(held.grant.allocated);
              held.grant.close();
            }
          };
          const open = WriteCredits.open.bind(WriteCredits);
          const capture = vi.spyOn(WriteCredits, "open").mockImplementation(async (...args) => {
            credits = await open(...args);
            return credits;
          });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with the original governor below.
          const admit = Governor.prototype.admitGroup;
          const admission = vi.spyOn(Governor.prototype, "admitGroup").mockImplementation(function (
            this: Governor,
            options,
          ) {
            if (
              !state.observe ||
              !options.label.startsWith("query#") ||
              !options.label.endsWith("/fixed-buffers")
            )
              return admit.call(this, options);
            const original = options.grants.reduce((n, g) => n + g.minimumBytes, 0),
              additional = Math.max(0, readerMinimum + extra * MiB - original),
              total = original + additional;
            expect(
              total,
              "the reader's complete minimum fits the unchanged cap",
            ).toBeLessThanOrEqual(this.capBytes);
            if (!credits) throw new Error("Missing writer credits");
            const accounts = [credits.rows, credits.metadata, credits.commits, credits.merge];
            const unused = accounts.reduce(
              (n, g) => n + Math.max(0, g.held - g.allocated - g.loaned),
              0,
            );
            expect(
              this.usedBytes - unused + total,
              "reader plus all actual ownership fits together",
            ).toBeLessThanOrEqual(this.capBytes);
            reach.reach("reader and live ownership fit together");
            const request = admit.call(this, {
              ...options,
              grants: [
                ...options.grants,
                {
                  label: `${options.label}/reader-fixture`,
                  minimumBytes: additional,
                  spillable: false,
                },
              ],
            });
            return (async () => {
              const done = { value: false };
              void request.then(
                () => {
                  done.value = true;
                },
                () => {
                  done.value = true;
                },
              );
              for (let turn = 0; turn < 16 && !done.value; turn++) await yieldTask();
              if (state.active && !done.value) {
                state.blocked++;
                // Cleanup only: make the parent fail by bounded-progress assertion,
                // not timeout. Actual allocations and loans remain owned.
                for (const grant of accounts) {
                  const idle = Math.max(0, grant.held - grant.allocated - grant.loaned);
                  if (idle > 0) grant.release(idle);
                }
              }
              const grants = await request;
              if (!state.active || stop.signal.aborted) {
                for (const grant of grants) grant.close();
                throw stop.signal.reason ?? new Error("Admission history closed");
              }
              const grant = grants.at(-1);
              if (!grant) throw new Error("Missing reader fixture grant");
              const held: { grant: Grant; buffer?: Uint8Array } = { grant };
              padding.push(held);
              expect(grant.allocate(additional)).toBe(true);
              held.buffer = new Uint8Array(additional).fill(23);
              reach.reach(extra === 0 ? "reported reader minimum" : "larger real reader workspace");
              return grants.slice(0, -1);
            })();
          });
          const close = () =>
            (closing ??= (async () => {
              state.active = state.observe = false;
              stop.abort(new Error("Native write/query history ended"));
              releasePadding();
              if (pending) await Promise.allSettled([pending]);
              await db?.close();
              capture.mockRestore();
              admission.mockRestore();
            })());
          onTestFinished(close);
          try {
            db = await MinnowDatabase.open({
              backend: new MemoryBackend(),
              memoryLimitBytes: cap * MiB,
              signal: stop.signal,
            });
            await db.execute(
              "CREATE TABLE person_record(id INT PRIMARY KEY,first_name TEXT,amount NUMERIC)",
            );
            await db.execute(
              "INSERT INTO person_record VALUES(1,'before',12.3400),(2,'later',22.0000)",
            );
            const database = db;
            const read = async () => {
              state.observe = true;
              try {
                const query = database.query(
                  "SELECT * FROM person_record ORDER BY id LIMIT $1",
                  [1],
                  { signal: stop.signal },
                );
                pending = query;
                const result = await query;
                return result.rows();
              } finally {
                state.observe = false;
                pending = undefined;
                releasePadding();
              }
            };
            expect(await read()).toEqual([{ id: 1, first_name: "before", amount: "12.3400" }]);
            expect(
              state.blocked,
              "idle writer retention must not block the first fitting query",
            ).toBe(0);
            reach.reach("initial exact rows");
            if (transaction) await db.execute("BEGIN");
            reach.reach(transaction ? "idle explicit transaction" : "autocommit writer");
            await db.execute("UPDATE person_record SET first_name=$1 WHERE id=1", [value]);
            reach.reach("accepted keyed mutation");
            expect(await read()).toEqual([{ id: 1, first_name: value, amount: "12.3400" }]);
            expect(
              state.blocked,
              "a fitting post-write reader must not wait for idle writer forecasts",
            ).toBe(0);
            reach.reach("post-write query progressed");
            reach.reach("exact updated rows");
            await db.execute("UPDATE person_record SET first_name='continued' WHERE id=2");
            reach.reach("continued writes after reclamation");
            if (transaction) await db.execute("COMMIT");
          } finally {
            await close();
          }
          expect(credits?.governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 17,
        examples: [
          [64, 0, "after", false],
          [64, 0, "after", true],
          [56, 4, "水🙂", false],
          [56, 4, "水🙂", true],
          [60, 2, "", false],
          [60, 2, "", true],
          [52, 4, "after", false],
          [52, 4, "after", true],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  }, 60000);
