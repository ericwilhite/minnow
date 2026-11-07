/** A cancelled execution stops its input walk at the new sequence backpressure suspension. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, OWNER_SEQUENCE_PRESSURE_STATES } from "../../testing/reach.js";
import { cleanups, harness } from "./owner-harness.js";

const reach = createReach("owner sequence pressure cancellation", OWNER_SEQUENCE_PRESSURE_STATES);
for (const seed of seedsFor("next-owner-sequence-pressure", [0x51a]))
  it(`stops a cancelled batched INSERT at its sequence wait (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({ rows: fc.integer({ min: 2, max: 8 }), at: fc.nat() }),
        async (s) => {
          reach.beginHistory();
          const stop = 1 + (s.at % s.rows);
          reach.reach(stop === s.rows ? "last value" : "remaining values");
          const cleanupAt = cleanups.length;
          const { connect, writePath } = harness();
          const client = await connect();
          let release: () => void = () => undefined;
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          try {
            await client.query("CREATE SEQUENCE s");
            await client.query("CREATE TABLE t (id BIGINT PRIMARY KEY, n INT)");
            let entered: () => void = () => undefined;
            const waiting = new Promise<void>((resolve) => {
              entered = resolve;
            });
            const next = writePath.sequences.next.bind(writePath.sequences);
            let calls = 0;
            writePath.sequences.next = (...args) => {
              calls++;
              if (calls !== stop) return next(...args);
              entered();
              return gate.then(() => next(...args));
            };
            const insert = client.send({
              t: "statement",
              session: 0,
              text: `INSERT INTO t VALUES ${Array.from({ length: s.rows }, (_, k) => `(nextval('s'), ${String(k)})`).join(", ")} RETURNING id`,
              prepared: null,
              knownColumnsVersion: -1,
              params: [],
              result: { windowBytes: 1024, frameRows: null, wantRows: true },
            });
            const outcome = insert.promise.then(
              () => "committed",
              () => "cancelled",
            );
            await waiting;
            expect(calls).toBe(stop);
            client.cancel(insert.seq);
            expect(await outcome).toBe("cancelled");
            release();
            // A later message is a task barrier: the suspended interpreter has resumed by its reply.
            expect((await client.query("SELECT * FROM t")).rows()).toEqual([]);
            expect(calls, "cancelled interpreter evaluated another sequence value").toBe(stop);
            reach.reach("no later sequence call");
            reach.reach("statement rolled back");
          } finally {
            release();
            client.close();
            for (const cleanup of cleanups.splice(cleanupAt)) cleanup();
          }
        },
      ),
      { seed, numRuns: 24 },
    );
  }, 120_000);
it("reaches cancellation before and at the final value", () => reach.assertReached());
