/** Ending callback ownership must not discard a COMMIT's durable replay authority. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  PUBLIC_SESSION_OUTCOME_REACH,
  PUBLIC_SESSION_FLAGS_REACH,
} from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

for (const seed of seedsFor("next-public-session-outcomes", [0xb768, 0xb769]))
  it(`keeps lost end replies exact and retires callback recovery state (${String(seed)})`, async () => {
    const reach = createReach("callback terminal outcomes", PUBLIC_SESSION_OUTCOME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 1, max: 999 }), async (commit, value) => {
        reach.beginHistory();
        const owner = await publicOwner({ planCacheBytes: 0 }),
          db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
        const wires: MessageChannel[] = [];
        const post = owner.channel.port2.postMessage.bind(owner.channel.port2);
        let notify: () => void = () => undefined;
        const dropped = new Promise<void>((resolve) => {
          notify = resolve;
        });
        let drop = true,
          bodies = 0;
        const bodyError = new Error("callback body failure");
        try {
          await db.execute("CREATE TABLE t(n INT PRIMARY KEY)");
          owner.channel.port2.postMessage = (
            message: unknown,
            options?: Transferable[] | StructuredSerializeOptions,
          ) => {
            const reply = message as { t?: string; summary?: { command?: string } };
            if (
              drop &&
              reply.t === "done" &&
              reply.summary?.command === (commit ? "COMMIT" : "ROLLBACK")
            ) {
              drop = false;
              notify();
              return;
            }
            if (Array.isArray(options)) post(message, options);
            else post(message, options);
          };
          const running = db.transaction(async (tx) => {
            bodies++;
            await tx.execute("INSERT INTO t VALUES($1)", [value]);
            if (!commit) throw bodyError;
            return value;
          });
          const settled = running.then(
            (result) => ({ result }),
            (error: unknown) => ({ error }),
          );
          await dropped;
          if (!commit) {
            owner.client.giveUp("lost callback rollback reply");
            expect(await settled).toEqual({ error: bodyError });
            expect(owner.client.lostSessions.size).toBe(0);
            reach.reach("failed rollback reply retires local recovery state");
          }
          const wire = new MessageChannel();
          wires.push(wire);
          owner.server.accept(wire.port2);
          await owner.client.reconnect(wire.port1);
          if (commit) {
            expect(await settled).toEqual({ result: value });
            reach.reach("lost COMMIT replays after retirement");
          }
          expect(bodies).toBe(1);
          expect((await db.query("SELECT n FROM t")).values(0)).toEqual(commit ? [value] : []);
          await owner.client.send({ t: "admin", op: "catalog", args: [] }).promise;
          expect(owner.client.retainedSessionCount).toBe(1); // Only default SQL session0.
          expect(owner.client.lostSessions.size).toBe(0);
          // The old connection's default-session generation fence stays protected.
          expect(owner.core.harness.path.ledger.endedSessionCount).toBe(1);
          expect(() => owner.client.retireSession(0)).toThrow("Only a callback session");
          reach.reach("callback body never replayed");
          reach.reach("default session is not retired");
        } finally {
          owner.channel.port2.postMessage = post;
          await db.close();
          await owner.close();
          for (const wire of wires) {
            wire.port1.close();
            wire.port2.close();
          }
        }
        expect(owner.core.harness.governor.usedBytes).toBe(0);
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [true, 7],
          [false, 9],
        ],
      },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-public-session-retire-flags", [0xb76a, 0xb76b]))
  it(`refuses malformed retirement before changing the default transaction (${String(seed)})`, async () => {
    const reach = createReach("callback retirement flags", PUBLIC_SESSION_FLAGS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("default", "missing", "null", "wrongflag"),
        async (shape) => {
          reach.beginHistory();
          const owner = await publicOwner({ planCacheBytes: 0 });
          try {
            await owner.client.begin(0, true);
            const invalid: Record<string, unknown> = { t: "commit", session: 0, retire: true };
            if (shape === "missing") delete invalid.session;
            if (shape === "null") invalid.session = null;
            if (shape === "wrongflag") invalid.retire = "yes";
            await expect(
              owner.client.send(invalid as unknown as Parameters<typeof owner.client.send>[0])
                .promise,
            ).rejects.toMatchObject({ code: "MINNOW_PROTOCOL" });
            expect(owner.client.hasPins).toBe(true);
            expect((await owner.client.query("SELECT 1 AS n")).rows()).toEqual([[1]]);
            expect(owner.client.hasPins).toBe(true);
            await expect(owner.client.commit(0)).resolves.toMatchObject({
              summary: { command: "COMMIT" },
            });
            expect(owner.client.hasPins).toBe(false);
            reach.reach(shape);
          } finally {
            await owner.close();
          }
          expect(owner.core.harness.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 8, examples: [["default"], ["missing"], ["null"], ["wrongflag"]] },
    );
    reach.assertReached();
  }, 60000);
