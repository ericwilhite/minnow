/** Native inspection respects the exact session generation it depended on. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, INSPECTION_RECONNECT_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
for (const seed of seedsFor("next-inspection-reconnect", [46970, 46971]))
  it(`retries idle metadata but refuses a lost private session (${String(seed)})`, async () => {
    const reach = createReach("inspection reconnect", INSPECTION_RECONNECT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 1, max: 8 }),
        async (transaction, columns) => {
          reach.beginHistory();
          const owner = await publicOwner({ planCacheBytes: 0 }),
            db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
          const next = new MessageChannel();
          const handler = owner.channel.port1.onmessage;
          try {
            await db.execute("CREATE TABLE committed(id INT)");
            if (transaction) {
              await db.execute("BEGIN");
              await db.execute(
                `CREATE TABLE private_ddl(${Array.from({ length: columns }, (_, i) => `c${String(i)} INT`).join(",")})`,
              );
            }
            let captured: () => void = () => undefined;
            const posted = new Promise<void>((resolve) => {
              captured = resolve;
            });
            owner.channel.port1.onmessage = (event) => {
              if ((event.data as { t?: string }).t === "value")
                captured(); // Deliberately lose this reply, no ACK.
              else handler?.call(owner.channel.port1, event);
            };
            const lost = db.describeCatalog().then(
              (value) => value,
              (error: unknown) => error,
            );
            await posted;
            owner.server.accept(next.port2);
            await owner.client.reconnect(next.port1);
            const outcome: unknown = await lost;
            if (transaction) {
              expect(outcome).toMatchObject({ code: "MINNOW_SESSION_LOST", lost: "transaction" });
              await expect(db.describeCatalog()).rejects.toMatchObject({
                code: "MINNOW_SESSION_LOST",
              });
              await db.execute("ROLLBACK");
              reach.reach("lost private inspection refuses");
            } else {
              expect(outcome).toMatchObject({ version: 1, tables: [{ name: "committed" }] });
              reach.reach("idle inspection resends");
            }
            expect((await db.describeCatalog()).tables.map((table) => table.name)).toEqual([
              "committed",
            ]);
            reach.reach("rollback restores inspection");
          } finally {
            owner.channel.port1.onmessage = handler;
            await db.close();
            await owner.close();
            next.port1.close();
            next.port2.close();
            expect(owner.core.harness.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, 1],
          [true, 8],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
