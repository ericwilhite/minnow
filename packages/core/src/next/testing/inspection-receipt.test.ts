/** Posted metadata remains charged until receipt, cancellation or connection retirement. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, INSPECTION_RECEIPT_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
for (const seed of seedsFor("next-inspection-receipt", [46968, 46969]))
  it(`retires queued metadata on cancel and disconnect (${String(seed)})`, async () => {
    const reach = createReach("inspection receipt", INSPECTION_RECEIPT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 1, max: 16 }),
        async (disconnect, columns) => {
          reach.beginHistory();
          const owner = await publicOwner({ planCacheBytes: 0 }),
            db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
          const governor = owner.core.harness.governor;
          const incoming = owner.channel.port1.onmessage,
            server = owner.channel.port2.onmessage;
          const receipt: { held: MessageEvent | null } = { held: null };
          try {
            await db.execute(
              `CREATE TABLE t(${Array.from({ length: columns }, (_, i) => `c${String(i)} INTEGER`).join(",")})`,
            );
            await db.catalog();
            const baseline = governor.usedBytes;
            let captured: () => void = () => undefined,
              retired: () => void = () => undefined;
            const posted = new Promise<void>((resolve) => {
              captured = resolve;
            });
            const terminal = new Promise<void>((resolve) => {
              retired = resolve;
            });
            owner.channel.port1.onmessage = (event) => {
              if ((event.data as { t?: string }).t === "value") {
                receipt.held = event;
                captured();
              } else incoming?.call(owner.channel.port1, event);
            };
            owner.channel.port2.onmessage = (event) => {
              server?.call(owner.channel.port2, event);
              const kind = (event.data as { t?: string }).t;
              if (kind === (disconnect ? "bye" : "cancel")) retired();
            };
            const abort = new AbortController();
            const receiving = db.describeCatalog({ signal: abort.signal });
            // Observe failure before triggering close; no unhandled abandoned-body rejection.
            const outcome = receiving.then(
              (value) => value,
              (error: unknown) => error,
            );
            await posted;
            expect(governor.usedBytes).toBeGreaterThan(baseline);
            if (disconnect) owner.client.close();
            else abort.abort(new Error("cancel after post"));
            await terminal;
            expect(governor.usedBytes).toBeLessThanOrEqual(baseline);
            if (disconnect) {
              expect(await outcome).toBeInstanceOf(Error);
              reach.reach("connection end releases reply");
            } else {
              if (receipt.held === null) throw new Error("Missing queued reply");
              owner.channel.port1.onmessage = incoming;
              incoming?.call(owner.channel.port1, receipt.held);
              // The owner already posted its successful read; cancellation retires its
              // credit but does not invent a write effect or erase the received snapshot.
              expect(await outcome).toMatchObject({ version: 1, tables: [{ name: "t" }] });
              await db.catalog();
              reach.reach("cancel posted reply releases credit");
            }
            reach.reach("paused receipt retains credit");
          } finally {
            owner.channel.port1.onmessage = incoming;
            owner.channel.port2.onmessage = server;
            await db.close();
            await owner.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, 1],
          [true, 16],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
