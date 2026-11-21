/**
 * Native trigger writes over values from a few bytes to several MiB, through the public path
 * (production funding), with or without a competitor that holds all ordinary memory but 1 MiB
 * and never releases. A body binds the NEW (and OLD) value and copies it into another table.
 * Law (GOVERNOR-WAIT-POLICY: grant, cancellable wait, or spill; never an untyped internal error):
 * each write either completes with exactly the expected rows, waits for memory, or is refused
 * with 53200 because its need is above the cap. A waiting write ends promptly on cancellation with
 * the caller's AbortError and leaves nothing behind, and once the competitor is gone it completes.
 * A refused write leaves nothing behind. Any other outcome fails.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_VALUE_WIDTH_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

const MiB = 1 << 20;

for (const seed of seedsFor("next-native-trigger-value-width", [53011, 53012]))
  it(`binds values of every width in native trigger bodies (${String(seed)})`, async () => {
    const reach = createReach("native trigger value width", NATIVE_TRIGGER_VALUE_WIDTH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(8, 64 << 10, MiB, 2 * MiB, 6 * MiB),
        fc.boolean(),
        fc.constantFrom("UPDATE", "DELETE"),
        async (width, pressure, event) => {
          reach.beginHistory();
          const owner = await publicOwner({ capBytes: 64 << 20 });
          const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
          const governor = owner.core.harness.governor;
          let competitor: Awaited<ReturnType<typeof governor.admit>> | null = null;
          const value = "v".repeat(width);
          try {
            await db.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
            await db.execute("CREATE TABLE copy(id INT, before TEXT, after TEXT)");
            await db.execute(
              event === "UPDATE"
                ? "CREATE TRIGGER keep AFTER UPDATE ON t BEGIN INSERT INTO copy VALUES(NEW.id, OLD.v, NEW.v); END"
                : "CREATE TRIGGER keep AFTER DELETE ON t BEGIN INSERT INTO copy VALUES(OLD.id, OLD.v, NULL); END",
            );
            await db.execute("INSERT INTO t VALUES(1, $1)", [value]);
            reach.reach(
              width <= 64 << 10
                ? "small value"
                : width <= 2 * MiB
                  ? "value of a MiB or two"
                  : "value of several MiB",
            );
            if (event === "UPDATE") reach.reach("OLD and NEW bound");
            if (pressure) {
              competitor = await governor.admit({
                label: "competitor",
                minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes - MiB,
                spillable: false,
              });
              expect(competitor.allocate(competitor.held)).toBe(true);
              reach.reach("competitor holds all but 1 MiB");
            }
            const statement = event === "UPDATE" ? "UPDATE t SET v = v || 'b'" : "DELETE FROM t";
            const untouched = async () => {
              expect((await db.query("SELECT count(*) AS n FROM copy")).rows()).toEqual([{ n: 0 }]);
              expect((await db.query("SELECT id FROM t WHERE v = $1", [value])).rows()).toEqual([
                { id: 1 },
              ]);
            };
            /** Completed, or the one lawful refusal: a need above the cap itself (53200). */
            const lawful = (outcome: unknown): "completed" | "refused" => {
              if (outcome === "completed") return "completed";
              expect(outcome, `write over a ${String(width)}-byte value`).toMatchObject({
                sqlState: "53200",
              });
              const refused = outcome as { bytes: number; capacity: number };
              expect(refused.bytes).toBeGreaterThan(refused.capacity);
              return "refused";
            };
            const stop = new AbortController();
            const run = db.execute(statement, [], { signal: stop.signal }).then(
              () => "completed" as const,
              (error: unknown) => error,
            );
            let outcome = await Promise.race([
              run,
              new Promise((resolve) => setTimeout(() => resolve("waiting"), 3_000)),
            ]);
            if (outcome === "waiting") {
              // Only a write waiting for memory a holder keeps may still be running.
              expect(pressure, "a write waits only behind a holder").toBe(true);
              const cancelledAt = performance.now();
              stop.abort();
              const cancelled = await run;
              expect(performance.now() - cancelledAt).toBeLessThan(2_000);
              expect(cancelled).toBe(stop.signal.reason);
              competitor?.close();
              await untouched();
              reach.reach("waited and was cancelled");
              // With the holder gone, the write runs (or is refused, if it needs more than the cap).
              outcome = await db.execute(statement).then(
                () => "completed" as const,
                (error: unknown) => error,
              );
            }
            competitor?.close();
            if (lawful(outcome) === "refused") {
              await untouched();
              reach.reach("refused above the cap");
              return;
            }
            if (!stop.signal.aborted)
              reach.reach(
                pressure ? "completed beside the competitor" : "completed without pressure",
              );
            expect((await db.query("SELECT count(*) AS n FROM copy")).rows()).toEqual([{ n: 1 }]);
            const kept =
              event === "UPDATE"
                ? (
                    await db.query("SELECT id FROM copy WHERE before = $1 AND after = $2", [
                      value,
                      `${value}b`,
                    ])
                  ).rows()
                : (
                    await db.query("SELECT id FROM copy WHERE before = $1 AND after IS NULL", [
                      value,
                    ])
                  ).rows();
            expect(kept).toEqual([{ id: 1 }]);
          } finally {
            competitor?.close();
            await db.close();
            await owner.close();
          }
          expect(governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [MiB, true, "UPDATE"],
          [2 * MiB, true, "DELETE"],
          [6 * MiB, true, "UPDATE"],
          [8, false, "UPDATE"],
        ],
      },
    );
    reach.assertReached();
  }, 600_000);
