/** Native trigger bodies run their input queries on prepaid shares (owner/prepaid-query.ts): a
 * producer that holds a share may not admit again, so no sizing pass may hold a segment list
 * across the source sizing that follows it. Point and range lookups inside nested AFTER bodies
 * must run, with or without a competitor that leaves no spare credit. (A key-order window never
 * reaches a body: its input plans have a modify root, which orderedSort declines.) */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_BODY_SIZING_REACH } from "../../testing/reach.js";
import { PrepaidQueryGovernor } from "../owner/prepaid-query.js";
import { commandRootFixture } from "./command-root.test-support.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-native-body-sizing", [0xb7e0, 0xb7e1]))
  it(`runs point and range lookups inside nested AFTER bodies seed ${String(seed)}`, async () => {
    const reach = createReach("native body sizing", NATIVE_BODY_SIZING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 40 }),
        fc.integer({ min: 1, max: 12 }),
        fc.integer({ min: 0, max: 5 }),
        fc.boolean(),
        fc.boolean(),
        async (count, low, span, published, pressure) => {
          reach.beginHistory();
          const f = await commandRootFixture(
            [
              "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
              "CREATE TABLE audit(id INT PRIMARY KEY,v INT)",
              "CREATE TABLE sink(id INT PRIMARY KEY,n INT)",
              "CREATE TABLE trace(id INT PRIMARY KEY,v TEXT)",
              "INSERT INTO audit VALUES(1,0),(2,0)",
              "INSERT INTO trace VALUES(1,'')",
              `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},0)`).join(",")}`,
              `INSERT INTO sink VALUES${Array.from({ length: 12 }, (_, i) => `(${String(i + 1)},0)`).join(",")}`,
              // Nested AFTER bodies: a point lookup, then a point lookup and a primary-key range
              // lookup nested under it.
              "CREATE TRIGGER p_after AFTER UPDATE ON p BEGIN UPDATE audit SET v=v+1 WHERE id=1; END",
              "CREATE TRIGGER audit_after AFTER UPDATE ON audit BEGIN UPDATE trace SET v=v||CAST(NEW.v AS TEXT)||';' WHERE id=1; " +
                `UPDATE sink SET n=n+1 WHERE id BETWEEN ${String(low)} AND ${String(low + span)}; END`,
            ],
            "UPDATE p SET v=v+1",
            published,
            64 << 20,
          );
          const gov = f.core.harness.governor;
          const competitor = pressure
            ? await gov.admit({
                label: "native body competitor",
                minimumBytes: Math.max(
                  0,
                  gov.capBytes - gov.reserveBytes - gov.usedBytes - (1 << 20),
                ),
                spillable: false,
              })
            : null;
          if (competitor !== null) expect(competitor.allocate(competitor.held)).toBe(true);
          // Every admission a native body makes, by label.
          const labels: string[] = [];
          const group: PrepaidQueryGovernor["admitGroup"] = Reflect.get(
            PrepaidQueryGovernor.prototype,
            "admitGroup",
          );
          const admissions = vi
            .spyOn(PrepaidQueryGovernor.prototype, "admitGroup")
            .mockImplementation(function (this: PrepaidQueryGovernor, options) {
              labels.push(options.label, ...options.grants.map((part) => part.label));
              return group.call(this, options);
            });
          try {
            expect(await f.root.run({ slots: [] })).toBe(count);
            await f.commit();
            competitor?.close();
            await f.retire();
            reach.reach(published ? "published" : "resident");
            if (pressure) reach.reach("competitor holds the spare credit");
            expect(labels.length).toBeGreaterThan(0);
            reach.reach("native body admitted");
            // Native bodies size the whole source: no sizing pass holds a list across admission.
            expect(labels.filter((l) => /^(index-lookup|key-order)\//u.test(l))).toEqual([]);
            const trace = await f.core.execute("SELECT v FROM trace");
            if (!("batches" in trace)) throw Error("trace");
            expect(oracleRows(trace)).toEqual([
              { v: Array.from({ length: count }, (_, i) => `${String(i + 1)};`).join("") },
            ]);
            reach.reach("point lookup in nested AFTER");
            const sink = await f.core.execute("SELECT id,n FROM sink ORDER BY id");
            if (!("batches" in sink)) throw Error("sink");
            expect(oracleRows(sink)).toEqual(
              Array.from({ length: 12 }, (_, i) => ({
                id: i + 1,
                n: i + 1 >= low && i + 1 <= low + span ? count : 0,
              })),
            );
            reach.reach(
              low > 12 - span ? "range lookup past the last key" : "range lookup in nested AFTER",
            );
          } finally {
            admissions.mockRestore();
            competitor?.close();
            await f.close();
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [32, 1, 3, false, true],
          [8, 2, 5, true, false],
          [3, 11, 4, false, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
