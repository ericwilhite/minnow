/** Generated outputs keep one binding and borrow fence, even when a wide row needs spill progress. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_RULE_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
it.each(seedsFor("next-modify-rule-pressure", [0xb5c5]))(
  "streams wide generated updates at 64 MiB (seed %i)",
  async (seed) => {
    const core = new StorageCore(256),
      governor = new Governor(64 * 1024 * 1024);
    Object.defineProperty(core.harness, "governor", { value: governor });
    const reach = createReach("generated pressure", MODIFY_RULE_PRESSURE_REACH);
    const admit = governor.tryAdmit.bind(governor);
    governor.tryAdmit = (options) => {
      if (options.label.startsWith("query#") && options.label.endsWith("/fixed-buffers")) {
        expect(
          options.minimumBytes + governor.usedBytes,
          "unfunded generated producer minimum",
        ).toBeLessThanOrEqual(governor.capBytes);
        reach.reach("funded admission");
      }
      return admit(options);
    };
    await core.open();
    try {
      await core.execute(
        "CREATE TABLE wide_rule(id INTEGER PRIMARY KEY,note TEXT NOT NULL,g TEXT GENERATED ALWAYS AS(CASE WHEN id>0 THEN note ELSE NULL END) STORED NOT NULL,CHECK(id>0))",
      );
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 65537, max: 600000 }), async (length) => {
          reach.beginHistory();
          await core.execute("DELETE FROM wide_rule");
          const values = [
            "a".repeat(length),
            "λ".repeat(Math.floor(length / 2)),
            "z".repeat(length - 17),
          ];
          await core.execute("INSERT INTO wide_rule(id,note) VALUES(1,$1),(2,$2),(3,$3)", values);
          reach.reach("wide generated insert");
          for (let i = 0; i < 2; i++) {
            await core.harness.path.quiesce();
            const rows = await core.execute("UPDATE wide_rule SET note=note RETURNING id,note,g");
            if (!("returning" in rows)) throw new Error("returning");
            expect(oracleRows(rows.returning)).toEqual(
              values.map((note, k) => ({ id: k + 1, note, g: note })),
            );
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("wide generated update");
          }
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          reach.reach("64 MiB");
          reach.reach("spill cleanup");
        }),
        { seed, numRuns: 5, examples: [[70000], [250000], [600000]] },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
    expect(governor.usedBytes).toBe(0);
  },
  120000,
);
