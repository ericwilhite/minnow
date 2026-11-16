/** RETURNING owns copied values, independent of retired scans and the consumer's credit window. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_RETURNING_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-returning-pressure", [0xb5b9]))(
  "spills wide RETURNING at 64 MiB (seed %i)",
  async (seed) => {
    const core = new StorageCore(256);
    Object.defineProperty(core.harness, "governor", { value: new Governor(64 * 1024 * 1024) });
    const reach = createReach("wide RETURNING", MODIFY_RETURNING_PRESSURE_REACH);
    const governor = core.harness.governor;
    const immediate = governor.tryAdmit.bind(governor);
    governor.tryAdmit = (options) => {
      if (options.label.startsWith("query#") && options.label.endsWith("/fixed-buffers")) {
        expect(
          options.minimumBytes + governor.usedBytes,
          "unfunded source minimum beside write owner",
        ).toBeLessThanOrEqual(governor.capBytes);
        reach.reach("funded before admission");
      }
      return immediate(options);
    };
    await core.open();
    try {
      await core.execute("CREATE TABLE pressure (id INTEGER PRIMARY KEY,note TEXT)");
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 65537, max: 600000 }), async (length) => {
          reach.beginHistory();
          await core.execute("DELETE FROM pressure");
          const values = [
            "a".repeat(length),
            "λ".repeat(Math.floor(length / 2)),
            "z".repeat(length - 17),
          ];
          await core.execute("INSERT INTO pressure VALUES (1,$1),(2,$2),(3,$3)", values);
          for (const kind of ["UPDATE pressure SET note=note", "DELETE FROM pressure"]) {
            await core.harness.path.quiesce();
            const actual = await core.execute(
              `${kind} RETURNING id,note,note AS echo,CASE WHEN id>0 THEN note ELSE NULL END AS chosen`,
            );
            if (!("returning" in actual)) throw new Error("Missing RETURNING");
            expect(oracleRows(actual.returning)).toEqual(
              values.map((note, i) => ({ id: i + 1, note, echo: note, chosen: note })),
            );
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach(kind.startsWith("UPDATE") ? "new rows" : "old rows");
          }
          expect(core.harness.governor.peakBytes).toBeLessThanOrEqual(
            core.harness.governor.capBytes,
          );
          reach.reach("64 MiB");
          reach.reach("wide input page");
          reach.reach("repeated output value");
          reach.reach("all credit returned");
        }),
        { seed, numRuns: 5, examples: [[70000], [250000], [600000]] },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  120000,
);
