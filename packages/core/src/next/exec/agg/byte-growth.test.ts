/** A funded key step must cover every live old/replacement pair, including arbitrary jumps. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, BYTE_GROWTH_REACH } from "../../../testing/reach.js";
import { Governor } from "../../memory/governor.js";
import { GroupTable } from "./groups.js";
import { HashWorkspace } from "./hash-workspace.js";
import { TEXT, texts, dictionary } from "../kernels/test-support.js";

it.each(seedsFor("hash-byte-transition", [0xb7bd, 0xb7be]))(
  "funds complete byte-key transitions seed%i",
  async (seed) => {
    const reach = createReach("byte transition peak", BYTE_GROWTH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4095, max: 810000 }),
        fc.integer({ min: 1, max: 16 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (width, columns, encoded, selected, reverse) => {
          reach.beginHistory();
          const governor = new Governor(128 << 20);
          const grant = await governor.admit({
            label: "keys",
            minimumBytes: 1 << 20,
            spillable: true,
          });
          const workspace = new HashWorkspace(grant, grant.held);
          const types = Array.from({ length: columns }, () => TEXT);
          const table = new GroupTable(types, workspace);
          const ids = new Int32Array(8);
          let peak = 0;
          try {
            for (const prefix of [true, false]) {
              const keys = types.map((_, c) => {
                const values = prefix
                  ? ["p" + String(c), null]
                  : ["x".repeat(width) + String(c), "z" + String(c)];
                const entries = texts(prefix ? ["p" + String(c), "q" + String(c)] : values);
                return encoded
                  ? dictionary(
                      entries,
                      selected ? [0, prefix ? null : 1, 0, 1] : [0, prefix ? null : 1],
                    )
                  : texts(selected ? [values[0] ?? null, values[1] ?? null, null, null] : values);
              });
              const rows = selected
                ? { rows: Uint16Array.from(reverse ? [1, 0] : [0, 1]), count: 2 }
                : null;
              const length = selected ? 4 : 2;
              const ceiling = workspace.used + table.growthBytes(keys, rows?.count ?? length);
              expect(await workspace.prepare(ceiling)).toBe(true);
              table.assign(keys, length, rows, ids);
              peak = Math.max(peak, grant.allocated);
              expect(workspace.used).toBeLessThanOrEqual(ceiling);
              expect(ids[rows?.rows[0] ?? 0]).not.toBe(ids[rows?.rows[1] ?? 1]);
            }
            reach.reach("funded transition");
            reach.reach(encoded ? "dictionary" : "flat");
            reach.reach(columns === 1 ? "single key" : "compound keys");
            if (selected) reach.reach("selected");
            if (!reverse) reach.reach("large then small");
            expect(peak).toBeLessThan(governor.capBytes);
          } finally {
            table.close();
            workspace.close();
            grant.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero");
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        endOnFailure: true,
        examples: [
          [800000, 1, false, false, false],
          [800000, 3, true, false, false],
          [131071, 16, false, true, false],
          [65535, 3, true, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
