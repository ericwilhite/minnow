/** The lead's no-higher-fitting-peak rule. These ceilings were captured from
 * main82e4aa00 on four unequal-width batches, never from candidate output.
 * They are memory acceptance baselines, not a SQL semantics oracle.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, HASH_PARENT_PEAK_REACH } from "../../../testing/reach.js";
import { Governor } from "../../memory/governor.js";
import { GroupTable } from "./groups.js";
import { HashWorkspace } from "./hash-workspace.js";
import { TEXT, texts, dictionary } from "../kernels/test-support.js";
interface Witness {
  readonly id: number;
  readonly widths: ReadonlyArray<ReadonlyArray<readonly number[]>>;
  readonly parentPeaks: readonly number[];
}
const keyStates: Record<number, (typeof HASH_PARENT_PEAK_REACH)[number]> = {
  2: "2 keys",
  3: "3 keys",
  4: "4 keys",
  8: "8 keys",
  16: "16 keys",
};
const witnesses: readonly Witness[] = [
  {
    id: 3,
    widths: [
      [
        [65543, 524297],
        [200008, 524297],
        [800008, 65543],
        [4105, 200008],
      ],
      [
        [400008, 131079],
        [1048585, 65545],
        [600008, 200008],
        [600008, 4103],
      ],
      [
        [65545, 524297],
        [32775, 131079],
        [4103, 600008],
        [524297, 32775],
      ],
      [
        [9, 9],
        [32775, 131079],
        [800008, 65545],
        [65543, 65545],
      ],
    ],
    parentPeaks: [18213625, 18213625, 18213657, 18213657],
  },
  {
    id: 4,
    widths: [
      [
        [200008, 4103],
        [4105, 4103],
        [65545, 1048585],
        [400008, 4103],
        [65545, 200008],
        [4105, 400008],
        [65543, 32775],
        [800008, 4103],
      ],
      [
        [65545, 65545],
        [65543, 4103],
        [400008, 4103],
        [1048585, 524297],
        [600008, 131079],
        [65543, 600008],
        [4103, 131079],
        [32775, 400008],
      ],
      [
        [600008, 131079],
        [65543, 400008],
        [600008, 1048585],
        [400008, 600008],
        [524297, 524297],
        [65545, 800008],
        [65545, 32775],
        [1048585, 200008],
      ],
      [
        [400008, 800008],
        [4105, 200008],
        [4103, 800008],
        [4105, 4105],
        [200008, 400008],
        [200008, 32775],
        [32775, 4103],
        [9, 400008],
      ],
    ],
    parentPeaks: [51428136, 51428136, 51428200, 51428200],
  },
  {
    id: 14,
    widths: [
      [
        [524298, 200009],
        [10, 800009],
        [32776, 4104],
      ],
      [
        [4106, 10],
        [131080, 10],
        [65544, 200009],
      ],
      [
        [400009, 65546],
        [10, 4104],
        [400009, 10],
      ],
      [
        [524298, 10],
        [4106, 65546],
        [1048586, 4104],
      ],
    ],
    parentPeaks: [15376768, 15376768, 15376792, 15376792],
  },
  {
    id: 19,
    widths: [
      [
        [1048586, 65546],
        [131080, 10],
      ],
      [
        [200009, 524298],
        [65544, 65546],
      ],
      [
        [400009, 65546],
        [600009, 4106],
      ],
      [
        [65544, 800009],
        [1048586, 65546],
      ],
    ],
    parentPeaks: [9521552, 9521552, 9521568, 9521568],
  },
  {
    id: 35,
    widths: [
      [
        [32776, 524298],
        [600009, 4106],
        [10, 10],
        [32776, 400009],
        [1048586, 131080],
        [800009, 32776],
        [32776, 4104],
        [400009, 1048586],
        [10, 65544],
        [32776, 131080],
        [600010, 65547],
        [131081, 11],
        [4105, 65547],
        [11, 400010],
        [400010, 400010],
        [65545, 131081],
      ],
      [
        [1048586, 4104],
        [65546, 4106],
        [131080, 524298],
        [200009, 600009],
        [400009, 524298],
        [400009, 65546],
        [200009, 65546],
        [200009, 200009],
        [131080, 800009],
        [131080, 4106],
        [400010, 32777],
        [11, 65545],
        [800010, 400010],
        [4107, 800010],
        [131081, 4107],
        [65545, 524299],
      ],
      [
        [4106, 4104],
        [65546, 65544],
        [131080, 600009],
        [200009, 32776],
        [65546, 65544],
        [131080, 400009],
        [4106, 600009],
        [10, 4104],
        [10, 65544],
        [400009, 131080],
        [524299, 200010],
        [524299, 32777],
        [1048587, 1048587],
        [11, 200010],
        [400010, 400010],
        [11, 200010],
      ],
      [
        [65546, 524298],
        [32776, 200009],
        [1048586, 4106],
        [10, 200009],
        [600009, 131080],
        [1048586, 32776],
        [131080, 65544],
        [65546, 131080],
        [10, 800009],
        [600009, 4104],
        [32777, 65545],
        [4105, 32777],
        [800010, 1048587],
        [600010, 200010],
        [524299, 4107],
        [32777, 4105],
      ],
    ],
    parentPeaks: [97186808, 97186808, 97186936, 97186936],
  },
];
it.each(seedsFor("hash-key-parent-peaks", [0xb7c1, 0xb7c2]))(
  "keeps later-batch unequal-key peaks at or below main seed%i",
  async (seed) => {
    const reach = createReach("hash parent peak", HASH_PARENT_PEAK_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...witnesses),
        fc.boolean(),
        fc.boolean(),
        async (w, encoded, selected) => {
          reach.beginHistory();
          const first = w.widths[0];
          if (!first) throw new Error("Missing parent fixture");
          const gov = new Governor(256 << 20),
            grant = await gov.admit({
              label: "parent peak",
              minimumBytes: 1 << 20,
              spillable: true,
            }),
            ws = new HashWorkspace(grant, grant.held),
            types = first.map(() => TEXT),
            table = new GroupTable(types, ws),
            ids = new Int32Array(4);
          try {
            for (let b = 0; b < w.widths.length; b++) {
              const step = w.widths[b];
              if (!step) throw new Error("Missing parent batch");
              const keys = step.map((widths, c) => {
                const values = widths.map((width, r) => {
                  const suffix = `/${String(w.id)}/${String(b)}/${String(c)}/${String(r)}`;
                  return "x".repeat(width - suffix.length) + suffix;
                });
                const entries = texts(values);
                return encoded
                  ? dictionary(entries, selected ? [0, 0, 1, 0] : [0, 1])
                  : texts(selected ? [values[0] ?? null, null, values[1] ?? null, null] : values);
              });
              expect(await ws.prepare(ws.used + table.growthBytes(keys, 2))).toBe(true);
              table.assign(
                keys,
                selected ? 4 : 2,
                selected ? { rows: Uint16Array.from([0, 2]), count: 2 } : null,
                ids,
              );
              expect(ids[0]).toBe(2 * b);
              expect(ids[selected ? 2 : 1]).toBe(2 * b + 1);
            }
            const ceiling = w.parentPeaks[2 * Number(encoded) + Number(selected)];
            if (ceiling === undefined) throw new Error("Missing parent ceiling");
            expect(gov.peakBytes).toBeLessThanOrEqual(ceiling);
            reach.reach(encoded ? "dictionary" : "flat");
            reach.reach(selected ? "selected" : "dense");
            reach.reach("unequal widths");
            reach.reach("later batch");
            reach.reach("parent peak");
            const keyState = keyStates[first.length];
            if (!keyState) throw new Error("Unregistered parent key count");
            reach.reach(keyState);
          } finally {
            table.close();
            ws.close();
            grant.close();
            expect(gov.usedBytes).toBe(0);
            reach.reach("zero");
          }
        },
      ),
      {
        seed,
        numRuns: 32, // All twenty fixed forms plus twelve generated cases.
        endOnFailure: true,
        examples: witnesses.flatMap((w) =>
          [false, true].flatMap((encoded) =>
            [false, true].map((selected): [Witness, boolean, boolean] => [w, encoded, selected]),
          ),
        ),
      },
    );
    reach.assertReached();
  },
  120000,
);
