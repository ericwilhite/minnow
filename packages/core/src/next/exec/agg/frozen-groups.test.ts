/** Frozen membership uses SQL equality and never creates a group after spill mode begins. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import { batchesOf, INT, DBL, TEXT, BIG, NUM, type Row } from "../join-fixtures.js";
import { GroupTable } from "./groups.js";
import { HashWorkspace } from "./hash-workspace.js";
import { FrozenGroups } from "./frozen-groups.js";

it.each(seedsFor("aggregate-frozen-groups", [0xb4b3, 0xb4b4]))(
  "finds only resident groups across encodings, selections and growth (seed %i)",
  async (seed) => {
    const reach = createReach("hybrid frozen membership", [
      "growth funded",
      "resident",
      "new group",
      "selection",
      "dictionary",
      "all key families",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1500, max: 3500 }), async (count) => {
        reach.beginHistory();
        for (const types of [[INT], [DBL], [TEXT], [BIG], [NUM], [INT, TEXT, DBL]]) {
          const gov = new Governor(256 << 20),
            grant = await gov.admit({ label: "hash", minimumBytes: 1 << 20, spillable: true }),
            workspace = new HashWorkspace(grant, 1 << 20),
            table = new GroupTable(types, workspace),
            gids = new Int32Array(2048),
            expected: number[] = [];
          let frozen: FrozenGroups | null = null;
          const row = (k: number): Row =>
            types.map((t) =>
              k % 97 === 0
                ? null
                : t.kind === "text"
                  ? `key-${String(k)}`
                  : t.kind === "decimal" || t.kind === "int64"
                    ? BigInt(k)
                    : k,
            );
          const rows = Array.from({ length: count }, (_, k) => row(k));
          const batches = [
            ...batchesOf(types, rows, { batchRows: 512, skip: 3, dictionary: true }),
          ];
          try {
            for (const batch of batches) {
              expect(
                await workspace.prepare(
                  workspace.used +
                    table.growthBytes(batch.columns, batch.selection?.count ?? batch.length),
                ),
              ).toBe(true);
              table.assign(batch.columns, batch.length, batch.selection, gids);
              for (let k = 0; k < (batch.selection?.count ?? batch.length); k++)
                expected.push(gids[batch.selection?.rows[k] ?? k] ?? -1);
            }
            reach.reach("growth funded");
            // Freeze uses already-admitted room, never a synchronous grant expansion.
            expect(await workspace.prepare(workspace.used + 32 * 1024 * 1024)).toBe(true);
            frozen = new FrozenGroups(workspace, table.groupCount);
            frozen.build(table, () => undefined);
            let at = 0;
            for (const batch of batches) {
              frozen.encode(batch, batch.columns);
              for (let k = 0; k < (batch.selection?.count ?? batch.length); k++)
                expect(frozen.find(k)).toBe(expected[at++]);
            }
            reach.reach("resident");
            reach.reach("selection");
            reach.reach("dictionary");
            const missing = (count + 101) % 97 === 0 ? count + 102 : count + 101;
            const absent = [...batchesOf(types, [row(missing)])][0];
            if (!absent) throw new Error("No probe batch");
            frozen.encode(absent, absent.columns);
            expect(frozen.find(0)).toBe(-1);
            reach.reach("new group");
            reach.reach("all key families");
          } finally {
            frozen?.close();
            table.close();
            workspace.close();
            grant.close();
          }
          expect(gov.usedBytes).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 5 },
    );
    reach.assertReached();
  },
);
