/** Packing (manifest.ts): every reference into an extent shares one ExtentRef per root. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, ROOT_PACKING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { ExtentRef } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import type { ColumnChunk, GroupRoot } from "./manifest.js";
import {
  decodeGroupRoot,
  encodeGroupRoot,
  parseCharge,
  selectGroupRootColumns,
  tryDecodeGroupRootView,
} from "./manifest-codec.js";

function references(columns: readonly ColumnChunk[]): ExtentRef[] {
  return columns.flatMap((column) => [
    column.data.extent,
    column.directory.extent,
    ...(column.distinctSketch === null ? [] : [column.distinctSketch.extent]),
  ]);
}
function packed(refs: readonly ExtentRef[]): void {
  const seen = new Map<string, ExtentRef>();
  for (const ref of refs) {
    const key = `${String(ref.offset)}:${String(ref.length)}`;
    if (seen.has(key)) expect(ref).toBe(seen.get(key));
    else seen.set(key, ref);
  }
}
for (const seed of seedsFor("next-root-packing", [0xb301, 0xb302]))
  it(`complete lazy roots preserve packed row-ID extent identity seed ${String(seed)}`, () => {
    const reach = createReach("complete root packing", ROOT_PACKING_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 7, max: 65536 }),
        fc.integer({ min: 3, max: 8 }),
        fc.constantFrom("all", "selected", "unselected"),
        fc.boolean(),
        (rowCount, count, sharing, completeFirst) => {
          reach.beginHistory();
          const extent = { offset: 4096, length: 4096 };
          const blob = { extent, offset: 32, length: 16, checksum: 12 };
          const types: PhysicalType[] = [
            { kind: "enum", name: "e", labels: ["a", "b"] },
            { kind: "array", element: { kind: "text" } },
            { kind: "int64" },
          ];
          const source: GroupRoot = {
            rowCount,
            rowIds: { kind: "column", ref: blob },
            columns: Array.from({ length: count }, (_, i) => {
              const shared = sharing === "all" || i === (sharing === "selected" ? 0 : count - 1);
              const owner = shared ? extent : { offset: (i + 2) * 4096, length: 4096 };
              const ref = { ...blob, extent: owner };
              return {
                columnId: i + 1,
                type: types[i % types.length] ?? { kind: "int64" },
                data: { extent: owner, offset: i * 64, length: 32 },
                directory: ref,
                statistics: {
                  nullCount: i,
                  min: new Uint8Array([i, 2]),
                  max: new Uint8Array([6, i]),
                  dictionarySize: null,
                },
                distinctSketch: i === 0 ? null : ref,
              };
            }),
          };
          const bytes = encodeGroupRoot(source);
          let held = 0;
          const budget = {
            allocate(n: number) {
              held += n;
              return true;
            },
            free(n: number) {
              held -= n;
            },
          };
          const eager = decodeGroupRoot(bytes, budget);
          const lazy = tryDecodeGroupRootView(bytes, budget);
          if (lazy?.value.rowIds.kind !== "column") throw new Error("fixture headroom");
          // Each independent selected graph has its own full parse credit before construction.
          const selectedCharge = 2 * parseCharge(bytes.length);
          budget.allocate(selectedCharge);
          try {
            const complete = () => {
              expect(lazy.value).toEqual(eager.value);
              packed([
                lazy.value.rowIds.kind === "column" ? lazy.value.rowIds.ref.extent : extent,
                ...references(lazy.value.columns),
              ]);
              expect(encodeGroupRoot(lazy.value)).toEqual(bytes);
            };
            if (completeFirst) complete();
            const first = selectGroupRootColumns(lazy.value, [1]);
            expect(first).toEqual([source.columns[0]]);
            const a = selectGroupRootColumns(lazy.value, [1, count]);
            const b = selectGroupRootColumns(lazy.value, [1, count]);
            const aa = references(a),
              bb = references(b);
            packed(aa);
            packed(bb);
            for (const ref of aa) {
              expect(ref).not.toBe(lazy.value.rowIds.ref.extent);
              expect(bb.includes(ref)).toBe(false);
            }
            reach.reach("fresh selected extent graphs");
            if (!completeFirst) complete();
            reach.reach(completeFirst ? "complete before selection" : "selection before complete");
            if (sharing === "all") reach.reach("one packed extent");
            if (sharing !== "unselected") reach.reach("row IDs share selected data");
            if (sharing !== "selected") reach.reach("row IDs share unselected data");
          } finally {
            budget.free(selectedCharge + eager.charge + lazy.charge);
          }
          expect(held).toBe(0);
          reach.reach("canonical bytes and zero charges");
        },
      ),
      // Includes the reviewer's three-column, one-extent fixture in the generated campaign.
      { seed, numRuns: 60, examples: [[7, 3, "all", true]] },
    );
    reach.assertReached();
  });
