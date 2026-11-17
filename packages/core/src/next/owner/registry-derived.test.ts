/** RI proposals share immutable OLD payloads and replace NEW scalars without decoder copies. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, REGISTRY_DERIVED_REACH } from "../../testing/reach.js";
import { prefixFixture } from "./prefix-images.test-support.js";
import { batch, ints, texts } from "../write/test-support.js";
import { valueAt, decimal, decimals } from "../exec/kernels/test-support.js";

function required<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Missing native image column");
  return value;
}

for (const seed of seedsFor("next-registry-derived", [0xb631, 0xb632]))
  it(`derives native RI images without copying unchanged payloads ${String(seed)}`, async () => {
    const reach = createReach("derived native images", REGISTRY_DERIVED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom(0, 24000, 80000),
        fc.boolean(),
        async (published, nullable, reverse, width, alias) => {
          reach.beginHistory();
          const f = await prefixFixture(2, width, published, false, true);
          try {
            const id = f.ids[1],
              table = f.statement.catalog.table(f.tableId);
            if (id === undefined || !table) throw new Error("Missing native image fixture");
            const old = await f.reader.capture(f.tableId, id, f.stop.signal),
              key = table.columns[0],
              payload = table.columns[2];
            if (!key || !payload) throw new Error("Missing physical columns");
            const sourceTable = f.statement.catalog.table(f.extraId),
              sourceKey = sourceTable?.columns[0];
            if (!sourceTable || !sourceKey) throw new Error("Missing linked table");
            const sourceInput = (id: number) => ({
                batch: batch(ints([id]), decimals(decimal(38, 0), [[1n, 0]]), texts(["source"])),
                row: 0,
              }),
              parent = await f.images.append(sourceTable.id, sourceInput(76), sourceInput(77));
            const read = vi.spyOn(f.fragments, "readInto"),
              append = vi.spyOn(f.fragments, "append"),
              paid = f.root.allocated;
            let updated;
            try {
              const changes = [
                {
                  columnId: key.id,
                  value: alias
                    ? { image: parent, side: "new" as const, columnId: sourceKey.id }
                    : ints([77]),
                },
              ];
              updated = await f.images.derive(old, "old", changes, f.stop.signal);
              expect(read, "derivation reads scalar descriptors only").not.toHaveBeenCalled();
              expect(
                append,
                "unchanged payloads share their root fragments",
              ).not.toHaveBeenCalled();
              expect(f.root.allocated, "derivation has no row-count metadata growth").toBe(paid);
            } finally {
              read.mockRestore();
              append.mockRestore();
            }
            const changes = [
              { columnId: key.id, value: ints([88]) },
              { columnId: payload.id, value: nullable ? null : texts(["replacement"]) },
            ];
            if (reverse) changes.reverse();
            const patched = await f.images.derive(updated, "new", changes, f.stop.signal),
              final = await f.images.derive(patched, "new", [], f.stop.signal);
            for (const [ref, expected] of [
              [old, [2, f.payloads[1]]],
              [updated, [77, f.payloads[1]]],
              [final, [88, nullable ? null : "replacement"]],
            ] as const) {
              const image = await f.images.readRowImages(ref, f.stop.signal);
              try {
                const before = image.old;
                if (!before) throw new Error("Missing canonical OLD");
                expect(valueAt(required(before.columns[0]), 0), "canonical OLD never changes").toBe(
                  2,
                );
                expect(valueAt(required(before.columns[2]), 0)).toBe(f.payloads[1]);
                if (ref !== old) {
                  const after = image.new;
                  if (!after) throw new Error("Missing derived NEW");
                  expect(valueAt(required(after.columns[0]), 0)).toBe(expected[0]);
                  expect(valueAt(required(after.columns[2]), 0)).toBe(expected[1]);
                  expect(valueAt(required(after.columns[1]), 0)).toEqual([f.coefficients[1], 0]);
                } else expect(image.new).toBeNull();
              } finally {
                image.release();
              }
            }
            const inserted = await f.images.append(sourceTable.id, null, sourceInput(90)),
              finalized = await f.images.derive(
                inserted,
                "new",
                [{ columnId: sourceKey.id, value: ints([99]) }],
                f.stop.signal,
              ),
              insertImage = await f.images.readRowImages(finalized, f.stop.signal);
            try {
              expect(insertImage.old).toBeNull();
              expect(valueAt(required(insertImage.new?.columns[0]), 0)).toBe(99);
            } finally {
              insertImage.release();
            }
            reach.reach("insert NEW");
            reach.reach(alias ? "linked parent scalar" : "owned scalar");
            reach.reach(published ? "published" : "resident");
            reach.reach(nullable ? "NULL replacement" : "value replacement");
            reach.reach(reverse ? "reverse replacements" : "ordered replacements");
            reach.reach(width > 65536 ? "large shared payload" : "small shared payload");
            reach.reach("no payload copy");
            reach.reach("canonical OLD");
            reach.reach("successive derivations");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [false, false, false, 0, false],
          [true, true, true, 80000, true],
          [false, true, false, 24000, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
