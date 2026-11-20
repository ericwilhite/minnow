/** page/large-value.ts: page workspace counts pointers, never out-of-line payloads. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_GROUP_VALUE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { readChunk, sameValue, vectorOf } from "../page/test-support.js";
import { GroupWriter, groupWriterMinimum } from "../page/writer.js";
import { MetadataBudget } from "./budget.js";
import { nativeGroupOptions } from "./group-options.js";
import { Memtable } from "./memtable.js";
import { TEXT } from "./test-support.js";

const sizes = [null, 0, 1, 32_768, 65_535, 65_536, 65_537, 1_048_577] as const;
for (const seed of seedsFor("next-write-group-value-bounds", [47209, 47210]))
  it(`keeps exact page bounds through rollback and flush (seed ${String(seed)})`, async () => {
    const reach = createReach("native group value bounds", WRITE_GROUP_VALUE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.constantFrom(...sizes), { minLength: 2, maxLength: 4 }),
        async (lengths) => {
          reach.beginHistory();
          const governor = new Governor(32 << 20);
          const [rows, metadata] = await governor.admitGroup({
            label: "native group fixture",
            grants: [
              { label: "native rows", minimumBytes: 0, spillable: true },
              { label: "native workspace", minimumBytes: 0, spillable: true },
            ],
          });
          if (!rows || !metadata) throw new Error("Incomplete group fixture");
          const budget = new MetadataBudget(metadata, () => 0, governor.capBytes);
          const memtable = new Memtable(
            1,
            0,
            1,
            [
              {
                id: 1,
                name: "value",
                type: TEXT,
                declared: { type: "string" },
                nullable: true,
                defaultSql: null,
                generatedSql: null,
                identitySequence: null,
                missing: null,
              },
            ],
            [],
            rows,
          );
          const space = new MemoryExtentSpace(),
            writer = space.beginWrite("native bounded values");
          let group: GroupWriter | undefined;
          const makeValues = (ns: ReadonlyArray<number | null>) =>
            ns.map((n) => (n === null ? null : new Uint8Array(n).fill(65)));
          let values = makeValues(lengths);
          const options = () => {
            const steps = nativeGroupOptions(memtable);
            for (;;) {
              const step = steps.next();
              if (step.done) return step.value;
            }
          };
          const verifyBounds = () => {
            const bounds = options();
            const encoded = values.map((v) =>
              v === null
                ? 0
                : v.length > 65_536
                  ? 16 + 4 * Math.ceil(v.length / 1_048_576)
                  : v.length,
            );
            expect(bounds.maxRows).toBe(values.length);
            expect(bounds.columns[0]).toMatchObject({
              valueBytes: encoded.reduce((sum, n) => sum + n, 0),
              maxValueBytes: Math.max(...encoded),
              dictionaryValueBytes: values.reduce(
                (sum, v) => sum + (v !== null && v.length <= 65_536 ? v.length : 0),
                0,
              ),
            });
            return bounds;
          };
          try {
            memtable.append([vectorOf(TEXT, values)], null, 0, values.length);
            memtable.mark(1);
            const first = verifyBounds();
            if (lengths.some((n) => n !== null && n > 65_536))
              reach.reach("out-of-line pointer bounds");
            if (lengths.includes(65_536)) reach.reach("inline limit");
            if (lengths.includes(null)) reach.reach("NULL");
            if (lengths.includes(0)) reach.reach("empty valid value");
            // Keep the same chunk and restore the same final row count with different bytes.
            memtable.truncate(1, 1);
            const replacements = values
              .slice(1)
              .map((_, i) => (i % 2 === 0 ? new Uint8Array(3).fill(66) : null));
            memtable.append([vectorOf(TEXT, replacements)], null, 0, replacements.length);
            values = [values[0] ?? null, ...replacements];
            const second = verifyBounds();
            reach.reach("same-length rollback invalidates cached bounds");
            expect(second.maxRows).toBe(first.maxRows);
            const source = memtable.chunks[0]?.[0];
            if (!source) throw new Error("Missing native column");
            const minimum = groupWriterMinimum(second);
            group = new GroupWriter({ ...second, writer, charge: budget });
            const held = metadata.held;
            expect(await group.append([source.view()], values.length)).toBe(values.length);
            const output = await group.finish();
            expect(group.peakWorkingBytes).toBeLessThanOrEqual(minimum);
            expect(metadata.held).toBe(held);
            const chunk = output.columns[0];
            if (!chunk) throw new Error("Missing written column");
            const actual = await readChunk(space, chunk);
            expect(actual.length).toBe(values.length);
            expect(actual.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
            reach.reach("exact native group flush");
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            await group?.abort();
            await writer.abort();
            memtable.releaseAll();
            metadata.close();
            rows.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [[[65_536, 65_537, null, 0]], [[1_048_577, 1]]],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
