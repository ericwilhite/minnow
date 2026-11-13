/** A row-count hint changes reservation only, never native page or chunk-tail bytes. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, KNOWN_ROW_ESTIMATE_STATES } from "../../testing/reach.js";
import { GroupWriter } from "./writer.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import { defined, vectorOf } from "./test-support.js";
const reach = createReach("known-row extent estimates", KNOWN_ROW_ESTIMATE_STATES);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-known-row-estimate", [0xb518]))
  it(`keeps native bytes identical while bounding known-row reservations (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2049, 49155, 65537),
        fc.integer({ min: 2, max: 8 }),
        async (width, count) => {
          reach.beginHistory();
          const data = Array.from({ length: count }, (_, i) => {
            const b = new Uint8Array(width).fill(65);
            b[b.length - 1] = 66 + i;
            return b;
          });
          async function write(hinted: boolean) {
            const space = new MemoryExtentSpace(),
              writer = space.beginWrite("known rows"),
              open = writer.open.bind(writer),
              caps: number[] = [],
              parts: Uint8Array[] = [];
            // Test storage artifacts ignore exaggerated reservations, so the parent-byte oracle
            // cannot allocate gigabytes merely to compare a few hundred KiB of actual output.
            writer.open = (capacity) => {
              caps.push(capacity);
              const builder = open(Math.min(capacity, 256 * 1024)),
                append = builder.append.bind(builder);
              builder.append = async (buffers) => {
                for (const buffer of buffers) parts.push(buffer.slice());
                return await append(buffers);
              };
              return builder;
            };
            const group = new GroupWriter({
              columns: [
                { columnId: 1, type: { kind: "text" } },
                { columnId: 2, type: { kind: "int32", bits: 32 } },
              ],
              writer,
              ...(hinted ? { estimatedRows: count } : {}),
            });
            try {
              expect(
                await group.append(
                  [
                    vectorOf({ kind: "text" }, data),
                    vectorOf(
                      { kind: "int32", bits: 32 },
                      Array.from({ length: count }, (_, i) => i),
                    ),
                  ],
                  count,
                ),
              ).toBe(count);
              await group.finish();
              return { parts, caps };
            } finally {
              await writer.abort();
            }
          }
          const parent = await write(false),
            current = await write(true);
          expect(current.parts.map((p) => p.length)).toEqual(parent.parts.map((p) => p.length));
          for (let i = 0; i < current.parts.length; i++)
            expect(Buffer.compare(defined(current.parts[i]), defined(parent.parts[i]))).toBe(0);
          expect(Math.max(...current.caps)).toBeLessThan(2 * 1024 * 1024);
          reach.reach("native bytes identical");
          reach.reach("bounded hint");
          expect(Math.max(...current.caps)).toBeLessThanOrEqual(Math.max(...parent.caps));
          if (Math.max(...parent.caps) > Math.max(...current.caps))
            reach.reach("long row projection");
        },
      ),
      { seed, numRuns: 12 },
    );
  });
