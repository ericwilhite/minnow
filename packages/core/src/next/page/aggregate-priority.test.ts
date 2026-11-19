/** Optional A1 state must yield before it changes a formerly fitting dictionary to plain.
 * Authority: GroupWriter's canonical value-byte policy and A1's ordinary-page preservation.
 * A bounded transient grant shortage is injected at the existing dictionary allocation seam;
 * it can fit only after optional summary state is released, never by blindly retrying.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, GROUP_AGGREGATE_PRIORITY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { PageEncoding } from "../format/manifest.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import { GroupWriter } from "./writer.js";
import { defined, openChunk, vectorOf } from "./test-support.js";

async function write(rows: number, salt: number, constrain: boolean) {
  const space = new MemoryExtentSpace();
  let held = 0,
    ceiling = Number.MAX_SAFE_INTEGER,
    denials = 0,
    reclaimed = 0;
  const charge = {
    allocate(bytes: number) {
      if (held + bytes > ceiling) {
        denials++;
        return false;
      }
      held += bytes;
      return true;
    },
    free(bytes: number) {
      held -= bytes;
      expect(held).toBeGreaterThanOrEqual(0);
    },
  };
  const columns = [
    { columnId: 1, type: { kind: "int32" as const, bits: 32 as const } },
    { columnId: 2, type: { kind: "text" as const } },
  ];
  const writer = new GroupWriter({
    columns,
    writer: space.beginWrite("A1 priority"),
    charge,
    maxRows: rows,
    pageBytes: 512,
    rawPageBytes: 512,
    inlineBytes: 64,
  });
  const allocate = writer.memory.dictionaries.allocate.bind(writer.memory.dictionaries);
  let first = true;
  writer.memory.dictionaries.allocate = (bytes) => {
    if (!constrain || !first) return allocate(bytes);
    first = false;
    const before = held;
    ceiling = held + bytes - 1;
    try {
      const accepted = allocate(bytes);
      if (accepted && held < before + bytes) reclaimed++;
      return accepted;
    } finally {
      ceiling = Number.MAX_SAFE_INTEGER;
    }
  };
  const encoder = new TextEncoder();
  try {
    await writer.append(
      [
        vectorOf(
          defined(columns[0]).type,
          Array.from({ length: rows }, (_, row) => row * salt),
        ),
        vectorOf(
          defined(columns[1]).type,
          Array.from({ length: rows }, (_, row) =>
            encoder.encode(`${String(row % 3)}:${String(salt)}:`.padEnd(80, "x")),
          ),
        ),
      ],
      rows,
    );
    const group = await writer.finish();
    expect(held).toBe(0);
    return { space, group, denials, reclaimed };
  } catch (error) {
    await writer.abort();
    throw error;
  }
}
for (const seed of seedsFor("next-group-aggregate-priority", [0xb2eb, 0xb2ec]))
  it(`preserves value pages when dictionary growth reclaims summaries seed ${String(seed)}`, async () => {
    const reach = createReach("A1 ordinary page priority", GROUP_AGGREGATE_PRIORITY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 129, max: 512 }),
        fc.integer({ min: 1, max: 31 }),
        async (rows, salt) => {
          reach.beginHistory();
          const baseline = await write(rows, salt, false),
            candidate = await write(rows, salt, true);
          expect(baseline.group.columns[0]?.aggregateSummary).not.toBeNull();
          expect(typeof baseline.group.columns[0]?.aggregateSummary?.checksum).toBe("number");
          reach.reach("summary initially funded");
          expect(candidate.denials).toBe(1);
          reach.reach("dictionary denied");
          expect(candidate.reclaimed).toBe(1);
          expect(candidate.group.columns[0]?.aggregateSummary).toBeNull();
          reach.reach("summary reclaimed");
          for (let index = 0; index < baseline.group.columns.length; index++) {
            const a = defined(baseline.group.columns[index]),
              b = defined(candidate.group.columns[index]);
            const bytes = (space: MemoryExtentSpace, column: typeof a) =>
              space.bytes.subarray(
                column.data.extent.offset + column.data.offset,
                column.data.extent.offset + column.data.offset + column.data.length,
              );
            expect(bytes(candidate.space, b)).toEqual(bytes(baseline.space, a));
          }
          reach.reach("value bytes identical");
          const text = defined(candidate.group.columns[1]),
            observing = await openChunk(candidate.space, text);
          try {
            expect(
              observing.directory.pages.some((p) => p.encoding === PageEncoding.dictionary),
            ).toBe(true);
            expect(text.statistics.dictionarySize).toBe(3);
            reach.reach("dictionary kept");
          } finally {
            observing.close();
          }
          reach.reach("zero charges");
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  }, 60_000);
