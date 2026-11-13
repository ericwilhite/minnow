/** Detached native row copies preserve each field's byte high-water mark. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, JSON_RECORD_WIDTH_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { FlatVector } from "../vector/vector.js";
import { JsonRecords } from "./json-records.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
function vector(text: string): FlatVector {
  const bytes = new TextEncoder().encode(text);
  return {
    form: "flat",
    type: { kind: "text" },
    length: 1,
    validity: null,
    data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
  };
}
it.each(seedsFor("next-json-record-widths", [0xb73a, 0xb73b]))(
  "keeps independent field bounds through seal/copy/append (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "native JSON checkpoint field bounds",
      JSON_RECORD_WIDTH_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(0, 1, 4096, 65536),
        fc.constantFrom(0, 1, 4096, 65536),
        fc.boolean(),
        async (a, b, utf8) => {
          reach.beginHistory();
          const governor = new Governor(16 << 20),
            spill = new MemorySpillService();
          const grant = await governor.admit({
            label: "native field bounds",
            minimumBytes: 2 << 20,
            spillable: true,
          });
          const pool = new Pool(new Charge(grant), 2 << 20),
            pages = new PageWriter(pool, 16384);
          const files = new SpillFiles(spill, 1, {
            allocate: () => false,
            free: () => {
              throw new Error("No write cache");
            },
          });
          const controller = new AbortController();
          const io = new SpillIo(
            pool,
            files,
            pages,
            { grant, queryId: 1, signal: controller.signal },
            () => controller.signal.throwIfAborted(),
          );
          const types = [{ kind: "text" as const }, { kind: "text" as const }];
          const source = new JsonRecords(io, spill, types),
            target = new JsonRecords(io, spill, types);
          const input = [
            [(utf8 ? "汉" : "x").repeat(a), "k"],
            ["y", (utf8 ? "é" : "z").repeat(b)],
            ["t", "u"],
          ];
          try {
            for (const row of input.slice(0, 2)) await source.write(row.map(vector), 0);
            await source.seal();
            await source.copyTo(target, io);
            await source.close();
            reach.reach("copy after seal");
            await target.write((input[2] ?? []).map(vector), 0);
            await target.seal();
            for (const column of [0, 1]) {
              const expected = Math.max(
                ...input.map((row) => new TextEncoder().encode(row[column] ?? "").length),
              );
              expect(target.columnBytes(column)).toBe(expected);
              reach.reach(column === 0 ? "first field bound" : "second field bound");
            }
            reach.reach("append does not shrink bounds");
            const output: string[][] = [];
            expect(pool.allocate(target.retainedBytes)).toBe(true);
            try {
              await target.read(io, target.retainedBytes, async (columns) => {
                output.push(
                  columns.map((c) => {
                    if (c.data.layout !== "varlen") throw new Error("Expected text");
                    return new TextDecoder().decode(
                      c.data.bytes.subarray(c.data.offsets[0], c.data.offsets[1]),
                    );
                  }),
                );
              });
            } finally {
              pool.free(target.retainedBytes);
            }
            expect(output).toEqual(input);
            reach.reach("exact restored bytes");
            reach.reach(utf8 ? "UTF8 fields" : "ASCII fields");
          } finally {
            await Promise.allSettled([source.close(), target.close()]);
            await files.closeAll();
            pages.close();
            pool.settle();
            pool.close();
            expect(grant.allocated).toBe(0);
            grant.close();
            expect(governor.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
          }
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [65536, 1, false],
          [1, 65536, true],
          [0, 0, true],
          [4096, 4096, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
