/** Query directory facts are fully prepaid, independently owned, evictable and drain on close. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, QUERY_DIRECTORY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { writeGroup } from "./test-support.js";
import { QueryRoots } from "./query-roots.js";
import { ChunkReader, directoryCharge, DECODE_MEMO_BYTES, ReaderClosedError } from "./reader.js";
import * as codec from "./directory.js";
import { PageCorruptionError } from "./format.js";
import { parseCharge } from "../format/manifest-codec.js";
import { Governor } from "../memory/governor.js";
import { fullBlobKey } from "../format/blob-cache.js";

for (const buffer of [false, true])
  for (const seed of seedsFor("next-query-directory", [0xb325, 0xb326]))
    it(`query directory ownership Buffer=${String(buffer)} seed ${String(seed)}`, async () => {
      const reach = createReach("query directory", QUERY_DIRECTORY_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 33, max: 97 }),
          fc.integer({ min: 1, max: 19 }),
          async (n, salt) => {
            reach.beginHistory();
            const made = await writeGroup(
              [
                {
                  type: { kind: "text" },
                  values: Array.from({ length: n }, (_, r) =>
                    new TextEncoder().encode(`value:${String((r + salt) % 13)}`),
                  ),
                },
              ],
              { dictionaryEntries: 0 },
            );
            const chunk = made.group.columns[0];
            if (!chunk) throw new Error("chunk");
            const lease = await made.space.read([chunk.directory], { budget: UNLIMITED });
            const original = new Uint8Array(lease.bytes[0] ?? []);
            lease.release();
            const source = buffer ? Buffer.from(original) : original.slice();
            const key = fullBlobKey(chunk.directory);
            const pages = {
              peek: (ref: Parameters<MemoryExtentSpace["peek"]>[0]) =>
                fullBlobKey(ref) === key ? source : made.space.peek(ref),
              read: made.space.read.bind(made.space),
            };
            const governor = new Governor(8 << 20),
              cache = new QueryRoots(governor, []);
            const grant = await governor.admit({
              label: "directory holder",
              minimumBytes: 1 << 20,
              spillable: false,
            });
            const reader = cache.reader(pages);
            try {
              const parse = codec.parseDirectory;
              const spy = vi.spyOn(codec, "parseDirectory").mockImplementation((bytes, length) => {
                expect(grant.allocated).toBeGreaterThanOrEqual(
                  parseCharge(chunk.directory.length) + DECODE_MEMO_BYTES,
                );
                return parse(bytes, length);
              });
              let prepared;
              try {
                prepared = await ChunkReader.prepare(reader, chunk, { budget: grant, once: false });
              } finally {
                spy.mockRestore();
              }
              const expected = structuredClone(prepared.directory);
              const native = directoryCharge(expected) + DECODE_MEMO_BYTES;
              expect(prepared.nativeBytes).toBe(native);
              expect(grant.allocated).toBe(parseCharge(chunk.directory.length) + DECODE_MEMO_BYTES);
              expect(cache.directories.evict(Infinity)).toBe(0);
              reach.reach("full cache graph prepaid before parse");
              reach.reach("active directory survives eviction");
              prepared.release();
              prepared.release();
              expect(() => prepared.reader()).toThrow(ReaderClosedError);
              expect(grant.allocated).toBe(0);
              expect(cache.directories.residentBytes).toBe(parseCharge(chunk.directory.length));
              source.fill(0);
              const warm = ChunkReader.prepare(reader, chunk, { budget: grant });
              expect(warm).not.toBeInstanceOf(Promise);
              if (warm instanceof Promise) throw new Error("cache sync");
              expect(warm.directory).toEqual(expected);
              warm.release();
              reach.reach("source reuse cannot mutate metadata");
              reach.reach("synchronous validated hit");
              for (const ref of [
                {
                  ...chunk.directory,
                  extent: { ...chunk.directory.extent, length: chunk.directory.extent.length + 1 },
                },
                { ...chunk.directory, offset: chunk.directory.offset + 1 },
                { ...chunk.directory, checksum: chunk.directory.checksum ^ 1 },
              ])
                expect(cache.directories.pin(ref)).toBeNull();
              reach.reach("full directory reference identity");
              source.set(original);
              for (const wrong of [
                { ...chunk, type: { kind: "int32" as const, bits: 32 as const } },
                { ...chunk, data: { ...chunk.data, length: 0 } },
              ])
                expect(() => ChunkReader.prepare(reader, wrong, { budget: grant })).toThrow(
                  PageCorruptionError,
                );
              expect(grant.allocated).toBe(0);
              reach.reach("type and data bounds rechecked");
              let held = 0,
                required = 0;
              const tight = {
                allocate(bytes: number) {
                  required = Math.max(required, held + bytes);
                  if (held + bytes > native) return false;
                  held += bytes;
                  return true;
                },
                optionalAllocate(bytes: number) {
                  if (held + bytes > native) return false;
                  held += bytes;
                  return true;
                },
                free(bytes: number) {
                  held -= bytes;
                },
              };
              const fallback = await ChunkReader.prepare(reader, chunk, { budget: tight });
              expect(fallback.directory).toEqual(expected);
              expect(required).toBe(native);
              fallback.release();
              expect(held).toBe(0);
              reach.reach("refusal keeps original native minimum");
              expect(cache.directories.evict(Infinity)).toBe(parseCharge(chunk.directory.length));
              reach.reach("idle facts evict");
              const onepass = await ChunkReader.prepare(reader, chunk, {
                budget: grant,
                once: true,
              });
              expect(cache.directories.entryCount).toBe(0);
              onepass.release();
              expect(cache.directories.entryCount).toBe(0);
              reach.reach("explicit once does not admit directory facts");
              let resume: () => void = () => undefined;
              const gate = new Promise<void>((resolve) => {
                resume = resolve;
              });
              const blocked = cache.reader({
                peek: () => null,
                read: async (refs, options) => {
                  await gate;
                  return made.space.read(refs, options);
                },
              });
              const pending = Promise.resolve(
                ChunkReader.prepare(blocked, chunk, { budget: grant, once: false }),
              );
              cache.close();
              resume();
              const late = await pending;
              expect(late.directory).toEqual(expected);
              late.release();
              expect(cache.directories.residentBytes).toBe(0);
              expect(grant.allocated).toBe(0);
              reach.reach("late completion cannot retain after close");
            } finally {
              cache.close();
              grant.close();
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero charges");
          },
        ),
        { seed, numRuns: 8, endOnFailure: true },
      );
      reach.assertReached();
    });
