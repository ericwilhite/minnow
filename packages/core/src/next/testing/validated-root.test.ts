/** Complete validation precedes selective materialization; only owner-charged roots cross queries. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  VALIDATED_ROOT_REACH,
  VALIDATED_ROOT_PRESSURE_REACH,
} from "../../testing/reach.js";
import type { Snapshot } from "../access/snapshot.js";
import { boundSource } from "../page/read-bounds.js";
import { ByteReader, ManifestCorruptionError } from "../format/bytes.js";
import { BlobCache, fullBlobKey } from "../format/blob-cache.js";
import {
  decodeGroupRoot,
  encodeGroupRoot,
  groupRootColumnCount,
  parseCharge,
  selectGroupRootColumns,
  tryDecodeGroupRootView,
} from "../format/manifest-codec.js";
import {
  bindMetadataCaches,
  metadataCachesFor,
  MetadataReader,
} from "../format/metadata-reader.js";
import type { GroupRoot } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { GroupRead } from "../page/group-read.js";
import { QueryRoots } from "../page/query-roots.js";
import { Governor } from "../memory/governor.js";

it.each(seedsFor("next-validated-root", [0xb2f1, 0xb2f2]))(
  "validates all columns once, selects fresh metadata, and drains every owner (seed %i)",
  async (seed) => {
    const reach = createReach("owner validated roots", VALIDATED_ROOT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 65536 }),
        fc.integer({ min: 24, max: 40 }),
        fc.boolean(),
        fc.integer({ min: 0, max: 3 }),
        async (rowCount, count, rowColumn, shape) => {
          reach.beginHistory();
          const type: PhysicalType = [
            { kind: "int64" },
            { kind: "enum", name: "e", labels: ["", "a", "長"] },
            { kind: "array", element: { kind: "array", element: { kind: "text" } } },
            { kind: "decimal", precision: null, scale: null },
          ][shape] as PhysicalType;
          const extent = { offset: 2 ** 40 + rowCount, length: 65536 };
          const blob = { extent, offset: 0, length: 32, checksum: 12 };
          const root: GroupRoot = {
            rowCount,
            rowIds: rowColumn ? { kind: "column", ref: blob } : { kind: "dense" },
            columns: Array.from({ length: count }, (_, i) => ({
              columnId: 2 ** 40 + i,
              type,
              data: { extent, offset: i * 64, length: 64 },
              directory: blob,
              statistics: {
                nullCount: i % 2 ? rowCount : 0,
                min: new Uint8Array([i, 0, 255]),
                max: new Uint8Array([255, i]),
                dictionarySize: null,
              },
              distinctSketch: i % 2 ? blob : null,
            })),
          };
          const bytes = encodeGroupRoot(root),
            governor = new Governor(8 << 20);
          const cache = new BlobCache<GroupRoot>((c) => governor.registerCache(c), fullBlobKey);
          const shared = { validatedRoots: true, groupRoots: cache };
          const space = new MemoryExtentSpace();
          const ref = (await space.beginWrite("validated root").write([bytes])).blobs[0];
          if (!ref) throw new Error("root fixture");
          const unbind = bindMetadataCaches(space, shared);
          const grant = await governor.admit({
            label: "root caller",
            minimumBytes: 2 << 20,
            spillable: false,
          });
          const reader = new MetadataReader(space, shared);
          const wanted = [2 ** 40 + count - 1, 2 ** 40 + 1];
          let queries: QueryRoots | undefined;
          try {
            expect(metadataCachesFor(space)).toBe(shared);
            expect(metadataCachesFor(new MemoryExtentSpace())).toBeUndefined();
            reach.reach("owner isolation");
            const cold = await reader.groupRoot(ref, grant);
            expect(groupRootColumnCount(cold.value)).toBe(count);
            const value = cold.value;
            expect(Object.getOwnPropertyDescriptor(value, "columns")).toHaveProperty("get");
            cold.release();
            expect(cache.evictableBytes).toBeGreaterThan(parseCharge(bytes.length));
            const uint = vi.spyOn(ByteReader.prototype, "uint");
            let warm;
            try {
              warm = reader.groupRoot(ref, grant);
              expect(warm).not.toBeInstanceOf(Promise);
              expect(uint).not.toHaveBeenCalled();
              reach.reach("warm hit skips all validation");
              const selected = selectGroupRootColumns(value, wanted);
              expect(uint.mock.calls.length).toBeLessThan(40);
              expect(selected).toEqual(root.columns.filter((c) => wanted.includes(c.columnId)));
              expect(selectGroupRootColumns(value, [])).toEqual([]);
              reach.reach("only selected columns materialize");
              const again = selectGroupRootColumns(value, wanted);
              expect(again).toEqual(selected);
              expect(again[0]).not.toBe(selected[0]);
              if (again[0]?.statistics.min instanceof Uint8Array) again[0].statistics.min.fill(7);
              expect(selectGroupRootColumns(value, wanted)).toEqual(selected);
              expect(Object.isFrozen(selected[0]?.type)).toBe(true);
              if (type.kind === "enum")
                expect(
                  Object.isFrozen(selected[0]?.type.kind === "enum" && selected[0].type.labels),
                ).toBe(true);
              if (type.kind === "array" && selected[0]?.type.kind === "array") {
                expect(Object.isFrozen(selected[0].type.element)).toBe(true);
                if (selected[0].type.element.kind === "array")
                  expect(Object.isFrozen(selected[0].type.element.element)).toBe(true);
              }
              reach.reach("fresh ref and statistic ownership");
            } finally {
              uint.mockRestore();
            }
            if (warm instanceof Promise) throw new Error("expected resident root");
            const allocated = grant.allocated;
            expect(cache.evict(Infinity)).toBe(0);
            expect(allocated).toBe(cache.residentBytes);
            expect(allocated).toBe(
              parseCharge(bytes.length) + bytes.length + 16 + 16 * count + 1024,
            );
            warm.release();
            expect(grant.allocated).toBe(0);
            reach.reach("pinned roots remain charged");
            for (const alias of [
              {
                ...ref,
                extent: { ...ref.extent, offset: ref.extent.offset + 1 },
                offset: ref.offset - 1,
              },
              { ...ref, extent: { ...ref.extent, length: ref.extent.length + 1 } },
              { ...ref, length: ref.length + 1 },
              { ...ref, checksum: ref.checksum ^ 1 },
            ])
              expect(cache.pin(alias)).toBeNull();
            reach.reach("full reference identity");
            // A large optional representation cannot refuse a fitting ordinary full decode.
            let held = 0;
            const minimal = {
              allocate: (n: number) => {
                if (held + n > parseCharge(bytes.length)) return false;
                held += n;
                return true;
              },
              free: (n: number) => {
                held -= n;
              },
            };
            const fallback = await reader.groupRoot(ref, minimal);
            expect(fallback.value).toEqual(root);
            fallback.release();
            expect(held).toBe(0);
            reach.reach("optional refusal preserves ordinary minimum");
            // New query facts borrow the same validated owner root without forcing columns.
            queries = new QueryRoots(governor, [{ tableId: 1, columns: wanted }], shared);
            const facts = await queries.read(1, ref, space, grant, wanted);
            expect(facts.value.columns).toEqual(
              root.columns.filter((c) => wanted.includes(c.columnId)),
            );
            expect(grant.allocated).toBe(parseCharge(ref.length));
            facts.release();
            queries.close();
            queries = undefined;
            // Even a byte-invalidated source cannot mutate a successfully owned validated view.
            const source = space.peek(ref);
            if (!source) throw new Error("missing encoded root");
            source.fill(0);
            expect(selectGroupRootColumns(value, wanted)).toEqual(
              root.columns.filter((c) => wanted.includes(c.columnId)),
            );
            expect(value.columns).toEqual(root.columns);
            reach.reach("owned encoded bytes survive source release");
            cache.clear();
            expect(cache.residentBytes).toBe(0);
            // Concurrent cold borrowers may race admission, but each owns a complete charge.
            const coldPages = { peek: () => null, read: space.read.bind(space) };
            // Restore the deliberately poisoned source in an independent space first.
            const concurrentSpace = new MemoryExtentSpace();
            const concurrentRef = (
              await concurrentSpace.beginWrite("concurrent root").write([bytes])
            ).blobs[0];
            if (!concurrentRef) throw new Error("concurrent ref");
            const concurrentReader = new MetadataReader(
              { ...coldPages, read: concurrentSpace.read.bind(concurrentSpace) },
              shared,
            );
            const both = await Promise.all([
              concurrentReader.groupRoot(concurrentRef, grant),
              concurrentReader.groupRoot(concurrentRef, grant),
            ]);
            expect(both[0].value).toBe(both[1].value);
            both.forEach((h) => h.release());
            cache.clear();
            reach.reach("concurrent admission drains");
            let resume: () => void = () => undefined;
            const blocked = new Promise<void>((resolve) => {
              resume = resolve;
            });
            queries = new QueryRoots(governor, [{ tableId: 1, columns: wanted }], shared);
            const group = new GroupRead(
              concurrentRef,
              {
                peek: () => null,
                read: async (refs, options) => {
                  await blocked;
                  return concurrentSpace.read(refs, options);
                },
              },
              grant,
              queries,
              1,
            );
            const pending = Promise.resolve(group.prepare(wanted));
            group.release();
            resume();
            await expect(pending).rejects.toThrow("Group metadata read ended");
            queries.close();
            queries = undefined;
            expect(grant.allocated).toBe(0);
            expect(cache.evictableBytes).toBe(cache.residentBytes);
            cache.clear();
            reach.reach("cancelled borrower hands off or releases");
            // Corruption in an UNSELECTED column still refuses cold validation.
            const bad = encodeGroupRoot({
              ...root,
              columns: root.columns.map((c, i) =>
                i === count - 2
                  ? { ...c, statistics: { ...c.statistics, nullCount: rowCount + 1 } }
                  : c,
              ),
            });
            expect(() => decodeGroupRoot(bad, grant)).toThrow(ManifestCorruptionError);
            expect(() => tryDecodeGroupRootView(bad, grant)).toThrow(ManifestCorruptionError);
            expect(grant.allocated).toBe(0);
            reach.reach("unselected corruption refused");
            // Complete validity and stored bytes are unchanged, including column row IDs/types.
            const eager = decodeGroupRoot(bytes, grant);
            const lazy = tryDecodeGroupRootView(bytes, grant);
            if (!lazy) throw new Error("optional test headroom");
            expect(lazy.value).toEqual(eager.value);
            expect(encodeGroupRoot(lazy.value)).toEqual(bytes);
            grant.free(lazy.charge);
            grant.free(eager.charge);
            reach.reach("complete eager equality");
            // Buffer is a valid Uint8Array input, but its slice() aliases the source slab.
            const sourceBuffer = Buffer.from(bytes);
            const fromBuffer = tryDecodeGroupRootView(sourceBuffer, grant);
            if (fromBuffer === null) throw new Error("optional Buffer test headroom");
            sourceBuffer.fill(0);
            expect(encodeGroupRoot(fromBuffer.value)).toEqual(bytes);
            grant.free(fromBuffer.charge);
            reach.reach("Buffer inputs own a tight copy");
          } finally {
            queries?.close();
            unbind();
            cache.clear();
            grant.close();
            expect(metadataCachesFor(space)).toBeUndefined();
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero charges and detached binding");
          }
        },
      ),
      { seed, numRuns: 20 },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-validated-root-pressure", [0xb2f3, 0xb2f4]))(
  "optional sizing workspace never waits or changes the native bound (seed %i)",
  async (seed) => {
    const reach = createReach("validated roots under pressure", VALIDATED_ROOT_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 256, max: 288 }), async (count) => {
        reach.beginHistory();
        const space = new MemoryExtentSpace(),
          governor = new Governor(8 << 20);
        const extent = { offset: 4096, length: 1024 };
        const root: GroupRoot = {
          rowCount: 1,
          rowIds: { kind: "dense" },
          columns: Array.from({ length: count }, (_, i) => ({
            columnId: i + 1,
            type: { kind: "int32", bits: 32 },
            data: { extent, offset: 0, length: 128 },
            directory: { extent, offset: 128, length: 0, checksum: 0 },
            statistics: { nullCount: 0, min: null, max: null, dictionarySize: null },
            distinctSketch: null,
          })),
        };
        const ref = (await space.beginWrite("sizing").write([encodeGroupRoot(root)])).blobs[0];
        if (!ref) throw new Error("sizing ref");
        const cache = new BlobCache<GroupRoot>((c) => governor.registerCache(c), fullBlobKey);
        const shared = { groupRoots: cache, validatedRoots: true };
        const queries = new QueryRoots(governor, [{ tableId: 1, columns: [] }], shared);
        const view = {
          pages: space,
          table: () => ({
            tableId: 1,
            liveRowCount: 1,
            columns: [],
            segments: [{ kind: "group", rowCount: 1, liveRowCount: 1, group: { root: ref } }],
          }),
        } as unknown as Snapshot;
        const source = {
          id: 1,
          tableId: 1,
          columns: [],
          types: [],
          windowBytes: 0,
          stagingBytes: 0,
          rows: null,
        };
        const signal = new AbortController().signal;
        const before = await boundSource(view, source, governor, signal, 4096);
        const original = governor.tryAdmit.bind(governor),
          group = governor.admitGroup.bind(governor);
        const mandatory = Math.max(65536, parseCharge(ref.length) + ref.length);
        let refused = 0;
        const admission = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
          if (options.label === "read-step/sizing" && options.minimumBytes > mandatory) {
            refused++;
            return null;
          }
          return original(options);
        });
        const queued = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
          if (options.label === "read-step/sizing")
            expect(
              options.grants.reduce((n, part) => n + part.minimumBytes, 0),
            ).toBeLessThanOrEqual(mandatory);
          return group(options);
        });
        try {
          const after = await boundSource(view, source, governor, signal, 4096, undefined, queries);
          expect(after).toEqual(before);
          expect(refused).toBeGreaterThan(0);
          reach.reach("optional workspace refused synchronously");
          reach.reach("ordinary native bound unchanged");
          reach.reach("no optional FIFO wait");
        } finally {
          admission.mockRestore();
          queued.mockRestore();
          queries.close();
          cache.clear();
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero charges after fallback");
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  },
);
