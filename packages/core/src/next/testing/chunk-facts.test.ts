/** Admission and execution share immutable validated chunk metadata; no sizing reader survives. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, QUERY_CHUNK_FACTS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { fullBlobKey } from "../format/blob-cache.js";
import * as directory from "../page/directory.js";
import { ChunkReader } from "../page/reader.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-query-chunk-facts", [0xb321, 0xb322]))
  it(`opens selected inline chunks once across admission and execution seed ${String(seed)}`, async () => {
    const reach = createReach("query chunk facts", QUERY_CHUNK_FACTS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2049, max: 4097 }),
        fc.integer({ min: 2, max: 17 }),
        fc.boolean(),
        async (count, period, extra) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open();
          try {
            await core.execute(
              "CREATE TABLE chunk_facts (i INTEGER,f DOUBLE PRECISION,b BOOLEAN,unused TEXT)",
            );
            const rows = Array.from({ length: count }, (_, r) => ({
              i: r,
              f: r % period === 0 ? null : r / 8,
              b: r % 3 === 0,
              unused: `unused:${String(r % 23)}`,
            }));
            await core.writeBatch("insertBatch", "chunk_facts", rows);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const selected = extra ? ["i", "f", "b"] : ["i", "f"];
            let blockedKey: string | undefined;
            for (let query = 0; query < 2; query++) {
              const opens = vi.spyOn(ChunkReader, "open"),
                parses = vi.spyOn(directory, "parseDirectory");
              try {
                const result = await core.execute(`SELECT ${selected.join(",")} FROM chunk_facts`);
                if (!("batches" in result)) throw new Error("query rows");
                expect(oracleRows(result)).toEqual(
                  rows.map(({ i, f, b }) => (extra ? { i, f, b } : { i, f })),
                );
                const keys = new Set(
                  opens.mock.calls.map(([, chunk]) => fullBlobKey(chunk.directory)),
                );
                const first = opens.mock.calls[0]?.[1];
                if (first) blockedKey = fullBlobKey(first.directory);
                expect(keys.size).toBeGreaterThanOrEqual(selected.length);
                expect(opens.mock.calls.length).toBe(keys.size);
                expect(parses.mock.calls.length).toBe(keys.size);
                reach.reach("one active inline reader per chunk");
                reach.reach("one directory parse per query");
                reach.reach("fresh query owns its directory facts");
                reach.reach("selected columns and multiple batches");
                reach.reach("NULL and exact source values");
              } finally {
                opens.mockRestore();
                parses.mockRestore();
              }
            }
            let resume: () => void = () => undefined,
              enter: () => void = () => undefined,
              blocked = false;
            const gate = new Promise<void>((resolve) => {
                resume = resolve;
              }),
              entered = new Promise<void>((resolve) => {
                enter = resolve;
              });
            const original = core.harness.space.read.bind(core.harness.space);
            const read = vi
              .spyOn(core.harness.space, "read")
              .mockImplementation(async (refs, options) => {
                if (!blocked && refs.some((ref) => fullBlobKey(ref) === blockedKey)) {
                  blocked = true;
                  enter();
                  await gate;
                }
                return original(refs, options);
              });
            const run = core.start(core.prepare(`SELECT ${selected.join(",")} FROM chunk_facts`));
            const next = run.execution.results.next().then(
              (value) => ({ value }),
              (error: unknown) => ({ error }),
            );
            try {
              await entered;
              run.execution.cancel(new Error("cancel blocked directory preparation"));
              resume();
              expect(await next).toHaveProperty("error");
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              reach.reach("cancel before admission drains facts");
            } finally {
              resume();
              run.execution.cancel(new Error("cleanup"));
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              run.release();
              read.mockRestore();
            }
          } finally {
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          reach.reach("zero charges after query and owner close");
        },
      ),
      { seed, numRuns: 4, endOnFailure: true },
    );
    reach.assertReached();
  });

// Fresh reader/space per reference call: no parsed-descriptor or range memo is shared.
import { boundSource } from "../page/read-bounds.js";
import { QueryRoots } from "../page/query-roots.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { writeGroup } from "../page/test-support.js";
import { encodeGroupRoot } from "../format/manifest-codec.js";
import { Governor } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { Snapshot } from "../access/snapshot.js";
import { PageEncoding } from "../format/manifest.js";
import { PREPARED_BOUND_REACH } from "../../testing/reach.js";

for (const shape of ["plain", "dictionary", "large", "gzip"] as const)
  for (const seed of seedsFor("next-prepared-read-bound", [0xb323, 0xb324]))
    it(`prepared ${shape} bounds equal fresh-reader bounds seed ${String(seed)}`, async () => {
      const reach = createReach(`prepared bounds ${shape}`, PREPARED_BOUND_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 129, max: 257 }),
          fc.integer({ min: 1, max: 17 }),
          fc.constantFrom("all", "subset", "empty"),
          fc.boolean(),
          fc.boolean(),
          async (count, salt, selection, partial, windowed) => {
            reach.beginHistory();
            const storage = new MemoryExtentSpace(),
              encoder = new TextEncoder();
            const types: readonly [PhysicalType, PhysicalType] = [
              { kind: "int32", bits: 32 },
              shape === "plain" ? { kind: "float64" } : { kind: "text" },
            ];
            const segments = [];
            for (let g = 0; g < 2; g++) {
              const values = Array.from({ length: count }, (_, r) =>
                shape === "plain"
                  ? r / 8 + salt + g
                  : encoder.encode(
                      shape === "large" && r === 0
                        ? "L".repeat(70000 + salt)
                        : shape === "gzip"
                          ? `${String((r + g) % 7)}:${"x".repeat(510)}`
                          : `v${String((r + salt + g) % 13)}`,
                    ),
              );
              const made = await writeGroup(
                [
                  {
                    type: types[0],
                    values: Array.from({ length: count }, (_, r) => r + salt + g),
                  },
                  { type: types[1], values },
                ],
                {
                  space: storage,
                  gzip: shape === "gzip",
                  ...(shape === "dictionary" ? {} : { dictionaryEntries: 0 }),
                },
              );
              const header = {
                rowCount: count,
                rowIds: { kind: "dense" as const },
                columns: made.group.columns,
              };
              const ref = (
                await storage.beginWrite("bound group root").write([encodeGroupRoot(header)])
              ).blobs[0];
              if (!ref) throw new Error("root ref");
              segments.push({
                kind: "group" as const,
                rowCount: count,
                liveRowCount: count,
                group: { root: ref },
              });
              if (shape !== "plain") {
                const chunk = made.group.columns[1];
                if (!chunk) throw new Error("chunk");
                const reader = await ChunkReader.open(storage, chunk, {
                  budget: { allocate: () => true, free: () => undefined },
                });
                try {
                  if (shape === "gzip")
                    expect(reader.directory.pages.some((p) => p.encoding & PageEncoding.gzip)).toBe(
                      true,
                    );
                  if (shape === "large")
                    expect(reader.directory.pages.some((p) => p.largeRegion !== undefined)).toBe(
                      true,
                    );
                  if (shape === "dictionary") expect(reader.directory.dictionary).not.toBeNull();
                } finally {
                  reader.close();
                }
              }
            }
            const columns = selection === "empty" ? [] : selection === "subset" ? [2] : [1, 2];
            const source = {
              id: 1,
              tableId: 1,
              columns,
              types: columns.map((id) => {
                const type = types[id - 1];
                if (!type) throw new Error("type");
                return type;
              }),
              stagingBytes: 0,
              windowBytes: windowed ? 65536 : 0,
              rows: partial ? count - 1 : null,
            };
            const table = {
              tableId: 1,
              liveRowCount: 2 * count,
              columns: [{ id: 1 }, { id: 2 }],
              segments,
            };
            const governor = new Governor(16 << 20),
              referenceGovernor = new Governor(16 << 20);
            const cache = new QueryRoots(governor, [{ tableId: 1, columns }]);
            const signal = new AbortController().signal;
            const snapshot = {
              table: () => table,
              pages: storage.view({ cache: false }),
            } as unknown as Snapshot;
            const fresh = {
              table: () => table,
              pages: storage.view({ cache: false }),
            } as unknown as Snapshot;
            try {
              const referenceOpens = vi.spyOn(ChunkReader, "open");
              let expected;
              try {
                expected = await boundSource(fresh, source, referenceGovernor, signal, 4096);
                expect(referenceOpens.mock.calls.length > 0).toBe(selection !== "empty");
              } finally {
                referenceOpens.mockRestore();
              }
              const actual = await boundSource(
                snapshot,
                source,
                governor,
                signal,
                4096,
                undefined,
                cache,
              );
              expect(actual).toEqual(expected);
              // ReadBound.progress funds four copies of the largest source value (R25),
              // independently of both implementations under comparison.
              if (shape === "large" && selection !== "empty")
                expect(actual.progress).toBeGreaterThanOrEqual(4 * (70000 + salt));
              // A different prefix/target revisits the immutable facts, not a cached visibility bound.
              const changed = { ...source, rows: count + Math.floor(count / 2), windowBytes: 0 };
              const next = await boundSource(
                {
                  table: () => table,
                  pages: storage.view({ cache: false }),
                },
                changed,
                referenceGovernor,
                signal,
                1,
                1,
              );
              expect(await boundSource(snapshot, changed, governor, signal, 1, 1, cache)).toEqual(
                next,
              );
              if (selection === "empty") reach.reach("empty selection");
              if (selection === "subset") reach.reach("selected column subset");
              reach.reach("fresh uncached reference");
              reach.reach("complete bounds unchanged");
              reach.reach("multiple groups and changed prefixes");
            } finally {
              cache.close();
            }
            expect(governor.usedBytes).toBe(0);
            expect(referenceGovernor.usedBytes).toBe(0);
            reach.reach("zero charges");
          },
        ),
        { seed, numRuns: 12, endOnFailure: true },
      );
      reach.assertReached();
    });
