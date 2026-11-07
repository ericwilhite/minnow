/** Window work obeys the worker task clock even when every storage promise resolves at once.
 * The group/root descriptors are fixture stubs; page bytes, checksums, leases and scans are real.
 * Virtual CPU ticks make the scheduling law deterministic, not a throughput measurement.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  createReach,
  SCAN_WINDOW_PACING_REACH,
  SCAN_WINDOW_CANCEL_REACH,
  SHARED_WINDOW_CLOSE_REACH,
  WINDOW_CANCEL_REJECTION_REACH,
} from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { ColumnDef } from "../access/catalog.js";
import type { GroupSegment, TableSnapshot } from "../access/snapshot.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { beginTask, taskClock } from "../exec/pipeline.js";
import { chunkReads, type ChunkReads } from "../exec/scan.js";
import { TableScan } from "../exec/table-scan.js";
import { writeGroup } from "../page/test-support.js";
import { ChunkReader, dataAddress } from "../page/reader.js";
import { PageCache } from "../storage/page-cache.js";
import { blobAddress, type BlobRef } from "../storage/extents.js";
import type { StoreSession } from "../storage/contract.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import { allocateFlat } from "../vector/vector.js";

const ROWS = 65_536;
const TYPE = { kind: "float64" } as const;

async function makeFixture() {
  const random = mulberry32(0xb2ca);
  const values = Array.from({ length: 24 }, () => {
    const words = new Uint32Array(2 * ROWS);
    for (let k = 0; k < words.length; k += 2) {
      words[k] = random() * 2 ** 32;
      const high = random() * 2 ** 32;
      // Finite, varied exponents/mantissas: raw f64 pages, not scaled/constant compression.
      words[k + 1] = (high & 0x800fffff) | ((1 + (high % 2046)) << 20);
    }
    return new Float64Array(words.buffer);
  });
  const encoder = new TextEncoder();
  const written = await writeGroup([
    ...values.map((v) => ({ type: TYPE, values: Array.from(v) })),
    {
      type: { kind: "text" },
      values: Array.from({ length: ROWS }, (_, k) => encoder.encode(`label${String(k % 8)}`)),
    },
  ]);
  const defs: ColumnDef[] = values.map((_, k) => ({
    id: k + 1,
    name: `c${String(k)}`,
    type: TYPE,
    declared: { name: "float8" } as never,
    nullable: false,
    defaultSql: null,
    generatedSql: null,
    identitySequence: null,
    missing: null,
  }));
  const firstDef = defs[0];
  if (!firstDef) throw new Error("Missing first definition");
  defs.push({ ...firstDef, id: 25, name: "dictionary", type: { kind: "text" } });
  return {
    data: written.space.bytes,
    chunks: written.group.columns,
    first: values[0] ?? new Float64Array(),
    values,
    table(columns: number): TableSnapshot {
      const segment: GroupSegment = {
        kind: "group",
        index: 0,
        rowCount: ROWS,
        liveRowCount: ROWS,
        staged: false,
        group: {} as GroupSegment["group"],
        root: () => ({}) as Awaited<ReturnType<GroupSegment["root"]>>,
        chunk: (id) => written.group.columns[id - 1] ?? null,
        deletions: () => null,
        statistics: () => null,
        pages: () => [],
      };
      return {
        tableId: 17,
        schemaVersion: 1,
        columns: defs.slice(0, columns),
        segments: [segment],
        liveRowCount: ROWS,
      };
    },
  };
}
let fixture: ReturnType<typeof makeFixture> | undefined;
const fixtureOf = () => (fixture ??= makeFixture());

/** Synchronous backend work, followed by an already-resolved continuation (OPFS's shape). */
function context(data: Uint8Array, beforeRead: () => Promise<void> | void) {
  const memory = countingMemory(Infinity);
  let calls = 0;
  const cache = new PageCache({
    readDataInto(at: number, out: Uint8Array) {
      calls++;
      const wait = beforeRead();
      const read = () => {
        out.set(data.subarray(at, at + out.length));
      };
      if (wait !== undefined) return wait.then(read);
      read();
      return Promise.resolve();
    },
  } as StoreSession);
  cache.attach(memory);
  return {
    cache,
    memory,
    calls: () => calls,
    pages: {
      read: cache.read.bind(cache),
      peek: cache.peek.bind(cache),
      parsed: cache.parsed.bind(cache),
      attach: cache.attachParsed.bind(cache),
    },
    close() {
      cache.close();
      expect(memory.used).toBe(0);
    },
  };
}

describe("coalesced scan window task lifecycle", () => {
  it.each(seedsFor("next-window-cancel-rejection", [0xb2c7, 0xb2c8]))(
    "standalone cancellation rejection is observed while preparation yields (seed %i)",
    async (seed) => {
      const source = await fixtureOf();
      const reach = createReach("next-window-cancel-rejection", WINDOW_CANCEL_REJECTION_REACH);
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 2, max: 4 }), fc.boolean(), async (delay, microtask) => {
          reach.beginHistory();
          let now = 0;
          const ctx = context(source.data, () => undefined);
          const budget = countingBudget(Infinity),
            arena = new OutputArena(budget, 2048);
          const unhandled: unknown[] = [];
          const record = (error: unknown) => {
            unhandled.push(error);
          };
          process.on("unhandledRejection", record);
          const reads: ChunkReads = {
            open(p, c, options) {
              return chunkReads.open(p, c, {
                ...options,
                task: {
                  ...options.task,
                  yieldTask: () =>
                    new Promise<void>((resolve) =>
                      setTimeout(resolve, c.columnId === 25 ? 0 : delay),
                    ),
                },
              });
            },
            loadTogether(loads) {
              expect(loads.some(({ reader }) => reader.isDictionaryRange(0, 1))).toBe(true);
              reach.reach("dictionary pending");
              now = 5;
              const cancel = () => {
                scan.abort();
                reach.reach("cancel while yielded");
              };
              if (microtask) queueMicrotask(cancel);
              else setTimeout(cancel, 0);
              return chunkReads.loadTogether?.(loads);
            },
          };
          const scan = new TableScan({
            table: source.table(25),
            pages: ctx.pages,
            reads,
            budget,
            columns: Array.from({ length: 25 }, (_, k) => k + 1),
            filter: null,
            emitLocator: false,
            readWindowBytes: 64 * 1024 * 1024,
            now: () => now,
            context: { arena, params: { slots: [] }, input: [] },
          });
          try {
            expect(await scan.next()).toBeNull();
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
            expect(unhandled, "started rejections must be observed immediately").toHaveLength(0);
            reach.reach("rejection observed");
          } finally {
            scan.close();
            arena.close();
            ctx.close();
            process.off("unhandledRejection", record);
          }
          expect(budget.used).toBe(0);
        }),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
    },
    120_000,
  );
  it.each(seedsFor("next-window-scan-cancel", [0xb2c3, 0xb2c4]))(
    "a blocked native read drains, but cancel starts no more backend calls (seed %i)",
    async (seed) => {
      const source = await fixtureOf();
      const reach = createReach("next-window-scan-cancel", SCAN_WINDOW_CANCEL_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(16, 24),
          fc.constantFrom(1, 3),
          async (columns, blockAt) => {
            reach.beginHistory();
            let entered = (): void => undefined,
              unblock = (): void => undefined;
            const arrived = new Promise<void>((r) => {
              entered = r;
            });
            const gate = new Promise<void>((r) => {
              unblock = r;
            });
            let inWindow = false,
              nativeCalls = 0,
              blocked = false,
              attempt = 0;
            const ctx = context(source.data, () => {
              if (inWindow && ++nativeCalls === blockAt && !blocked) {
                blocked = true;
                entered();
                return gate;
              }
            });
            const signals = [
              new Set<AbortSignal | undefined>(),
              new Set<AbortSignal | undefined>(),
            ];
            const storageSignals = new Set<AbortSignal | undefined>();
            const read: typeof ctx.pages.read = (refs, options) => {
              if (inWindow && attempt === 0) storageSignals.add(options.signal);
              return ctx.pages.read(refs, options);
            };
            let windowDrain: Promise<void> | undefined;
            const reads: ChunkReads = {
              open(p, c, options) {
                signals[attempt]?.add(options.signal);
                return chunkReads.open(p, c, options);
              },
              loadTogether(loads) {
                inWindow = true;
                const work = Promise.resolve(chunkReads.loadTogether?.(loads));
                windowDrain = work;
                return work.finally(() => {
                  inWindow = false;
                });
              },
            };
            const budget = countingBudget(Infinity),
              arena = new OutputArena(budget, 2048);
            const scan = new TableScan({
              table: source.table(columns),
              pages: { ...ctx.pages, read },
              reads,
              budget,
              columns: Array.from({ length: columns }, (_, k) => k + 1),
              filter: null,
              emitLocator: false,
              readWindowBytes: 64 * 1024 * 1024,
              now: () => 0,
              context: { arena, params: { slots: [] }, input: [] },
            });
            let settled = false;
            const pending = Promise.resolve(scan.next()).finally(() => {
              settled = true;
            });
            try {
              expect(
                await Promise.race([arrived.then(() => true), pending.then(() => false)]),
              ).toBe(true);
              reach.reach(
                blockAt === 1 ? "first native read blocked" : "later native read blocked",
              );
              const atCancel = ctx.calls();
              await new Promise<void>((resolve) =>
                setTimeout(() => {
                  scan.abort();
                  resolve();
                }, 0),
              );
              await Promise.resolve();
              expect(settled, "started native work must drain before the scan settles").toBe(false);
              expect(budget.used, "the started buffer must remain charged").toBeGreaterThan(0);
              reach.reach("started work remains charged");
              unblock();
              expect(await pending).toBeNull();
              expect(ctx.calls(), "cancel must not start another backend read").toBe(atCancel);
              reach.reach("no backend calls after cancel");
              const firstSignal = [...(signals[0] ?? [])][0];
              expect(firstSignal, "scan-owned signal must reach readers").toBeDefined();
              expect(signals[0]?.size).toBe(1);
              expect(firstSignal?.aborted).toBe(true);
              expect([...storageSignals].every((s) => s === firstSignal)).toBe(true);
              reach.reach("signal forwarded to storage");
              scan.close();
              arena.close();
              expect(budget.used).toBe(0);
              attempt = 1;
              const retryBudget = countingBudget(Infinity),
                retryArena = new OutputArena(retryBudget, 2048);
              const retry = new TableScan({
                table: source.table(columns),
                pages: ctx.pages,
                reads,
                budget: retryBudget,
                columns: Array.from({ length: columns }, (_, k) => k + 1),
                filter: null,
                emitLocator: false,
                readWindowBytes: 64 * 1024 * 1024,
                now: () => 0,
                context: { arena: retryArena, params: { slots: [] }, input: [] },
              });
              try {
                let rows = 0;
                for (let item = await retry.next(); item !== null; item = await retry.next())
                  rows += item.batch.length;
                expect(rows).toBe(ROWS);
                const retrySignal = [...(signals[1] ?? [])][0];
                expect(retrySignal).toBeDefined();
                expect(retrySignal).not.toBe(firstSignal);
                expect(retrySignal?.aborted).toBe(false);
                reach.reach("fresh signal retry");
              } finally {
                retry.close();
                retryArena.close();
              }
              expect(retryBudget.used).toBe(0);
            } finally {
              unblock();
              await Promise.allSettled([pending, ...(windowDrain ? [windowDrain] : [])]);
              scan.close();
              arena.close();
              ctx.close();
            }
            expect(budget.used).toBe(0);
          },
        ),
        { seed, numRuns: 32 },
      );
      reach.assertReached();
    },
    120_000,
  );

  it.each(seedsFor("next-shared-window-close", [0xb2c5, 0xb2c6]))(
    "closed readers lose unstarted jobs while other readers still finish (seed %i)",
    async (seed) => {
      const source = await fixtureOf();
      const reach = createReach("next-shared-window-close", SHARED_WINDOW_CLOSE_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(16, 24),
          fc.constantFrom("all", "tail", "alternating"),
          fc.boolean(),
          fc.constantFrom(1, 2),
          async (columns, shape, distinctSignal, blockAt) => {
            reach.beginHistory();
            let entered = (): void => undefined,
              unblock = (): void => undefined;
            const arrived = new Promise<void>((r) => {
              entered = r;
            });
            const gate = new Promise<void>((r) => {
              unblock = r;
            });
            let active = false,
              afterClose = false,
              calls = 0;
            const ctx = context(source.data, () => {
              if (active && ++calls === blockAt) {
                entered();
                return gate;
              }
            });
            const requestedAfterClose: BlobRef[] = [];
            const read: typeof ctx.pages.read = (refs, options) => {
              if (afterClose) requestedAfterClose.push(...refs);
              return ctx.pages.read(refs, options);
            };
            const pages = { ...ctx.pages, read },
              budget = countingBudget(Infinity);
            const controller = new AbortController(),
              reason = new Error("reader canceled");
            const readers: ChunkReader[] = [];
            for (let k = 0; k < columns; k++) {
              const chunk = source.chunks[k];
              if (!chunk) throw new Error("Missing fixture chunk");
              readers.push(
                await ChunkReader.open(pages, chunk, {
                  budget,
                  once: true,
                  ...(distinctSignal && k === 0 ? { signal: controller.signal } : {}),
                }),
              );
            }
            const loads = readers.map((reader) => {
              const [from, end] = reader.pagesInRange(0, ROWS);
              return { reader, pages: Array.from({ length: end - from }, (_, k) => from + k) };
            });
            active = true;
            const pending = Promise.resolve(ChunkReader.loadTogether(loads));
            const closed = new Set<number>();
            try {
              expect(
                await Promise.race([arrived.then(() => true), pending.then(() => false)]),
              ).toBe(true);
              for (let k = 0; k < columns; k++)
                if (
                  shape === "all" ||
                  (shape === "tail" && k >= columns - 4) ||
                  (shape === "alternating" && k % 2 === 0) ||
                  (distinctSignal && k === 0)
                ) {
                  readers[k]?.close();
                  closed.add(k);
                }
              if (distinctSignal) controller.abort(reason);
              afterClose = true;
              unblock();
              const failure = await pending.then(
                () => null,
                (error: unknown) => error,
              );
              if (distinctSignal && failure !== null) expect(failure).toBe(reason);
              else expect(failure).toBeNull();
              const ranges = [...closed].map((k) => {
                const data = source.chunks[k]?.data;
                if (!data) throw new Error("Missing closed chunk");
                return [dataAddress(data), dataAddress(data) + data.length] as const;
              });
              expect(
                requestedAfterClose.every(
                  (ref) =>
                    !ranges.some(
                      ([from, end]) => blobAddress(ref) >= from && blobAddress(ref) < end,
                    ),
                ),
                "unstarted closed-reader refs must be skipped",
              ).toBe(true);
              reach.reach("closed jobs omitted");
              if (shape === "all") {
                expect(requestedAfterClose).toHaveLength(0);
                reach.reach("all readers closed");
              } else {
                const target = allocateFlat(TYPE, 1);
                if (target.data.layout !== "f64") throw new Error("Bad target");
                for (let k = 0; k < readers.length; k++) {
                  if (closed.has(k)) continue;
                  const reader = readers[k];
                  if (!reader) throw new Error("Missing live reader");
                  for (const page of loads[k]?.pages ?? [])
                    expect(reader.isLoaded(page), "live reader pages must finish").toBe(true);
                  for (const row of [0, ROWS - 1]) {
                    reader.decodeRange(row, row + 1, target);
                    expect(target.data.values[0]).toBe(source.values[k]?.[row]);
                  }
                }
                reach.reach("live readers complete");
                reach.reach(shape === "tail" ? "closed suffix" : "alternating closure");
                if (distinctSignal) reach.reach("independent cancel signals");
              }
            } finally {
              unblock();
              await Promise.allSettled([pending]);
              for (const reader of readers) reader.close();
              ctx.close();
            }
            expect(budget.used).toBe(0);
          },
        ),
        { seed, numRuns: 48 },
      );
      reach.assertReached();
    },
    120_000,
  );

  it.each(seedsFor("next-window-task-pacing", [0xb2c1, 0xb2c2]))(
    "pending timers and cancel messages run inside a multi-read window (seed %i)",
    async (seed) => {
      const source = await fixtureOf();
      const reach = createReach("next-window-task-pacing", SCAN_WINDOW_PACING_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(16, 24),
          fc.constantFrom({ priorWork: 3, tick: 1 }, { priorWork: 0, tick: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (columns, { priorWork, tick }, cancel, follow) => {
            reach.beginHistory();
            const oldNow = taskClock.now;
            let now = 0,
              inWindow = false,
              windowCalls = 0,
              firstYieldCalls = 0,
              timerInside = false,
              forwardedClock = true;
            taskClock.now = () => now;
            beginTask();
            // Another operator used this same task before the scan and resolved storage callbacks.
            now = priorWork;
            const ctx = context(source.data, () => undefined);
            // One bounded extent read can contain several native slab reads. Charge virtual
            // work per extent-read boundary, where the reader can actually yield.
            const read: typeof ctx.pages.read = (refs, options) => {
              if (inWindow) {
                now += tick;
                windowCalls++;
              }
              return ctx.pages.read(refs, options);
            };
            const budget = countingBudget(Infinity),
              arena = new OutputArena(budget, 2048);
            let windowLoads: Parameters<NonNullable<ChunkReads["loadTogether"]>>[0] = [];
            let follower: Promise<void> | undefined;
            const reads: ChunkReads = {
              open(pages, chunk, options) {
                forwardedClock &&= options.task?.clock === taskClock;
                return chunkReads.open(pages, chunk, {
                  ...options,
                  task: {
                    clock: taskClock,
                    yieldTask: () => {
                      firstYieldCalls ||= windowCalls;
                      return new Promise<void>((resolve) =>
                        setTimeout(() => {
                          if (follow && !cancel && follower === undefined) {
                            follower = Promise.resolve(chunkReads.loadTogether?.(windowLoads));
                          }
                          resolve();
                        }, 0),
                      );
                    },
                  },
                });
              },
              async loadTogether(loads) {
                windowLoads = loads;
                inWindow = true;
                const timer = setTimeout(() => {
                  timerInside = inWindow;
                  if (cancel) scan.abort();
                }, 0);
                try {
                  await chunkReads.loadTogether?.(loads);
                  await follower;
                } finally {
                  inWindow = false;
                  clearTimeout(timer);
                }
              },
            };
            const scan = new TableScan({
              table: source.table(columns),
              columns: Array.from({ length: columns }, (_, k) => k + 1),
              pages: { ...ctx.pages, read },
              reads,
              budget,
              readWindowBytes: 64 * 1024 * 1024,
              filter: null,
              emitLocator: false,
              context: { arena, params: { slots: [] }, input: [] },
            });
            let rows = 0;
            try {
              for (let item = await scan.next(); item !== null; item = await scan.next()) {
                const first = item.batch.columns[0];
                if (first?.form !== "flat" || first.data.layout !== "f64")
                  throw new Error("Bad fixture vector");
                expect(first.data.values[0]).toBe(source.first[rows]);
                rows += item.batch.length;
              }
              expect(timerInside, "timer must run before loadTogether returns").toBe(true);
              expect(forwardedClock, "the scan must forward its shared task clock").toBe(true);
              expect(rows).toBe(cancel ? 0 : ROWS);
              expect(
                source.chunks.slice(0, columns).reduce((n, c) => n + c.data.length, 0),
              ).toBeGreaterThan(8 * 1024 * 1024);
              if (!cancel) expect(windowCalls).toBeGreaterThan(1);
              reach.reach("multi-read window");
              reach.reach("immediately resolved storage");
              if (follow && !cancel) {
                expect(follower).toBeDefined();
                reach.reach("concurrent follower");
                if (columns === 24 && tick === 3) reach.reach("follower across later yield");
              }
              reach.reach(cancel ? "cancel message inside window" : "timer inside window");
              if (priorWork === 3) {
                expect(firstYieldCalls, "resolved storage must not restart the clock").toBe(1);
                reach.reach("carried task budget");
              }
            } finally {
              scan.close();
              arena.close();
              ctx.close();
              taskClock.now = oldNow;
              beginTask();
            }
            expect(budget.used).toBe(0);
          },
        ),
        { seed, numRuns: 64 },
      );
      reach.assertReached();
    },
    120_000,
  );
});
