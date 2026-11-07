/** Window work obeys the worker task clock even when every storage promise resolves at once.
 * The group/root descriptors are fixture stubs; page bytes, checksums, leases and scans are real.
 * Virtual CPU ticks make the scheduling law deterministic, not a throughput measurement.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createReach, SCAN_WINDOW_PACING_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { ColumnDef } from "../access/catalog.js";
import type { GroupSegment, TableSnapshot } from "../access/snapshot.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { beginTask, taskClock } from "../exec/pipeline.js";
import { chunkReads, type ChunkReads } from "../exec/scan.js";
import { TableScan } from "../exec/table-scan.js";
import { writeGroup } from "../page/test-support.js";
import { PageCache } from "../storage/page-cache.js";
import type { StoreSession } from "../storage/contract.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";

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
  const written = await writeGroup(values.map((v) => ({ type: TYPE, values: Array.from(v) })));
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
  return {
    data: written.space.bytes,
    chunks: written.group.columns,
    first: values[0] ?? new Float64Array(),
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
