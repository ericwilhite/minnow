/** The Gate A bridge must exercise the production executor, carry exact values through actual
 * transfers and credits, and return every execution charge. Input storage must survive transfer.
 */
import fc from "fast-check";
import { describe, expect, it, vi } from "vitest";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import {
  createReach,
  EXECUTOR_EXPORT_REACH,
  EXPORT_RETURN_REACH,
  EXECUTOR_POLICY_REACH,
  RESULT_OWNED_REACH,
} from "../../../packages/core/src/testing/reach.js";
import * as factory from "../../../packages/core/src/next/exec/create.js";
import { PageCache } from "../../../packages/core/src/next/storage/page-cache.js";
import { VERIFY_CACHED_BYTES } from "../../../packages/core/src/next/storage/verify-cached.js";
import { ResultSink } from "../../../packages/core/src/next/exec/result-sink.js";
import { ColumnTarget } from "../../../packages/core/src/next/exec/column-target.js";
import type { StoreSession } from "../../../packages/core/src/next/storage/contract.js";
import type { ExtentReader } from "../../../packages/core/src/next/storage/extents.js";
import { chunkReads } from "../../../packages/core/src/next/exec/scan.js";
import type { PhysicalPlan, ScalarExpr } from "../../../packages/core/src/next/ir/ir.js";
import { numbers } from "../../../packages/core/src/next/exec/kernels/test-support.js";
import { Governor } from "../../../packages/core/src/next/memory/governor.js";
import { ReturnPool } from "../../../packages/core/src/next/exec/return-pool.js";
import { recyclingFrame } from "../../../packages/core/src/next/owner/recycling-frame.js";
import { StreamDictionaries } from "../../../packages/core/src/next/owner/frame.js";
import { MemoryExtentSpace } from "../../../packages/core/src/next/page/memory-extents.js";
import {
  defined,
  openChunk,
  readChunk,
  sameValue,
  valueAt,
  writeGroup,
  type TestValue,
} from "../../../packages/core/src/next/page/test-support.js";
import { isValid } from "../../../packages/core/src/next/vector/validity.js";
import type { FlatVector } from "../../../packages/core/src/next/vector/vector.js";
import type { FramePayload } from "../../../packages/core/src/next/exec/result.js";
import { prepareExecutorExport } from "./executor-export.js";

// The R8 proof must fail by byte/buffer ownership, without a poison rehash detecting damage.
vi.mock("../../../packages/core/src/next/storage/verify-cached.js", async (original) => ({
  ...(await original<typeof import("../../../packages/core/src/next/storage/verify-cached.js")>()),
  VERIFY_CACHED_BYTES: false,
}));

describe.each(
  (["full", "identity", "subset", "project", "filter"] as const).flatMap((shape) =>
    [false, true].map((recycle) => ({ shape, recycle })),
  ),
)(
  "terminal policy $shape with explicit streaming export buffer recycling=$recycle",
  ({ shape, recycle }) => {
    it.each(seedsFor("next-executor-export", [0xb2e1, 0xb2e2]))(
      "production executor, exact MessagePort values, credits and cleanup (seed %i)",
      async (seed) => {
        const reach = createReach("next-executor-export", [
          ...EXECUTOR_EXPORT_REACH,
          ...(recycle ? EXPORT_RETURN_REACH.slice(0, 2) : []),
          RESULT_OWNED_REACH.cache,
          ...(shape === "full" ? [RESULT_OWNED_REACH.adopt] : []),
          shape === "full" || shape === "identity"
            ? EXECUTOR_POLICY_REACH.full
            : EXECUTOR_POLICY_REACH.cached,
        ]);
        const actualCreate = factory.createExecutor;
        const create = vi.spyOn(factory, "createExecutor").mockImplementation(() => {
          const executor = actualCreate();
          return {
            ...executor,
            prepare(plan: PhysicalPlan) {
              const root = plan.root;
              if (root.op !== "scan") throw new Error("Expected fixture scan");
              if (shape === "project" || shape === "identity")
                return executor.prepare({
                  ...plan,
                  root: {
                    id: 2,
                    op: "project",
                    input: root,
                    output: root.output,
                    exprs: root.output.map((c, index): ScalarExpr =>
                      shape === "project" && index === 0
                        ? { op: "cast", input: { op: "column", index, type: c.type }, type: c.type }
                        : { op: "column", index, type: c.type },
                    ),
                  },
                });
              if (shape === "subset")
                return executor.prepare({
                  ...plan,
                  columns: plan.columns.slice(0, 2),
                  root: {
                    ...root,
                    columns: root.columns.slice(0, 2),
                    output: root.output.slice(0, 2),
                  },
                });
              if (shape === "filter")
                return executor.prepare({
                  ...plan,
                  root: {
                    ...root,
                    filter: {
                      op: "compare",
                      cmp: ">=",
                      collation: "C",
                      type: { kind: "bool" },
                      left: { op: "column", index: 0, type: { kind: "int32", bits: 32 } },
                      right: {
                        op: "literal",
                        value: numbers({ kind: "int32", bits: 32 }, [0]),
                        type: { kind: "int32", bits: 32 },
                      },
                    },
                  },
                });
              return executor.prepare(plan);
            },
          };
        });
        const dedicated = new Set<ArrayBuffer>();
        const actualOwned = ColumnTarget.prototype.decodeOwned;
        const decodeOwned = vi
          .spyOn(ColumnTarget.prototype, "decodeOwned")
          .mockImplementation(function (this: ColumnTarget, ...args) {
            const result = actualOwned.apply(this, args);
            for (const buffer of result.buffers) dedicated.add(buffer);
            return result;
          });
        const batchTarget = vi.spyOn(ResultSink.prototype, "terminalBatchTarget");
        const opened = vi.spyOn(chunkReads, "open"),
          together = vi.spyOn(chunkReads, "loadTogether");
        try {
          await fc.assert(
            fc.asyncProperty(
              fc.integer({ min: 65, max: 160 }),
              fc.integer({ min: 1, max: 31 }),
              fc.boolean(),
              async (count, salt, returnBeforeCredit) => {
                reach.beginHistory();
                const space = new MemoryExtentSpace(),
                  governor = new Governor(64 * 1024 * 1024),
                  returnPool = recycle ? new ReturnPool(governor, 4 * 1024 * 1024) : undefined;
                const sources = Array.from({ length: 2 }, (_, group) => [
                  {
                    type: { kind: "int32", bits: 32 } as const,
                    values: Array.from({ length: count }, (_, row) => group * count + row + 1),
                  },
                  {
                    type: { kind: "text" } as const,
                    values: Array.from({ length: count }, (_, row) =>
                      row === 3
                        ? new Uint8Array(65536 + salt).fill(65 + group)
                        : (row + salt) % 7 === 0
                          ? null
                          : new TextEncoder().encode(
                              `${String(group)}:${String(row)}:${"水".repeat(salt)}`,
                            ),
                    ),
                  },
                  {
                    type: { kind: "int64" } as const,
                    values: Array.from(
                      { length: count },
                      (_, row) => 2n ** 60n + BigInt(group * count + row),
                    ),
                  },
                  {
                    type: { kind: "text" } as const,
                    values: Array.from({ length: count }, (_, row) =>
                      (row + salt) % 11 === 0
                        ? null
                        : new TextEncoder().encode(`dictionary:${String((row + salt) % 4)}`),
                    ),
                  },
                ]);
                const fixtures = [];
                for (const columns of sources)
                  fixtures.push(await writeGroup(columns, { space, inlineBytes: 0 }));
                expect(fixtures).toHaveLength(2);
                for (const fixture of fixtures) {
                  const dictionary = await openChunk(space, defined(fixture.group.columns[3]));
                  try {
                    expect(dictionary.directory.dictionary).not.toBeNull();
                  } finally {
                    dictionary.close();
                  }
                }
                reach.reach("multiple source groups");
                expect(sources.every((columns) => columns[1]?.values.some((v) => v === null))).toBe(
                  true,
                );
                reach.reach("sparse nulls");
                for (const fixture of fixtures) {
                  const reader = await openChunk(space, defined(fixture.group.columns[1]));
                  try {
                    expect(
                      reader.directory.pages.some((p) => (p.largeRegion?.length ?? 0) > 65536),
                    ).toBe(true);
                  } finally {
                    reader.close();
                  }
                }
                reach.reach("out of line >64KiB");
                expect(VERIFY_CACHED_BYTES).toBe(false);
                const pageBuffers = new Set<ArrayBufferLike>();
                const cache = new PageCache({
                  readDataInto: (offset: number, out: Uint8Array) => {
                    out.set(space.bytes.subarray(offset, offset + out.byteLength));
                    return Promise.resolve();
                  },
                } as unknown as StoreSession);
                cache.attach(governor.registerCache(cache));
                const pages: ExtentReader = {
                  read: async (refs, options) => {
                    const lease = await cache.read(refs, options);
                    for (const bytes of lease.bytes) pageBuffers.add(bytes.buffer);
                    return lease;
                  },
                  peek: (ref, options) => {
                    const bytes = cache.peek(ref, options);
                    if (bytes) pageBuffers.add(bytes.buffer);
                    return bytes;
                  },
                  parsed: cache.parsed.bind(cache),
                  attach: cache.attachParsed.bind(cache),
                };
                const ports = new MessageChannel(),
                  dictionaries = new StreamDictionaries();
                let adopted = 0;
                const send = ports.port1.postMessage.bind(ports.port1);
                ports.port1.postMessage = (
                  message: unknown,
                  options?: Transferable[] | StructuredSerializeOptions,
                ) => {
                  const buffers = Array.isArray(options) ? options : (options?.transfer ?? []);
                  expect(buffers.some((buffer) => pageBuffers.has(buffer as ArrayBuffer))).toBe(
                    false,
                  );
                  adopted += buffers.filter((buffer) =>
                    dedicated.has(buffer as ArrayBuffer),
                  ).length;
                  if (Array.isArray(options)) send(message, options);
                  else send(message, options);
                };
                let received = 0,
                  frames = 0,
                  creditTasks = 0,
                  failure: unknown;
                const released: Array<Promise<void>> = [];
                ports.port2.onmessage = (
                  event: MessageEvent<{ frames: FramePayload[]; bufferIds?: number[] }>,
                ) => {
                  for (const frame of event.data.frames) {
                    try {
                      // A recycling consumer owns compact stream dictionaries independently of released frames.
                      dictionaries.apply(
                        recycle
                          ? { ...frame, dictionaries: structuredClone(frame.dictionaries) }
                          : frame,
                      );
                      for (let row = 0; row < frame.rowCount; row++) {
                        const absolute = received + row,
                          group = Math.floor(absolute / count),
                          index = absolute % count;
                        for (let c = 0; c < frame.columns.length; c++) {
                          const wire = defined(frame.columns[c]);
                          let source = wire.form === "constant" ? wire.value : wire,
                            at = wire.form === "constant" ? 0 : row;
                          if (source.form === "dictionary") {
                            if (!isValid(source.validity, row)) {
                              expect(defined(sources[group]?.[c]).values[index]).toBeNull();
                              continue;
                            }
                            const entry = dictionaries.locate(
                              source.dictionary,
                              source.codes[row] ?? 0,
                            );
                            source = entry.entries;
                            at = entry.index;
                          }
                          const actual: TestValue = isValid(source.validity, at)
                            ? valueAt(
                                {
                                  ...source,
                                  type: defined(sources[group]?.[c]).type,
                                } as FlatVector,
                                at,
                              )
                            : null;
                          const expected = defined(sources[group]?.[c]).values[index] ?? null;
                          if (!sameValue(actual, expected)) expect(actual).toEqual(expected);
                        }
                      }
                    } catch (error) {
                      failure = error;
                    }
                    received += frame.rowCount;
                    frames++;
                    const handle = event.data.bufferIds
                      ? recyclingFrame(frame, event.data.bufferIds, (buffers, transfer) =>
                          ports.port2.postMessage(
                            { t: "return", target: 1, through: frame.sequence, buffers },
                            transfer,
                          ),
                        )
                      : null;
                    const beforeRelease = frame.columns[0];
                    const retained =
                      beforeRelease?.form === "flat" && beforeRelease.data.layout === "i32"
                        ? beforeRelease.data.values
                        : null;
                    const firstValue = retained?.[0];
                    const relinquish = () => {
                      if (retained) expect(retained[0]).toBe(firstValue);
                      handle?.release();
                      if (handle) reach.reach(EXPORT_RETURN_REACH[0]);
                      if (handle && retained) expect(retained.length).toBe(0);
                    };
                    if (returnBeforeCredit) relinquish();
                    // Credit is independent: callers can retain views after credit but before release.
                    released.push(
                      new Promise<void>((resolve) =>
                        setTimeout(() => {
                          creditTasks++;
                          ports.port2.postMessage({
                            t: "release",
                            target: 1,
                            through: frame.sequence,
                          });
                          if (!returnBeforeCredit) {
                            if (handle) reach.reach(EXPORT_RETURN_REACH[1]);
                            try {
                              relinquish();
                            } catch (error) {
                              failure = error;
                            }
                          }
                          resolve();
                        }, 0),
                      ),
                    );
                  }
                };
                ports.port2.start();
                try {
                  const before = create.mock.calls.length,
                    openBefore = opened.mock.calls.length,
                    sharedBefore = together.mock.calls.length,
                    batchBefore = batchTarget.mock.calls.length;
                  const run = prepareExecutorExport(
                    pages,
                    fixtures.map((f) => ({ rows: count, chunks: f.group.columns })),
                    {
                      governor,
                      ...(returnPool ? { returnPool } : {}),
                      port: ports.port1,
                    },
                  );
                  expect(create.mock.calls.length).toBe(before + 1);
                  reach.reach("production executor preparation");
                  const result = await run();
                  await Promise.all(released);
                  if (failure) throw failure;
                  expect(batchTarget.mock.calls.length - batchBefore).toBe(
                    shape === "full" && recycle ? 1 : 0,
                  );
                  expect(result.rows).toBe(2 * count);
                  expect(result.executorStats.outputRecycling).toBe(recycle);
                  expect(
                    opened.mock.calls
                      .slice(openBefore)
                      .every(
                        (c) =>
                          c[2].once ===
                          (shape === "full" || shape === "identity" ? true : undefined),
                      ),
                  ).toBe(true);
                  if (shape === "full" || shape === "identity")
                    expect(together.mock.calls.length - sharedBefore).toBeGreaterThan(0);
                  else expect(together.mock.calls.length - sharedBefore).toBe(0);
                  reach.reach(
                    shape === "full" || shape === "identity"
                      ? EXECUTOR_POLICY_REACH.full
                      : EXECUTOR_POLICY_REACH.cached,
                  );
                  expect(received).toBe(result.rows);
                  reach.reach("exact transferred values");
                  expect(creditTasks).toBeGreaterThan(0);
                  reach.reach("task-delayed cumulative credit");
                  expect(result.executorStats.frames).toBe(frames);
                  expect(frames).toBeGreaterThan(1);
                  expect(result.executorStats.transferredBuffers).toBeGreaterThan(0);
                  expect(
                    result.executorStats.profile.operators.some(
                      (op) => op.op === "scan" && op.rowsOut === result.rows,
                    ),
                  ).toBe(true);
                  expect(pageBuffers.size).toBeGreaterThan(0);
                  reach.reach(RESULT_OWNED_REACH.cache);
                  if (shape === "full") {
                    expect(adopted).toBeGreaterThan(0);
                    reach.reach(RESULT_OWNED_REACH.adopt);
                  }
                  cache.close();
                  returnPool?.close();
                  expect(governor.usedBytes).toBe(0);
                  expect(space.leased).toBe(0);
                  reach.reach("execution charges returned");
                  for (let group = 0; group < fixtures.length; group++) {
                    const fixture = defined(fixtures[group]);
                    for (let c = 0; c < fixture.group.columns.length; c++) {
                      const actual = await readChunk(space, defined(fixture.group.columns[c]));
                      const expected = defined(sources[group]?.[c]).values;
                      expect(actual.length).toBe(expected.length);
                      for (let row = 0; row < count; row++)
                        if (!sameValue(actual[row] ?? null, expected[row] ?? null))
                          expect(actual[row]).toEqual(expected[row]);
                    }
                  }
                  reach.reach("source survives transfer");
                } finally {
                  returnPool?.close();
                  cache.close();
                  ports.port2.close();
                }
              },
            ),
            { seed, numRuns: 16 },
          );
          reach.assertReached();
        } finally {
          create.mockRestore();
          decodeOwned.mockRestore();
          batchTarget.mockRestore();
          opened.mockRestore();
          together.mockRestore();
        }
      },
    );
  },
);

for (const seed of seedsFor("next-executor-return-cancel", [0xb2c4, 0xb2c5]))
  it(`cancellation never waits for a retained export frame (${String(seed)})`, async () => {
    const reach = createReach("export return cancellation", EXPORT_RETURN_REACH.slice(2));
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 65, max: 96 }),
        fc.integer({ min: 1, max: 31 }),
        async (count, salt) => {
          reach.beginHistory();
          const fixture = await writeGroup([
            {
              type: { kind: "text" },
              values: Array.from({ length: count }, (_, i) =>
                new Uint8Array(65536 + salt + i).fill(65 + (i % 20)),
              ),
            },
          ]);
          const governor = new Governor(64 * 1048576),
            pool = new ReturnPool(governor, 4 * 1048576),
            ports = new MessageChannel();
          let retained: ReturnType<typeof recyclingFrame> | undefined;
          ports.port2.onmessage = (
            event: MessageEvent<{ frames: FramePayload[]; bufferIds: number[] }>,
          ) => {
            const payload = event.data.frames[0];
            if (!payload || retained) return;
            retained = recyclingFrame(payload, event.data.bufferIds, (buffers, transfer) =>
              ports.port2.postMessage(
                { t: "return", target: 1, through: payload.sequence, buffers },
                transfer,
              ),
            );
            ports.port2.postMessage({ t: "release", target: 1, through: Number.MAX_SAFE_INTEGER });
          };
          ports.port2.start();
          try {
            const run = prepareExecutorExport(
              fixture.space,
              [{ rows: count, chunks: fixture.group.columns }],
              { governor, returnPool: pool, port: ports.port1 },
            );
            await expect(run()).rejects.toThrow();
            expect(retained).toBeDefined();
            reach.reach(EXPORT_RETURN_REACH[2]);
            // Cancellation closes execution charges even though the client still owns its frame.
            pool.close();
            expect(governor.usedBytes).toBe(0);
            retained?.release(); // late/lost return is never awaited by cancellation
            reach.reach(EXPORT_RETURN_REACH[3]);
            expect(governor.usedBytes).toBe(0);
          } finally {
            pool.close();
            ports.port1.close();
            ports.port2.close();
          }
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });
