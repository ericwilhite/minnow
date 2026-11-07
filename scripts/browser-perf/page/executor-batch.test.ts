/** The direct terminal export may use a larger owned batch, inside its unchanged credit
 * budget. Source/client values and ownership survive that policy, including sparse NULLs.
 */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, TERMINAL_BATCH_REACH } from "../../../packages/core/src/testing/reach.js";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import { Governor } from "../../../packages/core/src/next/memory/governor.js";
import { xxHash32 } from "../../../packages/core/src/next/hash/xxhash32.js";
import { MemoryExtentSpace } from "../../../packages/core/src/next/page/memory-extents.js";
import { vectorOf } from "../../../packages/core/src/next/page/test-support.js";
import { GroupWriter } from "../../../packages/core/src/next/page/writer.js";
import { isValid } from "../../../packages/core/src/next/vector/validity.js";
import {
  RESULT_FRAME_TARGET_BYTES,
  type FramePayload,
} from "../../../packages/core/src/next/exec/result.js";
import { ResultSink } from "../../../packages/core/src/next/exec/result-sink.js";
import { ResultChannel } from "../../../packages/core/src/next/exec/result.js";
import { prepareExecutorExport } from "./executor-export.js";
const reach = createReach("terminal wide batch", TERMINAL_BATCH_REACH);
afterAll(() => reach.assertReached());
for (const sparse of [false, true])
  for (const seed of seedsFor("terminal-wide-batch", [0xb2b0, 0xb2b1]))
    it(`larger terminal batches remain exact (${String(sparse)}, seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 3072, max: 4096 }),
          fc.integer({ min: 1, max: 31 }),
          async (count, salt) => {
            reach.beginHistory();
            const type = { kind: "int32", bits: 32 } as const,
              space = new MemoryExtentSpace();
            const writer = new GroupWriter({
              writer: space.beginWrite("batch proof"),
              columns: Array.from({ length: 200 }, (_, c) => ({ columnId: c + 1, type })),
            });
            const value = (c: number, row: number) =>
              sparse && (row + c + salt) % 13 === 0 ? null : (row * 31 + c * salt) % 100003;
            await writer.append(
              Array.from({ length: 200 }, (_, c) =>
                vectorOf(
                  type,
                  Array.from({ length: count }, (_, row) => value(c, row)),
                ),
              ),
              count,
            );
            const group = await writer.finish(),
              originalHash = xxHash32(space.bytes);
            const governor = new Governor(64 * 1048576),
              ports = new MessageChannel();
            let rows = 0,
              large = false;
            let error: unknown;
            ports.port2.onmessage = (event: MessageEvent<{ frames: FramePayload[] }>) => {
              try {
                for (const frame of event.data.frames) {
                  for (let row = 0; row < frame.rowCount; row++)
                    for (let c = 0; c < frame.columns.length; c++) {
                      const column = frame.columns[c];
                      if (column?.form !== "flat" || column.data.layout !== "i32")
                        throw new Error("batch proof layout");
                      const actual = isValid(column.validity, row) ? column.data.values[row] : null;
                      if (actual !== value(c, rows + row))
                        throw new Error(`wrong terminal value ${String(c)}:${String(rows + row)}`);
                    }
                  rows += frame.rowCount;
                  if (frame.rowCount > 1400) large = true;
                  ports.port2.postMessage({ t: "release", target: 1, through: frame.sequence });
                }
              } catch (e) {
                error = e;
                ports.port2.postMessage({ t: "release", target: 1, through: 1e9 });
              }
            };
            ports.port2.start();
            try {
              const result = await prepareExecutorExport(
                space,
                [{ rows: count, chunks: group.columns }],
                { governor, port: ports.port1 },
              )();
              if (error) throw error;
              expect(rows).toBe(count);
              expect(large).toBe(true);
              expect(result.executorStats.maxFrameBytes).toBeGreaterThan(RESULT_FRAME_TARGET_BYTES);
              expect(result.executorStats.maxFrameBytes).toBeLessThanOrEqual(
                result.executorStats.resultWindowBytes,
              );
              expect(result.executorStats.transferredBuffers).toBeGreaterThan(0);
              expect(governor.usedBytes).toBe(0);
              expect(xxHash32(space.bytes)).toBe(originalHash);
              reach.reach(sparse ? "sparse NULLs" : "all valid");
              reach.reach("owned frame above 1MiB");
              reach.reach("unchanged credit bound");
              reach.reach("exact received values");
            } finally {
              ports.port1.close();
              ports.port2.close();
            }
          },
        ),
        { seed, numRuns: 8 },
      );
    });

for (const seed of seedsFor("terminal-batch-credit", [0xb2b2, 0xb2b3]))
  it(`terminal target respects a small credit window (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 4096, max: 65536 }), async (windowBytes) => {
        reach.beginHistory();
        const governor = new Governor(8 * 1048576),
          grant = await governor.admit({
            label: "small window",
            minimumBytes: windowBytes,
            spillable: true,
          });
        const type = { kind: "int32", bits: 32 } as const;
        const channel = new ResultChannel([{ name: "x", type, nullable: false }], { windowBytes });
        const sink = new ResultSink([type], grant, channel, 0);
        expect(sink.terminalBatchTarget(2 * 1048576)).toBe(windowBytes);
        await sink.finish();
        await channel.delivered;
        grant.close();
        expect(governor.usedBytes).toBe(0);
        reach.reach("small credit cap");
      }),
      { seed, numRuns: 32 },
    );
  });
