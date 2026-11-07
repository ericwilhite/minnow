/** The Gate A bridge must exercise the production executor, carry exact values through actual
 * transfers and credits, and return every execution charge. Input storage must survive transfer.
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import { createReach, EXECUTOR_EXPORT_REACH } from "../../../packages/core/src/testing/reach.js";
import * as factory from "../../../packages/core/src/next/exec/create.js";
import { Governor } from "../../../packages/core/src/next/memory/governor.js";
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

it.each(seedsFor("next-executor-export", [0xb2e1, 0xb2e2]))(
  "production executor, exact MessagePort values, credits and cleanup (seed %i)",
  async (seed) => {
    const reach = createReach("next-executor-export", EXECUTOR_EXPORT_REACH);
    const create = vi.spyOn(factory, "createExecutor");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 65, max: 160 }),
          fc.integer({ min: 1, max: 31 }),
          async (count, salt) => {
            reach.beginHistory();
            const space = new MemoryExtentSpace(),
              governor = new Governor(64 * 1024 * 1024);
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
            ]);
            const fixtures = [];
            for (const columns of sources)
              fixtures.push(await writeGroup(columns, { space, inlineBytes: 0 }));
            expect(fixtures).toHaveLength(2);
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
            const ports = new MessageChannel(),
              dictionaries = new StreamDictionaries();
            let received = 0,
              frames = 0,
              creditTasks = 0,
              failure: unknown;
            ports.port2.onmessage = (event: MessageEvent<{ frames: FramePayload[] }>) => {
              for (const frame of event.data.frames) {
                try {
                  dictionaries.apply(frame);
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
                            { ...source, type: defined(sources[group]?.[c]).type } as FlatVector,
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
                // A task-delayed cumulative owner credit, never release in the sender on offer.
                setTimeout(() => {
                  creditTasks++;
                  ports.port2.postMessage({ t: "release", target: 1, through: frame.sequence });
                }, 0);
              }
            };
            ports.port2.start();
            try {
              const before = create.mock.calls.length;
              const run = prepareExecutorExport(
                space,
                fixtures.map((f) => ({ rows: count, chunks: f.group.columns })),
                {
                  governor,
                  port: ports.port1,
                },
              );
              expect(create.mock.calls.length).toBe(before + 1);
              reach.reach("production executor preparation");
              const result = await run();
              if (failure) throw failure;
              expect(result.rows).toBe(2 * count);
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
              ports.port2.close();
            }
          },
        ),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
    } finally {
      create.mockRestore();
    }
  },
);
