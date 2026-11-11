/** Frame contract: one owned backing allocation per proven binding, shared only within a frame. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, RESULT_ALIAS_FRAME_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { Batch, Vector } from "../vector/vector.js";
import type { PhysicalType } from "../types.js";
import {
  I32,
  F64,
  TEXT,
  decimal,
  decimals,
  numbers,
  texts,
  valueAt,
} from "./kernels/test-support.js";
import { vectorOf } from "./join-fixtures.js";
import { ResultChannel, frameByteLength, type WireVector } from "./result.js";
import { ResultSink } from "./result-sink.js";
import { validAt } from "./kernels/vectors.js";

const reach = createReach("aliased result frames", RESULT_ALIAS_FRAME_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-result-alias-frames", [0xb4f1, 0xb4f2]))
  it(`owns one exact physical copy through transfer, selection and cancellation (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          kind: fc.constantFrom("text", "dictionary", "numeric", "float"),
          rows: fc.integer({ min: 3, max: 8 }),
          copies: fc.integer({ min: 2, max: 6 }),
          selected: fc.boolean(),
        }),
        async ({ kind, rows, copies, selected }) => {
          reach.beginHistory();
          for (const cancel of [false, true]) {
            const first: Vector =
              kind === "numeric"
                ? decimals(
                    decimal(null, null),
                    Array.from({ length: rows }, (_, i) =>
                      i % 3 === 0 ? null : ([10n ** 80n + BigInt(i), i % 2] as [bigint, number]),
                    ),
                    "wide",
                  )
                : kind === "float"
                  ? numbers(
                      F64,
                      Array.from({ length: rows }, (_, i) =>
                        i === 0 ? -0 : i === 1 ? NaN : i === 2 ? -Infinity : i,
                      ),
                    )
                  : kind === "dictionary"
                    ? vectorOf(
                        TEXT,
                        Array.from({ length: rows }, (_, i) =>
                          i % 3 === 0 ? null : `汉${String(i % 2)}`,
                        ),
                        { dictionary: true },
                      )
                    : texts(
                        Array.from({ length: rows }, (_, i) =>
                          i % 3 === 0 ? null : `汉${String(i)}`,
                        ),
                      );
            const physical = [
              first,
              numbers(
                I32,
                Array.from({ length: rows }, (_, i) => 100 + i),
              ),
            ];
            const restore = Array.from({ length: 2 * copies }, (_, i) => i % 2);
            const types = restore.map((i) => physical[i]?.type ?? I32);
            const batch: Batch = {
              length: rows,
              columns: restore.map((i) => {
                const v = physical[i];
                if (!v) throw Error("fixture column");
                return v;
              }),
              selection: selected
                ? {
                    rows: Uint16Array.from(Array.from({ length: rows }, (_, i) => rows - 1 - i)),
                    count: rows,
                  }
                : null,
            };
            const sourceBuffers = new Set<ArrayBuffer>();
            const inspect = (value: unknown): void => {
              if (ArrayBuffer.isView(value)) {
                if (value.buffer instanceof ArrayBuffer) sourceBuffers.add(value.buffer);
              } else if (value !== null && typeof value === "object")
                for (const child of Object.values(value)) inspect(child);
            };
            inspect(physical);
            const expected = Array.from({ length: rows }, (_, i) =>
              restore.map((c) => {
                const v = physical[c];
                if (!v) throw Error("fixture column");
                return valueAt(v, selected ? rows - 1 - i : i);
              }),
            );
            const governor = new Governor(8 << 20),
              grant = await governor.admit({
                label: "result",
                minimumBytes: 128 << 10,
                spillable: true,
              });
            const channel = new ResultChannel(
              types.map((type, i) => ({ name: String(i), type, nullable: true })),
              { windowBytes: 1 },
            );
            const sink = new ResultSink(types, grant, channel, 17, { now: () => 0 });
            sink.copyLayout({ sources: [0, 1], restore, types: physical.map((v) => v.type) });
            let released = 0;
            const producer = (async () => {
              if (
                sink.push(batch, {
                  release: () => {
                    released++;
                  },
                }) === "blocked"
              )
                await sink.ready();
              await sink.finish();
            })();
            void producer.catch(() => undefined);
            const actual: unknown[][] = [],
              buffers = new Set<ArrayBuffer>(),
              dictionaries = new Map<number, unknown[]>();
            let releaseFrame: () => void = () => undefined;
            const reason = new Error("cancel handed-out alias frame");
            try {
              for (;;) {
                const frame = await channel.next();
                if (frame === null) break;
                releaseFrame = () => frame.release();
                const columns = frame.payload.columns;
                const a = columns[0],
                  b = columns[1];
                if (!a || !b) throw Error("missing frame columns");
                expect(frame.byteLength).toBe(
                  frameByteLength({ ...frame.payload, columns: [a, b] }),
                );
                expect(frame.byteLength).toBeLessThanOrEqual(grant.allocated);
                expect(new Set(frame.transfer).size).toBe(frame.transfer.length);
                for (const buffer of frame.transfer) {
                  expect(buffers.has(buffer)).toBe(false);
                  expect(sourceBuffers.has(buffer)).toBe(false);
                  buffers.add(buffer);
                }
                reach.reach("one backing allocation");
                const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
                for (const delta of received.dictionaries) {
                  const entries = dictionaries.get(delta.id) ?? [];
                  expect(delta.from).toBe(entries.length);
                  for (let i = 0; i < delta.values.length; i++)
                    entries.push(valueAt({ ...delta.values, type: TEXT }, i));
                  dictionaries.set(delta.id, entries);
                  reach.reach("dictionary delta");
                }
                const cell = (column: WireVector, type: PhysicalType, row: number): unknown => {
                  if (column.form === "dictionary")
                    return validAt(column.validity, row)
                      ? dictionaries.get(column.dictionary)?.[column.codes[row] ?? 0]
                      : null;
                  if (column.form === "constant") return valueAt({ ...column.value, type }, 0);
                  if (column.data.layout === "decimal" && column.data.coefficients.repr === "wide")
                    reach.reach("wide numeric clone");
                  return valueAt({ ...column, type }, row);
                };
                for (let row = 0; row < received.rowCount; row++)
                  actual.push(
                    received.columns.map((column, c) => cell(column, types[c] ?? I32, row)),
                  );
                if (cancel) {
                  sink.discard(reason);
                  expect(grant.allocated).toBe(0);
                  channel.cancel(reason);
                  frame.release();
                  reach.reach("cancel blocked");
                  break;
                }
                frame.release();
              }
              if (cancel) await expect(producer).rejects.toBe(reason);
              else {
                await producer;
                await channel.delivered;
                expect(actual).toEqual(expected);
                reach.reach("exact values");
              }
              expect(released).toBe(1);
              if (selected) reach.reach("selected order");
              if (kind === "float") reach.reach("float bits");
            } finally {
              sink.discard(reason);
              channel.cancel(reason);
              releaseFrame();
              await producer.catch(() => undefined);
              await channel.delivered.catch(() => undefined);
              expect(grant.allocated).toBe(0);
              grant.close();
              expect(governor.usedBytes).toBe(0);
              reach.reach("all charges returned");
            }
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [{ kind: "text", rows: 4, copies: 3, selected: true }],
          [{ kind: "dictionary", rows: 4, copies: 3, selected: false }],
          [{ kind: "numeric", rows: 4, copies: 4, selected: true }],
          [{ kind: "float", rows: 4, copies: 2, selected: false }],
        ],
      },
    );
  }, 60000);
