import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RESULT_DICTIONARY_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StreamDictionaries } from "../owner/frame.js";
import type { Batch, DictionaryVector } from "../vector/vector.js";
import { isValid } from "../vector/validity.js";
import { TEXT, texts, valueAt } from "../exec/kernels/test-support.js";
import { ResultChannel, STREAM_DICTIONARY_BYTES } from "../exec/result.js";
import { ResultSink } from "../exec/result-sink.js";

for (const shape of [
  "reuse",
  "replace",
  "selection",
  "cap",
  "large",
  "accumulate",
  "empty",
  "cancel",
  "pressure",
] as const)
  for (const seed of seedsFor("next-result-dictionaries", [0xb2f1, 0xb2f2]))
    it(`dictionary result/client equality ${shape} seed ${String(seed)}`, async () => {
      const reach = createReach(`result dictionary ${shape}`, [
        RESULT_DICTIONARY_REACH.equality,
        shape === "cap" ? RESULT_DICTIONARY_REACH.fallback : RESULT_DICTIONARY_REACH.codes,
        ...(shape === "pressure" ? [RESULT_DICTIONARY_REACH.loan] : []),
      ]);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 33, max: 180 }),
          fc.integer({ min: 1, max: 29 }),
          async (n, salt) => {
            reach.beginHistory();
            const governor = new Governor((shape === "pressure" ? 4 : 16) * 1024 * 1024);
            const hog =
              shape === "pressure"
                ? await governor.admit({
                    label: "hog",
                    minimumBytes: 3 * 1024 * 1024 - 1024,
                    spillable: false,
                  })
                : null;
            const grant = await governor.admit({
              label: "result",
              minimumBytes: 1024,
              spillable: true,
            });
            let loans = 0;
            const borrow = grant.borrow.bind(grant);
            grant.borrow = (bytes, signal) => {
              loans++;
              return borrow(bytes, signal);
            };
            const channel = new ResultChannel([{ name: "text", type: TEXT, nullable: true }], {
              windowBytes: shape === "cancel" ? 64 : 65536,
            });
            const sink = new ResultSink([TEXT], grant, channel, 0, { now: () => 0 });
            const entries = ["", "世界", `salt:${String(salt)}`, "repeated"];
            if (shape === "large" || shape === "accumulate" || shape === "pressure")
              entries.push("y".repeat(900000));
            if (shape === "cap") entries.push("x".repeat(STREAM_DICTIONARY_BYTES));
            const shared = texts(entries);
            const dictionaries = new StreamDictionaries();
            const expected: unknown[] = [],
              actual: unknown[] = [];
            let dictionaryBytes = 0;
            let released = 0,
              frames = 0,
              deltas = 0,
              dictColumns = 0;
            const batches: Batch[] = Array.from({ length: 2 }, (_, group) => {
              const dictionary =
                shape === "replace" || shape === "accumulate"
                  ? texts(entries.map((s) => `${s}:${String(group)}`))
                  : shared;
              const validity = new Uint32Array(Math.ceil(n / 32));
              const codes = Int32Array.from({ length: n }, (_, row) => {
                if ((row + salt) % 5 !== 0)
                  validity[row >>> 5] = (validity[row >>> 5] ?? 0) | (1 << (row & 31));
                return (row + salt) % 4;
              });
              const vector: DictionaryVector = {
                form: "dictionary",
                type: TEXT,
                length: n,
                validity,
                codes,
                dictionary,
                dictionaryId: group + 1,
              };
              const rows = Uint16Array.from({ length: Math.floor(n / 2) }, (_, row) => row * 2);
              const selection =
                shape === "empty" && group === 1
                  ? { rows: new Uint16Array(0), count: 0 }
                  : shape === "selection"
                    ? { rows, count: rows.length }
                    : null;
              for (const row of selection?.rows ?? Array.from({ length: n }, (_, i) => i))
                expected.push(isValid(validity, row) ? valueAt(dictionary, codes[row] ?? 0) : null);
              return { length: n, columns: [vector], selection };
            });
            const sourceBuffers = new Set(
              batches.flatMap((b) => {
                const v = b.columns[0] as DictionaryVector;
                if (v.dictionary.data.layout !== "varlen") throw new Error("fixture");
                return [
                  v.codes.buffer,
                  v.dictionary.data.bytes.buffer,
                  v.dictionary.data.offsets.buffer,
                  v.validity?.buffer,
                ];
              }),
            );
            const producer = (async () => {
              for (const batch of batches) {
                if (
                  sink.push(batch, {
                    release: () => {
                      released++;
                      (batch.columns[0] as DictionaryVector).codes.fill(-999);
                    },
                  }) === "blocked"
                )
                  await sink.ready();
              }
              await sink.finish();
            })();
            producer.catch(() => undefined);
            const reason = new Error("cancelled dictionary history");
            try {
              for (;;) {
                const frame = await channel.next();
                if (frame === null) break;
                frames++;
                expect(frame.transfer.some((b) => sourceBuffers.has(b))).toBe(false);
                const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
                dictionaries.apply(received);
                deltas += received.dictionaries.length;
                for (const delta of received.dictionaries) {
                  if (delta.values.data.layout !== "varlen") throw new Error("non-text delta");
                  dictionaryBytes +=
                    delta.values.data.bytes.byteLength + delta.values.data.offsets.byteLength;
                }
                expect(dictionaryBytes).toBeLessThanOrEqual(STREAM_DICTIONARY_BYTES);
                for (const column of received.columns) {
                  if (column.form === "dictionary") dictColumns++;
                  for (let row = 0; row < received.rowCount; row++) {
                    if (column.form === "dictionary") {
                      if (!isValid(column.validity, row)) actual.push(null);
                      else {
                        const slot = dictionaries.locate(column.dictionary, column.codes[row] ?? 0);
                        actual.push(valueAt({ ...slot.entries, type: TEXT }, slot.index));
                      }
                    } else if (column.form === "flat")
                      actual.push(valueAt({ ...column, type: TEXT }, row));
                    else throw new Error("unexpected constant");
                  }
                }
                if (shape === "cancel") {
                  sink.discard(reason);
                  channel.cancel(reason);
                  frame.release();
                  await expect(producer).rejects.toThrow(reason.message);
                  break;
                }
                frame.release();
              }
              if (shape === "cancel") {
                expect(actual).toEqual(expected.slice(0, actual.length));
                expect(released).toBe(1);
              }
              if (shape !== "cancel") {
                await producer;
                await channel.delivered;
                expect(actual).toEqual(expected);
                expect(released).toBe(2);
                if (shape === "cap") {
                  expect(dictColumns).toBe(0);
                  expect(deltas).toBe(0);
                } else if (shape === "accumulate") {
                  expect(dictColumns).toBeGreaterThan(0);
                  expect(dictColumns).toBeLessThan(frames);
                  expect(deltas).toBe(1);
                } else {
                  expect(dictColumns).toBe(frames);
                  expect(deltas).toBe(shape === "replace" ? 2 : 1);
                }
              }
              if (shape === "pressure") {
                expect(loans).toBeGreaterThan(0);
                reach.reach(RESULT_DICTIONARY_REACH.loan);
              }
              expect(grant.allocated).toBe(0);
              reach.reach(RESULT_DICTIONARY_REACH.equality);
              reach.reach(
                shape === "cap" ? RESULT_DICTIONARY_REACH.fallback : RESULT_DICTIONARY_REACH.codes,
              );
            } finally {
              sink.discard(reason);
              channel.cancel(reason);
              await producer.catch(() => undefined);
              grant.close();
              hog?.close();
            }
            expect(governor.usedBytes).toBe(0);
          },
        ),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
    });
