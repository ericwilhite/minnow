import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, DISCARD_RECEIVER_REACH } from "../../../packages/core/src/testing/reach.js";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import {
  frameTransferList,
  type FramePayload,
  type WireFlat,
} from "../../../packages/core/src/next/exec/result.js";
import { StreamDictionaries, payloadBytes } from "../../../packages/core/src/next/owner/frame.js";
import { DiscardReceiver } from "./discard-receiver.js";

const reach = createReach("discard receiver", DISCARD_RECEIVER_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("discard-receiver", [0x2520, 0x2521]))
  it(`disposes consumed transferred columns, retaining stream dictionary bytes (seed ${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.array(fc.integer(), { minLength: 1, maxLength: 128 }),
        fc.boolean(),
        (values, sparse) => {
          reach.beginHistory();
          const receiver = new DiscardReceiver(),
            dictionaries = new StreamDictionaries();
          const entry: WireFlat = {
            form: "flat",
            length: 2,
            validity: null,
            data: {
              layout: "varlen",
              bytes: new TextEncoder().encode("alphabeta"),
              offsets: new Int32Array([0, 5, 9]),
            },
          };
          const receivedDeltas: ArrayBuffer[] = [];
          let bytes = 0;
          for (let sequence = 0; sequence < 3; sequence++) {
            const n = sequence === 2 ? 0 : values.length;
            const validity = sparse ? new Uint32Array(Math.ceil(n / 32)).fill(0x55555555) : null;
            const frame: FramePayload = {
              sequence,
              rowCount: n,
              columns: [
                {
                  form: "flat",
                  length: n,
                  validity,
                  data: { layout: "i32", values: Int32Array.from(values.slice(0, n)) },
                },
                {
                  form: "dictionary",
                  length: n,
                  validity: validity?.slice() ?? null,
                  dictionary: 7,
                  codes: Int32Array.from({ length: n }, (_, i) => i % 2),
                },
                {
                  form: "constant",
                  length: n,
                  value: {
                    form: "flat",
                    length: 1,
                    validity: null,
                    data: { layout: "f64", values: new Float64Array([-0]) },
                  },
                },
              ],
              dictionaries: sequence === 0 ? [{ id: 7, from: 0, values: entry }] : [],
            };
            const sent = frameTransferList(frame),
              received = structuredClone(frame, { transfer: sent });
            expect(sent.every((b) => b.byteLength === 0)).toBe(true);
            dictionaries.apply(received);
            const columns = frameTransferList({ ...received, dictionaries: [] });
            if (sequence === 0)
              receivedDeltas.push(...frameTransferList({ ...received, columns: [] }));
            const ints = received.columns[0],
              codes = received.columns[1];
            if (ints?.form !== "flat" || ints.data.layout !== "i32" || codes?.form !== "dictionary")
              throw new Error("fixture");
            expect([...ints.data.values]).toEqual(values.slice(0, n));
            for (let i = 0; i < n; i++) {
              const { entries, index } = dictionaries.locate(7, codes.codes[i] ?? 0);
              if (entries.data.layout !== "varlen") throw new Error("fixture dictionary");
              expect(
                new TextDecoder().decode(
                  entries.data.bytes.subarray(
                    entries.data.offsets[index],
                    entries.data.offsets[index + 1],
                  ),
                ),
              ).toBe(i % 2 === 0 ? "alpha" : "beta");
            }
            bytes += payloadBytes(received);
            receiver.consume(received);
            expect(columns.every((b) => b.byteLength === 0)).toBe(true);
            expect(receivedDeltas.every((b) => b.byteLength > 0)).toBe(true);
            reach.reach("disposed columns");
            reach.reach("live dictionary");
            reach.reach(sparse ? "sparse" : "all valid");
            if (n === 0) reach.reach("empty frame");
          }
          receiver.close();
          receiver.close();
          expect(receivedDeltas.every((b) => b.byteLength === 0)).toBe(true);
          expect(receiver.disposedBytes).toBe(bytes);
        },
      ),
      { seed, numRuns: 32 },
    );
  });
