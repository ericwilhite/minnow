/** R8: only tight, explicitly detachable buffers in the result grant may enter a frame.
 * Client values equal source values, and every cache/pool stand-in survives real detachment.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, RESULT_OWNED_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { StreamDictionaries } from "../owner/frame.js";
import type { Batch, DictionaryVector } from "../vector/vector.js";
import { isValid } from "../vector/validity.js";
import { F64, I32, TEXT, numbers, texts, valueAt } from "../exec/kernels/test-support.js";
import { ResultChannel } from "../exec/result.js";
import { ResultSink } from "../exec/result-sink.js";

for (const shape of [
  "dedicated",
  "mixed",
  "foreign",
  "borrowed",
  "selection",
  "row-cap",
  "cancel",
  "dictionary-cap",
] as const)
  for (const seed of seedsFor("next-result-owned", [0xb2f3, 0xb2f4]))
    it(`result owned buffers ${shape} seed ${String(seed)}`, async () => {
      const adopting =
        shape === "dedicated" ||
        shape === "mixed" ||
        shape === "cancel" ||
        shape === "dictionary-cap";
      const reach = createReach(`result owned ${shape}`, [
        RESULT_OWNED_REACH.equality,
        RESULT_OWNED_REACH.cache,
        ...(adopting ? [RESULT_OWNED_REACH.adopt] : [RESULT_OWNED_REACH.copy]),
        ...(shape === "dictionary-cap" ? [RESULT_OWNED_REACH.capped] : []),
      ]);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 33, max: 150 }),
          fc.integer({ min: 1, max: 31 }),
          async (n, salt) => {
            reach.beginHistory();
            const governor = new Governor(16 * 1024 * 1024);
            const grant = await governor.admit({
              label: "result",
              minimumBytes: 65536,
              spillable: true,
            });
            const other =
              shape === "foreign"
                ? await governor.admit({ label: "source", minimumBytes: 65536, spillable: true })
                : null;
            const sourceGrant = other ?? grant;
            const channel = new ResultChannel(
              [I32, F64, TEXT].map((type, c) => ({ type, name: String(c), nullable: true })),
              { windowBytes: 65536 },
            );
            const sink = new ResultSink([I32, F64, TEXT], grant, channel, 0, {
              now: () => 0,
              ...(shape === "row-cap" ? { maxRows: 13 } : {}),
            });
            const integer = numbers(
              I32,
              Array.from({ length: n }, (_, r) => ((r + salt) % 7 === 0 ? null : r + salt)),
            );
            const floating = numbers(
              F64,
              Array.from({ length: n }, (_, r) => (r % 11 === 0 ? -0 : r + 0.125)),
            );
            const dictionary = texts([
              "",
              "世界",
              `value:${String(salt)}`,
              ...(shape === "dictionary-cap" ? ["x".repeat(1048576)] : []),
            ]);
            const codes = Int32Array.from({ length: n }, (_, r) => r % 3);
            const validity = Uint32Array.from({ length: Math.ceil(n / 32) }, () => 0xffffffff);
            for (let r = 0; r < n; r++)
              if ((r + salt) % 5 === 0)
                validity[r >>> 5] = (validity[r >>> 5] ?? 0) & ~(1 << (r & 31));
            const text: DictionaryVector = {
              form: "dictionary",
              type: TEXT,
              length: n,
              codes,
              validity,
              dictionary,
              dictionaryId: 1,
            };
            const columns = [integer, floating, text];
            const indices =
              shape === "selection"
                ? Array.from({ length: Math.floor(n / 2) }, (_, r) => r * 2)
                : Array.from({ length: n }, (_, r) => r);
            const expected = indices.map((r) => columns.map((v) => valueAt(v, r)));
            const batch: Batch = {
              length: n,
              columns,
              selection:
                shape === "selection"
                  ? { rows: Uint16Array.from(indices), count: indices.length }
                  : null,
            };
            const dataBuffers = new Set<ArrayBuffer>();
            for (const v of [integer, floating]) {
              if (v.data.layout !== "i32" && v.data.layout !== "f64") throw new Error("fixture");
              dataBuffers.add(v.data.values.buffer as ArrayBuffer);
              if (v.validity) dataBuffers.add(v.validity.buffer as ArrayBuffer);
            }
            const codeBuffers = [codes.buffer, validity.buffer] as ArrayBuffer[];
            for (const b of codeBuffers) dataBuffers.add(b);
            const charge = [...dataBuffers].reduce((sum, b) => sum + b.byteLength, 0);
            expect(sourceGrant.allocate(charge)).toBe(true);
            const allowed = new Set(dataBuffers);
            if (shape === "mixed") for (const b of codeBuffers) allowed.delete(b);
            if (dictionary.data.layout !== "varlen") throw new Error("fixture");
            const cached = new Set<ArrayBuffer>([
              dictionary.data.bytes.buffer as ArrayBuffer,
              dictionary.data.offsets.buffer as ArrayBuffer,
            ]);
            for (const b of dataBuffers) if (!adopting || !allowed.has(b)) cached.add(b);
            const cacheSizes = new Map([...cached].map((b) => [b, b.byteLength]));
            let releases = 0,
              adopted = 0,
              seen = 0;
            const transfer = {
              ...(shape === "borrowed"
                ? {}
                : { detachableBuffers: allowed, detachableBudget: sourceGrant }),
              release: () => {
                if (releases++ === 0) sourceGrant.free(charge);
              },
            };
            const producer = (async () => {
              if (sink.push(batch, transfer) === "blocked") await sink.ready();
              await sink.finish();
            })();
            producer.catch(() => undefined);
            const dictionaries = new StreamDictionaries(),
              actual: unknown[][] = [];
            const reason = new Error("owned frame cancelled");
            try {
              for (;;) {
                const frame = await channel.next();
                if (frame === null) break;
                expect(frame.transfer.some((b) => cached.has(b))).toBe(false);
                adopted += frame.transfer.filter((b) => allowed.has(b)).length;
                if (adopting) {
                  expect(releases).toBe(1);
                  expect(grant.allocated).toBeGreaterThanOrEqual(
                    frame.transfer.reduce((sum, b) => sum + b.byteLength, 0),
                  );
                }
                const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
                dictionaries.apply(received);
                if (shape === "dictionary-cap") {
                  expect(received.dictionaries).toHaveLength(0);
                  expect(received.columns[2]?.form).toBe("flat");
                  reach.reach(RESULT_OWNED_REACH.capped);
                }
                if (shape === "row-cap") expect(received.rowCount).toBeLessThanOrEqual(13);
                for (let row = 0; row < received.rowCount; row++)
                  actual.push(
                    received.columns.map((v, c) => {
                      const type = [I32, F64, TEXT][c];
                      if (!type) throw new Error("type");
                      if (v.form === "flat") return valueAt({ ...v, type }, row);
                      if (v.form !== "dictionary") throw new Error("form");
                      if (!isValid(v.validity, row)) return null;
                      const d = dictionaries.locate(v.dictionary, v.codes[row] ?? 0);
                      return valueAt({ ...d.entries, type }, d.index);
                    }),
                  );
                seen += received.rowCount;
                if (shape === "cancel") {
                  sink.discard(reason);
                  channel.cancel(reason);
                  frame.release();
                  break;
                }
                frame.release();
              }
              await producer;
              if (shape !== "cancel") await channel.delivered;
              expect(actual).toEqual(expected.slice(0, seen));
              if (shape !== "cancel") expect(seen).toBe(indices.length);
              expect(releases).toBe(1);
              if (adopting) {
                expect(adopted).toBeGreaterThan(0);
                reach.reach(RESULT_OWNED_REACH.adopt);
              } else {
                expect(adopted).toBe(0);
                reach.reach(RESULT_OWNED_REACH.copy);
              }
              for (const [buffer, size] of cacheSizes) expect(buffer.byteLength).toBe(size);
              expect(grant.allocated).toBe(0);
              expect(sourceGrant.allocated).toBe(0);
              reach.reach(RESULT_OWNED_REACH.cache);
              reach.reach(RESULT_OWNED_REACH.equality);
            } finally {
              sink.discard(reason);
              channel.cancel(reason);
              await producer.catch(() => undefined);
              other?.close();
              grant.close();
            }
            expect(governor.usedBytes).toBe(0);
          },
        ),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
    });
