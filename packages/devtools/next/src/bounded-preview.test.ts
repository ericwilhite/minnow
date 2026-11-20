/** Preview work is bounded by inspected cells, not the retained result's row count. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../../core/dist/testing/seeds.js";
import { createReach, NATIVE_PREVIEW_DECODE_REACH } from "../../../core/dist/testing/reach.js";
import { decodeResult, resultCell } from "../../../core/dist/next/owner/result.js";
import type { FramePayload, WireVector } from "../../../core/dist/next/owner/frame.js";
import { resultPresentation } from "./result.js";

afterEach(() => vi.restoreAllMocks());
for (const seed of seedsFor("next-native-preview-decode", [46980, 46981]))
  it(`decodes only requested visible cells (${String(seed)})`, () => {
    const reach = createReach("native preview decoding", NATIVE_PREVIEW_DECODE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 1001, max: 10000 }),
        fc.integer({ min: 0, max: 1000 }),
        fc.constantFrom("flat", "constant", "dictionary"),
        fc.boolean(),
        fc.string({ minLength: 1, maxLength: 20 }),
        (count, limit, form, split, text) => {
          reach.beginHistory();
          const encoder = new TextEncoder(),
            frames: FramePayload[] = [];
          const entry = (values: readonly string[]) => {
            const data = values.map((value) => encoder.encode(value)),
              offsets = new Int32Array(data.length + 1);
            let size = 0;
            for (const [i, value] of data.entries()) {
              size += value.length;
              offsets[i + 1] = size;
            }
            const bytes = new Uint8Array(size);
            for (const [i, value] of data.entries()) bytes.set(value, offsets[i]);
            return {
              form: "flat" as const,
              length: values.length,
              validity: null,
              data: { layout: "varlen" as const, bytes, offsets },
            };
          };
          const starts = split ? [0, Math.floor(count / 2), count] : [0, count];
          for (let at = 0; at < starts.length - 1; at++) {
            const from = starts[at] ?? 0,
              to = starts[at + 1] ?? 0,
              length = to - from;
            let column: WireVector;
            if (form === "flat")
              column = entry(Array.from({ length }, (_, i) => `${text}:${String(from + i)}`));
            else if (form === "constant")
              column = { form: "constant", length, value: entry([text]) };
            else
              column = {
                form: "dictionary",
                length,
                dictionary: 1,
                codes: new Int32Array(length),
                validity: null,
              };
            frames.push({
              sequence: at,
              rowCount: length,
              columns: [column],
              dictionaries:
                form === "dictionary" && at === 0
                  ? [{ id: 1, from: 0, values: entry([text]) }]
                  : [],
            });
          }
          const result = decodeResult({
            columns: [
              { name: "x", type: { kind: "text" }, sqlType: "text", nullable: false, render: null },
            ],
            frames,
            summary: {
              command: "SELECT",
              rowCount: count,
              counts: null,
              lsn: null,
              replayed: false,
              closedEarly: false,
              transaction: "idle",
              catalogVersion: 1,
            },
          });
          const decode = vi.spyOn(TextDecoder.prototype, "decode");
          try {
            const preview = resultPresentation(result, limit);
            expect(decode).not.toHaveBeenCalled();
            expect(preview.visibleRows).toBe(limit);
            expect(preview.truncated).toBe(true);
            if (limit) {
              expect(preview.value(0, 0)).toBe(form === "flat" ? `${text}:0` : text);
              expect(decode).toHaveBeenCalledTimes(1);
              reach.reach("one-cell decode");
              for (let i = 0; i < limit; i++)
                expect(preview.rows[i]?.["@0"]).toBe(
                  form === "flat" ? `${text}:${String(i)}` : text,
                );
              const calls =
                form === "flat"
                  ? limit
                  : form === "constant"
                    ? starts.slice(0, -1).filter((start) => start < limit).length
                    : 1;
              expect(decode).toHaveBeenCalledTimes(calls);
              expect(preview.value(0, 0)).toBe(resultCell(result, 0, 0));
              expect(decode).toHaveBeenCalledTimes(calls);
              reach.reach("cached entry reuse");
              reach.reach("bounded preview");
              reach.reach("visible-only decoding");
            } else reach.reach("empty preview");
            expect(() => preview.value(limit, 0)).toThrow(RangeError);
            reach.reach("off-screen refusal");
            expect(() => resultCell(result, -1, 0)).toThrow(RangeError);
            expect(() => resultCell(result, 0, 1)).toThrow(RangeError);
            reach.reach(form);
            if (split) reach.reach("multiple frames");
          } finally {
            decode.mockRestore();
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [10000, 1000, "flat", true, "é漢"],
          [1001, 0, "constant", false, "x"],
          [10000, 1000, "constant", true, "c"],
          [10000, 1000, "dictionary", true, "d"],
        ],
      },
    );
    reach.assertReached();
  });
