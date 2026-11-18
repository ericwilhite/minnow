/** Lead's Result ownership boundary: delivery transfers memory to the caller; lazy
 * projections share decoded entries rather than multiplying their payloads. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_RESULT_RETENTION_REACH } from "../../testing/reach.js";
import { decodeResult } from "../owner/result.js";
import * as resultColumns from "../owner/result-columns.js";
import type { WireFlat, FramePayload } from "../owner/frame.js";
import type { WireColumnMeta } from "../owner/protocol.js";
import { encodeArrayValue } from "../vector/array-value.js";
import type { PhysicalType } from "../types.js";
const SUMMARY = {
  command: "SELECT",
  rowCount: null,
  counts: null,
  lsn: null,
  replayed: false,
  closedEarly: false,
  transaction: "idle",
  catalogVersion: 1,
} as const;

for (const seed of seedsFor("next-public-result-retention", [0xb75e, 0xb75f]))
  it(`shares decoded payloads across access order and encodings (${String(seed)})`, () => {
    const reach = createReach("public Result retention", PUBLIC_RESULT_RETENTION_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 2, max: 67 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        (length, dictionary, array, rowsFirst) => {
          reach.beginHistory();
          const element: PhysicalType = { kind: "int32", bits: 32 };
          const type: PhysicalType = array ? { kind: "array", element } : { kind: "bytes" };
          const bytes = array
            ? encodeArrayValue(element, { dimensions: [2], lowerBounds: [1], elements: [17, 23] })
            : Uint8Array.of(17, 23);
          const entry: WireFlat = {
            form: "flat",
            length: 1,
            validity: null,
            data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
          };
          const meta: WireColumnMeta = {
            name: "v",
            type,
            nullable: false,
            sqlType: array ? "integer[]" : "bytea",
            render: null,
          };
          const frames: FramePayload[] = [
            {
              sequence: 0,
              rowCount: length,
              dictionaries: dictionary ? [{ id: 1, from: 0, values: entry }] : [],
              columns: [
                dictionary
                  ? {
                      form: "dictionary",
                      dictionary: 1,
                      length,
                      validity: null,
                      codes: new Int32Array(length),
                    }
                  : { form: "constant", length, value: entry },
              ],
            },
          ];
          const result = decodeResult({ columns: [meta], frames, summary: SUMMARY });
          const materialized = vi.spyOn(resultColumns, "materialize");
          try {
            const row = rowsFirst ? result.row(0) : undefined;
            const tuples = rowsFirst ? undefined : result.tuples();
            const values = result.values(0),
              rows = result.rows();
            expect(rowsFirst ? row?.v : tuples?.[0]?.[0]).toBe(values[0]);
            expect(result.row(0)).toBe(rows[0]);
            expect(result.rows()).toBe(rows);
            for (let i = 0; i < length; i++) {
              expect(values[i]).toBe(values[0]);
              expect(rows[i]?.v).toBe(values[0]);
              expect(result.tuples()[i]?.[0]).toBe(values[0]);
            }
            expect(materialized).not.toHaveBeenCalled();
            expect(result.column(0)).toBe(result.column(0));
            expect(materialized).toHaveBeenCalledTimes(1);
            reach.reach(dictionary ? "dictionary entry shared" : "constant entry shared");
            reach.reach(array ? "nested array shared" : "bytea shared");
            reach.reach(rowsFirst ? "row before values" : "tuples before values");
            reach.reach("views only on explicit request");
          } finally {
            materialized.mockRestore();
          }
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [2, false, false, true],
          [67, true, true, false],
          [2, true, true, true],
          [2, false, true, false],
        ],
      },
    );
    reach.assertReached();
  });
