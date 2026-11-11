/** Release pacing counts backing allocations once, independently of logical references. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, FRAME_PACING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { payloadBytes, type FramePayload, type WireVector } from "./frame.js";

const reach = createReach("frame release pacing", FRAME_PACING_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-frame-pacing", [0xb4fb, 0xb4fc]))
  it(`counts repeated flat and dictionary backing allocations once (${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom("flat", "dictionary", "wide"),
        fc.integer({ min: 2, max: 8 }),
        fc.integer({ min: 1, max: 64 }),
        (kind, copies, rows) => {
          const dictionary = kind === "dictionary";
          reach.beginHistory();
          const values = new Int32Array(rows),
            validity = new Uint32Array(Math.ceil(rows / 32));
          const first: WireVector = dictionary
            ? { form: "dictionary", length: rows, codes: values, validity, dictionary: 0 }
            : {
                form: "flat",
                length: rows,
                validity,
                data:
                  kind === "wide"
                    ? {
                        layout: "decimal",
                        scale: 0,
                        scales: null,
                        specials: null,
                        coefficients: {
                          repr: "wide",
                          values: Array.from({ length: rows }, (_, i) => 10n ** 80n + BigInt(i)),
                        },
                      }
                    : { layout: "i32", values },
              };
          const separate = structuredClone(first);
          const payload: FramePayload = {
            sequence: 0,
            rowCount: rows,
            columns: [...Array.from({ length: copies }, () => first), separate],
            dictionaries: dictionary
              ? [
                  {
                    id: 0,
                    from: 0,
                    values: {
                      form: "flat",
                      length: rows,
                      validity: null,
                      data: {
                        layout: "varlen",
                        offsets: Int32Array.from({ length: rows + 1 }, (_, i) => i),
                        bytes: new Uint8Array(rows),
                      },
                    },
                  },
                ]
              : [],
          };
          const bytes =
            2 * ((kind === "wide" ? 16 * rows : values.byteLength) + validity.byteLength) +
            (dictionary ? 4 * (rows + 1) + rows : 0);
          expect(payloadBytes(payload)).toBe(bytes);
          reach.reach(dictionary ? "dictionary aliases" : "flat aliases");
          if (kind === "wide") reach.reach("wide arrays counted once");
          reach.reach("distinct backing");
          const received = structuredClone(payload, { transfer: [values.buffer, validity.buffer] });
          expect(payloadBytes(received)).toBe(bytes);
          reach.reach("transferred payload");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ["flat", 3, 7],
          ["dictionary", 5, 33],
          ["wide", 3, 4],
        ],
      },
    );
  });
