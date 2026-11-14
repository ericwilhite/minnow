/** Incremental array output admits one decoded coefficient BEFORE conversion, then yields. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, TEXT_OUTPUT_SLICE_REACH } from "../../../testing/reach.js";
import { encodeArrayValue } from "../../vector/array-value.js";
import { compileExpr } from "./compile.js";
import { OutputArena } from "./vectors.js";
import { installJsonHost, JsonNode } from "./json.js";
import { Slice, runToEnd } from "../slices.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- apply with supplied receiver
const decode = TextDecoder.prototype.decode;
const NUM = { kind: "decimal", precision: null, scale: null } as const,
  ARRAY = { kind: "array", element: NUM } as const;
for (const seed of seedsFor("text-output-slice", [47073, 47074]))
  it(`funds and cancels exact coefficient steps seed${String(seed)}`, () => {
    const reach = createReach("text output slice", TEXT_OUTPUT_SLICE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 1024, max: 4096 }),
        fc.constantFrom(0, 8193, 32768),
        fc.boolean(),
        (size, padding, cancel) => {
          reach.beginHistory();
          const coefficient = (1n << BigInt(8 * size - 2)) - 1n,
            original = encodeArrayValue(NUM, {
              dimensions: [1],
              lowerBounds: [1],
              elements: [{ coefficient, scale: 0 }],
            }),
            bytes = new Uint8Array(original.length + padding);
          bytes.set(original);
          new DataView(bytes.buffer).setUint32(15, size + padding, true);
          let used = 0,
            temp = false,
            decoded = 0,
            yields = 0;
          const needed = 4096 + 16 * size,
            arena = new OutputArena(
              {
                allocate: (n) => {
                  used += n;
                  return true;
                },
                free: (n) => {
                  used -= n;
                },
              },
              1,
            ),
            slice = new Slice(0);
          installJsonHost(arena, {
            slice,
            memory: () => ({
              // eslint-disable-next-line require-yield -- already admitted test workspace
              *take(n) {
                expect(n).toBeLessThanOrEqual(needed);
                arena.take(n);
                if (n === needed) temp = true;
              },
              free(n) {
                if (n === needed) temp = false;
                arena.give(n);
              },
            }),
            evaluate: (node, batch, rows) => runToEnd(node.run(batch, rows)),
          });
          const node = compileExpr(
            { op: "cast", input: { op: "column", index: 0, type: ARRAY }, type: { kind: "text" } },
            { arena, input: [ARRAY], params: { slots: [] } },
          );
          if (!(node instanceof JsonNode)) throw new Error("Resumable output required");
          const spy = vi.spyOn(TextDecoder.prototype, "decode").mockImplementation(function (
            this: TextDecoder,
            ...args
          ) {
            if (args[0]?.byteLength === 2 * size) {
              expect(temp).toBe(true);
              decoded++;
            }
            return decode.apply(this, args);
          });
          const job = node.run(
            {
              length: 1,
              selection: null,
              columns: [
                {
                  form: "flat",
                  type: ARRAY,
                  length: 1,
                  validity: null,
                  data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
                },
              ],
            },
            null,
          );
          try {
            for (;;) {
              const step = job.next();
              if (step.done) {
                if (cancel) throw new Error("Cancellation point missing");
                if (step.value.form !== "flat" || step.value.data.layout !== "varlen")
                  throw new Error("Text output required");
                expect(decode.call(new TextDecoder(), step.value.data.bytes)).toBe(
                  "{" + String(coefficient) + "}",
                );
                expect(decoded).toBe(2);
                expect(temp).toBe(false);
                reach.reach("funded decode");
                reach.reach("complete");
                break;
              }
              expect(step.value).toBeUndefined();
              yields++;
              if (yields === 1) {
                expect(decoded).toBe(0);
                reach.reach("before decode yield");
                if (cancel) {
                  node.discard(job);
                  expect(temp).toBe(false);
                  reach.reach("cancel");
                  break;
                }
              }
            }
            if (padding) reach.reach("redundant sign bytes");
          } finally {
            node.discard(job);
            node.reset();
            arena.close();
            spy.mockRestore();
          }
          expect(used).toBe(0);
          reach.reach("zero");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [1024, 0, false],
          [4096, 32768, false],
          [1024, 8193, true],
        ],
      },
    );
    reach.assertReached();
  });
