/** R23.5: a compact NULL array can have millions of elements; bitmap work is sliced. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, CONCAT_SLICE_REACH } from "../../../testing/reach.js";
import { arrayHeader, nativeArray } from "../array-data.js";
import { Slice, runToEnd } from "../slices.js";
import { compileExpr } from "./compile.js";
import { installJsonHost, JsonNode } from "./json.js";
import { OutputArena } from "./vectors.js";
const INT = { kind: "int32", bits: 32 } as const,
  ARRAY = { kind: "array", element: INT } as const;
for (const seed of seedsFor("concat-slices", [47063, 47064]))
  it(`slices array bitmap writes and releases cancelled output seed${String(seed)}`, () => {
    const reach = createReach("concat slices", CONCAT_SLICE_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 2048, max: 16384 }), fc.boolean(), (size, cancel) => {
        reach.beginHistory();
        const count = size * 8,
          header = arrayHeader(INT, [count], [1], true),
          input = new Uint8Array(header.length + size);
        input.set(header);
        input.fill(255, header.length);
        let used = 0;
        const arena = new OutputArena(
            {
              allocate(n) {
                used += n;
                return true;
              },
              free(n) {
                used -= n;
              },
            },
            1,
          ),
          slice = new Slice(0);
        installJsonHost(arena, {
          slice,
          memory: () => ({
            // eslint-disable-next-line require-yield -- this test's already-admitted arena never blocks
            *take(n) {
              arena.take(n);
            },
            free: (n) => arena.give(n),
          }),
          evaluate: (node, batch, rows) => runToEnd(node.run(batch, rows)),
        });
        const node = compileExpr(
          {
            op: "call",
            fn: "array_append",
            type: ARRAY,
            args: [
              { op: "column", index: 0, type: ARRAY },
              { op: "column", index: 1, type: INT },
            ],
          },
          { arena, input: [ARRAY, INT], params: { slots: [] } },
        );
        if (!(node instanceof JsonNode))
          throw new Error("Concatenation must use the resumable runner");
        const batch = {
            length: 1,
            selection: null,
            columns: [
              {
                form: "flat" as const,
                type: ARRAY,
                length: 1,
                validity: null,
                data: {
                  layout: "varlen" as const,
                  bytes: input,
                  offsets: Int32Array.of(0, input.length),
                },
              },
              {
                form: "flat" as const,
                type: INT,
                length: 1,
                validity: new Uint32Array(1),
                data: { layout: "i32" as const, values: new Int32Array(1) },
              },
            ],
          },
          job = node.run(batch, null);
        let yields = 0;
        try {
          for (;;) {
            const step = job.next();
            if (step.done) {
              if (cancel) throw new Error("Cancellation did not reach a bitmap suspension");
              if (step.value.form !== "flat") throw new Error("Flat array required");
              const d = step.value.data;
              if (d.layout !== "varlen") throw new Error("Array bytes required");
              const value = nativeArray(INT, d.bytes);
              expect(value.count).toBe(count + 1);
              expect(value.nulls?.subarray(0, size).every((v) => v === 255)).toBe(true);
              expect(value.nulls?.[size]).toBe(1);
              expect(value.payload.length).toBe(0);
              expect(yields).toBeGreaterThanOrEqual(3);
              reach.reach("complete");
              break;
            }
            expect(step.value).toBeUndefined();
            yields++;
            if (cancel && yields === 2) {
              node.discard(job);
              reach.reach("cancel");
              break;
            }
          }
          reach.reach("bitmap yield");
        } finally {
          node.discard(job);
          node.reset();
          arena.close();
        }
        expect(used).toBe(0);
        reach.reach("zero");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [2048, false],
          [16384, true],
        ],
      },
    );
    reach.assertReached();
  });
