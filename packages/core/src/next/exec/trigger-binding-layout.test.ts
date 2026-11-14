/** Less common native representations are copied as buffers, not decoded/coerced JS values. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_BINDING_LAYOUT_REACH } from "../../testing/reach.js";
import type { FlatVector } from "../vector/vector.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { TriggerBindingProgram } from "./trigger-bindings.js";
import { decimals, decimal, I32, dictionary, constant } from "./kernels/test-support.js";
import { sortRowView } from "./sort-row.js";

for (const seed of seedsFor("next-trigger-binding-layout", [0xb7d4, 0xb7d5]))
  it(`preserves scalar layouts and exact buffer admission seed ${String(seed)}`, async () => {
    const reach = createReach("trigger scalar layouts", TRIGGER_BINDING_LAYOUT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer(),
        fc.boolean(),
        fc.integer({ min: 0, max: 2 }),
        async (value, missing, form) => {
          reach.beginHistory();
          const bytes = encodeArrayValue(I32, {
              dimensions: [3],
              lowerBounds: [-2],
              elements: [value, null, -1],
            }),
            array: FlatVector = {
              form: "flat",
              type: { kind: "array", element: I32 },
              length: 1,
              validity: null,
              data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
            },
            interval: FlatVector = {
              form: "flat",
              type: { kind: "interval" },
              length: 1,
              validity: null,
              data: {
                layout: "interval",
                months: Int32Array.of(value),
                days: Int32Array.of(-value),
                micros: { repr: "int64", values: BigInt64Array.of(2n ** 60n + BigInt(value)) },
              },
            },
            uuid: FlatVector = {
              form: "flat",
              type: { kind: "uuid" },
              length: 1,
              validity: null,
              data: {
                layout: "fixed16",
                bytes: Uint8Array.from({ length: 16 }, (_, i) => (value + i) & 255),
              },
            },
            tier = decimal(null, null),
            values = [
              array,
              interval,
              uuid,
              ...(["NaN", "Infinity", "-Infinity"] as const).map((s) =>
                decimals(tier, [s], "wide"),
              ),
            ];
          for (const [i, source] of values.entries()) {
            const type = source.type;
            if (missing) source.validity = Uint32Array.of(0);
            const vector =
                form === 0 ? source : form === 1 ? constant(source, 1) : dictionary(source, [0]),
              expected = structuredClone(sortRowView(vector, 0)),
              program = new TriggerBindingProgram(
                { id: 1, columns: [{ id: 17, name: "v", type }] },
                [
                  { source: "new", columnId: 17, name: "v", type },
                  { source: "new", columnId: 17, name: "v", type },
                ],
              );
            let held = 0,
              peak = 0,
              releases = 0;
            const charge = {
                allocate: (n: number) => {
                  held += n;
                  peak = Math.max(peak, held);
                  return true;
                },
                free: (n: number) => {
                  held -= n;
                },
              },
              reader = {
                readRowBindings: async () => ({
                  tableId: 1,
                  old: null,
                  new: [vector],
                  release: () => {
                    releases++;
                  },
                }),
              },
              scope = await program.bind({ spoolId: 1, row: 0 }, reader, charge, {
                signal: new AbortController().signal,
              });
            try {
              expect(scope.slots).toEqual([expected, expected]);
              expect(scope.slots[0]).toBe(scope.slots[1]);
              expect(releases).toBe(1);
              reach.reach(
                (
                  [
                    "SQL array",
                    "interval",
                    "UUID",
                    "decimal NaN",
                    "decimal Infinity",
                    "decimal -Infinity",
                  ] as const
                )[i] ?? "SQL array",
              );
            } finally {
              scope.close();
              scope.close();
            }
            expect(held).toBe(0);
            // The exact known-good reservation is sufficient; one byte less unwinds, without a
            // second resource request or a copy/borrow leaking on the failed allocation.
            const attempt = async (limit: number) => {
              let used = 0;
              const exact = {
                allocate: (n: number) => {
                  if (used + n > limit) return false;
                  used += n;
                  return true;
                },
                free: (n: number) => {
                  used -= n;
                },
              };
              const promise = program.bind({ spoolId: 1, row: 0 }, reader, exact, {
                signal: new AbortController().signal,
              });
              if (limit === peak) (await promise).close();
              else await expect(promise).rejects.toThrow("not admitted");
              expect(used).toBe(0);
            };
            await attempt(peak);
            await attempt(peak - 1);
            reach.reach("exact admitted boundary");
            reach.reach(missing ? "NULL payload ignored" : "live payload");
          }
          reach.reach(form === 0 ? "flat" : form === 1 ? "constant" : "dictionary");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [7, false, 0],
          [-9, true, 1],
          [1, false, 2],
        ],
      },
    );
    reach.assertReached();
  });

it("copies only the selected varlen range, not its decoded page's unrelated prefix or suffix", async () => {
  const source: FlatVector = {
      form: "flat",
      type: { kind: "bytes" },
      length: 1,
      validity: null,
      data: {
        layout: "varlen",
        bytes: Uint8Array.of(99, 98, 1, 2, 3, 97, 96),
        offsets: Int32Array.of(2, 5),
      },
    },
    program = new TriggerBindingProgram(
      { id: 1, columns: [{ id: 1, name: "x", type: source.type }] },
      [{ source: "old", columnId: 1, name: "x", type: source.type }],
    );
  let held = 0;
  const scope = await program.bind(
    { spoolId: 1, row: 0 },
    {
      readRowBindings: async () => ({
        tableId: 1,
        old: [source],
        new: null,
        release: () => undefined,
      }),
    },
    {
      allocate: (n) => {
        held += n;
        return true;
      },
      free: (n) => {
        held -= n;
      },
    },
    { signal: new AbortController().signal },
  );
  try {
    expect(scope.slots[0]?.data).toEqual({
      layout: "varlen",
      bytes: Uint8Array.of(1, 2, 3),
      offsets: Int32Array.of(0, 3),
    });
    expect(held).toBe(program.minimumBytes + 3 + 8);
  } finally {
    scope.close();
  }
  expect(held).toBe(0);
});
