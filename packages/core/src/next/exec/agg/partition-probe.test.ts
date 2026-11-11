/** Snapshot replay partitions SQL key classes, never rows within a class. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Vector } from "../../vector/vector.js";
import { F64, I32, TEXT, dictionary, numbers, texts, valueAt } from "../kernels/test-support.js";
import { GroupPartitionProbe } from "./partition-probe.js";

function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new Error("Missing generated partition value");
  return value;
}

it.each(seedsFor("aggregate-replay-partitions", [0xb491, 0xb492]))(
  "plans disjoint SQL key classes and retains each class's row order (seed %i)",
  (seed) => {
    const reach = createReach("aggregate replay partitions", [
      "flat",
      "dictionary",
      "NULL",
      "signed zero",
      "NaN",
      "selection",
      "multiple passes",
      "released",
    ]);
    fc.assert(
      fc.property(
        fc.array(fc.integer({ min: 0, max: 8 }), { minLength: 96, maxLength: 160 }),
        fc.integer({ min: 1, max: 7 }),
        (codes, shift) => {
          reach.beginHistory();
          let bytes = 0;
          const charge = {
            allocate(n: number) {
              bytes += n;
              return true;
            },
            free(n: number) {
              bytes -= n;
            },
          };
          const probe = new GroupPartitionProbe([F64, TEXT], 0, charge, 4, 32);
          const batches: Array<{
            keys: Vector[];
            length: number;
            selection: { rows: Uint16Array; count: number };
            start: number;
          }> = [];
          const model = new Map<string, number[]>();
          const key = (a: unknown, b: unknown) =>
            JSON.stringify([Number.isNaN(a) ? "NaN" : a === 0 ? 0 : a, b]);
          try {
            for (let start = 0; start < codes.length; start += 24) {
              const part = codes.slice(start, start + 24);
              const entries = [null, -0, 0, NaN, 1.25, -2.5, 3.75, 8.125, 9.5];
              const a = part.map((c, i) => entries[(c + i) % entries.length] ?? null);
              const b = part.map((c, i) => (i % 5 === 0 ? null : `k${String((c + shift) % 3)}`));
              const flat = numbers(F64, a);
              const keys: Vector[] =
                start % 48 === 0
                  ? [flat, texts(b)]
                  : [
                      dictionary(
                        numbers(
                          F64,
                          a.map((v) => v ?? 99),
                        ),
                        a.map((v, i) => (v === null ? null : i)),
                      ),
                      dictionary(
                        texts(b.map((v) => v ?? "unused")),
                        b.map((v, i) => (v === null ? null : i)),
                      ),
                    ];
              reach.reach(start % 48 === 0 ? "flat" : "dictionary");
              const rows = Uint16Array.from(part.flatMap((_, i) => (i % 4 === 1 ? [] : [i])));
              const selection = { rows, count: rows.length };
              for (const row of rows) {
                const id = key(a[row], b[row]);
                const found = model.get(id) ?? [];
                found.push(start + row);
                model.set(id, found);
                if (a[row] === null || b[row] === null) reach.reach("NULL");
                if (Object.is(a[row], -0)) reach.reach("signed zero");
                if (Number.isNaN(a[row])) reach.reach("NaN");
              }
              expect(probe.offer(keys, [], part.length, selection)).toBe(true);
              batches.push({ keys, length: part.length, selection, start });
            }
            reach.reach("selection");
            const bounds = probe.seal();
            expect(bounds).not.toBeNull();
            if (!bounds) throw new Error("Missing complete certificate");
            expect(bounds.passes).toBeGreaterThan(1);
            reach.reach("multiple passes");
            const seen = new Map<string, { pass: number; rows: number[] }>();
            const out = { rows: new Uint16Array(2048), count: 0 };
            for (let pass = 0; pass < bounds.passes; pass++) {
              for (const batch of batches) {
                probe.select(batch.keys, batch.length, batch.selection, pass, out);
                for (const row of out.rows.subarray(0, out.count)) {
                  const id = key(valueAt(at(batch.keys, 0), row), valueAt(at(batch.keys, 1), row));
                  const found = seen.get(id) ?? { pass, rows: [] };
                  expect(found.pass).toBe(pass);
                  found.rows.push(batch.start + row);
                  seen.set(id, found);
                }
              }
            }
            expect(new Map([...seen].map(([k, v]) => [k, v.rows]))).toEqual(model);
          } finally {
            probe.close();
          }
          expect(bytes).toBe(0);
          reach.reach("released");
        },
      ),
      { seed, numRuns: 30 },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("aggregate-replay-probe-limits", [0xb493, 0xb494]))(
  "never seals an incomplete bounded probe (seed %i)",
  (seed) => {
    const reach = createReach("aggregate replay proof limits", [
      "domain product",
      "scratch bound",
      "changed snapshot",
      "released",
    ]);
    fc.assert(
      fc.property(fc.integer({ min: 5, max: 40 }), (count) => {
        reach.beginHistory();
        for (const mode of ["domain product", "scratch bound", "changed snapshot"] as const) {
          let bytes = 0;
          const probe = new GroupPartitionProbe(
            [I32],
            0,
            {
              allocate(n) {
                bytes += n;
                return true;
              },
              free(n) {
                bytes -= n;
              },
            },
            mode === "changed snapshot" ? 128 : 2,
            1,
            mode === "scratch bound" ? 24576 : 1 << 20,
          );
          try {
            const keys = [
              numbers(
                I32,
                Array.from(
                  { length: mode === "scratch bound" ? count + 1024 : count },
                  (_, i) => i * 7919,
                ),
              ),
            ];
            const accepted = probe.offer(keys, [], keys[0]?.length ?? 0, null);
            const bounds = probe.seal();
            if (mode === "changed snapshot") {
              expect(accepted).toBe(true);
              expect(bounds).not.toBeNull();
              expect(() =>
                probe.select([numbers(I32, [-1])], 1, null, 0, {
                  rows: new Uint16Array(1),
                  count: 0,
                }),
              ).toThrow("Pinned snapshot changed");
            } else {
              expect(accepted).toBe(false);
              expect(bounds).toBeNull();
            }
            reach.reach(mode);
          } finally {
            probe.close();
          }
          expect(bytes).toBe(0);
        }
        reach.reach("released");
      }),
      { seed, numRuns: 10 },
    );
    reach.assertReached();
  },
);
