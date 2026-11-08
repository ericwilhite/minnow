/** Exact integer SUM and C11 AVG=sum::float8/count::float8, judged by PostgreSQL. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { AggregateCall } from "../../ir/ir.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch, Vector } from "../../vector/vector.js";
import {
  I32,
  I64,
  F64,
  UNCHARGED,
  col,
  numbers,
  dictionary,
  constant,
  valueAt,
} from "../kernels/test-support.js";
import { IDENTITY } from "../kernels/vectors.js";
import { PackedIntegers, packedIntegerCalls } from "./packed-integers.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const input = Array.from({ length: 7 }, () => I32);
const call = (fn: AggregateCall["fn"], column: number, type = I64): AggregateCall => ({
  fn,
  type,
  args: column < 0 ? [] : [col(column, I32)],
  filter: null,
  distinct: false,
  orderBy: [],
});
const calls = [
  call("count_star", -1),
  ...Array.from({ length: 5 }, (_, i) => call("sum", i)),
  call("avg", 5, F64),
  call("count", 6),
];
function make(charge: ByteCharge = UNCHARGED) {
  const selected = packedIntegerCalls(calls, input);
  if (!selected) throw new Error("Missing packed integer plan");
  return new PackedIntegers(selected, input, charge);
}
function push(bank: PackedIntegers, batch: Batch, gids: Int32Array) {
  const rows = batch.selection?.rows ?? IDENTITY,
    n = batch.selection?.count ?? batch.length;
  if (bank.tryUpdate(batch, gids, rows, n)) return;
  for (const { index, accumulator } of bank.members) {
    const arg = calls[index]?.args[0];
    accumulator.update(
      arg?.op === "column" ? (batch.columns[arg.index] ?? null) : null,
      gids,
      rows,
      n,
      false,
    );
  }
}
function grow(bank: PackedIntegers, n: number) {
  for (const m of bank.members) m.accumulator.grow(n);
}
function close(bank: PackedIntegers) {
  for (const m of bank.members) m.accumulator.close();
}

it.each(seedsFor("packed-integer-transitions", [0xb471, 0xb472]))(
  "restores exact prefixes across masks, encodings, growth and selections (seed %i)",
  async (seed) => {
    const reach = createReach("packed integer prefix", [
      "packed",
      "nullable",
      "dictionary",
      "constant",
      "large group fallback",
      "resume",
      "pg",
      "borrowed input",
      "release",
      "interleaved",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: -100000, max: 100000 }), { minLength: 24, maxLength: 24 }),
        fc.boolean(),
        fc.integer({ min: 0, max: 5 }),
        async (base, selected, target) => {
          reach.beginHistory();
          for (const mode of ["nullable", "dictionary", "constant", "groups"] as const) {
            let held = 0;
            const bank = make({
              allocate(n) {
                held += n;
                return true;
              },
              free(n) {
                held -= n;
              },
            });
            grow(bank, 4);
            const other = make();
            grow(other, 1);
            const otherColumns = input.map((_, c) =>
              numbers(I32, c === 6 ? [null, 100] : [100, 100]),
            );
            const raw: Array<Array<number | null>> = [];
            const groups: number[] = [];
            let order = 0;
            for (let phase = 0; phase < 3; phase++) {
              const length = phase === 0 ? 8 : 16;
              const columns: Vector[] = input.map((_, c) =>
                numbers(
                  I32,
                  Array.from({ length }, (_, i) =>
                    c === 6 && i % 3 === 0
                      ? null
                      : (base[(i + phase) % base.length] ?? 0) * (c + 1),
                  ),
                ),
              );
              if (phase === 1) {
                const values = Array.from({ length }, (_, i) => (base[i] ?? 0) * (target + 1));
                if (mode === "nullable")
                  columns[target] = numbers(
                    I32,
                    values.map((v, i) => (i % 2 ? v : null)),
                    -2147483648,
                  );
                if (mode === "dictionary")
                  columns[target] = dictionary(
                    numbers(I32, [3, -11, 7]),
                    values.map((_, i) => i % 3),
                  );
                if (mode === "constant") columns[target] = constant(numbers(I32, [7]), length);
                if (mode === "groups") grow(bank, 5000);
              }
              const gids = Int32Array.from({ length }, (_, i) => (i + phase) % 4),
                rows = selected
                  ? Uint16Array.from({ length: length / 2 }, (_, i) => i * 2)
                  : IDENTITY,
                count = selected ? length / 2 : length,
                batch: Batch = { length, columns, selection: selected ? { rows, count } : null };
              push(bank, batch, gids);
              push(other, { columns: otherColumns, length: 2, selection: null }, new Int32Array(2));
              if (phase === 0) {
                expect(bank.fusedRows).toBe(count);
                reach.reach("packed");
              }
              if (phase === 2) reach.reach("resume");
              for (let k = 0; k < count; k++) {
                const row = rows[k] ?? 0;
                raw.push(columns.map((v) => valueAt(v, row) as number | null));
                groups.push(gids[row] ?? 0);
                order++;
              }
              // Borrowed input can be overwritten immediately after synchronous push.
              for (const v of columns)
                if (v.form === "flat" && v.data.layout === "i32") v.data.values.fill(0);
              reach.reach("borrowed input");
            }
            const values = raw.map(
              (row, i) =>
                `(${String(i)},${String(groups[i])},${row.map((v) => (v === null ? "NULL::int4" : `${String(v)}::int4`)).join(",")})`,
            );
            const expected = (
              await pg.query<Record<string, string | null>>(`WITH inp(ord,g,c0,c1,c2,c3,c4,c5,c6) AS
            (VALUES ${values.join(",")}) SELECT g::text,count(*)::text AS a0,
            ${Array.from({ length: 5 }, (_, i) => `sum(c${String(i)})::text AS a${String(i + 1)}`).join(",")},
            (sum(c5)::float8/count(c5)::float8)::text AS a6,count(c6)::text AS a7 FROM inp GROUP BY g ORDER BY g`)
            ).rows;
            for (const row of expected) {
              const g = Number(row.g);
              for (let slot = 0; slot < 8; slot++) {
                const member = bank.members[slot];
                if (!member) throw new Error("member");
                const result = valueAt(member.accumulator.emit(g, 1), 0),
                  value = row[`a${String(slot)}`];
                expect(result).toEqual(
                  value === null ? null : slot === 6 ? Number(value) : BigInt(value ?? "0"),
                );
              }
            }
            expect(order).toBe(raw.length);
            reach.reach(mode === "groups" ? "large group fallback" : mode);
            reach.reach("pg");
            for (let slot = 0; slot < 8; slot++) {
              const member = other.members[slot];
              if (!member) throw new Error("other member");
              expect(valueAt(member.accumulator.emit(0, 1), 0)).toEqual(
                slot === 0 ? 6n : slot === 7 ? 3n : slot === 6 ? 100 : 600n,
              );
            }
            close(other);
            reach.reach("interleaved");
            close(bank);
            expect(held).toBe(0);
            reach.reach("release");
          }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("packed-integer-admission", [0xb474]))(
  "unwinds every partial promotion after an allocation refusal (seed %i)",
  (seed) => {
    const reach = createReach("packed integer admission", [
      "charged prefix",
      "partial promotion",
      "refusal",
      "complete",
      "released",
    ]);
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 30 }), (refusal) => {
        reach.beginHistory();
        for (const failAt of [refusal, 1, 100]) {
          let held = 0,
            peak = 0,
            allocations = 0,
            armed = false;
          const bank = make({
            allocate(n) {
              if (armed && ++allocations === failAt) return false;
              held += n;
              peak = Math.max(peak, held);
              return true;
            },
            free(n) {
              held -= n;
              expect(held).toBeGreaterThanOrEqual(0);
            },
          });
          grow(bank, 4);
          const columns = input.map(() => numbers(I32, [1, 2, 3, 4])),
            gids = Int32Array.from([0, 1, 2, 3]);
          push(bank, { columns, length: 4, selection: null }, gids);
          expect(held).toBe(4 * 8 * 8);
          reach.reach("charged prefix");
          columns[0] = numbers(I32, [null, null, null, null]);
          armed = true;
          try {
            push(bank, { columns, length: 4, selection: null }, gids);
            reach.reach("complete");
          } catch (error) {
            expect(error).toBeInstanceOf(RangeError);
            expect(String(error)).toContain("aggregation growth was refused");
            reach.reach("refusal");
          } finally {
            close(bank);
          }
          if (peak > 4 * 8 * 8) reach.reach("partial promotion");
          expect(held).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 25 },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("packed-integer-bound", [0xb473]))(
  "promotes at the written exactness bound (seed %i)",
  async (seed) => {
    const reach = createReach("packed integer exactness bound", [
      "positive",
      "negative",
      "carry",
      "pg",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 7 }), async (extra) => {
        reach.beginHistory();
        for (const negative of [false, true]) {
          const value = negative ? -2147483648 : 2147483647,
            total = 2 ** 22 + extra,
            bank = make();
          grow(bank, 1);
          const columns = input.map((_, c) =>
              numbers(
                I32,
                Array.from({ length: 2048 }, (_, i) => (c === 6 && i % 2 === 0 ? null : value)),
              ),
            ),
            gids = new Int32Array(2048);
          for (let i = 0; i < total; i += 2048)
            push(bank, { length: Math.min(2048, total - i), columns, selection: null }, gids);
          reach.reach("carry");
          // A same-sign constant int4 stream has |v*N| < 2^63 at every prefix.
          // PG int8 multiplication therefore gives its exact SUM, without making
          // PostgreSQL materialize 4M generate_series rows for each history. The
          // bank still executes all N transitions; small campaigns use real SUM.
          const expected = (
            await pg.query<{ n: string; s: string; a: string; nullable: string }>(
              `WITH bounds(v,n) AS (VALUES ('${String(value)}'::int4::int8,${String(total)}::bigint))
             SELECT n::text AS n,(v*n)::text AS s,((v*n)::float8/n::float8)::text AS a,
             (n/2)::text AS nullable FROM bounds`,
            )
          ).rows[0];
          if (!expected) throw new Error("PG bound result");
          for (let slot = 0; slot < 8; slot++) {
            const member = bank.members[slot];
            if (!member) throw new Error("member");
            const want =
              slot === 0
                ? BigInt(expected.n)
                : slot === 7
                  ? BigInt(expected.nullable)
                  : slot === 6
                    ? Number(expected.a)
                    : BigInt(expected.s);
            expect(valueAt(member.accumulator.emit(0, 1), 0)).toEqual(want);
          }
          expect(bank.fusedRows).toBe(2 ** 21);
          reach.reach(negative ? "negative" : "positive");
          reach.reach("pg");
          close(bank);
        }
      }),
      { seed, numRuns: 3, endOnFailure: true },
    );
    reach.assertReached();
  },
  60000,
);
