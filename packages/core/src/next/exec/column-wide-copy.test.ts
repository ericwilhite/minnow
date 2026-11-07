/** ColumnStore.appendFrom contract: copy values by row id, with -1 producing NULL. */
import { afterAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { bigintBytes } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { INT, DBL, BOOL, TEXT, vectorOf, valueAt, type Value } from "./join-fixtures.js";
const NUM: PhysicalType = { kind: "decimal", precision: null, scale: 0 };
const reach = createReach("next-column-wide-copy", [
  "wide first",
  "wide middle",
  "wide last",
  "multiple wide",
  "NULL coefficients",
  "negative ids",
  "duplicate ids",
  "negative wide",
  "129 bits",
  "511 bits",
  "partial destination",
  "reset",
  "released charges",
]);
afterAll(() => reach.assertReached());
class Ledger {
  held = 0;
  allocate(bytes: number): boolean {
    this.held += bytes;
    return true;
  }
  free(bytes: number): void {
    this.held -= bytes;
    if (this.held < 0) throw new Error("over-release");
  }
}
function wide(values: readonly Value[]): FlatVector {
  const validity = new Uint32Array(Math.ceil(values.length / 32)).fill(0xffffffff);
  values.forEach((v, i) => {
    if (v === null) validity[i >>> 5] = (validity[i >>> 5] ?? 0) & ~(1 << (i & 31));
  });
  return {
    form: "flat",
    type: NUM,
    length: values.length,
    validity,
    data: {
      layout: "decimal",
      scale: 0,
      scales: null,
      specials: null,
      coefficients: { repr: "wide", values: values.map((v) => (v === null ? 0n : BigInt(v))) },
    },
  };
}
it("minimal mixed INT, DOUBLE, 2^200 NUMERIC copy", () => {
  const ledger = new Ledger(),
    types = [INT, DBL, NUM];
  const source = new ColumnStore(types, ledger),
    out = new ColumnStore(types, ledger);
  try {
    expect(
      source.append([vectorOf(INT, [1]), vectorOf(DBL, [1]), wide([1n << 200n])], null, 0, 1),
    ).toBe(1);
    expect(out.appendFrom(source, [0, 1, 2], Int32Array.of(0), 0, 1)).toBe(1);
    expect(types.map((_, c) => valueAt(out.chunk(c, 0), 0))).toEqual([1, 1, 1n << 200n]);
  } finally {
    out.close();
    source.close();
    expect(ledger.held).toBe(0);
  }
});
for (const seed of seedsFor("next-column-wide-copy", [0xc011, 0xc012]))
  for (const position of ["first", "middle", "last"] as const)
    it(`wide ${position} preserves mixed rows seed ${String(seed)}`, () => {
      reach.beginHistory();
      const random = mulberry32(seed);
      const n = 65 + Math.floor(random() * 40);
      const input: Value[][] = [
        Array.from({ length: n }, (_, i) => i - 30),
        Array.from({ length: n }, (_, i) => (i % 7 === 0 ? null : `v${String(i)}`)),
        Array.from({ length: n }, (_, i) => i % 2 === 0),
        Array.from({ length: n }, (_, i) => i / 4),
        ...[0, 1].map((c) =>
          Array.from({ length: n }, (_, i) =>
            i % 7 === c
              ? null
              : (i % 2 === 0 ? -1n : 1n) *
                ((1n << BigInt(([129, 200, 511][i % 3] ?? 200) - 1)) + BigInt(i + seed + c)),
          ),
        ),
      ];
      const base = [INT, TEXT, BOOL, DBL, NUM, NUM];
      const order =
        position === "first"
          ? [4, 5, 0, 1, 2, 3]
          : position === "middle"
            ? [0, 1, 4, 5, 2, 3]
            : [0, 1, 2, 3, 4, 5];
      const types = order.map((c) => base[c] ?? INT),
        values = order.map((c) => input[c] ?? []);
      const ledger = new Ledger(),
        source = new ColumnStore(types, ledger, { chunkRows: 16 }),
        out = new ColumnStore(types, ledger, { chunkRows: 32 });
      try {
        expect(
          source.append(
            types.map((t, c) =>
              t.kind === "decimal" ? wide(values[c] ?? []) : vectorOf(t, values[c] ?? []),
            ),
            null,
            0,
            n,
          ),
        ).toBe(n);
        for (let round = 0; round < 2; round++) {
          const ids = Int32Array.from({ length: 107 }, (_, i) =>
            i % 11 === 0 ? -1 : Math.floor(random() * n),
          );
          ids[1] = n - 1;
          ids[2] = n - 1;
          ids[3] = 2;
          ids[4] = 6;
          ids[5] = 1;
          ids[6] = 0;
          expect(
            out.appendFrom(
              source,
              types.map((_, c) => c),
              ids,
              0,
              ids.length,
            ),
          ).toBe(ids.length);
          for (let c = 0; c < types.length; c++) {
            const actual: Value[] = [];
            for (let k = 0; k < out.chunkCount; k++) {
              const v = out.chunk(c, k);
              for (let i = 0; i < v.length; i++) actual.push(valueAt(v, i));
            }
            expect(actual).toEqual(
              Array.from(ids, (id) => (id < 0 ? null : (values[c]?.[id] ?? null))),
            );
          }
          let dynamic = 0;
          for (let c = 0; c < types.length; c++)
            if (types[c]?.kind === "decimal")
              for (const id of ids)
                dynamic += bigintBytes(id < 0 ? 0n : BigInt(values[c]?.[id] ?? 0));
          expect(out.retainedBytes).toBe(out.fixedBytes(out.rows) + dynamic);
          expect(ledger.held).toBe(source.retainedBytes + out.retainedBytes);
          out.truncate(0);
          out.reset();
          expect(out.retainedBytes).toBe(0);
        }
        for (const state of [
          `wide ${position}`,
          "multiple wide",
          "NULL coefficients",
          "negative ids",
          "duplicate ids",
          "negative wide",
          "129 bits",
          "511 bits",
          "partial destination",
          "reset",
        ] as const)
          reach.reach(state);
      } finally {
        out.close();
        source.close();
        expect(ledger.held).toBe(0);
        reach.reach("released charges");
      }
    });
