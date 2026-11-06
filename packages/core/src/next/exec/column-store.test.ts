/**
 * The column store (exec/column-store.ts): exact values through every representation change,
 * appends that stop at a row boundary when refused, and charges that match what it holds.
 */
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import type { Vector } from "../vector/vector.js";
import {
  ANYNUM,
  BIG,
  BOOL,
  DBL,
  INT,
  NUM,
  TEXT,
  valueAt,
  vectorOf,
  type Value,
} from "./join-fixtures.js";
import { ColumnStore, TIER_INT128, TIER_INT64, TIER_SAFE, nullConstant } from "./column-store.js";

const SEEDS = seedsFor("next-column-store", [0x570e, 99]);

class Ledger {
  held = 0;
  limit = Infinity;
  allocate = (bytes: number) => {
    if (this.held + bytes > this.limit) return false;
    this.held += bytes;
    return true;
  };
  free = (bytes: number) => {
    this.held -= bytes;
    if (this.held < 0) throw new Error("freed more than allocated");
  };
}

function storeValues(store: ColumnStore, column: number): Value[] {
  const out: Value[] = [];
  for (let k = 0; k < store.chunkCount; k++) {
    const chunk = store.chunk(column, k);
    for (let i = 0; i < chunk.length; i++) out.push(valueAt(chunk, i));
  }
  return out;
}

describe("column store", () => {
  for (const seed of SEEDS)
    it(`keeps values exact across forms and tiers (seed ${String(seed)})`, () => {
      const random = mulberry32(seed);
      const types: PhysicalType[] = [INT, BIG, DBL, TEXT, BOOL, NUM, ANYNUM];
      const ledger = new Ledger();
      const store = new ColumnStore(types, ledger, { chunkRows: 64 });
      const expected: Value[][] = types.map(() => []);
      for (let b = 0; b < 30; b++) {
        const n = 1 + Math.floor(random() * 90);
        const wide = random() < 0.2;
        const columns: Value[][] = [
          Array.from({ length: n }, () =>
            random() < 0.1 ? null : Math.floor(random() * 1000) - 500,
          ),
          Array.from({ length: n }, () =>
            random() < 0.1
              ? null
              : wide
                ? 2n ** 60n + BigInt(b)
                : BigInt(Math.floor(random() * 1e9)),
          ),
          Array.from({ length: n }, () => (random() < 0.1 ? null : random())),
          Array.from({ length: n }, () =>
            random() < 0.1 ? null : "s".repeat(Math.floor(random() * 80)),
          ),
          Array.from({ length: n }, () => (random() < 0.1 ? null : random() < 0.5)),
          Array.from({ length: n }, () => (random() < 0.1 ? null : Math.floor(random() * 1e6))),
          Array.from({ length: n }, () =>
            random() < 0.1 ? null : BigInt(Math.floor(random() * 1e6)) * (b > 20 ? 10n ** 25n : 1n),
          ),
        ];
        const options = {
          int64: wide || random() < 0.5 ? ("int64" as const) : ("safe" as const),
          dictionary: random() < 0.4,
        };
        const vectors: Vector[] = types.map((type, c) => vectorOf(type, columns[c] ?? [], options));
        // A selection over every other row.
        const ids = Uint16Array.from({ length: Math.ceil(n / 2) }, (_, i) => i * 2);
        const take = random() < 0.5;
        const appended = take
          ? store.append(vectors, ids, 0, ids.length)
          : store.append(vectors, null, 0, n);
        expect(appended).toBe(take ? ids.length : n);
        types.forEach((_, c) => {
          const source = columns[c] ?? [];
          if (take) for (const row of ids) expected[c]?.push(source[row] ?? null);
          else expected[c]?.push(...source);
        });
      }
      types.forEach((type, c) => {
        const got = storeValues(store, c);
        const want = (expected[c] ?? []).map((v) =>
          typeof v === "number" && (type.kind === "int64" || type.kind === "decimal")
            ? BigInt(v)
            : v,
        );
        expect(got).toEqual(want);
      });
      expect(store.tier(1)).toBeGreaterThanOrEqual(TIER_SAFE);
      expect(ledger.held).toBe(store.retainedBytes);
      // Store-to-store gathers with null rows, into a single-chunk output.
      const out = new ColumnStore(types, ledger, { targetBytes: 1 << 20 });
      const pick = Int32Array.from({ length: 50 }, (_, i) =>
        i % 7 === 0 ? -1 : Math.floor(random() * store.rows),
      );
      expect(
        out.appendFrom(
          store,
          types.map((_, c) => c),
          pick,
          0,
          pick.length,
        ),
      ).toBe(pick.length);
      types.forEach((type, c) => {
        const all = expected[c] ?? [];
        const want = [...pick].map((id) => {
          const v = id < 0 ? null : (all[id] ?? null);
          return typeof v === "number" && (type.kind === "int64" || type.kind === "decimal")
            ? BigInt(v)
            : v;
        });
        expect(storeValues(out, c)).toEqual(want);
      });
      out.close();
      store.close();
      expect(ledger.held).toBe(0);
    });

  it("narrows int64 batches that are safe and upgrades the column when one is not", () => {
    const ledger = new Ledger();
    const store = new ColumnStore([BIG], ledger, { chunkRows: 16 });
    store.append([vectorOf(BIG, [1n, 2n, 3n], { int64: "int64" })], null, 0, 3);
    expect(store.tier(0)).toBe(TIER_SAFE);
    store.append(
      [
        vectorOf(
          BIG,
          Array.from({ length: 20 }, (_, i) => BigInt(i)),
          { int64: "safe" },
        ),
      ],
      null,
      0,
      20,
    );
    store.append([vectorOf(BIG, [2n ** 62n, -(2n ** 63n)], { int64: "int64" })], null, 0, 2);
    expect(store.tier(0)).toBe(TIER_INT64);
    expect(storeValues(store, 0)).toEqual([
      1n,
      2n,
      3n,
      ...Array.from({ length: 20 }, (_, i) => BigInt(i)),
      2n ** 62n,
      -(2n ** 63n),
    ]);
    expect(ledger.held).toBe(store.retainedBytes);
    store.close();
  });

  it("raises decimal tiers without changing a value", () => {
    const ledger = new Ledger();
    const store = new ColumnStore([ANYNUM], ledger, { chunkRows: 16 });
    store.append(
      [vectorOf(NUM, [150, -7, null])].map((v) => ({ ...v, type: ANYNUM })),
      null,
      0,
      3,
    );
    store.append([vectorOf(ANYNUM, [10n ** 30n, -(10n ** 20n)])], null, 0, 2);
    expect(store.tier(0)).toBe(TIER_INT128);
    expect(storeValues(store, 0)).toEqual([150n, -7n, null, 10n ** 30n, -(10n ** 20n)]);
    store.close();
  });

  it("stops at a row boundary when refused and keeps charges exact", () => {
    const ledger = new Ledger();
    const store = new ColumnStore([INT, TEXT], ledger, { chunkRows: 32, varlenBytes: 64 });
    const rows = 200;
    const vectors = [
      vectorOf(
        INT,
        Array.from({ length: rows }, (_, i) => i),
      ),
      vectorOf(
        TEXT,
        Array.from({ length: rows }, (_, i) => "x".repeat(i % 50)),
      ),
    ];
    ledger.limit = 4000;
    const first = store.append(vectors, null, 0, rows);
    expect(first).toBeGreaterThan(0);
    expect(first).toBeLessThan(rows);
    expect(ledger.held).toBe(store.retainedBytes);
    expect(storeValues(store, 0)).toEqual(Array.from({ length: first }, (_, i) => i));
    ledger.limit = Infinity;
    expect(store.append(vectors, null, first, rows - first)).toBe(rows - first);
    expect(storeValues(store, 1)).toEqual(
      Array.from({ length: rows }, (_, i) => "x".repeat(i % 50)),
    );
    store.truncate(40);
    expect(storeValues(store, 0)).toEqual(Array.from({ length: 40 }, (_, i) => i));
    expect(ledger.held).toBe(store.retainedBytes);
    store.close();
    expect(ledger.held).toBe(0);
  });

  it("single-chunk mode stops at the byte target but admits one oversized row alone", () => {
    const ledger = new Ledger();
    const store = new ColumnStore([TEXT], ledger, { targetBytes: 100, varlenBytes: 64 });
    const vector = vectorOf(TEXT, ["a".repeat(60), "b".repeat(60), "c".repeat(500)]);
    expect(store.append([vector], null, 0, 3)).toBe(1);
    store.reset();
    expect(store.append([vector], null, 2, 1)).toBe(1);
    expect(storeValues(store, 0)).toEqual(["c".repeat(500)]);
    store.reset();
    expect(ledger.held).toBe(store.retainedBytes);
    store.close();
    expect(ledger.held).toBe(0);
  });

  it("null constants are null for every row", () => {
    const vector = nullConstant(TEXT, 5);
    expect([0, 1, 4].map((i) => valueAt(vector, i))).toEqual([null, null, null]);
  });
});

const gatherReach = createReach("next-column-gather", [
  "contiguous",
  "interior-permuted",
  "null-id",
  "cross-chunk",
]);
afterAll(() => gatherReach.assertReached());

// A near-contiguous permutation must not be mistaken for a contiguous span from endpoints.
for (const seed of seedsFor("next-column-gather", [0xc021, 0x154]))
  it(`contiguous and interior-permuted store gathers (seed ${String(seed)})`, () => {
    const random = mulberry32(seed);
    const ledger = new Ledger();
    const types = [INT, DBL, BIG];
    const n = 512;
    const floats = Array.from({ length: n }, (_, i) =>
      i % 11 === 0 ? NaN : i % 13 === 0 ? Infinity : i % 17 === 0 ? -0 : i / 4,
    );
    const ints = Array.from({ length: n }, (_, i) => i - 300);
    const bigs = Array.from({ length: n }, (_, i) => BigInt(i) * 23n);
    const columns = [ints, floats, bigs];
    const source = new ColumnStore(types, ledger, { chunkRows: 64 });
    expect(
      source.append(
        types.map((type, c) => vectorOf(type, columns[c] ?? [])),
        null,
        0,
        n,
      ),
    ).toBe(n);
    for (const mode of ["contiguous", "interior-permuted", "null-id"] as const) {
      const start = 1 + Math.floor(random() * 40);
      const ids = Int32Array.from({ length: 200 }, (_, i) => start + i);
      if (mode === "interior-permuted") {
        ids[73] = start + 74;
        ids[74] = start + 73;
      }
      if (mode === "null-id") ids[73] = -1;
      const out = new ColumnStore(types, ledger, { chunkRows: 64 });
      expect(out.appendFrom(source, [0, 1, 2], ids, 0, ids.length)).toBe(ids.length);
      types.forEach((_, c) =>
        expect(storeValues(out, c), mode).toEqual(
          [...ids].map((id) => (id < 0 ? null : columns[c]?.[id])),
        ),
      );
      out.close();
      gatherReach.beginHistory();
      gatherReach.reach(mode);
      if (start + ids.length > source.chunkRows) gatherReach.reach("cross-chunk");
    }
    source.close();
    expect(ledger.held).toBe(0);
  });
