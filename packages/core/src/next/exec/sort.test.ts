/**
 * Sort, top-N and per-group top-k against an oracle with PostgreSQL's ORDER BY semantics (NaN
 * above every number, −0 = 0, NULLS FIRST/LAST, DESC, UTF-8 byte order for C, `Intl.Collator`
 * then bytes for an ICU collation: DECISIONS.md C8), resident and spilled (tight grants force
 * runs and merge passes). Ties make the order of equal-key rows free, so the checks are: the
 * output is a sub-multiset of the input, it is sorted, and its key sequence is the oracle's.
 * sort-pglite.test.ts holds the key order itself to PostgreSQL.
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import {
  ArraySource,
  defined,
  BIG,
  BOOL,
  CollectSink,
  DBL,
  INT,
  TEXT,
  batchesOf,
  canonical,
  type Row,
  type Value,
} from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import type { SortColumn } from "./sort-keys.js";
import { GroupTopKOperator, SortOperator, sortMinimumBytes, type SortProfile } from "./sort.js";

const SEEDS = seedsFor("next-sort", [0x50e7, 0xa11]);
const encoder = new TextEncoder();

function compareValue(type: PhysicalType, a: Value, b: Value, collation: string | null): number {
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isNaN(a)) return Number.isNaN(b) ? 0 : 1;
    if (Number.isNaN(b)) return -1;
    return a < b ? -1 : a > b ? 1 : 0;
  }
  if (typeof a === "bigint" && typeof b === "bigint") return a < b ? -1 : a > b ? 1 : 0;
  if (typeof a === "boolean" && typeof b === "boolean") return a === b ? 0 : a ? 1 : -1;
  if (typeof a === "string" && typeof b === "string") {
    if (collation !== null && type.kind === "text") {
      const order = new Intl.Collator(collation).compare(a, b);
      if (order !== 0) return order < 0 ? -1 : 1;
    }
    return Buffer.compare(encoder.encode(a), encoder.encode(b));
  }
  throw new Error(`cannot compare ${typeof a} and ${typeof b}`);
}

function oracleCompare(types: readonly PhysicalType[], keys: readonly SortColumn[]) {
  return (x: Row, y: Row): number => {
    for (const key of keys) {
      const a = x[key.column] ?? null;
      const b = y[key.column] ?? null;
      if (a === null || b === null) {
        if (a === null && b === null) continue;
        return (a === null) === (key.nulls === "first") ? -1 : 1;
      }
      const order = compareValue(defined(types[key.column]), a, b, key.collation);
      if (order !== 0) return key.direction === "desc" ? -order : order;
    }
    return 0;
  };
}

interface Run {
  readonly rows: Row[];
  readonly profile: SortProfile;
  readonly governor: Governor;
  readonly spill: MemorySpillService;
}

async function runSort(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  rows: readonly Row[],
  options: {
    limit?: number | null;
    offset?: number;
    withTies?: boolean;
    groupBy?: readonly number[];
    k?: number;
    tight?: number;
    blockEvery?: number;
    batchRows?: number;
    dictionary?: boolean;
  } = {},
): Promise<Run> {
  const minimum = sortMinimumBytes(types);
  const cap =
    options.tight === undefined
      ? 512 << 20
      : Math.max(
          minimum + options.tight + (1 << 20),
          Math.ceil(((minimum + options.tight) * 8) / 7),
        );
  const governor = new Governor(cap);
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "sort", minimumBytes: minimum, spillable: true });
  const sink = new CollectSink(options.blockEvery ?? 0);
  const context = { grant, spill, queryId: 3 };
  const operator =
    options.groupBy === undefined
      ? new SortOperator(
          {
            types,
            keys,
            limit: options.limit ?? null,
            offset: options.offset ?? 0,
            withTies: options.withTies ?? false,
          },
          context,
          sink,
        )
      : new GroupTopKOperator(
          { types, partitionBy: options.groupBy, orderBy: keys, k: options.k ?? 1 },
          context,
          sink,
        );
  await drive(
    new ArraySource(
      batchesOf(types, rows, {
        batchRows: options.batchRows ?? 2048,
        dictionary: options.dictionary ?? false,
      }),
    ),
    operator,
  );
  expect(sink.finished).toBe(true);
  await operator.closed;
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
  return { rows: sink.rows, profile: operator.profile, governor, spill };
}

/** Output ⊆ input, sorted, and with the oracle's key sequence. */
function checkOrder(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  input: readonly Row[],
  output: readonly Row[],
  expected: readonly Row[],
): void {
  const cmp = oracleCompare(types, keys);
  expect(output.length).toBe(expected.length);
  for (let i = 1; i < output.length; i++)
    expect(cmp(defined(output[i - 1]), defined(output[i]))).toBeLessThanOrEqual(0);
  const keyOf = (row: Row) => keys.map((key) => row[key.column] ?? null);
  expect(canonical(output.map(keyOf))).toEqual(canonical(expected.map(keyOf)));
  const pool = new Map<string, number>();
  for (const row of canonical(input)) pool.set(row, (pool.get(row) ?? 0) + 1);
  for (const row of canonical(output)) {
    const left = pool.get(row) ?? 0;
    expect(left, `row ${row} not in the input`).toBeGreaterThan(0);
    pool.set(row, left - 1);
  }
}

function generate(random: () => number, n: number, domain: number): Row[] {
  return Array.from({ length: n }, (_, i): Row => [
    random() < 0.1 ? null : Math.floor(random() * domain) - domain / 2,
    random() < 0.1 ? null : BigInt(Math.floor(random() * domain)) * 1_000_000_007n,
    random() < 0.1
      ? null
      : random() < 0.05
        ? Number.NaN
        : random() < 0.05
          ? -0
          : Math.floor(random() * domain) / 4,
    random() < 0.1
      ? null
      : `${["", "a", "ab", "b", "é", "Z", "z", "😀", "ä", "ae"][Math.floor(random() * 10)] ?? ""}${String(Math.floor(random() * 3))}`,
    random() < 0.1 ? null : random() < 0.5,
    i,
  ]);
}
const TYPES = [INT, BIG, DBL, TEXT, BOOL, INT];

function randomKeys(random: () => number, collation: string | null = null): SortColumn[] {
  const columns = [0, 1, 2, 3, 4].sort(() => random() - 0.5).slice(0, 1 + Math.floor(random() * 3));
  return columns.map((column) => ({
    column,
    direction: random() < 0.5 ? "asc" : "desc",
    nulls: random() < 0.5 ? "first" : "last",
    collation: column === 3 ? collation : null,
  }));
}

describe("sort", () => {
  for (const seed of SEEDS)
    for (const tight of [undefined, 64 * 1024])
      it(`full sort, top-N, offsets and ties (seed ${String(seed)}${tight === undefined ? "" : ", spilled"})`, async () => {
        const random = mulberry32(seed + (tight ?? 0));
        for (let round = 0; round < 6; round++) {
          const rows = generate(
            random,
            tight === undefined ? 1 + Math.floor(random() * 3000) : 20_000,
            40,
          );
          const keys = randomKeys(random);
          const sorted = [...rows].sort(oracleCompare(TYPES, keys));
          const common = {
            ...(tight === undefined ? {} : { tight }),
            blockEvery: random() < 0.4 ? 2 : 0,
            batchRows: 1 + Math.floor(random() * 2048),
            dictionary: random() < 0.3,
          };
          const full = await runSort(TYPES, keys, rows, common);
          checkOrder(TYPES, keys, rows, full.rows, sorted);
          if (tight !== undefined) expect(full.profile.runs).toBeGreaterThan(0);
          const limit = Math.floor(random() * 50);
          const offset = Math.floor(random() * 100);
          const withTies = random() < 0.5;
          const top = await runSort(TYPES, keys, rows, { ...common, limit, offset, withTies });
          let end = Math.min(sorted.length, offset + limit);
          const cmp = oracleCompare(TYPES, keys);
          if (withTies && end > offset)
            while (end < sorted.length && cmp(defined(sorted[end - 1]), defined(sorted[end])) === 0)
              end++;
          checkOrder(TYPES, keys, rows, top.rows, sorted.slice(offset, end));
        }
      }, 120_000);

  it("a top-N rejects most rows before storing them and stays O(k)", async () => {
    const random = mulberry32(5);
    const rows = generate(random, 100_000, 1_000_000);
    const keys: SortColumn[] = [
      { column: 2, direction: "desc", nulls: "last", collation: null },
      { column: 5, direction: "asc", nulls: "last", collation: null },
    ];
    const result = await runSort(TYPES, keys, rows, { limit: 100, offset: 0 });
    checkOrder(
      TYPES,
      keys,
      rows,
      result.rows,
      [...rows].sort(oracleCompare(TYPES, keys)).slice(0, 100),
    );
    expect(result.profile.rejected).toBeGreaterThan(80_000);
    expect(result.profile.runs).toBe(0);
  });

  it("WITH TIES survives the top-N threshold: rows equal to the k-th are kept", async () => {
    // Low-cardinality key: the threshold forms after compaction and must not drop the ties.
    const random = mulberry32(17);
    const rows = generate(random, 60_000, 6);
    const keys: SortColumn[] = [{ column: 0, direction: "asc", nulls: "last", collation: null }];
    const result = await runSort(TYPES, keys, rows, { limit: 10, offset: 0, withTies: true });
    const sorted = [...rows].sort(oracleCompare(TYPES, keys));
    const cmp = oracleCompare(TYPES, keys);
    let end = 10;
    while (end < sorted.length && cmp(defined(sorted[end - 1]), defined(sorted[end])) === 0) end++;
    expect(result.profile.compactions).toBeGreaterThan(0);
    checkOrder(TYPES, keys, rows, result.rows, sorted.slice(0, end));
  });

  it("merges in passes when there are more runs than one merge reads", async () => {
    const random = mulberry32(9);
    const rows = generate(random, 120_000, 1_000_000);
    const keys: SortColumn[] = [
      { column: 3, direction: "asc", nulls: "first", collation: null },
      { column: 5, direction: "asc", nulls: "first", collation: null },
    ];
    const result = await runSort(TYPES, keys, rows, { tight: 0, limit: 10, offset: 100_000 });
    expect(result.profile.runs).toBeGreaterThan(8);
    expect(result.profile.mergePasses).toBeGreaterThan(1);
    checkOrder(
      TYPES,
      keys,
      rows,
      result.rows,
      [...rows].sort(oracleCompare(TYPES, keys)).slice(100_000, 100_010),
    );
  }, 120_000);

  for (const seed of SEEDS)
    for (const tight of [undefined, 64 * 1024])
      it(`ICU collation: query-wide ranks, compare mode when spilled (seed ${String(seed)}${tight === undefined ? "" : ", spilled"})`, async () => {
        const random = mulberry32(seed ^ 0xc011);
        for (const collation of ["en-US", "de", "sv"]) {
          const rows = generate(random, tight === undefined ? 2000 : 12_000, 30);
          const keys: SortColumn[] = [
            { column: 3, direction: random() < 0.5 ? "asc" : "desc", nulls: "last", collation },
            { column: 5, direction: "asc", nulls: "last", collation: null },
          ];
          const result = await runSort(TYPES, keys, rows, tight === undefined ? {} : { tight });
          expect(result.profile.mode).toBe(tight === undefined ? "ranks" : "compare");
          checkOrder(TYPES, keys, rows, result.rows, [...rows].sort(oracleCompare(TYPES, keys)));
        }
      }, 120_000);
});

describe("per-group top-k (ROW_NUMBER() <= k)", () => {
  it("per-partition thresholds reject most rows before storing them", async () => {
    const random = mulberry32(31);
    const rows = generate(random, 200_000, 1_000_000);
    const order: SortColumn[] = [
      { column: 2, direction: "desc", nulls: "last", collation: null },
      { column: 5, direction: "asc", nulls: "last", collation: null },
    ];
    const result = await runSort(TYPES, order, rows, { groupBy: [3], k: 2 });
    expect(result.profile.rejected).toBeGreaterThan(100_000);
    const groups = new Set(rows.map((row) => JSON.stringify(row[3] ?? null)));
    expect(result.rows.length).toBe(2 * groups.size);
  });

  for (const seed of SEEDS)
    for (const tight of [undefined, 32 * 1024])
      it(`first k rows of every partition, numbered (seed ${String(seed)}${tight === undefined ? "" : ", spilled"})`, async () => {
        const random = mulberry32(seed + 77);
        for (let round = 0; round < 4; round++) {
          const rows = generate(random, tight === undefined ? 5000 : 30_000, 50);
          const k = 1 + Math.floor(random() * 4);
          const partition = [3];
          const order: SortColumn[] = [
            { column: 2, direction: "desc", nulls: "last", collation: null },
            { column: 5, direction: "asc", nulls: "last", collation: null },
          ];
          const result = await runSort(TYPES, order, rows, {
            groupBy: partition,
            k,
            ...(tight === undefined ? {} : { tight }),
            blockEvery: random() < 0.5 ? 3 : 0,
          });
          // Expected: per partition (text bytes equality), the first k by the order.
          const groups = new Map<string, Row[]>();
          for (const row of rows) {
            const g = JSON.stringify(row[3] ?? null);
            groups.set(g, [...(groups.get(g) ?? []), row]);
          }
          const cmp = oracleCompare(TYPES, order);
          let expected = 0;
          const seen = new Map<string, number>();
          for (const out of result.rows) {
            const g = JSON.stringify(out[3] ?? null);
            const n = (seen.get(g) ?? 0) + 1;
            seen.set(g, n);
            expect(out[6]).toBe(BigInt(n));
            const best = [...(groups.get(g) ?? [])].sort(cmp);
            expect(cmp(out, defined(best[n - 1]))).toBe(0);
          }
          for (const [, members] of groups) expected += Math.min(k, members.length);
          expect(result.rows.length).toBe(expected);
          if (tight !== undefined)
            expect(result.profile.runs + result.profile.compactions).toBeGreaterThan(0);
        }
      }, 120_000);
});
