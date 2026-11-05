/**
 * Dynamic join filters (exec/join-filter.ts). The law is soundness: a filter never drops a probe
 * row that the join would match (it may keep rows that cannot). Its value is measured too: the
 * false-positive rate, and that it stops testing when it keeps nearly everything.
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Selection } from "../vector/vector.js";
import {
  BIG,
  DBL,
  INT,
  TEXT,
  ArraySource,
  batchesOf,
  vectorOf,
  type Value,
  defined,
} from "./join-fixtures.js";
import { DROP_AFTER_PROBES, JoinRuntimeFilter, kindPublishes } from "./join-filter.js";
import { ColumnStore } from "./column-store.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec } from "./join.js";
import { drive } from "./pipeline.js";

const SEEDS = seedsFor("next-join-filter", [0xf1, 0x7e]);
const FREE = { allocate: () => true, free: () => undefined };

function storeOf(type: PhysicalType, values: readonly Value[]): ColumnStore {
  const store = new ColumnStore([type], FREE);
  store.append([vectorOf(type, values)], null, 0, values.length);
  return store;
}

function apply(
  filter: JoinRuntimeFilter,
  type: PhysicalType,
  values: readonly Value[],
): Set<number> {
  const kept = new Set<number>();
  for (let at = 0; at < values.length; at += 2048) {
    const slice = values.slice(at, at + 2048);
    const out: Selection = { rows: new Uint16Array(2048), count: 0 };
    const result = filter.apply(vectorOf(type, slice), slice.length, null, out);
    if (result === null) slice.forEach((_, i) => kept.add(at + i));
    else for (let i = 0; i < result.count; i++) kept.add(at + (result.rows[i] ?? 0));
  }
  return kept;
}

const same = (a: Value, b: Value) =>
  a !== null &&
  b !== null &&
  (a === b ||
    (typeof a === "number" && typeof b === "number" && Number.isNaN(a) && Number.isNaN(b)));

describe("runtime join filters", () => {
  for (const seed of SEEDS)
    for (const [type, gen] of [
      [INT, (r: () => number) => Math.floor(r() * 5000)],
      [INT, (r: () => number) => Math.floor(r() * 2 ** 30)],
      [BIG, (r: () => number) => BigInt(Math.floor(r() * 2 ** 40)) * 3n],
      [
        DBL,
        (r: () => number) =>
          r() < 0.02 ? Number.NaN : r() < 0.02 ? -0 : Math.floor(r() * 1e6) / 7,
      ],
      [TEXT, (r: () => number) => `key-${String(Math.floor(r() * 1e6))}`],
    ] as const)
      it(`never drops a matching row: ${type.kind} (seed ${String(seed)})`, () => {
        const random = mulberry32(seed);
        const build: Value[] = Array.from({ length: 3000 }, () =>
          random() < 0.05 ? null : gen(random),
        );
        const filter = JoinRuntimeFilter.build(1, storeOf(type, build), 0, false, FREE, 2048);
        if (filter === null) throw new Error("refused");
        const probe: Value[] = Array.from({ length: 20_000 }, (_, i) =>
          random() < 0.05
            ? null
            : i % 4 === 0
              ? (build[Math.floor(random() * build.length)] ?? null)
              : gen(random),
        );
        const kept = apply(filter, type, probe);
        let misses = 0;
        let falsePositives = 0;
        probe.forEach((value, i) => {
          const matches = build.some((b) => same(b, value));
          if (matches)
            expect(kept.has(i), `row ${String(i)} (${String(value)}) dropped`).toBe(true);
          else {
            misses++;
            if (kept.has(i)) falsePositives++;
          }
          if (value === null) expect(kept.has(i)).toBe(false);
        });
        // ~12 bits per key: well under 3 % of misses survive.
        expect(falsePositives / Math.max(1, misses)).toBeLessThan(0.03);
      });

  it("is exact over a small dense integer domain and prunes pages by bounds", () => {
    const filter = JoinRuntimeFilter.build(2, storeOf(INT, [10, 12, 14, 40]), 0, false, FREE, 2048);
    expect(filter?.shape).toBe("exact");
    const kept = apply(
      defined(filter),
      INT,
      Array.from({ length: 500 }, (_, i) => i),
    );
    expect([...kept]).toEqual([10, 12, 14, 40]);
    expect(filter?.excludes(41, 900, 0, 0)).toBe(true);
    expect(filter?.excludes(0, 9, 0, 0)).toBe(true);
    expect(filter?.excludes(0, 10, 0, 0)).toBe(false);
  });

  it("keeps a probe NULL only under IS NOT DISTINCT FROM with a NULL on the build side", () => {
    const strict = JoinRuntimeFilter.build(1, storeOf(INT, [1, null]), 0, false, FREE, 2048);
    const safe = JoinRuntimeFilter.build(1, storeOf(INT, [1, null]), 0, true, FREE, 2048);
    const safeNoNull = JoinRuntimeFilter.build(1, storeOf(INT, [1]), 0, true, FREE, 2048);
    expect([...apply(defined(strict), INT, [null, 1])]).toEqual([1]);
    expect([...apply(defined(safe), INT, [null, 1])]).toEqual([0, 1]);
    expect([...apply(defined(safeNoNull), INT, [null, 1])]).toEqual([1]);
  });

  it("never prunes a page by bounds when a NULL or NaN on it could match (review B7-p2a #2)", () => {
    // IS NOT DISTINCT FROM, build [1, NULL]; probe page [2, NULL] has bounds [2, 2].
    const safe = defined(JoinRuntimeFilter.build(1, storeOf(INT, [1, null]), 0, true, FREE, 2048));
    expect(safe.excludes(2, 2, null, 0)).toBe(false);
    expect(safe.excludes(2, 2, 1, 0)).toBe(false);
    expect(safe.excludes(2, 2, 0, 0)).toBe(true);
    // A NaN in the build matches a NaN probe (NaN = NaN): bounds cannot exclude a page with NaN.
    const nan = defined(
      JoinRuntimeFilter.build(1, storeOf(DBL, [1.5, Number.NaN]), 0, false, FREE, 2048),
    );
    expect(nan.excludes(2, 3, 0, null)).toBe(false);
    expect(nan.excludes(2, 3, 0, 2)).toBe(false);
    expect(nan.excludes(2, 3, 0, 0)).toBe(true);
    // With `=`, a NULL never matches: nulls on the page do not keep it.
    const strict = defined(
      JoinRuntimeFilter.build(1, storeOf(INT, [1, null]), 0, false, FREE, 2048),
    );
    expect(strict.excludes(2, 2, null, null)).toBe(true);
  });

  it("an empty build excludes everything", () => {
    const filter = defined(JoinRuntimeFilter.build(1, storeOf(INT, []), 0, false, FREE, 2048));
    expect(apply(filter, INT, [1, 2, null]).size).toBe(0);
    expect(filter.excludes(-1e9, 1e9, 0, 0)).toBe(true);
  });

  it("stops testing once it keeps more than 90 % of probes", () => {
    const filter = defined(
      JoinRuntimeFilter.build(
        1,
        storeOf(
          INT,
          Array.from({ length: 1000 }, (_, i) => i),
        ),
        0,
        false,
        FREE,
        2048,
      ),
    );
    apply(
      filter,
      INT,
      Array.from({ length: DROP_AFTER_PROBES + 2048 }, (_, i) => i % 1000),
    );
    expect(filter.active).toBe(false);
    const out: Selection = { rows: new Uint16Array(4), count: 0 };
    expect(filter.apply(vectorOf(INT, [5000, 6000]), 2, null, out)).toBeNull();
  });

  it("is published only by joins whose probe misses are dropped, and not when the build spilled", async () => {
    expect(["inner", "semi", "right"].every((k) => kindPublishes(k as "inner"))).toBe(true);
    expect(["left", "full", "anti", "mark"].some((k) => kindPublishes(k as "inner"))).toBe(false);
    const spec: HashJoinSpec = {
      kind: "left",
      probeTypes: [INT],
      buildTypes: [INT],
      probeKeys: [0],
      buildKeys: [0],
      nullSafe: [false],
      residual: null,
      publishes: [{ id: 1, key: 0 }],
    };
    const governor = new Governor(64 << 20);
    const grant = await governor.admit({
      label: "j",
      minimumBytes: hashJoinMinimumBytes(spec),
      spillable: true,
    });
    expect(
      () => new HashJoin(spec, { grant, spill: new MemorySpillService(), queryId: 1 }),
    ).toThrow(TypeError);
    grant.close();

    for (const tight of [false, true]) {
      const inner: HashJoinSpec = { ...spec, kind: "inner", buildTypes: [INT, TEXT] };
      const minimum = hashJoinMinimumBytes(inner);
      const cap = tight
        ? Math.max(minimum + (1 << 20) + 64 * 1024, Math.ceil(((minimum + 64 * 1024) * 8) / 7))
        : 64 << 20;
      const g = new Governor(cap);
      const innerGrant = await g.admit({ label: "j", minimumBytes: minimum, spillable: true });
      const join = new HashJoin(inner, {
        grant: innerGrant,
        spill: new MemorySpillService(),
        queryId: 1,
      });
      const rows = Array.from({ length: 20_000 }, (_, i) => [i * 3, "x".repeat(40)] as const);
      await drive(new ArraySource(batchesOf([INT, TEXT], rows)), join.build);
      expect(join.runtimeFilters.length).toBe(tight ? 0 : 1);
      join.abort(new Error("done"));
      expect(g.usedBytes).toBe(0);
    }
  });
});
