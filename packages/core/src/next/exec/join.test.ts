/**
 * The hash join against a nested-loop oracle with SQL semantics (join-fixtures.ts
 * `oracleJoin`), over generated tables with duplicates, NULLs and every join kind, resident and
 * spilled (tight grants force grace partitioning, recursion and chunked heavy hitters), with and
 * without downstream backpressure. join.pglite.test.ts holds the same oracle to PostgreSQL.
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import {
  ArraySource,
  BIG,
  CollectSink,
  DBL,
  INT,
  TEXT,
  batchesOf,
  canonical,
  oracleJoin,
  residualOf,
  runJoin,
  type Row,
  type RunOptions,
  type Value,
  defined,
} from "./join-fixtures.js";
import { Governor, ValueTooLargeError } from "../memory/governor.js";
import { SpillCorruptionError, type SpillService } from "../spill/contract.js";
import { MemorySpillFile, MemorySpillService } from "../spill/memory-spill.js";
import type { Batch } from "../vector/vector.js";
import { HASH_SEED, fmix32, levelHash } from "./join-keys.js";
import { HashJoin, hashJoinMinimumBytes } from "./join.js";
import { Charge, Pool } from "./spill-pool.js";
import { drive, type Operator } from "./pipeline.js";
import type { HashJoinKind, HashJoinSpec } from "./join.js";

const KINDS: readonly HashJoinKind[] = ["inner", "left", "right", "full", "semi", "anti", "mark"];
const SEEDS = seedsFor("next-hash-join", [0xb7, 0x10_1a, 0x5eed]);

function spec(
  kind: HashJoinKind,
  probeTypes: readonly PhysicalType[],
  buildTypes: readonly PhysicalType[],
  keys: number,
  extra: Partial<HashJoinSpec> = {},
): HashJoinSpec {
  return {
    kind,
    probeTypes,
    buildTypes,
    probeKeys: Array.from({ length: keys }, (_, i) => i),
    buildKeys: Array.from({ length: keys }, (_, i) => i),
    nullSafe: new Array<boolean>(keys).fill(false),
    residual: null,
    publishes: [],
    ...extra,
  };
}

async function check(
  s: HashJoinSpec,
  build: readonly Row[],
  probe: readonly Row[],
  options: RunOptions = {},
  residual?: (p: Row, b: Row) => boolean | null,
) {
  const full: HashJoinSpec =
    residual === undefined ? s : { ...s, residual: residualOf(s.probeTypes.length, residual) };
  const result = await runJoin(full, build, probe, options);
  const expected = oracleJoin(
    {
      kind: s.kind,
      probeKeys: s.probeKeys,
      buildKeys: s.buildKeys,
      nullSafe: s.nullSafe,
      ...(s.correlated === undefined ? {} : { correlated: s.correlated }),
      ...(residual === undefined ? {} : { residual }),
      probeWidth: s.probeTypes.length,
      buildWidth: s.buildTypes.length,
    },
    build,
    probe,
  );
  expect(canonical(result.rows)).toEqual(canonical(expected));
  expect(result.governor.usedBytes).toBe(0);
  expect(result.spill.bytesInUse).toBe(0);
  return result;
}

describe("hash join: examples", () => {
  const build: Row[] = [
    [1, "a"],
    [2, "b"],
    [2, "b2"],
    [null, "n"],
    [4, "d"],
  ];
  const probe: Row[] = [
    [1, 10],
    [2, 20],
    [3, 30],
    [null, 40],
  ];
  for (const kind of KINDS)
    it(kind, async () => {
      await check(spec(kind, [INT, INT], [INT, TEXT], 1), build, probe);
    });

  it("NOT IN with a NULL in the subquery is never TRUE", async () => {
    const s = spec("mark", [INT], [INT], 1, { correlated: 0 });
    const result = await check(s, [[1], [null]], [[1], [2], [null]]);
    expect(canonical(result.rows)).toEqual(
      canonical([
        [1, true],
        [2, null],
        [null, null],
      ]),
    );
  });

  it("composite IN is exact per column: (1, 2) IN (SELECT 3, NULL) is FALSE", async () => {
    const s = spec("mark", [INT, INT], [INT, INT], 2, { correlated: 0 });
    const probe: Row[] = [
      [1, 2],
      [3, 5],
      [3, null],
    ];
    const result = await check(s, [[3, null]], probe);
    expect(canonical(result.rows)).toEqual(
      canonical([
        [1, 2, false],
        [3, 5, null],
        [3, null, null],
      ]),
    );
  });

  it("a mark's NULL residual makes the match UNKNOWN (ruling R20.1)", async () => {
    const s = spec("mark", [INT, INT], [INT, INT], 1, { correlated: 0 });
    // Residual: probe payload < build payload, NULL when the build payload is NULL.
    const residual = (p: Row, b: Row) =>
      b[1] === null || p[1] === null ? null : (p[1] as number) < (b[1] as number);
    const result = await check(
      s,
      [
        [1, null],
        [2, 10],
        [2, 0],
      ],
      [
        [1, 5],
        [2, 5],
        [3, 5],
      ],
      {},
      residual,
    );
    expect(canonical(result.rows)).toEqual(
      canonical([
        [1, 5, null],
        [2, 5, true],
        [3, 5, false],
      ]),
    );
  });

  it("a mark's filter is the subquery's WHERE: rows where it is NULL take no part (R20.5)", async () => {
    // p(x = 1, w = 0), b(y = 1, z = NULL): x NOT IN (SELECT y FROM b WHERE b.z > p.w) is TRUE
    // in PostgreSQL, so the mark (x IN …) is FALSE. As a residual it would be NULL.
    const s = spec("mark", [INT, INT], [INT, INT], 1, { correlated: 0 });
    const where = (p: Row, b: Row) =>
      b[1] === null || p[1] === null ? null : (b[1] as number) > (p[1] as number);
    const asFilter = await runJoin({ ...s, filter: residualOf(2, where) }, [[1, null]], [[1, 0]]);
    expect(canonical(asFilter.rows)).toEqual(canonical([[1, 0, false]]));
    const asResidual = await runJoin(
      { ...s, residual: residualOf(2, where) },
      [[1, null]],
      [[1, 0]],
    );
    expect(canonical(asResidual.rows)).toEqual(canonical([[1, 0, null]]));
    expect(
      () =>
        new HashJoin(
          { ...spec("semi", [INT], [INT], 1), filter: residualOf(1, where) },
          {} as never,
        ),
    ).toThrow("Only a mark join has a filter");
  });

  it("1 NOT IN (SELECT 1 WHERE NULL::int > 0) is TRUE: the WHERE excludes the row (review #5)", async () => {
    const s = spec("mark", [INT], [INT, INT], 1, { correlated: 0 });
    const where = (_p: Row, b: Row) => (b[1] === null ? null : (b[1] as number) > 0);
    const result = await runJoin({ ...s, filter: residualOf(1, where) }, [[1, null]], [[1]]);
    expect(canonical(result.rows)).toEqual(canonical([[1, false]]));
  });

  it("IN over an empty set is FALSE even for a NULL", async () => {
    const s = spec("mark", [INT], [INT], 1, { correlated: 0 });
    const result = await check(s, [], [[1], [null]]);
    expect(canonical(result.rows)).toEqual(
      canonical([
        [1, false],
        [null, false],
      ]),
    );
  });

  it("float keys: -0 equals 0 and NaN equals NaN across sides", async () => {
    const build: Row[] = [
      [0, "pos"],
      [-0, "neg"],
      [Number.NaN, "nan"],
      [1.5, "x"],
    ];
    const probe: Row[] = [
      [-0, 1],
      [0, 2],
      [Number.NaN, 3],
      [2.5, 4],
    ];
    for (const kind of KINDS) await check(spec(kind, [DBL, INT], [DBL, TEXT], 1), build, probe);
  });

  it("text keys whose hashes collide are still different keys", async () => {
    // xxHash32 with the join's seed maps both strings to 1871889161 (found by search).
    const build: Row[] = [
      ["k52703", 1],
      ["k122690", 2],
      ["k52703", 3],
    ];
    const probe: Row[] = [
      ["k122690", 10],
      ["k52703", 20],
      ["k0", 30],
    ];
    for (const kind of KINDS) await check(spec(kind, [TEXT, INT], [TEXT, INT], 1), build, probe);
  });

  it("dense integer keys use direct addressing", async () => {
    const build = Array.from({ length: 5000 }, (_, i): Row => [i, i * 2]);
    const probe = Array.from({ length: 6000 }, (_, i): Row => [(i * 7) % 5500, i]);
    const result = await check(spec("inner", [INT, INT], [INT, INT], 1), build, probe);
    expect(result.join.profile.table).toBe("direct");
  });

  it("sparse keys hash", async () => {
    const build = Array.from({ length: 3000 }, (_, i): Row => [i * 1009, i]);
    const probe = Array.from({ length: 4000 }, (_, i): Row => [(i % 3500) * 1009, i]);
    const result = await check(spec("left", [INT, INT], [INT, INT], 1), build, probe);
    expect(result.join.profile.table).toBe("words");
  });
});

/** Random tables: small key domains (duplicates), nulls, the given key types. */
function generate(
  random: () => number,
  keyTypes: readonly PhysicalType[],
  rows: number,
  domain: number,
  nullRate: number,
): Row[] {
  const out: Row[] = [];
  for (let i = 0; i < rows; i++) {
    const row: Value[] = keyTypes.map((type) => {
      if (random() < nullRate) return null;
      const v = Math.floor(random() * domain);
      switch (type.kind) {
        case "int64":
          return BigInt(v) * 1_000_000_007n - 5n;
        case "float64":
          return v === 0 ? -0 : v === 1 ? Number.NaN : v / 4;
        case "text":
          return `k${String(v)}`;
        default:
          return v;
      }
    });
    row.push(i);
    out.push(row);
  }
  return out;
}

const KEY_SETS: ReadonlyArray<readonly PhysicalType[]> = [[INT], [BIG], [DBL], [TEXT], [INT, TEXT]];

describe("hash join: generated against the oracle", () => {
  for (const seed of SEEDS)
    for (const keyTypes of KEY_SETS)
      it(`seed ${String(seed)} keys ${keyTypes.map((t) => t.kind).join("+")}`, async () => {
        const random = mulberry32(seed ^ keyTypes.length);
        for (const kind of KINDS) {
          const n = keyTypes.length;
          const types = [...keyTypes, INT];
          const build = generate(random, keyTypes, 1 + Math.floor(random() * 300), 40, 0.1);
          const probe = generate(random, keyTypes, 1 + Math.floor(random() * 300), 50, 0.1);
          const nullSafe = keyTypes.map(() => kind !== "mark" && random() < 0.3);
          const s = spec(kind, types, types, n, {
            nullSafe,
            ...(kind === "mark" ? { correlated: Math.floor(random() * n) } : {}),
          });
          const options: RunOptions = {
            batchRows: 1 + Math.floor(random() * 300),
            blockEvery: random() < 0.5 ? 1 + Math.floor(random() * 3) : 0,
            skip: random() < 0.3 ? 3 : undefined,
            int64: random() < 0.5 ? "int64" : "safe",
            dictionary: random() < 0.3,
          } as RunOptions;
          await check(s, build, probe, options);
          // With a residual: probe payload < build payload + 150 (NULL never: payloads are set).
          await check(s, build, probe, options, (p, b) => {
            const x = p[n] as number;
            const y = b[n] as number;
            return x % 7 === 3 ? null : x < y + 150;
          });
        }
      });
});

describe("hash join: spill", () => {
  for (const seed of SEEDS)
    for (const kind of KINDS)
      it(`seed ${String(seed)} ${kind} under a tight grant`, async () => {
        const random = mulberry32(seed + 17);
        const types = [INT, TEXT, INT];
        const rows = (count: number, domain: number): Row[] =>
          Array.from({ length: count }, (_, i): Row => {
            const k = random() < 0.02 ? null : Math.floor(random() * domain);
            return [k, `payload-${String(i)}-${"x".repeat(i % 40)}`, i];
          });
        const build = rows(12_000, 9000);
        const probe = rows(9000, 12_000);
        const s = spec(kind, types, types, 1, kind === "mark" ? { correlated: 0 } : {});
        const result = await check(s, build, probe, {
          cap: "tight",
          slack: 256 * 1024,
          blockEvery: 2,
        });
        expect(result.join.profile.spilledBytes).toBeGreaterThan(0);
      });

  it("a heavy hitter key is processed in chunks", async () => {
    const types = [INT, TEXT];
    // One key, several chunks of build rows (more than the pool and grant hold at once); two
    // probe rows hit it, so semi/anti/outer state must carry across the chunks.
    const build: Row[] = Array.from({ length: 30_000 }, (_, i): Row => [
      7,
      `b${String(i)}-${"y".repeat(100)}`,
    ]);
    const probe: Row[] = Array.from({ length: 40 }, (_, i): Row => [
      i === 0 || i === 21 ? 7 : i + 100,
      `p${String(i)}`,
    ]);
    for (const kind of ["inner", "left", "full", "semi", "anti"] as const) {
      const result = await check(spec(kind, types, types, 1), build, probe, {
        cap: "tight",
        slack: 128 * 1024,
      });
      expect(result.join.profile.chunkPasses).toBeGreaterThan(1);
    }
  });

  it("values larger than the pool progress with one loan per row-sized step (review #1)", async () => {
    // 450 KiB values: each spill step (stage a row, read its page, load it, emit it) needs room
    // the minimum grant does not have, and each takes a loan of about one row, never a page × 4.
    const types = [INT, TEXT];
    const big = (k: number, i: number): Row => [k, `${String(i)}-${"v".repeat(450 * 1024)}`];
    const build: Row[] = Array.from({ length: 10 }, (_, i) => big(i % 3, i));
    const probe: Row[] = [big(0, 100), big(1, 101), [9, "miss"]];
    for (const kind of ["inner", "left", "full", "anti"] as const) {
      const result = await check(spec(kind, types, types, 1), build, probe, {
        cap: "tight",
        slack: 0,
      });
      expect(result.join.profile.spilledBytes).toBeGreaterThan(0);
      expect(result.join.profile.loans).toBeGreaterThan(0);
      expect(result.join.profile.largestLoan).toBeLessThan(result.governor.reserveBytes);
      // One-row pages go down a level whole: one copy of a wide row, not three.
      expect(result.join.profile.forwardedPages).toBeGreaterThan(0);
    }
  }, 120_000);

  it("recursion splits a partition that is still too large", async () => {
    // 90 % of the build lands in level-0 partition 0 (chosen by hash), so that partition cannot
    // fit and must split again one level down with independent hash bits.
    const types = [INT, TEXT];
    const hot: number[] = [];
    const cold: number[] = [];
    for (let k = 0; hot.length < 18_000 || cold.length < 2000; k++) {
      const partition = levelHash(fmix32((k ^ HASH_SEED) >>> 0), 0) >>> 28;
      if (partition === 0 && hot.length < 18_000) hot.push(k);
      else if (partition !== 0 && cold.length < 2000) cold.push(k);
    }
    const keys = [...hot, ...cold];
    const build: Row[] = keys.map((k, i): Row => [k, `v${String(i)}-${"z".repeat(50)}`]);
    const probe: Row[] = keys
      .filter((_, i) => i % 2 === 0)
      .map((k, i): Row => [k, `p${String(i)}`]);
    const result = await check(spec("inner", types, types, 1), build, probe, {
      cap: "tight",
      slack: 64 * 1024,
      direct: false,
    });
    expect(result.join.profile.spillPasses).toBeGreaterThan(16);
    expect(result.join.profile.chunkPasses).toBe(0);
  });
});

describe("hash join: R23.1 two joins interleaved on one worker", () => {
  it("spilled joins with different key types keep their own scratch", async () => {
    // Both spill, block on their sinks and resume interleaved: shared module scratch (key
    // extraction, the mark join's compatibility test) would mix their rows.
    const random = mulberry32(0x23_1);
    const ints = (count: number, domain: number): Row[] =>
      Array.from({ length: count }, (_, i): Row => [
        random() < 0.05 ? null : Math.floor(random() * domain),
        random() < 0.05 ? null : Math.floor(random() * 4),
        `i${String(i)}-${"x".repeat(i % 30)}`,
      ]);
    const texts = (count: number, domain: number): Row[] =>
      Array.from({ length: count }, (_, i): Row => [
        random() < 0.05 ? null : `k${String(Math.floor(random() * domain))}`,
        random() < 0.05 ? null : `c${String(Math.floor(random() * 4))}`,
        i,
      ]);
    const options = { cap: "tight", slack: 128 * 1024, blockEvery: 1 } as const;
    const a = spec("mark", [INT, INT, TEXT], [INT, INT, TEXT], 2, { correlated: 1 });
    const b = spec("mark", [TEXT, TEXT, INT], [TEXT, TEXT, INT], 2, { correlated: 1 });
    const c = spec("full", [INT, INT, TEXT], [INT, INT, TEXT], 2);
    const results = await Promise.all([
      check(a, ints(6000, 3000), ints(5000, 3000), options),
      check(b, texts(6000, 3000), texts(5000, 3000), options),
      check(c, ints(6000, 3000), ints(5000, 3000), options),
    ]);
    for (const result of results) expect(result.join.profile.spilledBytes).toBeGreaterThan(0);
  }, 120_000);
});

describe("hash join: lifecycle", () => {
  const types = [INT, TEXT];
  const rows = (n: number): Row[] =>
    Array.from({ length: n }, (_, i): Row => [i % 5000, `r${String(i)}-${"w".repeat(40)}`]);

  async function setup(s: HashJoinSpec, cap: number | "tight", spill = new MemorySpillService()) {
    const minimum = hashJoinMinimumBytes(s);
    const bytes =
      cap === "tight"
        ? Math.max(minimum + (1 << 20) + 65_536, Math.ceil(((minimum + 65_536) * 8) / 7))
        : cap;
    const governor = new Governor(bytes);
    const grant = await governor.admit({ label: "j", minimumBytes: minimum, spillable: true });
    return { governor, spill, join: new HashJoin(s, { grant, spill, queryId: 7 }) };
  }

  it("runs synchronously end to end when nothing spills", async () => {
    const { governor, join } = await setup(spec("inner", types, types, 1), 64 << 20);
    // No task yields (they are the driver's, tested in pipeline.test.ts): the join never awaits.
    const options = { yieldIntervalMs: Infinity };
    expect(
      drive(new ArraySource(batchesOf(types, rows(3000))), join.build, undefined, options),
    ).toBeUndefined();
    const sink = new CollectSink();
    expect(
      drive(new ArraySource(batchesOf(types, rows(3000))), join.probe(sink), undefined, options),
    ).toBeUndefined();
    expect(sink.finished).toBe(true);
    expect(governor.usedBytes).toBe(0);
  });

  it("refuses a probe before the build finished", async () => {
    const { join } = await setup(spec("inner", types, types, 1), 64 << 20);
    const probe = join.probe(new CollectSink());
    const [batch] = batchesOf(types, rows(10));
    expect(() => probe.push(defined(batch))).toThrow(TypeError);
    join.abort(new Error("stop"));
  });

  it("an abort during a spilled probe releases memory and spill space", async () => {
    const { governor, spill, join } = await setup(spec("left", types, types, 1), "tight");
    await drive(new ArraySource(batchesOf(types, rows(20_000))), join.build);
    expect(join.profile.spilledBytes).toBeGreaterThan(0);
    const controller = new AbortController();
    const sink = new CollectSink(1);
    let pushes = 0;
    const counting: Operator = {
      push: (batch: Batch) => {
        if (++pushes === 3) controller.abort(new Error("cancelled"));
        return sink.push(batch);
      },
      ready: () => sink.ready(),
      finish: () => {
        sink.finish();
      },
      abort: (reason: unknown) => {
        sink.abort(reason);
      },
    };
    await expect(
      drive(
        new ArraySource(batchesOf(types, rows(20_000))),
        join.probe(counting),
        controller.signal,
      ),
    ).rejects.toThrow("cancelled");
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  });

  it("a cancel while a spill file is being created leaks no spill space (review B7-p2a #3)", async () => {
    const service = new MemorySpillService();
    let resume: (() => void) | null = null;
    let entered: (() => void) | null = null;
    const gate = new Promise<void>((resolve) => (resume = resolve));
    const started = new Promise<void>((resolve) => (entered = resolve));
    const slow: SpillService = {
      get bytesInUse() {
        return service.bytesInUse;
      },
      sweep: () => service.sweep(),
      create: async (queryId) => {
        entered?.();
        await gate;
        return service.create(queryId);
      },
    };
    const { governor, join } = await setup(
      spec("inner", types, types, 1),
      "tight",
      slow as MemorySpillService,
    );
    const running = Promise.resolve(
      drive(new ArraySource(batchesOf(types, rows(12_000))), join.build),
    ).then(
      () => "finished",
      (error: unknown) => String(error),
    );
    await started;
    join.abort(new Error("cancel"));
    (resume as (() => void) | null)?.();
    expect(await running).toMatch(/cancel|closed/);
    await join.closed;
    expect(governor.usedBytes).toBe(0);
    expect(service.bytesInUse).toBe(0);
  });

  it("a spill file that fails to close fails the query, and the failure is kept", async () => {
    const service = new MemorySpillService();
    const failing: SpillService = {
      get bytesInUse() {
        return service.bytesInUse;
      },
      sweep: () => service.sweep(),
      create: async (queryId) => {
        const file = await service.create(queryId);
        const close = file.close.bind(file);
        file.close = async () => {
          await close();
          throw new Error("close failed");
        };
        return file;
      },
    };
    const { join } = await setup(
      spec("inner", types, types, 1),
      "tight",
      failing as MemorySpillService,
    );
    const run = async () => {
      await drive(new ArraySource(batchesOf(types, rows(20_000))), join.build);
      await drive(new ArraySource(batchesOf(types, rows(2000))), join.probe(new CollectSink()));
    };
    await expect(run()).rejects.toThrow("close failed");
    await expect(join.closed).rejects.toThrow("close failed");
    expect(String(join.cleanupError)).toMatch("close failed");
  });

  it("a close failure after an abort is kept, not dropped", async () => {
    const service = new MemorySpillService();
    const failing: SpillService = {
      get bytesInUse() {
        return service.bytesInUse;
      },
      sweep: () => service.sweep(),
      create: async (queryId) => {
        const file = await service.create(queryId);
        const close = file.close.bind(file);
        file.close = async () => {
          await close();
          throw new Error("close failed");
        };
        return file;
      },
    };
    const { join } = await setup(
      spec("inner", types, types, 1),
      "tight",
      failing as MemorySpillService,
    );
    await drive(new ArraySource(batchesOf(types, rows(20_000))), join.build);
    expect(join.profile.spilledBytes).toBeGreaterThan(0);
    join.abort(new Error("cancel"));
    await expect(join.closed).rejects.toThrow("close failed");
    await Promise.resolve();
    expect(String(join.cleanupError)).toMatch("close failed");
  });

  it("a corrupt spill page fails the query and releases everything", async () => {
    const service = new MemorySpillService();
    const files: MemorySpillFile[] = [];
    const corrupting: SpillService = {
      get bytesInUse() {
        return service.bytesInUse;
      },
      sweep: () => service.sweep(),
      create: async (queryId, budget) => {
        void budget;
        const file = (await service.create(queryId)) as MemorySpillFile;
        files.push(file);
        const append = file.append.bind(file);
        file.append = async (page) => {
          const ref = await append(page);
          if (files.length === 3) file.corrupt(ref, 40);
          return ref;
        };
        return file;
      },
    };
    const { governor, join } = await setup(
      spec("inner", types, types, 1),
      "tight",
      corrupting as MemorySpillService,
    );
    const run = async () => {
      await drive(new ArraySource(batchesOf(types, rows(20_000))), join.build);
      await drive(new ArraySource(batchesOf(types, rows(20_000))), join.probe(new CollectSink()));
    };
    await expect(run()).rejects.toBeInstanceOf(SpillCorruptionError);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(governor.usedBytes).toBe(0);
    expect(service.bytesInUse).toBe(0);
  });
});

describe("hash join: the spill pool's progress loans", () => {
  async function pool(minimum: number) {
    // cap − reserve = the minimum: the grant cannot grow, so past the pool's size is a loan.
    const governor = new Governor(minimum + (1 << 20));
    const grant = await governor.admit({ label: "pool", minimumBytes: minimum, spillable: true });
    const charge = new Charge(grant);
    return { governor, grant, pool: new Pool(charge, minimum) };
  }

  it("lends exactly a step's shortfall, holds it for the step, repays when its usage leaves", async () => {
    const { governor, grant, pool: p } = await pool(256 * 1024);
    expect(p.allocate(200 * 1024)).toBe(true);
    await p.need(100 * 1024, grant);
    expect(p.loans).toBe(1);
    expect(p.largestLoan).toBe(44 * 1024);
    // Before the step allocates, a reset (free + settle) must not give the room back.
    p.free(10 * 1024);
    p.settle();
    expect(grant.loaned).toBe(44 * 1024);
    expect(p.allocate(100 * 1024)).toBe(true);
    // Within the step the room may grow once more from the grant only: a second loan is refused.
    await expect(p.need(200 * 1024, grant)).rejects.toThrow("second progress loan");
    p.free(100 * 1024);
    expect(grant.loaned).toBe(0);
    p.free(190 * 1024);
    p.close();
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("refuses a step larger than the reserve: the governor's one size error", async () => {
    const { grant, pool: p } = await pool(64 * 1024);
    await expect(p.need(4 << 20, grant)).rejects.toBeInstanceOf(ValueTooLargeError);
    p.close();
    grant.close();
  });
});
