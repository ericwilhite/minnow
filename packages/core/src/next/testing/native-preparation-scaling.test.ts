/** Preparation allocation scales no faster than its charge. For each listed grammar shape, the
 * statement is lowered at size N and at 8N under a counting copy meter. The charge has two parts:
 * the base share (linear in the text) and the metered copies. Each measure below must grow no
 * faster than the faster-growing part:
 * - allocation per site: every JS heap allocation during lowering, garbage included (objects
 *   collected by minor or major GC), sampled by V8's heap profiler through node:inspector every
 *   128 bytes, one run per size. Builtins (push, Set, Array.from, spread helpers) are
 *   attributed to the JS function that called them. Judging each site alone matters: a copy that
 *   is quadratic with a small constant (one spread of a name list per tree level) is a few
 *   hundred KiB inside megabytes of linear allocation, invisible in the total, but alone it grows
 *   about 64x per 8x. A site under 16 KiB at size N is judged against 16 KiB, so sampling noise
 *   and fixed costs do not count as growth. Slack 3: 1.5 as below, times 2 for container growth
 *   steps (a Map or Set doubles its table; an array grows by 1.5).
 * - array volume: elements produced by the array builtins (flatMap, flat, concat, slice, map,
 *   filter, Array.from), counted exactly, so the common copy-per-ancestor patterns fail
 *   deterministically as well. Slack 1.5.
 * A linear shape's sites grow about 8x against a limit near 3 x 8 = 24; a quadratic site grows
 * about 64x. Operator chains vary the operator and the operand type, so each per-operator typing
 * path is lowered. Shapes whose metered charge grows faster than linear (each join lists every
 * column of its inputs) have their sites judged at quadratic order, 64x: a cubic site fails
 * there, but an uncharged quadratic one cannot be told apart by scaling. Their absolute cover is
 * native-preparation-bound.test.ts's. */
// eslint-disable-next-line no-restricted-imports -- Test-only allocation sampling.
import { Session } from "node:inspector";
import fc from "fast-check";
// eslint-disable-next-line no-restricted-imports -- Test-only: deterministic tiers while sampling.
import { setFlagsFromString } from "node:v8";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREPARATION_SCALING_REACH } from "../../testing/reach.js";
import { withCopyMeter, type CopyMeter } from "../../engine/copy-meter.js";
import {
  PREPARATION_TEXT_BYTES,
  PREPARATION_TOKEN_BYTES,
  scanSql,
} from "../owner/native-preparation-bound.js";
import { StorageCore } from "./storage-core.js";

const SLACK = 1.5,
  SITE_SLACK = 2 * SLACK,
  FACTOR = 8,
  SITE_FLOOR = 16 << 10;

// Optimizing compilers make a site's bytes depend on history: TurboFan and Maglev inline callees
// (the sampling profiler then charges their allocations to the caller) and remove allocations by
// escape analysis, at a time set by what earlier shapes made hot. With both off, lowering runs in
// the interpreter and baseline tier, whose allocations follow the code alone; with constant
// sampling intervals the measure is then the same on every run and every Node version. Restored
// for any later file in this worker.
const DETERMINISTIC = [
  "--no-turbofan",
  "--no-maglev",
  "--sampling-heap-profiler-suppress-randomness",
] as const;
beforeAll(() => {
  for (const flag of DETERMINISTIC) setFlagsFromString(flag);
});
afterAll(() => {
  for (const flag of ["--turbofan", "--maglev", "--no-sampling-heap-profiler-suppress-randomness"])
    setFlagsFromString(flag);
});

/** Operator chains: the operator and its operand type, as one INSERT into a typed column. */
const CHAINS: ReadonlyArray<readonly [string, string, string, string]> = [
  ["int +", "n", "1", " + 1"],
  ["int -", "n", "1", " - 1"],
  ["int *", "n", "1", " * 1"],
  ["int /", "n", "1", " / 1"],
  ["numeric +", "x", "1.5", " + 1.5"],
  ["numeric -", "x", "1.5", " - 1.5"],
  ["numeric *", "x", "1.5", " * 1"],
  ["numeric /", "x", "1.5", " / 1.5"],
  ["text ||", "s", "'a'", " || 'a'"],
  ["boolean AND", "b", "true", " AND true"],
  ["boolean OR", "b", "true", " OR false"],
  // `=` and `<` do not associate in PostgreSQL, so each comparison is one AND/OR operand.
  ["comparison =", "b", "true", " AND 1 = 1"],
  ["comparison <", "b", "false", " OR 1 < 2"],
  ["date +", "d", "DATE '2020-01-01'", " + 1"],
  ["date -", "d", "DATE '2020-01-01'", " - 1"],
  ["timestamp +", "ts", "TIMESTAMP '2020-01-01 00:00:00'", " + INTERVAL '1 day'"],
  ["timestamp -", "ts", "TIMESTAMP '2020-01-01 00:00:00'", " - INTERVAL '1 day'"],
  ["interval +", "iv", "INTERVAL '1 day'", " + INTERVAL '1 day'"],
  ["interval -", "iv", "INTERVAL '1 day'", " - INTERVAL '1 day'"],
];

/** Statement text for one grammar shape at size n. */
const SHAPES: Readonly<Record<string, (n: number) => string>> = {
  ...Object.fromEntries(
    CHAINS.map(([name, column, first, next]) => [
      `chain ${name}`,
      (n: number) => `INSERT INTO log(${column}) VALUES (${first}${next.repeat(n)})`,
    ]),
  ),
  // Each DATE - DATE asks both operands' datetime kind (dateDifference), beside integer typing.
  "date difference chain": (n) =>
    `INSERT INTO log(n) VALUES ((DATE '2020-01-02' - DATE '2020-01-01')${" + (DATE '2020-01-02' - DATE '2020-01-01')".repeat(n)})`,
  "binary chain": (n) => `INSERT INTO log(n) VALUES (1${" + 1".repeat(n)})`,
  "column chain": (n) => `UPDATE t SET v = v${" + v".repeat(n)} WHERE id = 1`,
  "select column chain": (n) => `SELECT id${" + id".repeat(n)} FROM t`,
  "nested parentheses": (n) => `UPDATE t SET v = ${"(".repeat(n)}1${")".repeat(n)} WHERE id = 1`,
  "CASE arms": (n) =>
    `UPDATE t SET v = CASE ${Array.from({ length: n }, (_, i) => `WHEN v = ${String(i)} THEN ${String(i)}`).join(" ")} ELSE 0 END WHERE id = 1`,
  "IN list": (n) =>
    `DELETE FROM t WHERE id IN (${Array.from({ length: n }, (_, i) => String(i)).join(", ")})`,
  "row IN list": (n) =>
    `DELETE FROM t WHERE (id, v) IN (${Array.from({ length: n }, (_, i) => `(${String(i)}, 1)`).join(", ")})`,
  "row NOT IN list": (n) =>
    `DELETE FROM t WHERE (id, v) NOT IN (${Array.from({ length: n }, (_, i) => `(${String(i)}, 1)`).join(", ")})`,
  "function nesting": (n) =>
    `UPDATE t SET v = ${"coalesce(".repeat(n)}1${", 0)".repeat(n)} WHERE id = 1`,
  "subquery nesting": (n) =>
    `DELETE FROM t WHERE id IN ${"(SELECT k FROM u WHERE k IN ".repeat(n)}(1)${")".repeat(n)}`,
  "join chain": (n) =>
    `DELETE FROM t WHERE id IN (SELECT u0.k FROM u u0${Array.from({ length: n }, (_, i) => ` JOIN u u${String(i + 1)} ON u${String(i + 1)}.k = u${String(i)}.k`).join("")})`,
  "USING join chain": (n) =>
    `DELETE FROM t WHERE id IN (SELECT u0.k FROM u u0${Array.from({ length: n }, (_, i) => ` JOIN u u${String(i + 1)} USING (k)`).join("")})`,
  "grouping sets": (n) =>
    `DELETE FROM t WHERE id IN (SELECT k FROM u GROUP BY GROUPING SETS (${Array.from({ length: n }, () => "(k)").join(", ")}))`,
  "DEFAULT rows": (n) =>
    `INSERT INTO log(n, note) VALUES ${Array.from({ length: n }, (_, i) => `(${String(i)}, DEFAULT)`).join(", ")}`,
  "correlated select items": (n) =>
    `SELECT ${Array.from({ length: n }, () => "(SELECT max(u.w) FROM u WHERE u.k = t.id)").join(" + ")} FROM t`,
};

/** Nesting stays under the parser's 128-level limit at 8N. */
const NESTED = new Set(["nested parentheses", "function nesting", "subquery nesting"]);
/** Shapes whose plan grows with joins x columns run at quarter size, to keep the 8N run short. */
const JOINED = new Set(["join chain", "USING join chain", "correlated select items"]);
const sizeOf = (shape: string, n: number): number =>
  NESTED.has(shape) ? Math.min(n, 15) : JOINED.has(shape) ? n >> 2 : n;

const ARRAY_METHODS = ["flatMap", "flat", "concat", "slice", "map", "filter"] as const;

/** Elements produced by the array builtins while `run` executes (patched only meanwhile). */
function countArrayVolume(run: () => void): number {
  let volume = 0;
  const proto = Array.prototype as unknown as Record<string, (...args: unknown[]) => unknown>;
  const originals = ARRAY_METHODS.map((name) => {
    const original = proto[name];
    if (!original) throw new Error(`Missing Array.prototype.${name}`);
    return [name, original] as const;
  });
  const from = Array.from;
  for (const [name, original] of originals)
    proto[name] = function (this: unknown, ...args: unknown[]) {
      const result = original.apply(this, args);
      if (Array.isArray(result)) volume += result.length;
      return result;
    };
  Array.from = function (...args: Parameters<typeof Array.from>) {
    const result = from.apply(Array, args);
    volume += result.length;
    return result;
  } as typeof Array.from;
  try {
    run();
  } finally {
    for (const [name, original] of originals) proto[name] = original;
    Array.from = from;
  }
  return volume;
}

interface ProfileNode {
  readonly selfSize: number;
  readonly callFrame: {
    readonly functionName: string;
    readonly url: string;
    readonly lineNumber: number;
    readonly columnNumber: number;
  };
  readonly children: readonly ProfileNode[];
}
const session = new Session();
session.connect();
/** Posts synchronously: this session lives on the test's own thread. */
function post(method: string, params: object = {}): unknown {
  const state: { done: boolean; result: unknown; failure: Error | null } = {
    done: false,
    result: undefined,
    failure: null,
  };
  session.post(method, params, (error, value) => {
    state.done = true;
    state.failure = error;
    state.result = value;
  });
  if (!state.done) throw new Error(`${method} did not complete synchronously`);
  if (state.failure) throw state.failure;
  return state.result;
}
/** Adds the bytes one sampled run of `run` allocates to `sites`, per allocating JS function,
 * garbage included. A builtin's allocation is charged to the nearest JS caller. */
function sampleSites(sites: Map<string, number>, run: () => void): void {
  post("HeapProfiler.startSampling", {
    samplingInterval: 128,
    includeObjectsCollectedByMajorGC: true,
    includeObjectsCollectedByMinorGC: true,
  });
  try {
    run();
  } finally {
    const { profile } = post("HeapProfiler.stopSampling") as { profile: { head: ProfileNode } };
    const pending: Array<readonly [ProfileNode, string]> = [[profile.head, "(root)"]];
    for (let item = pending.pop(); item; item = pending.pop()) {
      const [node, owner] = item;
      const frame = node.callFrame;
      const site = frame.url
        ? `${frame.functionName || "(anonymous)"} ${frame.url.split("/").slice(-2).join("/")}:${String(frame.lineNumber + 1)}:${String(frame.columnNumber + 1)}`
        : owner;
      if (node.selfSize > 0) sites.set(site, (sites.get(site) ?? 0) + node.selfSize);
      for (const child of node.children) pending.push([child, site]);
    }
  }
}

interface Measure {
  readonly base: number;
  metered: number;
  volume: number;
  readonly sites: Map<string, number>;
}
/** Measures one statement at both sizes: first one counted run per size (charge and array
 * volume together), which also fills any one-time caches; then one sampled run per size. With
 * the optimizing tiers off a run's allocations do not depend on what ran before it, so neither
 * a warm-up nor repeated samples are needed. */
function measurePair(core: StorageCore, sizes: readonly [string, string]): [Measure, Measure] {
  const measures = sizes.map((sql): Measure => {
    const scan = scanSql(sql);
    return {
      base: PREPARATION_TOKEN_BYTES * scan.tokens + PREPARATION_TEXT_BYTES * scan.characters,
      metered: 0,
      volume: 0,
      sites: new Map(),
    };
  });
  const lowerer = (index: number, meter: CopyMeter) => () => {
    withCopyMeter(meter, () => core.lowerer.lower(at(sizes, index), core.context()));
  };
  for (const [index, measure] of measures.entries())
    measure.volume = countArrayVolume(
      lowerer(index, {
        charge(bytes) {
          measure.metered += bytes;
        },
      }),
    );
  const uncharged: CopyMeter = { charge: () => undefined };
  for (const [index, measure] of measures.entries())
    sampleSites(measure.sites, lowerer(index, uncharged));
  return [at(measures, 0), at(measures, 1)];
}

function at<T>(items: readonly T[], index: number): T {
  const item = items[index];
  if (item === undefined) throw new Error(`No item ${String(index)}`);
  return item;
}

for (const seed of seedsFor("next-native-preparation-scaling", [49741, 49742]))
  it(`keeps preparation allocation within its charge's scaling (${String(seed)})`, async () => {
    const reach = createReach("native preparation scaling", NATIVE_PREPARATION_SCALING_REACH);
    const core = new StorageCore(256);
    await core.open();
    try {
      for (const sql of [
        "CREATE TABLE t(id INT PRIMARY KEY, v INT)",
        "CREATE TABLE u(k INT PRIMARY KEY, w INT)",
        "CREATE TABLE log(n INT, note TEXT DEFAULT 'x', x NUMERIC, s TEXT, b BOOLEAN, d DATE, ts TIMESTAMP, iv INTERVAL)",
      ])
        await core.execute(sql);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...Object.keys(SHAPES)),
          fc.integer({ min: 32, max: 64 }),

          async (shape, n) => {
            reach.beginHistory();
            const text = SHAPES[shape];
            if (!text) throw new Error("Unknown shape");
            const size = sizeOf(shape, n);
            const [small, large] = measurePair(core, [text(size), text(FACTOR * size)]);
            // The faster-growing part of the charge: the base share or the metered copies.
            const meteredRatio = small.metered > 0 ? large.metered / small.metered : 0;
            const chargeRatio = Math.max(large.base / small.base, meteredRatio);
            // A superlinear metered charge holds a per-join term quadratic in the joins. Blended
            // with linear copies its ratio at these sizes understates that term's own growth, so
            // sites there are judged at quadratic order: a cubic site still fails.
            const siteRatio = meteredRatio > SLACK * FACTOR ? FACTOR * FACTOR : chargeRatio;
            expect(
              large.volume / Math.max(1, small.volume),
              `${shape}: array volume`,
            ).toBeLessThanOrEqual(SLACK * chargeRatio);
            for (const [site, bytes] of large.sites)
              expect(
                bytes / Math.max(SITE_FLOOR, small.sites.get(site) ?? 0),
                `${shape}: allocation at ${site} (${String(small.sites.get(site) ?? 0)} -> ${String(bytes)} bytes)`,
              ).toBeLessThanOrEqual(SITE_SLACK * siteRatio);
            reach.reach(shape as (typeof NATIVE_PREPARATION_SCALING_REACH)[number]);
            if (n >= 56) reach.reach("large size");
          },
        ),
        {
          seed,
          // Every shape runs once as an example at size 48; 15 generated runs follow.
          numRuns: Object.keys(SHAPES).length + 15,
          endOnFailure: true,
          examples: Object.keys(SHAPES).map((shape) => [shape, 48] as [string, number]),
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  }, 600000);
