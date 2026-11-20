/** Preparation allocation scales no faster than its charge. For each grammar shape the trigger
 * preparation path can lower, the statement is lowered at size N and at 4N under a counting copy
 * meter, and two measures must grow no faster than the charge (base share plus metered copies):
 * - array volume: elements produced by the array builtins during lowering (flatMap, flat,
 *   concat, slice, map, filter, Array.from), counted deterministically. A pass that copies every
 *   descendant's list again at each ancestor is quadratic here even though its garbage is short
 *   lived, so this catches it without depending on GC timing;
 * - heap growth after lowering returns (median of three gc'd samples), for object-heavy paths.
 * Slack: growth ratio <= 1.5 x charge ratio. A quadratic bypass at 4N gives a ratio near 16
 * against a linear charge ratio near 4, far outside it. */
// eslint-disable-next-line no-restricted-imports -- Test-only GC access for heap samples.
import { setFlagsFromString } from "node:v8";
// eslint-disable-next-line no-restricted-imports -- Test-only GC access for heap samples.
import { runInNewContext } from "node:vm";
import fc from "fast-check";
import { expect, it } from "vitest";
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
  FACTOR = 4;

/** Statement text for one grammar shape at size n. */
const SHAPES: Readonly<Record<string, (n: number) => string>> = {
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

const NESTED = new Set(["nested parentheses", "function nesting", "subquery nesting"]);

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

let gc: () => void = () => undefined;
function heapGrowth(run: () => void): number {
  const samples: number[] = [];
  for (let i = 0; i < 3; i++) {
    gc();
    const before = process.memoryUsage().heapUsed;
    run();
    samples.push(Math.max(0, process.memoryUsage().heapUsed - before));
  }
  samples.sort((a, b) => a - b);
  return samples[1] ?? 0;
}

interface Measure {
  readonly charge: number;
  readonly volume: number;
  readonly heap: number;
}
function measure(core: StorageCore, sql: string): Measure {
  const scan = scanSql(sql);
  let charge = PREPARATION_TOKEN_BYTES * scan.tokens + PREPARATION_TEXT_BYTES * scan.characters;
  const meter: CopyMeter = {
    charge(bytes) {
      charge += bytes;
    },
  };
  const lower = () => {
    withCopyMeter(meter, () => core.lowerer.lower(sql, core.context()));
  };
  lower();
  const once = charge;
  const volume = countArrayVolume(lower);
  return { charge: once, volume, heap: heapGrowth(lower) };
}

for (const seed of seedsFor("next-native-preparation-scaling", [49741, 49742]))
  it(`keeps preparation allocation within its charge's scaling (${String(seed)})`, async () => {
    setFlagsFromString("--expose-gc");
    gc = runInNewContext("gc") as () => void;
    const reach = createReach("native preparation scaling", NATIVE_PREPARATION_SCALING_REACH);
    const core = new StorageCore(256);
    await core.open();
    try {
      for (const sql of [
        "CREATE TABLE t(id INT PRIMARY KEY, v INT)",
        "CREATE TABLE u(k INT PRIMARY KEY, w INT)",
        "CREATE TABLE log(n INT, note TEXT DEFAULT 'x')",
      ])
        await core.execute(sql);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...Object.keys(SHAPES)),
          fc.integer({ min: 24, max: 48 }),

          async (shape, n) => {
            reach.beginHistory();
            const text = SHAPES[shape];
            if (!text) throw new Error("Unknown shape");
            // Nesting shapes stay under the parser's 128-level limit at the larger size.
            const size = NESTED.has(shape) ? Math.min(n, 30) : n;
            const small = measure(core, text(size)),
              large = measure(core, text(FACTOR * size));
            const chargeRatio = large.charge / small.charge;
            expect(
              large.volume / Math.max(1, small.volume),
              `${shape}: array volume`,
            ).toBeLessThanOrEqual(SLACK * chargeRatio);
            // Heap growth below a few hundred KiB is all noise; scale is judged above it.
            if (large.heap > 1 << 20)
              expect(
                large.heap / Math.max(256 << 10, small.heap),
                `${shape}: heap growth`,
              ).toBeLessThanOrEqual(SLACK * chargeRatio);
            reach.reach(shape as (typeof NATIVE_PREPARATION_SCALING_REACH)[number]);
            if (n >= 40) reach.reach("large size");
          },
        ),
        {
          seed,
          numRuns: 30,
          endOnFailure: true,
          examples: Object.keys(SHAPES).map((shape) => [shape, 32] as [string, number]),
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  }, 600000);
