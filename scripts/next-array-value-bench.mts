/**
 * Array NUMERIC refusal and valid-value API costs, over preconstructed inputs.
 * node scripts/next-array-value-bench.mts --a <parent-dist> --b <candidate-dist> [--repeats 7]
 * CPU milliseconds, alternating A/B and B/A; dataset construction is outside every timer.
 */
import assert from "node:assert/strict";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { PhysicalType } from "../packages/core/dist/next/types.js";
import type { ArrayValue } from "../packages/core/dist/next/vector/array-value.js";

type Codec = typeof import("../packages/core/dist/next/vector/array-value.js");
const args = process.argv.slice(2);
const option = (name: string): string | undefined => {
  const at = args.indexOf(name);
  return at < 0 ? undefined : args[at + 1];
};
const repeats = Number(option("--repeats") ?? 7);
if (!Number.isInteger(repeats) || repeats < 5)
  throw new RangeError("At least five rounds required");
async function load(flag: string): Promise<Codec> {
  const directory = option(flag);
  if (directory === undefined) throw new Error(`Missing ${flag} <dist-directory>`);
  return (await import(
    pathToFileURL(path.resolve(directory, "next/vector/array-value.js")).href
  )) as Codec;
}
const a = await load("--a");
const b = await load("--b");
const type: PhysicalType = { kind: "decimal", precision: null, scale: null };
const magnitude = 10n ** 2_000_000n;
const huge: ArrayValue[] = [magnitude, -magnitude, magnitude + 1n, -magnitude - 1n].map(
  (coefficient) => ({
    dimensions: [1],
    lowerBounds: [1],
    elements: [{ coefficient, scale: 0 }],
  }),
);
const ordinary: ArrayValue = {
  dimensions: [4],
  lowerBounds: [1],
  elements: [
    { coefficient: 12345n, scale: 2 },
    { coefficient: -990n, scale: 1 },
    { coefficient: 1n, scale: 16383 },
    null,
  ],
};
assert.deepEqual(a.encodeArrayValue(type, ordinary), b.encodeArrayValue(type, ordinary));
for (const codec of [a, b])
  assert.deepEqual(codec.decodeArrayValue(type, codec.encodeArrayValue(type, ordinary)), ordinary);
const cases = [
  {
    name: "oversized refusal (40 validations)",
    once(codec: Codec) {
      for (let i = 0; i < 40; i++) {
        let refused = false;
        try {
          codec.validateArrayValue(type, huge[i % huge.length] as ArrayValue);
        } catch (error) {
          if (!(error instanceof codec.MalformedArrayError)) throw error;
          refused = true;
        }
        assert(refused);
      }
    },
  },
  {
    name: "valid arrays (1000 encode/decode round trips)",
    once(codec: Codec) {
      for (let i = 0; i < 1000; i++)
        codec.decodeArrayValue(type, codec.encodeArrayValue(type, ordinary));
    },
  },
];
function summary(runs: number[]) {
  const sorted = [...runs].sort((x, y) => x - y);
  return {
    median: sorted[Math.floor(sorted.length / 2)] ?? 0,
    spread: (sorted[sorted.length - 1] ?? 0) - (sorted[0] ?? 0),
    runs,
  };
}
const results = [];
for (const component of cases) {
  for (let w = 0; w < 2; w++) {
    component.once(a);
    component.once(b);
  }
  const runs = { a: [] as number[], b: [] as number[] };
  for (let round = 0; round < repeats; round++)
    for (const key of round % 2 === 0 ? (["a", "b"] as const) : (["b", "a"] as const)) {
      const before = process.cpuUsage();
      component.once(key === "a" ? a : b);
      const spent = process.cpuUsage(before);
      runs[key].push((spent.user + spent.system) / 1000);
    }
  const result = { component: component.name, a: summary(runs.a), b: summary(runs.b) };
  results.push(result);
  console.log(JSON.stringify(result));
}
console.log(JSON.stringify({ cpuMilliseconds: true, repeats, results }));
