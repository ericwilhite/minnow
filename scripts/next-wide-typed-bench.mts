/** Isolated Gate B wide aggregate component: decoded inputs, exact same rows as gen.js.
 * No scan/storage/SQL CPU is attributed to this measurement. */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir, cpus } from "node:os";
import { join, resolve } from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { wideDecodedFixture } from "./lib/wide-decoded-fixture.mts";
import {
  I32,
  I64,
  F64,
  TEXT,
  TS,
  decimal,
  numbers,
  texts,
  dictionary,
  decimals,
  col,
} from "../packages/core/dist/next/exec/kernels/test-support.js";
import { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import { lightComparison } from "./next-aggregation-light.mts";

const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const index = args.indexOf(name);
  return index < 0 ? fallback : (args[index + 1] ?? fallback);
};
const rows = Number(option("--rows", "1000000"));
const rounds = Number(option("--repeats", "7"));
if (
  !Number.isSafeInteger(rows) ||
  rows < 1 ||
  rows > 1000000 ||
  !Number.isSafeInteger(rounds) ||
  rounds < 5
)
  throw new RangeError("1..1M rows and >=5 rounds required");
const strategy = option("--strategy", "both");
if (!["packed", "dictionary", "both"].includes(strategy)) throw new RangeError("unknown strategy");
const controlledGC = args.includes("--controlled-gc");
const collect = controlledGC ? globalThis.gc : undefined;
if (controlledGC && collect === undefined)
  throw new Error("--controlled-gc needs node --expose-gc");
const warmups = Number(option("--warmups", "3"));
if (!Number.isSafeInteger(warmups) || warmups < 3)
  throw new RangeError("at least three warmups required");
const nativeOnly = args.includes("--native-only");
const output = resolve(option("--output", join(tmpdir(), "minnow-wide-typed")));
await mkdir(output, { recursive: true });
const fixture = wideDecodedFixture(
  { I32, I64, F64, TEXT, TS, decimal, numbers, texts, dictionary, decimals, col },
  rows,
);
const sourcePaths = [
  "scripts/next-wide-typed-bench.mts",
  "scripts/lib/wide-decoded-fixture.mts",
  "scripts/next-aggregation-light.mts",
  "scripts/lib/aggregation-light-kernels.mts",
  "packages/core/browser/next-agg-shapes.ts",
  "packages/core/dist/next/exec/agg/aggregate.js",
  "packages/core/dist/next/exec/agg/states.js",
  "packages/core/dist/next/exec/agg/packed-averages.js",
  "packages/core/dist/next/exec/agg/packed-integers.js",
  "packages/core/dist/next/exec/agg/groups.js",
];
const hashes = Object.fromEntries(
  await Promise.all(
    sourcePaths.map(async (path) => [
      path,
      createHash("sha256")
        .update(await readFile(path))
        .digest("hex"),
    ]),
  ),
);
const metadata = {
  lane: "Node isolated component",
  strategy,
  controlledGC,
  warmups,
  nativeOnly,
  rows,
  rounds,
  node: process.version,
  cpu: cpus()[0]?.model,
  revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  patchSha256: createHash("sha256")
    .update(execFileSync("git", ["diff", "HEAD", "--binary"]))
    .digest("hex"),
  sourceHashes: hashes,
  fixture:
    "gen.js-equivalent values; 2048-row batches; first-seen page-local dictionaries; numeric widening is untimed setup",
};
await writeFile(join(output, "metadata.json"), JSON.stringify(metadata, null, 2));
const core = await new StorageCore().open();
try {
  const result = await lightComparison(
    core,
    "agg_3dims_20aggs",
    fixture.rel,
    fixture.input,
    fixture.batches,
    rounds,
    output,
    undefined,
    undefined,
    {
      packedAverages: strategy !== "dictionary",
      packedIntegers: strategy !== "dictionary",
      batchDictionaryMinMax: strategy !== "packed",
    },
    { ...(collect === undefined ? {} : { collect }), warmups, nativeOnly },
  );
  await writeFile(join(output, "component.json"), JSON.stringify({ ...metadata, result }, null, 2));
  console.log(JSON.stringify(result.cpuMs));
} finally {
  await core.close();
}
