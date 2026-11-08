/** Isolated decoded highcard component; no write/scan/SQL CPU is timed. */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir, cpus } from "node:os";
import { join, resolve } from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import * as lib from "../packages/core/dist/next/exec/kernels/test-support.js";
import { aggShapes } from "../packages/core/browser/next-agg-shapes.ts";
import type { Rel } from "../packages/core/dist/next/ir/ir.js";
import { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import { lightComparison } from "./next-aggregation-light.mts";
const args = process.argv.slice(2);
const option = (n: string, f: string) => {
  const i = args.indexOf(n);
  return i < 0 ? f : (args[i + 1] ?? f);
};
const rows = Number(option("--rows", "1000000")),
  rounds = Number(option("--repeats", "9")),
  warmups = Number(option("--warmups", "3"));
if (
  !Number.isSafeInteger(rows) ||
  rows < 1 ||
  rows > 1000000 ||
  !Number.isSafeInteger(rounds) ||
  rounds < 5 ||
  !Number.isSafeInteger(warmups) ||
  warmups < 1 ||
  warmups > 100
)
  throw new RangeError("1..1M rows, >=5 rounds");
const output = resolve(option("--output", join(tmpdir(), "minnow-highcard-domain")));
await mkdir(output, { recursive: true });
const shape = aggShapes(lib, rows, ["agg_highcard_int"])[0];
if (!shape) throw new Error("shape");
const input = shape.input.map((type, i) => ({
  name: `c${String(i)}`,
  type,
  nullable: false,
}));
const rel: Extract<Rel, { op: "aggregate" }> = {
  id: 1,
  op: "aggregate",
  input: { id: 0, op: "values", rows: [], output: input },
  groupBy: shape.groupBy,
  groupingSets: null,
  aggregates: shape.aggregates,
  output: [
    ...shape.groupBy.map((i) => {
      const c = input[i];
      if (!c) throw new Error("key");
      return c;
    }),
    ...shape.aggregates.map((a, i) => ({
      name: `a${String(i)}`,
      type: a.type,
      nullable: a.fn !== "count_star",
    })),
  ],
};
const files = [
  "scripts/next-highcard-domain-bench.mts",
  "scripts/next-aggregation-light.mts",
  "scripts/lib/aggregation-light-kernels.mts",
  "packages/core/browser/next-agg-shapes.ts",
  "packages/core/dist/next/exec/agg/aggregate.js",
  "packages/core/dist/next/exec/agg/groups.js",
  "packages/core/dist/next/exec/agg/states.js",
  "packages/core/dist/next/exec/agg/group-top-n.js",
  ...(args.includes("--candidate")
    ? [
        "packages/core/dist/next/exec/agg/domain-count-sum.js",
        "packages/core/src/next/exec/agg/domain-count-sum.ts",
        "packages/core/src/next/exec/agg/domain-count-sum.test.ts",
      ]
    : []),
];
const metadata = {
  lane: "Node isolated decoded component",
  rows,
  rounds,
  warmups,
  operatorOnly: args.includes("--operator-only"),
  candidate: args.includes("--candidate"),
  node: process.version,
  cpu: cpus()[0]?.model,
  revision: execFileSync("git", ["rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim(),
  patchSha256: createHash("sha256")
    .update(execFileSync("git", ["diff", "HEAD", "--binary"]))
    .digest("hex"),
  sourceHashes: Object.fromEntries(
    await Promise.all(
      files.map(async (p) => [
        p,
        createHash("sha256")
          .update(await readFile(p))
          .digest("hex"),
      ]),
    ),
  ),
  certification:
    "actual min/max probe included in direct kernel CPU; wider domains fall back to exact hash",
  note: "plain hash, direct-address plain kernel, current operator; no executor direct-address plan enabled",
};
await writeFile(join(output, "metadata.json"), JSON.stringify(metadata, null, 2));
const core = await new StorageCore().open();
try {
  const result = await lightComparison(
    core,
    "agg_highcard_int",
    rel,
    shape.input,
    shape.batches,
    rounds,
    output,
    undefined,
    undefined,
    args.includes("--candidate") ? {} : undefined,
    {
      warmups,
      omitAliases: true,
      domainCountSum: args.includes("--candidate"),
      operatorOnly: args.includes("--operator-only"),
    },
  );
  await writeFile(join(output, "component.json"), JSON.stringify({ ...metadata, result }, null, 2));
  console.log(JSON.stringify(result.cpuMs));
} finally {
  await core.close();
}
