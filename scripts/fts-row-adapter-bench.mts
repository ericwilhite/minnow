/** FTS row-helper component: supply true-parent/candidate ESM bundles exporting `fts` (or helpers directly). */
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { loadavg } from "node:os";
import { pairedCPU } from "./lib/paired-cpu.mts";
type Functions = Pick<
  typeof import("../packages/core/dist/engine/fts.js"),
  "tokenize" | "tokenizeQuery" | "cachedQueryTerms" | "ftsMatchTruth" | "ftsBm25Row"
>;
async function load(path: string | undefined): Promise<Functions> {
  if (!path)
    throw new Error("Usage: node scripts/fts-row-adapter-bench.mts parent.mjs candidate.mjs");
  const imported: unknown = await import(pathToFileURL(resolve(path)).href);
  if (!imported || typeof imported !== "object") throw new Error("Not an FTS module");
  const source: unknown = "fts" in imported ? imported.fts : imported;
  if (!source || typeof source !== "object") throw new Error("Missing FTS exports");
  for (const name of [
    "tokenize",
    "tokenizeQuery",
    "cachedQueryTerms",
    "ftsMatchTruth",
    "ftsBm25Row",
  ])
    if (typeof Reflect.get(source, name) !== "function") throw new Error(`Missing ${name}`);
  return source as Functions;
}
const parent = await load(process.argv[2]),
  candidate = await load(process.argv[3]);
const terms = parent.tokenizeQuery("alpha al*"),
  stats = { docCount: 100, totalTokens: 600, dfByTerm: [50, 70] };
let sink = 0;
const cases: Record<string, (f: Functions) => number> = {
  tokenize: (f) => f.tokenize("Alpha alpine beta 東京タワー 42").length,
  cache: (f) => f.cachedQueryTerms("alpha al*").length,
  match: (f) => Number(f.ftsMatchTruth(terms, ["alpha", "alpine"])),
  matchNull: (f) => Number(f.ftsMatchTruth(terms, [null, null])),
  bm25: (f) => f.ftsBm25Row(terms, ["alpha", "alpine"], stats) ?? 0,
  bm25Null: (f) => f.ftsBm25Row(terms, [null, null], stats) ?? 0,
};
let regression = false;
for (const [name, fn] of Object.entries(cases)) {
  const n = name === "cache" ? 1000000 : 150000;
  if (!Object.is(fn(parent), fn(candidate))) throw new Error(`${name}: differing helper result`);
  const run = (f: Functions) => {
    const load = loadavg(),
      cpu = process.cpuUsage(),
      wall = performance.now();
    for (let i = 0; i < n; i++) sink += fn(f);
    const used = process.cpuUsage(cpu);
    return { cpuMs: (used.user + used.system) / 1000, wallMs: performance.now() - wall, load };
  };
  for (let i = 0; i < 4; i++) {
    run(parent);
    run(candidate);
  }
  const pairs = [];
  for (let i = 0; i < 10; i++) {
    let a, b;
    if (i % 2) {
      b = run(candidate);
      a = run(parent);
    } else {
      a = run(parent);
      b = run(candidate);
    }
    pairs.push({ parent: a, candidate: b });
  }
  const decision = pairedCPU(
    pairs.map((p) => p.parent.cpuMs),
    pairs.map((p) => p.candidate.cpuMs),
  );
  console.log(JSON.stringify({ name, iterations: n, pairs, cpu: decision }));
  regression ||= decision.significantRegression;
}
console.log(JSON.stringify({ sink, significantRegression: regression }));
if (regression) process.exitCode = 1;
