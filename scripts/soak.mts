/**
 * The soak runner: the half of the testing story that finds bugs rather than pinning them.
 *
 * The generative suites run a fixed seed on every commit, which makes them a reliable regression
 * net and a frontier that never moves. This runs the same suites on seeds nobody has tried,
 * one after another, until something breaks or the budget runs out.
 *
 * A failure prints the seed. That seed is the whole artifact: it replays the exact corpus that
 * broke, and belongs in packages/core/regression-seeds.json under the suite that failed, where
 * the deterministic run picks it up forever after. This is the loop Turso's simulator runs on
 * (`--seed` to replay) and the reason SQLite's fuzzers keep a corpus of interesting cases.
 *
 *   npm run soak                      # 20 rounds of every generative suite
 *   npm run soak -- --rounds 200      # a longer sitting
 *   npm run soak -- --suite differential
 *   npm run soak -- --seed 123456     # replay one seed across every suite
 *
 * Nothing here writes to the repository. Recording a seed is a decision, made by committing it.
 */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";

interface Suite {
  readonly name: string;
  readonly file: string;
}

/** Every suite whose corpus is generated from a seed. A suite with a fixed corpus gains nothing here. */
const SUITES: Suite[] = [
  { name: "sql-conformance", file: "packages/core/src/engine/sql-conformance.test.ts" },
  {
    name: "sql-mutation-conformance",
    file: "packages/core/src/engine/sql-mutation-conformance.test.ts",
  },
  { name: "differential", file: "packages/core/src/engine/differential.test.ts" },
  { name: "deterministic-simulator", file: "packages/core/src/testing/simulator.test.ts" },
  { name: "compaction-soak", file: "packages/core/src/engine/compaction-soak.test.ts" },
  {
    name: "auto-compaction-soak",
    file: "packages/core/src/engine/auto-compaction-soak.test.ts",
  },
];

function argument(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  return index === -1 ? undefined : process.argv[index + 1];
}

const rounds = Number(argument("--rounds") ?? 20);
const only = argument("--suite");
const fixedSeed = argument("--seed");
const suites = only === undefined ? SUITES : SUITES.filter((suite) => suite.name === only);

if (suites.length === 0) {
  console.error(`Unknown suite: ${String(only)}. Known: ${SUITES.map((s) => s.name).join(", ")}`);
  process.exit(2);
}
if (!Number.isFinite(rounds) || rounds < 1) {
  console.error(`--rounds must be a positive number, got: ${String(argument("--rounds"))}`);
  process.exit(2);
}

/** Seeds already on file, so a soak spends its time on ground nobody has covered. */
const recorded = new Set<number>(
  Object.values(
    (
      JSON.parse(
        readFileSync(new URL("../packages/core/regression-seeds.json", import.meta.url), "utf8"),
      ) as { suites: Record<string, number[]> }
    ).suites,
  ).flat(),
);

function nextSeed(): number {
  for (;;) {
    // 2^31 keeps the value inside what the RNGs treat as a 32-bit signed seed.
    const seed = Math.floor(Math.random() * 0x7fffffff);
    if (!recorded.has(seed)) {
      recorded.add(seed);
      return seed;
    }
  }
}

function run(suite: Suite, seed: number): { passed: boolean; output: string } {
  const result = spawnSync("npx", ["vitest", "run", suite.file], {
    encoding: "utf8",
    env: { ...process.env, MINNOW_SEED: String(seed) },
  });
  return {
    passed: result.status === 0,
    output: `${result.stdout ?? ""}${result.stderr ?? ""}`,
  };
}

const started = Date.now();
let executed = 0;
const failures: Array<{ suite: string; seed: number; output: string }> = [];

console.log(
  fixedSeed === undefined
    ? `Soaking ${String(suites.length)} suite(s) for ${String(rounds)} round(s).`
    : `Replaying seed ${fixedSeed} across ${String(suites.length)} suite(s).`,
);

outer: for (let round = 0; round < rounds; round += 1) {
  for (const suite of suites) {
    const seed = fixedSeed === undefined ? nextSeed() : Number(fixedSeed);
    const { passed, output } = run(suite, seed);
    executed += 1;
    process.stdout.write(
      `${passed ? "ok  " : "FAIL"} ${suite.name.padEnd(26)} seed ${String(seed)}\n`,
    );
    if (!passed) {
      failures.push({ suite: suite.name, seed, output });
      // Stop at the first break. A soak that keeps going past a failure buries the one piece of
      // information it exists to produce.
      break outer;
    }
  }
}

const elapsed = ((Date.now() - started) / 1000).toFixed(1);
console.log(`\n${String(executed)} run(s) in ${elapsed}s.`);

if (failures.length === 0) {
  console.log("No failures. Nothing to record.");
  process.exit(0);
}

for (const failure of failures) {
  console.log(`\n${"=".repeat(78)}`);
  console.log(`FAILED: ${failure.suite} at seed ${String(failure.seed)}`);
  console.log(`${"=".repeat(78)}\n`);
  console.log(failure.output.split("\n").slice(-60).join("\n"));
  console.log(`\nReplay it:\n  MINNOW_SEED=${String(failure.seed)} npx vitest run\n`);
  console.log(
    `Record it in packages/core/regression-seeds.json:\n` +
      `  "${failure.suite}": [${String(failure.seed)}]\n`,
  );
}
process.exit(1);
