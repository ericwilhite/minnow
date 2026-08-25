import { spawnSync } from "node:child_process";
import {
  parsePerformanceFailures,
  performanceFailureKey,
  repeatedPerformanceFailures,
} from "./lib/performance-confirmation.mts";

interface GateRun {
  readonly status: number;
  readonly output: string;
}

const gateArguments = process.argv.slice(2);

function runGate(): GateRun {
  const result = spawnSync(
    "npm",
    ["run", "benchmark:gate:single", ...(gateArguments.length > 0 ? ["--", ...gateArguments] : [])],
    { encoding: "utf8" },
  );
  const stdout = result.stdout ?? "";
  const stderr = result.stderr ?? "";
  process.stdout.write(stdout);
  process.stderr.write(stderr);
  if (result.error !== undefined) {
    process.stderr.write(`Unable to run the performance gate: ${result.error.message}\n`);
  }
  return { status: result.status ?? 1, output: `${stdout}\n${stderr}` };
}

function main(): number {
  const first = runGate();
  if (first.status === 0) return 0;

  const firstFailures = parsePerformanceFailures(first.output);
  // Baseline updates are a single calibration run. An absent structured regression means the gate
  // failed before completing its measurements, which must never be dismissed as runner noise.
  if (gateArguments.includes("--update") || firstFailures.length === 0) return first.status;

  console.log("\nConfirming the flagged comparisons in a fresh process...");
  const second = runGate();
  if (second.status === 0) {
    console.log(
      `\nThe first-run flag did not repeat: ${firstFailures.map(performanceFailureKey).join(", ")}.`,
    );
    return 0;
  }

  const secondFailures = parsePerformanceFailures(second.output);
  if (secondFailures.length === 0) return second.status;

  const repeated = repeatedPerformanceFailures(firstFailures, secondFailures);
  if (repeated.length === 0) {
    console.log(
      "\nThe two runs flagged different comparisons; no repeatable performance regression was found.",
    );
    return 0;
  }

  console.error("\nRepeatable performance regression(s):");
  for (const failure of repeated) console.error(`  ${performanceFailureKey(failure)}`);
  return 1;
}

process.exitCode = main();
