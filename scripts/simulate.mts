/**
 * Replayable deterministic database simulator.
 *
 * The plan fixes both the workload and the completion schedule. Generated plans are compact JSON
 * artifacts, so a failing CI seed can be replayed exactly or checked in as a regression fixture.
 */
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import {
  generateSimulationPlan,
  parseSimulationPlan,
  runSimulation,
  type SimulationPlan,
} from "../packages/core/src/testing/simulator.js";

const values = new Map<string, string>();
const known = new Set([
  "--seed",
  "--rounds",
  "--clients",
  "--key-space",
  "--plan",
  "--write-plan",
  "--trace-events",
]);
for (let index = 2; index < process.argv.length; index++) {
  const flag = process.argv[index];
  if (flag === "--help") {
    printHelp();
    process.exit(0);
  }
  if (flag === undefined || !known.has(flag)) failUsage(`Unknown option: ${String(flag)}`);
  const value = process.argv[++index];
  if (value === undefined || value.startsWith("--")) failUsage(`Missing value for ${flag}`);
  if (values.has(flag)) failUsage(`Option can only be used once: ${flag}`);
  values.set(flag, value);
}

const planFile = values.get("--plan");
const generationFlags = ["--seed", "--rounds", "--clients", "--key-space"].filter((flag) =>
  values.has(flag),
);
if (planFile !== undefined && generationFlags.length > 0) {
  failUsage(`--plan cannot be combined with ${generationFlags.join(", ")}`);
}

const plan = await loadPlan(planFile);
const writePlan = values.get("--write-plan");
if (writePlan !== undefined) {
  await writeFile(resolve(writePlan), `${JSON.stringify(plan, undefined, 2)}\n`, "utf8");
}

const replay =
  planFile === undefined
    ? `npm run simulate -- --seed ${String(plan.seed)} --rounds ${String(generatedNumber("--rounds", 96))} ` +
      `--clients ${String(plan.clients)} --key-space ${String(plan.keySpace)}`
    : `npm run simulate -- --plan ${JSON.stringify(planFile)}`;

try {
  const started = performance.now();
  const result = await runSimulation(plan, {
    traceEvents: numericOption("--trace-events", 256, 0, 100_000),
  });
  const elapsed = ((performance.now() - started) / 1_000).toFixed(2);
  console.log(`ok deterministic simulator seed ${String(result.seed)} (${elapsed}s)`);
  console.log(
    `${String(result.acceptedMutations)} accepted mutations; ` +
      `${String(result.rejectedConflicts)} conflicts; ${String(result.injectedFaults)} faults; ` +
      `${String(result.checkpoints)} checkpoints; ` +
      `${String(result.maintenanceCompactions)} final compactions; ` +
      `${String(result.collectionPasses)} final collection passes`,
  );
  console.log(
    `${String(result.rows.length)} live rows; ${String(result.blockCount)} blocks; ` +
      `${String(result.segmentCount)} segments; ${String(result.transactionCount)} transactions; ` +
      `${String(result.manifestCount)} manifests; ${String(result.leaseCount)} leases; ` +
      `${String(result.storageBytes)} logical bytes`,
  );
  console.log(
    `scheduler high-water ${String(result.schedulerHighWater)}; ` +
      `bounded trace ${String(result.trace.length)} events; digest ${result.traceDigest}`,
  );
  console.log(`replay: ${replay}`);
} catch (error) {
  console.error(`FAIL deterministic simulator seed ${String(plan.seed)}`);
  console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
  console.error(`replay: ${replay}`);
  process.exitCode = 1;
}

async function loadPlan(file: string | undefined): Promise<SimulationPlan> {
  if (file !== undefined) return parseSimulationPlan(await readFile(resolve(file), "utf8"));
  return generateSimulationPlan(numericOption("--seed", 24_301, 0, 0x7fff_ffff), {
    rounds: generatedNumber("--rounds", 96),
    clients: numericOption("--clients", 4, 1, 32),
    keySpace: numericOption("--key-space", 32, 1, 10_000),
  });
}

function generatedNumber(flag: string, fallback: number): number {
  return numericOption(flag, fallback, 1, 100_000);
}

function numericOption(flag: string, fallback: number, minimum: number, maximum: number): number {
  const source = values.get(flag);
  if (source === undefined) return fallback;
  const value = Number(source);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    failUsage(`${flag} must be a whole number from ${String(minimum)} through ${String(maximum)}`);
  }
  return value;
}

function failUsage(message: string): never {
  console.error(message);
  console.error("Run with --help for usage.");
  process.exit(2);
}

function printHelp(): void {
  console.log(`Usage: npm run simulate -- [options]

Generate and run:
  --seed NUMBER          Workload and scheduler seed (default: 24301)
  --rounds NUMBER        Concurrent workload rounds (default: 96)
  --clients NUMBER       Independent database clients (default: 4)
  --key-space NUMBER     Bounded primary-key space (default: 32; at least clients)

Replay and diagnostics:
  --plan FILE            Replay a JSON plan instead of generating one
  --write-plan FILE      Save the validated plan used by this run
  --trace-events NUMBER  Retained diagnostic tail (default: 256; 0 disables it)
  --help                 Show this help`);
}
