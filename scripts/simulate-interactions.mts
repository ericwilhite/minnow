/**
 * Replayable interaction-plan simulator over the in-process stores.
 *
 * Generates a plan from a seed (or replays a JSON plan), runs it through real SQL over the
 * chosen block store, and prints the counters a run produced. A failure prints the interaction,
 * the property, the recent SQL, and the exact command to replay it.
 */
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { IDBFactory } from "fake-indexeddb";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
} from "../packages/core/src/storage/index.js";
import {
  createDatabaseDriver,
  generateInteractionPlan,
  parseInteractionPlan,
  runInteractionPlan,
  type InteractionPlan,
} from "../packages/core/src/testing/interaction-simulator.js";
import { MemoryOpfs } from "../packages/core/src/testing/opfs-shim.js";

const values = new Map<string, string>();
const known = new Set([
  "--seed",
  "--length",
  "--connections",
  "--tables",
  "--key-space",
  "--store",
  "--plan",
  "--write-plan",
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
const generationFlags = ["--seed", "--length", "--connections", "--tables", "--key-space"].filter(
  (flag) => values.has(flag),
);
if (planFile !== undefined && generationFlags.length > 0) {
  failUsage(`--plan cannot be combined with ${generationFlags.join(", ")}`);
}
const storeName = values.get("--store") ?? "memory";
if (!["memory", "indexeddb", "opfs"].includes(storeName))
  failUsage("--store must be memory, indexeddb, or opfs");

const plan = await loadPlan(planFile);
const writePlan = values.get("--write-plan");
if (writePlan !== undefined) {
  await writeFile(resolve(writePlan), `${JSON.stringify(plan, undefined, 2)}\n`, "utf8");
}
const replay =
  planFile === undefined
    ? `npm run simulate:interactions -- --seed ${String(plan.seed)} --length ${String(numericOption("--length", 300, 1, 100_000))} ` +
      `--connections ${String(plan.connections)} --tables ${String(numericOption("--tables", 3, 1, 8))} ` +
      `--key-space ${String(numericOption("--key-space", 24, 2, 10_000))} --store ${storeName}`
    : `npm run simulate:interactions -- --plan ${JSON.stringify(planFile)} --store ${storeName}`;

const store =
  storeName === "memory"
    ? new MemoryBlockStore()
    : storeName === "indexeddb"
      ? await IndexedDbBlockStore.open({
          name: "simulate-interactions",
          indexedDB: new IDBFactory(),
        })
      : await OpfsBlockStore.open({ name: "simulate-interactions", root: new MemoryOpfs().root });
try {
  const started = performance.now();
  const result = await runInteractionPlan(
    plan,
    createDatabaseDriver(store, { databaseOptions: { rowsPerBlock: 8 } }),
  );
  const elapsed = ((performance.now() - started) / 1_000).toFixed(2);
  console.log(
    `ok interaction simulator seed ${String(result.seed)} over ${storeName} (${elapsed}s)`,
  );
  console.log(
    `${String(result.interactions)} interactions; ${String(result.statements)} statements; ${String(result.queries)} queries; ` +
      `${String(result.acceptedWrites)} accepted writes; ${String(result.rejectedConflicts)} conflicts; ` +
      `${String(result.expectedFailures)} expected refusals; ${String(result.faultsInjected)} faults injected; ` +
      `${String(result.faultsSkipped)} faults skipped; ${String(result.reopens)} reopens; ${String(result.checkpoints)} checkpoints`,
  );
  console.log(
    `${String(result.tablesAtEnd)} tables and ${String(result.rowsAtEnd)} rows at the end`,
  );
  console.log(`replay: ${replay}`);
} catch (error) {
  console.error(`FAIL interaction simulator seed ${String(plan.seed)} over ${storeName}`);
  console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
  console.error(`replay: ${replay}`);
  process.exitCode = 1;
} finally {
  store.close();
}

async function loadPlan(file: string | undefined): Promise<InteractionPlan> {
  if (file !== undefined) return parseInteractionPlan(await readFile(resolve(file), "utf8"));
  return generateInteractionPlan(numericOption("--seed", 24_301, 0, 0x7fff_ffff), {
    length: numericOption("--length", 300, 1, 100_000),
    connections: numericOption("--connections", 4, 1, 16),
    tables: numericOption("--tables", 3, 1, 8),
    keySpace: numericOption("--key-space", 24, 2, 10_000),
  });
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
  console.log(`Usage: npm run simulate:interactions -- [options]

Generate and run:
  --seed NUMBER          Plan seed (default: 24301)
  --length NUMBER        Interactions to generate (default: 300)
  --connections NUMBER   Connections sharing the database (default: 4)
  --tables NUMBER        Tables the plan may hold at once (default: 3)
  --key-space NUMBER     Primary-key space per table (default: 24)
  --store NAME           memory, indexeddb, or opfs (default: memory)

Replay:
  --plan FILE            Replay a JSON plan instead of generating one
  --write-plan FILE      Save the plan used by this run
  --help                 Show this help`);
}
