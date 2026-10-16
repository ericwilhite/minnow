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
  type DriverStoreSource,
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
  "--rekeying",
  "--constraints",
  "--schema-changes",
  "--full-text",
  "--burst-commits",
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
const generationFlags = [
  "--seed",
  "--length",
  "--connections",
  "--tables",
  "--key-space",
  "--rekeying",
  "--constraints",
  "--schema-changes",
  "--full-text",
  "--burst-commits",
].filter((flag) => values.has(flag));
const rekeying = booleanOption("--rekeying");
const constraints = booleanOption("--constraints");
const schemaChangesFlag = values.get("--schema-changes") ?? "false";
if (!["true", "false", "populated"].includes(schemaChangesFlag)) {
  failUsage("--schema-changes must be true, false, or populated");
}
const schemaChanges: boolean | "populated" =
  schemaChangesFlag === "populated" ? "populated" : schemaChangesFlag === "true";
const fullText = booleanOption("--full-text");
const burstCommits = values.has("--burst-commits")
  ? numericOption("--burst-commits", 0, 1, 100_000)
  : undefined;
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
      `--key-space ${String(numericOption("--key-space", 24, 2, 10_000))}` +
      `${rekeying ? " --rekeying true" : ""}${constraints ? " --constraints true" : ""}` +
      `${schemaChanges === false ? "" : ` --schema-changes ${String(schemaChanges)}`}${fullText ? " --full-text true" : ""}` +
      `${burstCommits === undefined ? "" : ` --burst-commits ${String(burstCommits)}`} --store ${storeName}`
    : `npm run simulate:interactions -- --plan ${JSON.stringify(planFile)} --store ${storeName}`;

// Memory has no durable identity to share, so its connections share one instance. IndexedDB and
// OPFS open a separate adapter per connection over one database, as separate tabs do, and as
// the Vitest suite does: a cross-tab cache or a recovery path that another tab's records
// confuse only shows that way.
const memory = storeName === "memory" ? new MemoryBlockStore() : undefined;
const indexedDB = new IDBFactory();
const opfsRoot = new MemoryOpfs().root;
const source: DriverStoreSource =
  memory ??
  (storeName === "indexeddb"
    ? () => IndexedDbBlockStore.open({ name: "simulate-interactions", indexedDB })
    : () => OpfsBlockStore.open({ name: "simulate-interactions", root: opfsRoot }));
try {
  const started = performance.now();
  const result = await runInteractionPlan(
    plan,
    createDatabaseDriver(source, {
      // A low full-text threshold lets MATCH build the index the append-only tables can hold.
      databaseOptions: {
        rowsPerBlock: 8,
        ...(plan.interactions.some((interaction) => interaction.kind === "match")
          ? { ftsAutoIndexRows: 8 }
          : {}),
      },
    }),
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
    `${String(result.tablesAtEnd)} tables and ${String(result.rowsAtEnd)} rows at the end; ` +
      `${String(result.indexPathQueries)} index-path lookups; ${String(result.burstCommits)} burst commits ` +
      `(peak ${String(result.peakBurstSegments)} visible segments)`,
  );
  console.log(`replay: ${replay}`);
} catch (error) {
  console.error(`FAIL interaction simulator seed ${String(plan.seed)} over ${storeName}`);
  console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
  console.error(`replay: ${replay}`);
  process.exitCode = 1;
} finally {
  memory?.close();
}

async function loadPlan(file: string | undefined): Promise<InteractionPlan> {
  if (file !== undefined) return parseInteractionPlan(await readFile(resolve(file), "utf8"));
  return generateInteractionPlan(numericOption("--seed", 24_301, 0, 0x7fff_ffff), {
    length: numericOption("--length", 300, 1, 100_000),
    connections: numericOption("--connections", 4, 1, 16),
    tables: numericOption("--tables", 3, 1, 8),
    keySpace: numericOption("--key-space", 24, 2, 10_000),
    rekeying,
    constraints,
    schemaChanges,
    fullText,
    ...(burstCommits === undefined ? {} : { burstCommits }),
  });
}

function booleanOption(flag: string): boolean {
  const value = values.get(flag) ?? "false";
  if (value !== "true" && value !== "false") failUsage(`${flag} must be true or false`);
  return value === "true";
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
  --rekeying true|false  Add primary-key changes and self-references (default: false)
  --constraints true|false
                         Add UNIQUE columns and indexes, upserts on them, enforced foreign
                         keys, and index-shaped lookups (default: false)
  --schema-changes true|false|populated
                         Add ALTER TABLE ADD/DROP/RENAME COLUMN and CREATE/DROP INDEX
                         interleaved with the writes; populated also adds and drops columns
                         on tables that hold rows (default: false)
  --full-text true|false Add MATCH queries and append-only tables they index (default: false)
  --burst-commits NUMBER Add bursts of up to this many single-row commits to one table
  --store NAME           memory, indexeddb, or opfs (default: memory)

Replay:
  --plan FILE            Replay a JSON plan instead of generating one
  --write-plan FILE      Save the plan used by this run
  --help                 Show this help`);
}
