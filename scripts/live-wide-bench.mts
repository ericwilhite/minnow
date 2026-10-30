/**
 * The wide live-query bench: what one large live query costs from subscribe to rows on the page.
 *
 * The shape is an application's grid query: 15,933 rows, 13 joined tables, 63 columns, 12 of
 * them `JSON_OBJECT` display columns built from the joins, decoded through Kysely with
 * `resultDecoding: { numeric: "number", json: "parse" }`. The database runs in a real worker
 * thread on the stock worker host, so the result crosses a real message port, and each thread
 * samples its own allocations — garbage included, since a startup burst is mostly garbage.
 *
 * Three lifecycles, each on a fresh live query:
 *   - `subscribe`: one subscriber, one delivery. The per-run cost of the query.
 *   - `suspense`: what React Suspense does in production: `refresh()` loads the query before any
 *     subscriber exists, then the component commits and subscribes.
 *   - `suspense-strict`: the same under StrictMode, which subscribes, unsubscribes, and
 *     subscribes again on mount. Simulated in one turn, this can understate what real React does
 *     (it also lets the first subscription open); `packages/react/src/suspense.test.ts` counts
 *     engine subscriptions under real Suspense and StrictMode.
 *
 *   npm run benchmark:live-wide -- [--rounds 3] [--rows 15933] [--top]
 *
 * Runs on the packages' built `dist`, as an application would; the npm script builds it first.
 * Nothing here writes to the repository.
 */
import { MessageChannel, Worker } from "node:worker_threads";
import { Session } from "node:inspector/promises";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { MinnowDialect, createKyselyLiveQueries } from "@minnowdb/kysely";
import { CompiledQuery, Kysely, type KyselyPlugin } from "kysely";
import { startAllocationSampling, type AllocationReport } from "./lib/allocation-sampling.mts";

function argument(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

const ROUNDS = Number(argument("--rounds") ?? 3);
const TICKETS = Number(argument("--rows") ?? 15_933);
const SHOW_TOP = process.argv.includes("--top");
/** How long without a delivery counts as settled: longer than one execution of the query. */
const QUIET_MS = 3000;
const MB = (bytes: number): string => (bytes / 1_048_576).toFixed(1);

// Deterministic data: a small xorshift generator, so every run measures the same rows.
let seed = 0x9e3779b9;
function random(): number {
  seed ^= seed << 13;
  seed ^= seed >>> 17;
  seed ^= seed << 5;
  return (seed >>> 0) / 4_294_967_296;
}
const pick = <T,>(values: readonly T[]): T => values[Math.floor(random() * values.length)] as T;
const words = ["north", "gravel", "river", "base", "stone", "crushed", "fill", "sand", "west"];
const phrase = (count: number): string =>
  Array.from({ length: count }, () => pick(words)).join(" ");
const key = (prefix: string, index: number): string =>
  `${prefix}-${index.toString(16).padStart(8, "0")}-4000-8000-${Math.floor(random() * 1e12)
    .toString(16)
    .padStart(12, "0")}`;

interface Lookup {
  readonly table: string;
  readonly keyColumn: string;
  readonly count: number;
  readonly columns: Readonly<Record<string, () => string | number>>;
}

const LOOKUPS: readonly Lookup[] = [
  {
    table: "regions",
    keyColumn: "regionKey",
    count: 6,
    columns: { regionName: () => phrase(1), regionDescription: () => phrase(3) },
  },
  {
    table: "locations",
    keyColumn: "locationKey",
    count: 40,
    columns: { locationName: () => phrase(2), locationDescription: () => phrase(4) },
  },
  {
    table: "yards",
    keyColumn: "yardKey",
    count: 30,
    columns: { yardName: () => phrase(2), yardDescription: () => phrase(4) },
  },
  {
    table: "customers",
    keyColumn: "customerKey",
    count: 952,
    columns: {
      customerID: () => `C${String(Math.floor(random() * 99_999))}`,
      customerName: () => phrase(3),
    },
  },
  {
    table: "orders",
    keyColumn: "orderKey",
    count: 900,
    columns: {
      orderNumber: () => `O-${String(Math.floor(random() * 999_999))}`,
      description: () => phrase(5),
    },
  },
  {
    table: "requests",
    keyColumn: "requestKey",
    count: 500,
    columns: {
      info: () => phrase(6),
      referenceNum: () => `R${String(Math.floor(random() * 99_999))}`,
    },
  },
  {
    table: "products",
    keyColumn: "productKey",
    count: 400,
    columns: {
      productID: () => `P${String(Math.floor(random() * 9_999))}`,
      productDescription: () => phrase(4),
      unitOfMeasure: () => pick(["TON", "YD", "LOAD"]),
    },
  },
  {
    table: "haulers",
    keyColumn: "haulerKey",
    count: 300,
    columns: {
      haulerID: () => `H${String(Math.floor(random() * 9_999))}`,
      haulerName: () => phrase(3),
    },
  },
  {
    table: "shipTos",
    keyColumn: "shipToKey",
    count: 800,
    columns: { description: () => phrase(5) },
  },
  {
    table: "taxCodes",
    keyColumn: "taxCodeKey",
    count: 40,
    columns: { code: () => `T${String(Math.floor(random() * 999))}`, description: () => phrase(3) },
  },
];
const AMOUNTS = [
  "ticketMaterialRate",
  "ticketFreightRate",
  "ticketSurchargeRate",
  "materialAmount",
  "freightAmount",
  "surchargeAmount",
  "materialTax",
  "freightTax",
  "surchargeTax",
  "materialTax2",
  "freightTax2",
  "surchargeTax2",
  "subTotal",
  "taxTotal",
  "grandTotal",
];
const TEXTS = [
  "invoiceKey",
  "invoiceNum",
  "job",
  "note",
  "operator",
  "phase",
  "ticketNumber",
  "ticketPONumber",
  "udf1",
  "udf2",
  "udf3",
  "pdfBlobKey",
  "imageBlobKey",
  "signatureBlobKey",
];
const WEIGHTS = ["grossWeight", "netWeight", "tareWeight"];

/** A row as `insertBatch` takes it. */
type Row = Record<string, boolean | number | string | Date | null>;

async function seed_(client: MinnowDatabaseClient): Promise<void> {
  const keys = new Map<string, string[]>();
  for (const lookup of LOOKUPS) {
    const columns = Object.keys(lookup.columns).map((name) => `"${name}" TEXT`);
    await client.execute(
      `CREATE TABLE "${lookup.table}" ("${lookup.keyColumn}" TEXT PRIMARY KEY, ${columns.join(", ")})`,
    );
    const rows = Array.from({ length: lookup.count }, (_, index) => {
      const row: Record<string, string | number> = { [lookup.keyColumn]: key(lookup.table, index) };
      for (const [name, value] of Object.entries(lookup.columns)) row[name] = value();
      return row;
    });
    keys.set(
      lookup.table,
      rows.map((row) => row[lookup.keyColumn] as string),
    );
    await client.insertBatch(lookup.table, rows);
  }
  const haulers = keys.get("haulers") ?? [];
  await client.execute(
    'CREATE TABLE "trucks" ("truckKey" TEXT PRIMARY KEY, "truckID" TEXT, "haulerKey" TEXT)',
  );
  const trucks = Array.from({ length: 900 }, (_, index) => ({
    truckKey: key("trucks", index),
    truckID: `TR${String(index)}`,
    haulerKey: pick(haulers),
  }));
  keys.set(
    "trucks",
    trucks.map((truck) => truck.truckKey),
  );
  await client.insertBatch("trucks", trucks);

  await client.execute(
    `CREATE TABLE "loadTickets" ("ticketKey" TEXT PRIMARY KEY, "batchNum" INTEGER, ` +
      `"creditRebill" TEXT, "revised" BOOLEAN, "void" BOOLEAN, "version" INTEGER, ` +
      `"ticketDate" TEXT, "ticketDateTime" TIMESTAMP, "truckKey" TEXT, ` +
      `${LOOKUPS.map((lookup) => `"${lookup.keyColumn}" TEXT`).join(", ")}, ` +
      `${TEXTS.map((name) => `"${name}" TEXT`).join(", ")}, ` +
      `${WEIGHTS.map((name) => `"${name}" NUMERIC(12,2)`).join(", ")}, ` +
      `${AMOUNTS.map((name) => `"${name}" NUMERIC(12,2)`).join(", ")})`,
  );
  await client.execute(
    'CREATE TABLE "loadTicketAdjustments" ("ticketKey" TEXT PRIMARY KEY, ' +
      '"adjustmentKey1" TEXT, "adjustmentKey2" TEXT, "adjustmentKey3" TEXT)',
  );
  const start = Date.UTC(2022, 10, 1);
  const span = Date.UTC(2026, 9, 1) - start;
  const tickets: Row[] = [];
  const adjustments: Row[] = [];
  for (let index = 0; index < TICKETS; index += 1) {
    const at = new Date(start + Math.floor(random() * span));
    const ticketKey = key("ticket", index);
    const ticket: Row = {
      ticketKey,
      batchNum: Math.floor(random() * 500),
      creditRebill: null,
      revised: random() < 0.1,
      void: false,
      version: Math.floor(random() * 5),
      ticketDate: at.toISOString().slice(0, 10),
      ticketDateTime: at,
      truckKey: pick(keys.get("trucks") ?? []),
    };
    for (const lookup of LOOKUPS) {
      // Optional lookups are sometimes absent, so the CASE arms see both branches.
      const optional = lookup.table !== "regions" && random() < 0.15;
      ticket[lookup.keyColumn] = optional ? null : pick(keys.get(lookup.table) ?? []);
    }
    for (const name of TEXTS) ticket[name] = random() < 0.3 ? null : phrase(2);
    for (const name of [...WEIGHTS, ...AMOUNTS]) ticket[name] = Math.round(random() * 1e6) / 100;
    tickets.push(ticket);
    if (random() < 0.2) {
      adjustments.push({
        ticketKey,
        adjustmentKey1: key("adjustment", index),
        adjustmentKey2: null,
        adjustmentKey3: null,
      });
    }
  }
  for (let offset = 0; offset < tickets.length; offset += 4000) {
    await client.insertBatch("loadTickets", tickets.slice(offset, offset + 4000));
  }
  await client.insertBatch("loadTicketAdjustments", adjustments);
}

const object = (
  alias: string,
  table: string,
  columns: readonly string[],
  when?: string,
): string => {
  const members = columns.map((name) => `'${name}' VALUE "${table}"."${name}"`).join(", ");
  const built = `JSON_OBJECT(${members})`;
  return when === undefined
    ? `${built} AS "${alias}"`
    : `CASE WHEN "${table}"."${when}" IS NOT NULL THEN ${built} END AS "${alias}"`;
};

const TICKET_COLUMNS = [
  "batchNum",
  "creditRebill",
  "revised",
  "void",
  "version",
  "ticketDate",
  "ticketDateTime",
  "ticketKey",
  "truckKey",
  ...LOOKUPS.map((lookup) => lookup.keyColumn),
  ...TEXTS,
  ...WEIGHTS,
];
const SQL =
  `SELECT ${TICKET_COLUMNS.map((name) => `"loadTickets"."${name}"`).join(", ")}, ` +
  [
    object("region", "regions", ["regionKey", "regionName", "regionDescription"]),
    object(
      "location",
      "locations",
      ["locationKey", "locationName", "locationDescription"],
      "locationKey",
    ),
    object("yard", "yards", ["yardKey", "yardName", "yardDescription"], "yardKey"),
    object("customer", "customers", ["customerKey", "customerID", "customerName"], "customerKey"),
    object("order", "orders", ["orderKey", "orderNumber", "description"], "orderKey"),
    object("request", "requests", ["requestKey", "info", "referenceNum"], "requestKey"),
    object(
      "product",
      "products",
      ["productKey", "productID", "productDescription", "unitOfMeasure"],
      "productKey",
    ),
    object("hauler", "haulers", ["haulerID", "haulerKey", "haulerName"], "haulerKey"),
    object("truck", "trucks", ["truckKey", "truckID", "haulerKey"]),
    object("shipTo", "shipTos", ["shipToKey", "description"], "shipToKey"),
    object("taxCode", "taxCodes", ["taxCodeKey", "code", "description"], "taxCodeKey"),
  ].join(", ") +
  `, ${AMOUNTS.map((name) => `"loadTickets"."${name}"`).join(", ")}, ` +
  object(
    "adjustments",
    "loadTicketAdjustments",
    ["adjustmentKey1", "adjustmentKey2", "adjustmentKey3"],
    "ticketKey",
  ) +
  ` FROM "loadTickets"` +
  ` INNER JOIN "regions" ON "regions"."regionKey" = "loadTickets"."regionKey"` +
  LOOKUPS.filter((lookup) => lookup.table !== "regions")
    .map(
      (lookup) =>
        ` LEFT JOIN "${lookup.table}" ON "${lookup.table}"."${lookup.keyColumn}" = "loadTickets"."${lookup.keyColumn}"`,
    )
    .join("") +
  ` INNER JOIN "trucks" ON "trucks"."truckKey" = "loadTickets"."truckKey"` +
  ` LEFT JOIN "loadTicketAdjustments" ON "loadTicketAdjustments"."ticketKey" = "loadTickets"."ticketKey"` +
  ` WHERE ("loadTickets"."ticketDate" >= $1 AND "loadTickets"."ticketDate" <= $2)` +
  ` AND "loadTickets"."creditRebill" IS NULL AND "loadTickets"."void" = $3` +
  ` ORDER BY "loadTickets"."ticketDateTime" DESC`;
const PARAMS = ["2022-10-29", "2026-10-29", false];

// --- The two threads ---------------------------------------------------------------------------

const databaseChannel = new MessageChannel();
const controlChannel = new MessageChannel();
const worker = new Worker(new URL("./lib/live-wide-bench-worker.mts", import.meta.url), {
  workerData: { database: databaseChannel.port1, control: controlChannel.port1 },
  transferList: [databaseChannel.port1, controlChannel.port1],
});
await new Promise((resolve) => worker.once("message", resolve));
const control = controlChannel.port2;
const workerCommand = (command: "start" | "stop"): Promise<unknown> =>
  new Promise((resolve) => {
    control.once("message", resolve);
    control.postMessage(command);
  });

databaseChannel.port2.start();
const client = new MinnowDatabaseClient(
  databaseChannel.port2 as unknown as ConstructorParameters<typeof MinnowDatabaseClient>[0],
  { store: { kind: "memory" } },
);
await client.ready();
await seed_(client);

let deliveries = 0;
let lastDeliveryAt = 0;
/** Every delivered result passes each plugin's `transformResult` once: a delivery counter. */
const countDeliveries: KyselyPlugin = {
  transformQuery: (args) => args.node,
  transformResult: async (args) => {
    deliveries += 1;
    lastDeliveryAt = performance.now();
    return args.result;
  },
};
const db = new Kysely<Record<string, never>>({
  dialect: new MinnowDialect({
    driver: client,
    resultDecoding: { numeric: "number", json: "parse" },
  }),
  plugins: [countDeliveries],
});
const live = createKyselyLiveQueries({ driver: client, db });
const compiled = CompiledQuery.raw(SQL, PARAMS);
const builder = {
  compile: () => compiled,
  execute: async () => (await db.executeQuery(compiled)).rows as object[],
};

const session = new Session();
session.connect();
const nextTurn = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

type Lifecycle = "subscribe" | "suspense" | "suspense-strict";

interface Measurement {
  readonly deliveries: number;
  readonly rows: number;
  readonly readyMs: number;
  readonly worker: AllocationReport;
  readonly main: AllocationReport;
}

async function measure(lifecycle: Lifecycle): Promise<Measurement> {
  const query = live(builder);
  await workerCommand("start");
  const stopMain = await startAllocationSampling(session);
  deliveries = 0;
  const started = performance.now();
  const listener = (): void => undefined;
  let unsubscribe: () => void;
  if (lifecycle === "subscribe") {
    unsubscribe = query.subscribe(listener);
    while (query.getSnapshot().status !== "ready") await sleep(1);
  } else {
    // Suspense: the thrown refresh() loads the query; React then commits and subscribes.
    await query.refresh();
    await nextTurn();
    unsubscribe = query.subscribe(listener);
    if (lifecycle === "suspense-strict") {
      unsubscribe();
      unsubscribe = query.subscribe(listener);
    }
  }
  const readyMs = performance.now() - started;
  // Settle: any further delivery the lifecycle caused arrives and is decoded.
  lastDeliveryAt = performance.now();
  while (performance.now() - lastDeliveryAt < QUIET_MS) await sleep(50);
  const main = await stopMain();
  const workerReport = (await workerCommand("stop")) as AllocationReport;
  const rows = query.getSnapshot().rows.length;
  unsubscribe();
  query.close();
  await sleep(100);
  return { deliveries, rows, readyMs, worker: workerReport, main };
}

const median = (values: readonly number[]): number => {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)] ?? Number.NaN;
};

console.log(`live-wide-bench: ${String(TICKETS)} tickets, ${String(ROUNDS)} rounds per lifecycle`);
// One warm-up so the JIT and the engine's plan caches are in the state an app's second query sees.
await measure("subscribe");
for (const lifecycle of ["subscribe", "suspense", "suspense-strict"] as const) {
  const runs: Measurement[] = [];
  for (let round = 0; round < ROUNDS; round += 1) runs.push(await measure(lifecycle));
  const last = runs.at(-1);
  console.log(
    `${lifecycle.padEnd(16)} deliveries ${String(median(runs.map((run) => run.deliveries)))}` +
      `  rows ${String(last?.rows)}` +
      `  worker ${MB(median(runs.map((run) => run.worker.allocatedBytes)))} MB` +
      `  main ${MB(median(runs.map((run) => run.main.allocatedBytes)))} MB` +
      `  ready ${median(runs.map((run) => run.readyMs)).toFixed(0)} ms`,
  );
  if (SHOW_TOP && last !== undefined) {
    for (const [side, report] of [
      ["worker", last.worker],
      ["main", last.main],
    ] as const) {
      for (const { site, bytes } of report.top)
        console.log(`    ${side} ${MB(bytes).padStart(6)}  ${site}`);
    }
  }
}

await client.close();
await worker.terminate();
