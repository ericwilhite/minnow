/**
 * The OPFS block store on real browser storage, with real workers on the other side.
 *
 * The Node suites run the same store over an in-memory shim that mimics OPFS's locking; this is
 * where the mimicry is checked against the genuine article, per engine: that sync access handles
 * exist in the published worker at all, that the exclusive handle on the command log's next
 * sequence file really arbitrates two independent workers, that terminating a worker releases
 * its locks and loses nothing acknowledged, and that a fresh worker replays a log that has
 * crossed a checkpoint boundary.
 */
import { MinnowDatabaseClient } from "../src/engine/client.js";

interface OpfsTestResult {
  /** One worker over the OPFS store: create, insert, read back. */
  roundTrip: { rows: number; firstRegion: string | null };
  /** Two workers, one directory, unawaited interleaved writes through both. */
  competingCommits: {
    acknowledged: number;
    rejectedCleanly: number;
    unexpected: string[];
    firstCount: number;
    secondCount: number;
  };
  /** A worker terminated mid-life; a fresh one reads what was acknowledged. */
  survivesTermination: { rowsAfterRestart: number };
  /** Enough commits to cross the checkpoint interval, then a cold reopen. */
  checkpointCrossing: { rowsAfterReopen: number };
  /** A persisted index base and mutation tail, selected again after a cold OPFS reopen. */
  secondaryIndex: {
    baseMatches: number;
    tailMatches: number;
    matchesAfterReopen: number;
    usedBeforeReopen: boolean;
    usedAfterReopen: boolean;
  };
}

function spawn(): Worker {
  return new Worker(new URL("../src/engine/worker.ts", import.meta.url), { type: "module" });
}

function uniqueName(prefix: string): string {
  return `${prefix}-${String(Date.now())}-${String(Math.random()).slice(2)}`;
}

const columns = [
  { name: "id", type: "number" },
  { name: "region", type: "string" },
  { name: "amount", type: "number" },
] as const;

let phase = "start";

async function run(): Promise<OpfsTestResult> {
  try {
    return await runPhases();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`[phase: ${phase}] ${message}`, { cause: error });
  }
}

async function runPhases(): Promise<OpfsTestResult> {
  phase = "round-trip";
  // --- one worker: the plain path --------------------------------------------------------------
  const roundTripName = uniqueName("opfs-round-trip");
  const first = spawn();
  const client = new MinnowDatabaseClient(first, {
    store: { kind: "opfs", name: roundTripName },
    databaseOptions: { rowsPerBlock: 64 },
  });
  await client.createTable({ name: "events", uniqueKey: "id", columns: [...columns] });
  const regions = ["west", "east", "north", "south"];
  await client.insertBatch(
    "events",
    Array.from({ length: 500 }, (_, index) => ({
      id: index + 1,
      region: regions[index % regions.length] ?? "west",
      amount: index,
    })),
  );
  const small = await client.query("SELECT region FROM events WHERE id = 1");
  const all = await client.query("SELECT id FROM events");
  const roundTrip = {
    rows: all.rows.length,
    firstRegion: (small.rows[0] as { region?: string } | undefined)?.region ?? null,
  };

  // The base is written through OPFS's staged generation protocol. The extra row becomes a
  // delta, so reopening below verifies both durable pieces rather than only the initial build.
  await client.execute("CREATE INDEX events_by_region ON events(region)");
  const baseIndexed = await client.query(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'north'",
    { memoize: false },
  );
  const indexPlan = await client.explain("SELECT COUNT(*) AS n FROM events WHERE region = 'north'");
  await client.insertBatch("events", [{ id: 501, region: "special", amount: 500 }]);
  const tailIndexed = await client.query(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'special'",
    { memoize: false },
  );
  await client.close({ terminateWorker: true });
  const indexReopenWorker = spawn();
  const indexReopen = new MinnowDatabaseClient(indexReopenWorker, {
    store: { kind: "opfs", name: roundTripName },
  });
  const reopenedIndexed = await indexReopen.query(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'special'",
    { memoize: false },
  );
  const reopenedIndexPlan = await indexReopen.explain(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'special'",
  );
  const secondaryIndex = {
    baseMatches: (baseIndexed.rows[0] as { n?: number } | undefined)?.n ?? -1,
    tailMatches: (tailIndexed.rows[0] as { n?: number } | undefined)?.n ?? -1,
    matchesAfterReopen: (reopenedIndexed.rows[0] as { n?: number } | undefined)?.n ?? -1,
    usedBeforeReopen: indexPlan.includes("secondary index prunes"),
    usedAfterReopen: reopenedIndexPlan.includes("secondary index prunes"),
  };
  await indexReopen.close({ terminateWorker: true });

  phase = "competing-commits";
  // --- two workers, one directory: the exclusive-handle CAS for real ---------------------------
  const sharedName = uniqueName("opfs-competing");
  const workerA = spawn();
  const workerB = spawn();
  const clientA = new MinnowDatabaseClient(workerA, { store: { kind: "opfs", name: sharedName } });
  const clientB = new MinnowDatabaseClient(workerB, { store: { kind: "opfs", name: sharedName } });
  await clientA.createTable({ name: "items", uniqueKey: "id", columns: [...columns] });

  let acknowledged = 0;
  let rejectedCleanly = 0;
  const unexpected: string[] = [];
  await Promise.all(
    Array.from({ length: 24 }, (_, index) =>
      (index % 2 === 0 ? clientA : clientB)
        .insertBatch("items", [{ id: index, region: "west", amount: index * 10 }])
        .then(
          () => {
            acknowledged += 1;
          },
          (error: unknown) => {
            const message = error instanceof Error ? error.message : String(error);
            if (/Manifest changed|conflict/i.test(message)) rejectedCleanly += 1;
            else unexpected.push(message);
          },
        ),
    ),
  );
  const countOf = async (candidate: MinnowDatabaseClient): Promise<number> => {
    const result = await candidate.query("SELECT COUNT(*) AS n FROM items", { memoize: false });
    return (result.rows[0] as { n: number }).n;
  };
  const competingCommits = {
    acknowledged,
    rejectedCleanly,
    unexpected,
    firstCount: await countOf(clientA),
    secondCount: await countOf(clientB),
  };

  phase = "termination";
  // --- termination: acknowledged writes survive, locks die with the worker ---------------------
  workerA.terminate();
  const workerC = spawn();
  const clientC = new MinnowDatabaseClient(workerC, { store: { kind: "opfs", name: sharedName } });
  const survivesTermination = { rowsAfterRestart: await countOf(clientC) };

  phase = "checkpoint-crossing";
  // --- checkpoint crossing: many commits, then a cold replay -----------------------------------
  const checkpointName = uniqueName("opfs-checkpoint");
  const workerD = spawn();
  const clientD = new MinnowDatabaseClient(workerD, {
    store: { kind: "opfs", name: checkpointName },
  });
  await clientD.createTable({ name: "steps", uniqueKey: "id", columns: [...columns] });
  // Each batch commits several log entries, so forty batches cross the checkpoint interval
  // multiple times over.
  for (let index = 0; index < 40; index += 1) {
    await clientD.insertBatch("steps", [{ id: index, region: "west", amount: index }]);
  }
  await clientD.close({ terminateWorker: true });
  const workerE = spawn();
  const clientE = new MinnowDatabaseClient(workerE, {
    store: { kind: "opfs", name: checkpointName },
  });
  const reopened = await clientE.query("SELECT COUNT(*) AS n FROM steps", { memoize: false });
  const checkpointCrossing = {
    rowsAfterReopen: (reopened.rows[0] as { n: number }).n,
  };

  return { roundTrip, competingCommits, survivesTermination, checkpointCrossing, secondaryIndex };
}

declare global {
  interface Window {
    runOpfsStoreTest: () => Promise<OpfsTestResult>;
  }
}

window.runOpfsStoreTest = run;
const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "OPFS store tests ready";
