/**
 * The published worker entry, driven the way an application drives it.
 *
 * Every other test of the worker boundary substitutes for it: `client.test.ts` runs a faithful
 * in-process stand-in with structured cloning and ordered delivery, which is right for testing
 * the protocol but cannot fail the way a real worker fails. What it does not cover is everything
 * that only exists once there really are two threads — that `@minnowdb/core/worker` resolves and
 * boots as a module worker at all, that transferred buffers arrive intact and detached on the
 * sending side, that a worker holding an IndexedDB database is visible to the page that opened
 * it, and that terminating one leaves the data behind rather than a half-written database.
 *
 * `worker.ts` is two lines and had no test at all; those two lines are what every production
 * request goes through.
 */
import { MinnowDatabaseClient } from "../src/engine/client.js";

interface WorkerTestResult {
  /** The worker booted, answered, and reported the rows it was given. */
  roundTrip: { rows: number; firstRegion: string | null };
  /** A result set large enough that its buffers are transferred rather than copied. */
  largeTransfer: { rows: number; checksum: number };
  /** A rejection raised inside the worker, surfaced on the page with its message intact. */
  workerError: { rejected: boolean; message: string };
  /** Data written through one worker, read back through a second one over the same store. */
  survivesTermination: { rowsAfterRestart: number; reopenedSameDatabase: boolean };
  /** Writes issued without awaiting, to prove the protocol keeps them ordered under load. */
  concurrentWrites: { acknowledged: number; rowsPersisted: number; reasons: string[] };
  /** A durable secondary index built and mutated in one worker, then reused by the next. */
  secondaryIndex: {
    initialMatches: number;
    mutationMatches: number;
    matchesAfterRestart: number;
    usedBeforeRestart: boolean;
    usedAfterRestart: boolean;
  };
}

/** A module worker at the published entry — the exact construction the documentation shows. */
function spawn(): Worker {
  return new Worker(new URL("../src/engine/worker.ts", import.meta.url), { type: "module" });
}

async function run(): Promise<WorkerTestResult> {
  const databaseName = `worker-boundary-${String(Date.now())}-${String(Math.random()).slice(2)}`;
  const store = { kind: "indexeddb", name: databaseName } as const;

  // --- boot, round trip, and a transfer big enough to matter -----------------------------------
  const worker = spawn();
  const client = new MinnowDatabaseClient(worker, { store, options: { rowsPerBlock: 256 } });
  await client.createTable({
    name: "events",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  const regions = ["west", "east", "north", "south"];
  const rows = Array.from({ length: 4_000 }, (_, index) => ({
    id: index + 1,
    region: regions[index % regions.length] ?? "west",
    amount: index,
  }));
  await client.insertBatch("events", rows);

  // Build through SQL over the real worker boundary. The selective query and EXPLAIN together
  // prove both correctness and that the browser-backed database actually chose the index.
  await client.execute("CREATE INDEX events_by_region ON events(region)");
  const initialIndexed = await client.query(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'west'",
    { memoize: false },
  );
  const initialIndexPlan = await client.explain(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'west'",
  );

  const small = await client.query("SELECT region FROM events WHERE id = 1");
  const roundTrip = {
    rows: small.rows.length,
    firstRegion: (small.rows[0] as { region?: string } | undefined)?.region ?? null,
  };

  // Every row back across the boundary. The engine transfers the underlying buffers rather than
  // copying them, so this is the case where a detached or mis-sliced buffer would show up as
  // wrong numbers rather than as an error.
  const large = await client.query("SELECT id, amount FROM events ORDER BY id");
  let checksum = 0;
  for (const row of large.rows as Array<{ id: number; amount: number }>) {
    checksum = (checksum + row.id * 2 - row.amount) % 1_000_003;
  }
  const largeTransfer = { rows: large.rows.length, checksum };

  // --- an error raised inside the worker ------------------------------------------------------
  let rejected = false;
  let message = "";
  try {
    await client.query("SELECT nope FROM events");
  } catch (error) {
    rejected = true;
    message = error instanceof Error ? error.message : String(error);
  }
  const workerError = { rejected, message };

  // --- writes in flight together --------------------------------------------------------------
  // Issued without awaiting each one: the protocol has to keep them ordered and distinct across
  // a real postMessage queue, not just across an in-process promise chain.
  const reasons: string[] = [];
  const acknowledgements = await Promise.all(
    Array.from({ length: 16 }, (_, index) =>
      client.insertBatch("events", [{ id: 10_000 + index, region: "central", amount: index }]).then(
        () => 1,
        (error: unknown) => {
          reasons.push(error instanceof Error ? error.message : String(error));
          return 0;
        },
      ),
    ),
  );
  const persisted = await client.query("SELECT COUNT(*) AS n FROM events WHERE region = 'central'");
  const concurrentWrites = {
    acknowledged: acknowledgements.reduce((total, one) => total + one, 0),
    rowsPersisted: (persisted.rows[0] as { n?: number } | undefined)?.n ?? -1,
    reasons: [...new Set(reasons)],
  };
  const secondaryIndex = {
    initialMatches: (initialIndexed.rows[0] as { n?: number } | undefined)?.n ?? -1,
    mutationMatches: concurrentWrites.rowsPersisted,
    matchesAfterRestart: -1,
    usedBeforeRestart: initialIndexPlan.includes("secondary index prunes"),
    usedAfterRestart: false,
  };

  // --- terminate, then reopen the same store through a second worker --------------------------
  await client.close();
  worker.terminate();

  const second = spawn();
  const reopened = new MinnowDatabaseClient(second, { store });
  const after = await reopened.query("SELECT COUNT(*) AS n FROM events");
  const indexedAfter = await reopened.query(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'central'",
    { memoize: false },
  );
  const reopenedIndexPlan = await reopened.explain(
    "SELECT COUNT(*) AS n FROM events WHERE region = 'central'",
  );
  secondaryIndex.matchesAfterRestart =
    (indexedAfter.rows[0] as { n?: number } | undefined)?.n ?? -1;
  secondaryIndex.usedAfterRestart = reopenedIndexPlan.includes("secondary index prunes");
  const tables = await reopened.listTables();
  const survivesTermination = {
    rowsAfterRestart: (after.rows[0] as { n?: number } | undefined)?.n ?? -1,
    reopenedSameDatabase: tables.some((table) => table.name === "events"),
  };
  await reopened.close();
  second.terminate();

  return {
    roundTrip,
    largeTransfer,
    workerError,
    survivesTermination,
    concurrentWrites,
    secondaryIndex,
  };
}

declare global {
  interface Window {
    runWorkerBoundaryTest(): Promise<WorkerTestResult>;
  }
}

window.runWorkerBoundaryTest = run;

const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "Worker boundary tests ready";
