/**
 * The live-query suite: N subscriptions registered through the engine's main-thread client, one
 * commit, and the time until every affected subscription has been told — through the client,
 * so the number includes the RPC, the worker's commit, the live-query sweep, and the change
 * event crossing the channel with its rows rebuilt on the receiving side. That is the latency an
 * application's UI sees between a write and the re-render it causes.
 *
 * Two rules keep the numbers honest:
 *
 *  - The suite creates its own tables beside the dataset's and never touches the dataset's
 *    rows, so the read suite's oracles stay true afterwards. One table is watched and written;
 *    a second, quiet one is only watched, so a case can measure subscriptions the commit does
 *    not affect — the cost of deciding not to re-run them.
 *  - Nothing counts until it is verified: every affected subscription must fire exactly once per
 *    commit and end on the row count the commits imply, and no unaffected one may fire at all.
 *
 * Only engines with a live-query layer take part; the others are reported as unsupported with
 * that reason, not as a slow number.
 */
import type {
  EngineId,
  LiveCaseReport,
  LiveEngineMeasurement,
  LiveSuitePayload,
  LiveSuiteResult,
} from "../protocol";
import {
  loadDriver,
  requireMaterialization,
  type LiveSession,
  type LiveSubscriptionHandle,
  type WriteBatch,
  type WriteTableSchema,
} from "../engines/session";
import { getDataset } from "./registry";
import {
  assertNotCancelled,
  progress,
  summarizeSamples,
  validateDatasetSuitePayload,
} from "./support";

/** Commits per case; each is one inserted row and one timed notification round. */
const SAMPLE_COUNT = 5;
/** Rows in the watched table before the first commit; the queries aggregate over them. */
const SEED_ROWS = 1_000;
/** A notification that has not arrived by then is a defect, not a slow sample. */
const NOTIFICATION_TIMEOUT_MS = 10_000;

const COLUMNS = ["key_id", "payload_num", "payload_text"] as const;

export interface LiveCaseDefinition {
  id: string;
  name: string;
  /** Subscriptions registered before the commit. */
  subscriptions: number;
  /** How many of them query the table the commit writes; the rest watch the quiet table. */
  affected: number;
}

export function validateLivePayload(value: unknown): LiveSuitePayload {
  return validateDatasetSuitePayload(value);
}

/**
 * One subscription is the floor; ten and a hundred show how the sweep scales when every
 * subscription re-runs; the last keeps ninety-nine quiet so the cost of ruling them out is
 * measured on its own.
 */
export function liveCaseDefinitions(): LiveCaseDefinition[] {
  return [
    { id: "live-1", name: "1 subscription", subscriptions: 1, affected: 1 },
    { id: "live-10", name: "10 subscriptions", subscriptions: 10, affected: 10 },
    { id: "live-100", name: "100 subscriptions", subscriptions: 100, affected: 100 },
    { id: "live-100-1", name: "100 subscriptions, 1 affected", subscriptions: 100, affected: 1 },
  ];
}

export async function runLiveSuite(
  requestId: string,
  payload: LiveSuitePayload,
): Promise<LiveSuiteResult> {
  const record = await getDataset(payload.datasetId);
  const definitions = liveCaseDefinitions();
  const sessions = new Map<EngineId, LiveSession | Error>();
  for (const engine of payload.engines) {
    try {
      requireMaterialization(record, engine);
      const driver = await loadDriver(engine);
      if (driver.openLiveSession === undefined) {
        throw new Error("This engine has no live-query layer");
      }
      sessions.set(engine, await driver.openLiveSession(record));
    } catch (error) {
      sessions.set(engine, error instanceof Error ? error : new Error(String(error)));
    }
  }
  // Fresh table names per run: minnow has no DROP TABLE, so a fixed name would collide with a
  // previous run on the same dataset.
  const runToken = Math.random().toString(36).slice(2, 8);
  const totalSteps = definitions.length * payload.engines.length;
  let completed = 0;
  const cases: LiveCaseReport[] = [];
  try {
    for (const definition of definitions) {
      const measurements: LiveEngineMeasurement[] = [];
      for (const engine of payload.engines) {
        assertNotCancelled(requestId);
        progress(requestId, {
          phase: "live",
          completed,
          total: totalSteps,
          message: `${definition.name} · ${engine}`,
        });
        const session = sessions.get(engine);
        measurements.push(
          session === undefined || session instanceof Error
            ? unsupported(
                engine,
                session instanceof Error ? session.message : "session unavailable",
              )
            : await measureLiveCase(session, definition, `${runToken}_${definition.id}`),
        );
        completed += 1;
      }
      cases.push({
        id: definition.id,
        name: definition.name,
        subscriptions: definition.subscriptions,
        affected: definition.affected,
        engines: measurements,
      });
    }
  } finally {
    for (const session of sessions.values()) {
      if (!(session instanceof Error)) await session.close().catch(() => undefined);
    }
  }
  const supportedByEngine: Partial<Record<EngineId, number>> = {};
  for (const engine of payload.engines) {
    supportedByEngine[engine] = cases
      .flatMap((report) => report.engines)
      .filter((measurement) => measurement.engine === engine && measurement.supported).length;
  }
  progress(requestId, {
    phase: "complete",
    completed: totalSteps,
    total: totalSteps,
    message: "Live-query suite complete",
  });
  return {
    datasetId: record.id,
    scale: record.scale,
    sampleCount: SAMPLE_COUNT,
    engines: payload.engines,
    cases,
    supportedByEngine,
    passed: cases.every((report) =>
      report.engines.every((measurement) => !measurement.supported || measurement.verified),
    ),
  };
}

function unsupported(engine: EngineId, error: string): LiveEngineMeasurement {
  return {
    engine,
    supported: false,
    error,
    subscribeMs: 0,
    medianMs: 0,
    p95Ms: 0,
    notifications: 0,
    verified: false,
  };
}

/**
 * Runs one case on one session: creates and seeds the tables, registers the subscriptions, then
 * commits one row at a time and clocks each commit until the last affected subscription has
 * reported. The first commit warms the path and is not timed.
 */
export async function measureLiveCase(
  session: LiveSession,
  definition: LiveCaseDefinition,
  tableToken: string,
  options: { notificationTimeoutMs?: number } = {},
): Promise<LiveEngineMeasurement> {
  const timeoutMs = options.notificationTimeoutMs ?? NOTIFICATION_TIMEOUT_MS;
  // Case ids carry hyphens, which SQL would read as subtraction inside an unquoted table name.
  const token = tableToken.replaceAll("-", "_");
  const watched = `bl_${token}_w`;
  const quiet = `bl_${token}_q`;
  const handles: LiveSubscriptionHandle[] = [];
  try {
    await session.createTable(tableSchema(watched));
    await session.createTable(tableSchema(quiet));
    await session.insert(watched, batchFor(1, SEED_ROWS));
    await session.insert(quiet, batchFor(1, 16));

    // Each affected subscription counts the rows above its own threshold, so every commit of a
    // new highest key changes every one of them and the row count each ends on is predictable.
    // Unaffected ones ask the quiet table the same question and must never fire.
    let armed = false;
    let fired = 0;
    let strayFired = 0;
    const latest = new Array<number | null>(definition.affected).fill(null);
    let settle: (() => void) | undefined;
    const subscribeStarted = performance.now();
    for (let index = 0; index < definition.subscriptions; index += 1) {
      const affected = index < definition.affected;
      const table = affected ? watched : quiet;
      const sql = `SELECT COUNT(*) AS n FROM ${table} WHERE key_id > ${String(index)}`;
      handles.push(
        await session.subscribe(sql, (rows) => {
          if (!armed) return;
          if (!affected) {
            strayFired += 1;
            return;
          }
          latest[index] = numberOf(rows[0]?.n);
          fired += 1;
          if (fired === definition.affected) settle?.();
        }),
      );
    }
    const subscribeMs = performance.now() - subscribeStarted;
    armed = true;

    const samples: number[] = [];
    let verified = true;
    let notifications = 0;
    for (let sample = 0; sample <= SAMPLE_COUNT; sample += 1) {
      fired = 0;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const settled = new Promise<void>((resolve, reject) => {
        settle = resolve;
        timer = setTimeout(() => {
          reject(
            new Error(
              `${String(definition.affected - fired)} of ${String(definition.affected)} subscriptions were not notified within ${String(timeoutMs)} ms`,
            ),
          );
        }, timeoutMs);
      });
      const key = SEED_ROWS + sample + 1;
      const started = performance.now();
      await session.insert(watched, batchFor(key, 1));
      try {
        await settled;
      } finally {
        clearTimeout(timer);
      }
      const elapsed = performance.now() - started;
      // Every affected subscription must now count the rows above its threshold.
      const rowsNow = key;
      for (let index = 0; index < definition.affected; index += 1) {
        if (latest[index] !== rowsNow - index) verified = false;
      }
      notifications = fired;
      // The first commit warms the sweep and the channel; it is not a sample.
      if (sample > 0) samples.push(elapsed);
    }
    // A notification for a subscription the commit could not affect is a defect too — the
    // selective sweep is the thing being measured, and it is not allowed to be selective
    // by accident. Give late events a tick to land before judging.
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
    if (strayFired > 0) verified = false;
    const { medianMs, p95Ms } = summarizeSamples(samples);
    return {
      engine: session.engine,
      supported: true,
      subscribeMs,
      medianMs,
      p95Ms,
      notifications,
      verified: verified && notifications === definition.affected,
    };
  } catch (error) {
    return unsupported(session.engine, error instanceof Error ? error.message : String(error));
  } finally {
    for (const handle of handles) await handle.close().catch(() => undefined);
  }
}

function tableSchema(name: string): WriteTableSchema {
  return {
    name,
    primaryKey: COLUMNS[0],
    columns: [
      { name: COLUMNS[0], type: "number" },
      { name: COLUMNS[1], type: "number" },
      { name: COLUMNS[2], type: "string" },
    ],
  };
}

/** `rowCount` deterministic rows keyed `firstKey…`; the payload is there to be a real row. */
function batchFor(firstKey: number, rowCount: number): WriteBatch {
  const keys: number[] = [];
  const numbers: number[] = [];
  const texts: string[] = [];
  for (let index = 0; index < rowCount; index += 1) {
    const key = firstKey + index;
    keys.push(key);
    numbers.push(key % 4_096);
    texts.push(`row-${String(key)}`);
  }
  return {
    rowCount,
    columns: { [COLUMNS[0]]: keys, [COLUMNS[1]]: numbers, [COLUMNS[2]]: texts },
  };
}

function numberOf(value: unknown): number | null {
  return typeof value === "number" ? value : typeof value === "bigint" ? Number(value) : null;
}
