/**
 * Ad-hoc query execution against a persisted dataset: each requested engine prepares
 * once, warms up once, then executes seven measured iterations. Checksums compare the
 * canonicalized full result across engines.
 */
import { resultChecksum } from "../engines/shared.js";
import { loadDriver, requireMaterialization } from "../engines/session.js";
import type { EngineId, EngineQueryRun, RunQueryPayload, RunQueryResult } from "../protocol.js";
import { engineIds } from "../protocol.js";
import { getDataset } from "./registry.js";
import { summarizeSamples } from "./support.js";

const ITERATIONS = 7;
const PREVIEW_ROWS = 50;

export function validateRunQueryPayload(value: unknown): RunQueryPayload {
  if (typeof value !== "object" || value === null) throw new Error("Invalid query payload");
  const payload = value as Partial<RunQueryPayload>;
  if (typeof payload.datasetId !== "string") throw new TypeError("Dataset id must be a string");
  if (typeof payload.sql !== "string" || payload.sql.trim().length === 0) {
    throw new TypeError("Query SQL must be a non-empty string");
  }
  if (
    !Array.isArray(payload.engines) ||
    payload.engines.length === 0 ||
    !payload.engines.every((engine): engine is EngineId => engineIds.includes(engine))
  ) {
    throw new Error("Select at least one engine");
  }
  return {
    datasetId: payload.datasetId,
    sql: payload.sql,
    engines: [...new Set(payload.engines)],
  };
}

export async function runQuery(payload: RunQueryPayload): Promise<RunQueryResult> {
  const record = await getDataset(payload.datasetId);
  const runs: EngineQueryRun[] = [];
  for (const engine of payload.engines) {
    runs.push(await runOnEngine(record, engine, payload.sql));
  }
  const checksums = runs.filter((run) => run.ok).map((run) => run.checksum);
  const checksumAgreement =
    checksums.length < 2
      ? null
      : checksums.every((checksum) => checksum === checksums[0])
        ? "match"
        : "mismatch";
  return { datasetId: record.id, sql: payload.sql.trim(), runs, checksumAgreement };
}

async function runOnEngine(
  record: Awaited<ReturnType<typeof getDataset>>,
  engine: EngineId,
  sql: string,
): Promise<EngineQueryRun> {
  const failed = (error: unknown): EngineQueryRun => ({
    engine,
    ok: false,
    error: error instanceof Error ? error.message : String(error),
    prepareMs: 0,
    medianMs: 0,
    p95Ms: 0,
    iterations: 0,
    rowCount: 0,
    columns: [],
    previewRows: [],
    truncated: false,
    checksum: 0,
  });
  try {
    requireMaterialization(record, engine);
    const driver = await loadDriver(engine);
    const session = await driver.openSession(record);
    try {
      const prepareStarted = performance.now();
      const prepared = await session.prepare(sql);
      const prepareMs = performance.now() - prepareStarted;
      try {
        let rows = await prepared.execute();
        const samples: number[] = [];
        for (let index = 0; index < ITERATIONS; index += 1) {
          const started = performance.now();
          rows = await prepared.execute();
          samples.push(performance.now() - started);
        }
        const { medianMs, p95Ms } = summarizeSamples(samples);
        const columns = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
        return {
          engine,
          ok: true,
          prepareMs,
          medianMs,
          p95Ms,
          iterations: ITERATIONS,
          rowCount: rows.length,
          columns,
          previewRows: rows.slice(0, PREVIEW_ROWS),
          truncated: rows.length > PREVIEW_ROWS,
          checksum: resultChecksum(rows),
          ...(prepared.plan === undefined ? {} : { plan: prepared.plan }),
        };
      } finally {
        prepared.close();
      }
    } finally {
      await session.close();
    }
  } catch (error) {
    return failed(error);
  }
}
