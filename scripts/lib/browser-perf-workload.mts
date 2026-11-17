/** A baseline may judge only the exact workload it measured. No SQL normalization. */
import { createHash } from "node:crypto";
import {
  GENERATOR_VERSION,
  POINT_CELLS,
  POINT_ROWS,
  POINT_UUID_ROWS,
  analyticsDataset,
  analyticsCells,
  principalRows,
  sqlFor,
} from "../browser-perf/catalogue.ts";
import {
  POINT_TABLES,
  INSERT_SQL,
  READ_SQL,
  LITERAL_READ_SQL,
  UPDATE_SQL,
} from "../browser-perf/point-sql.ts";

export function workloadHash(sql: unknown, dataset: unknown): string {
  return createHash("sha256").update(JSON.stringify({ sql, dataset })).digest("hex");
}

export function browserWorkload(cellId: string): string {
  const [id, name] = cellId.split("/");
  if (id === "point") {
    if (!POINT_CELLS.some((cell) => cell === name)) throw new Error(`Unknown workload ${cellId}`);
    // Point cells include imports, maintenance, concurrent clients and read modes, not just a
    // SELECT. Include the whole executed SQL family and both loaded tables conservatively.
    return workloadHash(
      Object.keys(POINT_TABLES).map((table) => {
        const t = table as keyof typeof POINT_TABLES;
        return [
          POINT_TABLES[t],
          INSERT_SQL(t),
          READ_SQL(t),
          LITERAL_READ_SQL(t, "<key>"),
          UPDATE_SQL(t),
        ];
      }),
      { id, cell: name, rows: POINT_ROWS, uuidRows: POINT_UUID_ROWS, generator: GENERATOR_VERSION },
    );
  }
  const dataset = analyticsDataset(id ?? "");
  const cell = analyticsCells(dataset).find((c) => c.id === cellId);
  if (cell === undefined) throw new Error(`Unknown workload ${cellId}`);
  return workloadHash(
    {
      minnow: sqlFor(cell.query, "minnow", principalRows(dataset)),
      duckdb: sqlFor(cell.query, "duckdb", principalRows(dataset)),
      sqlite: sqlFor(cell.query, "sqlite", principalRows(dataset)),
    },
    { ...dataset, generator: GENERATOR_VERSION, budgetBytes: cell.budget?.bytes ?? null },
  );
}

/** S_q7 changed deliberately; only hosted evidence may replace its old calibration. */
export function calibrationProvenance(cellId: string, env: NodeJS.ProcessEnv) {
  if (!cellId.endsWith("/S_q7")) return undefined;
  if (
    env.GITHUB_ACTIONS !== "true" ||
    !/^\d+$/.test(env.GITHUB_RUN_ID ?? "") ||
    !env.GITHUB_REPOSITORY
  )
    throw new Error(
      "S_q7 recalibration requires the hosted recalibrate dispatch, authority 4e7b6a0c",
    );
  return {
    authority: "4e7b6a0c: PostgreSQL-valid NUMERIC ROUND workload correction",
    run: `https://github.com/${env.GITHUB_REPOSITORY}/actions/runs/${env.GITHUB_RUN_ID}`,
  };
}

/** A targeted correction must record a measured, verified success, never normalize a failure. */
export function requireMeasuredCalibration(
  observation: import("./browser-perf-baseline.mts").CellObservation,
): void {
  if (!observation.cell.endsWith("/S_q7")) return;
  if (
    !observation.minnow.ok ||
    observation.referenceOutcome?.ok !== true ||
    observation.minnow.timing === undefined ||
    observation.referenceOutcome.timing === undefined ||
    !["match", "tolerance", "ties"].includes(observation.minnow.check ?? "")
  )
    throw new Error(
      "S_q7 recalibration requires successful, result-verified measurements from both engines",
    );
}
