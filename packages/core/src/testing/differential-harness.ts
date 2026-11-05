import type { CreateTableInput, DatabaseRow } from "../engine/database.js";
import type { QueryParameter } from "../engine/query.js";

/**
 * Rebuild differential contract (version 1).
 *
 * Identity compares column names in projection order, statement kind, affected counts,
 * RETURNING rows, and values. Rows are a duplicate-preserving multiset unless the workload
 * supplies a proof of total ORDER BY (a unique key, or all projected values). Merely seeing
 * ORDER BY is insufficient. LIMIT with unresolved ties must not be generated.
 *
 * Values retain their public SQL representation: NUMERIC and DATE strings are exact,
 * including NUMERIC scale; boolean, null, arrays and blobs have distinct tags. JSON text
 * remains exact text; object-form JSON has sorted keys. Datetimes use ISO UTC text.
 * Numbers use IEEE-754 bytes, preserving -0 and infinities; NaNs share one tag because JS
 * does not promise a NaN payload. No floating tolerance is granted, including aggregates:
 * sources must use exact inputs or explicitly exclude order-dependent floating aggregates.
 * Errors compare constructor name and optional code, never messages or stacks.
 * Storage IDs, timings, bytes and manifest versions are deliberately outside SQL identity.
 *
 * A frozen answer is evidence of old behaviour, NOT authority. Changes require a documented
 * SQL/type contract or PostgreSQL authority. Factory open and close failures escape the runner.
 * Workloads are serial histories, not a scheduler or a durability/crash oracle.
 */
export type StoreKind = "memory" | "indexeddb" | "opfs";
export type Operation =
  | { kind: "sql"; sql: string; params?: readonly QueryParameter[]; totalOrder?: string }
  | { kind: "schema"; schema: CreateTableInput }
  | { kind: "insertBatch" | "upsertBatch"; table: string; rows: DatabaseRow[] }
  | { kind: "deleteBatch"; table: string; keys: Array<string | number> }
  | { kind: "reopen" };
export interface Workload {
  name: string;
  operations: Operation[];
}
export interface EngineResult {
  kind: string;
  columns?: readonly string[];
  domains?: readonly unknown[];
  rows?: ReadonlyArray<Readonly<Record<string, unknown>>>;
  counts?: Readonly<Record<string, number>>;
}
export interface EngineUnderTest {
  run(operation: Operation): Promise<EngineResult>;
  close(): Promise<void>;
}
export type EngineFactory = (store: StoreKind) => Promise<EngineUnderTest>;
export type CanonicalOutcome =
  | {
      ok: true;
      kind: string;
      columns: readonly string[];
      domains: string;
      rows: string[];
      counts: string;
    }
  | { ok: false; error: string; code: string | null };

export function encodeValue(value: unknown): string {
  if (value === null) return '["null"]';
  if (typeof value === "number") {
    if (Number.isNaN(value)) return '["number","NaN"]';
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, value);
    return JSON.stringify([
      "number",
      Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join(""),
    ]);
  }
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "bigint")
    return JSON.stringify([typeof value, String(value)]);
  if (value instanceof Date) return JSON.stringify(["datetime", value.toISOString()]);
  if (value instanceof Uint8Array) return JSON.stringify(["blob", [...value]]);
  if (Array.isArray(value)) return JSON.stringify(["array", value.map(encodeValue)]);
  if (typeof value === "object")
    return JSON.stringify([
      "object",
      Object.keys(value)
        .sort()
        .map((key) => [key, encodeValue((value as Record<string, unknown>)[key])]),
    ]);
  throw new TypeError("Unsupported differential value");
}

export function canonicalResult(result: EngineResult, operation: Operation): CanonicalOutcome {
  const columns = [...(result.columns ?? [])];
  const rows = (result.rows ?? []).map((row) => encodeValue(columns.map((column) => row[column])));
  if (operation.kind !== "sql" || operation.totalOrder === undefined) rows.sort();
  return {
    ok: true,
    kind: result.kind,
    columns,
    domains: encodeValue(result.domains ?? []),
    rows,
    counts: encodeValue(result.counts ?? {}),
  };
}
export async function replay(
  engine: EngineUnderTest,
  workload: Workload,
): Promise<CanonicalOutcome[]> {
  const outcomes: CanonicalOutcome[] = [];
  for (const operation of workload.operations) {
    let result: EngineResult;
    try {
      result = await engine.run(operation);
    } catch (error) {
      if (!(error instanceof Error)) throw error;
      const code = "code" in error ? error.code : undefined;
      if (
        code !== undefined &&
        code !== null &&
        typeof code !== "string" &&
        typeof code !== "number"
      )
        throw new TypeError("Error code must be a string or number", { cause: error });
      outcomes.push({
        ok: false,
        error: error.constructor.name,
        code: code === undefined || code === null ? null : String(code),
      });
      continue;
    }
    // Encoding failures are harness defects, never engine errors.
    outcomes.push(canonicalResult(result, operation));
  }
  return outcomes;
}
export function assertIdentical(
  workload: Workload,
  expected: CanonicalOutcome[],
  actual: CanonicalOutcome[],
): void {
  if (expected.length !== actual.length) throw new Error(workload.name + ": outcome count");
  for (let i = 0; i < expected.length; i++)
    if (JSON.stringify(expected[i]) !== JSON.stringify(actual[i]))
      throw new Error(
        JSON.stringify({
          workload: workload.name,
          step: i,
          operation: workload.operations[i],
          expected: expected[i],
          actual: actual[i],
        }),
      );
}
export async function runWorkload(
  factory: EngineFactory,
  store: StoreKind,
  workload: Workload,
): Promise<CanonicalOutcome[]> {
  const engine = await factory(store);
  try {
    return await replay(engine, workload);
  } finally {
    await engine.close();
  }
}

/** Compare in lockstep, retaining only one statement's answers. Large SQLLogicTest profiles
 * must not retain every result in memory. Factories/cleanup belong to the caller so this also
 * works with externally owned candidate connections. Observation happens only after agreement. */
export async function compareEngines(
  workload: Workload,
  engines: readonly EngineUnderTest[],
  observe?: (outcome: CanonicalOutcome) => void,
): Promise<void> {
  if (engines.length < 2) throw new RangeError("Differential comparison needs two engines");
  for (const [step, operation] of workload.operations.entries()) {
    const single = { name: `${workload.name}:step-${String(step)}`, operations: [operation] };
    const reference = engines[0];
    if (reference === undefined) throw new Error("Missing reference engine");
    const expected = await replay(reference, single);
    for (const candidate of engines.slice(1))
      assertIdentical(single, expected, await replay(candidate, single));
    const outcome = expected[0];
    if (outcome !== undefined) observe?.(outcome);
  }
}
