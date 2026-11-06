/**
 * Lowering plus the IR reference interpreter as a small database for tests: DDL applies to an
 * in-memory catalog, plans run row by row (ir-reference.ts). Used by the differential tests
 * against the current engine and against PostgreSQL (PGlite).
 */
import type { ExecutorCosts } from "../ir/costs.js";
import { createLowerer } from "../lower/create.js";
import type {
  LoweredModify,
  LoweredStatement,
  LoweringContext,
  WriteShape,
} from "../lower/lowerer.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import type { PhysicalType } from "../types.js";
import { checkPlan } from "./ir-check.js";
import { run, withInitPlans, type Environment, type Row, type Value } from "./ir-reference.js";

export const TEST_COSTS: ExecutorCosts = {
  decodePerValue: 1,
  filterPerRow: 1,
  hashBuildPerRow: 20,
  hashProbePerRow: 15,
  aggregatePerRow: 5,
  sortPerRowLog: 3,
  emitPerValue: 2,
  spillBytesPerSecond: 600e6,
};

/** A reference value in the form tests compare: integers as numbers, floats to 12 digits. */
export function comparable(value: Value): unknown {
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number" && !Number.isInteger(value)) return Number(value.toPrecision(12));
  return value;
}

/** A client value bound to a slot of `type`. */
export function bindValue(value: unknown, type: PhysicalType): Value {
  if (value === null || value === undefined) return null;
  if (type.kind === "int32" || type.kind === "int64") return BigInt(value as number);
  if (type.kind === "float64" || type.kind === "float32") return Number(value);
  if (type.kind === "bool") return Boolean(value);
  return typeof value === "string" ? value : JSON.stringify(value);
}

export type ResultRow = Record<string, unknown>;

export class LoweredDatabase {
  readonly catalog = new MemoryCatalog();
  readonly tables = new Map<number, Row[]>();
  readonly sequences = new Map<number, bigint>();
  readonly lowerer = createLowerer();

  context(): LoweringContext {
    return { catalog: this.catalog.view, statistics: { table: () => null }, costs: TEST_COSTS };
  }

  lower(sql: string): LoweredStatement {
    return this.lowerer.lower(sql, this.context());
  }

  /** Lowers and runs one statement (DDL and its follow-up included). */
  execute(sql: string, params: readonly unknown[] = []): ResultRow[] {
    const lowered = this.lower(sql);
    if (lowered.kind === "ddl") {
      if (lowered.change !== null) {
        const created = this.catalog.apply(lowered.change);
        if (lowered.change.kind === "createTable" && created.id !== null)
          this.tables.set(created.id, []);
      }
      return lowered.followUp === null ? [] : this.execute(lowered.followUp);
    }
    if (lowered.kind !== "query" && lowered.kind !== "modify")
      throw new Error(`Cannot run ${lowered.kind}`);
    return this.runPlan(lowered, params, []);
  }

  /** A batch write (`Lowerer.lowerWrite`) of `rows`, in `shape.names` order. */
  write(shape: WriteShape, rows: ReadonlyArray<readonly unknown[]>): ResultRow[] {
    const lowered = this.lowerer.lowerWrite(shape, this.context());
    const root = lowered.plan.root;
    const input = root.op === "modify" ? findInput(root) : null;
    const types = input?.output.map((column) => column.type) ?? [];
    return this.runPlan(
      lowered,
      [],
      rows.map((row) =>
        row.map((value, index) => bindValue(value, types[index] ?? { kind: "text" })),
      ),
    );
  }

  runPlan(
    lowered: LoweredStatement & { readonly kind: "query" | "modify" },
    params: readonly unknown[],
    input: readonly Row[],
  ): ResultRow[] {
    const plan = lowered.plan;
    // Every slot: placeholders, then the clock (a fixed instant); init plans run lazily, on the
    // first read of their slot (R24.1).
    const slots: Value[] = plan.parameters.map((type, index) =>
      index < plan.placeholderCount ? bindValue(params[index], type) : null,
    );
    if (plan.clockSlot !== null) slots[plan.clockSlot] = 820_454_400_000_000n;
    const env: Environment = withInitPlans(plan, {
      tables: this.tables,
      catalog: this.catalog.view,
      params: slots,
      rules: (table) => this.lowerer.lowerTableRules(table, this.catalog.view),
      sequences: this.sequences,
      input,
    });
    // Every plan the tests run is also checked for well-formed types (ir-check.ts).
    checkPlan(lowered.plan, this.catalog.view);
    // Statement atomicity: a failing statement leaves the tables as they were.
    const saved = new Map([...this.tables].map(([id, rows]) => [id, rows.map((row) => [...row])]));
    try {
      const rows = run(plan.root, env);
      return rows.map((row) =>
        Object.fromEntries(
          lowered.columns.map((column, index) => [column.name, comparable(row[index] ?? null)]),
        ),
      );
    } catch (error) {
      for (const [id, rows] of saved) this.tables.set(id, rows);
      throw error;
    }
  }
}

function findInput(
  rel: LoweredModify["plan"]["root"],
): Extract<typeof rel, { op: "input" }> | null {
  if (rel.op === "input") return rel;
  return "input" in rel && typeof rel.input === "object" ? findInput(rel.input) : null;
}
