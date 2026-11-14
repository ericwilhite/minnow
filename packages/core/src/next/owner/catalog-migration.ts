/** Atomic native schema migration. The shipped planner owns proofs and managed-drop policy. */
import {
  planMigration,
  isDestructiveStep,
  type AnyTable,
  type SchemaDefinition,
  type PlanMigrationOptions,
  type MigrationPlan,
} from "../../engine/schema.js";
import type { WritePath } from "../access/write-path.js";
import type { StatementPlanner } from "./planner.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { lowerMigrationStep } from "../lower/migration.js";
import { compileTableRules } from "../exec/rules.js";
import { EXECUTOR_COSTS } from "../exec/executor.js";
import { PlanningError } from "../lower/errors.js";
import type { MemoryGovernor } from "../memory/governor.js";
import { identityMaximum } from "../write/identity-maximum.js";

export interface MigrationOptions extends PlanMigrationOptions {
  readonly allowDestructive?: boolean;
  readonly expectedVersion?: number;
  readonly signal?: AbortSignal;
}
export interface CatalogMigrationResult extends MigrationPlan {
  readonly changedTables: readonly number[];
}
export async function migrateCatalog(
  path: WritePath,
  lowerer: StatementPlanner,
  governor: MemoryGovernor,
  definition: SchemaDefinition<readonly AnyTable[]>,
  options: MigrationOptions = {},
): Promise<CatalogMigrationResult> {
  const transaction = await path.begin(options.signal ? { signal: options.signal } : undefined);
  const context = { statistics: { table: () => null }, costs: EXECUTOR_COSTS };
  try {
    if (
      options.expectedVersion !== undefined &&
      options.expectedVersion !== transaction.catalog.version
    )
      throw new PlanningError("Schema changed while planning migration", "40001");
    const plan = planMigration(inspectCatalog(transaction.catalog), definition, options);
    if (options.allowDestructive !== true && plan.steps.some(isDestructiveStep))
      throw new PlanningError("Migration would destroy data; pass allowDestructive: true", "0A000");
    const changedTables = new Set(transaction.catalog.tables.map((table) => table.id));
    for (const step of plan.steps) {
      options.signal?.throwIfAborted();
      const statement = transaction.statement({
        rules: (id) => {
          const table = statement.catalog.table(id);
          if (!table) throw new PlanningError("Missing migration table", "XX000");
          return compileTableRules(
            table,
            statement.catalog.version,
            lowerer.lowerTableRules(table, statement.catalog),
          );
        },
      });
      try {
        let start: bigint | undefined;
        if (step.kind === "set-auto-increment" && step.enabled) {
          const table = statement.catalog.tableByName(step.tableName),
            column = table?.columns.find((column) => column.name === step.columnName);
          if (!table || !column)
            throw new PlanningError("Missing identity migration column", "42703");
          const grant = await governor.admit({
            label: "migration/identity",
            minimumBytes: 4 * 1024 * 1024,
            spillable: false,
            ...(options.signal ? { signal: options.signal } : {}),
          });
          try {
            const view = await statement.view.table(table.id, { budget: grant });
            const maximum = await identityMaximum(statement.view, view, column, grant, () =>
              options.signal?.throwIfAborted(),
            );
            start = maximum !== null && maximum >= 1n ? maximum + 1n : 1n;
          } finally {
            grant.close();
          }
        }
        const change = lowerMigrationStep(step, statement.catalog, lowerer, context, start);
        if (change) await statement.changeCatalog(change);
        await statement.end();
      } catch (error) {
        statement.rollback();
        throw error;
      }
    }
    options.signal?.throwIfAborted();
    for (const table of transaction.catalog.tables) changedTables.add(table.id);
    const receipt = await transaction.commit(null);
    await receipt.durable;
    return { ...plan, changedTables: [...changedTables] };
  } catch (error) {
    transaction.rollback();
    throw error;
  }
}
