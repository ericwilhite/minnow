/** Statement bindings are funded by the root admission, and outlive all root/FK writer borrows. */
import { statementRuleReach } from "./statement-reach.js";
import type { CatalogView } from "../access/catalog.js";
import type { SpillService } from "../spill/contract.js";
import { ModifyTargetSet, MODIFY_TARGET_SET_BYTES } from "./modify-target-set.js";
import type { TableRules } from "../access/modify.js";
import type { ByteCharge } from "../vector/varlen.js";
import { BATCH_ROWS, type Batch, type Vector } from "../vector/vector.js";
import { UnsupportedPlanError } from "./errors.js";
import { bindTableRules, tableRuleMinimumBytes, type BoundTableRules } from "./row-rules.js";
import type { Pool } from "./spill-pool.js";
import type { Rel } from "../ir/ir.js";
import { expressionInputBytes, projectedInputBytes } from "./input-size-plan.js";
import { tableRuleProgram } from "./rule-program.js";
import type { Grant } from "../memory/governor.js";
import { ExpressionMemoryError } from "./kernels/vectors.js";

export class StatementRules {
  readonly #tables: number[];
  readonly requiresTriggers: boolean;
  readonly requiresReferentialRuntime: boolean;
  readonly #bound = new Map<number, BoundTableRules>();
  readonly #native = new Map<number, TableRules>();
  #targets: ModifyTargetSet | null = null;
  #closing: Promise<void> | null = null;
  readonly targets = {
    testAndSet: (tableId: number, rowId: number): Promise<boolean> => {
      if (!this.#targets)
        throw new UnsupportedPlanError("repeat tracking requires its admitted scope");
      return this.#targets.testAndSet(tableId, rowId);
    },
  };
  #charge: ByteCharge | null = null;
  #metadata = 0;
  #denied = 0;
  constructor(
    readonly catalog: CatalogView,
    readonly root: number,
    readonly lookup: (id: number) => TableRules,
    modify?: Extract<Rel, { op: "modify" }>,
  ) {
    const reach = statementRuleReach(catalog, root, modify);
    this.requiresTriggers = reach.triggers;
    this.requiresReferentialRuntime = reach.unsupportedReferentialActions;
    this.#tables = reach.tables.filter((id) => {
      const table = catalog.table(id);
      return (
        table && (table.checks.length > 0 || table.columns.some((c) => c.generatedSql !== null))
      );
    });
  }
  /** No program/buffer reference survives sizing. Eviction during admission remains safe. */
  minimumBytes(): number {
    return (
      MODIFY_TARGET_SET_BYTES +
      (this.#tables.length === 0
        ? 0
        : 4096 +
          this.#tables.length * 1024 +
          this.#tables.reduce((n, id) => n + tableRuleMinimumBytes(this.lookup(id), BATCH_ROWS), 0))
    );
  }
  progressCopies(): number {
    let generated = 0;
    for (const id of this.#tables)
      generated = Math.max(
        generated,
        this.catalog.table(id)?.columns.filter((c) => c.generatedSql !== null).length ?? 0,
      );
    return 2 * generated;
  }
  /** Logical output widths, independent of duplicated OLD/assignment aliases. */
  outputProgress(rel: Extract<Rel, { op: "modify" }>, input: readonly number[]): number | null {
    const table = this.catalog.table(rel.tableId);
    const program = tableRuleProgram(this.lookup(rel.tableId));
    if (!table || !program) return null;
    const insert = rel.kind === "insert" || rel.kind === "upsert";
    const start = insert ? 0 : 1 + rel.assigned.length;
    const widths = table.columns.map((column, i) => {
      const assigned = rel.assigned.indexOf(column.id);
      return input[!insert && assigned >= 0 ? 1 + assigned : start + i] ?? 0;
    });
    const source: Rel = {
      op: "scan",
      id: -1,
      tableId: table.id,
      columns: table.columns.map((c) => c.id),
      output: table.columns,
      filter: null,
      runtimeFilters: [],
      emitLocator: false,
    };
    const sizes = {
      parameter: () => 0,
      source: (_id: number, column: number) => widths[column] ?? null,
    };
    let generated = 0;
    for (const g of program.generated) {
      const bytes = expressionInputBytes(g.expr, source, sizes);
      if (bytes === null) return null;
      widths[g.index] = bytes;
      generated += bytes;
    }
    const returned = projectedInputBytes(rel.returning ?? [], source, sizes);
    return returned === null ? null : generated + returned;
  }
  bind(charge: ByteCharge, signal: AbortSignal, spill: SpillService, queryId: number): void {
    if (this.#charge) throw new TypeError("Statement row rules already bound");
    this.#metadata = this.#tables.length === 0 ? 0 : 4096 + this.#tables.length * 1024;
    if (!charge.allocate(this.#metadata)) throw new ExpressionMemoryError(this.#metadata);
    this.#charge = charge;
    const counted: ByteCharge = {
      allocate: (n) => {
        const ok = charge.allocate(n);
        if (!ok) this.#denied = Math.max(this.#denied, n);
        return ok;
      },
      free: (n) => charge.free(n),
    };
    try {
      this.#targets = new ModifyTargetSet(counted, spill, queryId, signal);
      for (const id of this.#tables) {
        const bound = bindTableRules(this.lookup(id), counted, { capacity: BATCH_ROWS, signal });
        this.#bound.set(id, bound);
        this.#native.set(id, {
          check: bound.check.bind(bound),
          ...(bound.validate ? { validate: bound.validate.bind(bound) } : {}),
          generate: (batch, columns) => {
            bound.retireGenerated();
            bound.generate(batch, columns);
          },
        });
      }
    } catch (error) {
      void this.close(); // Binding precedes all spill I/O; cleanup is synchronous here.
      throw error;
    }
  }
  table(id: number): TableRules {
    const bound = this.#native.get(id);
    if (bound) return bound;
    if (this.#tables.includes(id))
      throw new UnsupportedPlanError("row rules require their admitted statement binding");
    return this.lookup(id);
  }
  /** Expired outputs only. FK generation itself remains synchronous inside the native writer. */
  retire(id: number): void {
    this.#bound.get(id)?.retireGenerated();
  }
  async prepare(
    id: number,
    batch: Batch,
    generated: boolean,
    pool: Pool,
    grant: Grant,
    signal: AbortSignal,
  ): Promise<readonly Vector[]> {
    const table = this.catalog.table(id);
    if (!table) throw new TypeError("Missing rule table");
    const bound = this.#bound.get(id);
    if (!bound) {
      if (generated || table.checks.length > 0)
        throw new UnsupportedPlanError("row constraints require bound row rules");
      return batch.columns;
    }
    const run = () => {
      const release = pool.pinStep();
      try {
        const columns = generated
          ? batch.columns.map((vector, c) =>
              table.columns[c]?.generatedSql !== null ? { ...vector } : vector,
            )
          : batch.columns;
        this.#denied = 0;
        bound.retireGenerated();
        if (generated)
          bound.generate(
            { ...batch, columns },
            table.columns.map((column) => column.id),
          );
        bound.validate?.({ ...batch, columns }, null);
        return columns;
      } finally {
        release();
      }
    };
    try {
      return run();
    } catch (error) {
      if (!(error instanceof ExpressionMemoryError)) throw error;
      bound.retireGenerated();
      if ((batch.selection?.count ?? batch.length) > 1) throw error;
      await pool.need(Math.max(65536, 2 * this.#denied), grant, signal);
      return run();
    }
  }

  close(): void | Promise<void> {
    if (this.#closing) return this.#closing;
    for (const bound of this.#bound.values()) bound.close();
    this.#bound.clear();
    this.#native.clear();
    this.#charge?.free(this.#metadata);
    this.#charge = null;
    this.#metadata = 0;
    const targets = this.#targets;
    this.#targets = null;
    const closing = targets?.close();
    if (closing instanceof Promise) this.#closing = closing;
    return closing;
  }
}
