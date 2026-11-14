/** One proposal at a time: arbitrate against prior writes before evaluating EXCLUDED. */
import type { TableDef } from "../access/catalog.js";
import { CardinalityViolationError, ConflictKind, type Statement } from "../access/modify.js";
import type { TableSnapshot } from "../access/snapshot.js";
import type { StatementRules } from "./statement-rules.js";
import type { Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { locatorPosition, locatorSegment } from "../vector/locator.js";
import type { Batch, Vector } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { ProjectOperator } from "./filter.js";
import { selectedSegment } from "./late-fetch.js";
import { compileExpr, type CompileContext } from "./kernels/compile.js";
import {
  ExpressionMemoryError,
  isValidSlot,
  flatOf,
  slotIndex,
  type Compiled,
} from "./kernels/vectors.js";
import type { ModifyRel } from "./modify-returning.js";
import { pushAndWait, type Operator, type Transfer } from "./pipeline.js";
import type { ChunkReads } from "./scan.js";
import { sortRowView, SORT_ROW_VIEW_BYTES } from "./sort-row.js";
import type { Pool } from "./spill-pool.js";
import { TableScan, memoryScanBufferBytes } from "./table-scan.js";

export function upsertTypes(rel: ModifyRel): readonly PhysicalType[] {
  const n = rel.input.output.length - (rel.passthrough ?? 0);
  const table = rel.input.output.slice(0, n).map((column) => column.type);
  return [
    ...table,
    ...table,
    ...(rel.onConflict?.assignments.map((a) => a.expr.type) ?? []),
    ...rel.input.output.slice(n).map((column) => column.type),
  ];
}
export function upsertMetadata(rel: ModifyRel): number {
  const n = rel.input.output.length - (rel.passthrough ?? 0);
  return 8192 + SORT_ROW_VIEW_BYTES * (rel.input.output.length + 2 * n);
}
export function upsertWorkspace(rel: ModifyRel): number {
  const n = rel.input.output.length - (rel.passthrough ?? 0);
  return 262144 + memoryScanBufferBytes(rel.input.output.slice(0, n).map((c) => c.type)) + n * 8192;
}
/** Also used by immutable compiler sizing, with no native writer or read side effects. */
export class UpsertExpressions {
  readonly where: Compiled | null;
  readonly assignments: readonly Compiled[];
  constructor(rel: ModifyRel, context: CompileContext) {
    this.where = rel.onConflict?.where ? compileExpr(rel.onConflict.where, context) : null;
    this.assignments = rel.onConflict?.assignments.map((a) => compileExpr(a.expr, context)) ?? [];
  }
}
export class UpsertWriter implements Operator {
  readonly #insert: ReturnType<Statement["table"]>;
  readonly #update: ReturnType<Statement["table"]>;
  readonly #conflicts: {
    kinds: Uint8Array;
    locators: Float64Array;
    earlier: Int32Array;
    indexIds: Int32Array;
  };
  readonly #locator: ReturnType<CompileContext["arena"]["flat"]>;
  readonly #old: ColumnStore;
  readonly #assigned: ColumnStore;
  readonly #kernels: UpsertExpressions;
  #project: ProjectOperator | null;
  #table: TableSnapshot | null = null;
  #pending: Promise<void> = Promise.resolve();
  constructor(
    readonly rel: ModifyRel,
    readonly statement: Statement,
    readonly table: TableDef,
    readonly context: CompileContext,
    readonly output: Operator,
    readonly pool: Pool,
    readonly grant: Grant,
    readonly reads: ChunkReads,
    readonly signal: AbortSignal,
    readonly counts: { inserted: number; updated: number; deleted: number },
    readonly scope: StatementRules | undefined,
  ) {
    context.arena.take(upsertMetadata(rel));
    this.#conflicts = {
      kinds: new Uint8Array(1),
      locators: new Float64Array(1),
      earlier: new Int32Array(1),
      indexIds: new Int32Array(1),
    };
    this.#locator = context.arena.flat({ kind: "locator", tableId: table.id });
    this.#old = new ColumnStore(
      table.columns.map((c) => c.type),
      pool,
      { chunkRows: 1, targetBytes: 65536 },
    );
    this.#assigned = new ColumnStore(
      rel.onConflict?.assignments.map((a) => a.expr.type) ?? [],
      pool,
      { chunkRows: 1, targetBytes: 65536 },
    );
    this.#insert = statement.table(table.id);
    this.#update = statement.table(table.id, {
      assigned: rel.onConflict?.assignments.map((a) => a.column) ?? [],
      onRepeat: "error",
      requireTrackedTargets: true,
    });
    this.#kernels = new UpsertExpressions(rel, { ...context, input: upsertTypes(rel) });
    this.#project =
      rel.returning === null
        ? null
        : new ProjectOperator(rel.returning, context, output, {
            signal,
            available: () => Infinity,
          });
  }
  push(batch: Batch, transfer?: Transfer): "blocked" {
    this.#pending = this.#write(batch).finally(() => transfer?.release());
    return "blocked";
  }
  ready(): Promise<void> {
    return this.#pending;
  }
  async #fetch(locator: number): Promise<void> {
    const snapshot = this.statement.view;
    const original = (this.#table ??= await snapshot.table(this.table.id, { budget: this.pool }));
    const source = original.segments[locatorSegment(locator)];
    if (!source || locatorPosition(locator) >= source.rowCount)
      throw new RangeError("Conflict locator is outside statement snapshot");
    this.#old.reset(true);
    const segment = selectedSegment(source, 0, new Uint16Array([locatorPosition(locator)]));
    const scan = new TableScan({
      table: {
        tableId: original.tableId,
        schemaVersion: original.schemaVersion,
        columns: original.columns,
        liveRowCount: 1,
        segments: [segment],
      },
      columns: this.table.columns.map((c) => c.id),
      filter: null,
      emitLocator: false,
      context: { ...this.context, input: this.table.columns.map((c) => c.type) },
      budget: this.pool,
      pages: snapshot.pages,
      reads: this.reads,
      boundedMemory: true,
      targetBytes: 16384,
      rowsNeeded: () => 1,
      loans: {
        borrow: async (bytes) => {
          await this.pool.need(bytes, this.grant, this.signal);
          return { bytes, repay: () => undefined };
        },
      },
    });
    try {
      const item = await scan.next();
      if (!item || (item.batch.selection?.count ?? item.batch.length) !== 1)
        throw new RangeError("Conflict row disappeared");
      try {
        const row = item.batch.selection?.rows[0] ?? 0;
        await this.pool.need(
          2 * this.#old.appendNeed(item.batch.columns, row) + 65536,
          this.grant,
          this.signal,
        );
        if (this.#old.append(item.batch.columns, item.batch.selection?.rows ?? null, 0, 1) !== 1)
          throw new RangeError("Conflict row copy was not funded");
      } finally {
        item.transfer?.release();
      }
    } finally {
      scan.abort();
    }
  }
  async #write(batch: Batch): Promise<void> {
    const conflict = this.rel.onConflict;
    if (!conflict) throw new TypeError("Missing ON CONFLICT clause");
    const n = this.table.columns.length;
    for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
      this.signal.throwIfAborted();
      const row = batch.selection?.rows[k] ?? k;
      let proposed: Vector[] = batch.columns.map((column) => sortRowView(column, row));
      if (this.scope) {
        const complete = await this.scope.prepare(
          this.table.id,
          { length: 1, selection: null, columns: proposed.slice(0, n) },
          this.rel.deferGenerated === true,
          this.pool,
          this.grant,
          this.signal,
        );
        proposed = [...complete, ...proposed.slice(n)];
      } else if (this.rel.deferGenerated)
        throw new TypeError("Generated upsert needs bound row rules");
      const input: Batch = { length: 1, selection: null, columns: proposed.slice(0, n) };
      await this.#insert.conflicts(input, conflict.arbiters, this.#conflicts);
      const kind = this.#conflicts.kinds[0];
      let affected: Vector[];
      if (kind === ConflictKind.none) {
        this.pool.trimStep();
        const idle = this.grant.held - this.grant.allocated - this.grant.loaned;
        if (idle > 0) this.grant.release(idle);
        this.counts.inserted += await this.#insert.insert(input, null);
        affected = proposed;
      } else {
        if (conflict.action === "nothing") {
          this.scope?.retire(this.table.id);
          continue;
        }
        if (kind === ConflictKind.statement || kind === ConflictKind.batch)
          throw new CardinalityViolationError(this.table.id, row);
        const locator = this.#conflicts.locators[0] ?? -1;
        await this.#fetch(locator);
        const current = this.#old.vectors();
        const values: Vector[] = [
          ...current,
          ...proposed.slice(0, n),
          ...this.#kernels.assignments.map(() => this.#locator.vector),
          ...proposed.slice(n),
        ];
        const evaluate: Batch = { length: 1, selection: null, columns: values };
        const condition = this.#kernels.where?.evaluate(evaluate, null);
        if (condition) {
          const data = flatOf(condition).data;
          if (data.layout !== "bool") throw new TypeError("Conflict WHERE must be boolean");
          if (!isValidSlot(condition, 0) || data.values[slotIndex(condition, 0)] !== 1) {
            this.scope?.retire(this.table.id);
            this.context.arena.retireVarlen();
            this.#old.reset(true);
            continue;
          }
        }
        for (const [j, kernel] of this.#kernels.assignments.entries())
          values[2 * n + j] = kernel.evaluate(evaluate, null);
        const locatorVector = this.#locator.vector;
        if (locatorVector.data.layout !== "f64") throw new TypeError("Conflict locator layout");
        locatorVector.data.values[0] = locator;
        locatorVector.length = 1;
        let assigned = values.slice(2 * n, 2 * n + this.#kernels.assignments.length);
        if (this.rel.deferGenerated) {
          this.#assigned.reset(true);
          await this.pool.need(
            2 * this.#assigned.appendNeed(assigned, 0) + 65536,
            this.grant,
            this.signal,
          );
          if (this.#assigned.append(assigned, null, 0, 1) !== 1)
            throw new ExpressionMemoryError(this.#assigned.appendNeed(assigned, 0));
          assigned = this.#assigned.vectors();
        }
        let next: Vector[] = [...current];
        for (const [j, assignment] of conflict.assignments.entries()) {
          const at = this.table.columns.findIndex((column) => column.id === assignment.column),
            value = assigned[j];
          if (at < 0 || !value) throw new TypeError("Missing conflict assignment");
          next[at] = value;
        }
        if (this.scope)
          next = [
            ...(await this.scope.prepare(
              this.table.id,
              { length: 1, selection: null, columns: next },
              this.rel.deferGenerated === true,
              this.pool,
              this.grant,
              this.signal,
            )),
          ];
        const writes = conflict.assignments.map((assignment) => {
          const value =
            next[this.table.columns.findIndex((column) => column.id === assignment.column)];
          if (!value) throw new TypeError("Missing generated conflict assignment");
          return value;
        });
        this.pool.trimStep();
        const idle = this.grant.held - this.grant.allocated - this.grant.loaned;
        if (idle > 0) this.grant.release(idle);
        this.counts.updated += await this.#update.update(
          { length: 1, selection: null, columns: [locatorVector, ...writes] },
          null,
        );
        affected = [...next, ...proposed.slice(n)];
      }
      if (this.#project)
        await pushAndWait(this.#project, { length: 1, selection: null, columns: affected });
      this.context.arena.retireVarlen();
      this.scope?.retire(this.table.id);
      this.#old.reset(true);
      this.#assigned.reset(true);
    }
  }
  finish() {
    return this.#project?.finish() ?? this.output.finish();
  }
  abort(reason: unknown): void {
    this.#project?.abort(reason);
  }
  async close(): Promise<void> {
    await this.#pending.catch(() => undefined);
    this.#project = null;
    this.#assigned.close();
    this.#old.close();
    this.#table = null;
  }
}
