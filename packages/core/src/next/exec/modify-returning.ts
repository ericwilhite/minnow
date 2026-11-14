/** Mutation-phase kernels: complete scoped row rules, then RETURNING only for accepted writes. */
import type { TableDef } from "../access/catalog.js";
import type { Statement } from "../access/modify.js";
import type { Rel } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { Batch, Selection, Vector } from "../vector/vector.js";
import type { CompileContext } from "./kernels/compile.js";
import type { Operator, Transfer } from "./pipeline.js";
import { ProjectOperator } from "./filter.js";
import { ExpressionMemoryError } from "./kernels/vectors.js";
import type { StatementRules } from "./statement-rules.js";
import type { Pool } from "./spill-pool.js";
import type { Grant } from "../memory/governor.js";

export type ModifyRel = Extract<Rel, { op: "modify" }>;
export const MODIFY_ROWS = 256;
export function returningShape(rel: ModifyRel): {
  start: number;
  width: number;
  types: readonly PhysicalType[];
} {
  const start = rel.kind === "insert" || rel.kind === "upsert" ? 0 : 1 + rel.assigned.length;
  const width = rel.input.output.length - start - (rel.passthrough ?? 0);
  return { start, width, types: rel.input.output.slice(start).map((column) => column.type) };
}
export const returningMetadata = (rel: ModifyRel): number =>
  2048 + 64 * rel.input.output.length + 4 * MODIFY_ROWS;
export class ReturningWriter implements Operator {
  readonly #written: Selection;
  readonly #part: Selection;
  #project: ProjectOperator | null;
  readonly #assigned: readonly number[];
  readonly #start: number;
  readonly #width: number;
  readonly #writer: ReturnType<Statement["table"]>;
  #pending: Promise<void> = Promise.resolve();
  constructor(
    readonly rel: ModifyRel,
    statement: Statement,
    readonly table: TableDef,
    readonly context: CompileContext,
    readonly output: Operator,
    readonly signal: AbortSignal,
    readonly counts: { inserted: number; updated: number; deleted: number },
    readonly scope: StatementRules | undefined,
    readonly pool: Pool,
    readonly grant: Grant,
    uniqueTargets: boolean,
  ) {
    context.arena.take(returningMetadata(rel));
    this.#written = { rows: new Uint16Array(MODIFY_ROWS), count: 0 };
    this.#part = { rows: new Uint16Array(MODIFY_ROWS), count: 0 };
    this.#assigned = rel.assigned.map((id) =>
      table.columns.findIndex((column) => column.id === id),
    );
    const shape = returningShape(rel);
    this.#start = shape.start;
    this.#width = shape.width;
    this.#writer = statement.table(rel.tableId, {
      assigned: rel.assigned,
      onRepeat: rel.onRepeat,
      uniqueTargets,
      ...(!uniqueTargets && rel.kind !== "insert" ? { requireTrackedTargets: true } : {}),
    });
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
  async #write(batch: Batch): Promise<void> {
    const total = batch.selection?.count ?? batch.length;
    for (let from = 0; from < total;) {
      this.signal.throwIfAborted();
      let count = total - from;
      let prepared: { columns: readonly Vector[]; affected: readonly Vector[] };
      for (;;) {
        for (let k = 0; k < count; k++)
          this.#part.rows[k] = batch.selection?.rows[from + k] ?? from + k;
        this.#part.count = count;
        try {
          prepared = await this.#prepare({ ...batch, selection: this.#part });
          break;
        } catch (error) {
          if (!(error instanceof ExpressionMemoryError) || count === 1) throw error;
          count = Math.max(1, Math.floor(count / 2));
        }
      }
      const kind = this.rel.kind as "insert" | "update" | "delete";
      this.pool.trimStep();
      const idle = this.grant.held - this.grant.allocated - this.grant.loaned;
      if (idle > 0) this.grant.release(idle);
      const written = await this.#writer[kind](
        { ...batch, selection: this.#part, columns: prepared.columns },
        this.#written,
      );
      this.counts[kind === "insert" ? "inserted" : kind === "update" ? "updated" : "deleted"] +=
        written;
      const project = this.#project;
      if (project && this.#written.count > 0) {
        if (
          project.push({
            length: batch.length,
            columns: prepared.affected,
            selection: this.#written,
          }) === "blocked"
        )
          await project.ready();
      }
      this.context.arena.retireVarlen();
      this.scope?.retire(this.table.id);
      from += count;
    }
  }
  async #prepare(
    batch: Batch,
  ): Promise<{ columns: readonly Vector[]; affected: readonly Vector[] }> {
    const kind = this.rel.kind;
    let write = batch.columns.slice(0, kind === "insert" ? this.#width : this.#start);
    let affected = kind === "insert" ? [...batch.columns] : batch.columns.slice(this.#start);
    if (kind === "update" && this.#width > 0) {
      for (const [j, position] of this.#assigned.entries()) {
        const value = batch.columns[j + 1];
        if (position < 0 || !value) throw new TypeError("Missing affected assignment");
        affected[position] = value;
      }
    }
    if (kind !== "delete" && this.#width > 0 && this.scope) {
      const complete = await this.scope.prepare(
        this.table.id,
        { ...batch, columns: affected.slice(0, this.#width) },
        this.rel.deferGenerated === true,
        this.pool,
        this.grant,
        this.signal,
      );
      affected = [...complete, ...affected.slice(this.#width)];
      if (kind === "insert") write = [...complete];
      else {
        const locator = batch.columns[0];
        if (!locator) throw new TypeError("Missing update locator");
        write = [
          locator,
          ...this.#assigned.map((position) => {
            const value = complete[position];
            if (!value) throw new TypeError("Missing completed assignment");
            return value;
          }),
        ];
      }
    } else if (kind !== "delete" && this.rel.deferGenerated)
      throw new TypeError("Generated writes need their admitted row-rule scope");
    return { columns: write, affected };
  }
  ready() {
    return this.#pending;
  }
  finish() {
    return this.#project ? this.#project.finish() : this.output.finish();
  }
  async close(): Promise<void> {
    await this.#pending.catch(() => undefined);
    this.#project = null;
  }
  abort(reason: unknown): void {
    this.#project?.abort(reason);
  }
}
