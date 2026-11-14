/** Mutation-phase kernels: evaluate RETURNING only for the physical rows the native writer accepted. */
import type { TableDef } from "../access/catalog.js";
import type { Statement } from "../access/modify.js";
import type { Rel } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { Batch, Selection } from "../vector/vector.js";
import type { CompileContext } from "./kernels/compile.js";
import type { Operator, Transfer } from "./pipeline.js";
import { ProjectOperator } from "./filter.js";

export type ModifyRel = Extract<Rel, { op: "modify" }>;
export const MODIFY_ROWS = 256;
export function returningShape(rel: ModifyRel): {
  start: number;
  width: number;
  types: readonly PhysicalType[];
} {
  const start = rel.kind === "insert" ? 0 : 1 + rel.assigned.length;
  const width = rel.input.output.length - start - (rel.passthrough ?? 0);
  return { start, width, types: rel.input.output.slice(start).map((column) => column.type) };
}
export const returningMetadata = (rel: ModifyRel): number =>
  1024 + 32 * rel.input.output.length + 2 * MODIFY_ROWS;
export class ReturningWriter implements Operator {
  readonly #written: Selection;
  #project: ProjectOperator | null;
  readonly #assigned: readonly number[];
  readonly #start: number;
  readonly #width: number;
  readonly #writer: ReturnType<Statement["table"]>;
  #pending: Promise<void> = Promise.resolve();
  constructor(
    readonly rel: ModifyRel,
    statement: Statement,
    table: TableDef,
    readonly context: CompileContext,
    output: Operator,
    readonly signal: AbortSignal,
    readonly counts: { inserted: number; updated: number; deleted: number },
  ) {
    context.arena.take(returningMetadata(rel));
    this.#written = { rows: new Uint16Array(MODIFY_ROWS), count: 0 };
    this.#assigned = rel.assigned.map((id) =>
      table.columns.findIndex((column) => column.id === id),
    );
    const shape = returningShape(rel);
    this.#start = shape.start;
    this.#width = shape.width;
    this.#writer = statement.table(rel.tableId, {
      assigned: rel.assigned,
      onRepeat: rel.onRepeat,
      uniqueTargets: rel.uniqueTargets === true,
    });
    this.#project = new ProjectOperator(rel.returning ?? [], context, output, {
      signal,
      available: () => Infinity,
    });
  }
  push(batch: Batch, transfer?: Transfer): "blocked" {
    this.#pending = (async () => {
      this.signal.throwIfAborted();
      const project = this.#project;
      if (!project) throw new TypeError("RETURNING writer is closed");
      const kind = this.rel.kind as "insert" | "update" | "delete";
      const columns = batch.columns.slice(0, kind === "insert" ? this.#width : this.#start);
      const n = await this.#writer[kind]({ ...batch, columns }, this.#written);
      this.counts[kind === "insert" ? "inserted" : kind === "update" ? "updated" : "deleted"] += n;
      if (this.#written.count === 0) return;
      const affected = kind === "insert" ? [...batch.columns] : batch.columns.slice(this.#start);
      if (kind === "update")
        for (const [j, position] of this.#assigned.entries()) {
          const value = batch.columns[1 + j];
          if (position < 0 || !value) throw new TypeError("Missing assigned RETURNING value");
          affected[position] = value;
        }
      if (
        project.push({
          length: batch.length,
          columns: affected,
          selection: this.#written,
        }) === "blocked"
      )
        await project.ready();
      this.context.arena.retireVarlen();
    })().finally(() => transfer?.release());
    return "blocked";
  }
  ready(): Promise<void> {
    return this.#pending;
  }
  finish() {
    const project = this.#project;
    if (!project) throw new TypeError("RETURNING writer is closed");
    return project.finish();
  }
  async close(): Promise<void> {
    await this.#pending.catch(() => undefined);
    // A projection can retain borrowed input wrappers after finish. Drop the
    // kernels before releasing the spill page/arena that funded those values.
    this.#project = null;
  }
  abort(reason: unknown): void {
    this.#project?.abort(reason);
  }
}
