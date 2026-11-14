/** Execution-owned row-rule kernels. Cached programs never retain mutable buffers or grants. */
import type { ScalarExpr } from "../ir/ir.js";
import type { Vector } from "../vector/vector.js";
import type { TableRules } from "../access/modify.js";
import type { ByteCharge } from "../vector/varlen.js";
import { BATCH_ROWS, type Batch, type Selection } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { compileExpr } from "./kernels/compile.js";
import {
  OutputArena,
  ExpressionMemoryError,
  isValidSlot,
  flatOf,
  slotIndex,
} from "./kernels/vectors.js";
import { tableRuleProgram, ruleRetentionBytes, type RuleProgram } from "./rule-program.js";

export interface BoundTableRules extends TableRules {
  close(): void;
}
export interface RowRuleContext extends RowRuleOptions {
  readonly charge: ByteCharge;
}
export interface RowRuleOptions {
  readonly capacity?: number;
  readonly signal?: AbortSignal;
}
function capacityOf(options: RowRuleOptions): number {
  const n = options.capacity ?? BATCH_ROWS;
  if (!Number.isInteger(n) || n < 1 || n > BATCH_ROWS || (n & (n - 1)) !== 0)
    throw new RangeError("Row-rule capacity must be a power of two up to BATCH_ROWS");
  return n;
}
/** The same bounded affine compiler probes used by the relational compiler; no query rows run.
 * Data-dependent varlen/coefficient growth is separate and charged at evaluation time. */
export function tableRuleMinimumBytes(rules: TableRules, capacity = BATCH_ROWS): number {
  const program = tableRuleProgram(rules);
  if (!program) return 0;
  capacityOf({ capacity });
  const probe = (rows: number) => {
    let used = 0,
      peak = 0;
    const charge: ByteCharge = {
      allocate: (n) => {
        used += n;
        peak = Math.max(peak, used);
        return true;
      },
      free: (n) => {
        used -= n;
      },
    };
    const bound = new Evaluator(program, charge, { capacity: rows });
    try {
      return peak;
    } finally {
      bound.close();
    }
  };
  const output = outputMinimum(program, capacity);
  if (capacity <= 32) return probe(capacity) + output;
  const first = probe(32),
    second = probe(64);
  return Math.ceil(first + ((second - first) * (capacity - 32)) / 32) + output;
}
function outputMinimum(program: RuleProgram, capacity: number): number {
  let total = 0,
    largest = 0;
  for (const g of program.generated) {
    const store = new ColumnStore(
      [g.expr.type],
      { allocate: () => false, free: () => undefined },
      { chunkRows: capacity },
    );
    const bytes = store.fixedBytes(capacity);
    store.close();
    total += bytes;
    largest = Math.max(largest, bytes);
  }
  return total + largest;
}
/** Bind only AFTER admission, one instance per table/statement. Close after writer/FK borrows. */
export function bindTableRules(
  rules: TableRules,
  charge: ByteCharge,
  options: RowRuleOptions = {},
): BoundTableRules {
  const program = tableRuleProgram(rules);
  return program
    ? new Evaluator(program, charge, options)
    : {
        check: rules.check.bind(rules),
        generate: rules.generate.bind(rules),
        close() {
          return undefined;
        },
      };
}
/** These nodes cannot raise a SQL expression error on valid physical inputs. */
function total(expr: ScalarExpr): boolean {
  switch (expr.op) {
    case "column":
    case "literal":
      return true;
    case "compare":
    case "distinctFrom":
      return total(expr.left) && total(expr.right);
    case "not":
    case "isNull":
      return total(expr.arg);
    case "and":
    case "or":
      return expr.args.every(total);
    default:
      return false;
  }
}
class Evaluator implements BoundTableRules {
  readonly #arena: OutputArena;
  readonly #one: Selection;
  readonly #checks;
  readonly #generated;
  #closed = false;
  constructor(
    readonly program: RuleProgram,
    readonly charge: ByteCharge,
    readonly options: RowRuleOptions,
  ) {
    const capacity = capacityOf(options);
    const arena = (this.#arena = new OutputArena(charge, capacity));

    try {
      arena.take(
        ruleRetentionBytes(program) +
          256 +
          2 +
          program.table.columns.length * 32 +
          program.checks.length * 64 +
          program.generated.length * 64,
      );
      this.#one = { rows: new Uint16Array(1), count: 1 };
      const context = {
        arena,
        params: { slots: [] },
        input: program.table.columns.map((c) => c.type),
      };
      this.#checks = program.checks.map((c) => ({
        ...c,
        kernel: compileExpr(c.condition, context),
        total: total(c.condition),
        value: null as Vector | null,
      }));
      this.#generated = program.generated.map((g) => ({
        ...g,
        kernel: compileExpr(g.expr, context),
        store: null as ColumnStore | null,
      }));
    } catch (e) {
      arena.close();
      throw e;
    }
  }

  #validate(batch: Batch): void {
    if (this.#closed) throw new TypeError("Row-rule binding is closed");
    this.options.signal?.throwIfAborted();
    if (
      !Number.isInteger(batch.length) ||
      batch.length < 0 ||
      batch.length > this.#arena.capacity ||
      batch.columns.length !== this.program.table.columns.length
    )
      throw new RangeError("Row-rule batch does not match its admitted shape");
  }
  #changed(changed: readonly number[] | null): ReadonlySet<number> | null {
    if (changed === null) return null;
    const ids = new Set(changed);
    for (const g of this.#generated)
      if (g.reads.size === 0 || changed.some((id) => g.reads.has(id))) ids.add(g.columnId);
    return ids;
  }
  check(batch: Batch, changed: readonly number[] | null): ReturnType<TableRules["check"]> {
    this.#validate(batch);
    const ids = this.#changed(changed);
    const checks = this.#checks.filter((c) => {
      if (c.reads.size === 0 || ids === null) return true;
      for (const id of c.reads) if (ids.has(id)) return true;
      return false;
    });
    // Common comparisons run once per batch. Potentially throwing expressions run only
    // when their row/constraint is reached, preserving PostgreSQL's first-error order.
    try {
      for (const c of checks) if (c.total) c.value = c.kernel.evaluate(batch, batch.selection);
      const count = batch.selection?.count ?? batch.length;
      for (let k = 0; k < count; k++) {
        this.options.signal?.throwIfAborted();
        const row = batch.selection?.rows[k] ?? k;
        this.#one.rows[0] = row;
        for (const c of checks) {
          const result = c.value ?? c.kernel.evaluate(batch, this.#one);
          if (!isValidSlot(result, row)) continue;
          const data = flatOf(result).data;
          if (data.layout !== "bool") throw new TypeError("CHECK kernel did not produce BOOLEAN");
          if (data.values[slotIndex(result, row)] === 0) return { row, checkId: c.def.id };
        }
      }
      return null;
    } finally {
      for (const c of checks) c.value = null;
    }
  }
  generate(batch: Batch, changed: readonly number[]): void {
    this.#validate(batch);
    if ((batch.selection?.count ?? batch.length) === 0) return;
    for (const g of this.#generated) {
      if (g.reads.size !== 0 && !changed.some((id) => g.reads.has(id))) continue;
      const target = batch.columns[g.index];
      if (target?.form !== "flat")
        throw new TypeError("Generated target must be an owned flat vector wrapper");
      const value = g.kernel.evaluate(batch, batch.selection);
      const store = new ColumnStore([g.expr.type], this.charge, {
        chunkRows: this.#arena.capacity,
      });
      const source = [value];
      try {
        let active = 0;
        for (let row = 0; row < batch.length; row++) {
          this.options.signal?.throwIfAborted();
          const take =
            batch.selection === null ||
            (active < batch.selection.count && batch.selection.rows[active] === row);
          if (take) active++;
          this.#one.rows[0] = row;
          source[0] = take ? value : target;
          if (store.append(source, this.#one.rows, 0, 1) !== 1)
            throw new ExpressionMemoryError(store.fixedBytes(batch.length));
        }
        const out = store.vectors()[0];
        if (!out) throw new TypeError("Missing generated result");
        // Replace the caller-owned wrapper, preserving OLD's borrowed backing buffers.
        Object.assign(target, { data: out.data, validity: out.validity, length: batch.length });
        g.store?.close();
        g.store = store;
      } catch (error) {
        store.close();
        throw error;
      }
    }
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const g of this.#generated) g.store?.close();
    this.#arena.close();
  }
}
