/** Identify repeated immutable bindings for result-copy admission. Each logical output
 * still owns independent mutable buffers. Computed values and new producer rows stay distinct.
 */
import type { Rel } from "../ir/ir.js";
import { samePhysicalType, type PhysicalType } from "../types.js";

export interface ResultCopyLayout {
  readonly sources: readonly number[];
  readonly restore: readonly number[];
  readonly types: readonly PhysicalType[];
  /** Every repeated variable binding is an unchanged value from a table scan. */
  readonly sourceBacked: boolean;
  readonly variableCopies: number;
}
export function outputCopyLayout(root: Rel): ResultCopyLayout | null {
  const sourceBindings = new Set<symbol>();
  const origins = (rel: Rel): symbol[] => {
    if (rel.op === "scan")
      return rel.output.map(() => {
        const binding = Symbol();
        sourceBindings.add(binding);
        return binding;
      });
    if (rel.op === "project") {
      const input = origins(rel.input);
      return rel.exprs.map((expr) => {
        const source = expr.op === "column" ? rel.input.output[expr.index] : undefined;
        return expr.op === "column" && source && samePhysicalType(source.type, expr.type)
          ? (input[expr.index] ?? Symbol())
          : Symbol();
      });
    }
    if (["sort", "filter", "limit", "window"].includes(rel.op) && "input" in rel) {
      const input = origins(rel.input);
      return rel.output.map((column, index) => {
        const source = rel.input.output[index];
        return source && samePhysicalType(source.type, column.type)
          ? (input[index] ?? Symbol())
          : Symbol();
      });
    }
    return rel.output.map(() => Symbol());
  };
  const fields = origins(root),
    seen = new Map<symbol, number>();
  const sources: number[] = [],
    restore: number[] = [];
  for (const [index, field] of fields.entries()) {
    const previous = seen.get(field);
    if (previous !== undefined) restore.push(previous);
    else {
      seen.set(field, sources.length);
      restore.push(sources.length);
      sources.push(index);
    }
  }
  if (sources.length === root.output.length) return null;
  const counts = new Uint32Array(sources.length);
  for (const slot of restore) counts[slot] = (counts[slot] ?? 0) + 1;
  const variable = (physical: number): boolean =>
    ["text", "bytes", "decimal", "array", "json", "enum"].includes(
      root.output[sources[physical] ?? -1]?.type.kind ?? "",
    );
  let variableCopies = 1;
  for (let physical = 0; physical < counts.length; physical++)
    if (variable(physical)) variableCopies = Math.max(variableCopies, counts[physical] ?? 1);
  return {
    sources,
    restore,
    variableCopies,
    sourceBacked: sources.every(
      (index, physical) =>
        !variable(physical) ||
        counts[physical] === 1 ||
        sourceBindings.has(fields[index] ?? Symbol()),
    ),
    types: sources.map((source) => {
      const column = root.output[source];
      if (!column) throw new TypeError("Result column outside layout");
      return column.type;
    }),
  };
}

/** Count terminal aliases of one checkpoint row. Only unchanged positional wrappers
 * are certified here; expanding expressions retain their own complete-row phase. */
export function checkpointDeliveryCopies(root: Rel, producer: Rel): readonly number[] | undefined {
  const route = (rel: Rel): Array<number | null> | null => {
    if (rel.id === producer.id) return rel.output.map((_, i) => i);
    if (!("input" in rel)) return null;
    const input = route(rel.input);
    if (!input) return null;
    if (rel.op === "project")
      return rel.exprs.map((e) => (e.op === "column" ? (input[e.index] ?? null) : null));
    if (["filter", "sort", "limit"].includes(rel.op))
      return rel.output.map((_, i) => input[i] ?? null);
    return null;
  };
  const fields = route(root);
  if (!fields) return undefined;
  const copies = producer.output.map(() => 0);
  for (const field of fields) if (field !== null) copies[field] = (copies[field] ?? 0) + 1;
  return copies.some((n) => n > 1) ? copies : undefined;
}
