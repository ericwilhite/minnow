/** Private representation recipe for a direct one-to-three-inner-join/grouped-float pipeline.
 * It changes equality-class representation, never join order or a transition expression.
 * Scans still evaluate their original predicates before the caller encodes their output.
 * Unknown/computed/residual/volatile shapes keep the original plan before input opens.
 */
import type { Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";

type Aggregate = Extract<Rel, { op: "aggregate" }>;
type Scan = Extract<Rel, { op: "scan" }>;
interface Origin {
  readonly scan: Scan;
  readonly index: number;
}
export interface StarBookShape {
  readonly sources: readonly Origin[];
}
export interface StarDictionaryShape {
  readonly aggregate: Aggregate;
  /** Original scan columns, including predicate dependencies. */
  readonly scans: readonly Scan[];
  readonly books: readonly StarBookShape[];
  /** Book ID by original output column, -1 for an unchanged physical type. */
  readonly columns: ReadonlyMap<number, readonly number[]>;
  readonly typed: ReadonlyMap<number, Rel>;
  readonly groupBooks: readonly number[];
}
const I32: PhysicalType = { kind: "int32", bits: 32 };

export function starDictionaryShape(rel: Aggregate, volatile: boolean): StarDictionaryShape | null {
  if (
    volatile ||
    rel.groupingSets !== null ||
    rel.groupBy.length === 0 ||
    rel.groupBy.length > 8 ||
    !rel.aggregates.every(
      (a) =>
        !a.distinct &&
        a.filter === null &&
        a.orderBy.length === 0 &&
        ((a.fn === "count_star" && a.args.length === 0) ||
          (["sum", "avg", "min", "max"].includes(a.fn) &&
            a.args.length === 1 &&
            a.args[0]?.op === "column" &&
            a.args[0].type.kind === "float64")),
    )
  )
    return null;
  const origins = new Map<number, readonly Origin[]>(),
    parents = new Map<Origin, Origin>(),
    nodes: Rel[] = [],
    scans: Scan[] = [];
  let joins = 0;
  const root = (o: Origin): Origin => {
    const p = parents.get(o);
    if (!p || p === o) return o;
    const r = root(p);
    parents.set(o, r);
    return r;
  };
  const visit = (node: Rel): readonly Origin[] | null => {
    nodes.push(node);
    let output: readonly Origin[];
    if (node.op === "scan") {
      if (node.emitLocator || node.runtimeFilters.length !== 0) return null;
      scans.push(node);
      output = node.output.map((_, index) => ({ scan: node, index }));
    } else if (node.op === "project") {
      const input = visit(node.input);
      if (!input) return null;
      const selected: Origin[] = [];
      for (const expr of node.exprs) {
        if (expr.op !== "column") return null;
        const source = input[expr.index];
        if (!source) return null;
        selected.push(source);
      }
      output = selected;
    } else if (node.op === "join") {
      if (
        node.kind !== "inner" ||
        node.algorithm !== "hash" ||
        node.residual !== null ||
        (node.filter ?? null) !== null ||
        node.nullSafe.some(Boolean) ||
        node.leftKeys.length === 0 ||
        node.leftKeys.length !== node.rightKeys.length
      )
        return null;
      joins++;
      const left = visit(node.left),
        right = visit(node.right);
      if (!left || !right) return null;
      for (let i = 0; i < node.leftKeys.length; i++) {
        const l = node.leftKeys[i],
          r = node.rightKeys[i];
        if (l?.op !== "column" || r?.op !== "column") return null;
        const a = left[l.index],
          b = right[r.index];
        if (!a || !b || l.type.kind !== "text" || r.type.kind !== "text") return null;
        parents.set(root(b), root(a));
      }
      output = [...left, ...right];
    } else return null;
    if (output.length !== node.output.length) return null;
    origins.set(node.id, output);
    return output;
  };
  const input = visit(rel.input);
  if (!input || joins < 1 || joins > 3 || scans.length !== joins + 1) return null;
  const bookIds = new Map<Origin, number>(),
    sources: Origin[][] = [];
  for (const scan of scans)
    for (const o of origins.get(scan.id) ?? []) {
      if (scan.output[o.index]?.type.kind !== "text") continue;
      const key = root(o);
      let id = bookIds.get(key);
      if (id === undefined) {
        id = sources.length;
        sources.push([]);
        bookIds.set(key, id);
      }
      sources[id]?.push(o);
    }
  const columns = new Map<number, readonly number[]>();
  for (const node of nodes)
    columns.set(
      node.id,
      (origins.get(node.id) ?? []).map((o) => bookIds.get(root(o)) ?? -1),
    );
  const groupBooks: number[] = [];
  for (const index of rel.groupBy) {
    const book = columns.get(rel.input.id)?.[index] ?? -1;
    if (book < 0) return null;
    groupBooks.push(book);
  }
  const typed = new Map<number, Rel>();
  const rewrite = (node: Rel): Rel => {
    const ids = columns.get(node.id) ?? [];
    const output = node.output.map((c, i) => ((ids[i] ?? -1) < 0 ? c : { ...c, type: I32 }));
    let result: Rel;
    if (node.op === "scan") result = { ...node, output };
    else if (node.op === "project") {
      const input = rewrite(node.input);
      result = {
        ...node,
        input,
        output,
        exprs: node.exprs.map((e) =>
          e.op === "column" ? { ...e, type: input.output[e.index]?.type ?? e.type } : e,
        ),
      };
    } else if (node.op === "join") {
      const left = rewrite(node.left),
        right = rewrite(node.right);
      const keys = (es: readonly ScalarExpr[], input: Rel) =>
        es.map((e) =>
          e.op === "column" ? { ...e, type: input.output[e.index]?.type ?? e.type } : e,
        );
      result = {
        ...node,
        left,
        right,
        output,
        leftKeys: keys(node.leftKeys, left),
        rightKeys: keys(node.rightKeys, right),
      };
    } else throw new Error("Unproven star node");
    typed.set(node.id, result);
    return result;
  };
  const child = rewrite(rel.input);
  const aggregate = {
    ...rel,
    input: child,
    output: rel.output.map((c, i) => (i < rel.groupBy.length ? { ...c, type: I32 } : c)),
  };
  typed.set(rel.id, aggregate);
  return {
    aggregate,
    scans,
    books: sources.map((sources) => ({ sources })),
    columns,
    typed,
    groupBooks,
  };
}
