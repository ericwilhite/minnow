/** JSON expression placement and conservative document provenance for fixed admission. */
import type { Rel, ScalarExpr } from "../ir/ir.js";

/** Constants can feed VALUES/derived sources as well as a direct extractor argument. */
export function jsonLiteralBytes(value: unknown): number {
  if (value === null || typeof value !== "object" || ArrayBuffer.isView(value)) return 0;
  if ("op" in value && value.op === "literal" && "value" in value) {
    const vector = value.value as {
      validity: Uint32Array | null;
      data: { layout: string; offsets?: Int32Array };
    };
    if (vector.validity !== null && ((vector.validity[0] ?? 0) & 1) === 0) return 0;
    return vector.data.layout === "varlen"
      ? Math.max(0, (vector.data.offsets?.[1] ?? 0) - (vector.data.offsets?.[0] ?? 0))
      : 0;
  }
  let maximum = 0;
  for (const child of Array.isArray(value) ? value : Object.values(value))
    maximum = Math.max(maximum, jsonLiteralBytes(child));
  return maximum;
}

/** Follow documents through relational projections before promising an input-sized workspace.
 * Document parameters, init plans and foreign expanding producers remain outside the
 * accepted source forms. Selector keys are deliberately outside this provenance check. */
export function jsonInputsBounded(expr: ScalarExpr, input: Rel): boolean {
  const column = (rel: Rel, index: number): boolean => {
    switch (rel.op) {
      case "scan":
      case "keyLookup":
      case "indexRange":
        return true;
      case "values":
        return rel.rows.every((row) => row[index] !== undefined && origin(row[index], rel));
      case "project":
        return rel.exprs[index] !== undefined && origin(rel.exprs[index], rel.input);
      case "fetch":
        return index >= rel.input.output.length || column(rel.input, index);
      case "filter":
      case "limit":
      case "sort":
      case "distinct":
        return column(rel.input, index);
      case "window":
        return index < rel.input.output.length && column(rel.input, index);
      case "join":
        return index < rel.left.output.length
          ? column(rel.left, index)
          : index < rel.left.output.length + rel.right.output.length &&
              column(rel.right, index - rel.left.output.length);
      case "setOp":
        return rel.inputs.every((child) => column(child, index));
      case "aggregate":
        return rel.groupBy[index] !== undefined && column(rel.input, rel.groupBy[index]);
      default:
        return false;
    }
  };
  const origin = (source: ScalarExpr, rel: Rel): boolean => {
    if (source.op === "literal") return true;
    if (source.op === "column") return column(rel, source.index);
    if (source.op === "cast")
      return (
        // Only representation-preserving or shrinking casts stay inside the source envelope.
        (source.input.type.kind === "text" || source.input.type.kind === "json") &&
        (source.type.kind === "text" || source.type.kind === "json") &&
        origin(source.input, rel)
      );
    if (source.op === "case")
      return (
        source.branches.every((branch) => origin(branch.then, rel)) &&
        (source.otherwise === null || origin(source.otherwise, rel))
      );
    if (source.op === "call") {
      if (source.fn === "json_get" || source.fn === "json_get_text" || source.fn === "nullif")
        return source.args[0] !== undefined && origin(source.args[0], rel);
      if (source.fn === "coalesce") return source.args.every((arg) => origin(arg, rel));
    }
    return false;
  };
  if (expr.op === "call" && (expr.fn === "json_get" || expr.fn === "json_get_text"))
    return (
      expr.args[0] !== undefined &&
      origin(expr.args[0], input) &&
      expr.args.every((arg) => jsonInputsBounded(arg, input))
    );
  if (
    expr.op === "cast" &&
    expr.type.kind === "text" &&
    expr.input.type.kind === "json" &&
    expr.input.type.binary
  )
    return origin(expr.input, input) && jsonInputsBounded(expr.input, input);
  if (expr.op === "cast" && expr.type.kind === "json" && expr.input.type.kind === "text")
    return origin(expr.input, input) && jsonInputsBounded(expr.input, input);
  for (const value of Object.values(expr)) {
    if (value === null || typeof value !== "object") continue;
    for (const item of Array.isArray(value) ? value : [value]) {
      if (item === null || typeof item !== "object") continue;
      if ("op" in item && !jsonInputsBounded(item as ScalarExpr, input)) return false;
      if (
        "then" in item &&
        (!jsonInputsBounded((item as { then: ScalarExpr }).then, input) ||
          !jsonInputsBounded((item as { when: ScalarExpr }).when, input))
      )
        return false;
    }
  }
  return true;
}

/** Only retaining ancestors participate in a JSON cut. A live descendant can be
 * awaiting the cut's finish(), and must never be checkpointed from that same cut. */
export function jsonRetainingParents(root: Rel): {
  readonly supported: ReadonlySet<number>;
  readonly excepts: ReadonlyMap<number, ReadonlySet<number>>;
} {
  const supported = new Set<number>(),
    excepts = new Map<number, ReadonlySet<number>>();
  const visit = (rel: Rel, allowed: boolean, ancestors: readonly number[]): void => {
    if (allowed) {
      supported.add(rel.id);
      excepts.set(rel.id, new Set(ancestors));
    }
    const next = allowed && rel.op !== "join" && rel.op !== "window";
    if ("input" in rel) visit(rel.input, next, ancestors);
    if ("left" in rel && "right" in rel) {
      visit(rel.left, false, ancestors);
      visit(rel.right, false, ancestors);
    }
    if ("inputs" in rel)
      for (const input of rel.inputs)
        visit(input, next && rel.kind === "except", [...ancestors, rel.id]);
  };
  visit(root, true, []);
  return { supported, excepts };
}
