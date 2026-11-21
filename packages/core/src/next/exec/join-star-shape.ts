/** Exact-order eligibility for one resident star; estimates never enter this recipe. */
import type { CatalogView } from "../access/catalog.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
import type { StarDictionaryShape } from "./star-shape.js";
import type { StarBound } from "./star-bound.js";
import type { PartitionBounds } from "./agg/partition-probe.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { StarBuckets } from "./join-star-buckets.js";
type Scan = Extract<Rel, { op: "scan" }>;
type Join = Extract<Rel, { op: "join" }>;
interface Origin {
  scan: number;
  column: number;
}
export interface StarLookupDimension {
  readonly scan: Scan;
  readonly key: number;
  readonly group: number;
  readonly book: number;
}
export interface StarLookupShape {
  readonly fact: Scan;
  readonly a: StarLookupDimension;
  readonly b: StarLookupDimension;
  readonly c: StarLookupDimension;
  readonly factKeys: readonly [number, number, number];
  readonly factValue: number;
  /** Aggregate input positions, in A/B/C order. */
  readonly groups: readonly [number, number, number];
  readonly value: number;
  readonly input: Rel;
  readonly joins: readonly Join[];
}
export interface StarLookupBound {
  readonly facts: number;
  readonly a: number;
  readonly b: number;
  readonly domains: readonly [number, number, number];
  readonly bytes: number;
  readonly owner: Join;
  readonly minimum: number;
}
function strip(node: Rel): Rel {
  while (node.op === "project") node = node.input;
  return node;
}
function origin(node: Rel, index: number): Origin | null {
  if (node.op === "scan")
    return index < node.columns.length ? { scan: node.id, column: index } : null;
  if (node.op === "project") {
    const e = node.exprs[index];
    return e?.op === "column" ? origin(node.input, e.index) : null;
  }
  if (node.op === "join")
    return index < node.left.output.length
      ? origin(node.left, index)
      : origin(node.right, index - node.left.output.length);
  return null;
}
const keyOrigin = (node: Rel, expr: ScalarExpr | undefined) =>
  expr?.op === "column" ? origin(node, expr.index) : null;
export function starLookupShape(shape: StarDictionaryShape): StarLookupShape | null {
  const aggregate = shape.aggregate,
    last = strip(aggregate.input);
  if (aggregate.groupBy.length !== 3 || aggregate.aggregates.length !== 2 || last.op !== "join")
    return null;
  const middle = strip(last.right);
  if (middle.op !== "join") return null;
  const first = strip(middle.right);
  if (first.op !== "join") return null;
  // The lookup stores one key per dimension. The dictionary recipe also
  // supports composite joins, which must retain all their native key checks.
  if (
    [first, middle, last].some((join) => join.leftKeys.length !== 1 || join.rightKeys.length !== 1)
  )
    return null;
  const fs = strip(first.left),
    cs = strip(first.right),
    as = strip(middle.left),
    bs = strip(last.left);
  if (
    fs.op !== "scan" ||
    as.op !== "scan" ||
    bs.op !== "scan" ||
    cs.op !== "scan" ||
    new Set([fs.id, as.id, bs.id, cs.id]).size !== 4
  )
    return null;
  const scans = new Map(shape.scans.map((scan) => [scan.id, scan])),
    fact = scans.get(fs.id);
  if (!fact) return null;
  const dims = [as, bs, cs],
    joins = [middle, last, first],
    groups = aggregate.groupBy.map((index) => ({ index, origin: origin(aggregate.input, index) })),
    bindings: StarLookupDimension[] = [],
    factKeys: number[] = [],
    groupPositions: number[] = [];
  for (let i = 0; i < 3; i++) {
    const source = dims[i],
      join = joins[i];
    if (!source || !join) return null;
    const dim = scans.get(source.id),
      left = keyOrigin(join.left, join.leftKeys[0]),
      right = keyOrigin(join.right, join.rightKeys[0]),
      dimKey = i === 2 ? right : left,
      factKey = i === 2 ? left : right,
      group = groups.filter((g) => g.origin?.scan === source.id);
    if (
      !dim ||
      !dimKey ||
      !factKey ||
      dimKey.scan !== source.id ||
      factKey.scan !== fact.id ||
      group.length !== 1
    )
      return null;
    const g = group[0];
    if (
      !g?.origin ||
      dim.output[dimKey.column]?.type.kind !== "text" ||
      dim.output[g.origin.column]?.type.kind !== "text"
    )
      return null;
    const book = shape.columns.get(dim.id)?.[dimKey.column] ?? -1;
    if (book < 0 || shape.columns.get(fact.id)?.[factKey.column] !== book) return null;
    bindings.push({ scan: dim, key: dimKey.column, group: g.origin.column, book });
    factKeys.push(factKey.column);
    groupPositions.push(g.index);
  }
  const sum = aggregate.aggregates.find((a) => a.fn === "sum"),
    count = aggregate.aggregates.find((a) => a.fn === "count_star"),
    arg = sum?.args[0],
    source = arg?.op === "column" ? origin(aggregate.input, arg.index) : null;
  if (
    !sum ||
    count?.args.length !== 0 ||
    source?.scan !== fact.id ||
    sum.args.length !== 1 ||
    arg?.op !== "column" ||
    arg.type.kind !== "float64" ||
    fact.output[source.column]?.type.kind !== "float64"
  )
    return null;
  const [a, b, c] = bindings,
    [ka, kb, kc] = factKeys,
    [ga, gb, gc] = groupPositions;
  if (
    !a ||
    !b ||
    !c ||
    ka === undefined ||
    kb === undefined ||
    kc === undefined ||
    ga === undefined ||
    gb === undefined ||
    gc === undefined
  )
    return null;
  return {
    fact,
    a,
    b,
    c,
    factKeys: [ka, kb, kc],
    factValue: source.column,
    groups: [ga, gb, gc],
    value: arg.index,
    input: aggregate.input,
    joins: [first, middle, last],
  };
}
export function starLookupBound(
  shape: StarDictionaryShape,
  lookup: StarLookupShape,
  bound: StarBound,
  proofs: ReadonlyMap<number, PartitionBounds>,
  catalog: Pick<CatalogView, "table">,
): StarLookupBound | null {
  for (const dim of [lookup.a, lookup.b, lookup.c]) {
    const table = catalog.table(dim.scan.tableId),
      pk = table?.indexes.find((i) => i.id === table.primaryKey);
    if (!pk?.unique || pk.columnIds.length !== 1 || pk.columnIds[0] !== dim.scan.columns[dim.key])
      return null;
  }
  const domains = shape.books.map(() => 0);
  for (const scan of shape.scans) {
    const proof = proofs.get(scan.id);
    if (!proof) return null;
    let index = 0;
    for (const [column, book] of (shape.columns.get(scan.id) ?? []).entries()) {
      if (book < 0) continue;
      if (scan.output[column]?.type.kind !== "text") return null;
      const n = proof.domains[index++];
      if (n === undefined) return null;
      domains[book] = (domains[book] ?? 0) + n;
    }
  }
  const facts = proofs.get(lookup.fact.id)?.rows,
    da = domains[lookup.a.book],
    db = domains[lookup.b.book],
    dc = domains[lookup.c.book],
    ar = proofs.get(lookup.a.scan.id)?.rows,
    br = proofs.get(lookup.b.scan.id)?.rows;
  if (
    facts === undefined ||
    da === undefined ||
    db === undefined ||
    dc === undefined ||
    ar === undefined ||
    br === undefined ||
    ![facts, da, db, dc, ar, br].every((n) => Number.isSafeInteger(n) && n >= 0) ||
    Math.max(da, db, dc) > 8192
  )
    return null;
  // PK uniqueness and a complete exact domain bound (not NDV) cap dimension rows.
  const a = Math.min(ar, da),
    b = db,
    buckets = StarBuckets.bytes(facts, a, b);
  if (buckets === null) return null;
  const bytes =
    buckets +
    8 * facts + // charged A/B query-ID sidecars while native source barriers remain
    4 * (da + db + dc + a + b) +
    BATCH_ROWS * (3 * 4 + 8) +
    4 * (BATCH_ROWS / 32) * 4 +
    (192 << 10);
  let owner: Join | undefined,
    minimum = 0;
  for (const join of lookup.joins) {
    const n = bound.joins.get(join.id) ?? 0;
    if (n > minimum) {
      minimum = n;
      owner = join;
    }
  }
  // Reuse one complete, already-reserved native join allowance. No extra grant
  // waits, and no lower-cap claim: the original resident certificate still fits.
  return owner && bytes <= minimum
    ? { facts, a, b, domains: [da, db, dc], bytes, owner, minimum }
    : null;
}
