/** Optional complete resident certificate, never a cardinality estimate.
 * Sources are exact domains from the execution's pinned snapshot. PK uniqueness is a
 * catalog law enforced by the write path; filters and direct projections preserve it.
 * Keeping every native join resident preserves its probe/duplicate-chain order when
 * TEXT equality classes become integers. No spill partition may reorder float inputs.
 */
import type { CatalogView } from "../access/catalog.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import type { PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import { layoutOf } from "../types.js";
import { BATCH_ROWS } from "../vector/vector.js";
import type { PartitionBounds } from "./agg/partition-probe.js";
import { ColumnStore } from "./column-store.js";
import { KeyLayout } from "./join-keys.js";
import { JoinTable } from "./join-table.js";
import { hashJoinMinimumBytes, type HashJoinSpec } from "./join.js";
import type { StarDictionaryShape } from "./star-shape.js";

export interface StarBound {
  readonly books: number;
  readonly aggregate: number;
  readonly joins: ReadonlyMap<number, number>;
}

/** Cheap catalog-only eligibility before opening any domain proof reader. */
export function starPrimaryKeys(
  shape: StarDictionaryShape,
  catalog: Pick<CatalogView, "table">,
): boolean {
  const leaf = (input: Rel, key: ScalarExpr | undefined): boolean => {
    if (key?.op !== "column") return false;
    let index = key.index,
      node = input;
    while (node.op === "project") {
      const expr = node.exprs[index];
      if (expr?.op !== "column") return false;
      index = expr.index;
      node = node.input;
    }
    if (node.op !== "scan") return false;
    const table = catalog.table(node.tableId),
      primary = table?.indexes.find((i) => i.id === table.primaryKey);
    return (
      primary?.unique === true &&
      primary.columnIds.length === 1 &&
      primary.columnIds[0] === node.columns[index]
    );
  };
  return [...shape.typed.values()].every(
    (node) =>
      node.op !== "join" ||
      (node.leftKeys.length === 1 &&
        (leaf(node.left, node.leftKeys[0]) || leaf(node.right, node.rightKeys[0]))),
  );
}

/** Only a terminal grouped star, with non-expanding, non-aliasing ancestors. */
export function terminalStar(plan: PhysicalPlan, aggregate: number): boolean {
  if (plan.volatile || plan.parameters.length !== 0) return false;
  let node = plan.root;
  while (node.id !== aggregate) {
    if (node.op === "sort" || node.op === "limit") node = node.input;
    else if (node.op === "project") {
      const seen = new Set<number>();
      for (const expr of node.exprs) {
        if (expr.op !== "column" || seen.has(expr.index)) return false;
        seen.add(expr.index);
      }
      node = node.input;
    } else return false;
  }
  return true;
}

/** Error-free scan predicates only: proof reads do not evaluate or reorder SQL errors. */
export function starPredicate(expr: ScalarExpr | null): boolean {
  if (expr === null) return true;
  if (expr.op === "column" || expr.op === "literal") return true;
  if (expr.op === "compare")
    return expr.collation === null && starPredicate(expr.left) && starPredicate(expr.right);
  if (expr.op === "distinctFrom") return starPredicate(expr.left) && starPredicate(expr.right);
  return false;
}

export function starBound(
  shape: StarDictionaryShape,
  catalog: Pick<CatalogView, "table">,
  proofs: ReadonlyMap<number, PartitionBounds>,
  compiler: ReadonlyMap<number, number>,
): StarBound | null {
  if (shape.aggregate.aggregates.length > 8) return null;
  const domains = shape.books.map(() => 0),
    keyBytes = shape.books.map(() => 0),
    rows = new Map<number, number>(),
    unique = new Map<number, ReadonlySet<number>>();
  let fields = 0;
  for (const scan of shape.scans) {
    if (!starPredicate(scan.filter)) return null;
    const proof = proofs.get(scan.id),
      ids = shape.columns.get(scan.id),
      table = catalog.table(scan.tableId);
    if (!proof || !ids || !table || proof.rows > 2 ** 21) return null;
    const primary = table.indexes.find((index) => index.id === table.primaryKey);
    const pk = primary?.unique && primary.columnIds.length === 1 ? primary.columnIds[0] : -1;
    const keys = new Set<number>();
    let column = 0;
    for (const [index, type] of scan.output.entries()) {
      const book = ids[index] ?? -1;
      if (book < 0) {
        if (!["bool", "i32", "f32", "f64"].includes(layoutOf(type.type))) return null;
        continue;
      }
      const count = proof.domains[column],
        bytes = proof.keyBytes[column],
        dictionary = proof.dictionaryEntries[column];
      if (
        count === undefined ||
        bytes === undefined ||
        dictionary === undefined ||
        dictionary > BATCH_ROWS
      )
        return null;
      domains[book] = (domains[book] ?? 0) + count;
      keyBytes[book] = (keyBytes[book] ?? 0) + bytes;
      if (scan.columns[index] === pk) keys.add(book);
      fields++;
      column++;
    }
    if (column !== proof.domains.length) return null;
    rows.set(scan.id, proof.rows);
    unique.set(scan.id, keys);
  }
  if (domains.some((n) => !Number.isSafeInteger(n) || n > 8192)) return null;
  const joins = new Map<number, number>();
  const visit = (node: Rel): boolean => {
    if (node.op === "scan") return rows.has(node.id);
    if (node.op === "project") {
      if (!visit(node.input)) return false;
      rows.set(node.id, rows.get(node.input.id) ?? 0);
      const present = new Set(shape.columns.get(node.id));
      unique.set(
        node.id,
        new Set([...(unique.get(node.input.id) ?? [])].filter((id) => present.has(id))),
      );
      return true;
    }
    if (
      node.op !== "join" ||
      node.leftKeys.length !== 1 ||
      node.rightKeys.length !== 1 ||
      !visit(node.left) ||
      !visit(node.right)
    )
      return false;
    const left = node.leftKeys[0],
      right = node.rightKeys[0];
    if (left?.op !== "column" || right?.op !== "column") return false;
    const book = shape.columns.get(node.left.id)?.[left.index] ?? -1;
    if (book < 0 || book !== shape.columns.get(node.right.id)?.[right.index]) return false;
    const lu = unique.get(node.left.id)?.has(book) === true,
      ru = unique.get(node.right.id)?.has(book) === true;
    if (!lu && !ru) return false;
    const l = rows.get(node.left.id) ?? 0,
      r = rows.get(node.right.id) ?? 0;
    rows.set(node.id, lu && ru ? Math.min(l, r) : lu ? r : l);
    unique.set(
      node.id,
      new Set([
        ...(ru ? (unique.get(node.left.id) ?? []) : []),
        ...(lu ? (unique.get(node.right.id) ?? []) : []),
      ]),
    );
    const spec: HashJoinSpec = {
      kind: "inner",
      probeTypes: node.left.output.map((c) => c.type),
      buildTypes: node.right.output.map((c) => c.type),
      probeKeys: [left.index],
      buildKeys: [right.index],
      nullSafe: [false],
      residual: null,
      publishes: [],
    };
    const store = new ColumnStore(spec.buildTypes, {
      allocate: () => false,
      free: () => undefined,
    });
    const layout = new KeyLayout([{ kind: "int32", bits: 32 }], [false]);
    // The helper covers native scratch/output/I/O. Add the full retained fixed-width store
    // and the larger direct/word table (including next chains), before opening any source.
    const state =
      store.fixedBytes(r) +
      Math.max(JoinTable.estimateBytes(layout, r), 4 * (domains[book] ?? 0) + 4 * r);
    store.close();
    joins.set(node.id, (compiler.get(node.id) ?? 0) + hashJoinMinimumBytes(spec) + state);
    return true;
  };
  if (!visit(shape.aggregate.input)) return null;
  const groups = shape.groupBooks.reduce((n, book) => n * (domains[book] ?? 0), 1);
  if (!Number.isSafeInteger(groups) || groups > 32768) return null;
  // Include each exposed dictionary position separately: downstream can copy it separately.
  // Keep dictionaries <=1/8 of the existing result progress floor (MAX_BLOB_BYTES).
  // <=8 keys and <=8 COUNT/SUM outputs need <200KiB per fixed-code output batch;
  // dictionary staging and two such batches fit the floor, including frame metadata.
  if (shape.groupBooks.reduce((n, book) => n + (keyBytes[book] ?? 0), 0) > MAX_BLOB_BYTES / 8)
    return null;
  const capacity = Math.max(1024, 2 * groups);
  // Group tables: direct-table old/new <=512KiB; per-key integer hash/value/tuple arrays,
  // resize overlap and batch IDs <128C. Per float call, AVG is the largest state:
  // three f64 fields (24C), old/new growth <=72C, plus validity/output <=9C.
  // SUM and fixed-width MIN/MAX are smaller. <=8 calls cannot select the 21-call bank.
  // AVG keeps every Youngs-Cramer Sxx update and its PostgreSQL overflow check.
  const aggregate =
    (compiler.get(shape.aggregate.id) ?? 0) +
    (512 << 10) +
    shape.groupBooks.length * (128 * capacity + 8 * BATCH_ROWS) +
    shape.aggregate.aggregates.length * 128 * capacity;
  // One TEXT book: initial/hash/id/offset/translation arrays and geometric resize <128C;
  // byte arena old+new+final output <=5 times its complete byte bound, plus initial4KiB.
  const books =
    domains.reduce(
      (n, count, book) => n + 128 * Math.max(1024, 2 * count) + 5 * (keyBytes[book] ?? 0) + 4096,
      0,
    ) +
    fields * (4 * BATCH_ROWS + BATCH_ROWS / 8);
  return [books, aggregate, ...joins.values()].every(Number.isSafeInteger)
    ? { books, aggregate, joins }
    : null;
}
