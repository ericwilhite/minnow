/** PostgreSQL LockClauseStrength, shared by parsing, catalog expansion and lowering. */
import {
  DUAL_TABLE,
  type CompiledQuery,
  type RowLocking,
  type TableSource,
} from "../plan/model.js";

const ORDER: readonly RowLocking[] = ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"];

export function strongestRowLock(locks: Iterable<RowLocking | undefined>): RowLocking | undefined {
  let strongest: RowLocking | undefined;
  for (const lock of locks)
    if (
      lock !== undefined &&
      (strongest === undefined || ORDER.indexOf(lock) > ORDER.indexOf(strongest))
    )
      strongest = lock;
  return strongest;
}

/** An outer clause reaches FROM subqueries, but cannot cross a CTE-reference boundary. */
export function sourceLocksRelation(
  source: TableSource,
  cteReferences: WeakSet<CompiledQuery>,
): boolean {
  const block = (query: CompiledQuery): boolean =>
    [query.base, ...query.joins].some((inner) => sourceLocksRelation(inner, cteReferences));
  if (source.recursive !== undefined) return false;
  if (source.derived !== undefined)
    return !cteReferences.has(source.derived) && block(source.derived);
  if (source.union !== undefined) return source.union.blocks.some(block);
  if (source.windowed !== undefined) return block(source.windowed.block);
  // A view is also a relation: PostgreSQL refuses its outer lock even for a constant view.
  return source.table !== DUAL_TABLE;
}

/** View expansion may add clauses absent from the caller's SQL, including through other views. */
export function expandedRowLock(
  query: CompiledQuery,
  bodies: Iterable<CompiledQuery>,
): RowLocking | undefined {
  return strongestRowLock([query.rowLocking, ...Array.from(bodies, (body) => body.rowLocking)]);
}

export function withRowLock<T extends CompiledQuery>(query: T, lock: RowLocking | undefined): T {
  if (lock !== undefined) query.rowLocking = strongestRowLock([query.rowLocking, lock]) ?? lock;
  return query;
}
