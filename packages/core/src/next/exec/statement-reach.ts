/** Immutable catalog reach for one admitted command. No stored SQL is parsed here. */
import type { CatalogView } from "../access/catalog.js";
import type { TriggerEvent } from "../access/trigger-def.js";
import type { Rel } from "../ir/ir.js";
export interface RuleReach {
  readonly tables: readonly number[];
  readonly triggers: boolean;
  /** New action descriptors must not fall through to legacy follow/delete behavior. */
  readonly unsupportedReferentialActions: boolean;
}
interface Work {
  tableId: number;
  event: TriggerEvent;
  budget: number;
  assigned: readonly number[] | null;
}
export function statementRuleReach(
  catalog: CatalogView,
  tableId: number,
  modify?: Extract<Rel, { op: "modify" }>,
  budget = 1,
): RuleReach {
  if (!Number.isInteger(budget) || budget < -1)
    throw new RangeError("Trigger budget must be an integer at least -1");
  const pending: Work[] = [],
    tables = new Set<number>();
  const seen = new Map<string, Set<number> | null>();
  let triggers = false;
  let unsupportedReferentialActions = false;
  const enqueue = (
    id: number,
    event: TriggerEvent,
    left: number,
    assigned: readonly number[] | null,
  ) => {
    pending.push({ tableId: id, event, budget: left, assigned });
  };
  const kind = modify?.kind ?? "update";
  if (kind === "insert" || kind === "upsert") enqueue(tableId, "insert", budget, null);
  if (kind === "update" || (kind === "upsert" && modify?.onConflict?.action === "update"))
    enqueue(
      tableId,
      "update",
      budget,
      modify?.kind === "upsert"
        ? (modify.onConflict?.assignments.map((a) => a.column) ?? [])
        : (modify?.assigned ?? null),
    );
  if (kind === "delete") enqueue(tableId, "delete", budget, null);
  while (pending.length > 0) {
    const state = pending.pop();
    if (!state) break;
    const key = `${String(state.tableId)}:${state.event}:${String(state.budget)}`;
    const prior = seen.get(key);
    if (prior === null || (prior !== undefined && state.assigned?.every((id) => prior.has(id))))
      continue;
    const own = catalog.table(state.tableId);
    if (!own) throw new TypeError("Missing reachable table");
    const assigned =
      state.assigned === null
        ? null
        : new Set([
            ...(prior ?? []),
            ...state.assigned,
            // Recomputed generated keys can move even when the SET list omits them.
            ...own.columns.filter((c) => c.generatedSql !== null).map((c) => c.id),
          ]);
    seen.set(key, assigned);
    if (state.event !== "delete") tables.add(own.id);
    for (const trigger of catalog.triggers) {
      if (trigger.tableId !== own.id || trigger.event !== state.event) continue;
      triggers = true;
      // Shipped recursion fires at 1 and 0; a body may write at -1, but cannot fire again.
      if (state.budget < 0) continue;
      for (const body of trigger.statements)
        enqueue(
          body.targetTableId,
          body.kind,
          state.budget - 1,
          body.kind === "update" ? body.assignedColumnIds : null,
        );
    }
    if (state.event === "insert") continue;
    for (const child of catalog.tables)
      for (const fk of child.foreignKeys) {
        if (fk.parentTableId !== own.id) continue;
        if (state.event === "delete") {
          if (!fk.enforced) continue;
          if (fk.onDelete === "setDefault") unsupportedReferentialActions = true;
          if (fk.onDelete === "cascade") enqueue(child.id, "delete", state.budget, null);
          else if (fk.onDelete === "setNull")
            enqueue(child.id, "update", state.budget, fk.columnIds);
        } else {
          const index = own.indexes.find((i) => i.id === fk.parentIndexId);
          if (assigned !== null && !index?.columnIds.some((id) => assigned.has(id))) continue;
          // The existing executor implements enforced, omitted-update follow only.
          // Explicit modes and informational follow need the bounded RI driver.
          if (fk.onUpdate !== "follow" || !fk.enforced) unsupportedReferentialActions = true;
          // Conservative reach until the RI driver consumes exact action descriptors.
          // Informational follow references also need their child's row rules.
          const columns = [
            ...fk.columnIds,
            ...child.columns.filter((c) => c.generatedSql !== null).map((c) => c.id),
          ];
          enqueue(child.id, "update", state.budget, columns);
        }
      }
  }
  return { tables: [...tables], triggers, unsupportedReferentialActions };
}
