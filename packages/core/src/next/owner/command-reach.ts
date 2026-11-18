/** Charged static preparation reach. It contains catalog identities, never affected rows.
 * Root/body budgets are 1/0/-1; RI keeps its budget. Terminal matching definitions are
 * discovered by the controller at runtime without preparing their unreachable bodies. */
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { TriggerDef } from "../access/trigger-def.js";
import type { WriteEvent } from "../access/statement-command.js";
import type { Rel } from "../ir/ir.js";
import type { ByteCharge } from "../vector/varlen.js";
import { pauseIfDue, sortInSlices, type TaskWork } from "../task.js";
type Modify = Extract<Rel, { op: "modify" }>;
const EVENTS: readonly WriteEvent[] = ["insert", "update", "delete"];
export function nativeCommandReachBytes(catalog: CatalogView): number {
  let columns = 0;
  for (const table of catalog.tables) columns += table.columns.length;
  const bytes = 4096 + 64 * catalog.tables.length + 32 * columns + 32 * catalog.triggers.length;
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native catalog reach is too large");
  return bytes;
}
function invalid(): never {
  throw new TypeError("Invalid native command reach");
}
function compareNames(a: string, b: string): number {
  let i = 0,
    j = 0;
  while (i < a.length && j < b.length) {
    const x = a.codePointAt(i) ?? 0,
      y = b.codePointAt(j) ?? 0;
    if (x !== y) return x - y;
    i += x > 65535 ? 2 : 1;
    j += y > 65535 ? 2 : 1;
  }
  return a.length - i - (b.length - j);
}
export class NativeCommandReach {
  #catalog: CatalogView | null;
  #charge: ByteCharge | null;
  readonly #bytes: number;
  #states: Uint8Array | null;
  #columns: Uint8Array | null;
  #offsets: Float64Array | null;
  #prepared: Uint8Array | null;
  #tables: number[] = [];
  #triggers: TriggerDef[] = [];
  #frames = 1;
  #rowEvents = 0;
  private constructor(catalog: CatalogView, charge: ByteCharge, bytes: number) {
    this.#catalog = catalog;
    this.#charge = charge;
    this.#bytes = bytes;
    this.#states = new Uint8Array(9 * catalog.tables.length);
    this.#offsets = new Float64Array(catalog.tables.length + 1);
    let columns = 0;
    for (const [i, table] of catalog.tables.entries()) {
      this.#offsets[i] = columns;
      columns += table.columns.length;
    }
    this.#offsets[catalog.tables.length] = columns;
    this.#columns = new Uint8Array(9 * columns);
    this.#prepared = new Uint8Array(catalog.triggers.length);
  }
  static async prepare(
    catalog: CatalogView,
    root: Modify,
    charge: ByteCharge,
    work: TaskWork & { readonly signal: AbortSignal },
  ): Promise<NativeCommandReach> {
    work.signal.throwIfAborted();
    const bytes = nativeCommandReachBytes(catalog);
    if (!charge.allocate(bytes)) throw new RangeError("Native reach metadata must be admitted");
    let reach: NativeCommandReach | null = null;
    try {
      reach = new NativeCommandReach(catalog, charge, bytes);
      await reach.#run(root, work);
      return reach;
    } catch (error) {
      if (reach) reach.close();
      else charge.free(bytes);
      throw error;
    }
  }
  private get catalog() {
    return this.#catalog ?? invalid();
  }
  get tables(): readonly number[] {
    if (!this.#charge) invalid();
    return this.#tables;
  }
  get triggers(): readonly TriggerDef[] {
    if (!this.#charge) invalid();
    return this.#triggers;
  }
  get frames(): number {
    if (!this.#charge) invalid();
    return this.#frames;
  }
  get rowEvents(): number {
    if (!this.#charge) invalid();
    return this.#rowEvents;
  }
  #push(id: number, event: WriteEvent, budget: number, assigned: readonly number[] | null) {
    if (!Number.isInteger(budget) || budget < -1 || budget > 1) invalid();
    const index = this.catalog.tables.findIndex((table) => table.id === id),
      table = this.catalog.tables[index];
    if (!table) invalid();
    const route = (budget + 1) * 3 + EVENTS.indexOf(event),
      at = index * 9 + route,
      states = this.#states ?? invalid(),
      flags = this.#columns ?? invalid(),
      offsets = this.#offsets ?? invalid(),
      start = 9 * (offsets[index] ?? invalid()) + route * table.columns.length;
    let changed = states[at] === 0;
    if (event === "update") {
      for (const [i, column] of table.columns.entries())
        if (assigned === null || assigned.includes(column.id) || column.generatedSql !== null) {
          if (flags[start + i] !== 1) changed = true;
          flags[start + i] = 1;
        }
      if (assigned?.some((id) => !table.columns.some((column) => column.id === id))) invalid();
    }
    if (changed) states[at] = 1;
  }
  #moves(table: TableDef, indexId: number, at: number): boolean {
    const index = table.indexes.find((index) => index.id === indexId);
    if (!index) invalid();
    const flags = this.#columns ?? invalid();
    return index.columnIds.some(
      (id) => flags[at + table.columns.findIndex((column) => column.id === id)] === 1,
    );
  }
  async #run(root: Modify, work: TaskWork & { readonly signal: AbortSignal }) {
    if (root.kind === "insert" || root.kind === "upsert")
      this.#push(root.tableId, "insert", 1, null);
    if (root.kind === "update") this.#push(root.tableId, "update", 1, root.assigned);
    if (root.kind === "delete") this.#push(root.tableId, "delete", 1, null);
    if (root.kind === "upsert" && root.onConflict?.action === "update")
      this.#push(
        root.tableId,
        "update",
        1,
        root.onConflict.assignments.map((a) => a.column),
      );
    const states = this.#states ?? invalid(),
      prepared = this.#prepared ?? invalid(),
      offsets = this.#offsets ?? invalid();
    let dirty = true;
    while (dirty) {
      dirty = false;
      for (const [i, table] of this.catalog.tables.entries())
        for (let route = 0; route < 9; route++) {
          work.signal.throwIfAborted();
          const pause = pauseIfDue(work);
          if (pause) await pause;
          const state = i * 9 + route;
          if (states[state] !== 1) continue;
          states[state] = 2;
          dirty = true;
          const budget = Math.floor(route / 3) - 1,
            event = EVENTS[route % 3] ?? invalid(),
            columnStart = 9 * (offsets[i] ?? invalid()) + route * table.columns.length;
          this.#frames = Math.max(this.#frames, 2 - budget);
          let events = 0;
          if (event !== "delete") for (const fk of table.foreignKeys) if (fk.enforced) events++;
          for (const [t, trigger] of this.catalog.triggers.entries()) {
            if (trigger.tableId !== table.id || trigger.event !== event) continue;
            if (trigger.timing === "after") events++;
            if (budget < 0) continue;
            prepared[t] = 1;
            for (const body of trigger.statements)
              this.#push(
                body.targetTableId,
                body.kind,
                budget - 1,
                body.kind === "update" ? body.assignedColumnIds : null,
              );
          }
          if (event !== "insert")
            for (const child of this.catalog.tables)
              for (const fk of child.foreignKeys) {
                if (
                  fk.parentTableId !== table.id ||
                  (event === "delete"
                    ? !fk.enforced
                    : !this.#moves(table, fk.parentIndexId, columnStart))
                )
                  continue;
                const mode = event === "delete" ? fk.onDelete : fk.onUpdate;
                if (!fk.enforced && mode !== "follow") continue;
                events++;
                if (mode === "cascade")
                  this.#push(child.id, event, budget, event === "update" ? fk.columnIds : null);
                else if (mode === "follow" || mode === "setNull" || mode === "setDefault")
                  this.#push(child.id, "update", budget, fk.columnIds);
              }
          this.#rowEvents = Math.max(this.#rowEvents, events);
        }
    }
    for (const [i, table] of this.catalog.tables.entries())
      if (states.subarray(i * 9, (i + 1) * 9).some((value) => value !== 0))
        this.#tables.push(table.id);
    for (const [i, trigger] of this.catalog.triggers.entries())
      if (prepared[i]) this.#triggers.push(trigger);
    await sortInSlices(this.#tables, (a, b) => a - b, work);
    await sortInSlices(this.#triggers, (a, b) => compareNames(a.name, b.name) || a.id - b.id, work);
    work.signal.throwIfAborted();
  }
  close(): void {
    if (!this.#charge) return;
    const charge = this.#charge;
    this.#charge = this.#catalog = null;
    this.#states = this.#columns = this.#offsets = this.#prepared = null;
    this.#tables.length = this.#triggers.length = 0;
    charge.free(this.#bytes);
  }
}
