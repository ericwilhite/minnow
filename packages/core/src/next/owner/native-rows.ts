/** Common admitted primary continuation for root/body/RI writes. Discovery and rule
 * programs are immutable root preparation; no independent governor admission or end. */
import type { TableDef } from "../access/catalog.js";
import type {
  CommandToken,
  NativeRowHooks,
  ReferentialEntry,
  RowImageRef,
  WriteEvent,
} from "../access/statement-command.js";
import type { BoundTableRules } from "../exec/row-rules.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { Vector } from "../vector/vector.js";
import type { StatementImpl } from "../write/transaction.js";
import type { NativePrimaryAdapter } from "../write/primary-command.js";
import type { RegistryCommands, AcceptedCommandRow } from "./registry-commands.js";
import type { RegistryImages } from "./registry-images.js";
import type { NativeCommandChecks } from "./command-checks.js";
import type { RegistryTargets } from "./registry-targets.js";

interface TableBinding {
  readonly table: TableDef;
  readonly rules: BoundTableRules;
}
interface Slot {
  command: number;
  readonly widths: number[];
  readonly columns: Vector[];
  readonly entries: ReferentialEntry[];
  readonly changes: Array<{ columnId: number; value: Vector | null }>;
}
export interface NativeRowScope {
  readonly statement: StatementImpl;
  readonly primary: NativePrimaryAdapter;
  readonly commands: RegistryCommands;
  readonly images: RegistryImages;
  readonly targets: RegistryTargets;
  readonly tables: readonly TableBinding[];
  readonly frames: number;
  readonly rowEvents: number;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  /** Root computes rule workspace from immutable image widths/programs. No image/source
   * borrow exists here. Validation runs after writer credit is protected: its room belongs
   * to the same prepaid complete root step, never an independent governor admission. */
  prepareRules(
    table: TableDef,
    images: RowImageRef,
    phase: "generate" | "validate",
    signal: AbortSignal,
  ): Promise<void>;
  readonly checks: NativeCommandChecks;
  /** Return unused root source/image work credit before entering callbacks or waiting
   * for writer growth. Never returns metadata, live payloads, or protected writer credit. */
  releaseIdleStep(): void;
}
export function nativeRowMinimumBytes(columns: number, frames: number, rowEvents: number): number {
  if (
    !Number.isInteger(columns) ||
    columns < 0 ||
    !Number.isInteger(frames) ||
    frames < 1 ||
    frames > 3 ||
    !Number.isSafeInteger(rowEvents) ||
    rowEvents < 0
  )
    throw new TypeError("Invalid native row sizing");
  const bytes = 4096 + frames * (2048 + 512 * columns + 512 * rowEvents);
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native row metadata too large");
  return bytes;
}
export class NativeRows {
  #scope: NativeRowScope | null;
  #slots: Slot[] = [];
  #active = 0;
  readonly #bytes: number;
  constructor(scope: NativeRowScope) {
    scope.signal.throwIfAborted();
    let columns = 0;
    for (const binding of scope.tables) {
      if (scope.statement.catalog.table(binding.table.id) !== binding.table)
        throw new TypeError("Foreign native row program");
      columns = Math.max(columns, binding.table.columns.length);
    }
    this.#bytes = nativeRowMinimumBytes(columns, scope.frames, scope.rowEvents);
    if (!scope.charge.allocate(this.#bytes))
      throw new RangeError("Native row metadata must be admitted");
    this.#scope = scope;
    try {
      for (let i = 0; i < scope.frames; i++)
        this.#slots.push({
          command: 0,
          widths: Array.from({ length: columns }, () => 0),
          columns: [],
          changes: [],
          entries: [],
        });
    } catch (error) {
      this.#slots = [];
      this.#scope = null;
      scope.charge.free(this.#bytes);
      throw error;
    }
  }
  private get scope(): NativeRowScope {
    if (!this.#scope) throw new TypeError("Native rows closed");
    return this.#scope;
  }
  /** Caller has sealed the complete read input. RI captured identities are unique;
   * joined/body/upsert inputs must pass their charged command-local repeat check first. */
  async write(
    command: CommandToken,
    tableId: number,
    event: WriteEvent,
    images: RowImageRef,
    oldRowId: number | null,
    hooks: NativeRowHooks,
    signal: AbortSignal,
  ): Promise<AcceptedCommandRow> {
    const scope = this.scope,
      binding = scope.tables.find((binding) => binding.table.id === tableId),
      slot = this.#slots.find((slot) => slot.command === 0),
      budget = scope.commands.budget(command);
    if (
      !binding ||
      !slot ||
      this.#slots.some((slot) => slot.command === command.id) ||
      (event === "insert") !== (oldRowId === null) ||
      (oldRowId !== null && (!Number.isSafeInteger(oldRowId) || oldRowId < 0))
    )
      throw new TypeError("Invalid native row continuation");
    slot.command = command.id;
    this.#active++;
    const check = () => {
      scope.signal.throwIfAborted();
      signal.throwIfAborted();
      if (scope.commands.budget(command) !== budget)
        throw new TypeError("Retired native row command");
    };
    try {
      return await scope.statement.guard(async () => {
        check();
        slot.widths.length = binding.table.columns.length;
        const original = await scope.images.describe(images, slot.widths, signal);
        check();
        if (
          original.tableId !== tableId ||
          original.hasOld !== (event !== "insert") ||
          original.hasNew !== (event !== "delete")
        )
          throw new TypeError("Native row image sides differ from event");
        if (event !== "delete") {
          for (const column of binding.table.columns)
            if (column.generatedSql !== null)
              slot.changes.push({ columnId: column.id, value: null });
          if (slot.changes.length) {
            try {
              images = await scope.images.derive(images, "new", slot.changes, signal);
              check();
            } finally {
              slot.changes.length = 0;
            }
          }
        }
        scope.releaseIdleStep();
        await hooks.beforeRow(command, tableId, event, images, budget);
        check();
        if (
          event !== "delete" &&
          binding.table.columns.some((column) => column.generatedSql !== null)
        ) {
          binding.rules.retireGenerated();
          await scope.prepareRules(binding.table, images, "generate", signal);
          check();
          const lease = await scope.images.readRowImages(images, signal);
          try {
            check();
            const next = lease.new;
            if (lease.tableId !== tableId || !next)
              throw new TypeError("Missing proposed native NEW");
            for (const [i, column] of binding.table.columns.entries()) {
              const value = next.columns[i];
              if (!value) throw new TypeError("Missing native generated input");
              // The rule program replaces generated wrappers. Registry leases remain
              // immutable, including when a generator fails during precise replay.
              slot.columns.push(column.generatedSql === null ? value : { ...value });
            }
            binding.rules.generate(
              { ...next, columns: slot.columns },
              binding.table.columns.map((column) => column.id),
            );
            for (const [i, column] of binding.table.columns.entries())
              if (column.generatedSql !== null) {
                const vector = slot.columns[i];
                if (vector?.form !== "flat") throw new TypeError("Missing native generated output");
                slot.changes.push({ columnId: column.id, value: { ...vector } });
              }
          } finally {
            slot.columns.length = 0;
            lease.release();
          }
          try {
            images = await scope.images.derive(images, "new", slot.changes, signal);
            check();
          } finally {
            slot.changes.length = 0;
            binding.rules.retireGenerated();
          }
        }
        slot.widths.length = binding.table.columns.length;
        const shape = await scope.images.describe(images, slot.widths, signal);
        check();
        if (
          shape.tableId !== tableId ||
          shape.hasOld !== (event !== "insert") ||
          shape.hasNew !== (event !== "delete")
        )
          throw new TypeError("Native row image sides differ from event");
        scope.releaseIdleStep();
        const prepared = await scope.primary.prepare(
          command,
          tableId,
          event,
          oldRowId,
          slot.widths,
        );
        check();
        if (event !== "delete") {
          await scope.prepareRules(binding.table, images, "validate", signal);
          check();
        }
        const lease = await scope.images.readRowImages(images, signal);
        let newRowId: number | null;
        try {
          check();
          newRowId = await scope.primary.apply(prepared, lease.old, lease.new);
          check();
        } finally {
          lease.release();
          binding.rules.retireGenerated();
        }
        if (oldRowId !== null) {
          await scope.targets.note(tableId, oldRowId, newRowId);
          check();
        }
        const row: AcceptedCommandRow = { command, tableId, event, images, oldRowId, newRowId };
        await scope.commands.accept(row);
        check();
        const entries = slot.entries;
        await this.events(row, entries, signal);
        check();
        if (entries.length > scope.rowEvents)
          throw new TypeError("Native event workspace exceeded");
        await hooks.afterRow(command, tableId, event, images, entries, budget);
        check();
        return row;
      });
    } finally {
      binding.rules.retireGenerated();
      slot.columns.length = 0;
      slot.changes.length = 0;
      slot.entries.length = 0;
      slot.command = 0;
      this.#active--;
    }
  }
  /** Materialize only one row's bounded immutable event descriptors. All accepted
   * rows and subsequent queued events live in the paged root registry, not this list. */
  private async events(
    row: AcceptedCommandRow,
    entries: ReferentialEntry[],
    signal: AbortSignal,
  ): Promise<void> {
    const scope = this.scope,
      table = scope.statement.catalog.table(row.tableId);
    if (!table) throw new TypeError("Unknown native event table");
    const add = async (
      fkId: number,
      event: ReferentialEntry["event"],
      kind: ReferentialEntry["kind"],
    ) => {
      if (entries.length >= scope.rowEvents) throw new TypeError("Native event workspace exceeded");
      const id = await scope.commands.addReferential(row.command, row.images, {
        fkId,
        event,
        kind,
        targets: null,
      });
      signal.throwIfAborted();
      scope.signal.throwIfAborted();
      entries.push(await scope.commands.resolveReferential(id));
    };
    if (row.event !== "delete")
      for (const fk of table.foreignKeys) {
        if (fk.enforced && (await scope.checks.referenceNeedsCheck(row, fk.id, signal)))
          await add(
            fk.id,
            row.event === "insert" ? "childInsert" : "childUpdate",
            "checkReference",
          );
      }
    if (row.event !== "insert")
      for (const child of scope.statement.catalog.tables)
        for (const fk of child.foreignKeys) {
          if (
            fk.parentTableId !== table.id ||
            (!fk.enforced && !(row.event === "update" && fk.onUpdate === "follow"))
          )
            continue;
          if (
            row.event === "update" &&
            !(await scope.checks.parentKeyChanged(row, fk.parentIndexId, signal))
          )
            continue;
          await add(
            fk.id,
            row.event === "delete" ? "parentDelete" : "parentUpdate",
            row.event === "delete" ? fk.onDelete : fk.onUpdate,
          );
        }
  }
  /** Owning boundary cancels/fences all row/hook jobs before this non-owning binding close. */
  close(): void {
    if (!this.#scope) return;
    if (this.#active) throw new TypeError("Native row close requires command fence");
    this.#slots = [];
    this.#scope.charge.free(this.#bytes);
    this.#scope = null;
  }
}
