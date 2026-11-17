/** One concrete named RI continuation (a complete original-target group for follow).
 * Full OLD inputs are sealed before any mutation;
 * child BEFORE runs inline, while child AFTER appends through the caller's hooks. */
import type { StatementImpl } from "../write/transaction.js";
import type { CatalogView } from "../access/catalog.js";
import type {
  CommandToken,
  NativeRowHooks,
  ReferentialEntry,
} from "../access/statement-command.js";
import { UnsupportedPlanError } from "../exec/errors.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { NativeChildInputs } from "./child-inputs.js";
import type { NativeCommandChecks } from "./command-checks.js";
import type { NativeDefaults } from "./native-defaults.js";
import type { NativeRows } from "./native-rows.js";
import type { RegistryCommands } from "./registry-commands.js";
import type { RegistryFollowing } from "./registry-following.js";
import type { RegistryImages, RegistryImagePatch } from "./registry-images.js";
import type { RegistryTargets } from "./registry-targets.js";
import type { NativeReferentialChecks } from "./referential-checks.js";

interface MutationSlot {
  command: number;
  readonly changes: RegistryImagePatch[];
}
export interface NativeReferentialScope {
  readonly statement: StatementImpl;
  readonly catalog: CatalogView;
  readonly commands: RegistryCommands;
  readonly checks: NativeCommandChecks;
  readonly restrictions: NativeReferentialChecks;
  readonly images: RegistryImages;
  readonly targets: RegistryTargets;
  readonly following: RegistryFollowing;
  readonly inputs: NativeChildInputs;
  readonly rows: NativeRows;
  readonly defaults: NativeDefaults;
  readonly frames: number;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  checkpoint(): Promise<void>;
  releaseIdleStep(): void;
}
export function nativeReferentialMinimumBytes(catalog: CatalogView, frames: number): number {
  if (!Number.isInteger(frames) || frames < 1 || frames > 3)
    throw new TypeError("Invalid RI frames");
  let columns = 0;
  for (const table of catalog.tables) columns = Math.max(columns, table.columns.length);
  return 2048 + frames * (1024 + columns * 256);
}
export class NativeReferentialMutations {
  #scope: NativeReferentialScope | null;
  #slots: MutationSlot[] = [];
  readonly #bytes: number;
  constructor(scope: NativeReferentialScope) {
    scope.signal.throwIfAborted();
    this.#bytes = nativeReferentialMinimumBytes(scope.catalog, scope.frames);
    if (!scope.charge.allocate(this.#bytes)) throw new RangeError("RI work must be admitted");
    this.#scope = scope;
    try {
      for (let i = 0; i < scope.frames; i++) this.#slots.push({ command: 0, changes: [] });
    } catch (error) {
      this.#scope = null;
      scope.charge.free(this.#bytes);
      throw error;
    }
  }
  private get scope(): NativeReferentialScope {
    if (!this.#scope) throw new TypeError("RI executor closed");
    return this.#scope;
  }
  async run(entry: ReferentialEntry, hooks: NativeRowHooks, signal: AbortSignal): Promise<void> {
    await this.scope.statement.guard(async () => {
      if (entry.kind !== "follow") {
        await this.runAction(entry, hooks, signal);
        return;
      }
      for await (const member of this.scope.following.group(entry.id, signal)) {
        const child = this.scope.catalog.tables.find((table) =>
          table.foreignKeys.some((fk) => fk.id === member.fkId),
        );
        if (!child) throw new TypeError("Missing follow child");
        const targets = await this.scope.following.require(member.id, signal),
          input = await this.scope.inputs.seal(child.id, targets, signal);
        await this.scope.following.bindChildInput(member.id, input, signal);
        this.scope.releaseIdleStep();
      }
      for await (const member of this.scope.following.group(entry.id, signal))
        await this.runAction(member, hooks, signal);
      await this.scope.following.completeGroup(entry.id, signal);
    });
  }
  private async runAction(
    entry: ReferentialEntry,
    hooks: NativeRowHooks,
    signal: AbortSignal,
  ): Promise<void> {
    const scope = this.scope;
    if (entry.kind === "checkReference" || entry.kind === "restrict" || entry.kind === "noAction") {
      await scope.restrictions.run(entry, signal);
      return;
    }
    const command: CommandToken = {
        rootId: entry.images.spoolId,
        id: entry.commandId,
        frameId: entry.frameId,
      },
      slot = this.#slots.find((slot) => slot.command === 0),
      check = () => {
        signal.throwIfAborted();
        scope.signal.throwIfAborted();
        if (scope.commands.budget(command) !== entry.triggerBudget)
          throw new TypeError("Retired RI action");
      };
    check();
    if (!slot || this.#slots.some((slot) => slot.command === command.id))
      throw new TypeError("Concurrent RI action");
    slot.command = command.id;
    try {
      const parent = await scope.commands.accepted(command, entry.images);
      check();
      let childId: number | null = null;
      for (const table of scope.catalog.tables)
        for (const fk of table.foreignKeys) {
          if (fk.id !== entry.fkId || fk.parentTableId !== parent.tableId) continue;
          if (
            (!fk.enforced && !(entry.kind === "follow" && entry.event === "parentUpdate")) ||
            (entry.event === "parentDelete"
              ? fk.onDelete
              : entry.event === "parentUpdate"
                ? fk.onUpdate
                : null) !== entry.kind
          )
            throw new TypeError("RI mode differs from catalog");
          childId = table.id;
        }
      if (
        childId === null ||
        (entry.event === "parentDelete"
          ? parent.event !== "delete"
          : entry.event !== "parentUpdate" || parent.event !== "update")
      )
        throw new TypeError("Invalid mutating parent RI event");
      const child = scope.catalog.table(childId),
        parentTable = scope.catalog.table(parent.tableId),
        fk = child?.foreignKeys.find((fk) => fk.id === entry.fkId),
        parentIndex = parentTable?.indexes.find((index) => index.id === fk?.parentIndexId);
      if (!child || !parentIndex || fk?.columnIds.length !== parentIndex.columnIds.length)
        throw new TypeError("Invalid RI key mapping");
      if (
        (parent.event === "update" || entry.kind !== "cascade") &&
        child.columns.some(
          (column) => column.generatedSql !== null && fk.columnIds.includes(column.id),
        )
      )
        throw new UnsupportedPlanError(
          "referential assignments to generated columns require preflight",
        );
      if (
        parent.event === "update" &&
        !(await scope.checks.parentKeyChanged(parent, parentIndex.id, signal))
      )
        return;
      check();
      const targets =
        entry.kind === "follow"
          ? await scope.following.require(entry.id, signal)
          : await scope.checks.captureChildren(parent, child.id, fk.id, scope.targets, signal);
      check();
      const input =
        entry.kind === "follow"
          ? await scope.following.childInput(entry.id, signal)
          : await scope.inputs.seal(child.id, targets, signal);
      check();
      try {
        const shape = await scope.inputs.describe(input, signal);
        check();
        if (shape.tableId !== child.id) throw new TypeError("Foreign RI child input");
        scope.releaseIdleStep();
        for (let i = 0; i < shape.count; i++) {
          await scope.checkpoint();
          check();
          const row = await scope.inputs.row(input, i, signal);
          check();
          const event = entry.kind === "cascade" && parent.event === "delete" ? "delete" : "update";
          let images = row.images;
          if (event === "update") {
            const changes = slot.changes;
            if (entry.kind === "setDefault") {
              const defaults = await scope.defaults.evaluate(child.id, fk.columnIds, signal);
              check();
              try {
                for (const change of defaults) changes.push(change);
                images = await scope.images.derive(images, "old", changes, signal);
                check();
              } finally {
                changes.length = 0;
                scope.defaults.retire();
              }
            } else {
              for (const [i, id] of fk.columnIds.entries()) {
                const parentColumn = parentIndex.columnIds[i];
                if (parentColumn === undefined) throw new TypeError("Missing parent key column");
                changes.push({
                  columnId: id,
                  value:
                    entry.kind === "setNull"
                      ? null
                      : { image: parent.images, side: "new", columnId: parentColumn },
                });
              }
              try {
                images = await scope.images.derive(images, "old", changes, signal);
                check();
              } finally {
                changes.length = 0;
              }
            }
          }
          await scope.rows.write(command, child.id, event, images, row.rowId, hooks, signal);
          check();
        }
        // PG's SET DEFAULT action also checks the OLD parent key after its child
        // UPDATE, including a default equal to the disappearing key (no child key change).
        if (entry.kind === "setDefault") {
          const remaining = await scope.checks.captureChildren(
            parent,
            child.id,
            fk.id,
            scope.targets,
            signal,
          );
          check();
          await scope.checks.verifyParentRestriction(
            parent,
            fk.id,
            remaining.count > 0,
            false,
            signal,
          );
          check();
        }
      } finally {
        if (!signal.aborted && !scope.signal.aborted) await scope.inputs.retire(input, signal);
      }
    } finally {
      slot.changes.length = 0;
      slot.command = 0;
    }
  }
  close(): void {
    if (!this.#scope) return;
    if (this.#slots.some((slot) => slot.command !== 0))
      throw new TypeError("RI close requires boundary fence");
    this.#slots = [];
    this.#scope.charge.free(this.#bytes);
    this.#scope = null;
  }
}
