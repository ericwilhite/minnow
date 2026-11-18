/** Actual non-mutating RI actions at their named outer AFTER position. The native
 * boundary bounds concurrent frames and cancels/fences its jobs before close. */
import type { ByteCharge } from "../vector/varlen.js";
import type { CatalogView } from "../access/catalog.js";
import type { ReferentialEntry, CommandToken } from "../access/statement-command.js";
import type { RegistryCommands } from "./registry-commands.js";
import type { RegistryTargets } from "./registry-targets.js";
import type { NativeCommandChecks } from "./command-checks.js";

export const NATIVE_REFERENTIAL_CHECK_BYTES = 1024;
interface ReferenceScope {
  readonly catalog: CatalogView;
  readonly commands: RegistryCommands;
  readonly targets: RegistryTargets;
  readonly checks: NativeCommandChecks;
  readonly signal: AbortSignal;
  readonly charge: ByteCharge;
}
export class NativeReferentialChecks {
  #scope: ReferenceScope | null;
  #active = 0;
  constructor(scope: ReferenceScope) {
    scope.signal.throwIfAborted();
    if (!scope.charge.allocate(NATIVE_REFERENTIAL_CHECK_BYTES))
      throw new RangeError("RI check metadata must be admitted");
    this.#scope = scope;
  }
  get scope(): ReferenceScope {
    if (!this.#scope) throw new TypeError("RI checks closed");
    return this.#scope;
  }
  close(): void {
    if (!this.#scope) return;
    if (this.#active) throw new TypeError("RI check close requires native boundary fence");
    this.#scope.charge.free(NATIVE_REFERENTIAL_CHECK_BYTES);
    this.#scope = null;
  }
  async run(entry: ReferentialEntry, signal: AbortSignal): Promise<void> {
    const { scope } = this,
      command: CommandToken = {
        rootId: entry.images.spoolId,
        id: entry.commandId,
        frameId: entry.frameId,
      },
      check = () => {
        signal.throwIfAborted();
        scope.signal.throwIfAborted();
        if (scope.commands.budget(command) !== entry.triggerBudget)
          throw new TypeError("Invalid RI budget");
      };
    this.#active++;
    try {
      check();
      const row = await scope.commands.accepted(command, entry.images);
      check();
      if (entry.kind === "checkReference") {
        if (row.event === "delete" || !entry.event.startsWith("child"))
          throw new TypeError("Invalid child RI event");
        await scope.checks.verifyReference(row, entry.fkId, signal);
        check();
        return;
      }
      if (entry.kind !== "restrict" && entry.kind !== "noAction")
        throw new TypeError("Invalid non-mutating RI action");
      if (
        (entry.event === "parentDelete" && row.event !== "delete") ||
        (entry.event === "parentUpdate" && row.event !== "update")
      )
        throw new TypeError("RI event differs from accepted mutation");
      let childId: number | null = null,
        parentIndexId: number | null = null;
      for (const child of scope.catalog.tables)
        for (const fk of child.foreignKeys)
          if (fk.id === entry.fkId && fk.parentTableId === row.tableId && fk.enforced) {
            const expected =
              entry.event === "parentDelete"
                ? fk.onDelete
                : entry.event === "parentUpdate"
                  ? fk.onUpdate
                  : null;
            if (expected !== entry.kind) throw new TypeError("RI mode differs from catalog");
            childId = child.id;
            parentIndexId = fk.parentIndexId;
          }
      if (childId === null || parentIndexId === null)
        throw new TypeError("Invalid parent RI event");
      if (entry.event === "parentUpdate") {
        const changed = await scope.checks.parentKeyChanged(row, parentIndexId, signal);
        check();
        if (!changed) return;
      }
      const range = await scope.checks.captureChildren(
        row,
        childId,
        entry.fkId,
        scope.targets,
        signal,
      );
      check();
      await scope.checks.verifyParentRestriction(
        row,
        entry.fkId,
        range.count > 0,
        entry.kind === "restrict",
        signal,
      );
      check();
    } finally {
      this.#active--;
    }
  }
}
