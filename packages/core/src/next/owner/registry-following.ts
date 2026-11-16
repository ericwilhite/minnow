/** Original follow inputs bind by scalar RI ID. Queued entries never change, and every
 * range shares the root directory. Source capture is a checkpoint, not an AFTER drain. */
import type { CatalogView, ForeignKeyDef, TableDef } from "../access/catalog.js";
import type {
  CommandToken,
  ReferentialEntry,
  RowTargetRange,
} from "../access/statement-command.js";
import type { ByteCharge } from "../vector/varlen.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { RegistryCommands } from "./registry-commands.js";
import type { RegistryDirectory } from "./registry-directory.js";
import type { RegistryTargets } from "./registry-targets.js";
import type { NativeCommandChecks } from "./command-checks.js";

export const REGISTRY_FOLLOWING_BYTES = 1024;
const FOLLOWING = 9;
export interface RegistryFollowingScope {
  readonly rootId: number;
  readonly catalog: CatalogView;
  readonly commands: Pick<
    RegistryCommands,
    "verify" | "rowReferentialEntries" | "resolveReferential"
  >;
  readonly directory: RegistryDirectory;
  readonly targets: RegistryTargets;
  readonly checks: Pick<NativeCommandChecks, "captureChildren">;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
}
function invalid(): never {
  throw new TypeError("Invalid, unprepared or retired native follow input");
}
function corrupt(): never {
  throw new SpillCorruptionError("Invalid native follow binding");
}
const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
export class RegistryFollowing {
  readonly #stop = new AbortController();
  #scope: RegistryFollowingScope | null;
  #record: Uint8Array | null = null;
  #pending: Promise<unknown> | null = null;
  #closing: Promise<void> | null = null;
  #failure: { reason: unknown } | null = null;
  #closed = false;
  constructor(scope: RegistryFollowingScope) {
    scope.signal.throwIfAborted();
    if (!Number.isSafeInteger(scope.rootId) || scope.rootId < 1) invalid();
    if (!scope.charge.allocate(REGISTRY_FOLLOWING_BYTES))
      throw new RangeError("Follow metadata must be admitted");
    this.#scope = scope;
    try {
      this.#record = new Uint8Array(128);
    } catch (error) {
      scope.charge.free(REGISTRY_FOLLOWING_BYTES);
      this.#scope = null;
      throw error;
    }
  }
  private get scope(): RegistryFollowingScope {
    return this.#scope ?? invalid();
  }
  #check(signal?: AbortSignal): void {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#failure) throw this.#failure.reason;
  }
  #work<T>(signal: AbortSignal, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    this.#check(signal);
    if (this.#pending) invalid();
    const combined = AbortSignal.any([signal, this.scope.signal, this.#stop.signal]);
    const work = run(combined).catch((reason: unknown) => {
      this.#failure = { reason };
      throw reason;
    });
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  #foreignKey(entry: ReferentialEntry): { child: TableDef; fk: ForeignKeyDef } {
    if (
      entry.kind !== "follow" ||
      entry.event !== "parentUpdate" ||
      entry.images.spoolId !== this.scope.rootId
    )
      invalid();
    for (const child of this.scope.catalog.tables)
      for (const fk of child.foreignKeys)
        if (fk.id === entry.fkId) {
          if (fk.onUpdate !== "follow") invalid();
          return { child, fk };
        }
    return invalid();
  }
  async #read(entry: ReferentialEntry, signal: AbortSignal): Promise<RowTargetRange | null> {
    const record = this.#record ?? invalid();
    const found = await this.scope.directory.get(FOLLOWING, entry.id, record);
    this.#check(signal);
    if (!found) return null;
    const view = viewOf(record),
      { child, fk } = this.#foreignKey(entry);
    if (
      view.getFloat64(0, true) !== this.scope.rootId ||
      view.getFloat64(8, true) !== entry.id ||
      view.getFloat64(16, true) !== entry.commandId ||
      view.getFloat64(24, true) !== entry.frameId ||
      view.getFloat64(32, true) !== entry.fkId ||
      view.getFloat64(40, true) !== entry.images.row ||
      view.getFloat64(64, true) !== fk.parentTableId ||
      view.getFloat64(72, true) !== child.id ||
      view.getFloat64(80, true) !== child.schemaVersion ||
      view.getInt8(88) !== entry.triggerBudget
    )
      corrupt();
    const from = view.getFloat64(48, true),
      count = view.getFloat64(56, true);
    if (
      !Number.isSafeInteger(from) ||
      from < 1 ||
      !Number.isSafeInteger(count) ||
      count < 0 ||
      !Number.isSafeInteger(from + count)
    )
      corrupt();
    return { spoolId: this.scope.rootId, from, count };
  }
  /** After newly accepted primary rows pass uniqueness and before their AFTER drain.
   * Every new follow range is sealed before any RI writer runs. Subsequent checkpoints
   * visit NEW accepted rows only; re-probing an old key after follow writes is wrong. */
  prepare(command: CommandToken, signal: AbortSignal): Promise<void> {
    return this.#work(signal, async (stop) => {
      if (command.rootId !== this.scope.rootId) invalid();
      await this.scope.commands.verify(
        command,
        "followTargets",
        async (row, inner) => {
          for await (const entry of this.scope.commands.rowReferentialEntries(
            command,
            row.images,
          )) {
            this.#check(inner);
            if (entry.kind !== "follow") continue;
            const { child, fk } = this.#foreignKey(entry);
            if (fk.parentTableId !== row.tableId || row.event !== "update") invalid();
            if (await this.#read(entry, inner)) continue;
            const range =
              entry.targets ??
              (await this.scope.checks.captureChildren(
                row,
                child.id,
                fk.id,
                this.scope.targets,
                inner,
              ));
            this.#check(inner);
            if (
              range.spoolId !== this.scope.rootId ||
              !Number.isSafeInteger(range.from) ||
              range.from < 1 ||
              !Number.isSafeInteger(range.count) ||
              range.count < 0
            )
              invalid();
            const record = this.#record ?? invalid(),
              view = viewOf(record);
            record.fill(0);
            view.setFloat64(0, this.scope.rootId, true);
            view.setFloat64(8, entry.id, true);
            view.setFloat64(16, entry.commandId, true);
            view.setFloat64(24, entry.frameId, true);
            view.setFloat64(32, entry.fkId, true);
            view.setFloat64(40, entry.images.row, true);
            view.setFloat64(48, range.from, true);
            view.setFloat64(56, range.count, true);
            view.setFloat64(64, fk.parentTableId, true);
            view.setFloat64(72, child.id, true);
            view.setFloat64(80, child.schemaVersion, true);
            view.setInt8(88, entry.triggerBudget);
            await this.scope.directory.put(FOLLOWING, entry.id, record);
            this.#check(inner);
          }
        },
        stop,
      );
    });
  }
  /** Resolve active origin first; stale IDs cannot read a binding from an earlier command. */
  require(id: number, signal: AbortSignal): Promise<RowTargetRange> {
    return this.#work(signal, async (stop) => {
      const entry = await this.scope.commands.resolveReferential(id);
      this.#check(stop);
      this.#foreignKey(entry);
      return (await this.#read(entry, stop)) ?? invalid();
    });
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new Error("Native follow inputs closed"));
      await this.#pending?.catch(() => undefined);
      this.#record = null;
      this.#failure = null;
      this.scope.charge.free(REGISTRY_FOLLOWING_BYTES);
      this.#scope = null;
    })());
  }
}
