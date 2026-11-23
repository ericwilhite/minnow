/** Original follow inputs bind by scalar RI ID. Queued entries never change, and every
 * range shares the root directory. Source capture is a checkpoint, not an AFTER drain. */
import { stepSignal } from "../step-signal.js";
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
import type { NativeChildInputRef } from "./child-inputs.js";
import type { NativeCommandChecks } from "./command-checks.js";

export const REGISTRY_FOLLOWING_BYTES = 1024;
export function registryFollowingBytes(catalog: CatalogView, frames = 3): number {
  if (!Number.isInteger(frames) || frames < 1 || frames > 3) invalid();
  let keys = 0;
  for (const table of catalog.tables)
    for (const fk of table.foreignKeys) if (fk.onUpdate === "follow") keys++;
  const bytes = REGISTRY_FOLLOWING_BYTES + 256 + 8 * keys + frames * (256 + 8 * keys);
  if (!Number.isSafeInteger(bytes)) invalid();
  return bytes;
}
const FOLLOWING = 9,
  FOLLOW_INPUT = 14,
  FOLLOW_TARGET_NAMESPACE = 15,
  FOLLOW_TARGET_BITMAP = 16;
export interface RegistryFollowingScope {
  readonly rootId: number;
  readonly frames?: number;
  readonly catalog: CatalogView;
  readonly commands: Pick<
    RegistryCommands,
    "verify" | "rowReferentialEntries" | "resolveReferential" | "active"
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
  #nextVisited = 0;
  readonly #bytes: number;
  #keys: Float64Array | null = null;
  #groups: Array<{ command: CommandToken | null; heads: Float64Array }> = [];
  constructor(scope: RegistryFollowingScope) {
    scope.signal.throwIfAborted();
    if (!Number.isSafeInteger(scope.rootId) || scope.rootId < 1) invalid();
    this.#bytes = registryFollowingBytes(scope.catalog, scope.frames);
    if (!scope.charge.allocate(this.#bytes))
      throw new RangeError("Follow metadata must be admitted");
    this.#scope = scope;
    try {
      this.#record = new Uint8Array(128);
      let keys = 0;
      for (const table of scope.catalog.tables)
        for (const fk of table.foreignKeys) if (fk.onUpdate === "follow") keys++;
      this.#keys = new Float64Array(keys);
      let at = 0;
      for (const table of scope.catalog.tables)
        for (const fk of table.foreignKeys) if (fk.onUpdate === "follow") this.#keys[at++] = fk.id;
      this.#keys.sort();
      for (let n = 0; n < (scope.frames ?? 3); n++)
        this.#groups.push({ command: null, heads: new Float64Array(keys) });
    } catch (error) {
      scope.charge.free(this.#bytes);
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
    const step = stepSignal("following", signal, this.scope.signal, this.#stop.signal);
    let work: Promise<T>;
    try {
      work = run(step.signal).catch((reason: unknown) => {
        this.#failure = { reason };
        throw reason;
      });
    } catch (error) {
      step.close();
      throw error;
    }
    const pending = work.finally(() => {
      step.close();
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
  #groupSlot(command: CommandToken) {
    let slot = this.#groups.find((slot) => slot.command?.id === command.id);
    if (!slot) {
      slot = this.#groups.find(
        (slot) => !slot.command || !this.scope.commands.active(slot.command),
      );
      if (!slot) invalid();
      slot.command = command;
      slot.heads.fill(0);
    }
    return slot;
  }
  #keyIndex(fkId: number) {
    const keys = this.#keys ?? invalid();
    let lo = 0,
      hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if ((keys[mid] ?? Infinity) < fkId) lo = mid + 1;
      else hi = mid;
    }
    if (keys[lo] !== fkId) invalid();
    return lo;
  }
  async #packet(id: number, signal: AbortSignal) {
    const record = this.#record ?? invalid();
    if (!(await this.scope.directory.get(FOLLOWING, id, record))) invalid();
    this.#check(signal);
    const view = viewOf(record);
    if (view.getFloat64(0, true) !== this.scope.rootId || view.getFloat64(8, true) !== id)
      corrupt();
    return view;
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
            const slot = this.#groupSlot(command),
              key = this.#keyIndex(fk.id),
              saved = slot.heads[key] ?? 0,
              leader = saved === 0 ? entry.id : saved,
              record = this.#record ?? invalid(),
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
            view.setFloat64(104, leader, true);
            if (leader === entry.id) view.setFloat64(112, entry.id, true);
            await this.scope.directory.put(FOLLOWING, entry.id, record);
            this.#check(inner);
            if (leader === entry.id) slot.heads[key] = entry.id;
            else {
              const first = await this.#packet(leader, inner),
                tail = first.getFloat64(112, true);
              if (
                first.getFloat64(16, true) !== command.id ||
                first.getFloat64(32, true) !== fk.id ||
                first.getUint8(120) !== 0
              )
                corrupt();
              const last = await this.#packet(tail, inner);
              last.setFloat64(96, entry.id, true);
              await this.scope.directory.put(FOLLOWING, tail, record);
              const head = await this.#packet(leader, inner);
              head.setFloat64(112, entry.id, true);
              await this.scope.directory.put(FOLLOWING, leader, record);
              this.#check(inner);
            }
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
  /** One omitted-update follow group is a statement-end uniqueness unit. Packets and
   * captured original targets spill in shared storage; no per-row resident list exists. */
  async *group(id: number, signal: AbortSignal): AsyncIterable<ReferentialEntry> {
    const start = await this.#work(signal, async (stop) => {
      const entry = await this.scope.commands.resolveReferential(id);
      this.#foreignKey(entry);
      if (!(await this.#read(entry, stop))) invalid();
      const own = await this.#packet(id, stop),
        leader = own.getFloat64(104, true),
        head = await this.#packet(leader, stop);
      if (head.getUint8(120) === 1) return null;
      if (head.getUint8(120) !== 0 || head.getFloat64(104, true) !== leader) corrupt();
      return { leader, tail: head.getFloat64(112, true), command: entry.commandId, fk: entry.fkId };
    });
    if (!start) return;
    let cursor = start.leader;
    for (;;) {
      const item = await this.#work(signal, async (stop) => {
        const entry = await this.scope.commands.resolveReferential(cursor);
        if (entry.commandId !== start.command || entry.fkId !== start.fk) corrupt();
        this.#foreignKey(entry);
        const view = await this.#packet(cursor, stop);
        if (view.getFloat64(104, true) !== start.leader) corrupt();
        return { entry, next: view.getFloat64(96, true) };
      });
      yield item.entry;
      if (cursor === start.tail) break;
      if (!Number.isSafeInteger(item.next) || item.next <= cursor) corrupt();
      cursor = item.next;
    }
  }
  /** Scalar input handles use the shared directory; whole-group OLD payloads never form
   * a resident list or one file per parent. All seals precede the first group mutation. */
  bindChildInput(id: number, input: NativeChildInputRef, signal: AbortSignal): Promise<void> {
    return this.#work(signal, async (stop) => {
      const entry = await this.scope.commands.resolveReferential(id),
        { child } = this.#foreignKey(entry);
      if (!(await this.#read(entry, stop))) invalid();
      if (input.rootId !== this.scope.rootId || !Number.isSafeInteger(input.id) || input.id < 1)
        invalid();
      const record = this.#record ?? invalid();
      if (await this.scope.directory.get(FOLLOW_INPUT, id, record)) invalid();
      this.#check(stop);
      record.fill(0);
      const view = viewOf(record);
      view.setFloat64(0, this.scope.rootId, true);
      view.setFloat64(8, id, true);
      view.setFloat64(16, entry.commandId, true);
      view.setFloat64(24, entry.fkId, true);
      view.setFloat64(32, child.id, true);
      view.setFloat64(40, child.schemaVersion, true);
      view.setFloat64(48, input.id, true);
      await this.scope.directory.put(FOLLOW_INPUT, id, record);
      this.#check(stop);
    });
  }
  childInput(id: number, signal: AbortSignal): Promise<NativeChildInputRef> {
    return this.#work(signal, async (stop) => {
      const entry = await this.scope.commands.resolveReferential(id),
        { child } = this.#foreignKey(entry),
        record = this.#record ?? invalid();
      if (!(await this.scope.directory.get(FOLLOW_INPUT, id, record))) invalid();
      this.#check(stop);
      const view = viewOf(record);
      if (
        view.getFloat64(0, true) !== this.scope.rootId ||
        view.getFloat64(8, true) !== id ||
        view.getFloat64(16, true) !== entry.commandId ||
        view.getFloat64(24, true) !== entry.fkId ||
        view.getFloat64(32, true) !== child.id ||
        view.getFloat64(40, true) !== child.schemaVersion
      )
        corrupt();
      const input = view.getFloat64(48, true);
      if (!Number.isSafeInteger(input) || input < 1) corrupt();
      return { rootId: this.scope.rootId, id: input };
    });
  }
  /** A follow edge rewrites an original child at most once in its containing command.
   * Later RI rows may name newer physical versions of the same origin. The shared paged
   * bitmap prevents re-follow cycles without retaining a Set or a file per action. */
  testAndSetFollowTarget(id: number, originalId: number, signal: AbortSignal): Promise<boolean> {
    return this.#work(signal, async (stop) => {
      if (!Number.isSafeInteger(originalId) || originalId < 0) invalid();
      const entry = await this.scope.commands.resolveReferential(id);
      this.#foreignKey(entry);
      const record = this.#record ?? invalid(),
        found = await this.scope.directory.get(
          FOLLOW_TARGET_NAMESPACE,
          entry.fkId,
          record,
          entry.commandId,
        );
      this.#check(stop);
      let namespace: number;
      const view = viewOf(record);
      if (found) {
        namespace = view.getFloat64(0, true);
        if (
          !Number.isSafeInteger(namespace) ||
          namespace < 1 ||
          view.getFloat64(8, true) !== entry.fkId ||
          view.getFloat64(16, true) !== entry.commandId ||
          view.getFloat64(24, true) !== this.scope.rootId
        )
          corrupt();
      } else {
        if (!Number.isSafeInteger(this.#nextVisited + 1)) invalid();
        namespace = ++this.#nextVisited;
        record.fill(0);
        view.setFloat64(0, namespace, true);
        view.setFloat64(8, entry.fkId, true);
        view.setFloat64(16, entry.commandId, true);
        view.setFloat64(24, this.scope.rootId, true);
        await this.scope.directory.put(
          FOLLOW_TARGET_NAMESPACE,
          entry.fkId,
          record,
          entry.commandId,
        );
        this.#check(stop);
      }
      const block = Math.floor(originalId / 1024),
        bit = originalId % 1024,
        byte = Math.floor(bit / 8),
        mask = 1 << (bit % 8);
      await this.scope.directory.get(FOLLOW_TARGET_BITMAP, block, record, namespace);
      this.#check(stop);
      const seen = ((record[byte] ?? 0) & mask) !== 0;
      if (!seen) {
        record[byte] = (record[byte] ?? 0) | mask;
        await this.scope.directory.put(FOLLOW_TARGET_BITMAP, block, record, namespace);
        this.#check(stop);
      }
      return seen;
    });
  }
  completeGroup(id: number, signal: AbortSignal): Promise<void> {
    return this.#work(signal, async (stop) => {
      const entry = await this.scope.commands.resolveReferential(id);
      this.#foreignKey(entry);
      if (!(await this.#read(entry, stop))) invalid();
      const own = await this.#packet(id, stop),
        leader = own.getFloat64(104, true),
        head = await this.#packet(leader, stop);
      if (
        head.getFloat64(104, true) !== leader ||
        head.getFloat64(16, true) !== entry.commandId ||
        head.getFloat64(32, true) !== entry.fkId ||
        head.getUint8(120) > 1
      )
        corrupt();
      head.setUint8(120, 1);
      await this.scope.directory.put(FOLLOWING, leader, this.#record ?? invalid());
      const slot = this.#groups.find((slot) => slot.command?.id === entry.commandId),
        key = this.#keyIndex(entry.fkId);
      if (slot?.heads[key] === leader) slot.heads[key] = 0;
      this.#check(stop);
    });
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new Error("Native follow inputs closed"));
      await this.#pending?.catch(() => undefined);
      this.#record = null;
      this.#keys = null;
      this.#groups = [];
      this.#failure = null;
      this.scope.charge.free(this.#bytes);
      this.#scope = null;
    })());
  }
}
