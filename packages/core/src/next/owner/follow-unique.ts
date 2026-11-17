/** Implicit follow owns uniqueness of its captured original children until group completion.
 * Nested RI/reference work still runs. Membership and accepted versions share the root's
 * paged directory; no target-count-sized resident collection or per-group file exists. */
import type { CatalogView } from "../access/catalog.js";
import type { CommandToken, ReferentialEntry } from "../access/statement-command.js";
import type { MaybePromise } from "../access/key-index.js";
import type { ByteCharge } from "../vector/varlen.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { AcceptedCommandRow } from "./registry-commands.js";
import type { RegistryDirectory } from "./registry-directory.js";
import type { RegistryFollowing } from "./registry-following.js";
import type { RegistryTargets } from "./registry-targets.js";
import type { NativeCommandChecks } from "./command-checks.js";

const MEMBERS = 17,
  DEFERRED = 18;
const EVENTS = ["insert", "update", "delete"] as const;
interface Group {
  command: CommandToken | null;
  id: number;
  table: number;
  head: number;
  tail: number;
  count: number;
}
export interface NativeFollowUniqueScope {
  readonly rootId: number;
  readonly frames: number;
  readonly catalog: CatalogView;
  readonly directory: RegistryDirectory;
  readonly following: Pick<RegistryFollowing, "group" | "require">;
  readonly targets: Pick<RegistryTargets, "rows" | "resolve">;
  readonly checks: Pick<NativeCommandChecks, "verifyUnique">;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  checkpoint(): MaybePromise<void>;
}
export function nativeFollowUniqueBytes(frames: number): number {
  if (!Number.isInteger(frames) || frames < 1 || frames > 3)
    throw new TypeError("Invalid follow uniqueness frame bound");
  return 2048 + 512 * frames;
}
function invalid(): never {
  throw new TypeError("Invalid native follow uniqueness scope");
}
function corrupt(): never {
  throw new SpillCorruptionError("Invalid deferred follow uniqueness record");
}
function positive(n: number): boolean {
  return Number.isSafeInteger(n) && n > 0;
}
const viewOf = (b: Uint8Array) => new DataView(b.buffer, b.byteOffset, b.byteLength);

export class NativeFollowUnique {
  #scope: NativeFollowUniqueScope | null;
  #groups: Group[] = [];
  #depth = 0;
  #record: Uint8Array | null = null;
  #pending: Promise<unknown> | null = null;
  #closing: Promise<void> | null = null;
  #closed = false;
  readonly #bytes: number;
  constructor(scope: NativeFollowUniqueScope) {
    scope.signal.throwIfAborted();
    this.#bytes = nativeFollowUniqueBytes(scope.frames);
    if (!positive(scope.rootId) || !scope.charge.allocate(this.#bytes))
      throw new RangeError("Follow uniqueness must be admitted");
    this.#scope = scope;
    try {
      this.#record = new Uint8Array(128);
      for (let i = 0; i < scope.frames; i++)
        this.#groups.push({ command: null, id: 0, table: 0, head: 0, tail: 0, count: 0 });
    } catch (error) {
      this.#scope = null;
      scope.charge.free(this.#bytes);
      throw error;
    }
  }
  private get scope() {
    return this.#scope ?? invalid();
  }
  #check(signal: AbortSignal) {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal.throwIfAborted();
  }
  #work<T>(signal: AbortSignal, run: () => Promise<T>): Promise<T> {
    this.#check(signal);
    if (this.#pending) invalid();
    const work = run();
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  /** Register ALL original identities before any group member's BEFORE/body/write. */
  begin(entry: ReferentialEntry, signal: AbortSignal): Promise<boolean> {
    return this.#work(signal, async () => {
      if (
        entry.kind !== "follow" ||
        entry.event !== "parentUpdate" ||
        entry.images.spoolId !== this.scope.rootId
      )
        invalid();
      const slot = this.#groups[this.#depth];
      if (!slot || slot.command) invalid();
      const table = this.scope.catalog.tables.find((t) =>
        t.foreignKeys.some((f) => f.id === entry.fkId && f.onUpdate === "follow"),
      );
      if (!table) invalid();
      let found = false;
      for await (const member of this.scope.following.group(entry.id, signal)) {
        this.#check(signal);
        if (member.commandId !== entry.commandId || member.fkId !== entry.fkId) invalid();
        if (!found) {
          found = true;
          slot.command = { rootId: this.scope.rootId, id: entry.commandId, frameId: entry.frameId };
          slot.id = member.id;
          slot.table = table.id;
          slot.head = slot.tail = slot.count = 0;
        }
        const range = await this.scope.following.require(member.id, signal);
        for await (const row of this.scope.targets.rows(range, signal)) {
          this.#check(signal);
          if (row.tableId !== table.id) invalid();
          const block = Math.floor(row.originalId / 1024),
            bit = row.originalId % 1024,
            at = Math.floor(bit / 8),
            mask = 1 << (bit % 8),
            record = this.#record ?? invalid();
          await this.scope.directory.get(MEMBERS, block, record, slot.id);
          this.#check(signal);
          record[at] = (record[at] ?? 0) | mask;
          await this.scope.directory.put(MEMBERS, block, record, slot.id);
          this.#check(signal);
          await this.scope.checkpoint();
        }
      }
      if (found) this.#depth++;
      return found;
    });
  }
  /** Only original follow members qualify; unrelated child-table writes keep their
   * ordinary per-action/command checks. Every accepted version is linked, including a
   * nested rewrite whose original command will retire before this group completes. */
  defer(row: AcceptedCommandRow, signal: AbortSignal): MaybePromise<boolean> {
    this.#check(signal);
    if (
      row.newRowId === null ||
      !this.#groups.some((g, i) => i < this.#depth && g.table === row.tableId)
    )
      return false;
    return this.#work(signal, async () => {
      const original = await this.scope.targets.resolve(row.tableId, row.newRowId ?? invalid());
      this.#check(signal);
      if (!original) return false;
      for (let i = 0; i < this.#depth; i++) {
        const group = this.#groups[i] ?? invalid();
        if (group.table !== row.tableId) continue;
        const record = this.#record ?? invalid(),
          bit = original.originalId % 1024;
        await this.scope.directory.get(
          MEMBERS,
          Math.floor(original.originalId / 1024),
          record,
          group.id,
        );
        this.#check(signal);
        if (((record[Math.floor(bit / 8)] ?? 0) & (1 << (bit % 8))) === 0) continue;
        record.fill(0);
        const v = viewOf(record);
        v.setFloat64(8, row.command.id, true);
        v.setFloat64(16, row.command.frameId, true);
        v.setFloat64(24, row.tableId, true);
        v.setUint8(32, EVENTS.indexOf(row.event));
        v.setFloat64(40, row.newRowId ?? -1, true);
        v.setFloat64(48, row.oldRowId ?? -1, true);
        v.setFloat64(56, this.scope.rootId, true);
        v.setFloat64(64, row.images.row, true);
        v.setFloat64(72, group.id, true);
        await this.scope.directory.put(DEFERRED, row.images.row, record, group.id);
        this.#check(signal);
        if (group.tail) {
          if (!(await this.scope.directory.get(DEFERRED, group.tail, record, group.id))) corrupt();
          this.#check(signal);
          viewOf(record).setFloat64(0, row.images.row, true);
          await this.scope.directory.put(DEFERRED, group.tail, record, group.id);
          this.#check(signal);
        } else group.head = row.images.row;
        group.tail = row.images.row;
        if (!Number.isSafeInteger(++group.count)) invalid();
        return true;
      }
      return false;
    });
  }
  /** Check current live accepted versions before any deferred user observer runs. */
  finish(command: CommandToken, signal: AbortSignal): Promise<void> {
    return this.#work(signal, async () => {
      const group = this.#groups[this.#depth - 1];
      if (!group || group.command?.id !== command.id || group.command.frameId !== command.frameId)
        invalid();
      let cursor = group.head;
      for (let left = group.count; left > 0; left--) {
        const record = this.#record ?? invalid();
        if (
          !positive(cursor) ||
          !(await this.scope.directory.get(DEFERRED, cursor, record, group.id))
        )
          corrupt();
        this.#check(signal);
        const v = viewOf(record),
          event = EVENTS[v.getUint8(32)],
          id = v.getFloat64(64, true),
          next = v.getFloat64(0, true),
          newId = v.getFloat64(40, true),
          oldId = v.getFloat64(48, true),
          originalCommand = v.getFloat64(8, true),
          frameId = v.getFloat64(16, true);
        if (
          !event ||
          id !== cursor ||
          v.getFloat64(56, true) !== this.scope.rootId ||
          v.getFloat64(72, true) !== group.id ||
          v.getFloat64(24, true) !== group.table ||
          !positive(originalCommand) ||
          !positive(frameId) ||
          !Number.isSafeInteger(newId) ||
          newId < 0 ||
          !Number.isSafeInteger(oldId) ||
          oldId < -1
        )
          corrupt();
        await this.scope.checks.verifyUnique(
          {
            command: { rootId: this.scope.rootId, id: originalCommand, frameId },
            tableId: group.table,
            event,
            images: { spoolId: this.scope.rootId, row: id },
            oldRowId: oldId === -1 ? null : oldId,
            newRowId: newId,
          },
          signal,
        );
        this.#check(signal);
        cursor = next;
        await this.scope.checkpoint();
      }
      if (cursor !== 0) corrupt();
      group.command = null;
      this.#depth--;
    });
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      await this.#pending?.catch(() => undefined);
      this.#groups = [];
      this.#record = null;
      this.#depth = 0;
      this.scope.charge.free(this.#bytes);
      this.#scope = null;
    })());
  }
}
