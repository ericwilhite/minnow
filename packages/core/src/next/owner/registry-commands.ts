/** Command ownership and accepted/deferred work in the root's spilled directory. Three live
 * frames at most; successful checkpoints advance through newly accepted work, not undo.length. */
import { stepSignal } from "../step-signal.js";
import type {
  CommandToken,
  ReferentialEntry,
  RowImageRef,
  WriteEvent,
} from "../access/statement-command.js";
import type { MaybePromise } from "../access/key-index.js";
import type { SnapshotImpl } from "../write/view.js";
import type { ByteCharge } from "../vector/varlen.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { RegistryDirectory } from "./registry-directory.js";

const ACCEPTED = 5,
  REFERENTIAL = 6,
  TARGET_NAMESPACE = 7,
  TARGET_BITMAP = 8;
const EVENTS = ["childInsert", "childUpdate", "parentDelete", "parentUpdate"] as const;
const KINDS = [
  "checkReference",
  "follow",
  "cascade",
  "setNull",
  "setDefault",
  "restrict",
  "noAction",
] as const;
const WRITES = ["insert", "update", "delete"] as const;
const CHECKPOINTS: readonly string[] = ["unique", "references", "followTargets"];
let nextReferentialId = 0;
function invalid(): never {
  throw new TypeError("Invalid, foreign or retired native command work");
}
function corrupt(): never {
  throw new SpillCorruptionError("Invalid native command registry record");
}
const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
function positive(id: number): void {
  if (!Number.isSafeInteger(id) || id < 1) invalid();
}
function identity(id: number | null): void {
  if (id !== null && (!Number.isSafeInteger(id) || id < 0)) invalid();
}
export function registryCommandBytes(frames: number): number {
  if (!Number.isInteger(frames) || frames < 1 || frames > 3) invalid();
  return 2048 + frames * 512;
}
export interface AcceptedCommandRow {
  readonly command: CommandToken;
  readonly tableId: number;
  readonly event: WriteEvent;
  readonly images: RowImageRef;
  readonly oldRowId: number | null;
  readonly newRowId: number | null;
}
interface Frame {
  readonly command: CommandToken;
  readonly budget: number;
  head: number;
  tail: number;
  unique: number;
  references: number;
  followTargets: number;
  checking: Promise<void> | null;
  count: number;
  uniqueCount: number;
  referencesCount: number;
  followTargetsCount: number;
}
export interface RegistryCommandScope {
  readonly rootId: number;
  readonly frames: number;
  readonly rowEvents: number;
  readonly directory: RegistryDirectory;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  checkpoint(): MaybePromise<void>;
  /** Immutable root-prepared names, charged with the reachable FK program, not per row. */
  referentialName(fkId: number, event: ReferentialEntry["event"]): string;
}
export class RegistryCommands {
  readonly #stop = new AbortController();
  #scope: RegistryCommandScope | null;
  #frames: Array<Frame | null> = [];
  #record: Uint8Array | null = null;
  #next = 0;
  #nextTargets = 0;
  #pending: Promise<unknown> | null = null;
  #closed = false;
  #closing: Promise<void> | null = null;
  #failure: { reason: unknown } | null = null;
  private get scope(): RegistryCommandScope {
    return this.#scope ?? invalid();
  }
  constructor(scope: RegistryCommandScope) {
    scope.signal.throwIfAborted();
    positive(scope.rootId);
    if (!Number.isSafeInteger(scope.rowEvents) || scope.rowEvents < 0) invalid();
    const bytes = registryCommandBytes(scope.frames);
    this.#scope = scope;
    if (!scope.charge.allocate(bytes)) throw new RangeError("Command registry must be admitted");
    try {
      this.#record = new Uint8Array(128);
      for (let n = 0; n < scope.frames; n++) this.#frames.push(null);
    } catch (error) {
      this.#record = null;
      this.#frames = [];
      this.#scope = null;
      scope.charge.free(bytes);
      throw error;
    }
  }
  #check(signal?: AbortSignal): void {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#failure) throw this.#failure.reason;
  }
  #frame(command: CommandToken): Frame {
    this.#check();
    positive(command.id);
    positive(command.frameId);
    if (command.rootId !== this.scope.rootId) invalid();
    const frame = this.#frames.find((frame) => frame?.command.id === command.id);
    if (frame?.command.frameId !== command.frameId) invalid();
    return frame;
  }
  /** Bounded lifetime query for companion frame-owned metadata; never revives a token. */
  active(command: CommandToken): boolean {
    this.#check();
    return (
      command.rootId === this.scope.rootId &&
      this.#frames.some(
        (frame) => frame?.command.id === command.id && frame.command.frameId === command.frameId,
      )
    );
  }
  #work<T>(run: () => Promise<T>): Promise<T> {
    this.#check();
    if (this.#pending) invalid();
    const work = run().catch((reason: unknown) => {
      this.#failure = { reason };
      throw reason;
    });
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  beginCommand(frameId: number, triggerBudget: number): CommandToken {
    this.#check();
    positive(frameId);
    if (
      !Number.isInteger(triggerBudget) ||
      triggerBudget < -1 ||
      triggerBudget > 1 ||
      this.#frames.some((frame) => frame?.command.frameId === frameId)
    )
      invalid();
    const slot = this.#frames.indexOf(null);
    if (slot < 0) invalid();
    positive(this.#next + 1);
    const command = Object.freeze({ rootId: this.scope.rootId, id: ++this.#next, frameId });
    this.#frames[slot] = {
      command,
      budget: triggerBudget,
      head: 0,
      tail: 0,
      unique: 0,
      references: 0,
      followTargets: 0,
      checking: null,
      count: 0,
      uniqueCount: 0,
      referencesCount: 0,
      followTargetsCount: 0,
    };
    return command;
  }
  /** Synchronous live capability check for primary continuations, before and after awaits. */
  budget(command: CommandToken): number {
    return this.#frame(command).budget;
  }
  /** SQL join/body repeat policy is command-local. RI inputs with proven unique captured
   * targets do not use this tracker: separate actions may legally rewrite the same child.
   * Caller validates the input's statement origin and supplies its original target identity;
   * conflict versions resolve through lineage first, rather than using a rewritten physical ID.
   * Both namespaces and 1024-row bitmaps use the shared paged directory, never a file/Set
   * per command. The existing prepaid record is the only bitmap workspace. */
  testAndSetTarget(
    command: CommandToken,
    tableId: number,
    rowId: number,
    input: SnapshotImpl,
    signal: AbortSignal,
  ): Promise<boolean> {
    const check = () => {
      this.#frame(command);
      signal.throwIfAborted();
      input.check();
    };
    check();
    positive(tableId);
    if (!Number.isSafeInteger(rowId) || rowId < 0) invalid();
    return this.#work(async () => {
      const record = this.#record ?? invalid();
      const present = await this.scope.directory.get(TARGET_NAMESPACE, tableId, record, command.id);
      check();
      const header = viewOf(record);
      let namespace: number;
      if (present) {
        namespace = header.getFloat64(0, true);
        if (
          !Number.isSafeInteger(namespace) ||
          namespace < 1 ||
          header.getFloat64(8, true) !== tableId ||
          header.getFloat64(16, true) !== command.id
        )
          corrupt();
      } else {
        positive(this.#nextTargets + 1);
        namespace = ++this.#nextTargets;
        record.fill(0);
        header.setFloat64(0, namespace, true);
        header.setFloat64(8, tableId, true);
        header.setFloat64(16, command.id, true);
        await this.scope.directory.put(TARGET_NAMESPACE, tableId, record, command.id);
        check();
      }
      const block = Math.floor(rowId / 1024),
        bit = rowId % 1024,
        byte = Math.floor(bit / 8),
        mask = 1 << (bit % 8);
      await this.scope.directory.get(TARGET_BITMAP, block, record, namespace);
      check();
      const seen = ((record[byte] ?? 0) & mask) !== 0;
      if (!seen) {
        record[byte] = (record[byte] ?? 0) | mask;
        await this.scope.directory.put(TARGET_BITMAP, block, record, namespace);
        check();
      }
      return seen;
    });
  }
  async #get(kind: number, id: number): Promise<DataView> {
    const record = this.#record;
    if (!record) invalid();
    if (!(await this.scope.directory.get(kind, id, record))) invalid();
    this.#check();
    return viewOf(record);
  }
  accept(row: AcceptedCommandRow): Promise<void> {
    const frame = this.#frame(row.command);
    positive(row.tableId);
    positive(row.images.row);
    if (row.images.spoolId !== this.scope.rootId || !WRITES.includes(row.event)) invalid();
    identity(row.oldRowId);
    identity(row.newRowId);
    if (
      (row.event === "insert" && (row.oldRowId !== null || row.newRowId === null)) ||
      (row.event === "delete" && (row.oldRowId === null || row.newRowId !== null)) ||
      (row.event === "update" && (row.oldRowId === null || row.newRowId === null))
    )
      invalid();
    return this.#work(async () => {
      const record = this.#record;
      if (!record) invalid();
      if (await this.scope.directory.get(ACCEPTED, row.images.row, record)) invalid();
      this.#check();
      const view = viewOf(record);
      record.fill(0);
      view.setFloat64(0, row.command.id, true);
      view.setFloat64(8, row.command.frameId, true);
      view.setFloat64(16, row.tableId, true);
      view.setUint8(24, WRITES.indexOf(row.event));
      view.setFloat64(48, row.oldRowId ?? -1, true);
      view.setFloat64(56, row.newRowId ?? -1, true);
      await this.scope.directory.put(ACCEPTED, row.images.row, record);
      this.#check();
      if (frame.tail) {
        const tail = await this.#get(ACCEPTED, frame.tail);
        tail.setFloat64(32, row.images.row, true);
        await this.scope.directory.put(ACCEPTED, frame.tail, record);
        this.#check();
      } else frame.head = row.images.row;
      frame.tail = row.images.row;
      positive(frame.count + 1);
      frame.count++;
    });
  }
  async #row(
    command: CommandToken,
    id: number,
  ): Promise<{ row: AcceptedCommandRow; next: number }> {
    this.#frame(command);
    const view = await this.#get(ACCEPTED, id);
    if (view.getFloat64(0, true) !== command.id || view.getFloat64(8, true) !== command.frameId)
      invalid();
    const event = WRITES[view.getUint8(24)];
    if (!event) corrupt();
    const tableId = view.getFloat64(16, true),
      old = view.getFloat64(48, true),
      next = view.getFloat64(56, true);
    positive(tableId);
    identity(old === -1 ? null : old);
    identity(next === -1 ? null : next);
    return {
      row: {
        command,
        tableId,
        event,
        images: { spoolId: this.scope.rootId, row: id },
        oldRowId: old === -1 ? null : old,
        newRowId: next === -1 ? null : next,
      },
      next: view.getFloat64(32, true),
    };
  }
  /** Canonical accepted metadata; no mutable record or key buffer escapes. */
  accepted(command: CommandToken, images: RowImageRef): Promise<AcceptedCommandRow> {
    this.#frame(command);
    if (images.spoolId !== this.scope.rootId) invalid();
    return this.#work(async () => (await this.#row(command, images.row)).row);
  }
  /** Called for every action, including a second action under a previously verified token.
   * Only the successful prefix advances. No AFTER callback or root end is invoked here. */
  verify(
    command: CommandToken,
    kind: "unique" | "references" | "followTargets",
    check: (row: AcceptedCommandRow, signal: AbortSignal) => Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    const frame = this.#frame(command);
    this.#check(signal);
    if (frame.checking || !CHECKPOINTS.includes(kind)) invalid();
    const stop = frame.tail,
      countKey =
        kind === "unique"
          ? "uniqueCount"
          : kind === "references"
            ? "referencesCount"
            : "followTargetsCount",
      remaining = frame.count - frame[countKey],
      step = stepSignal("command checkpoint", signal, this.scope.signal, this.#stop.signal),
      combined = step.signal;
    const run = async () => {
      let cursor = frame[kind]
        ? (await this.#work(() => this.#row(command, frame[kind]))).next
        : frame.head;
      let left = remaining;
      while (left-- > 0) {
        positive(cursor);
        this.#check(combined);
        await this.scope.checkpoint();
        this.#check(combined);
        const item = await this.#work(() => this.#row(command, cursor));
        await check(item.row, combined);
        this.#check(combined);
        this.#frame(command);
        frame[kind] = cursor;
        frame[countKey]++;
        if (cursor === stop) {
          if (left !== 0) corrupt();
          break;
        }
        cursor = item.next;
        if (left === 0) corrupt();
      }
    };
    const pending = run()
      .catch((reason: unknown) => {
        this.#failure = { reason };
        throw reason;
      })
      .finally(() => {
        step.close();
        if (frame.checking === pending) frame.checking = null;
      });
    frame.checking = pending;
    return pending;
  }
  addReferential(
    command: CommandToken,
    row: RowImageRef,
    entry: Pick<ReferentialEntry, "fkId" | "event" | "kind" | "targets">,
  ): Promise<number> {
    const frame = this.#frame(command);
    if (row.spoolId !== this.scope.rootId) invalid();
    positive(entry.fkId);
    if (
      !EVENTS.includes(entry.event) ||
      !KINDS.includes(entry.kind) ||
      (entry.kind === "checkReference") !== entry.event.startsWith("child") ||
      (entry.kind === "follow" && entry.event !== "parentUpdate") ||
      (entry.targets &&
        (entry.targets.spoolId !== this.scope.rootId ||
          !Number.isSafeInteger(entry.targets.count) ||
          entry.targets.count < 0 ||
          !Number.isSafeInteger(entry.targets.from) ||
          entry.targets.from < 1))
    )
      invalid();
    return this.#work(async () => {
      await this.#row(command, row.row);
      const record = this.#record;
      if (!record) invalid();
      const view = viewOf(record),
        first = view.getFloat64(64, true),
        last = view.getFloat64(72, true),
        count = view.getFloat64(80, true);
      if (!Number.isSafeInteger(count) || count < 0 || count >= this.scope.rowEvents) invalid();
      positive(nextReferentialId + 1);
      const id = ++nextReferentialId;
      record.fill(0);
      view.setFloat64(0, id, true);
      view.setFloat64(8, command.id, true);
      view.setFloat64(16, command.frameId, true);
      view.setFloat64(24, entry.fkId, true);
      view.setFloat64(32, row.row, true);
      view.setFloat64(40, entry.targets?.from ?? 0, true);
      view.setFloat64(48, entry.targets?.count ?? 0, true);
      view.setUint8(56, EVENTS.indexOf(entry.event));
      view.setUint8(57, KINDS.indexOf(entry.kind));
      view.setUint8(58, frame.budget + 1);
      view.setUint8(59, entry.targets ? 1 : 0);
      await this.scope.directory.put(REFERENTIAL, id, record);
      this.#check();
      if (last) {
        const previous = await this.#get(REFERENTIAL, last);
        previous.setFloat64(72, id, true);
        await this.scope.directory.put(REFERENTIAL, last, record);
        this.#check();
      }
      const accepted = await this.#get(ACCEPTED, row.row);
      accepted.setFloat64(64, first || id, true);
      accepted.setFloat64(72, id, true);
      accepted.setFloat64(80, count + 1, true);
      await this.scope.directory.put(ACCEPTED, row.row, record);
      this.#check();
      return id;
    });
  }
  #decodeEntry(view: DataView): ReferentialEntry {
    const event = EVENTS[view.getUint8(56)],
      kind = KINDS[view.getUint8(57)],
      id = view.getFloat64(0, true),
      fkId = view.getFloat64(24, true),
      triggerBudget = view.getUint8(58) - 1;
    if (!event || !kind || view.getUint8(59) > 1 || view.getUint8(60) !== 0) invalid();
    positive(id);
    positive(fkId);
    const command = {
        rootId: this.scope.rootId,
        id: view.getFloat64(8, true),
        frameId: view.getFloat64(16, true),
      },
      frame = this.#frame(command);
    if (triggerBudget !== frame.budget) invalid();
    const from = view.getFloat64(40, true),
      count = view.getFloat64(48, true);
    if (!Number.isSafeInteger(count) || count < 0 || !Number.isSafeInteger(from) || from < 0)
      corrupt();
    const sortName = this.scope.referentialName(fkId, event);
    if (!sortName.startsWith("RI_ConstraintTrigger_")) invalid();
    return {
      id,
      commandId: command.id,
      frameId: command.frameId,
      fkId,
      event,
      kind,
      sortName,
      images: { spoolId: this.scope.rootId, row: view.getFloat64(32, true) },
      targets: view.getUint8(59) ? { spoolId: this.scope.rootId, from, count } : null,
      triggerBudget,
    };
  }
  resolveReferential(id: number): Promise<ReferentialEntry> {
    positive(id);
    return this.#work(async () => this.#decodeEntry(await this.#get(REFERENTIAL, id)));
  }
  async *rowReferentialEntries(
    command: CommandToken,
    row: RowImageRef,
  ): AsyncIterable<ReferentialEntry> {
    this.#frame(command);
    if (row.spoolId !== this.scope.rootId) invalid();
    const header = await this.#work(async () => {
      await this.#row(command, row.row);
      const view = viewOf(this.#record ?? invalid());
      return { head: view.getFloat64(64, true), count: view.getFloat64(80, true) };
    });
    if (
      !Number.isSafeInteger(header.count) ||
      header.count < 0 ||
      header.count > this.scope.rowEvents
    )
      corrupt();
    let cursor = header.head;
    for (let left = header.count; left > 0; left--) {
      positive(cursor);
      const item = await this.#work(async () => {
        const view = await this.#get(REFERENTIAL, cursor);
        return { entry: this.#decodeEntry(view), next: view.getFloat64(72, true) };
      });
      if (item.entry.commandId !== command.id || item.entry.images.row !== row.row) corrupt();
      yield item.entry;
      cursor = item.next;
    }
    if (cursor !== 0) corrupt();
  }
  retireReferential(id: number): Promise<void> {
    positive(id);
    return this.#work(async () => {
      const view = await this.#get(REFERENTIAL, id);
      this.#decodeEntry(view);
      view.setUint8(60, 1);
      await this.scope.directory.put(REFERENTIAL, id, this.#record ?? invalid());
      this.#check();
    });
  }
  retireCommand(command: CommandToken): void {
    const frame = this.#frame(command);
    if (frame.checking || frame.unique !== frame.tail || frame.references !== frame.tail) invalid();
    const slot = this.#frames.indexOf(frame);
    if (slot < 0) invalid();
    this.#frames[slot] = null;
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new TypeError("Native commands are closed"));
      await this.#pending?.catch(() => undefined);
      for (const frame of this.#frames) await frame?.checking?.catch(() => undefined);
      this.#record = null;
      this.#frames.length = 0;
      this.#pending = null;
      this.#failure = null;
      this.scope.charge.free(registryCommandBytes(this.scope.frames));
      this.#scope = null;
    })());
  }
}
