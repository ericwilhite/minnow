/** Native command boundary over the root's paid registries. The action executor owns
 * complete child input capture and writes; this boundary owns incremental checkpoints.
 * This wiring alone neither supplies a compiled body host nor enables SQL firing. */
import type {
  CommandToken,
  NativeCommandBoundary,
  NativeRowHooks,
  ReferentialEntry,
  RowBindingLease,
  RowBindingProjection,
  RowImageLease,
  RowImageRef,
} from "../access/statement-command.js";
import type { CatalogView } from "../access/catalog.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { NativeCommandChecks } from "./command-checks.js";
import type { RegistryImages } from "./registry-images.js";
import type { RegistryCommands } from "./registry-commands.js";
import type { RegistryFollowing } from "./registry-following.js";

function invalid(): never {
  throw new TypeError("Invalid native command execution");
}
export function nativeCommandExecutionBytes(frames: number): number {
  if (!Number.isInteger(frames) || frames < 1 || frames > 3) invalid();
  return 4096 + frames * 1024;
}
export interface NativeCommandExecutionScope {
  readonly frames: number;
  readonly catalog: CatalogView;
  readonly commands: RegistryCommands;
  readonly images: Pick<RegistryImages, "readRowImages" | "readRowBindings" | "close">;
  readonly checks: Pick<NativeCommandChecks, "verifyUnique" | "verifyReference" | "close">;
  /** Capture original follow identities after uniqueness, before named AFTER work. New
   * accepted rows on an existing token need a fresh checkpoint; completed ranges never re-probe. */
  readonly following: Pick<RegistryFollowing, "prepare" | "close">;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  /** Synchronous root-owned AbortController.abort, never a recursive close/rollback.
   * Cancels native writer/source waits as well as the boundary's own callback signals. */
  abortRoot(reason: unknown): void;
  /** Canonical entry only. Seal full child inputs before any child write; preserve budget,
   * append child AFTER to the containing frame and never drain outer AFTER here. Native
   * metadata/mode support must be preflighted before root writes, not skipped here. */
  runReferential(
    entry: ReferentialEntry,
    hooks: NativeRowHooks,
    signal: AbortSignal,
  ): Promise<void>;
}
function sameEntry(a: ReferentialEntry, b: ReferentialEntry): boolean {
  return (
    a.id === b.id &&
    a.commandId === b.commandId &&
    a.frameId === b.frameId &&
    a.fkId === b.fkId &&
    a.event === b.event &&
    a.kind === b.kind &&
    a.sortName === b.sortName &&
    a.triggerBudget === b.triggerBudget &&
    a.images.spoolId === b.images.spoolId &&
    a.images.row === b.images.row &&
    (a.targets === null
      ? b.targets === null
      : b.targets !== null &&
        a.targets.spoolId === b.targets.spoolId &&
        a.targets.from === b.targets.from &&
        a.targets.count === b.targets.count)
  );
}
export class NativeCommandExecution implements NativeCommandBoundary {
  #scope: NativeCommandExecutionScope | null = null;
  #stop: AbortController | null = null;
  #jobs: Array<Promise<unknown> | null> = [];
  #actions: number[] = [];
  #failure: { reason: unknown } | null = null;
  #closed = false;
  #closing: Promise<void> | null = null;
  readonly #bytes: number;
  constructor(scope: NativeCommandExecutionScope) {
    scope.signal.throwIfAborted();
    this.#bytes = nativeCommandExecutionBytes(scope.frames);
    if (!scope.charge.allocate(this.#bytes))
      throw new RangeError("Native execution must be admitted");
    try {
      this.#stop = new AbortController();
      // At most three serial body frames: one action and its incremental check per frame,
      // plus the single image helper. No task/descriptor collection grows with row count.
      this.#jobs = Array.from({ length: 2 * scope.frames + 2 }, () => null);
      this.#actions = Array.from({ length: scope.frames }, () => 0);
      this.#scope = scope;
    } catch (error) {
      this.#jobs = [];
      this.#actions = [];
      this.#stop = null;
      scope.charge.free(this.#bytes);
      throw error;
    }
  }
  private get scope(): NativeCommandExecutionScope {
    return this.#scope ?? invalid();
  }
  #check(signal?: AbortSignal): void {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#failure) throw this.#failure.reason;
  }
  #run<T>(signal: AbortSignal, body: (signal: AbortSignal) => Promise<T>): Promise<T> {
    this.#check(signal);
    const at = this.#jobs.indexOf(null);
    if (at < 0) invalid();
    const stop = this.#stop ?? invalid(),
      rootSignal = this.scope.signal,
      operation = new AbortController(),
      combined = operation.signal;
    const rootAbort = () => operation.abort(rootSignal.reason as unknown),
      callerAbort = () => {
        const reason: unknown = signal.reason;
        operation.abort(reason);
        this.scope.abortRoot(reason);
      },
      closeAbort = () => operation.abort(stop.signal.reason as unknown);
    rootSignal.addEventListener("abort", rootAbort, { once: true });
    if (signal !== rootSignal) signal.addEventListener("abort", callerAbort, { once: true });
    stop.signal.addEventListener("abort", closeAbort, { once: true });
    if (rootSignal.aborted) rootAbort();
    if (signal.aborted) callerAbort();
    if (stop.signal.aborted) closeAbort();
    // Defer entry until its fixed job slot is installed, including synchronous callbacks.
    const work = Promise.resolve()
      .then(async () => {
        this.#check(combined);
        const value = await body(combined);
        this.#check(combined);
        return value;
      })
      .catch((reason: unknown) => {
        this.#failure ??= { reason };
        throw reason;
      });
    const pending = work.finally(() => {
      rootSignal.removeEventListener("abort", rootAbort);
      if (signal !== rootSignal) signal.removeEventListener("abort", callerAbort);
      stop.signal.removeEventListener("abort", closeAbort);
      if (this.#jobs[at] === pending) this.#jobs[at] = null;
    });
    this.#jobs[at] = pending;
    return pending;
  }
  beginCommand(frameId: number, triggerBudget: number): CommandToken {
    this.#check();
    return this.scope.commands.beginCommand(frameId, triggerBudget);
  }
  async readRowImages(images: RowImageRef, signal: AbortSignal): Promise<RowImageLease> {
    let lease: RowImageLease | null = null;
    try {
      return await this.#run(signal, async (stop) => {
        lease = await this.scope.images.readRowImages(images, stop);
        return lease;
      });
    } catch (error) {
      (lease as RowImageLease | null)?.release();
      throw error;
    }
  }
  async readRowBindings(
    images: RowImageRef,
    projection: RowBindingProjection,
    signal: AbortSignal,
  ): Promise<RowBindingLease> {
    let lease: RowBindingLease | null = null;
    try {
      return await this.#run(signal, async (stop) => {
        lease = await this.scope.images.readRowBindings(images, projection, stop);
        return lease;
      });
    } catch (error) {
      (lease as RowBindingLease | null)?.release();
      throw error;
    }
  }
  async *rowReferentialEntries(
    command: CommandToken,
    row: RowImageRef,
  ): AsyncIterable<ReferentialEntry> {
    this.#check();
    const entries = this.scope.commands.rowReferentialEntries(command, row);
    const iterator = entries[Symbol.asyncIterator]();
    for (;;) {
      const item = await this.#run(this.scope.signal, async () => await iterator.next());
      if (item.done) return;
      yield item.value;
    }
  }
  verifyCommandUnique(command: CommandToken, signal: AbortSignal): Promise<void> {
    return this.#run(signal, async (stop) => {
      const { commands, checks, following } = this.scope;
      await commands.verify(command, "unique", (row, s) => checks.verifyUnique(row, s), stop);
      this.#check(stop);
      await following.prepare(command, stop);
    });
  }
  verifyCommandReferences(command: CommandToken, signal: AbortSignal): Promise<void> {
    return this.#run(signal, async (stop) => {
      const { commands, checks } = this.scope;
      await commands.verify(
        command,
        "references",
        async (row, s) => {
          const table = this.scope.catalog.table(row.tableId) ?? invalid();
          for (const fk of table.foreignKeys) {
            this.#check(s);
            if (fk.enforced) await checks.verifyReference(row, fk.id, s);
          }
        },
        stop,
      );
      this.#check(stop);
      commands.retireCommand(command);
    });
  }
  executeReferentialId(id: number, hooks: NativeRowHooks, signal: AbortSignal): Promise<void> {
    return this.#execute(id, null, hooks, signal);
  }
  executeReferential(
    entry: ReferentialEntry,
    hooks: NativeRowHooks,
    signal: AbortSignal,
  ): Promise<void> {
    return this.#execute(entry.id, entry, hooks, signal);
  }
  #execute(
    id: number,
    supplied: ReferentialEntry | null,
    hooks: NativeRowHooks,
    signal: AbortSignal,
  ): Promise<void> {
    return this.#run(signal, async (stop) => {
      const { commands } = this.scope;
      const entry = await commands.resolveReferential(id);
      this.#check(stop);
      if (supplied && !sameEntry(entry, supplied)) invalid();
      const command = { rootId: entry.images.spoolId, id: entry.commandId, frameId: entry.frameId };
      if (commands.budget(command) !== entry.triggerBudget) invalid();
      // Same-command descendants append to the containing AFTER queue. Only a nested
      // body command may execute another action while this continuation is suspended.
      if (this.#actions.includes(command.id)) invalid();
      const frame = this.#actions.indexOf(0);
      if (frame < 0) invalid();
      this.#actions[frame] = command.id;
      try {
        await this.scope.runReferential(entry, hooks, stop);
        this.#check(stop);
        // Never cache this by token: this action may have added new child work.
        await this.verifyCommandUnique(command, stop);
        this.#check(stop);
        await commands.retireReferential(entry.id);
      } finally {
        this.#actions[frame] = 0;
      }
    });
  }
  /** Root first cancels/fences controller/source/native primary work. This closes the owned
   * image/check/command components; shared directory/fragments and pins close afterward. */
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#closing = Promise.resolve().then(async () => {
      const scope = this.scope,
        reason = new Error("Native command execution closed"),
        errors: unknown[] = [];
      this.#stop?.abort(reason);
      try {
        scope.abortRoot(reason);
      } catch (error) {
        errors.push(error);
      }
      await Promise.allSettled(this.#jobs.filter((job) => job !== null));
      for (const close of [
        () => scope.following.close(),
        () => scope.images.close(),
        () => scope.checks.close(),
        () => scope.commands.close(),
      ]) {
        try {
          await close();
        } catch (error) {
          errors.push(error);
        }
      }
      this.#jobs.length = 0;
      this.#actions.length = 0;
      this.#stop = null;
      this.#failure = null;
      this.#scope = null;
      scope.charge.free(this.#bytes);
      if (errors.length)
        throw new AggregateError(errors, "Native command execution cleanup failed");
    });
    return this.#closing;
  }
}
