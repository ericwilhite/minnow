/** Concrete non-ending trigger/body host. Whole raw input and complete OLD rows seal before
 * mutations. Directory packets contain scalar identities; only three frames are resident. */
import type { Snapshot } from "../access/snapshot.js";
import { CardinalityViolationError } from "../access/modify.js";
import type { CommandToken, NativeRowHooks, RowImageRef } from "../access/statement-command.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { BoundParameters, ExecuteRequest } from "../exec/executor.js";
import { PlanStaleError, type ExecutionError } from "../exec/errors.js";
import type { TriggerBodyHost, TriggerBodyInput } from "../exec/trigger-body.js";
import type { TriggerBody } from "../access/trigger-def.js";
import { sourceSlot } from "../write/columns.js";
import type { StatementImpl } from "../write/transaction.js";
import type { SnapshotImpl } from "../write/view.js";
import type { RegistryDirectory } from "./registry-directory.js";
import type { RegistryImages, RegistryImageLink, RegistryImagePatch } from "./registry-images.js";
import type { RegistryCommands } from "./registry-commands.js";
import type { NativePrefixImages } from "./prefix-images.js";
import type { NativeRows } from "./native-rows.js";
import type { RegistryTargets } from "./registry-targets.js";
import { PrepaidQueryGovernor, type PrepaidQueryScope } from "./prepaid-query.js";
import type { NativeBodyPrograms, NativeBodyProgram } from "./body-programs.js";
const INPUT = 12,
  ROW = 13;
interface Request {
  readonly command: CommandToken;
  readonly plan: PhysicalPlan;
  readonly body: TriggerBody | null;
  readonly inputView: Snapshot;
  readonly signal: AbortSignal;
}
interface Frame {
  input: number;
  command: CommandToken | null;
  program: NativeBodyProgram | null;
  view: SnapshotImpl | null;
  phase: "idle" | "source" | "sealed" | "apply" | "applied";
  schemaVersion: number;
  from: number;
  count: number;
  terminal: ExecutionError | CardinalityViolationError | null;
  readonly old: RegistryImageLink[];
  readonly next: RegistryImageLink[];
  readonly patches: RegistryImagePatch[];
}
export interface NativeBodyHostScope {
  readonly rootId: number;
  readonly frames: number;
  readonly programs: NativeBodyPrograms;
  readonly statement: StatementImpl;
  readonly commands: RegistryCommands;
  readonly images: RegistryImages;
  readonly directory: RegistryDirectory;
  readonly prefix: NativePrefixImages;
  readonly rows: NativeRows;
  readonly targets: RegistryTargets;
  readonly charge: ByteCharge;
  readonly query: PrepaidQueryScope;
  readonly execution: Omit<
    ExecuteRequest,
    "snapshot" | "parameters" | "statement" | "signal" | "governor"
  >;
  readonly signal: AbortSignal;
  abortRoot(reason: unknown): void;
  checkpoint(): Promise<void>;
  releaseIdleStep(): void;
}
export function nativeBodyHostBytes(columns: number, frames: number): number {
  if (
    !Number.isSafeInteger(columns) ||
    columns < 0 ||
    !Number.isInteger(frames) ||
    frames < 1 ||
    frames > 3
  )
    throw new TypeError("Invalid native body host sizing");
  const bytes = 8192 + frames * (4096 + 1024 * columns);
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native body host too large");
  return bytes;
}
function invalid(): never {
  throw new TypeError("Invalid, foreign or retired native body input");
}
function integer(n: number, min = 0): number {
  if (!Number.isSafeInteger(n) || n < min) invalid();
  return n;
}
function viewOf(bytes: Uint8Array) {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}
function sourceReference(vector: Vector, row: number): { flat: FlatVector | null; index: number } {
  const slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
  sourceSlot(vector, row, slot);
  return slot;
}
export class NativeBodyHost implements TriggerBodyHost {
  #scope: NativeBodyHostScope | null;
  #record: Uint8Array | null = null;
  #frames: Frame[] = [];
  #jobs: Array<Promise<unknown> | null> = [];
  #producer = false;
  #nextInput = 0;
  #nextRow = 0;
  #closed = false;
  #closing: Promise<void> | null = null;
  readonly #bytes: number;
  constructor(scope: NativeBodyHostScope) {
    scope.signal.throwIfAborted();
    integer(scope.rootId, 1);
    const columns = scope.statement.catalog.tables.reduce(
      (n, t) => Math.max(n, t.columns.length),
      0,
    );
    this.#bytes = nativeBodyHostBytes(columns, scope.frames);
    if (!scope.charge.allocate(this.#bytes))
      throw new RangeError("Native body host must be admitted");
    this.#scope = scope;
    try {
      this.#record = new Uint8Array(128);
      this.#frames = Array.from({ length: scope.frames }, () => ({
        input: 0,
        command: null,
        program: null,
        view: null,
        phase: "idle",
        schemaVersion: 0,
        from: 0,
        count: 0,
        terminal: null,
        old: [],
        next: [],
        patches: [],
      }));
      this.#jobs = Array.from({ length: scope.frames }, () => null);
    } catch (error) {
      scope.charge.free(this.#bytes);
      this.#scope = null;
      throw error;
    }
  }
  private get scope() {
    return this.#scope ?? invalid();
  }
  #check(signal?: AbortSignal) {
    if (this.#closed) invalid();
    this.scope.signal.throwIfAborted();
    signal?.throwIfAborted();
  }
  #frame(command: CommandToken, claim = false): Frame {
    this.#check();
    this.scope.commands.budget(command);
    if (command.rootId !== this.scope.rootId) invalid();
    const existing = this.#frames.find((frame) => frame.command === command);
    if (existing) return existing;
    if (!claim) invalid();
    const at = this.#frames.findIndex(
      (frame, index) => frame.phase === "idle" && frame.command === null && !this.#jobs[index],
    );
    const frame = this.#frames[at] ?? invalid();
    frame.command = command;
    return frame;
  }
  #work<T>(command: CommandToken, body: () => Promise<T>, guard = true): Promise<T> {
    let at = this.#frames.findIndex((frame) => frame.command === command);
    if (at < 0)
      at = this.#frames.findIndex((frame, index) => frame.phase === "idle" && !this.#jobs[index]);
    if (at < 0 || this.#jobs[at]) invalid();
    const work = guard ? Promise.resolve(this.scope.statement.guard(body)) : body();
    const pending = work.finally(() => {
      if (this.#jobs[at] === pending) this.#jobs[at] = null;
    });
    this.#jobs[at] = pending;
    return pending;
  }
  #identity(request: Request, frame: Frame) {
    this.#check(request.signal);
    this.scope.commands.budget(request.command);
    if (
      frame.command !== request.command ||
      frame.program?.plan !== request.plan ||
      frame.program.body !== request.body ||
      frame.view !== request.inputView
    )
      invalid();
    if (this.scope.statement.catalog.version !== request.plan.catalogVersion)
      throw new PlanStaleError(request.plan.catalogVersion, this.scope.statement.catalog.version);
    frame.view.check();
  }
  #rowId(frame: Frame, vector: Vector, row: number): number {
    const p = frame.program ?? invalid(),
      pin = frame.view ?? invalid(),
      root = p.plan.root;
    if (
      root.op !== "modify" ||
      vector.type.kind !== "locator" ||
      vector.type.tableId !== root.tableId
    )
      invalid();
    pin.checkRead();
    const slot = sourceReference(vector, row);
    if (!slot.flat || slot.index < 0 || slot.flat.data.layout !== "f64") invalid();
    const locator = slot.flat.data.values[slot.index];
    if (locator === undefined) invalid();
    integer(locator);
    return integer(pin.internalTable(root.tableId).rowId(locator));
  }
  async #target(frame: Frame, vector: Vector, row: number): Promise<boolean> {
    const root = frame.program?.plan.root ?? invalid(),
      command = frame.command ?? invalid(),
      pin = frame.view ?? invalid();
    if (root.op !== "modify") invalid();
    const rowId = this.#rowId(frame, vector, row);
    const repeated = await this.scope.commands.testAndSetTarget(
      command,
      root.tableId,
      rowId,
      pin,
      this.scope.signal,
    );
    pin.checkRead();
    if (repeated) {
      if (root.onRepeat === "error") throw new CardinalityViolationError(root.tableId, row);
      return false;
    }
    return true;
  }
  async #putRow(frame: Frame, raw: RowImageRef, rowId: number | null): Promise<void> {
    const p = frame.program ?? invalid(),
      root = p.plan.root,
      command = frame.command ?? invalid(),
      record = this.#record ?? invalid();
    if (root.op !== "modify" || raw.spoolId !== this.scope.rootId) invalid();
    const id = integer(this.#nextRow + 1, 1),
      view = viewOf(record);
    record.fill(0);
    view.setFloat64(0, frame.input, true);
    view.setFloat64(8, command.id, true);
    view.setFloat64(16, frame.count, true);
    view.setFloat64(24, root.tableId, true);
    view.setFloat64(32, rowId ?? -1, true);
    view.setFloat64(40, raw.row, true);
    await this.scope.directory.put(ROW, id, record);
    this.#check();
    this.#nextRow = id;
    frame.count++;
  }
  async #readRow(
    frame: Frame,
    offset: number,
  ): Promise<{ id: number | null; raw: RowImageRef; images: RowImageRef | null }> {
    const record = this.#record ?? invalid();
    integer(offset);
    if (offset >= frame.count) invalid();
    if (!(await this.scope.directory.get(ROW, frame.from + offset, record))) invalid();
    this.#check();
    const v = viewOf(record),
      root = frame.program?.plan.root ?? invalid();
    if (
      root.op !== "modify" ||
      v.getFloat64(0, true) !== frame.input ||
      v.getFloat64(8, true) !== frame.command?.id ||
      v.getFloat64(16, true) !== offset ||
      v.getFloat64(24, true) !== root.tableId
    )
      invalid();
    const id = v.getFloat64(32, true),
      raw = integer(v.getFloat64(40, true), 1),
      images = v.getFloat64(48, true);
    if (id !== -1) integer(id);
    if (images !== 0) integer(images, 1);
    return {
      id: id === -1 ? null : id,
      raw: { spoolId: this.scope.rootId, row: raw },
      images: images === 0 ? null : { spoolId: this.scope.rootId, row: images },
    };
  }
  async #completeRow(frame: Frame, offset: number, signal: AbortSignal): Promise<void> {
    const p = frame.program ?? invalid(),
      root = p.plan.root;
    if (root.op !== "modify") invalid();
    const table = this.scope.statement.catalog.table(root.tableId) ?? invalid(),
      row = await this.#readRow(frame, offset);
    const link = (columnId: number): RegistryImageLink => ({
      image: row.raw,
      side: "new",
      columnId,
    });
    let images: RowImageRef;
    frame.old.length = frame.next.length = frame.patches.length = 0;
    try {
      if (root.kind === "insert") {
        for (let i = 0; i < table.columns.length; i++) frame.next.push(link(i + 1));
        images = await this.scope.images.compose(table.id, null, frame.next, signal);
      } else {
        if (row.id === null) invalid();
        let old: RowImageRef;
        if (root.oldRow) {
          const start = 1 + (root.kind === "update" ? root.assigned.length : 0);
          for (let i = 0; i < table.columns.length; i++) frame.old.push(link(start + i + 1));
          old = await this.scope.images.compose(table.id, frame.old, null, signal);
        } else old = await this.scope.prefix.capture(table.id, row.id, signal);
        if (root.kind === "delete") images = old;
        else {
          for (const [i, columnId] of root.assigned.entries())
            frame.patches.push({ columnId, value: link(i + 2) });
          images = await this.scope.images.derive(old, "old", frame.patches, signal);
        }
      }
      this.#check(signal);
      frame.view?.checkRead();
      // Refresh the packet: composing/point capture reuses shared directory helpers.
      await this.#readRow(frame, offset);
      const record = this.#record ?? invalid();
      viewOf(record).setFloat64(48, images.row, true);
      await this.scope.directory.put(ROW, frame.from + offset, record);
      this.#check(signal);
      frame.view?.checkRead();
    } finally {
      frame.old.length = frame.next.length = frame.patches.length = 0;
    }
  }
  async #header(frame: Frame, retired = false): Promise<void> {
    const p = frame.program ?? invalid(),
      command = frame.command ?? invalid(),
      record = this.#record ?? invalid(),
      root = p.plan.root;
    if (root.op !== "modify") invalid();
    record.fill(0);
    const v = viewOf(record);
    v.setFloat64(0, this.scope.rootId, true);
    v.setFloat64(8, command.id, true);
    v.setFloat64(16, command.frameId, true);
    v.setFloat64(24, p.schema.id, true);
    v.setFloat64(32, frame.from, true);
    v.setFloat64(40, frame.count, true);
    v.setFloat64(48, p.plan.catalogVersion, true);
    v.setFloat64(56, frame.schemaVersion, true);
    v.setUint8(64, retired ? 2 : 1);
    await this.scope.directory.put(INPUT, frame.input, record);
    this.#check();
  }
  spoolBodyInput(
    request: Parameters<TriggerBodyHost["spoolBodyInput"]>[0],
  ): Promise<TriggerBodyInput> {
    return this.#spool(request);
  }
  spoolRootInput(
    request: Omit<Parameters<TriggerBodyHost["spoolBodyInput"]>[0], "body">,
  ): Promise<TriggerBodyInput> {
    return this.#spool({ ...request, body: null });
  }
  #spool(request: Request & { parameters: BoundParameters }): Promise<TriggerBodyInput> {
    const frame = this.#frame(request.command, true);
    if (frame.phase !== "idle" || this.#producer) invalid();
    const program = this.scope.programs.get(request.plan, request.body),
      pin = this.scope.statement.writerView(request.inputView);
    pin.checkRead();
    frame.input = integer(this.#nextInput + 1, 1);
    this.#nextInput = frame.input;
    frame.command = request.command;
    frame.program = program;
    frame.view = pin;
    frame.from = this.#nextRow + 1;
    frame.count = 0;
    frame.phase = "source";
    const table =
      this.scope.statement.catalog.table(
        request.plan.root.op === "modify" ? request.plan.root.tableId : invalid(),
      ) ?? invalid();
    frame.schemaVersion = table.schemaVersion;
    return this.#work(request.command, async () => {
      this.#producer = true;
      let query: PrepaidQueryGovernor | null = null;
      try {
        this.#identity(request, frame);
        query = new PrepaidQueryGovernor(this.scope.query);
        const root = request.plan.root;
        if (root.op !== "modify") invalid();
        const preTargets =
          root.kind === "update" && root.onRepeat === "error" && !root.uniqueTargets;
        frame.terminal = await program.input.run(
          {
            inputView: pin,
            parameters: request.parameters,
            signal: request.signal,
            metadata: this.scope.charge,
            execution: { ...this.scope.execution, governor: query },
            ...(preTargets
              ? {
                  targets: {
                    charge: this.scope.charge,
                    visit: (v: Vector, row: number) => this.#target(frame, v, row),
                  },
                }
              : {}),
          },
          (capture) => {
            let pending: Promise<void> | null = null;
            return {
              push: (batch, transfer) => {
                pending = (async () => {
                  try {
                    for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
                      this.#identity(request, frame);
                      pin.checkRead();
                      const row = batch.selection?.rows[k] ?? k,
                        locator = batch.columns[0];
                      let rowId: number | null = null;
                      if (root.kind !== "insert") {
                        if (!locator) invalid();
                        if (
                          !preTargets &&
                          !root.uniqueTargets &&
                          !(await this.#target(frame, locator, row))
                        )
                          continue;
                        rowId = this.#rowId(frame, locator, row);
                      }
                      const image = await this.scope.images.append(
                        program.schema.id,
                        null,
                        { batch, row },
                        capture,
                      );
                      this.#identity(request, frame);
                      pin.checkRead();
                      await this.#putRow(frame, image, rowId);
                      pin.checkRead();
                    }
                  } finally {
                    transfer?.release();
                  }
                })();
                return "blocked";
              },
              ready: () => pending ?? Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            };
          },
        );
        await query.close();
        query = null;
        this.scope.releaseIdleStep();
        this.#identity(request, frame);
        pin.checkRead();
        for (let offset = 0; offset < frame.count; offset++) {
          await this.scope.checkpoint();
          this.#identity(request, frame);
          pin.checkRead();
          await this.#completeRow(frame, offset, request.signal);
        }
        this.scope.releaseIdleStep();
        pin.checkRead();
        await this.#header(frame);
        pin.checkRead();
        frame.phase = "sealed";
        return { rootId: this.scope.rootId, id: frame.input };
      } finally {
        await query?.close();
        this.#producer = false;
      }
    });
  }
  applyBodyInput(request: Parameters<TriggerBodyHost["applyBodyInput"]>[0]): Promise<void> {
    return this.#apply(request);
  }
  async applyRootInput(
    request: Omit<Parameters<TriggerBodyHost["applyBodyInput"]>[0], "body">,
  ): Promise<number> {
    await this.#apply({ ...request, body: null });
    return this.#frame(request.command).count;
  }
  #apply(request: Request & { input: TriggerBodyInput; hooks: NativeRowHooks }): Promise<void> {
    const frame = this.#frame(request.command);
    this.#identity(request, frame);
    if (
      frame.phase !== "sealed" ||
      request.input.rootId !== this.scope.rootId ||
      request.input.id !== frame.input
    )
      invalid();
    frame.phase = "apply";
    return this.#work(request.command, async () => {
      const root = request.plan.root;
      if (root.op !== "modify" || !(["insert", "update", "delete"] as string[]).includes(root.kind))
        invalid();
      for (let offset = 0; offset < frame.count; offset++) {
        await this.scope.checkpoint();
        this.#identity(request, frame);
        const row = await this.#readRow(frame, offset);
        if (!row.images) invalid();
        if (row.id !== null) {
          const current = await this.scope.targets.resolve(root.tableId, row.id);
          if (current?.rowId !== row.id)
            throw new Error("A trigger changed a sealed modification target");
        }
        this.scope.releaseIdleStep();
        await this.scope.rows.write(
          request.command,
          root.tableId,
          root.kind as "insert" | "update" | "delete",
          row.images,
          row.id,
          request.hooks,
          request.signal,
        );
      }
      this.#identity(request, frame);
      if (frame.terminal) throw frame.terminal;
      frame.phase = "applied";
    });
  }
  releaseBodyInput(command: CommandToken, input: TriggerBodyInput): Promise<void> {
    this.#check();
    if (command.rootId !== this.scope.rootId || input.rootId !== this.scope.rootId) invalid();
    return this.#work(
      command,
      async () => {
        const record = this.#record ?? invalid();
        if (!(await this.scope.directory.get(INPUT, input.id, record))) invalid();
        this.#check();
        const view = viewOf(record);
        if (
          view.getFloat64(0, true) !== input.rootId ||
          view.getFloat64(8, true) !== command.id ||
          view.getFloat64(16, true) !== command.frameId
        )
          invalid();
        if (view.getUint8(64) === 2) return;
        const frame =
          this.#frames.find((frame) => frame.command === command && frame.input === input.id) ??
          invalid();
        if (frame.input !== input.id || frame.command !== command) invalid();
        await this.#header(frame, true);
        this.#reset(frame);
      },
      false,
    );
  }
  #reset(frame: Frame) {
    frame.input = 0;
    frame.schemaVersion = 0;
    frame.command = null;
    frame.program = null;
    frame.view = null;
    frame.phase = "idle";
    frame.count = frame.from = 0;
    frame.terminal = null;
    frame.old.length = frame.next.length = frame.patches.length = 0;
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      const scope = this.#scope;
      if (!scope) return;
      scope.abortRoot(new Error("Native body host closed"));
      await Promise.all(
        this.#jobs
          .filter((job): job is Promise<unknown> => job !== null)
          .map((job) => job.catch(() => undefined)),
      );
      for (const frame of this.#frames) this.#reset(frame);
      this.#frames = [];
      this.#jobs = [];
      this.#record = null;
      this.#scope = null;
      scope.charge.free(this.#bytes);
    })());
  }
}
