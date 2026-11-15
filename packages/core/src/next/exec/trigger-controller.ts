/** Named row-trigger sequencing. This is an injected native-runtime component, not a SQL
 * capability. Root owns admission, registries, source execution and final commit/rollback. */
import type { Statement } from "../access/modify.js";
import type {
  CommandToken,
  NativeCommandBoundary,
  NativeRowHooks,
  ReferentialEntry,
  RowImageRef,
  WriteEvent,
} from "../access/statement-command.js";
import type { TriggerDef } from "../access/trigger-def.js";
import { TRIGGER_SYSTEM_PREFIX } from "../access/trigger-def.js";
import type { SpillService } from "../spill/contract.js";
import { pauseIfDue, sortInSlices } from "../task.js";
import { ExecutionError } from "./errors.js";
import type { SpillIo } from "./spill-pool.js";
import {
  TriggerAfterQueue,
  TRIGGER_AFTER_QUEUE_BYTES,
  TRIGGER_MAX_FRAMES,
  type TriggerAfterEvent,
} from "./trigger-after-queue.js";
import type { TriggerBodyRunner, TriggerBodyScope } from "./trigger-body.js";

export interface PreparedRowTrigger {
  readonly definition: TriggerDef;
  readonly bodies: readonly TriggerBodyRunner[];
}
export interface TriggerControllerOptions {
  readonly statement: Pick<Statement, "catalog" | "readOwn">;
  readonly native: NativeCommandBoundary;
  readonly body: Omit<TriggerBodyScope, "statement" | "native" | "hooks">;
  /** Immutable preparation data in UTF8 name order (ties by ID). Every matching live
   * (budget >= 0) trigger is prepared. Spent-budget definitions need no body program. */
  readonly programs: readonly PreparedRowTrigger[];
  readonly io: SpillIo;
  readonly spill: SpillService;
  /** Static reach supplies maximum LIVE frames, not total body/event count. */
  readonly frameCount: number;
  /** Maximum user+RI entries for ONE accepted row on the reachable routes. */
  readonly rowEvents: number;
}
interface OrderedEntry {
  readonly name: string;
  readonly kind: "trigger" | "referential";
  readonly id: number;
}
interface Frame {
  readonly command: CommandToken;
  readonly budget: number;
  readonly queue: TriggerAfterQueue;
  readonly order: OrderedEntry[];
  ordering: boolean;
  completing: boolean;
  riDepth: number;
}
interface Route {
  readonly table: number;
  readonly event: WriteEvent;
  readonly before: boolean;
  readonly after: boolean;
  readonly prepared: boolean;
}
const FRAME_BYTES = 4096,
  ENTRY_BYTES = 128,
  ROUTE_SLOTS = 8; // Bounded scalar cache, covered by the root's FRAME_BYTES.
function depth(): never {
  throw new ExecutionError("Trigger cascade depth exceeded", "MINNOW_TRIGGER_DEPTH", "54001");
}
function invalid(message: string): never {
  throw new ExecutionError(
    `Invalid trigger controller ${message}`,
    "MINNOW_TRIGGER_STATE",
    "XX000",
  );
}
/** Whole ordinary workspace, excluding the shared root image/binding helper and body producer.
 * Row ordering scratch is charged once per live frame, never per event or queued row. */
export function triggerControllerMinimumBytes(frames: number, rowEvents: number): number {
  if (
    !Number.isSafeInteger(frames) ||
    frames < 1 ||
    frames > TRIGGER_MAX_FRAMES ||
    !Number.isSafeInteger(rowEvents) ||
    rowEvents < 0 ||
    !Number.isSafeInteger(
      frames * (TRIGGER_AFTER_QUEUE_BYTES + FRAME_BYTES + ENTRY_BYTES * rowEvents),
    )
  )
    invalid("workspace bounds");
  return frames * (TRIGGER_AFTER_QUEUE_BYTES + FRAME_BYTES + ENTRY_BYTES * rowEvents);
}
/** PostgreSQL strcmp on UTF8 identifiers; avoids locale and UTF16 surrogate ordering. */
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
export class TriggerController implements NativeRowHooks {
  readonly #cancel = new AbortController();
  readonly #scope: TriggerBodyScope;
  readonly #frames: Frame[] = [];
  readonly #pending = new Set<Promise<void>>();
  readonly #programs: readonly PreparedRowTrigger[];
  readonly #routes: Route[] = [];
  #routeCursor = 0;
  #root: Frame | null = null;
  #nextFrame = 1;
  #closed = false;
  #closing: Promise<void> | null = null;
  #failure: { reason: unknown } | null = null;
  private constructor(readonly options: TriggerControllerOptions) {
    triggerControllerMinimumBytes(options.frameCount, options.rowEvents);
    // Programs are immutable preparation data, not row/event registries. No per-event Map.
    this.#programs = options.programs;
    const signal = AbortSignal.any([options.body.work.signal, this.#cancel.signal]);
    this.#scope = {
      ...options.body,
      work: { ...options.body.work, signal },
      statement: options.statement,
      native: options.native,
      hooks: this,
    };
  }
  static async create(options: TriggerControllerOptions): Promise<TriggerController> {
    const controller = new TriggerController(options);
    try {
      controller.#check();
      let previous: PreparedRowTrigger | undefined;
      for (const program of options.programs) {
        const pause = pauseIfDue(controller.#scope.work);
        if (pause) await pause;
        const def = program.definition;
        if (
          (previous !== undefined &&
            (compareNames(previous.definition.name, def.name) > 0 ||
              (previous.definition.name === def.name && previous.definition.id >= def.id))) ||
          !options.statement.catalog.triggers.includes(def) ||
          program.bodies.length !== def.statements.length ||
          program.bodies.some((b, i) => b.body !== def.statements[i])
        )
          invalid("prepared trigger identity/order");
        previous = program;
      }
      controller.#root = await controller.#newFrame(1);
      controller.#check();
      return controller;
    } catch (error) {
      const cleanup: unknown[] = [];
      try {
        await controller.close();
      } catch (reason) {
        cleanup.push(reason);
      }
      if (cleanup.length)
        throw new AggregateError([error, ...cleanup], "Trigger initialization failed", {
          cause: error,
        });
      throw error;
    }
  }
  get root(): CommandToken {
    this.#check();
    if (!this.#root) invalid("missing root frame");
    return this.#root.command;
  }
  get retainedFrames(): number {
    return this.#frames.length;
  }
  #check(): void {
    if (this.#failure) throw this.#failure.reason;
    this.#scope.work.signal.throwIfAborted();
    if (this.#closed) invalid("closed");
  }
  #track(action: () => Promise<void>): Promise<void> {
    // Native command callers serialize work, except for bounded nested hook continuations.
    if (this.#pending.size >= 2 * this.options.frameCount) invalid("concurrent hook bound");
    const result = Promise.resolve()
      .then(() => {
        this.#check();
        return action();
      })
      .catch((reason: unknown) => {
        this.#failure ??= { reason };
        throw reason;
      });
    const pending = result.finally(() => this.#pending.delete(pending));
    this.#pending.add(pending);
    return pending;
  }
  async #newFrame(budget: number): Promise<Frame> {
    this.#check();
    if (this.#frames.length >= this.options.frameCount || budget < -1 || budget > 1)
      invalid("unadmitted frame depth/budget");
    const bytes = FRAME_BYTES + ENTRY_BYTES * this.options.rowEvents;
    if (!this.options.io.pool.allocate(bytes)) invalid("ordering workspace was not admitted");
    let queue: TriggerAfterQueue | null = null;
    try {
      const frameId = this.#nextFrame++;
      if (!Number.isSafeInteger(frameId)) invalid("exhausted frame IDs");
      // Root origin comes from the native command provider, not a guessed query ID.
      const command = this.options.native.beginCommand(frameId, budget);
      if (
        !Number.isSafeInteger(command.rootId) ||
        command.rootId < 1 ||
        !Number.isSafeInteger(command.id) ||
        command.id < 1 ||
        command.frameId !== frameId ||
        (this.#root && command.rootId !== this.#root.command.rootId)
      )
        invalid("command origin");
      queue = new TriggerAfterQueue(this.options.io, this.options.spill, command);
      const frame: Frame = {
        command,
        budget,
        queue,
        order: [],
        ordering: false,
        completing: false,
        riDepth: 0,
      };
      this.#frames.push(frame);
      return frame;
    } catch (error) {
      const cleanup: unknown[] = [];
      try {
        if (queue) await queue.close();
      } catch (reason) {
        cleanup.push(reason);
      } finally {
        this.options.io.pool.free(bytes);
      }
      if (cleanup.length)
        throw new AggregateError([error, ...cleanup], "Trigger frame initialization failed", {
          cause: error,
        });
      throw error;
    }
  }
  #frame(command: CommandToken, budget?: number): Frame {
    this.#check();
    const frame = this.#frames.find((f) => f.command.frameId === command.frameId);
    if (
      frame?.command.rootId !== command.rootId ||
      frame.command.id !== command.id ||
      (budget !== undefined && frame.budget !== budget)
    )
      invalid("foreign, retired or mismatched command");
    return frame;
  }
  #prepared(def: TriggerDef): PreparedRowTrigger | undefined {
    let lo = 0,
      hi = this.#programs.length;
    while (lo < hi) {
      const mid = Math.floor((lo + hi) / 2),
        p = this.#programs[mid];
      if (!p) invalid("sparse prepared programs");
      const cmp = compareNames(p.definition.name, def.name) || p.definition.id - def.id;
      if (cmp < 0) lo = mid + 1;
      else hi = mid;
    }
    const found = this.#programs[lo];
    return found?.definition === def ? found : undefined;
  }
  /** Guard the actual row route, independently of static reach/preparation. At spent budget
   * only catalog matching is needed: its body is unreachable and must not be prepared. */
  async #route(table: number, event: WriteEvent, budget: number): Promise<Route> {
    const at = this.#routes.findIndex((r) => r.table === table && r.event === event),
      cached = this.#routes[at];
    if (cached && (budget < 0 || cached.prepared)) return cached;
    let before = false,
      after = false,
      seen = 0;
    for (const def of this.options.statement.catalog.triggers) {
      if (def.tableId === table && def.event === event) {
        before ||= def.timing === "before";
        after ||= def.timing === "after";
        if (budget >= 0 && !this.#prepared(def)) invalid("matching trigger was not prepared");
      }
      if (++seen % 128 === 0) {
        const pause = pauseIfDue(this.#scope.work);
        if (pause) await pause;
      }
    }
    this.#check();
    const route: Route = { table, event, before, after, prepared: budget >= 0 };
    if (at >= 0) this.#routes[at] = route;
    else if (this.#routes.length < ROUTE_SLOTS) this.#routes.push(route);
    else this.#routes[this.#routeCursor++ % ROUTE_SLOTS] = route;
    return route;
  }
  async #retire(frame: Frame): Promise<void> {
    try {
      await frame.queue.close();
    } finally {
      frame.order.length = 0;
      const at = this.#frames.indexOf(frame);
      if (at >= 0) {
        this.#frames.splice(at, 1);
        this.options.io.pool.free(FRAME_BYTES + ENTRY_BYTES * this.options.rowEvents);
      }
    }
  }
  beforeRow(
    command: CommandToken,
    table: number,
    event: WriteEvent,
    images: RowImageRef,
    budget: number,
  ): Promise<void> {
    return this.#track(async () => {
      const frame = this.#frame(command, budget);
      if (frame.completing && frame.riDepth === 0) invalid("primary row after command completion");
      const route = await this.#route(table, event, budget);
      if (!route.before) return;
      if (budget < 0) depth();
      // Preparation already sorted names; callbacks allocate no row-count-sized metadata.
      let seen = 0;
      for (const program of this.#programs) {
        if (++seen % 128 === 0) {
          const pause = pauseIfDue(this.#scope.work);
          if (pause) await pause;
        }
        const def = program.definition;
        if (def.tableId !== table || def.event !== event || def.timing !== "before") continue;
        await this.#fire(program, images, budget);
        const pause = pauseIfDue(this.#scope.work);
        if (pause) await pause;
      }
    });
  }
  afterRow(
    command: CommandToken,
    table: number,
    event: WriteEvent,
    images: RowImageRef,
    ri: readonly ReferentialEntry[],
    budget: number,
  ): Promise<void> {
    return this.#track(async () => {
      const frame = this.#frame(command, budget);
      if (frame.completing && frame.riDepth === 0) invalid("primary row after command completion");
      if (frame.ordering) invalid("concurrent accepted row");
      frame.ordering = true;
      try {
        const add = (entry: OrderedEntry) => {
          if (frame.order.length >= this.options.rowEvents) invalid("row ordering capacity");
          frame.order.push(entry);
        };
        const route = await this.#route(table, event, budget);
        const addTrigger = (def: TriggerDef) => {
          if (def.tableId === table && def.event === event && def.timing === "after")
            add({ kind: "trigger", name: def.name, id: def.id });
        };
        if (route.after) {
          let seen = 0;
          if (budget < 0) {
            for (const def of this.options.statement.catalog.triggers) {
              addTrigger(def);
              if (++seen % 128 === 0) {
                const pause = pauseIfDue(this.#scope.work);
                if (pause) await pause;
              }
            }
          } else {
            for (const p of this.#programs) {
              addTrigger(p.definition);
              if (++seen % 128 === 0) {
                const pause = pauseIfDue(this.#scope.work);
                if (pause) await pause;
              }
            }
          }
        }
        for (const entry of ri) {
          if (
            entry.commandId !== command.id ||
            entry.frameId !== command.frameId ||
            entry.triggerBudget !== budget ||
            entry.images.spoolId !== images.spoolId ||
            entry.images.row !== images.row ||
            !entry.sortName.startsWith(TRIGGER_SYSTEM_PREFIX)
          )
            invalid("referential entry origin");
          add({ kind: "referential", name: entry.sortName, id: entry.id });
        }
        await sortInSlices(
          frame.order,
          (a, b) => compareNames(a.name, b.name) || a.id - b.id,
          this.#scope.work,
        );
        for (const entry of frame.order) {
          this.#check();
          await frame.queue.append({
            kind: entry.kind,
            id: entry.id,
            command,
            tableId: table,
            event,
            images,
            budget,
          });
        }
      } finally {
        frame.order.length = 0;
        frame.ordering = false;
      }
    });
  }
  async #fire(program: PreparedRowTrigger, images: RowImageRef, budget: number): Promise<void> {
    this.#check();
    if (budget < 0) depth();
    for (const body of program.bodies) {
      const frame = await this.#newFrame(budget - 1);
      let failure: { reason: unknown } | null = null;
      try {
        await body.run(frame.command, images, this.#scope);
        await this.#complete(frame);
      } catch (reason) {
        failure = { reason };
      }
      let cleanup: { reason: unknown } | null = null;
      try {
        await this.#retire(frame);
      } catch (reason) {
        cleanup = { reason };
      }
      if (cleanup) {
        if (failure)
          throw new AggregateError(
            [failure.reason, cleanup.reason],
            "Trigger frame cleanup failed",
            { cause: failure.reason },
          );
        throw cleanup.reason;
      }
      if (failure) throw failure.reason;
    }
  }
  async #visit(packet: TriggerAfterEvent): Promise<void> {
    const frame = this.#frame(packet.command, packet.budget);
    if (packet.kind === "referential") {
      frame.riDepth++;
      try {
        await this.options.native.executeReferentialId(packet.id, this, this.#scope.work.signal);
      } finally {
        frame.riDepth--;
      }
    } else {
      if (packet.budget < 0) depth();
      const program = this.#programs.find((p) => p.definition.id === packet.id);
      if (
        program?.definition.tableId !== packet.tableId ||
        program.definition.event !== packet.event ||
        program.definition.timing !== "after"
      )
        invalid("queued trigger identity");
      await this.#fire(program, packet.images, packet.budget);
    }
    this.#check();
  }
  async #complete(frame: Frame): Promise<void> {
    if (frame.completing) invalid("duplicate command completion");
    frame.completing = true;
    this.#check();
    await this.options.native.verifyCommandUnique(frame.command, this.#scope.work.signal);
    this.#check();
    await frame.queue.drain((packet) => this.#visit(packet));
    this.#check();
    await this.options.native.verifyCommandReferences(frame.command, this.#scope.work.signal);
    this.#check();
  }
  /** Root Statement.end hook, BEFORE its final checks. Never commits/ends the native root. */
  finishRoot(): Promise<void> {
    return this.#track(async () => {
      if (!this.#root) invalid("missing root completion");
      await this.#complete(this.#root);
    });
  }
  /** External owner cancels/drains callbacks before native rollback or release of shared grants.
   * Do not await this from inside a body callback; the owner awaits the failed callback first. */
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#cancel.abort(
      new ExecutionError("Trigger controller closed", "MINNOW_TRIGGER_CLOSED", "57014"),
    );
    return (this.#closing = (async () => {
      await Promise.allSettled([...this.#pending]);
      this.#routes.length = 0; // Drop cache before retiring its root ordinary charge.
      const results = await Promise.allSettled(this.#frames.map((frame) => this.#retire(frame)));
      this.#root = null;
      const errors = results
        .filter((r): r is PromiseRejectedResult => r.status === "rejected")
        .map((r) => r.reason as unknown);
      if (errors.length) throw new AggregateError(errors, "Trigger controller cleanup failed");
    })());
  }
}
