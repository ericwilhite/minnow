/** Bounded native AFTER FIFO. Existing events precede RI children appended while draining.
 * Root owns image/RI registries; these pages contain scalar references only, never row buffers.
 */
import type { CommandToken, RowImageRef, WriteEvent } from "../access/statement-command.js";
import type { SpillService } from "../spill/contract.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { RecursiveTape } from "./recursive-tape.js";
import { PageWriter, SpillClosedError, SpillFiles } from "./spill-pages.js";
import { SpillIo } from "./spill-pool.js";

const WIDTH = 10,
  ROWS = 64;
const TYPES = Array.from({ length: WIDTH }, () => ({ kind: "float64" }) as const);
const EVENTS = ["insert", "update", "delete"] as const;
const KINDS = ["trigger", "referential", "deferredTrigger"] as const;
/** Additive ordinary workspace in the root's admitted scope, per live frame (root +two nested bodies).
 * Two tape directories/staging blocks, one encoder, one released-read window and packet views.
 * No nested governor admission and no event-count multiplier. Shared spill I/O is reused.
 */
export const TRIGGER_AFTER_QUEUE_BYTES = 256 * 1024;
/** Shipped budgets 1 and 0 both fire; a terminal budget -1 write cannot fire again.
 * Root plus two nested body commands, with RI preserving the current frame/budget. */
export const TRIGGER_MAX_FRAMES = 3;
const FIXED = 8192;
export interface TriggerAfterEvent {
  readonly kind: "trigger" | "referential" | "deferredTrigger";
  readonly command: CommandToken;
  readonly tableId: number;
  readonly event: WriteEvent;
  readonly images: RowImageRef;
  readonly budget: number;
  /** Trigger catalog ID or native referential-registry ID. deferredTrigger retains its
   * original signed budget/images while routing through the containing follow command. */
  readonly id: number;
}
class Epoch {
  readonly store: ColumnStore;
  readonly tape: RecursiveTape;
  #closing: Promise<void> | null = null;
  constructor(readonly io: SpillIo) {
    this.store = new ColumnStore(TYPES, io.pool, {
      chunkRows: ROWS,
      targetBytes: 16384,
      varlenBytes: 64,
    });
    try {
      this.tape = new RecursiveTape(io, 0);
    } catch (error) {
      this.store.close();
      throw error;
    }
  }
  async flush(): Promise<void> {
    if (!this.store.rows) return;
    await this.tape.append(this.store.vectors(), this.store.rows);
    this.io.reset(this.store);
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      const errors: unknown[] = [];
      try {
        this.store.close();
      } catch (error) {
        errors.push(error);
      }
      try {
        await this.tape.close();
      } catch (error) {
        errors.push(error);
      }
      if (errors.length) throw new AggregateError(errors, "AFTER epoch cleanup failed");
    })());
  }
}
export class TriggerAfterQueue {
  readonly #files: SpillFiles;
  readonly #pages: PageWriter;
  readonly #io: SpillIo;
  #input: Float64Array | null = null;
  #columns: readonly FlatVector[] = [];
  #window: Float64Array | null = null;
  #windowRows = 0;
  #write: Epoch | null = null;
  #read: Epoch | null = null;
  #appending: Promise<void> | null = null;
  #draining: Promise<void> | null = null;
  #closing: Promise<void> | null = null;
  #closed = false;
  #failure: { readonly reason: unknown } | null = null;
  #rows = 0;
  constructor(
    readonly parent: SpillIo,
    service: SpillService,
    readonly origin: Pick<CommandToken, "rootId" | "frameId">,
  ) {
    if (
      !Number.isSafeInteger(origin.rootId) ||
      origin.rootId < 1 ||
      !Number.isSafeInteger(origin.frameId) ||
      origin.frameId < 1
    )
      throw new TypeError("Invalid AFTER frame origin");
    if (!parent.pool.allocate(FIXED))
      throw new Error("AFTER queue fixed workspace was not admitted");
    this.#files = new SpillFiles(service, parent.context.queryId, {
      allocate: () => false,
      free: () => {
        throw new Error("Unbuffered AFTER file freed cache");
      },
    });
    let pages: PageWriter | null = null;
    try {
      this.#pages = pages = new PageWriter(parent.pool, 16384);
      this.#io = new SpillIo(
        parent.pool,
        this.#files,
        pages,
        parent.context,
        () => this.#check(),
        parent.pace,
      );
      const input = (this.#input = new Float64Array(WIDTH));
      this.#window = new Float64Array(WIDTH * ROWS);
      this.#columns = TYPES.map((type, i) => ({
        form: "flat",
        type,
        length: 1,
        validity: null,
        data: { layout: "f64", values: input.subarray(i, i + 1) },
      }));
      this.#write = new Epoch(this.#io);
    } catch (error) {
      pages?.close();
      parent.pool.free(FIXED);
      throw error;
    }
  }
  get queued(): number {
    return this.#rows;
  }
  get retainedEpochs(): number {
    return Number(this.#read !== null) + Number(this.#write !== null);
  }
  #check(): void {
    this.parent.checkAborted();
    if (this.#closed) throw new SpillClosedError();
    if (this.#failure) throw this.#failure.reason;
  }
  append(event: TriggerAfterEvent): Promise<void> {
    this.#check();
    if (this.#appending) throw new TypeError("Concurrent AFTER append");
    const work = this.#append(event).catch((reason: unknown) => {
      this.#failure = { reason };
      throw reason;
    });
    const tracked = work.finally(() => {
      if (this.#appending === tracked) this.#appending = null;
    });
    this.#appending = tracked;
    return tracked;
  }
  async #append(e: TriggerAfterEvent): Promise<void> {
    const kind = KINDS.indexOf(e.kind),
      event = EVENTS.indexOf(e.event),
      numbers = [
        kind,
        e.command.rootId,
        e.command.id,
        e.command.frameId,
        e.tableId,
        event,
        e.images.spoolId,
        e.images.row,
        // Nonnegative packet scalar; preserve the spent -1 budget for terminal RI work.
        e.budget + 1,
        e.id,
      ];
    if (
      e.command.rootId !== this.origin.rootId ||
      e.command.frameId !== this.origin.frameId ||
      numbers.some((n) => !Number.isSafeInteger(n) || n < 0) ||
      kind < 0 ||
      event < 0 ||
      e.command.id < 1 ||
      e.tableId < 1 ||
      e.images.spoolId < 1 ||
      e.id < 1 ||
      e.budget > 1
    )
      throw new TypeError("Invalid AFTER event or foreign frame");
    const input = this.#input;
    if (!input) throw new SpillClosedError();
    input.set(numbers);
    const write = this.#write;
    if (!write) throw new Error("Missing AFTER append epoch");
    if (write.store.append(this.#columns, null, 0, 1) !== 1) {
      await write.flush();
      this.#check();
      if (write.store.append(this.#columns, null, 0, 1) !== 1)
        throw new Error("AFTER packet did not fit admitted fixed block");
    }
    this.#rows++;
    if (write.store.rows === ROWS) {
      await write.flush();
      this.#check();
    }
  }
  drain(visit: (event: TriggerAfterEvent) => Promise<void>): Promise<void> {
    this.#check();
    if (this.#appending || this.#draining) throw new TypeError("Concurrent AFTER drain");
    const work = this.#drain(visit).catch((reason: unknown) => {
      this.#failure = { reason };
      throw reason;
    });
    const tracked = work.finally(() => {
      if (this.#draining === tracked) this.#draining = null;
    });
    this.#draining = tracked;
    return tracked;
  }
  #copy(columns: readonly Vector[], rows: number): void {
    if (rows < 1 || rows > ROWS || columns.length !== WIDTH)
      throw new SpillCorruptionError("Invalid AFTER block shape");
    const window = this.#window;
    if (!window) throw new SpillClosedError();
    for (let c = 0; c < WIDTH; c++) {
      const column = columns[c];
      if (column?.form !== "flat" || column.data.layout !== "f64" || column.validity !== null)
        throw new SpillCorruptionError("Invalid AFTER block column");
      window.set(column.data.values.subarray(0, rows), c * ROWS);
    }
    this.#windowRows = rows;
  }
  #event(row: number): TriggerAfterEvent {
    const window = this.#window;
    if (!window) throw new SpillClosedError();
    const numbers = Array.from({ length: WIDTH }, (_, c) => window[c * ROWS + row] ?? NaN);
    if (numbers.some((n) => !Number.isSafeInteger(n) || n < 0))
      throw new SpillCorruptionError("Invalid AFTER scalar");
    const [kind, rootId, id, frameId, tableId, event, spoolId, at, encodedBudget, target] = numbers,
      writeEvent = EVENTS[event ?? -1],
      budget = encodedBudget === undefined ? undefined : encodedBudget - 1;
    if (
      (kind !== 0 && kind !== 1 && kind !== 2) ||
      rootId !== this.origin.rootId ||
      frameId !== this.origin.frameId ||
      !id ||
      !tableId ||
      !spoolId ||
      !target ||
      budget === undefined ||
      budget > 1 ||
      at === undefined ||
      !writeEvent
    )
      throw new SpillCorruptionError("Invalid AFTER event origin or kind");
    return {
      kind: kind === 0 ? "trigger" : kind === 1 ? "referential" : "deferredTrigger",
      command: { rootId, id, frameId },
      tableId,
      event: writeEvent,
      images: { spoolId, row: at },
      budget,
      id: target,
    };
  }
  async #drain(visit: (event: TriggerAfterEvent) => Promise<void>): Promise<void> {
    while (this.#rows) {
      this.#check();
      const read = this.#write;
      if (!read) throw new Error("Missing AFTER epoch");
      this.#read = read;
      this.#write = null; // install ownership before every await
      await read.flush();
      this.#check();
      this.#write = new Epoch(this.#io);
      await read.tape.eachCopied(
        TYPES,
        (columns, rows) => this.#copy(columns, rows),
        async () => {
          // eachCopied releases encoded/decoded read bytes, settling its loan, before callbacks.
          if (this.parent.pool.owes)
            throw new Error("AFTER read progress must retire before continuation");
          for (let row = 0; row < this.#windowRows; row++) {
            this.#check();
            const event = this.#event(row);
            this.#rows--;
            await visit(event);
            this.#check();
          }
          return true;
        },
      );
      await read.close();
      this.#read = null;
    }
  }
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    return (this.#closing = (async () => {
      await Promise.allSettled([this.#appending, this.#draining]);
      const results = await Promise.allSettled([this.#read?.close(), this.#write?.close()]);
      this.#read = null;
      this.#write = null;
      this.#rows = 0;
      const errors = results
        .filter((r): r is PromiseRejectedResult => r.status === "rejected")
        .map((r) => r.reason as unknown);
      try {
        await this.#files.closeAll();
      } catch (error) {
        errors.push(error);
      } finally {
        this.#input = null;
        this.#window = null;
        this.#columns = [];
        this.#pages.close();
        this.parent.pool.free(FIXED);
      }
      if (errors.length) throw new AggregateError(errors, "AFTER queue cleanup failed");
    })());
  }
}
