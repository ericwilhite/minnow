import { copyDate } from "../date-value.js";
import type { BatchValue } from "./batch.js";
import type { InsertBatchResult, MinnowDatabase, UpsertBatchResult } from "./database.js";

export interface BufferedWriterOptions {
  mode?: "insert" | "upsert";
  maxRows?: number;
  maxBytes?: number;
  maxAgeMs?: number;
  onError?: (error: unknown) => void;
}

export type BufferedFlushResult = InsertBatchResult | UpsertBatchResult;

export interface LifecycleFlushRequester {
  requestFlush(): void;
}

export interface LifecycleDocumentTarget {
  readonly visibilityState: string;
  addEventListener(type: "visibilitychange", listener: () => void): void;
  removeEventListener(type: "visibilitychange", listener: () => void): void;
}

export interface LifecyclePageTarget {
  addEventListener(type: "pagehide", listener: () => void): void;
  removeEventListener(type: "pagehide", listener: () => void): void;
}

export interface LifecycleFlushOptions {
  document?: LifecycleDocumentTarget;
  page?: LifecyclePageTarget;
}

/** Maximum accepted `add()` calls that have not completed. Callers must await for backpressure. */
export const MAX_BUFFERED_WRITER_PENDING_ADDS = 64;

/** Batches row-oriented writes by row count, estimated bytes, or age. */
export class BufferedTableWriter {
  readonly #mode: "insert" | "upsert";
  readonly #maxRows: number;
  readonly #maxBytes: number;
  readonly #maxAgeMs: number;
  readonly #onError: ((error: unknown) => void) | undefined;
  readonly #rows: Array<Readonly<Record<string, BatchValue>>> = [];
  #estimatedBytes = 0;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #inFlight: Promise<BufferedFlushResult> | undefined;
  #addTail: Promise<void> = Promise.resolve();
  #pendingAddCount = 0;
  #acceptingAdds = true;
  #closed = false;

  constructor(
    private readonly database: MinnowDatabase,
    private readonly tableName: string,
    options: BufferedWriterOptions = {},
  ) {
    this.#mode = options.mode ?? "insert";
    this.#maxRows = positiveWholeNumber(options.maxRows ?? 1_000, "Buffered row limit");
    this.#maxBytes = positiveWholeNumber(options.maxBytes ?? 1024 * 1024, "Buffered byte limit");
    this.#maxAgeMs = positiveWholeNumber(options.maxAgeMs ?? 1_000, "Buffered age limit");
    this.#onError = options.onError;
  }

  get pendingRowCount(): number {
    return this.#rows.length;
  }

  get estimatedBytes(): number {
    return this.#estimatedBytes;
  }

  async add(row: Readonly<Record<string, BatchValue>>): Promise<BufferedFlushResult | undefined> {
    this.#assertOpen();
    if (!this.#acceptingAdds) return Promise.reject(new Error("Buffered writer is closing"));
    if (this.#pendingAddCount >= MAX_BUFFERED_WRITER_PENDING_ADDS) {
      return Promise.reject(
        new RangeError(
          `Buffered writer cannot queue more than ${String(MAX_BUFFERED_WRITER_PENDING_ADDS)} adds; await add() for backpressure`,
        ),
      );
    }
    this.#pendingAddCount += 1;
    const operation = this.#addTail.then(() => this.#addSerial(row));
    this.#addTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation.finally(() => {
      this.#pendingAddCount -= 1;
    });
  }

  async flush(): Promise<BufferedFlushResult | undefined> {
    if (this.#inFlight !== undefined) return this.#inFlight;
    if (this.#rows.length === 0) return undefined;
    this.#clearTimer();
    const rows = this.#rows.splice(0);
    this.#estimatedBytes = 0;
    const operation =
      this.#mode === "upsert"
        ? this.database.upsertBatch(this.tableName, rows)
        : this.database.insertBatch(this.tableName, rows);
    this.#inFlight = operation;
    try {
      return await operation;
    } catch (error) {
      this.#rows.unshift(...rows);
      this.#estimatedBytes = this.#rows.reduce((total, row) => total + estimateRowBytes(row), 0);
      throw error;
    } finally {
      this.#inFlight = undefined;
    }
  }

  requestFlush(): void {
    if (this.#closed) return;
    void this.#flushPending().catch((error: unknown) => this.#onError?.(error));
  }

  async close(): Promise<BufferedFlushResult | undefined> {
    if (this.#closed) return undefined;
    this.#acceptingAdds = false;
    await this.#addTail;
    let result: BufferedFlushResult | undefined;
    while (this.#inFlight !== undefined || this.#rows.length > 0) {
      result = await this.flush();
      if (this.#inFlight !== undefined) await this.#inFlight;
    }
    this.#closed = true;
    this.#clearTimer();
    return result;
  }

  discard(): number {
    this.#assertOpen();
    if (this.#pendingAddCount > 0) {
      throw new Error("Cannot discard while buffered adds are pending");
    }
    const discarded = this.#rows.length;
    this.#rows.length = 0;
    this.#estimatedBytes = 0;
    this.#clearTimer();
    return discarded;
  }

  async #flushPending(): Promise<void> {
    while (this.#inFlight !== undefined || this.#rows.length > 0) {
      if (this.#inFlight !== undefined) await this.#inFlight;
      else await this.flush();
    }
  }

  async #addSerial(
    row: Readonly<Record<string, BatchValue>>,
  ): Promise<BufferedFlushResult | undefined> {
    const copy = cloneRow(row);
    this.#rows.push(copy);
    this.#estimatedBytes += estimateRowBytes(copy);
    this.#scheduleAgeFlush();
    if (this.#rows.length < this.#maxRows && this.#estimatedBytes < this.#maxBytes) {
      return undefined;
    }
    if (this.#inFlight !== undefined) await this.#inFlight;
    return this.flush();
  }

  #scheduleAgeFlush(): void {
    if (this.#timer !== undefined || this.#rows.length === 0 || this.#closed) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.#flushPending().catch((error: unknown) => this.#onError?.(error));
    }, this.#maxAgeMs);
  }

  #clearTimer(): void {
    if (this.#timer === undefined) return;
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("Buffered writer is closed");
  }
}

/** Requests a non-blocking flush when the browser hides or unloads the page. */
export function attachLifecycleFlush(
  requester: LifecycleFlushRequester,
  options: LifecycleFlushOptions = {},
): () => void {
  const documentTarget =
    options.document ?? (typeof document === "undefined" ? undefined : document);
  const pageTarget = options.page ?? (typeof window === "undefined" ? undefined : window);
  const onVisibilityChange = (): void => {
    if (documentTarget?.visibilityState === "hidden") requester.requestFlush();
  };
  const onPageHide = (): void => requester.requestFlush();
  documentTarget?.addEventListener("visibilitychange", onVisibilityChange);
  pageTarget?.addEventListener("pagehide", onPageHide);
  return () => {
    documentTarget?.removeEventListener("visibilitychange", onVisibilityChange);
    pageTarget?.removeEventListener("pagehide", onPageHide);
  };
}

function positiveWholeNumber(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive whole number`);
  }
  return value;
}

function cloneRow(row: Readonly<Record<string, BatchValue>>): Readonly<Record<string, BatchValue>> {
  return Object.fromEntries(
    Object.entries(row).map(([name, value]) => [
      name,
      value instanceof Date ? copyDate(value) : value,
    ]),
  );
}

function estimateRowBytes(row: Readonly<Record<string, BatchValue>>): number {
  let bytes = 0;
  for (const value of Object.values(row)) {
    if (typeof value === "string") bytes += 4 + value.length;
    else if (typeof value === "number" || value instanceof Date) bytes += 8;
    else bytes += 1;
  }
  return bytes;
}
