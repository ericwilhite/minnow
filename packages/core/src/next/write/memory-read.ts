/** Publication-stable memory reads: size logical rows, then fill only caller-owned output. */
import type { ColumnDef } from "../access/catalog.js";
import type { MaybePromise } from "../access/key-index.js";
import {
  MEMORY_CHUNK_ROWS,
  type ChunkOutput,
  type ChunkResult,
  type MemoryChunkStep,
  type MemoryReadContext,
  type MemoryReadHint,
  type MemoryReadRequest,
  type MemoryOutputNeed,
} from "../access/snapshot.js";
import type { SpaceReader } from "../access/space.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { MetadataReader } from "../format/metadata-reader.js";
import { parseCharge } from "../format/manifest-codec.js";
import { PageEncoding, type BlobRef } from "../format/manifest.js";
import {
  ChunkReader,
  PageBudgetError,
  MAX_BLOB_BYTES,
  MAX_READ_BYTES,
  PAGE_DESCRIPTOR_BYTES,
} from "../page/reader.js";
import { scratchBytes as predicateScratchBytes } from "../page/select.js";
import { GZIP_MAX_PAGE_BYTES } from "../page/gzip.js";
import { allocateData, type FlatVector, type VectorData } from "../vector/vector.js";
import type { PhysicalType } from "../types.js";
import { layoutOf } from "../types.js";
import {
  acceptsMemoryRow,
  copyMemoryRow,
  memoryNeed,
  memoryOutputBytes,
  memoryOutputLayout,
  memoryRowLayout,
  memoryValueBytes,
} from "./memory-output.js";

export interface MemoryReadSource {
  readonly rowCount: number;
  column(columnId: number): ColumnDef | undefined;
  /** Only used synchronously: never retain the view across a suspension or in a hint. */
  resident(columnId: number, chunk: number): FlatVector | null;
  publishedRoot(): BlobRef;
  /** Explicit source recipe, independently owned by the query, keeps this chunk resident. */
  retained?(chunk: number): boolean;
  check(): void;
  /** Snapshot release cancels work; unregister only after pending reads have drained. */
  watch(close: () => void): () => void;
}

// One root, one column directory, one page and dictionary, and one streamed value fragment.
// A directory blob's smallest entry is 26 bytes (page/reader.ts directoryChargeOf). The flush
// writer does not gzip memtables (engine.ts #writeGroup); a pinned publication never changes
// its codec. An already-published gzip group is sized with the larger format maximum below.
const METADATA =
  parseCharge(MAX_BLOB_BYTES) + 256 + (Math.floor(MAX_BLOB_BYTES / 26) + 1) * 192 + 128;
export const MEMORY_READ_RESIDENT_BYTES = METADATA + 3 * MAX_BLOB_BYTES + 2 * PAGE_DESCRIPTOR_BYTES;
// Conservative parse-alignment copies of a page AND dictionary, predicate scratch, scalar
// decode/object/BigInt temporaries. The alignment allowance is charged before reader.load.
const WORK = 2 * MAX_BLOB_BYTES + 512 * 1024;
// The reader charges these separately: one scaled-float page lane and its predicate scratch.
const DECODER_SCRATCH = 65536 * 8 + predicateScratchBytes(4096);

function run<T>(work: Generator<Promise<unknown>, T, unknown>): MaybePromise<T> {
  const advance = (value?: unknown): MaybePromise<T> => {
    const step = work.next(value);
    return step.done ? step.value : step.value.then(advance, fail);
  };
  const fail = (error: unknown): MaybePromise<T> => {
    const step = work.throw(error);
    return step.done ? step.value : step.value.then(advance, fail);
  };
  return advance();
}
function* wait<T>(value: MaybePromise<T>): Generator<Promise<unknown>, T, unknown> {
  return value instanceof Promise ? ((yield value) as T) : value;
}
function* pause(check: () => void): Generator<Promise<unknown>, void, unknown> {
  check();
  if (taskClock.due()) {
    yield yieldTask();
    check();
  }
}

/** Segment-owned token validation holds descriptors only, never producer payloads. */
export class MemoryReads {
  #hints = new WeakSet<MemoryReadHint>();
  constructor(readonly source: MemoryReadSource) {}
  /** Ending the explicit lease invalidates its lower-cost hints. */
  invalidateHints(): void {
    this.#hints = new WeakSet();
  }

  hint(request: MemoryReadRequest, context: MemoryReadContext): MaybePromise<MemoryReadHint> {
    this.#validate(request);
    return this.#during(context, (ctx, check) => this.#hint(request, ctx, check));
  }
  chunk(
    columnId: number,
    chunk: number,
    out: ChunkOutput,
    start: number,
    step: MemoryChunkStep,
  ): MaybePromise<ChunkResult> {
    this.source.check();
    const hint = step.hint,
      request = hint.request;
    if (
      !this.#hints.has(hint) ||
      !Object.isFrozen(hint.token) ||
      request.chunk !== chunk ||
      request.startRow !== start ||
      !request.columns.includes(columnId)
    )
      throw new TypeError("Memory read hint does not name this segment, column and interval");
    return this.#during(step.context, (ctx, check) => this.#chunk(columnId, out, hint, ctx, check));
  }

  #validate(r: MemoryReadRequest): void {
    this.source.check();
    const end = Math.min(MEMORY_CHUNK_ROWS, this.source.rowCount - r.chunk * MEMORY_CHUNK_ROWS);
    if (
      !Number.isSafeInteger(r.chunk) ||
      r.chunk < 0 ||
      !(end > 0) ||
      !Number.isInteger(r.startRow) ||
      r.startRow < 0 ||
      r.startRow >= end ||
      !Number.isInteger(r.maxRows) ||
      r.maxRows < 1 ||
      r.maxRows > MEMORY_CHUNK_ROWS ||
      !Number.isSafeInteger(r.targetBytes) ||
      r.targetBytes <= 0
    )
      throw new RangeError("Invalid memory read interval or byte target");
  }

  #during<T>(
    context: MemoryReadContext,
    work: (ctx: MemoryReadContext, check: () => void) => Generator<Promise<unknown>, T, unknown>,
  ): MaybePromise<T> {
    context.signal.throwIfAborted();
    if (!context.budget.allocate(1024)) throw new PageBudgetError(1024);
    const stop = new AbortController();
    const signal = AbortSignal.any([context.signal, stop.signal]);
    const check = (): void => {
      this.source.check();
      signal.throwIfAborted();
    };
    let unwatch: () => void;
    try {
      unwatch = this.source.watch(() => stop.abort());
    } catch (error) {
      context.budget.free(1024);
      throw error;
    }
    // Every new read is routed through the caller. Refuse late leases before decoder install.
    const pages: SpaceReader = {
      peek: (ref, options) => {
        check();
        return context.pages.peek(ref, options);
      },
      read: async (refs, options) => {
        check();
        const lease = await context.pages.read(refs, {
          ...options,
          budget: context.budget,
          signal,
        });
        try {
          check();
          return lease;
        } catch (error) {
          lease.release();
          throw error;
        }
      },
    };
    const execute = function* (): Generator<Promise<unknown>, T, unknown> {
      try {
        check();
        return yield* work({ budget: context.budget, pages, signal }, check);
      } finally {
        unwatch();
        context.budget.free(1024);
      }
    };
    return run(execute());
  }

  *#withReader<T>(
    columnId: number,
    context: MemoryReadContext,
    check: () => void,
    use: (reader: ChunkReader) => Generator<Promise<unknown>, T, unknown>,
  ): Generator<Promise<unknown>, T, unknown> {
    // Do not call a cold LazyGroupRoot.get(): its callback uses the writer's pages and budget.
    const root = yield* wait(
      new MetadataReader(context.pages).groupRoot(this.source.publishedRoot(), context.budget),
    );
    try {
      check();
      const stored = root.value.columns.find((c) => c.columnId === columnId);
      if (stored === undefined)
        throw new RangeError("The published memory group lacks this column");
      const reader = yield* wait(
        ChunkReader.open(context.pages, stored, { budget: context.budget, signal: context.signal }),
      );
      try {
        check();
        return yield* use(reader);
      } finally {
        reader.close();
      }
    } finally {
      root.release();
    }
  }

  *#lengths(
    columnId: number,
    request: MemoryReadRequest,
    rows: number,
    context: MemoryReadContext,
    check: () => void,
    each: (row: number, bytes: number) => void,
  ): Generator<Promise<unknown>, number, unknown> {
    const resident = this.#residentLengths(columnId, request, rows, each);
    if (resident) return 0;
    const from = request.chunk * MEMORY_CHUNK_ROWS + request.startRow;
    return yield* this.#withReader(columnId, context, check, function* (reader) {
      let gzip = 0;
      const dictionary = reader.directory.dictionary;
      if (dictionary !== null && (dictionary.encoding & PageEncoding.gzip) !== 0)
        gzip += GZIP_MAX_PAGE_BYTES - MAX_BLOB_BYTES;
      const [first, end] = reader.pagesInRange(from, from + rows);
      for (let page = first; page < end; page++) {
        yield* pause(check);
        yield* wait(reader.load([page]));
        check();
        const entry = reader.directory.pages[page];
        if (entry === undefined) throw new RangeError("Missing memory page");
        if ((entry.encoding & PageEncoding.gzip) !== 0)
          gzip = Math.max(
            gzip,
            GZIP_MAX_PAGE_BYTES -
              MAX_BLOB_BYTES +
              (dictionary !== null && (dictionary.encoding & PageEncoding.gzip) !== 0
                ? GZIP_MAX_PAGE_BYTES - MAX_BLOB_BYTES
                : 0),
          );
        for (
          let row = Math.max(from, entry.firstRow);
          row < Math.min(from + rows, entry.firstRow + entry.rowCount);
          row++
        )
          each(row - from, reader.varlenBytes(row, row + 1));
        reader.release([page]);
      }
      return gzip;
    });
  }
  #residentLengths(
    columnId: number,
    r: MemoryReadRequest,
    rows: number,
    each: (row: number, bytes: number) => void,
  ): boolean {
    const vector = this.source.resident(columnId, r.chunk);
    if (vector === null) return false;
    for (let row = 0; row < rows; row++) each(row, memoryValueBytes(vector, r.startRow + row));
    return true;
  }

  *#hint(
    input: MemoryReadRequest,
    context: MemoryReadContext,
    check: () => void,
  ): Generator<Promise<unknown>, MemoryReadHint, unknown> {
    const rows = Math.min(
      input.maxRows,
      this.source.rowCount - input.chunk * MEMORY_CHUNK_ROWS - input.startRow,
      MEMORY_CHUNK_ROWS - input.startRow,
    );
    const retained = this.source.retained?.(input.chunk) === true;
    const charge = (retained ? 4096 : WORK) + rows * 8 + 256 + input.columns.length * 256;
    if (!context.budget.allocate(charge)) throw new PageBudgetError(charge);
    try {
      // Copy before any suspension, after charging the request/Set/output descriptors.
      const request = Object.freeze({ ...input, columns: Object.freeze([...input.columns]) });
      const seen = new Set<number>();
      for (const id of request.columns) {
        if (seen.has(id) || this.source.column(id) === undefined)
          throw new RangeError("Invalid memory read columns");
        seen.add(id);
        yield* pause(check);
      }
      const widths = new Float64Array(rows);
      let gzip = 0,
        rowBytes = 0,
        offsetBytes = 0;
      for (const id of request.columns) {
        yield* pause(check);
        const type = this.source.column(id)?.type;
        if (type === undefined) throw new RangeError("Missing memory column");
        const extraOffset = layoutOf(type) === "varlen" ? 4 : 0;
        rowBytes += memoryOutputBytes(memoryOutputLayout(type), 1, 0) - 4 - extraOffset;
        offsetBytes += extraOffset;
        if (layoutOf(type) === "varlen")
          gzip = Math.max(
            gzip,
            yield* this.#lengths(id, request, rows, context, check, (row, bytes) => {
              widths[row] = (widths[row] ?? 0) + bytes;
            }),
          );
        // Fixed columns still need their format envelope, including recovered gzip pages.
        else if (this.source.resident(id, request.chunk) === null)
          gzip = Math.max(
            gzip,
            yield* this.#withReader(id, context, check, function* (reader) {
              yield* pause(check);
              const [a, b] = reader.pagesInRange(
                request.chunk * MEMORY_CHUNK_ROWS + request.startRow,
                request.chunk * MEMORY_CHUNK_ROWS + request.startRow + rows,
              );
              let extra =
                reader.directory.dictionary !== null &&
                (reader.directory.dictionary.encoding & PageEncoding.gzip) !== 0
                  ? GZIP_MAX_PAGE_BYTES - MAX_BLOB_BYTES
                  : 0;
              for (let k = a; k < b; k++)
                if (((reader.directory.pages[k]?.encoding ?? 0) & PageEncoding.gzip) !== 0) {
                  extra += GZIP_MAX_PAGE_BYTES - MAX_BLOB_BYTES;
                  break;
                }
              return extra;
            }),
          );
      }
      let count = 0,
        payload = 0,
        outputBytes = 0;
      for (let n = 1; n <= rows; n++) {
        payload += widths[n - 1] ?? 0;
        const bytes =
          payload + n * rowBytes + offsetBytes + request.columns.length * Math.ceil(n / 32) * 4;
        if (!Number.isSafeInteger(bytes))
          throw new RangeError("Memory read size exceeds safe accounting units");
        if (n > 1 && bytes > request.targetBytes) break;
        count = n;
        outputBytes = bytes;
        if (bytes > request.targetBytes) break;
      }
      const outputs: MemoryOutputNeed[] = [];
      for (const id of request.columns) {
        yield* pause(check);
        const type = this.source.column(id)?.type;
        if (type === undefined) throw new RangeError("Missing memory column");
        let bytes = 0;
        if (layoutOf(type) === "varlen")
          yield* this.#lengths(id, request, count, context, check, (_, n) => {
            bytes += n;
          });
        outputs.push(
          Object.freeze(
            memoryNeed(id, request.startRow, count, Object.freeze(memoryOutputLayout(type)), bytes),
          ),
        );
      }
      const hint = Object.freeze({
        request,
        rowCount: count,
        outputs: Object.freeze(outputs),
        outputBytes,
        residentBytes: retained ? 0 : MEMORY_READ_RESIDENT_BYTES + gzip,
        scratchBytes:
          1024 +
          (retained ? 4096 : WORK + DECODER_SCRATCH) +
          rows * 8 +
          256 +
          request.columns.length * 256,
        ioBytes: retained ? 0 : MAX_READ_BYTES,
        oversized: outputBytes > request.targetBytes,
        token: Object.freeze({}),
      });
      this.#hints.add(hint);
      return hint;
    } finally {
      context.budget.free(charge);
    }
  }

  *#chunk(
    columnId: number,
    out: ChunkOutput,
    hint: MemoryReadHint,
    context: MemoryReadContext,
    check: () => void,
  ): Generator<Promise<unknown>, ChunkResult, unknown> {
    const r = hint.request,
      first = r.chunk * MEMORY_CHUNK_ROWS,
      start = r.startRow;
    const length = Math.min(MEMORY_CHUNK_ROWS, this.source.rowCount - first);
    const type = this.source.column(columnId)?.type;
    if (type === undefined) throw new RangeError("Missing memory column");
    // Covers scalar decode, object/BigInt temporaries and possible parse alignment copies.
    const retained = this.source.retained?.(r.chunk) === true;
    const work = retained ? 4096 : WORK;
    if (!context.budget.allocate(work)) throw new PageBudgetError(work);
    try {
      out.flat.length = 0;
      let written = 0,
        bytes = 0,
        required: MemoryOutputNeed | undefined;
      const accept = (source: FlatVector, row: number): boolean => {
        const nextBytes = bytes + memoryValueBytes(source, row);
        if (
          out.codes.length <= written ||
          !acceptsMemoryRow(out.flat, source, row, written, nextBytes)
        ) {
          const shape = memoryRowLayout(source, row);
          // Preserve lanes already used by the completed prefix as well as the next row.
          const target = out.flat.data;
          const preparedInteger =
            target.layout === "int64"
              ? target.values.repr
              : target.layout === "interval"
                ? target.micros.repr
                : target.layout === "decimal" && target.coefficients.repr !== "wide"
                  ? target.coefficients.repr
                  : null;
          const ranks = { safe: 0, int64: 1, int128: 2 };
          const integer =
            shape.integer !== null &&
            preparedInteger !== null &&
            ranks[preparedInteger] > ranks[shape.integer]
              ? preparedInteger
              : shape.integer;
          const merged = {
            ...shape,
            integer,
            validity: shape.validity || out.flat.validity !== null,
            scales: shape.scales || (target.layout === "decimal" && target.scales !== null),
            specials: shape.specials || (target.layout === "decimal" && target.specials !== null),
          };
          required = memoryNeed(columnId, start + written, written + 1, merged, nextBytes);
          return false;
        }
        bytes = nextBytes;
        return true;
      };
      let published = false;
      for (; written < hint.rowCount; written++) {
        yield* pause(check);
        const copied = this.#copyResident(columnId, r, written, out, accept);
        if (copied === null) {
          published = true;
          break;
        }
        if (!copied) break;
      }
      if (published) {
        if (retained) throw new Error("Retained source recipe lost its resident storage");
        yield* this.#withReader(columnId, context, check, function* (reader) {
          const scalar = scalarOutput(type);
          const mask = new Uint8Array(1),
            selected = new Uint32Array(MEMORY_CHUNK_ROWS / 32);
          const nulls = reader.compilePredicate({ op: "isNull" });
          if (nulls === null)
            throw new TypeError("Null predicate is unavailable for a stored column");
          let page = -1;
          for (; written < hint.rowCount; written++) {
            yield* pause(check);
            const row = first + start + written;
            const [nextPage] = reader.pagesInRange(row, row + 1);
            if (page !== nextPage) {
              if (page >= 0) reader.release([page]);
              yield* wait(reader.load([nextPage]));
              check();
              page = nextPage;
            }
            if (layoutOf(type) !== "varlen") {
              reader.decodeRange(row, row + 1, scalar, 0);
              scalar.length = 1;
              if (!accept(scalar, 0)) break;
              copyMemoryRow(scalar, 0, out.flat, written);
            } else {
              reader.selectMask(nulls, row, row + 1, mask);
              const valid = mask[0] !== 1;
              const size = valid ? reader.varlenBytes(row, row + 1) : 0;
              // Metadata-only row view: the output arena is not read until capacity is proved.
              scalar.validity = selected;
              selected[0] = valid ? 1 : 0;
              if (scalar.data.layout !== "varlen") throw new TypeError("Invalid scalar layout");
              scalar.data.offsets[0] = 0;
              scalar.data.offsets[1] = size;
              if (!accept(scalar, 0)) break;
              const target = out.flat.data;
              if (target.layout !== "varlen") throw new TypeError("Invalid output layout");
              const begin = bytes - size;
              if (written === 0) target.offsets[0] = 0;
              if (valid && reader.largeValueBytes(row, row + 1) > 0) {
                let cursor = begin;
                const parts = reader.valueParts(row);
                try {
                  for (;;) {
                    const part = (yield parts.next()) as IteratorResult<Uint8Array>;
                    check();
                    if (part.done) break;
                    target.bytes.set(part.value, cursor);
                    cursor += part.value.length;
                    yield* pause(check);
                  }
                } finally {
                  yield parts.return(undefined);
                }
                if (cursor !== begin + size)
                  throw new RangeError("Streamed memory value has the wrong length");
              } else if (valid) {
                // Decode only one admitted value. Use scratch validity: a page's invisible null
                // suffix must not force a replacement of an all-valid caller output.
                const decoded: FlatVector = { ...out.flat, validity: selected };
                reader.decodeRange(row, row + 1, decoded, written);
              }
              target.offsets[written + 1] = begin + size;
              if (out.flat.validity !== null) {
                const k = written >>> 5,
                  bit = 1 << (written & 31);
                out.flat.validity[k] = valid
                  ? (out.flat.validity[k] ?? 0) | bit
                  : (out.flat.validity[k] ?? 0) & ~bit;
              }
              out.flat.length = written + 1;
            }
            out.codes[written] = written;
          }
          check();
        });
      }
      check();
      out.flat.length = written;
      const complete = start + written === length;
      return {
        vector: out.flat,
        firstRow: start,
        rowCount: written,
        complete,
        reason: required !== undefined ? "need" : complete ? null : "full",
        ...(required === undefined ? {} : { need: required }),
      };
    } finally {
      context.budget.free(work);
    }
  }
  #copyResident(
    columnId: number,
    r: MemoryReadRequest,
    row: number,
    out: ChunkOutput,
    accept: (source: FlatVector, row: number) => boolean,
  ): boolean | null {
    const vector = this.source.resident(columnId, r.chunk);
    if (vector === null) return null;
    if (!accept(vector, r.startRow + row)) return false;
    copyMemoryRow(vector, r.startRow + row, out.flat, row);
    out.codes[row] = row;
    return true;
  }
}

/** One precharged scalar, never an output replacement or a logical-value-size temporary. */
function scalarOutput(type: PhysicalType): FlatVector {
  const layout = layoutOf(type);
  let data: VectorData;
  switch (layout) {
    case "int64":
      data = { layout, values: { repr: "int64", values: new BigInt64Array(1) } };
      break;
    case "interval":
      data = {
        layout,
        months: new Int32Array(1),
        days: new Int32Array(1),
        micros: { repr: "int64", values: new BigInt64Array(1) },
      };
      break;
    case "decimal":
      data = {
        layout,
        coefficients: { repr: "int128", limbs: new Uint32Array(4) },
        scale: null,
        scales: new Uint16Array(1),
        specials: new Uint8Array(1),
      };
      break;
    default:
      data = allocateData(type, 1);
      break;
  }
  return { form: "flat", type, length: 0, validity: new Uint32Array(1), data };
}
