/** Size a complete source step before admitting any execution-owned buffers. */
type MaybePromise<T> = T | Promise<T>;
type ReadJob<T> = Generator<Promise<void>, T, void>;
import type { ByteBudget, ExtentReader as SpaceReader, BlobRef } from "../storage/extents.js";
import { ReadDeniedError } from "../storage/extents.js";
import { MetadataReader } from "../format/metadata-reader.js";
import type { QueryRoots, ScanRootFacts } from "./query-roots.js";
import { parseCharge, ParseBudgetError } from "../format/manifest-codec.js";
import { PageEncoding } from "../format/manifest.js";
import type { Grant, MemoryGovernor } from "../memory/governor.js";
import { admitWorkingSet } from "../memory/working-set.js";
import { MemoryAdmissionError } from "../memory/governor.js";
import { ChunkReader, PageBudgetError } from "../page/reader.js";
import { MAX_VALUE_BYTES } from "./large-value.js";
import { MAX_BLOB_BYTES, MAX_READ_BYTES } from "../storage/extents.js";
import { BATCH_ROWS, dataBytes } from "../vector/vector.js";
import { INT32_MAX, type PhysicalType } from "../types.js";
import { clockFor, pauseIfDue } from "../task.js";
import { Int32DomainCertificate, type Int32Domain } from "./domain-certificate.js";

interface Snapshot {
  readonly pages: SpaceReader;
  table(
    id: number,
    options: { budget: ByteBudget },
  ): MaybePromise<{
    readonly liveRowCount?: number;
    readonly columns: ReadonlyArray<{ readonly id: number }>;
    readonly segments: ReadonlyArray<
      | {
          readonly kind: "memory";
          readonly rowCount: number;
          readonly liveRowCount?: number;
          /** Structural subset of access/MemorySegment: page stays below access. */
          readHint?: (
            request: {
              readonly columns: readonly number[];
              readonly chunk: number;
              readonly startRow: number;
              readonly maxRows: number;
              readonly targetBytes: number;
            },
            context: {
              readonly budget: ByteBudget;
              readonly pages: SpaceReader;
              readonly signal: AbortSignal;
            },
          ) => MaybePromise<{
            readonly rowCount: number;
            readonly outputBytes: number;
            readonly residentBytes: number;
            readonly scratchBytes: number;
            readonly ioBytes: number;
          }>;
        }
      | {
          readonly kind: "group";
          readonly rowCount: number;
          readonly liveRowCount?: number;
          readonly group: { readonly root: BlobRef };
        }
    >;
  }>;
}
function run<T>(job: ReadJob<T>): MaybePromise<T> {
  const step = job.next();
  return step.done ? step.value : step.value.then(() => run(job));
}

export interface ReadSource {
  /** Explicit terminal export: sizing misses must not pollute the shared cache. */
  readonly retain?: boolean;
  readonly id: number;
  readonly tableId: number;
  readonly columns: readonly number[];
  readonly types: readonly PhysicalType[];
  readonly windowBytes: number;
  readonly stagingBytes: number;
  /** A proven unfiltered input prefix; null means the complete input can be reached. */
  readonly rows: number | null;
  /** Optional exact int32 address certificate for a complete, unfiltered source. */
  readonly domainColumn?: number;
}
export interface ReadBound {
  readonly retain?: boolean;
  readonly bytes: number;
  readonly io: number;
  readonly progress: number;
  /** Conservative logical column-value bound, without the progress-lane floor.
   * A selected key column can be sized independently from other row payloads. */
  readonly valueBytes?: number;
  readonly windowBytes: number;
  readonly domain?: Int32Domain;
}

/** A sizing pass cannot grow or hold a partial reservation while awaiting a bigger one. */
class SizingBudget implements ByteBudget {
  allocated = 0;
  required = 0;
  constructor(
    readonly owner: ByteBudget,
    readonly capacity: number,
  ) {}
  allocate(bytes: number): boolean {
    const needed = this.allocated + bytes;
    this.required = Math.max(this.required, needed);
    if (needed > this.capacity || !this.owner.allocate(bytes)) return false;
    this.allocated = needed;
    return true;
  }
  free(bytes: number): void {
    this.owner.free(bytes);
    this.allocated -= bytes;
  }
}

const NO_ALLOCATION: ByteBudget = { allocate: () => false, free: () => undefined };

/**
 * Only metadata is read for ordinary inline pages. Large-value header pages are inspected to
 * find their exact logical lengths; out-of-line payloads are not read. Each sizing attempt is
 * itself one fully admitted bounded step and closes all readers before it retries admission.
 * Its tables are used only for immutable segment descriptors, never for borrowed row values.
 */
function scratchMinimum(
  governor: MemoryGovernor,
  label: string,
  minimumBytes: number,
  signal: AbortSignal,
): MaybePromise<Grant> {
  const admitted = admitWorkingSet(governor, {
    label,
    includesProgress: true,
    signal,
    grants: [{ label, minimumBytes, spillable: false }],
  });
  const first = (parts: readonly Grant[]): Grant => {
    const grant = parts[0];
    if (!grant) throw new Error("source sizing did not receive its native minimum");
    return grant;
  };
  return admitted instanceof Promise ? admitted.then(first) : first(admitted);
}

export function boundSource(
  view: Snapshot,
  source: ReadSource,
  governor: MemoryGovernor,
  signal: AbortSignal,
  target: number,
  // One-row result-copy sizing must not repeatedly inspect a whole remaining interval.
  prefixRows = BATCH_ROWS,
  caches?: QueryRoots,
): MaybePromise<ReadBound> {
  return run(size());
  function* size(): ReadJob<ReadBound> {
    const table = yield* readValue(view.table(source.tableId, { budget: NO_ALLOCATION }));
    const certificate = new Int32DomainCertificate(
      source.domainColumn,
      table.liveRowCount,
      source.rows === null,
    );
    const fullColumns =
      source.columns.length === table.columns.length &&
      source.columns.every((id, k) => id === table.columns[k]?.id);
    const windowBytes = fullColumns ? source.windowBytes : 0;
    const retention = source.retain === false && fullColumns ? { retain: false } : {};
    const sizingPages: SpaceReader =
      retention.retain === false
        ? {
            peek: (ref, options) => view.pages.peek(ref, { ...options, once: true }),
            read: (refs, options) => view.pages.read(refs, { ...options, ...retention }),
          }
        : view.pages;
    const metadata = new MetadataReader({
      peek: (ref, options) => sizingPages.peek(ref, options),
      read: (refs, options) =>
        sizingPages.read(refs, { ...options, ...retention, signal, reuseBuffers: false }),
    });
    const task = { clock: clockFor(), signal };
    let maximum = 0,
      io = 0,
      largestValue = 0,
      valueBytes = 0,
      remaining = source.rows;
    for (const segment of table.segments) {
      signal.throwIfAborted();
      if (remaining === 0) break;
      if (segment.rowCount === 0) continue;
      // LIMIT counts live rows. A sparse/deleted group may reach any stored position,
      // including its last page, before it produces the requested live prefix.
      const live = segment.liveRowCount;
      const rows =
        live === segment.rowCount
          ? Math.min(segment.rowCount, remaining ?? segment.rowCount)
          : segment.rowCount;
      if (remaining !== null) remaining = live === undefined ? null : Math.max(0, remaining - live);
      if (segment.kind === "memory") {
        if (segment.liveRowCount !== 0) certificate.unknown();
        if (segment.readHint !== undefined && source.columns.length > 0) {
          let capacity = Math.min(governor.capBytes, 65536);
          for (;;) {
            const grant = yield* readValue(
              scratchMinimum(governor, "read-step/memory-sizing", capacity, signal),
            );
            const budget = new SizingBudget(grant, capacity);
            let retry = false,
              required = capacity;
            try {
              for (let first = 0; first < rows; first += BATCH_ROWS) {
                const end = Math.min(BATCH_ROWS, rows - first);
                for (let start = 0; start < end;) {
                  const hint = yield* readValue(
                    segment.readHint(
                      {
                        columns: source.columns,
                        chunk: first / BATCH_ROWS,
                        startRow: start,
                        maxRows: Math.min(prefixRows, end - start),
                        targetBytes: target,
                      },
                      { budget, pages: sizingPages, signal },
                    ),
                  );
                  if (
                    !Number.isInteger(hint.rowCount) ||
                    hint.rowCount < 1 ||
                    hint.rowCount > end - start
                  )
                    throw new RangeError("Memory sizing hint made no bounded progress");
                  // Every ordinary prefix fits target. A row larger than target forms its
                  // own prefix and is therefore visited by this pass. Any later sub-prefix
                  // is bounded by max(target, largest observed oversized row), even when
                  // a LIMIT or sparse selection changes its start. Native output shapes
                  // remain valid across publication. Include old/new output overlap and
                  // conservative descriptor/view/buffer headers in the source minimum.
                  const output = Math.max(target, hint.outputBytes);
                  maximum = Math.max(
                    maximum,
                    source.stagingBytes +
                      hint.residentBytes +
                      hint.scratchBytes +
                      2 * output +
                      8192 +
                      source.columns.length * 4096,
                  );
                  io = Math.max(io, hint.ioBytes);
                  largestValue = Math.max(largestValue, output);
                  valueBytes = Math.max(valueBytes, hint.outputBytes);
                  start += hint.rowCount;
                  const pause = pauseIfDue(task);
                  if (pause) yield pause;
                }
              }
            } catch (error) {
              if (!(
                error instanceof ReadDeniedError ||
                error instanceof PageBudgetError ||
                error instanceof ParseBudgetError
              ))
                throw error;
              retry = true;
              required = Math.max(capacity * 2, budget.required);
            } finally {
              grant.close();
            }
            if (!retry) break;
            if (capacity === governor.capBytes)
              throw new MemoryAdmissionError(
                "read-step/memory-sizing",
                Math.max(required, capacity + 1),
                governor.capBytes,
              );
            capacity = Math.min(required, governor.capBytes);
          }
          continue;
        }
        // Compatibility for legacy callers without readHint. Production uses the bounded
        // provider above. An unbounded chunk() can decode the
        // entire remaining 2048-row interval before the output builder stops. One
        // logical value can span 64MiB; neither the byte target nor a 1MiB blob bounds it.
        const columns = source.types.length;
        const variable = source.types.filter((type) =>
          ["text", "bytes", "decimal", "array", "json", "enum"].includes(type.kind),
        ).length;
        // Resident varlen buffers use signed int32 offsets; do not assume that the
        // eventual persisted-value limit was already checked before publication.
        const valueLimit = Math.max(INT32_MAX, MAX_VALUE_BYTES);
        const logical = Math.min(BATCH_ROWS, segment.rowCount) * variable * valueLimit;
        // 64MiB per selected column conservatively covers a maximum directory and its
        // parsed descriptors, both boundary pages/inflation, one header per interval row,
        // dictionary, decode temporaries and buffer overlap. 8x logical bytes covers
        // fragments, separate assembly, decoder copy and old/new output capacities.
        const worst =
          columns === 0
            ? source.stagingBytes
            : source.stagingBytes +
              parseCharge(MAX_BLOB_BYTES) +
              MAX_BLOB_BYTES +
              columns * 64 * 1024 * 1024 +
              8 * logical;
        if (!Number.isSafeInteger(worst) || worst > governor.capBytes)
          throw new MemoryAdmissionError(
            "read-step/legacy-memory-interval",
            worst,
            governor.capBytes,
          );
        maximum = Math.max(maximum, worst);
        io = Math.max(io, columns === 0 ? 0 : MAX_READ_BYTES);
        largestValue = Math.max(largestValue, logical);
        valueBytes = Math.max(valueBytes, logical);
        const pause = pauseIfDue(task);
        if (pause) yield pause;
        continue;
      }
      let capacity = Math.min(
        governor.capBytes,
        Math.max(65536, parseCharge(segment.group.root.length) + segment.group.root.length),
      );
      for (;;) {
        const grant = yield* readValue(
          scratchMinimum(governor, "read-step/sizing", capacity, signal),
        );
        const budget = new SizingBudget(grant, capacity);
        const readers: ChunkReader[] = [];
        let root: import("../format/reachable.js").Held<ScanRootFacts> | null = null;
        let retry = false,
          required = capacity;
        try {
          io = Math.max(io, Math.min(MAX_READ_BYTES, segment.group.root.length));
          root = yield* readValue(
            retention.retain === false || caches === undefined
              ? metadata.groupRoot(segment.group.root, budget)
              : caches.read(
                  source.tableId,
                  segment.group.root,
                  {
                    peek: (ref, options) => sizingPages.peek(ref, options),
                    read: (refs, options) =>
                      sizingPages.read(refs, { ...options, signal, reuseBuffers: false }),
                  },
                  budget,
                  source.domainColumn === undefined
                    ? source.columns
                    : [...source.columns, source.domainColumn],
                ),
          );
          if (source.domainColumn !== undefined && segment.liveRowCount !== 0)
            certificate.stored(
              segment.rowCount,
              root.value.columns.find((column) => column.columnId === source.domainColumn),
            );
          const stored = root.value.columns
            .filter((column) => source.columns.includes(column.columnId))
            .reduce((sum, column) => sum + column.data.length, 0);
          const span =
            windowBytes === 0
              ? BATCH_ROWS
              : Math.max(
                  BATCH_ROWS,
                  Math.floor((windowBytes * segment.rowCount) / Math.max(1, stored) / BATCH_ROWS) *
                    BATCH_ROWS,
                );
          const windows = Math.ceil(rows / span),
            arrayBytes = 24 * windows;
          if (!budget.allocate(arrayBytes)) throw new PageBudgetError(arrayBytes);
          const held = new Float64Array(windows),
            values = new Float64Array(windows),
            encodedBytes = new Float64Array(windows);
          let directories = parseCharge(segment.group.root.length),
            dictionaries = 0,
            largestOpen = segment.group.root.length;
          try {
            for (let c = 0; c < source.columns.length; c++) {
              const column = source.columns[c],
                type = source.types[c];
              if (column === undefined || type === undefined)
                throw new TypeError("read bound column");
              const chunk = root.value.columns.find((candidate) => candidate.columnId === column);
              if (!chunk) continue;
              const before = budget.allocated;
              // No directory/inflate admission is needed by the sizing pass. Exact-sized
              // encoded reads keep this attempt's complete reservation a hard limit.
              const pages: SpaceReader = {
                peek: () => null,
                read: (refs, options) =>
                  view.pages.read(refs, {
                    ...options,
                    ...retention,
                    once: false,
                    reuseBuffers: false,
                    signal,
                  }),
              };
              const reader = yield* readValue(
                ChunkReader.open(pages, chunk, {
                  budget,
                  once: true,
                  signal,
                  task,
                }),
              );
              readers.push(reader);
              directories += budget.allocated - before;
              largestOpen = Math.max(largestOpen, chunk.directory.length);
              io = Math.max(io, Math.min(MAX_READ_BYTES, chunk.directory.length));
              const dictionary = reader.directory.dictionary;
              if (dictionary) {
                const beforeDictionary = budget.allocated;
                const decoded = yield* readValue(reader.loadDictionary());
                const dictionaryBytes =
                  dictionary.length +
                  budget.allocated -
                  beforeDictionary +
                  4 * dictionary.rowCount +
                  1024;
                dictionaries += dictionaryBytes;
                valueBytes = Math.max(
                  valueBytes,
                  dictionaryBytes,
                  decoded?.data.layout === "varlen" ? decoded.data.bytes.byteLength : 0,
                );
                io = Math.max(io, Math.min(MAX_READ_BYTES, dictionary.length));
              }
              for (const [index, page] of reader.directory.pages.entries()) {
                if (page.firstRow >= rows) break;
                const encoded = page.length;
                let raw = encoded;
                const lane =
                  type.kind === "int64" || type.kind === "timestamp" || type.kind === "time"
                    ? 8 * page.rowCount
                    : 0;
                const first = Math.floor(page.firstRow / span),
                  last = Math.floor((Math.min(rows, page.firstRow + page.rowCount) - 1) / span);
                io = Math.max(io, Math.min(MAX_READ_BYTES, encoded));
                if (page.largeRegion || page.encoding & PageEncoding.gzip) {
                  const beforePage = budget.allocated;
                  yield* readValue(reader.load([index]));
                  raw = Math.max(encoded, budget.allocated - beforePage);
                  if (
                    !page.largeRegion &&
                    ["text", "bytes", "json", "array", "enum"].includes(type.kind)
                  )
                    valueBytes = Math.max(
                      valueBytes,
                      reader.varlenBytes(
                        page.firstRow,
                        Math.min(rows, page.firstRow + page.rowCount),
                      ),
                    );
                }
                if (!page.largeRegion) valueBytes = Math.max(valueBytes, raw);
                for (let w = first; w <= last; w++) {
                  held[w] = (held[w] ?? 0) + encoded + raw + lane + 1024;
                  encodedBytes[w] = (encodedBytes[w] ?? 0) + encoded;
                  if (page.largeRegion) {
                    const from = Math.max(w * span, page.firstRow),
                      to = Math.min(rows, (w + 1) * span, page.firstRow + page.rowCount);
                    const large = reader.largeValueBytes(from, to);
                    values[w] = (values[w] ?? 0) + large;
                    largestValue = Math.max(largestValue, large);
                    valueBytes = Math.max(valueBytes, large);
                    io = Math.max(io, Math.min(MAX_READ_BYTES, large));
                  }
                }
                reader.release([index]);
                if ((index & 63) === 0) {
                  const pause = pauseIfDue(task);
                  if (pause) yield pause;
                }
              }
              reader.close();
              readers.pop();
              const pause = pauseIfDue(task);
              if (pause) yield pause;
            }
            const rowCount = Math.max(64, Math.min(BATCH_ROWS, rows));
            const fixed = source.types.reduce(
              (sum, type) =>
                sum +
                dataBytes(type, rowCount) +
                4 * Math.ceil(rowCount / 32) +
                (type.kind === "text" ? 4 * rowCount : 0),
              0,
            );
            // The recipe already reserves one target of output; add the fixed arrays and
            // resize overlap here, rather than reserving that same target twice.
            const variableOutput = source.types.some((type) =>
              ["text", "bytes", "decimal", "array", "json", "enum"].includes(type.kind),
            );
            const output = 2 * fixed + (variableOutput ? target : 0);
            for (let w = 0; w < windows; w++) {
              largestValue = Math.max(largestValue, values[w] ?? 0);
              io = Math.max(io, Math.min(MAX_READ_BYTES, (encodedBytes[w] ?? 0) + dictionaries));
              maximum = Math.max(
                maximum,
                directories +
                  dictionaries +
                  largestOpen +
                  (held[w] ?? 0) +
                  4 * (values[w] ?? 0) +
                  output,
              );
            }
          } finally {
            budget.free(arrayBytes);
          }
        } catch (error) {
          if (!(
            error instanceof ReadDeniedError ||
            error instanceof PageBudgetError ||
            error instanceof ParseBudgetError
          ))
            throw error;
          retry = true;
          required = Math.max(capacity * 2, budget.required);
        } finally {
          for (const reader of readers) reader.close();
          root?.release();
          grant.close();
        }
        if (!retry) break;
        if (capacity === governor.capBytes)
          throw new MemoryAdmissionError(
            "read-step/sizing",
            Math.max(required, capacity + 1),
            governor.capBytes,
          );
        capacity = Math.min(required, governor.capBytes);
      }
    }
    const domain = certificate.result();
    return {
      bytes: maximum,
      io,
      progress: Math.max(65536, 4 * largestValue),
      valueBytes,
      windowBytes,
      ...retention,
      ...(domain === undefined ? {} : { domain }),
    };
  }
}

/** Resume inside the generator even on rejection, so its finally blocks release all holds. */
export function* readValue<T>(value: MaybePromise<T>): ReadJob<T> {
  if (!(value instanceof Promise)) return value;
  let result!: T;
  const failure: { error: unknown; failed: boolean } = { error: undefined, failed: false };
  yield value.then(
    (got) => {
      result = got;
    },
    (error: unknown) => {
      failure.error = error;
      failure.failed = true;
    },
  );
  if (failure.failed) throw failure.error;
  return result;
}
