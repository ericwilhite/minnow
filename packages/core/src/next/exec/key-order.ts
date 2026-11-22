/** Ordered index cursors and private locator fetches share the query's pinned view. */
import type { IndexCursor, IndexReader, IndexRangePlan } from "../access/key-index.js";
import { trackSizing, type ReadBound } from "../page/read-bounds.js";
import { admitWorkingSet } from "../memory/working-set.js";
import { directoryCacheFor } from "../page/directory-cache.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import { BATCH_ROWS, dataBytes, type FlatVector } from "../vector/vector.js";
import { batchTargetFor, OBJECT_OVERHEAD_BYTES, type MemoryGovernor } from "../memory/governor.js";
import { locatorSegment } from "../vector/locator.js";
import { pauseIfDue } from "../task.js";
import { layoutOf, samePhysicalType, type PhysicalType } from "../types.js";
import { OutputArena } from "./kernels/vectors.js";
import { drive, type BatchSource, type Operator, type SourceItem } from "./pipeline.js";
import { LateFetchOperator, ORDINAL_TYPE } from "./late-fetch.js";
import { memoryScanBufferBytes } from "./table-scan.js";
import { sortMinimumBytes } from "./sort.js";
import { isByteCollation } from "./sort-keys.js";
import type { OperatorIdentity, QueryRuntime } from "./run.js";

export function planIndexRange(reader: IndexReader, descending: boolean) {
  const plan = reader.planRange?.({ lower: null, upper: null }, descending ? "desc" : "asc");
  if (plan && (!Number.isSafeInteger(plan.minimumBytes) || plan.minimumBytes < 0))
    throw new RangeError("invalid index cursor minimum");
  return plan ?? null;
}

/** The grant label of the window sizing pass (its cursor is the query's sizing cursor). */
export const WINDOW_SIZING_LABEL = "key-order/window-sizing";
/** The grant label of the window's segment list, held until sizing finishes. */
export const WINDOW_SEGMENTS_LABEL = "key-order/window-segments";

/** A window's distinct segment indexes, ascending, and the charge that holds them. */
export interface WindowSegments {
  readonly segments: Uint32Array;
  /** Frees the list's charge once sizing no longer reads it. Idempotent. */
  release(): void;
}

/**
 * The segments a bounded primary-key window names: OFFSET skipped in the index, then up to
 * `rows` locators. One admission charges the cursor scratch with its locators, and the segment
 * list (4 bytes a locator, its bound). The scratch is released before this returns; the list's
 * charge stays with the list until the caller releases it after sizing, so only that list is
 * held while sizing admits. `plan` is a fresh single-use plan on the query's pinned view:
 * execution's cursor yields the same window there, so a read bound over exactly these segments
 * covers every row the fetch reads.
 */
export function windowSegments(
  plan: IndexRangePlan,
  offset: number,
  rows: number,
  governor: MemoryGovernor,
  signal: AbortSignal,
): Promise<WindowSegments> {
  // Registered like source sizing: teardown waits for this cursor and grant to unwind.
  return trackSizing(
    signal,
    readWindow(plan, offset, rows, governor, signal),
  ) as Promise<WindowSegments>;
}

async function readWindow(
  plan: IndexRangePlan,
  offset: number,
  rows: number,
  governor: MemoryGovernor,
  signal: AbortSignal,
): Promise<WindowSegments> {
  if (!Number.isSafeInteger(rows) || rows < 1 || rows > BATCH_ROWS)
    throw new RangeError("A sized window holds one cursor batch");
  const locatorBytes = rows * Float64Array.BYTES_PER_ELEMENT,
    segmentBytes = rows * Uint32Array.BYTES_PER_ELEMENT;
  const [grant, held] = await admitWorkingSet(governor, {
    label: "key-order/window",
    includesProgress: true,
    signal,
    grants: [
      {
        label: WINDOW_SIZING_LABEL,
        minimumBytes: plan.minimumBytes + locatorBytes,
        spillable: false,
      },
      { label: WINDOW_SEGMENTS_LABEL, minimumBytes: segmentBytes, spillable: false },
    ],
  });
  if (grant === undefined || held === undefined)
    throw new Error("window sizing did not receive its minimum");
  let kept = false;
  try {
    // Every buffer is charged before it is allocated; the cursor's scratch is in the plan minimum.
    if (!grant.allocate(locatorBytes) || !held.allocate(segmentBytes))
      throw new Error("window sizing did not receive its minimum");
    const out = new Float64Array(rows);
    const list = new Uint32Array(rows);
    let filled = 0;
    const cursor = plan.open({ budget: grant, signal });
    // Cancellation closes at once (cancelling pending I/O); the outstanding call still settles
    // before `out` and the scratch are released below.
    let open = true;
    const close = () => {
      if (!open) return;
      open = false;
      cursor.close();
    };
    signal.addEventListener("abort", close, { once: true });
    try {
      let skip = offset;
      while (skip > 0) {
        const skipped = await cursor.skip(skip);
        signal.throwIfAborted();
        if (!Number.isSafeInteger(skipped) || skipped < 0 || skipped > skip)
          throw new RangeError("invalid index cursor skip count");
        if (skipped < skip) break;
        skip -= skipped;
      }
      while (skip === 0 && filled < rows) {
        const count = await cursor.next(out.subarray(filled, rows));
        signal.throwIfAborted();
        if (!Number.isInteger(count) || count < 0 || count > rows - filled)
          throw new RangeError("invalid index cursor count");
        if (count === 0) break;
        for (let k = filled; k < filled + count; k++) list[k] = locatorSegment(out[k] ?? -1);
        filled += count;
      }
    } finally {
      signal.removeEventListener("abort", close);
      close();
      grant.free(locatorBytes);
      grant.close();
    }
    // Distinct, ascending, in place inside the charged list.
    const window = list.subarray(0, filled).sort();
    let distinct = 0;
    for (let k = 0; k < window.length; k++)
      if (k === 0 || window[k] !== window[k - 1]) window[distinct++] = window[k] ?? 0;
    let released = false;
    const result: WindowSegments = {
      segments: list.subarray(0, distinct),
      release() {
        if (released) return;
        released = true;
        held.free(segmentBytes);
        held.close();
      },
    };
    kept = true;
    return result;
  } finally {
    // A failed read releases both charges; a returned list keeps its own until released.
    if (grant.allocated > 0) grant.free(grant.allocated);
    grant.close();
    if (!kept) {
      if (held.allocated > 0) held.free(held.allocated);
      held.close();
    }
  }
}

export class IndexOrderSource implements BatchSource {
  #pending: Promise<SourceItem | null> | null = null;
  #aborted = false;
  rowsOut = 0;
  batchesOut = 0;
  constructor(
    readonly cursor: IndexCursor,
    readonly vector: FlatVector,
    readonly signal: AbortSignal,
    private remaining: number,
    private skip: number,
    readonly demand: () => number = () => BATCH_ROWS,
    readonly stopped: () => boolean = () => false,
  ) {}
  get closed(): Promise<void> {
    return (
      this.#pending?.then(
        () => undefined,
        () => undefined,
      ) ?? Promise.resolve()
    );
  }
  next(): Promise<SourceItem | null> {
    if (this.#pending !== null) throw new TypeError("concurrent cursor reads");
    const work = this.#next();
    this.#pending = work;
    return work.finally(() => {
      this.#pending = null;
    });
  }
  async #next(): Promise<SourceItem | null> {
    const data = this.vector.data;
    if (data.layout !== "f64") throw new TypeError("locator layout");
    while (!this.#cancelled() && !this.stopped() && this.remaining > 0) {
      this.signal.throwIfAborted();
      const pending = pauseIfDue({ signal: this.signal });
      if (pending) await pending;
      if (this.skip > 0) {
        const requested = this.skip;
        const skipped = await this.cursor.skip(requested);
        this.signal.throwIfAborted();
        if (this.#aborted) return null;
        if (!Number.isSafeInteger(skipped) || skipped < 0 || skipped > requested)
          throw new RangeError("invalid index cursor skip count");
        this.skip = 0;
        if (skipped < requested) break;
        continue;
      }
      const size = Math.min(BATCH_ROWS, this.remaining, this.demand());
      if (size <= 0) break;
      const count = await this.cursor.next(data.values.subarray(0, size));
      this.signal.throwIfAborted();
      if (this.#aborted) return null;
      if (!Number.isInteger(count) || count < 0 || count > size)
        throw new RangeError("invalid index cursor count");
      if (count === 0) break;
      this.remaining -= count;
      this.vector.length = count;
      this.rowsOut += count;
      this.batchesOut++;
      return { batch: { length: count, selection: null, columns: [this.vector] } };
    }
    this.abort();
    return null;
  }
  abort(): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.cursor.close();
  }
  #cancelled(): boolean {
    return this.#aborted;
  }
}

export interface IndexShape {
  readonly tableId: number;
  readonly columns: readonly number[];
  readonly types: readonly PhysicalType[];
  readonly source: OperatorIdentity;
  readonly fetch: OperatorIdentity;
  readonly restore: OperatorIdentity;
  readonly descending: boolean;
  /**
   * An unadmitted (diagnostic) ordered scan's cursor: funded as a fixed entry of the query, its
   * planned minimum (`legacyCursorMinimum`), and opened on that grant. The cursor then never
   * admits itself mid-query, while the query holds its other grants (GOVERNOR-WAIT-POLICY: no
   * hold-and-wait). A reader with no range plan keeps its own `range()` cursor.
   */
  readonly cursor?: OperatorIdentity;
}

/** The planned minimum of an unadmitted ordered scan's cursor, or 0 without a range plan. */
export function legacyCursorMinimum(runtime: QueryRuntime, shape: IndexShape, indexId: number) {
  return (
    planIndexRange(runtime.view.index(shape.tableId, indexId), shape.descending)?.minimumBytes ?? 0
  );
}
/** A byte target chooses real read batches; it is never a certificate about a value.
 * Unknown/underestimated widths still pass through ordinary bounded source sizing,
 * which includes oversized rows. LIMIT caps cursor output, not physical key positions. */
export function indexReadTarget(
  types: readonly PhysicalType[],
  cap: number,
  limit: number | null,
): number {
  const rows = Math.min(BATCH_ROWS, Math.max(1, limit ?? BATCH_ROWS));
  let width = 0;
  for (const type of types)
    width +=
      layoutOf(type) !== "varlen"
        ? dataBytes(type, 1)
        : type.kind === "text" && type.maxLength !== undefined
          ? Math.min(65536, 4 * type.maxLength)
          : 64;
  return Math.min(batchTargetFor(cap, 4), Math.max(4096, rows * Math.max(1, width)));
}
export function indexMinimums(
  shape: IndexShape,
  cap: number,
  targetBytes = batchTargetFor(cap, 4),
  rows = BATCH_ROWS,
) {
  return [
    { rel: shape.source, minimum: rows * 32 + 4096 },
    {
      rel: shape.fetch,
      minimum:
        targetBytes +
        memoryScanBufferBytes(shape.types) +
        // LIMIT bounds locators, not physical slots in a sparse payload scan. The
        // ordinal vector and five position/selection lists still hold a full batch.
        dataBytes(ORDINAL_TYPE, BATCH_ROWS) +
        4 * Math.ceil(BATCH_ROWS / 32) +
        5 * BATCH_ROWS * Uint16Array.BYTES_PER_ELEMENT +
        1 +
        rows * (80 + 8 * OBJECT_OVERHEAD_BYTES) +
        OBJECT_OVERHEAD_BYTES * (16 + 2 * shape.types.length),
    },
    ...(rows === 1
      ? []
      : [{ rel: shape.restore, minimum: sortMinimumBytes([...shape.types, ORDINAL_TYPE]) }]),
  ];
}

export interface AdmittedIndexRead {
  readonly plan: IndexRangePlan;
  readonly cursor: OperatorIdentity;
  readonly payload: ReadBound;
  readonly targetBytes: number;
  /** Exact SQL bound on cursor output, never a statistics estimate. */
  readonly rows: number;
}
export function admittedIndexMinimums(shape: IndexShape, cap: number, read: AdmittedIndexRead) {
  return [
    ...indexMinimums(shape, cap, read.targetBytes, read.rows).map((entry) => ({
      ...entry,
      minimum: entry.minimum + (entry.rel.id === shape.fetch.id ? read.payload.bytes : 0),
    })),
    { rel: read.cursor, minimum: read.plan.minimumBytes },
  ];
}

export async function runIndexOrder(
  runtime: QueryRuntime,
  sink: Operator,
  shape: IndexShape,
  indexId: number,
  limit: number | null,
  offset: number,
  demand?: () => number,
  stopped?: () => boolean,
  admitted?: AdmittedIndexRead,
): Promise<void> {
  // An unadmitted scan's cursor, funded with the query (`IndexShape.cursor`).
  const legacy =
    admitted === undefined && shape.cursor !== undefined
      ? planIndexRange(runtime.view.index(shape.tableId, indexId), shape.descending)
      : null;
  const entries = admitted
    ? admittedIndexMinimums(shape, runtime.governor.capBytes, admitted)
    : [
        ...indexMinimums(shape, runtime.governor.capBytes),
        ...(legacy !== null && shape.cursor !== undefined
          ? [{ rel: shape.cursor, minimum: legacy.minimumBytes }]
          : []),
      ];
  const grants = [];
  for (const entry of entries)
    grants.push(
      await runtime.admit(
        `${entry.rel.op}#${String(entry.rel.id)}`,
        entry.minimum,
        true,
        entry.rel,
      ),
    );
  const [sourceGrant, fetchGrant] = grants;
  const restoreGrant = admitted?.rows === 1 ? fetchGrant : grants[2];
  if (!sourceGrant || !fetchGrant || !restoreGrant) throw new Error("index admissions");
  const arena = new OutputArena(sourceGrant, admitted?.rows ?? BATCH_ROWS),
    fetchArena = new OutputArena(fetchGrant, BATCH_ROWS);
  runtime.dispose(() => arena.close());
  runtime.dispose(() => fetchArena.close());
  const vector = arena.flat({ kind: "locator", tableId: shape.tableId }).vector;
  const directories = directoryCacheFor(runtime.view.pages);
  const fetch = new LateFetchOperator(
    {
      snapshot: runtime.view,
      ...(admitted
        ? {
            onePass: true,
            boundedMemory: true,
            readTable: () => runtime.table(shape.tableId, fetchGrant),
            pages: runtime.readPages(runtime.view.pages),
          }
        : {}),
      tableId: shape.tableId,
      columns: shape.columns,
      types: shape.types,
      locator: 0,
      compile: { arena: fetchArena, params: runtime.params, input: [vector.type] },
      grant: fetchGrant,
      restoreGrant,
      reads: runtime.request.reads,
      ...(directories === undefined ? {} : { directories }),
      spill: runtime.request.spill,
      queryId: runtime.request.queryId,
      targetBytes: admitted?.targetBytes ?? batchTargetFor(runtime.governor.capBytes, 4),
      signal: runtime.signal,
      onPipeline: () => runtime.pipeline(),
      orderedShortcut: true,
    },
    sink,
  );
  runtime.onAbort((reason) => fetch.abort(reason));
  runtime.dispose(() => fetch.closed);
  runtime.signal.throwIfAborted();
  const planned = admitted?.plan ?? legacy;
  const cursorGrant = planned ? grants.at(-1) : undefined;
  if (planned && !cursorGrant) throw new Error("Missing admitted cursor budget");
  const cursor =
    planned && cursorGrant
      ? planned.open({ budget: cursorGrant, signal: runtime.signal })
      : runtime.view
          .index(shape.tableId, indexId)
          .range({ lower: null, upper: null }, shape.descending ? "desc" : "asc");
  const source = new IndexOrderSource(
    cursor,
    vector,
    runtime.signal,
    limit ?? Number.MAX_SAFE_INTEGER,
    offset,
    demand,
    stopped,
  );
  runtime.onAbort(() => source.abort());
  runtime.dispose(() => source.closed);
  runtime.pipeline();
  try {
    await drive(source, fetch, runtime.signal);
  } finally {
    source.abort();
    await source.closed;
  }
  const sourceProfile = runtime.profile(shape.source),
    fetched = runtime.profile(shape.fetch),
    restored = runtime.profile(shape.restore);
  sourceProfile.rowsOut = source.rowsOut;
  sourceProfile.batchesOut = source.batchesOut;
  fetched.rowsOut = restored.rowsOut = fetch.rowsOut;
  fetched.batchesOut = restored.batchesOut = fetch.batchesOut;
  fetched.pagesRead = fetch.pagesRead;
  restored.spilledBytes = fetch.spilledBytes;
  restored.spillPasses = fetch.spillPasses;
}

/** SQL lowering currently retains a sort. Match only the exact direct PK-order shape;
 * the immutable pinned catalog provides the index definition, with a generic recipe retained. */
export function orderedSort(plan: PhysicalPlan): {
  rel: Extract<Rel, { op: "sort" }>;
  scan: Extract<Rel, { op: "scan" }>;
  project: Extract<Rel, { op: "project" }>;
  projects: ReadonlyArray<Extract<Rel, { op: "project" }>>;
  columns: readonly number[];
  keys: readonly number[];
} | null {
  if (plan.hasRowLocking) return null;
  let root = plan.root;
  const trims: Array<Extract<Rel, { op: "project" }>> = [];
  while (root.op === "project") {
    trims.push(root);
    root = root.input;
  }
  const rel = root;
  if (
    rel.op !== "sort" ||
    rel.withTies ||
    (rel.limit === null && rel.offset === null) ||
    rel.keys.length === 0
  )
    return null;
  const project = rel.input;
  if (project.op !== "project" || project.input.op !== "scan") return null;
  const scan = project.input;
  if (scan.filter !== null || scan.emitLocator || scan.runtimeFilters.length !== 0) return null;
  const direct = (
    p: Extract<Rel, { op: "project" }>,
    columns: readonly number[],
    fields: Rel["output"],
  ) => {
    if (p.exprs.length !== p.output.length) return null;
    const result: number[] = [];
    for (const expr of p.exprs) {
      if (expr.op !== "column") return null;
      const field = fields[expr.index];
      if (!field || !samePhysicalType(expr.type, field.type)) return null;
      const column = columns[expr.index];
      if (column === undefined) return null;
      result.push(column);
    }
    return result;
  };
  let columns = direct(project, scan.columns, scan.output);
  if (columns === null) return null;
  const keys: number[] = [];
  for (const key of rel.keys) {
    if (
      key.expr.op !== "column" ||
      !isByteCollation(key.collation) ||
      key.direction !== rel.keys[0]?.direction
    )
      return null;
    const column = columns[key.expr.index];
    if (column === undefined) return null;
    keys.push(column);
  }
  let fields = project.output;
  for (let i = trims.length - 1; i >= 0; i--) {
    const trim = trims[i];
    if (!trim) throw new Error("projection chain");
    columns = direct(trim, columns, fields);
    if (columns === null) return null;
    fields = trim.output;
  }
  return { rel, scan, project: trims[0] ?? project, projects: [project, ...trims], columns, keys };
}
