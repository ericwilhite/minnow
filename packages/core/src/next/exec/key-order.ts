/** Ordered index cursors and private locator fetches share the query's pinned view. */
import type { IndexCursor, IndexReader, IndexRangePlan } from "../access/key-index.js";
import type { ReadBound } from "../page/read-bounds.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import { BATCH_ROWS, type FlatVector } from "../vector/vector.js";
import { batchTargetFor, OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import { pauseIfDue } from "../task.js";
import { samePhysicalType, type PhysicalType } from "../types.js";
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
}
export function indexMinimums(shape: IndexShape, cap: number) {
  return [
    { rel: shape.source, minimum: BATCH_ROWS * 32 + 4096 },
    {
      rel: shape.fetch,
      minimum:
        batchTargetFor(cap, 4) +
        memoryScanBufferBytes(shape.types) +
        BATCH_ROWS * (80 + 8 * OBJECT_OVERHEAD_BYTES) +
        OBJECT_OVERHEAD_BYTES * (16 + 2 * shape.types.length),
    },
    { rel: shape.restore, minimum: sortMinimumBytes([...shape.types, ORDINAL_TYPE]) },
  ];
}

export interface AdmittedIndexRead {
  readonly plan: IndexRangePlan;
  readonly cursor: OperatorIdentity;
  readonly payload: ReadBound;
}
export function admittedIndexMinimums(shape: IndexShape, cap: number, read: AdmittedIndexRead) {
  return [
    ...indexMinimums(shape, cap).map((entry) => ({
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
  const entries = admitted
    ? admittedIndexMinimums(shape, runtime.request.governor.capBytes, admitted)
    : indexMinimums(shape, runtime.request.governor.capBytes);
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
  const [sourceGrant, fetchGrant, restoreGrant] = grants;
  if (!sourceGrant || !fetchGrant || !restoreGrant) throw new Error("index admissions");
  const arena = new OutputArena(sourceGrant, BATCH_ROWS),
    fetchArena = new OutputArena(fetchGrant, BATCH_ROWS);
  runtime.dispose(() => arena.close());
  runtime.dispose(() => fetchArena.close());
  const vector = arena.flat({ kind: "locator", tableId: shape.tableId }).vector;
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
      spill: runtime.request.spill,
      queryId: runtime.request.queryId,
      targetBytes: batchTargetFor(runtime.request.governor.capBytes, 4),
      signal: runtime.signal,
      onPipeline: () => runtime.pipeline(),
      orderedShortcut: true,
    },
    sink,
  );
  runtime.onAbort((reason) => fetch.abort(reason));
  runtime.dispose(() => fetch.closed);
  runtime.signal.throwIfAborted();
  const cursorGrant = grants[3];
  if (admitted && !cursorGrant) throw new Error("Missing admitted cursor budget");
  const cursor =
    admitted && cursorGrant
      ? admitted.plan.open({ budget: cursorGrant, signal: runtime.signal })
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
