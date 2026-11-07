/** Private late-sort fetch: unique, non-null locators produced by one table scan.
 * This is a pipeline stage, not the general RowFetcher/IR fetch API. Read in physical order,
 * then restore the narrow sort's order using a charged, spillable ordinal sort of survivors.
 */
import type {
  DeletionVector,
  GroupSegment,
  MemorySegment,
  Segment,
  Snapshot,
  TableSnapshot,
} from "../access/snapshot.js";
import { SnapshotReleasedError } from "../access/snapshot.js";
import { OBJECT_OVERHEAD_BYTES, type Grant } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import { sortInSlices } from "../task.js";
import type { PhysicalType } from "../types.js";
import { locatorPosition, locatorSegment } from "../vector/locator.js";
import type { Batch, Selection, Vector } from "../vector/vector.js";
import type { CompileContext } from "./kernels/compile.js";
import { isValidSlot, slotOf } from "./kernels/vectors.js";
import { drive, type Operator, type PushResult, type Transfer } from "./pipeline.js";
import { reclaimingReader } from "./reclaim-reader.js";
import type { ChunkReads } from "./scan.js";
import { SortOperator } from "./sort.js";
import { TableScan } from "./table-scan.js";

export const ORDINAL_TYPE: PhysicalType = { kind: "float64" };

function lowerBound(rows: Uint16Array, position: number): number {
  let lo = 0,
    hi = rows.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if ((rows[mid] ?? 0) < position) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** A private scan view selects exactly the requested stored positions, including deleted rows:
 * a valid locator names a physical row, independently of its current deletion bitmap. */
class Picks implements DeletionVector {
  readonly deletedCount: number;
  constructor(
    readonly rows: Uint16Array,
    total: number,
  ) {
    this.deletedCount = total - rows.length;
  }
  isDeleted(position: number): boolean {
    return this.rows[lowerBound(this.rows, position)] !== position;
  }
  liveRows(from: number, length: number, input: Selection | null, out: Selection): void {
    let count = 0;
    if (input === null) {
      for (let k = lowerBound(this.rows, from); k < this.rows.length; k++) {
        const row = this.rows[k] ?? 0;
        if (row >= from + length) break;
        out.rows[count++] = row - from;
      }
    } else
      for (let k = 0; k < input.count; k++) {
        const row = input.rows[k] ?? 0;
        if (!this.isDeleted(from + row)) out.rows[count++] = row;
      }
    out.count = count;
  }
}

class SelectedGroup implements GroupSegment {
  readonly kind = "group";
  readonly liveRowCount: number;
  readonly mask: Picks;
  constructor(
    readonly source: GroupSegment,
    readonly index: number,
    rows: Uint16Array,
  ) {
    this.liveRowCount = rows.length;
    this.mask = new Picks(rows, source.rowCount);
  }
  get rowCount() {
    return this.source.rowCount;
  }
  get group() {
    return this.source.group;
  }
  get staged() {
    return this.source.staged;
  }
  root() {
    return this.source.root();
  }
  chunk(column: number) {
    return this.source.chunk(column);
  }
  deletions() {
    return this.mask;
  }
  statistics(column: number) {
    return this.source.statistics(column);
  }
  pages(column: number) {
    return this.source.pages(column);
  }
}

class SelectedMemory implements MemorySegment {
  readonly kind = "memory";
  readonly liveRowCount: number;
  readonly mask: Picks;
  constructor(
    readonly source: MemorySegment,
    readonly index: number,
    rows: Uint16Array,
  ) {
    this.liveRowCount = rows.length;
    this.mask = new Picks(rows, source.rowCount);
  }
  get rowCount() {
    return this.source.rowCount;
  }
  chunk(...args: Parameters<MemorySegment["chunk"]>) {
    return this.source.chunk(...args);
  }
  deletions() {
    return this.mask;
  }
  statistics(column: number) {
    return this.source.statistics(column);
  }
  pages(column: number) {
    return this.source.pages(column);
  }
}

/** Inner sorts return their buffers/loans; the execution owns the reusable outer account. */
function scopedGrant(grant: Grant): Grant {
  return {
    get label() {
      return grant.label;
    },
    get spillable() {
      return grant.spillable;
    },
    get held() {
      return grant.held;
    },
    get allocated() {
      return grant.allocated;
    },
    get loaned() {
      return grant.loaned;
    },
    allocate: (n) => grant.allocate(n),
    free: (n) => grant.free(n),
    tryGrow: (n) => grant.tryGrow(n),
    release: (n) => grant.release(n),
    makeRoom: (n, signal) => grant.makeRoom?.(n, signal) ?? Promise.resolve(false),
    borrow: (n, signal) => grant.borrow(n, signal),
    onReclaimRequest: (callback) => grant.onReclaimRequest(callback),
    close() {
      const unused = grant.held - grant.allocated - grant.loaned;
      if (unused > 0) grant.release(unused);
    },
  };
}

export interface LateFetchContext {
  readonly snapshot: Snapshot;
  readonly tableId: number;
  readonly columns: readonly number[];
  readonly types: readonly PhysicalType[];
  readonly locator: number;
  readonly compile: CompileContext;
  readonly grant: Grant;
  readonly restoreGrant: Grant;
  readonly reads: ChunkReads;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly targetBytes: number;
  readonly signal: AbortSignal;
  readonly onPipeline: () => void;
}

export class LateFetchOperator implements Operator {
  readonly #abort = new AbortController();
  readonly #ordinal: ReturnType<CompileContext["arena"]["flat"]>;
  readonly #inputColumns: Vector[];
  readonly #outputColumns: Vector[];
  #table: TableSnapshot | null = null;
  #scan: TableScan | null = null;
  #sort: SortOperator | null = null;
  #pending: Promise<void> | null = null;
  #transfer: Transfer | undefined;
  #aborted = false;
  rowsOut = 0;
  batchesOut = 0;
  pagesRead = 0;
  spilledBytes = 0;
  spillPasses = 0;
  constructor(
    readonly context: LateFetchContext,
    readonly downstream: Operator,
  ) {
    this.#ordinal = context.compile.arena.flat(ORDINAL_TYPE);
    context.compile.arena.take(OBJECT_OVERHEAD_BYTES * (4 + 2 * context.types.length));
    this.#inputColumns = new Array<Vector>(context.types.length + 1);
    this.#outputColumns = new Array<Vector>(context.types.length);
  }
  get closed(): Promise<void> {
    return (
      this.#pending?.then(
        () => undefined,
        () => undefined,
      ) ?? Promise.resolve()
    );
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    if (this.#pending !== null) throw new TypeError("fetch push before readiness");
    this.#transfer = transfer;
    this.#pending = this.#read(batch);
    void this.#pending.catch(() => undefined);
    return "blocked";
  }
  async ready(): Promise<void> {
    try {
      await this.#pending;
    } finally {
      this.#pending = null;
      this.#transfer?.release();
      this.#transfer = undefined;
    }
  }
  finish() {
    this.#check();
    return this.downstream.finish();
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#abort.abort(reason);
    this.#scan?.abort();
    this.#sort?.abort(reason);
    this.#transfer?.release();
    this.#transfer = undefined;
    this.downstream.abort(reason);
  }
  #check(): void {
    this.context.signal.throwIfAborted();
    this.#abort.signal.throwIfAborted();
    if (this.context.snapshot.released) throw new SnapshotReleasedError();
  }
  async #read(batch: Batch): Promise<void> {
    this.#check();
    const count = batch.selection?.count ?? batch.length;
    if (count === 0) return;
    const { context } = this;
    const scratch = count * (32 + 8 * OBJECT_OVERHEAD_BYTES) + 1024;
    context.compile.arena.take(scratch);
    try {
      const locators = new Float64Array(count),
        order = new Array<number>(count);
      const vector = batch.columns[context.locator];
      if (!vector) throw new TypeError("missing late-sort locator");
      for (let k = 0; k < count; k++) {
        const row = batch.selection?.rows[k] ?? k;
        if (!isValidSlot(vector, row)) throw new TypeError("a late-sort locator cannot be null");
        const [flat, slot] = slotOf(vector, row);
        if (flat.data.layout !== "f64") throw new TypeError("locator layout");
        const value = flat.data.values[slot] ?? -1;
        if (!Number.isSafeInteger(value) || value < 0) throw new RangeError("invalid locator");
        locators[k] = value;
        order[k] = k;
      }
      await sortInSlices(order, (a, b) => (locators[a] ?? 0) - (locators[b] ?? 0), {
        signal: this.#abort.signal,
      });
      this.#check();
      this.#table ??= await context.snapshot.table(context.tableId, { budget: context.grant });
      const segments: Segment[] = [];
      for (let from = 0; from < count;) {
        const first = locators[order[from] ?? 0] ?? -1,
          index = locatorSegment(first);
        const source = this.#table.segments[index];
        if (source?.index !== index) throw new RangeError("locator segment");
        let to = from + 1;
        while (to < count && locatorSegment(locators[order[to] ?? 0] ?? -1) === index) to++;
        const positions = new Uint16Array(to - from);
        for (let k = from; k < to; k++) {
          const value = locators[order[k] ?? 0] ?? -1,
            position = locatorPosition(value);
          if (position >= source.rowCount || (k > 0 && value === locators[order[k - 1] ?? 0]))
            throw new RangeError("late-sort locators must name unique visible scan rows");
          positions[k - from] = position;
        }
        // This is a private scan view, never a producer/interpreter of original locators.
        // Reindex it to preserve segments[i].index === i; emitLocator stays false.
        segments.push(
          source.kind === "group"
            ? new SelectedGroup(source, segments.length, positions)
            : new SelectedMemory(source, segments.length, positions),
        );
        from = to;
      }
      const table: TableSnapshot = {
        tableId: this.#table.tableId,
        schemaVersion: this.#table.schemaVersion,
        columns: this.#table.columns,
        liveRowCount: count,
        segments,
      };
      const output: Operator = {
        push: (fetched) => {
          this.rowsOut += fetched.selection?.count ?? fetched.length;
          this.batchesOut++;
          for (let c = 0; c < context.types.length; c++) {
            const column = fetched.columns[c];
            if (!column) throw new TypeError("missing fetched column");
            this.#outputColumns[c] = column;
          }
          return this.downstream.push({
            length: fetched.length,
            columns: this.#outputColumns,
            selection: fetched.selection,
          });
        },
        ready: () => this.downstream.ready(),
        finish: () => undefined,
        abort: (reason) => this.downstream.abort(reason),
      };
      const sort = new SortOperator(
        {
          types: [...context.types, ORDINAL_TYPE],
          keys: [
            { column: context.types.length, direction: "asc", nulls: "last", collation: null },
          ],
          limit: null,
          offset: 0,
          withTies: false,
        },
        {
          grant: scopedGrant(context.restoreGrant),
          spill: context.spill,
          queryId: context.queryId,
          signal: this.#abort.signal,
        },
        output,
      );
      this.#sort = sort;
      const scan = new TableScan({
        table,
        columns: context.columns,
        filter: null,
        emitLocator: false,
        pages: reclaimingReader(context.snapshot.pages, context.grant, this.#abort.signal),
        reads: context.reads,
        budget: context.grant,
        context: { ...context.compile, input: context.types },
        targetBytes: context.targetBytes,
        loans: { borrow: (n) => context.grant.borrow(n, this.#abort.signal) },
      });
      this.#scan = scan;
      let at = 0;
      const ordered: Operator = {
        push: (fetched, transfer) => {
          const ordinal = this.#ordinal.vector;
          if (ordinal.data.layout !== "f64") throw new TypeError("ordinal layout");
          for (let c = 0; c < context.types.length; c++) {
            const column = fetched.columns[c];
            if (!column) throw new TypeError("missing fetched column");
            this.#inputColumns[c] = column;
          }
          const n = fetched.selection?.count ?? fetched.length;
          for (let k = 0; k < n; k++)
            ordinal.data.values[fetched.selection?.rows[k] ?? k] = order[at++] ?? -1;
          ordinal.length = fetched.length;
          this.#inputColumns[context.types.length] = ordinal;
          return sort.push(
            { length: fetched.length, columns: this.#inputColumns, selection: fetched.selection },
            transfer,
          );
        },
        ready: () => sort.ready(),
        finish: () => {
          if (at !== count) throw new RangeError("fetch omitted requested rows");
          return sort.finish();
        },
        abort: (reason) => sort.abort(reason),
      };
      context.onPipeline();
      await drive(scan, ordered, this.#abort.signal);
    } catch (error) {
      this.#scan?.abort();
      this.#sort?.abort(error);
      throw error;
    } finally {
      const scan = this.#scan,
        sort = this.#sort;
      if (scan !== null) {
        scan.close();
        this.pagesRead += scan.stats.pagesRead;
      }
      if (sort !== null) {
        const profile = sort.profile;
        this.spilledBytes += profile.spilledBytes;
        this.spillPasses += profile.mergePasses;
        await sort.closed;
      }
      this.#scan = null;
      this.#sort = null;
      context.compile.arena.give(scratch);
    }
  }
}
