/** Bounded modification-input spool: retire read reservations before the writer can wait. */
import type { Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import type { SpillService } from "../spill/contract.js";
import { ColumnStore } from "./column-store.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import { RecursiveTape } from "./recursive-tape.js";
import { sortRowView, SORT_ROW_VIEW_BYTES } from "./sort-row.js";
import { pushAndWait, type Operator, type Transfer } from "./pipeline.js";

export const modifySpoolBytes = (columns: number): number => 262144 + columns * 8192;
export class ModifySpool implements Operator {
  readonly #pool: Pool;
  readonly #files: SpillFiles;
  readonly #pages: PageWriter;
  readonly #io: SpillIo;
  readonly #tape: RecursiveTape;
  readonly #store: ColumnStore;
  readonly #views: number;
  #pending: Promise<void> = Promise.resolve();
  #closing: Promise<void> | null = null;
  #sealed = false;
  readonly #ownsPool: boolean;
  #readMultiplier = 1;
  constructor(
    readonly types: readonly PhysicalType[],
    grant: Grant,
    spill: SpillService,
    queryId: number,
    readonly signal: AbortSignal,
    sharedPool?: Pool,
    readMultiplier = 1,
  ) {
    const charge = new Charge(grant);
    this.#pool = sharedPool ?? new Pool(charge, modifySpoolBytes(types.length));
    this.#ownsPool = sharedPool === undefined;
    this.#readMultiplier = readMultiplier;
    this.#files = new SpillFiles(spill, queryId, charge);
    this.#pages = new PageWriter(this.#pool, 16384);
    this.#io = new SpillIo(this.#pool, this.#files, this.#pages, { grant, queryId, signal }, () =>
      signal.throwIfAborted(),
    );
    this.#tape = new RecursiveTape(this.#io, 0);
    this.#store = new ColumnStore(types, this.#pool, { chunkRows: 256, targetBytes: 65536 });
    this.#views = types.length * SORT_ROW_VIEW_BYTES;
    if (!this.#pool.allocate(this.#views))
      throw new RangeError("Modification input views were not admitted");
  }
  push(batch: Batch, transfer?: Transfer): "blocked" {
    this.#pending = this.#append(batch).finally(() => transfer?.release());
    return "blocked";
  }
  ready(): Promise<void> {
    return this.#pending;
  }
  async #flush(): Promise<void> {
    if (this.#store.rows === 0) return;
    const oversized = PageWriter.encodedBytes(this.#store.vectors(), this.#store.rows) > 65536;
    await this.#tape.append(this.#store.vectors(), this.#store.rows);
    this.#io.reset(this.#store);
    this.#pages.shrinkIf(oversized || this.#pool.owes);
  }
  async #append(batch: Batch): Promise<void> {
    if (this.#sealed) throw new TypeError("Modification input spool is sealed");
    const count = batch.selection?.count ?? batch.length;
    let at = 0;
    while (at < count) {
      this.signal.throwIfAborted();
      const copied = this.#store.append(
        batch.columns,
        batch.selection?.rows ?? null,
        at,
        count - at,
      );
      at += copied;
      if (this.#store.rows === 1 && PageWriter.encodedBytes(this.#store.vectors(), 1) > 65536)
        await this.#flush();
      if (at === count) break;
      await this.#flush();
      const row = batch.selection?.rows[at] ?? at;
      if (this.#store.appendNeed(batch.columns, row) > 65536) {
        const views = batch.columns.map((column) => sortRowView(column, row));
        // Preflight the complete encode step before borrowing: a wide row gets one loan.
        await this.#io.need(2 * PageWriter.encodedBytes(views, 1) + 65536);
        await this.#tape.append(views, 1);
        this.#pages.shrinkIf(true);
        at++;
      } else if (copied === 0) {
        // A denied small row must obtain a complete step before retry, never spin.
        await this.#io.need(2 * this.#store.appendNeed(batch.columns, row) + 65536);
      }
    }
  }
  async finish(): Promise<void> {
    await this.#pending;
    await this.#flush();
    this.#store.close();
    this.#pages.shrinkIf(true);
    this.#sealed = true;
  }
  async drain(sink: Operator): Promise<void> {
    if (!this.#sealed) throw new TypeError("Modification input spool is not sealed");
    this.#pending = this.#tape.each(
      this.types,
      async (columns, length) => {
        await pushAndWait(sink, { columns, length, selection: null });
        return true;
      },
      this.#readMultiplier === 1
        ? undefined
        : (bytes) => this.#io.need(Math.ceil(this.#readMultiplier * bytes)),
    );
    await this.#pending;
    await sink.finish();
  }
  abort(): void {
    /* The execution aborts signal; close fences outstanding spill I/O. */
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      await this.#pending.catch(() => undefined);
      try {
        await this.#tape.close();
        await this.#files.closeAll();
      } finally {
        this.#store.close();
        this.#pages.close();
        this.#pool.free(this.#views);
        if (this.#ownsPool) this.#pool.close();
      }
    })());
  }
}
