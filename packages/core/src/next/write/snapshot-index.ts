/** Bounded native index reconstruction. Rows are never collected: one scalar row, a small
 * sorted delta, and online merge levels. FK scratch is private until footer validation. */
import type { CatalogView, ForeignKeyDef, IndexDef, TableDef } from "../access/catalog.js";
import {
  ReadDeniedError,
  type ByteBudget,
  type ExtentReader,
  type ExtentWriter,
} from "../storage/extents.js";
import type { IndexRoot, RunRef } from "../format/manifest.js";
import { SnapshotBudgetError, type SnapshotWork } from "../format/snapshot.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { compareBytes } from "../vector/varlen.js";
import type { FlatVector } from "../vector/vector.js";
import { snapshotFlat, fillSnapshotFlat } from "./snapshot-native.js";
import {
  SnapshotAccount,
  SnapshotColumnBytes,
  snapshotCheck,
  snapshotPace,
  type SnapshotStagedColumn,
} from "./snapshot-stage.js";
import { KeyDelta, type DeltaVisitor } from "./key-hash.js";
import { indexLimitViolation, indexMaximumKeyBytes } from "./index-limits.js";
import { RunBuilder, type RunSummary } from "./runs.js";
import { RunSink } from "./run-output.js";
import { mergeSortedRuns, runExtent, DEFAULT_MERGE_POLICY } from "./merge.js";
import { RunRangeInput, rangeRunBytes } from "./range-input.js";
import { runKeyBytes } from "./overflow-keys.js";

const DELTA_BYTES = 256 * 1024;
const cmp = (a: Uint8Array, b: Uint8Array) => compareBytes(a, 0, a.length, b, 0, b.length);
interface OwnedRuns {
  readonly runs: readonly RunRef[];
  release(): void;
}
/** Borrowed head survives only until advance. Directory/block/key work is paid before open. */
async function* entries(
  runs: readonly RunRef[],
  space: ExtentReader,
  budget: ByteBudget,
  work: SnapshotWork,
): AsyncGenerator<Uint8Array> {
  const account = new SnapshotAccount(budget);
  account.take(1024);
  try {
    const signal = work.signal ?? new AbortController().signal;
    for (const ref of runs) {
      const bytes = rangeRunBytes(ref, 1);
      account.take(bytes);
      let cursor: RunRangeInput | undefined;
      try {
        cursor = new RunRangeInput(
          space,
          budget,
          ref,
          { lower: null, upper: null },
          1,
          Infinity,
          signal,
        );
        await cursor.open();
        for (;;) {
          await snapshotPace(work);
          const more = await cursor.advance();
          signal.throwIfAborted();
          if (!more) break;
          snapshotCheck(!cursor.tombstone, "Tombstone in rebuilt snapshot index");
          yield cursor.key();
        }
      } finally {
        cursor?.close();
        account.free(bytes);
      }
    }
  } finally {
    account.close();
  }
}
async function unique(
  runs: readonly RunRef[],
  space: ExtentReader,
  budget: ByteBudget,
  work: SnapshotWork,
): Promise<void> {
  const account = new SnapshotAccount(budget);
  let maximum = 0;
  for (const ref of runs) maximum = Math.max(maximum, runKeyBytes(ref));
  account.take(maximum + 512);
  try {
    const previous = new Uint8Array(maximum);
    let length = -1;
    for await (const key of entries(runs, space, budget, work)) {
      snapshotCheck(key.length <= maximum, "Rebuilt key exceeds its complete bound");
      if (length >= 0)
        snapshotCheck(
          compareBytes(previous, 0, length, key, 0, key.length) < 0,
          "Duplicate imported UNIQUE/PRIMARY key",
        );
      previous.set(key);
      length = key.length;
    }
  } finally {
    account.close();
  }
}
async function foreign(
  child: readonly RunRef[],
  parent: readonly RunRef[],
  space: ExtentReader,
  budget: ByteBudget,
  work: SnapshotWork,
): Promise<void> {
  const children = entries(child, space, budget, work),
    parents = entries(parent, space, budget, work);
  try {
    let head = await parents.next();
    for await (const key of children) {
      while (!head.done && cmp(head.value, key) < 0) head = await parents.next();
      snapshotCheck(!head.done && cmp(head.value, key) === 0, "Imported foreign key has no parent");
    }
  } finally {
    await children.return(undefined);
    await parents.return(undefined);
  }
}

class NativeIndex {
  readonly #account: SnapshotAccount;
  readonly #owned: Set<OwnedRuns>;
  readonly #levels: Array<OwnedRuns | undefined>;
  #finished: IndexRoot | undefined;
  constructor(
    readonly def: IndexDef,
    readonly table: TableDef,
    readonly space: ExtentReader,
    readonly writer: ExtentWriter,
    readonly budget: ByteBudget,
    readonly work: SnapshotWork,
  ) {
    this.#account = new SnapshotAccount(budget);
    this.#account.take(8192);
    this.#owned = new Set();
    this.#levels = [];
  }
  #keep(runs: readonly RunRef[], release: () => void): OwnedRuns {
    const owned = { runs, release };
    this.#owned.add(owned);
    return owned;
  }
  #release(owned: OwnedRuns): void {
    if (!this.#owned.delete(owned)) return;
    owned.release();
    this.#account.free(512);
  }
  async #merge(a: OwnedRuns, b: OwnedRuns, level: number): Promise<OwnedRuns> {
    this.#account.take(512);
    let output: Awaited<ReturnType<typeof mergeSortedRuns>> | undefined,
      adopted = false;
    try {
      output = await mergeSortedRuns({
        space: this.space,
        writer: this.writer,
        grant: this.budget,
        outputBudget: this.budget,
        runs: [a.runs, b.runs],
        level,
        bottom: true,
        policy: { ...DEFAULT_MERGE_POLICY, splitBytes: 64 * 1024 },
        stopped: () => this.work.signal?.aborted === true,
        ...(this.work.signal ? { signal: this.work.signal } : {}),
      });
      const completed = output;
      const owned = this.#keep(completed.runs, () => completed.release());
      adopted = true;
      for (const ref of a.runs) await this.writer.discard(runExtent(ref));
      for (const ref of b.runs) await this.writer.discard(runExtent(ref));
      this.#release(a);
      this.#release(b);
      return owned;
    } finally {
      if (!adopted) {
        output?.release();
        this.#account.free(512);
      }
    }
  }
  async #accept(incoming: OwnedRuns): Promise<void> {
    let next = incoming;
    for (let level = 0; ; level++) {
      const prior = this.#levels[level];
      if (prior === undefined) {
        this.#levels[level] = next;
        return;
      }
      next = await this.#merge(prior, next, level + 1);
      this.#levels[level] = undefined;
    }
  }
  async #flush(delta: KeyDelta): Promise<void> {
    if (delta.keyCount === 0) return;
    this.#account.take(512);
    const summaries: RunSummary[] = [];
    let builder: RunBuilder | undefined,
      adopted = false;
    try {
      builder = new RunBuilder(false, false, this.budget, 2);
      const encoded = builder,
        sink = new RunSink(encoded, this.writer, Math.max(65536, delta.chargedBytes));
      const visit: DeltaVisitor = (arena, from, to, rowId, tombstone, hash, bloomHash) =>
        encoded.addRange(arena, from, to, rowId, tombstone, hash, bloomHash);
      visit.flush = () => sink.drain();
      await delta.forEachSorted(
        Number.MAX_SAFE_INTEGER,
        visit,
        null,
        () => this.work.signal?.throwIfAborted(),
        this.budget,
      );
      const ref = await sink.finish(summaries);
      snapshotCheck(ref !== null, "Empty rebuilt nonempty index delta");
      const owned = this.#keep([ref], () => {
        for (const summary of summaries) summary.release();
      });
      adopted = true;
      await this.#accept(owned);
    } finally {
      builder?.close();
      if (!adopted) {
        for (const summary of summaries) summary.release();
        this.#account.free(512);
      }
    }
  }
  async append(columns: readonly SnapshotStagedColumn[], firstRowId: number): Promise<void> {
    if (this.def.invalid === true) return;
    const working = new SnapshotAccount(this.budget);
    let keys: KeyBatch | undefined, delta: KeyDelta | undefined;
    const sources: SnapshotColumnBytes[] = [];
    // At most four charged staging-part leases, regardless of index width. Keeping a
    // small lease window also avoids reopening a single-column part for every scalar.
    const held: SnapshotColumnBytes[] = [];
    try {
      working.take(65536 + this.def.columnIds.length * 512 + this.table.columns.length * 16);
      const selected = this.def.columnIds.map((id) => {
        const column = columns.find((c) => c.column.id === id);
        snapshotCheck(column !== undefined, "Missing imported index column");
        return column;
      });
      const rows = selected[0]?.rows ?? 0,
        valid = new Uint8Array(rows),
        maxima = new Uint32Array(selected.length),
        record = new Uint8Array(64);
      for (let row = 0; row < rows; row++) {
        const nullKey = selected.some((column) => column.lengths[row] === 0xffffffff);
        snapshotCheck(
          !(nullKey && this.def.unique && this.def.role === "primary"),
          "NULL imported PRIMARY key",
        );
        if (nullKey) continue;
        valid[row] = 1;
        for (let c = 0; c < selected.length; c++) {
          const length = selected[c]?.lengths[row] ?? 0;
          snapshotCheck(
            length <= 65536,
            "Imported index component exceeds native scalar key bound",
          );
          maxima[c] = Math.max(maxima[c] ?? 0, length);
        }
      }
      let scalarBytes = 0;
      for (const maximum of maxima) scalarBytes += maximum;
      working.take(scalarBytes + selected.length * 4096);
      const flats = selected.map((column, c) =>
        snapshotFlat(column.column.type, 1, maxima[c] ?? 0),
      );
      for (const column of selected) sources.push(new SnapshotColumnBytes(column, this.space));
      // Sparse carrier is in table-column order; key policy only reads selected positions.
      const vectors: FlatVector[] = new Array<FlatVector>(this.table.columns.length);
      for (let c = 0; c < selected.length; c++) {
        const flat = flats[c];
        snapshotCheck(flat !== undefined, "Missing index vector");
        vectors[this.table.columns.findIndex((column) => column.id === selected[c]?.column.id)] =
          flat;
      }
      const keyColumns = flats.map((vector) => ({
        vector,
        direction: "asc" as const,
        nulls: "first" as const,
      }));
      const initial = Math.max(16, indexMaximumKeyBytes(this.def, this.table));
      // KeyBatch's bounded constructor has boolean minimum refusals. Prove the complete
      // constructor step before retaining it; no await can consume this headroom in between.
      working.take(initial + 1024);
      working.free(initial + 1024);
      keys = new KeyBatch(this.budget, Math.max(DELTA_BYTES, initial), initial);
      for (let row = 0; row < rows; row++) {
        await snapshotPace(this.work);
        if (valid[row] !== 1) continue;
        for (let c = 0; c < selected.length; c++) {
          const column = selected[c],
            flat = flats[c],
            source = sources[c];
          snapshotCheck(
            column !== undefined && flat !== undefined && source !== undefined,
            "Missing scalar index workspace",
          );
          flat.validity?.fill(0);
          const prior = held.indexOf(source);
          if (prior >= 0) held.splice(prior, 1);
          else if (held.length === 4) held.shift()?.close();
          held.push(source);
          try {
            await fillSnapshotFlat(column, source, row, row + 1, flat, record);
          } catch (error) {
            if (!(error instanceof ReadDeniedError) || held.length === 1) throw error;
            // No admission wait: drop reconstructible leases, then retry this scalar.
            // A remaining mandatory denial unwinds the entire load through its replay path.
            for (const other of held) other.close();
            held.length = 0;
            held.push(source);
            flat.validity?.fill(0);
            await fillSnapshotFlat(column, source, row, row + 1, flat, record);
          }
        }
        snapshotCheck(
          indexLimitViolation(this.def, this.table, vectors, null, 0, 1) === null,
          "Imported key exceeds its declared native index policy",
        );
        const encoded = encodeKeyBatch(
          "equality",
          keyColumns,
          { length: 1, selection: null },
          keys,
        );
        if (!encoded.done && encoded.reason === "denied")
          throw new SnapshotBudgetError(encoded.requiredBytes);
        snapshotCheck(
          encoded.done && encoded.encoded === 1,
          "No progress encoding imported index key",
        );
        delta ??= new KeyDelta(this.budget);
        delta.add(
          keys.writer.bytes,
          keys.offsets[0] ?? 0,
          keys.offsets[1] ?? 0,
          firstRowId + row,
          1,
          false,
        );
        if (delta.chargedBytes >= DELTA_BYTES) {
          await this.#flush(delta);
          delta.releaseCharge();
          delta = undefined;
        }
      }
      if (delta !== undefined) await this.#flush(delta);
    } finally {
      for (const source of sources) source.close();
      delta?.releaseCharge();
      keys?.close();
      working.close();
    }
  }
  async finish(): Promise<IndexRoot> {
    if (this.#finished !== undefined) return this.#finished;
    let combined: OwnedRuns | undefined;
    for (let level = 0; level < this.#levels.length; level++) {
      const next = this.#levels[level];
      this.#levels[level] = undefined;
      if (next !== undefined)
        combined = combined === undefined ? next : await this.#merge(combined, next, level + 1);
    }
    const runs = combined?.runs ?? [];
    if (this.def.unique) await unique(runs, this.space, this.budget, this.work);
    return (this.#finished = {
      indexId: this.def.id,
      columnIds: this.def.columnIds,
      unique: this.def.unique,
      runs,
    });
  }
  close(): void {
    for (const owned of this.#owned) this.#release(owned);
    this.#account.close();
  }
}
interface TableIndexes {
  readonly table: TableDef;
  readonly indexes: NativeIndex[];
  readonly foreign: Array<{ def: ForeignKeyDef; builder: NativeIndex }>;
}
export class SnapshotIndexes {
  readonly #account: SnapshotAccount;
  readonly #tables: Map<number, TableIndexes>;
  readonly #ready: Map<number, IndexRoot>;
  readonly #space: ExtentReader;
  constructor(
    catalog: CatalogView,
    space: ExtentReader,
    writer: ExtentWriter,
    scratch: ExtentWriter,
    readonly budget: ByteBudget,
    readonly work: SnapshotWork,
  ) {
    this.#account = new SnapshotAccount(budget);
    let count = 0;
    for (const table of catalog.tables) count += table.indexes.length + table.foreignKeys.length;
    this.#account.take(4096 + catalog.tables.length * 512 + count * 1024);
    this.#tables = new Map();
    this.#ready = new Map();
    // Existing hits remain usable; new private import reads never populate the shared cache.
    this.#space = {
      peek: (ref) => space.peek(ref),
      read: (refs, options) => space.read(refs, { ...options, retain: false }),
    };
    try {
      for (const table of catalog.tables) {
        const entry: TableIndexes = { table, indexes: [], foreign: [] };
        this.#tables.set(table.id, entry);
        for (const def of table.indexes)
          entry.indexes.push(new NativeIndex(def, table, this.#space, writer, budget, work));
        for (const def of table.foreignKeys)
          if (def.enforced) {
            const parent = catalog
              .table(def.parentTableId)
              ?.indexes.find((index) => index.id === def.parentIndexId);
            snapshotCheck(
              parent !== undefined && parent.unique && parent.invalid !== true,
              "Unusable imported foreign-key parent",
            );
            // The FK key must obey its parent's policy, NOT an invalidated support index's
            // smaller tuple limit. NULL references still skip (synthetic index is nonunique).
            const policy: IndexDef = {
              ...parent,
              id: def.id,
              name: def.name,
              tableId: table.id,
              columnIds: def.columnIds,
              unique: false,
            };
            entry.foreign.push({
              def,
              builder: new NativeIndex(policy, table, this.#space, scratch, budget, work),
            });
          }
      }
    } catch (error) {
      this.close();
      throw error;
    }
  }
  async append(
    tableId: number,
    columns: readonly SnapshotStagedColumn[],
    firstRowId: number,
  ): Promise<void> {
    const table = this.#tables.get(tableId);
    snapshotCheck(table !== undefined, "Unknown imported index table");
    for (const builder of table.indexes) await builder.append(columns, firstRowId);
    for (const check of table.foreign) await check.builder.append(columns, firstRowId);
  }
  async finishTable(
    tableId: number,
  ): Promise<{ keyIndex: IndexRoot | null; secondaryIndexes: IndexRoot[] }> {
    const entry = this.#tables.get(tableId);
    snapshotCheck(entry !== undefined, "Unknown imported index table");
    let keyIndex: IndexRoot | null = null;
    const secondaryIndexes: IndexRoot[] = [];
    for (const builder of entry.indexes) {
      const root = await builder.finish();
      this.#ready.set(root.indexId, root);
      if (root.indexId === entry.table.primaryKey) keyIndex = root;
      else secondaryIndexes.push(root);
    }
    for (const check of entry.foreign) await check.builder.finish();
    return { keyIndex, secondaryIndexes };
  }
  async validate(): Promise<void> {
    for (const table of this.#tables.values())
      for (const check of table.foreign) {
        const parent = this.#ready.get(check.def.parentIndexId);
        snapshotCheck(parent !== undefined, "Missing rebuilt foreign-key parent");
        const child = await check.builder.finish();
        await foreign(child.runs, parent.runs, this.#space, this.budget, this.work);
      }
  }
  close(): void {
    for (const table of this.#tables.values()) {
      for (const index of table.indexes) index.close();
      for (const check of table.foreign) check.builder.close();
    }
    this.#tables.clear();
    this.#ready.clear();
    this.#account.close();
  }
}
