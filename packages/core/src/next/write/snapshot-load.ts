/** Portable input -> private native generation -> ONE durable publication, after footer AND EOF.
 * The target factory owns a settled/exclusive owner (no active WritePath or background writers).
 * Publication rejection is indeterminate: close/reopen, never abort possibly published extents.
 */
import type { TableDef } from "../access/catalog.js";
import { MAX_LSN } from "../access/snapshot.js";
import {
  MANIFEST_FORMAT_VERSION,
  type ColumnTotals,
  type ColumnChunk,
  type GroupRef,
  type ManifestRoot,
  type TableRoot,
} from "../format/manifest.js";
import {
  decodeManifestRoot,
  encodeGroupIndex,
  encodeGroupPage,
  encodeManifestRoot,
  encodeTableStatistics,
  parseCharge,
  type GroupPageEntry,
} from "../format/manifest-codec.js";
import {
  SnapshotKind,
  SnapshotStreamReader,
  snapshotU53,
  type SnapshotWork,
} from "../format/snapshot.js";
import { ExtentListKeeper, listedExtents } from "../storage/extent-list.js";
import type {
  BlobRef,
  ByteBudget,
  ExtentRef,
  ExtentSpace,
  ExtentWriter,
} from "../storage/extents.js";
import { Catalog } from "./catalog.js";
import { SnapshotScopeError } from "./snapshot-export.js";
import { SnapshotNativeBuilder, snapshotBlockEnd } from "./snapshot-native.js";
import {
  SnapshotAccount,
  SnapshotStagedColumn,
  snapshotCheck,
  snapshotPace,
} from "./snapshot-stage.js";

export interface SnapshotLoadTarget {
  readonly space: ExtentSpace;
  readonly epoch: number;
  /** Settled old incarnation's last LSN, INCLUDING committed log-only state. */
  readonly currentLsn: number;
  /** Logical emptiness after recovery, not merely a null/empty durable root. */
  readonly empty: boolean;
  /** Releases the exclusive owner; successful callers reopen the ordinary write path. */
  close(): Promise<void>;
}
export interface SnapshotLoadOptions extends SnapshotWork {
  readonly replace?: boolean;
}

function stage3Scope(catalog: Catalog): void {
  if (catalog.sequences.length !== 0)
    throw new SnapshotScopeError("Sequence capture requires portable catalog1 (stage4)");
  for (const table of catalog.tables) {
    if (table.indexes.length !== 0 || table.foreignKeys.length !== 0 || table.checks.length !== 0)
      throw new SnapshotScopeError("Index/constraint snapshot reconstruction requires stage4");
    if (table.columns.some((column) => column.missing !== null))
      throw new SnapshotScopeError("Missing/backfill values require portable catalog1 (stage4)");
  }
}
class NativeTable {
  readonly account: SnapshotAccount;
  readonly pending: GroupRef[] = [];
  readonly pages: GroupPageEntry[] = [];
  readonly totals: Array<{ columnId: number; nullCount: number; values: number; bytes: number }>;
  rows = 0;
  groups = 0;
  constructor(
    readonly def: TableDef,
    readonly writer: ExtentWriter,
    readonly budget: ByteBudget,
    readonly work: SnapshotWork,
    readonly retained: SnapshotAccount,
  ) {
    this.account = new SnapshotAccount(budget);
    this.account.take(131072 + def.columns.length * 256);
    this.totals = def.columns.map((column) => ({
      columnId: column.id,
      nullCount: 0,
      values: 0,
      bytes: 0,
    }));
  }
  async add(
    group: GroupRef,
    columns: readonly SnapshotStagedColumn[],
    from: number,
    to: number,
    chunks: readonly ColumnChunk[],
  ): Promise<void> {
    this.pending.push(group);
    this.groups++;
    this.rows += group.rowCount;
    for (const column of columns) {
      const total = this.totals.find((total) => total.columnId === column.column.id);
      snapshotCheck(total !== undefined, "Unknown statistics column");
      total.bytes += chunks.find((chunk) => chunk.columnId === column.column.id)?.data.length ?? 0;
      for (let row = from; row < to; row++) {
        const length = column.lengths[row] ?? 0xffffffff;
        if (length === 0xffffffff) total.nullCount++;
        else {
          total.values++;
          // Raw logical lengths do not describe native chunk residency.
        }
      }
    }
    if (this.pending.length === 64) await this.flush();
  }
  async flush(): Promise<void> {
    if (this.pending.length === 0) return;
    await snapshotPace(this.work);
    this.account.take(512);
    this.retained.take(512);
    const written = await this.writer.write([encodeGroupPage(this.pending)]),
      ref = written.blobs[0],
      first = this.pending[0],
      last = this.pending.at(-1);
    snapshotCheck(
      ref !== undefined && first !== undefined && last !== undefined,
      "Missing group-directory page",
    );
    const rows = this.pending.reduce((sum, group) => sum + group.rowCount, 0);
    this.pages.push({
      ref,
      firstRowId: first.firstRowId,
      lastRowId: last.lastRowId,
      groupCount: this.pending.length,
      rowCount: rows,
      liveRowCount: rows,
    });
    this.pending.length = 0;
  }
  async finish(lsn: number): Promise<TableRoot> {
    await this.flush();
    this.retained.take(1024);
    const index = await this.writer.write([encodeGroupIndex(this.pages)]),
      groups = index.blobs[0];
    snapshotCheck(groups !== undefined, "Missing native group index");
    let statistics: BlobRef | null = null;
    if (this.groups > 0) {
      const totals: ColumnTotals[] = this.totals
        .slice()
        .sort((a, b) => a.columnId - b.columnId)
        .map((total) => ({
          ...total,
          unbounded: total.values > 0,
          min: null,
          max: null,
        }));
      const written = await this.writer.write([
        encodeTableStatistics({ groupCount: this.groups, columns: totals }),
      ]);
      statistics = written.blobs[0] ?? null;
      snapshotCheck(statistics !== null, "Missing native statistics");
    }
    return {
      tableId: this.def.id,
      schemaVersion: this.def.schemaVersion,
      nextRowId: this.rows + 1,
      flushedThroughLsn: lsn,
      liveRowCount: this.rows,
      groups,
      groupCount: this.groups,
      staged: [],
      keyIndex: null,
      secondaryIndexes: [],
      statistics,
    };
  }
  close(): void {
    this.account.close();
  }
}

/** Internal stage3 entry point. Its catalog decoder is replaced by portable catalog1 in stage4. */
export async function loadSnapshot(
  input: AsyncIterable<Uint8Array>,
  openTarget: () => Promise<SnapshotLoadTarget>,
  budget: ByteBudget,
  options: SnapshotLoadOptions = {},
): Promise<void> {
  const account = new SnapshotAccount(budget);
  let reader: SnapshotStreamReader | undefined,
    target: SnapshotLoadTarget | undefined,
    writer: ExtentWriter | undefined,
    scratchWriter: ExtentWriter | undefined,
    native: SnapshotNativeBuilder | undefined,
    table: NativeTable | undefined,
    list: ExtentListKeeper | undefined;
  let listPrepared = false,
    oldCharge = 0;
  let failed = false;
  const staged: SnapshotStagedColumn[] = [];
  let current: SnapshotStagedColumn | undefined;
  try {
    reader = await SnapshotStreamReader.open(input, budget, options);
    account.take(65536);
    const record = new Uint8Array(64),
      catalogParts: Uint8Array[] = [],
      tables: TableRoot[] = [];
    let catalogLength = 0,
      catalog: Catalog | undefined,
      catalogBytes: Uint8Array | undefined,
      tableIndex = 0,
      columnIndex = 0,
      old: ManifestRoot | null = null,
      lsn = 0,
      sequence = 0,
      blockRows = 0;
    for await (const frame of reader.frames()) {
      await snapshotPace(options);
      const payload = frame.payload;
      if (frame.kind === SnapshotKind.catalogPart) {
        snapshotCheck(
          catalogLength + payload.length <= 1024 * 1024,
          "Portable catalog exceeds native catalog capacity",
        );
        account.take(payload.length + 128);
        catalogParts.push(payload.slice());
        catalogLength += payload.length;
      } else if (frame.kind === SnapshotKind.catalogEnd) {
        account.take(catalogLength + parseCharge(catalogLength) * 4);
        catalogBytes = new Uint8Array(catalogLength);
        let at = 0;
        for (const part of catalogParts) {
          catalogBytes.set(part, at);
          at += part.length;
        }
        catalog = Catalog.decode(catalogBytes);
        stage3Scope(catalog);
        catalogParts.length = 0;
        account.free(catalogLength); // part copies consumed; slots remain funded
        target = await openTarget();
        snapshotCheck(
          target.empty || options.replace === true,
          "Snapshot target is not empty; replacement must be explicit",
        );
        snapshotCheck(
          Number.isSafeInteger(target.currentLsn) &&
            target.currentLsn >= 0 &&
            target.currentLsn < MAX_LSN,
          "Invalid or exhausted target LSN",
        );
        if (target.space.root !== null) {
          const parsed = decodeManifestRoot(target.space.root, budget);
          old = parsed.value;
          oldCharge = parsed.charge;
        }
        snapshotCheck(
          old === null || target.currentLsn >= old.globalLsn,
          "Target LSN precedes its durable root",
        );
        lsn = target.currentLsn + 1;
        sequence = (old?.sequence ?? 0) + 1;
        writer = target.space.beginWrite("snapshot native generation", { budget });
        scratchWriter = target.space.beginWrite("snapshot verified portable staging", { budget });
        native = new SnapshotNativeBuilder(writer, target.space, budget, account);
      } else if (frame.kind === SnapshotKind.tableBegin) {
        snapshotCheck(
          catalog !== undefined && writer !== undefined && table === undefined,
          "Table outside catalog/load scope",
        );
        const def = catalog.tables[tableIndex++];
        snapshotCheck(
          def?.id === snapshotU53(payload, 0) &&
            def.schemaVersion === snapshotU53(payload, 8) &&
            def.columns.length === snapshotU53(payload, 16),
          "Table/catalog identity mismatch",
        );
        table = new NativeTable(def, writer, budget, options, account);
        columnIndex = 0;
      } else if (frame.kind === SnapshotKind.columnBegin) {
        snapshotCheck(
          table !== undefined && scratchWriter !== undefined && current === undefined,
          "Column outside native table",
        );
        const columnId = snapshotU53(payload, 8),
          def = table.def.columns.find((column) => column.id === columnId);
        snapshotCheck(def !== undefined, "Portable column absent from catalog");
        // Ascending IDs are checked by the container. Complete block membership is checked here.
        snapshotCheck(
          staged.every((column) => column.column.id !== columnId),
          "Duplicate portable catalog column",
        );
        snapshotCheck(
          snapshotU53(payload, 24) === table.rows,
          "Portable row offset disagrees with native table",
        );
        blockRows = snapshotU53(payload, 32);
        current = new SnapshotStagedColumn(
          def,
          blockRows,
          snapshotU53(payload, 40),
          scratchWriter,
          budget,
          options,
        );
        staged.push(current);
        columnIndex++;
      } else if (frame.kind === SnapshotKind.columnPart) {
        snapshotCheck(current !== undefined, "Portable bytes outside column");
        await current.append(payload);
      } else if (frame.kind === SnapshotKind.columnEnd) {
        snapshotCheck(
          current !== undefined &&
            target !== undefined &&
            table !== undefined &&
            native !== undefined,
          "Incomplete native column",
        );
        await current.finish(target.space, record);
        current = undefined;
        if (columnIndex === table.def.columns.length) {
          for (let from = 0; from < blockRows;) {
            const to = snapshotBlockEnd(staged, from, blockRows);
            const group = await native.group(staged, from, to, table.rows + 1, table.groups + 1);
            try {
              await table.add(group.group, staged, from, to, group.chunks);
            } finally {
              group.release();
            }
            from = to;
          }
          for (const column of staged) await column.discard();
          staged.length = 0;
          columnIndex = 0;
        }
      } else if (frame.kind === SnapshotKind.tableEnd) {
        snapshotCheck(
          table !== undefined &&
            native !== undefined &&
            current === undefined &&
            staged.length === 0,
          "Incomplete native table",
        );
        const expected = snapshotU53(payload, 0);
        if (table.def.columns.length === 0)
          for (let from = 0; from < expected; from += 256) {
            const group = await native.group(
              [],
              0,
              Math.min(256, expected - from),
              table.rows + 1,
              table.groups + 1,
            );
            try {
              await table.add(group.group, [], 0, 0, group.chunks);
            } finally {
              group.release();
            }
          }
        snapshotCheck(table.rows === expected, "Native/portable table counts disagree");
        account.take(2048 + table.def.columns.length * 64);
        tables.push(await table.finish(lsn));
        table.close();
        table = undefined;
      }
    }
    // Exiting the iterator proves footer, every rolling/body CRC, counts AND physical EOF.
    snapshotCheck(
      target !== undefined &&
        writer !== undefined &&
        scratchWriter !== undefined &&
        catalog !== undefined &&
        catalogBytes !== undefined &&
        table === undefined &&
        tableIndex === catalog.tables.length,
      "Incomplete native snapshot generation",
    );
    await scratchWriter.abort();
    const writtenCatalog = await writer.write([catalogBytes]),
      catalogRef = writtenCatalog.blobs[0];
    snapshotCheck(catalogRef !== undefined, "Missing native catalog");
    const retired: ExtentRef[] = [],
      seen = new Set<number>();
    for await (const item of listedExtents(old?.extentList ?? [], target.space, budget, options)) {
      const extent = "extent" in item ? item.extent : item;
      if (!seen.has(extent.offset)) {
        account.take(192);
        seen.add(extent.offset);
        retired.push(extent);
      }
    }
    list = new ExtentListKeeper([], { reader: target.space, budget, ...options });
    const prepared = await list.prepare(1, { added: writer.extents, released: [], writer });
    listPrepared = true;
    const root: ManifestRoot = {
      formatVersion: MANIFEST_FORMAT_VERSION,
      sequence,
      epoch: target.epoch,
      log: { salt: ((old?.log.salt ?? 0) + 1) >>> 0, firstLsn: lsn + 1, rootSequence: sequence },
      globalLsn: lsn,
      catalogVersion: catalog.version,
      catalog: catalogRef,
      nextTableId: catalog.counters.nextTableId,
      sequences: [],
      tables,
      clients: [],
      extentList: prepared.list,
    };
    account.take(4 * 1024 * 1024); // RefWriter growth/output + publication/decoded-root scratch bound
    const rootBytes = encodeManifestRoot(root);
    await snapshotPace(options);
    await target.space.publish(rootBytes, {
      writers: [writer],
      retired,
      list: prepared.list,
      budget,
      ...options,
    });
    list.commit(1, sequence);
    listPrepared = false;
  } catch (error) {
    failed = true;
    if (writer?.state === "open") await writer.abort().catch(() => undefined);
    if (scratchWriter?.state === "open") await scratchWriter.abort().catch(() => undefined);
    throw error;
  } finally {
    reader?.close();
    native?.close();
    table?.close();
    for (const column of staged) column.close();
    if (listPrepared) list?.abort(1);
    list?.close();
    account.close();
    if (oldCharge !== 0) budget.free(oldCharge);
    await closeLoadTarget(target, failed);
  }
}

async function closeLoadTarget(
  target: SnapshotLoadTarget | undefined,
  failed: boolean,
): Promise<void> {
  try {
    await target?.close();
  } catch (error) {
    if (!failed) throw error;
  }
}
