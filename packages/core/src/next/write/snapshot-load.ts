import { captureWriterExtents, type CapturedWriterExtents } from "../storage/writer-extents.js";
import type { ChangeFeed } from "../access/changes.js";
/** Portable input -> private native generation -> ONE durable publication, after footer AND EOF.
 * The target factory owns a settled/exclusive owner (no active WritePath or background writers).
 * Publication rejection is indeterminate: close/reopen, never abort possibly published extents.
 */
import { indexKeyKind } from "../keys/key-encoding.js";
import { MemtableMemoryError } from "./bytes.js";
import { SnapshotIndexes } from "./snapshot-index.js";
import { MemoryAdmissionError, type MemoryGovernor } from "../memory/governor.js";
import {
  SnapshotReplay,
  SnapshotSourcePrefix,
  type ReplayableSnapshotInput,
} from "./snapshot-source.js";
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
  ParseBudgetError,
  type GroupPageEntry,
} from "../format/manifest-codec.js";
import {
  SnapshotBudgetError,
  SnapshotKind,
  SnapshotStreamReader,
  snapshotU53,
  type SnapshotWork,
} from "../format/snapshot.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import {
  ReadDeniedError,
  type BlobRef,
  type ByteBudget,
  type ExtentSpace,
  type ExtentWriter,
} from "../storage/extents.js";
import { decodeSnapshotCatalog, type SnapshotHorizon } from "./snapshot-catalog.js";
import { writeCatalogStorage } from "./catalog-storage.js";
import { UnsupportedCatalogError } from "./catalog-records.js";
import { SnapshotFormatError } from "../format/snapshot.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { DurableFormatError } from "../durable-format.js";
import type { Catalog } from "./catalog.js";
import { SnapshotScopeError } from "./snapshot-export.js";
import { SnapshotNativeBuilder, snapshotBlockEnd } from "./snapshot-native.js";
import {
  SnapshotAccount,
  SnapshotStagedColumn,
  snapshotCheck,
  snapshotPace,
} from "./snapshot-stage.js";

export interface SnapshotLoadTarget {
  /** Owner-lifetime feed, if observers survive the exclusive offline replacement. */
  readonly changes?: ChangeFeed;
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

function snapshotScope(catalog: Catalog): void {
  // TODO(B6/B2): CHECK evaluation and FTS postings reconstruction are not wired
  // into this offline loader. Carry nothing partially; refuse before acquiring the target.
  if (catalog.fts.length || catalog.triggers.length)
    throw new SnapshotScopeError("TODO(B6): portable FTS/trigger reconstruction is unsupported");
  if (catalog.views.some((view) => view.managed === true))
    throw new SnapshotScopeError("TODO(B6): managed view snapshot scope is unsupported");
  for (const table of catalog.tables) {
    if (table.managed === true)
      throw new SnapshotScopeError("TODO(B6): managed table snapshot scope is unsupported");
    for (const index of table.indexes) {
      snapshotCheck(
        index.role !== "primary" || (index.unique && table.primaryKey === index.id),
        "Orphan/nonunique imported PRIMARY definition",
      );
      let equality = false;
      for (const id of index.columnIds) {
        const column = table.columns.find((column) => column.id === id);
        snapshotCheck(column !== undefined, "Unresolved imported index column");
        try {
          if (indexKeyKind(column.type) === "equality") equality = true;
        } catch {
          throw new SnapshotScopeError("Unindexable portable index type");
        }
      }
      snapshotCheck(
        !equality || index.kind === "equality",
        "Ordered enum index cannot preserve its semantics",
      );
    }
    if (table.checks.length)
      throw new SnapshotScopeError("TODO(B6): offline CHECK evaluation is not wired");
    if (table.columns.some((column) => column.allowedValues !== undefined))
      throw new SnapshotScopeError("TODO(B6): portable closed-set validation");
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

/** Whole-attempt admission: unwind all retained inputs before a cancellable retry at byte 0.
 * The target factory must use an already-funded owner, never queue memory admission itself. */
export async function loadSnapshot(
  input: ReplayableSnapshotInput,
  openTarget: () => Promise<SnapshotLoadTarget>,
  governor: MemoryGovernor,
  options: SnapshotLoadOptions = {},
): Promise<void> {
  let minimum = 1024;
  // O(1) scalar retry bookkeeping, included in the existing 512B attempt control charge.
  const prefix = new SnapshotSourcePrefix(input.byteLength);
  for (;;) {
    const [grant] = await governor.admitGroup({
      label: "portable snapshot load",
      includesProgress: true,
      grants: [{ label: "snapshot complete step", minimumBytes: minimum, spillable: false }],
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
    if (grant === undefined) throw new TypeError("Missing snapshot load grant");
    let shortfall: { bytes: number; required: number } | undefined;
    const budget: ByteBudget = {
      ordinarySource: () => grant,
      allocate(bytes) {
        const required = grant.allocated + bytes;
        if (required > grant.held) {
          shortfall = { bytes, required };
          return false;
        }
        if (!grant.allocate(bytes)) throw new TypeError("Lost snapshot step admission");
        return true;
      },
      free(bytes) {
        grant.free(bytes);
      },
    };
    let replay: SnapshotReplay | undefined;
    let control = false;
    try {
      if (!budget.allocate(512)) throw new SnapshotBudgetError(512);
      control = true;
      await prefix.verify(input, budget, options.signal);
      replay = new SnapshotReplay(input, budget, options.signal, prefix);
      await loadAttempt(replay, openTarget, budget, options);
      return;
    } catch (error) {
      if (
        !(
          error instanceof SnapshotBudgetError ||
          error instanceof MemtableMemoryError ||
          error instanceof ParseBudgetError ||
          error instanceof ReadDeniedError
        ) ||
        shortfall?.bytes !== error.bytes
      )
        throw error;
      const required = shortfall.required;
      if (required > governor.capBytes)
        throw new MemoryAdmissionError("portable snapshot load", required, governor.capBytes);
      // Round only inside the cap: rounding must not turn a fitting step into a 53200.
      const availableAfterUnwind = governor.capBytes - governor.usedBytes + grant.held;
      minimum = Math.max(required, Math.min(availableAfterUnwind, minimum * 2));
    } finally {
      try {
        await replay?.close();
      } finally {
        if (control) budget.free(512);
        grant.close();
      }
    }
    // No grant, input, target, pin or unpublished writer survives this admission boundary.
  }
}

/** One private, entirely discardable attempt. No input is retained by the retry wrapper. */
async function loadAttempt(
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
    indexes: SnapshotIndexes | undefined,
    table: NativeTable | undefined,
    list: ExtentListKeeper | undefined,
    additions: CapturedWriterExtents | undefined;
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
      catalogReady = false,
      horizons: readonly SnapshotHorizon[] = [],
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
          Number.isSafeInteger(catalogLength + payload.length),
          "Portable catalog length exceeds u53",
        );
        account.take(payload.length + 128);
        catalogParts.push(payload.slice());
        catalogLength += payload.length;
      } else if (frame.kind === SnapshotKind.catalogEnd) {
        account.take(catalogLength);
        let catalogBytes: Uint8Array | undefined = new Uint8Array(catalogLength);
        let at = 0;
        for (const part of catalogParts) {
          catalogBytes.set(part, at);
          at += part.length;
        }
        try {
          const decoded = decodeSnapshotCatalog(catalogBytes, account);
          catalog = decoded.value.catalog;
          horizons = decoded.value.horizons;
        } catch (error) {
          if (error instanceof UnsupportedCatalogError) throw new SnapshotScopeError(error.message);
          if (error instanceof StoreCorruptionError || error instanceof DurableFormatError)
            throw new SnapshotFormatError(error.message);
          throw error;
        }
        snapshotScope(catalog);
        catalogReady = true;
        catalogBytes = undefined;
        account.free(catalogLength * 2 + catalogParts.length * 128);
        catalogParts.length = 0; // only the decoded catalog/horizon graph survives target open
        // Prove the publication step fits before acquiring the owner. Its scratch is not
        // allocated until publication, so it need not overlap temporary extent-list encoding.
        account.take(4 * 1024 * 1024);
        account.free(4 * 1024 * 1024);
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
        writer = target.space.beginWrite("snapshot native generation", {
          budget,
          ownershipBudget: budget,
        });
        scratchWriter = target.space.beginWrite("snapshot verified portable staging", {
          budget,
          ownershipBudget: budget,
        });
        native = new SnapshotNativeBuilder(writer, target.space, budget, account);
        indexes = new SnapshotIndexes(
          catalog,
          target.space,
          writer,
          scratchWriter,
          budget,
          options,
        );
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
          snapshotCheck(indexes !== undefined, "Missing native index builder");
          await indexes.append(table.def.id, staged, table.rows + 1);
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
        snapshotCheck(indexes !== undefined, "Missing native indexes");
        tables.push({ ...(await table.finish(lsn)), ...(await indexes.finishTable(table.def.id)) });
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
        catalogReady &&
        table === undefined &&
        tableIndex === catalog.tables.length,
      "Incomplete native snapshot generation",
    );
    snapshotCheck(indexes !== undefined, "Missing native index validation");
    await indexes.validate();
    await scratchWriter.abort();
    const catalogRef = await writeCatalogStorage(writer, catalog, budget, options.signal);
    list = new ExtentListKeeper([], { reader: target.space, budget, ...options });
    additions = await captureWriterExtents(writer, budget, options);
    const prepared = await list.prepare(1, { ...additions, released: [], writer });
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
      sequences: horizons,
      tables,
      clients: [],
      // Portable scope rejects FTS definitions: no old native acceleration/history
      // may follow table IDs into the replacement generation. R25 retires its pages.
      fts: { checkpoint: null, staged: null },
      extentList: prepared.list,
    };
    account.take(4 * 1024 * 1024);
    const rootBytes = encodeManifestRoot(root);
    await snapshotPace(options);
    await target.space.publish(rootBytes, {
      writers: [writer],
      retired: [],
      retireCurrentRoot: true,
      list: prepared.list,
      budget,
      ...options,
    });
    target.changes?.replace(lsn);
    list.commit(1, sequence);
    listPrepared = false;
  } catch (error) {
    failed = true;
    if (
      writer?.state === "indeterminate" &&
      (error instanceof SnapshotBudgetError ||
        error instanceof ReadDeniedError ||
        error instanceof ParseBudgetError)
    )
      throw new SnapshotScopeError(
        "Indeterminate publication: reopen target before retrying snapshot load",
      );
    if (writer?.state === "open") await writer.abort().catch(() => undefined);
    if (scratchWriter?.state === "open") await scratchWriter.abort().catch(() => undefined);
    throw error;
  } finally {
    try {
      reader?.close();
      indexes?.close();
      native?.close();
      table?.close();
      for (const column of staged) column.close();
      if (listPrepared) list?.abort(1);
      list?.close();
    } finally {
      try {
        await additions?.release().catch((error: unknown) => {
          if (!failed) throw error;
        });
      } finally {
        account.close();
        if (oldCharge !== 0) budget.free(oldCharge);
        await closeLoadTarget(target, failed);
      }
    }
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
