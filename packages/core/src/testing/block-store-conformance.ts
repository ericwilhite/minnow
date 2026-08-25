import {
  CompactionJobConflictError,
  LeaseConflictError,
  LeaseOwnerConflictError,
  MAX_LEVEL_ZERO_SEGMENTS,
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_STORAGE_BULK_READ_ITEMS,
  PostingBuildConflictError,
  SnapshotManifestMissingError,
  TableInUseError,
  TableRecordConflictError,
  TempOwnerConflictError,
  TransactionRecordConflictError,
  UniqueKeyConflictError,
  secondaryUniqueKeyNamespace,
  WriteConflictError,
  type BlockStore,
  type CompactionJobRecord,
  type SegmentRecord,
  type TableRecord,
  type TransactionRecord,
} from "../storage/types.js";

/**
 * The storage-contract conformance kit: the executable half of the `BlockStore` documentation,
 * for anyone implementing the interface against a new substrate — React Native storage, an
 * object store, the Node filesystem, an encrypted wrapper.
 *
 * Framework-agnostic on purpose: each case is a plain async function that throws a plain
 * `Error` naming what failed, so the kit runs under any test runner (or none):
 *
 * ```ts
 * import { blockStoreConformanceCases } from "@minnowdb/core/testing";
 *
 * for (const conformanceCase of blockStoreConformanceCases()) {
 *   it(conformanceCase.name, () =>
 *     conformanceCase.run({
 *       create: () => MyStore.open({ name: crypto.randomUUID() }),
 *       reopen: (store) => {
 *         store.close();
 *         return MyStore.open(sameName);
 *       },
 *     }),
 *   );
 * }
 * ```
 *
 * Passing the kit means the engine's semantics hold: atomic commits, compare-and-swap
 * conflicts thrown as the exact exported error classes, defensive copies, documented
 * ordering, and durability across a reopen. It is a floor, not the ceiling — the engine's own
 * deeper suites (fault sweeps, concurrency soaks, quota injection) also run against every
 * first-party adapter, and this kit itself runs against all of them in CI so it cannot drift
 * from what they do.
 */

export interface BlockStoreConformanceTarget {
  /** A fresh store over an empty database. Called once per case. */
  create(): Promise<BlockStore>;
  /**
   * Optional: close the given store and reopen the same underlying database. Provide it —
   * without it every durability case is skipped, and durability is half the contract.
   * (A purely in-memory store may honestly return the same instance.)
   */
  reopen?(store: BlockStore): Promise<BlockStore>;
}

export interface BlockStoreConformanceCase {
  name: string;
  /** Resolves on conformance; throws an `Error` naming the violated rule otherwise. */
  run(target: BlockStoreConformanceTarget): Promise<void>;
}

/** Runs every case in sequence; throws on the first failure, prefixed with the case name. */
export async function runBlockStoreConformance(target: BlockStoreConformanceTarget): Promise<void> {
  for (const conformanceCase of blockStoreConformanceCases()) {
    try {
      await conformanceCase.run(target);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`[${conformanceCase.name}] ${message}`, { cause: error });
    }
  }
}

// ------------------------------------------------------------------------------------------
// Assertions — deliberately tiny and dependency-free.
// ------------------------------------------------------------------------------------------

function check(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function checkEqual(actual: unknown, expected: unknown, message: string): void {
  if (!deepEqual(actual, expected)) {
    throw new Error(`${message}\n  expected: ${render(expected)}\n  actual:   ${render(actual)}`);
  }
}

async function checkThrows<T extends Error>(
  work: () => Promise<unknown>,
  errorClass: new (...args: never[]) => T,
  message: string,
): Promise<T> {
  try {
    await work();
  } catch (error) {
    check(
      error instanceof errorClass,
      `${message}: threw ${render(error)} instead of ${errorClass.name}. Conflicts must be ` +
        `thrown as the exact exported classes — the engine's retry loops match on them.`,
    );
    check(
      (error as Error).name === errorClass.name,
      `${message}: the error's own name is "${(error as Error).name}", not "${errorClass.name}" ` +
        `— the worker client rehydrates errors by constructor name.`,
    );
    return error;
  }
  throw new Error(`${message}: resolved instead of throwing ${errorClass.name}`);
}

function deepEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (left instanceof Uint8Array && right instanceof Uint8Array) {
    return left.byteLength === right.byteLength && left.every((byte, at) => right[at] === byte);
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((entry, at) => deepEqual(entry, right[at]));
  }
  if (
    typeof left === "object" &&
    typeof right === "object" &&
    left !== null &&
    right !== null &&
    !Array.isArray(left) &&
    !Array.isArray(right)
  ) {
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    return (
      deepEqual(leftKeys, rightKeys) &&
      leftKeys.every((key) =>
        deepEqual((left as Record<string, unknown>)[key], (right as Record<string, unknown>)[key]),
      )
    );
  }
  return false;
}

function render(value: unknown): string {
  if (value instanceof Error) return `${value.name}: ${value.message}`;
  try {
    return JSON.stringify(value, (_key, entry: unknown) =>
      typeof entry === "bigint"
        ? `${entry.toString()}n`
        : entry instanceof Uint8Array
          ? `Uint8Array(${String(entry.byteLength)})`
          : entry,
    );
  } catch {
    return String(value);
  }
}

async function manifestBlockIds(store: BlockStore, version: number): Promise<string[]> {
  const ids: string[] = [];
  let afterBlockId: string | null = null;
  for (;;) {
    const page = await store.listManifestBlockPage({ version, afterBlockId, limit: 256 });
    ids.push(...page.records.map(({ blockId }) => blockId));
    if (page.nextCursor === null) return ids;
    afterBlockId = page.nextCursor;
  }
}

async function currentManifestBlockIds(store: BlockStore): Promise<string[]> {
  const version = await store.getCurrentManifestVersion();
  return version === null ? [] : manifestBlockIds(store, version);
}

async function tableSegments(store: BlockStore, tableId: string): Promise<SegmentRecord[]> {
  const records: SegmentRecord[] = [];
  let afterId: string | null = null;
  for (;;) {
    const page = await store.listTableSegmentPage(tableId, afterId, 256);
    records.push(...page.records);
    if (page.nextCursor === null) return records;
    afterId = page.nextCursor;
  }
}

// ------------------------------------------------------------------------------------------
// Fixtures — the smallest records that exercise each rule.
// ------------------------------------------------------------------------------------------

const T0 = "2026-01-01T00:00:00.000Z";
const MIDDLE = "2026-01-01T00:30:00.000Z";
const LATER = "2026-01-01T01:00:00.000Z";
const PAST = "2000-01-01T00:00:00.000Z";

function table(name: string, keyed = false): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [
      { id: "col-id", name: "id", type: "number", nullable: false },
      { id: "col-v", name: "v", type: "string", nullable: true },
    ],
    managed: false,
    revision: 0,
    createdAt: T0,
    ...(keyed ? { uniqueKeyColumnId: "col-id" } : {}),
  };
}

function transaction(id: string, snapshotVersion: number | null): TransactionRecord {
  return {
    id,
    ownerId: `owner-${id}`,
    expiresAt: LATER,
    snapshotVersion,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: T0,
    updatedAt: T0,
    committedVersion: null,
  };
}

function segment(
  id: string,
  tableId: string,
  transactionId: string,
  blockId: string,
  commitOrdinal = 0,
): SegmentRecord {
  return {
    id,
    tableId,
    transactionId,
    rowCount: 2,
    rowIdStart: 1n,
    rowIdEndExclusive: 3n,
    columnBlockIds: { "col-id": [blockId], "col-v": [blockId] },
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal,
    rowIdSpans: [],
    createdAt: T0,
  };
}

/** Stages one block + one segment on a fresh transaction and commits it. */
async function commitOne(
  store: BlockStore,
  suffix: string,
  options: { keyTokens?: string[]; tableId?: string } = {},
): Promise<{ version: number; blockId: string }> {
  const version = await store.getCurrentManifestVersion();
  const transactionId = `txn-${suffix}`;
  const blockId = `table/t/segment/${suffix}/part/000000`;
  await store.createTransaction(transaction(transactionId, version));
  await store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks: [{ id: blockId, bytes: Uint8Array.of(1, 2, 3, 4) }],
    segments: [segment(`segment-${suffix}`, options.tableId ?? "table-t", transactionId, blockId)],
    updatedAt: T0,
  });
  const summary = await store.commitTransaction({
    transactionId,
    expectedTransactionRevision: 1,
    expectedManifestVersion: version,
    levelZeroSegmentLimits: [
      { tableId: options.tableId ?? "table-t", limit: MAX_LEVEL_ZERO_SEGMENTS },
    ],
    committedAt: LATER,
    ...(options.keyTokens === undefined
      ? {}
      : {
          uniqueKeyChanges: [
            {
              tableId: options.tableId ?? "table-t",
              keyTokens: options.keyTokens,
              requireAbsent: true,
            },
          ],
        }),
  });
  return { version: summary.version, blockId };
}

// ------------------------------------------------------------------------------------------
// The cases.
// ------------------------------------------------------------------------------------------

export function blockStoreConformanceCases(): BlockStoreConformanceCase[] {
  return [
    {
      name: "pending tables stay invisible across multi-batch reopen and publish only at commit",
      async run(target) {
        let store = await target.create();
        const pending = table("pending");
        const epoch = (await store.getCatalogProbe()).catalogEpoch;
        const begun = await store.beginTransaction({
          record: {
            id: "pending-owner",
            ownerId: "owner-pending",
            expiresAt: LATER,
            pendingBlockIds: [],
            pendingSegmentIds: [],
            status: "active",
            revision: 0,
            startedAt: T0,
            updatedAt: T0,
            committedVersion: null,
          },
          pendingTable: { record: pending, nextRowId: 1n, expectedCatalogEpoch: epoch },
        });
        check(begun.record.pendingTable?.id === pending.id, "pending reservation was not recorded");
        check(
          (await store.getTable(pending.id)) === undefined,
          "pending table became public early",
        );
        checkEqual(await store.listTables(), [], "pending table appeared in the public catalog");

        const firstBlock = "pending/block/1";
        await store.stageTransactionArtifacts({
          transactionId: begun.record.id,
          expectedRevision: begun.record.revision,
          blocks: [{ id: firstBlock, bytes: Uint8Array.of(1) }],
          segments: [segment("pending-segment-1", pending.id, begun.record.id, firstBlock)],
          updatedAt: MIDDLE,
        });
        if (target.reopen !== undefined) store = await target.reopen(store);
        check((await store.getTable(pending.id)) === undefined, "reopen exposed a pending table");
        const resumed = await store.getTransaction(begun.record.id);
        check(resumed?.revision === 1, "reopen lost the first pending-table stage");

        const secondBlock = "pending/block/2";
        await store.stageTransactionArtifacts({
          transactionId: begun.record.id,
          expectedRevision: resumed.revision,
          blocks: [{ id: secondBlock, bytes: Uint8Array.of(2) }],
          segments: [
            {
              ...segment("pending-segment-2", pending.id, begun.record.id, secondBlock),
              rowIdStart: 3n,
              rowIdEndExclusive: 5n,
              commitOrdinal: 1,
            },
          ],
          updatedAt: LATER,
        });
        const manifest = await store.commitTransaction({
          transactionId: begun.record.id,
          expectedTransactionRevision: 2,
          expectedManifestVersion: null,
          levelZeroSegmentLimits: [{ tableId: pending.id, limit: MAX_LEVEL_ZERO_SEGMENTS }],
          committedAt: LATER,
        });
        check(manifest.version === 0, "pending-table commit did not publish the first manifest");
        check(
          (await store.getTable(pending.id))?.name === pending.name,
          "commit lost pending table",
        );
        check(
          (await store.getTransaction(begun.record.id))?.pendingTable === undefined,
          "commit retained the pending catalog reservation",
        );
        store.close();
      },
    },
    {
      name: "staged blocks are immutable, copied both directions, and duplicates are refused",
      async run(target) {
        const store = await target.create();
        await store.createTransaction(transaction("txn-block-copy", null));
        const bytes = Uint8Array.of(1, 2, 3);
        await store.stageTransactionArtifacts({
          transactionId: "txn-block-copy",
          expectedRevision: 0,
          blocks: [{ id: "block-a", bytes }],
          segments: [],
          updatedAt: T0,
        });
        bytes[0] = 99; // The store must not alias the caller's buffer.
        const first = await store.getBlock("block-a");
        check(first?.[0] === 1, "stored bytes changed with the caller's buffer");
        first[1] = 99; // Nor may the caller's mutations reach the store.
        const second = await store.getBlock("block-a");
        check(second?.[1] === 2, "returned bytes alias the store's copy");
        try {
          await store.stageTransactionArtifacts({
            transactionId: "txn-block-copy",
            expectedRevision: 1,
            blocks: [{ id: "block-a", bytes: Uint8Array.of(9) }],
            segments: [],
            updatedAt: LATER,
          });
          throw new Error("a duplicate block id was accepted");
        } catch (error) {
          check(error instanceof Error, "duplicate rejection must be an Error");
        }
        checkEqual(
          await store.getBlocks(["missing", "block-a"]).then((all) => all[0]),
          undefined,
          "getBlocks must be positional with undefined for missing ids",
        );
        store.close();
      },
    },
    {
      name: "an artifact batch with any duplicate writes nothing at all",
      async run(target) {
        const store = await target.create();
        await store.createTransaction(transaction("txn-block-batch", null));
        await store.stageTransactionArtifacts({
          transactionId: "txn-block-batch",
          expectedRevision: 0,
          blocks: [{ id: "block-a", bytes: Uint8Array.of(1) }],
          segments: [],
          updatedAt: T0,
        });
        try {
          await store.stageTransactionArtifacts({
            transactionId: "txn-block-batch",
            expectedRevision: 1,
            blocks: [
              { id: "block-b", bytes: Uint8Array.of(2) },
              { id: "block-a", bytes: Uint8Array.of(3) },
              { id: "block-c", bytes: Uint8Array.of(4) },
            ],
            segments: [],
            updatedAt: LATER,
          });
          throw new Error("a batch containing an existing id was accepted");
        } catch {
          // Expected; what matters is that nothing landed.
        }
        checkEqual(await store.getBlock("block-b"), undefined, "a failed batch leaked block-b");
        checkEqual(await store.getBlock("block-c"), undefined, "a failed batch leaked block-c");
        checkEqual(await store.getBlock("block-a"), Uint8Array.of(1), "the first block changed");
        store.close();
      },
    },
    {
      name: "version-scoped block reads are exact, bounded, and reject missing history",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t"));
        const first = await commitOne(store, "manifest-read-first");
        const second = await commitOne(store, "manifest-read-second");
        checkEqual(
          await store.hasManifestBlocks(first.version, [first.blockId, "not-a-member"]),
          [true, false],
          "manifest membership must be positional and exact",
        );
        checkEqual(
          await store.readManifestBlock(first.version, first.blockId),
          Uint8Array.of(1, 2, 3, 4),
          "a readable manifest member must return its payload",
        );
        checkEqual(
          await store.readManifestBlock(first.version, second.blockId),
          undefined,
          "a non-member block must not be readable through an older manifest",
        );
        await checkThrows(
          () =>
            store.hasManifestBlocks(
              second.version,
              Array.from(
                { length: MAX_MANIFEST_BLOCK_PRESENCE_IDS + 1 },
                (_, index) => `presence-${String(index)}`,
              ),
            ),
          RangeError,
          "manifest membership cap+1",
        );
        await checkThrows(
          () =>
            store.getBlocks(
              Array.from(
                { length: MAX_STORAGE_BULK_READ_ITEMS + 1 },
                (_, index) => `block-${String(index)}`,
              ),
            ),
          RangeError,
          "block read cap+1",
        );
        const oversizedIds = Array.from(
          { length: MAX_STORAGE_BULK_READ_ITEMS + 1 },
          (_, index) => `id-${String(index)}`,
        );
        await checkThrows(
          () => store.getTransactions(oversizedIds),
          RangeError,
          "transaction read cap+1",
        );
        await checkThrows(
          () => store.getExistingUniqueKeys("table-t", oversizedIds),
          RangeError,
          "unique-key lookup cap+1",
        );
        let job = await store.createGarbageCollectionJob({
          id: "gc-prune-manifest-read",
          candidateManifestVersions: [first.version],
          candidateSegmentIds: [],
          candidateBlockIds: [],
          leaseCutoff: LATER,
          createdAt: T0,
        });
        while (job.state !== "completed") {
          job = (
            await store.runGarbageCollectionStep({
              jobId: job.id,
              expectedRevision: job.revision,
              maxItems: 16,
              updatedAt: LATER,
            })
          ).job;
        }
        checkEqual(
          await store.readManifestBlock(first.version, first.blockId),
          undefined,
          "a pruned manifest must not expose its former member",
        );
        store.close();
      },
    },
    {
      name: "table records round-trip, list sorted by name, and conflict by exact class",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("bravo"));
        await store.addTable(table("alpha"));
        const names = (await store.listTables()).map((record) => record.name);
        checkEqual(names, ["alpha", "bravo"], "listTables must sort by name");
        const byName = await store.getTableByName("alpha");
        check(byName?.id === "table-alpha", "getTableByName must resolve the record");
        await checkThrows(
          () => store.updateTable("table-alpha", 7, {}),
          TableRecordConflictError,
          "updateTable with a stale revision",
        );
        await checkThrows(
          () => store.removeTable("table-alpha", 7),
          TableRecordConflictError,
          "removeTable with a stale revision",
        );
        const updated = await store.updateTable("table-alpha", 0, {});
        check(updated.revision === 1, "updateTable must advance the revision");
        store.close();
      },
    },
    {
      name: "secondary-index metadata must describe one direction per indexed column",
      async run(target) {
        const store = await target.create();
        const record = table("bad-secondary");
        try {
          await store.addTable({
            ...record,
            secondaryIndexes: {
              bad: {
                name: "bad_direction_arity_index",
                columnId: "col-v",
                columnIds: ["col-v"],
                directions: ["asc", "desc"],
                termEncoding: "tuple-v1",
                storage: "postings-v1",
                storageColumnId: "secondary-index:bad",
                locator: "row-id",
                state: "ready",
                buildFromVersion: 0,
              },
            },
          });
          throw new Error("secondary-index metadata with mismatched direction arity was accepted");
        } catch (error) {
          check(error instanceof TypeError, "invalid secondary-index metadata must be rejected");
        }
        checkEqual(await store.listTables(), [], "a rejected catalog record must leave no table");
        store.close();
      },
    },
    {
      name: "removing a table removes everything keyed to it",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t", true));
        const { blockId } = await commitOne(store, "one", {
          keyTokens: ["number:1"],
          tableId: "table-t",
        });
        await store.reserveRowIds("table-t", 10);
        const dropped = await store.dropTable({
          tableId: "table-t",
          expectedTableRevision: 0,
          expectedManifestVersion: 0,
          expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
          committedAt: LATER,
        });
        check(dropped.version === 1, "dropTable must publish one successor manifest");
        checkEqual(
          await currentManifestBlockIds(store),
          [],
          "dropTable must retire the table's manifest blocks",
        );
        checkEqual(await tableSegments(store, "table-t"), [], "segments must go with the table");
        checkEqual(
          await store.getExistingUniqueKeys("table-t", ["number:1"]),
          [],
          "unique keys must go with the table",
        );
        await store.addTable(table("t", true));
        const range = await store.reserveRowIds("table-t", 1);
        check(range.start === 1n, "row-id counters must reset with the table");
        check(
          (await store.getBlock(blockId)) !== undefined,
          "blocks are not the catalog's to delete",
        );
        store.close();
      },
    },
    {
      name: "fused table drop rejects without change and resolves without partial state",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t"));
        const source = await commitOne(store, "source");
        const catalogEpoch = (await store.getCatalogProbe()).catalogEpoch;
        const drop = () =>
          store.dropTable({
            tableId: "table-t",
            expectedTableRevision: 0,
            expectedManifestVersion: source.version,
            expectedCatalogEpoch: catalogEpoch,
            committedAt: LATER,
          });
        await checkThrows(
          () =>
            store.dropTable({
              tableId: "table-t",
              expectedTableRevision: 9,
              expectedManifestVersion: source.version,
              expectedCatalogEpoch: catalogEpoch,
              committedAt: LATER,
            }),
          TableRecordConflictError,
          "dropping with a stale table revision",
        );
        await checkThrows(
          () =>
            store.dropTable({
              tableId: "table-t",
              expectedTableRevision: 0,
              expectedManifestVersion: source.version - 1,
              expectedCatalogEpoch: catalogEpoch,
              committedAt: LATER,
            }),
          WriteConflictError,
          "dropping with a stale manifest revision",
        );
        check(
          (await store.getTable("table-t")) !== undefined &&
            (await store.getCurrentManifestVersion()) === source.version,
          "a fused drop CAS refusal changed the catalog or manifest",
        );
        const active = transaction("txn-active-table", source.version);
        await store.createTransaction(active);
        const activeBlockId = "table/t/segment/active/part/000000";
        const activeSegment = segment("segment-active-table", "table-t", active.id, activeBlockId);
        const staged = await store.stageTransactionArtifacts({
          transactionId: active.id,
          expectedRevision: 0,
          blocks: [{ id: activeBlockId, bytes: Uint8Array.of(9, 8, 7, 6) }],
          segments: [activeSegment],
          updatedAt: LATER,
        });
        const activeRefusal = await checkThrows(
          drop,
          TableInUseError,
          "dropping a table with an active transaction",
        );
        checkEqual(
          {
            tableId: activeRefusal.tableId,
            ownerKind: activeRefusal.ownerKind,
            ownerId: activeRefusal.ownerId,
          },
          { tableId: "table-t", ownerKind: "transaction", ownerId: active.id },
          "an active-transaction refusal must identify its exact owner",
        );
        check(
          (await store.getTable("table-t")) !== undefined &&
            (await store.getSegment(activeSegment.id)) !== undefined &&
            (await store.getBlock(activeBlockId)) !== undefined &&
            (await store.getCurrentManifestVersion()) === source.version,
          "a refused table drop must leave catalog, manifest, and active artifacts unchanged",
        );
        await store.updateTransaction(active.id, staged.revision, {
          status: "aborted",
          updatedAt: LATER,
        });

        await store.createCompactionJob({
          id: "job-table-in-use",
          tableId: "table-t",
          sourceManifestVersion: source.version,
          sourceSegmentIds: ["segment-source"],
          sourceBlockIds: [source.blockId],
          outputBlockIds: [],
          cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
          processedRows: 0,
          sourceStoredBytes: 0,
          outputStoredBytes: 0,
          logicalBytes: 0,
          rewritePlan: { kind: "copy-v1" },
          outputCursor: null,
          memoryBudgetBytes: 0,
          minimumMemoryBytes: 0,
          level0SourceStoredBytes: 0,
          anchorSourceStoredBytes: 0,
          peakWorkingBytes: 0,
          outputLogicalBytes: 0,
          targetLevel: 1,
          state: "planned",
          transactionId: null,
          outputSegmentId: null,
          publishedVersion: null,
          revision: 0,
          createdAt: T0,
          updatedAt: T0,
        });
        const jobRefusal = await checkThrows(
          drop,
          TableInUseError,
          "dropping a table with a nonterminal compaction job",
        );
        checkEqual(
          {
            tableId: jobRefusal.tableId,
            ownerKind: jobRefusal.ownerKind,
            ownerId: jobRefusal.ownerId,
          },
          {
            tableId: "table-t",
            ownerKind: "compaction job",
            ownerId: "job-table-in-use",
          },
          "a compaction refusal must identify its exact owner",
        );
        check(
          (await store.getTable("table-t")) !== undefined &&
            (await store.getCurrentManifestVersion()) === source.version &&
            (await store.hasManifestBlocks(source.version, [source.blockId]))[0] === true,
          "a compaction refusal changed the catalog or manifest",
        );
        await store.cancelCompactionJob("job-table-in-use", 0, LATER);
        const published = await drop();
        check(
          published.version === source.version + 1,
          "a fused table drop must publish exactly one successor manifest",
        );
        check((await store.getTable("table-t")) === undefined, "a terminal owner blocked removal");
        checkEqual(await tableSegments(store, "table-t"), [], "a fused drop left table segments");
        checkEqual(await currentManifestBlockIds(store), [], "a fused drop left table blocks live");
        check(
          (await store.getBlock(source.blockId)) !== undefined,
          "a fused drop physically deleted a block pinned readers may still need",
        );
        store.close();
      },
    },
    {
      name: "counter reservations never overlap, even issued concurrently",
      async run(target) {
        const store = await target.create();
        const counterTable = table("t", true);
        const counterColumn = counterTable.columns[0];
        if (counterColumn === undefined) throw new Error("counter fixture column is missing");
        counterTable.columns[0] = {
          ...counterColumn,
          defaultValue: { kind: "autoincrement" },
        };
        await store.addTable(counterTable);
        const ranges = await Promise.all(
          Array.from({ length: 8 }, () => store.reserveRowIds("table-t", 5)),
        );
        const starts = new Set(ranges.map((range) => range.start));
        check(starts.size === 8, "concurrent reservations returned overlapping ranges");
        for (const range of ranges) {
          check(range.endExclusive - range.start === 5n, "a reservation returned the wrong span");
        }
        const bumped = await store.reserveAutoIncrement("table-t", "col-id", 3, 100n);
        check(bumped.start >= 100n, "reserveAutoIncrement must honor atLeast");

        const maximumExclusive = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
        const { snapshotVersion: _invalidSnapshot, ...invalidRecord } = transaction(
          "txn-invalid-counter",
          null,
        );
        void _invalidSnapshot;
        await checkThrows(
          () =>
            store.beginTransaction({
              record: invalidRecord,
              reserveAutoIncrement: {
                tableId: "table-t",
                columnId: "col-id",
                count: 0,
                atLeast: maximumExclusive + 1n,
              },
            }),
          RangeError,
          "an invalid fused auto-increment reservation",
        );
        check(
          (await store.getTransaction(invalidRecord.id)) === undefined,
          "a refused fused reservation must not create its transaction",
        );

        const { snapshotVersion: _validSnapshot, ...validRecord } = transaction(
          "txn-valid-counter",
          null,
        );
        void _validSnapshot;
        const valid = await store.beginTransaction({
          record: validRecord,
          reserveAutoIncrement: {
            tableId: "table-t",
            columnId: "col-id",
            count: 1,
            atLeast: 150n,
          },
        });
        check(
          valid.autoIncrementValues?.start === 150n &&
            valid.autoIncrementValues.endExclusive === 151n,
          "a valid fused reservation after refusal must use the unchanged counter",
        );
        await store.updateTransaction(valid.record.id, valid.record.revision, {
          status: "aborted",
          updatedAt: LATER,
        });
        await checkThrows(
          () => store.reserveAutoIncrement("table-t", "col-id", 0, maximumExclusive + 1n),
          RangeError,
          "an auto-increment bump outside the safe-integer range",
        );
        const afterRefusal = await store.reserveAutoIncrement("table-t", "col-id", 1, 200n);
        check(
          afterRefusal.start === 200n && afterRefusal.endExclusive === 201n,
          "a refused auto-increment bump must not mutate the counter",
        );
        const boundary = await store.reserveAutoIncrement(
          "table-t",
          "col-id",
          1,
          maximumExclusive - 1n,
        );
        check(
          boundary.endExclusive === maximumExclusive,
          "the final safe auto-increment value must remain reservable",
        );
        const exhausted = await store.reserveAutoIncrement(
          "table-t",
          "col-id",
          0,
          maximumExclusive,
        );
        check(
          exhausted.start === maximumExclusive && exhausted.endExclusive === maximumExclusive,
          "a zero-count bump to the exclusive boundary must be representable",
        );
        await checkThrows(
          () => store.reserveAutoIncrement("table-t", "col-id", 1),
          RangeError,
          "an auto-increment range crossing the safe-integer boundary",
        );
        store.close();
      },
    },
    {
      name: "transaction ownership renewal races expiry abort atomically",
      async run(target) {
        const store = await target.create();
        const record = transaction("txn-live", null);
        record.expiresAt = MIDDLE;
        await store.createTransaction(record);
        check(
          !(await store.renewTransaction({
            transactionId: record.id,
            ownerId: record.ownerId,
            expiresAtCutoff: MIDDLE,
            expiresAt: LATER,
          })),
          "ownership expired exactly at the cutoff must not be resurrected",
        );
        check(
          (await store.getTransaction(record.id))?.expiresAt === MIDDLE,
          "a refused expiry-boundary renewal mutated the record",
        );
        check(
          !(await store.renewTransaction({
            transactionId: record.id,
            ownerId: "wrong-owner",
            expiresAtCutoff: T0,
            expiresAt: LATER,
          })),
          "another owner must not renew a transaction",
        );
        check(
          (await store.getTransaction(record.id))?.revision === 0,
          "ownership renewal must not contend on the data revision",
        );

        const [renewed, aborted] = await Promise.all([
          store.renewTransaction({
            transactionId: record.id,
            ownerId: record.ownerId,
            expiresAtCutoff: T0,
            expiresAt: LATER,
          }),
          store.abortTransactionIfExpired({
            transactionId: record.id,
            expectedOwnerId: record.ownerId,
            expiresAtCutoff: MIDDLE,
            updatedAt: LATER,
          }),
        ]);
        const final = await store.getTransaction(record.id);
        check(final !== undefined, "the liveness race lost its transaction record");
        check(
          (renewed && aborted === undefined && final.status === "active") ||
            (!renewed && aborted?.status === "aborted" && final.status === "aborted"),
          "renewal and expiry abort must have one linearized winner",
        );
        if (final.status === "active") {
          check(final.expiresAt === LATER, "the winning renewal did not extend ownership");
          const expired = await store.abortTransactionIfExpired({
            transactionId: final.id,
            expectedOwnerId: final.ownerId,
            expiresAtCutoff: "9999-12-31T23:59:59.999Z",
            updatedAt: LATER,
          });
          check(expired?.status === "aborted", "an expired owner was not atomically aborted");
        }
        store.close();
      },
    },
    {
      name: "a commit is atomic and visible: manifest, record, segments, keys together",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t", true));
        const { version, blockId } = await commitOne(store, "one", {
          keyTokens: ["number:1"],
        });
        checkEqual(await store.getCurrentManifestVersion(), version, "the version must advance");
        const manifest = await store.getCurrentManifest();
        check(
          manifest !== undefined &&
            (await store.hasManifestBlocks(manifest.version, [blockId]))[0] === true,
          "the manifest must include the committed block",
        );
        const record = await store.getTransaction("txn-one");
        check(
          record?.status === "committed" && record.committedVersion === version,
          "the transaction record must flip to committed with the published version",
        );
        checkEqual(
          await store.getExistingUniqueKeys("table-t", ["number:1", "number:2"]),
          ["number:1"],
          "committed unique keys must be queryable",
        );
        const finalized = await store.getSegment("segment-one");
        check(
          finalized?.level === 0 && finalized.logicalOrder === version,
          "committed segments must be finalized with level and logical order",
        );
        store.close();
      },
    },
    {
      name: "commit conflicts are typed exactly, and a refused commit changes nothing",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t", true));
        await commitOne(store, "one");
        const staleVersion = null; // The world has moved on from the empty database.
        await store.createTransaction(transaction("txn-stale", staleVersion));
        await checkThrows(
          () =>
            store.commitTransaction({
              transactionId: "txn-stale",
              expectedTransactionRevision: 0,
              expectedManifestVersion: staleVersion,
              committedAt: LATER,
            }),
          WriteConflictError,
          "a commit against a moved manifest",
        );
        const movedVersion = await store.getCurrentManifestVersion();
        await checkThrows(
          () =>
            store.commitTransaction({
              transactionId: "txn-stale",
              expectedTransactionRevision: 41,
              expectedManifestVersion: movedVersion,
              committedAt: LATER,
            }),
          TransactionRecordConflictError,
          "a commit with a stale transaction revision",
        );
        const still = await store.getTransaction("txn-stale");
        check(still?.status === "active", "a refused commit must leave the transaction active");
        store.close();
      },
    },
    {
      name: "duplicate unique keys refuse with UniqueKeyConflictError and commit nothing",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t", true));
        const { version } = await commitOne(store, "one", { keyTokens: ["number:1"] });
        await store.createTransaction(transaction("txn-dup", version));
        await store.stageTransactionArtifacts({
          transactionId: "txn-dup",
          expectedRevision: 0,
          blocks: [{ id: "block-dup", bytes: Uint8Array.of(9) }],
          segments: [],
          updatedAt: T0,
        });
        await checkThrows(
          () =>
            store.commitTransaction({
              transactionId: "txn-dup",
              expectedTransactionRevision: 1,
              expectedManifestVersion: version,
              committedAt: LATER,
              uniqueKeyChanges: [
                { tableId: "table-t", keyTokens: ["number:1"], requireAbsent: true },
              ],
            }),
          UniqueKeyConflictError,
          "a commit inserting an existing unique key",
        );
        checkEqual(
          await store.getCurrentManifestVersion(),
          version,
          "a key conflict must not advance the version",
        );
        store.close();
      },
    },
    {
      name: "leases pin versions, renew by CAS, and expire honestly",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t"));
        const { version } = await commitOne(store, "one");
        await store.createLease({
          id: "lease-a",
          kind: "reader",
          manifestVersion: version,
          ownerId: "owner-a",
          createdAt: T0,
          expiresAt: LATER,
          revision: 0,
        });
        await checkThrows(
          () =>
            store.renewLease({
              id: "lease-a",
              expectedRevision: 9,
              expiresAtCutoff: T0,
              expiresAt: LATER,
            }),
          LeaseConflictError,
          "renewing with a stale revision",
        );
        const renewed = await store.renewLease({
          id: "lease-a",
          expectedRevision: 0,
          expiresAtCutoff: T0,
          expiresAt: LATER,
        });
        check(renewed.revision === 1, "a renewal must advance the revision");
        await checkThrows(
          () => store.removeLease({ id: "lease-a", ownerId: "wrong-owner" }),
          LeaseOwnerConflictError,
          "releasing another owner's lease",
        );
        check(
          (await store.getLease("lease-a"))?.revision === 1,
          "a refused lease release mutated the record",
        );
        checkEqual(
          await store.removeLeaseIfExpired("lease-a", 1, PAST),
          false,
          "an unexpired lease must not be removed",
        );
        checkEqual(
          await store.removeLeaseIfExpired("lease-a", 1, "3000-01-01T00:00:00.000Z"),
          true,
          "an expired lease must be removed",
        );
        await store.createLease({
          id: "lease-race",
          kind: "reader",
          manifestVersion: version,
          ownerId: "owner-a",
          createdAt: T0,
          expiresAt: LATER,
          revision: 0,
        });
        const [, release] = await Promise.allSettled([
          store.renewLease({
            id: "lease-race",
            expectedRevision: 0,
            expiresAtCutoff: T0,
            expiresAt: LATER,
          }),
          store.removeLease({ id: "lease-race", ownerId: "owner-a" }),
        ]);
        check(release.status === "fulfilled", "a same-owner renewal made release unsafe");
        check(
          (await store.getLease("lease-race")) === undefined,
          "same-owner release did not linearize after/before renewal",
        );
        store.close();
      },
    },
    {
      name: "garbage collection reclaims only unpinned, superseded state — atomically",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t"));
        const first = await commitOne(store, "one");
        // A fused table drop is the ordinary, intent-bound way to retire visible table bytes.
        await store.dropTable({
          tableId: "table-t",
          expectedTableRevision: 0,
          expectedManifestVersion: first.version,
          expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
          committedAt: LATER,
        });
        const retiredBeforeCollection = await store.listRetiredManifestBlockPage({
          removedThroughVersion: first.version + 1,
          afterBlockId: null,
          limit: 1,
        });
        checkEqual(
          retiredBeforeCollection.records.map((record) => record.blockId),
          [first.blockId],
          "retired provenance must remain independently pageable before collection",
        );
        const job = await store.createGarbageCollectionJob({
          id: "gc-1",
          candidateManifestVersions: [first.version],
          candidateSegmentIds: [],
          candidateBlockIds: [first.blockId],
          leaseCutoff: LATER,
          createdAt: LATER,
        });
        await checkThrows(
          () => store.removeGarbageCollectionJob(job.id),
          Error,
          "removing a non-completed garbage-collection job",
        );
        check(
          (await store.getGarbageCollectionJob(job.id))?.state === "planned",
          "a refused garbage-collection job removal mutated the record",
        );
        const step = await store.runGarbageCollectionStep({
          jobId: job.id,
          expectedRevision: job.revision,
          maxItems: 100,
          updatedAt: LATER,
        });
        checkEqual(
          step.prunedManifestVersions,
          [first.version],
          "the superseded version must prune",
        );
        checkEqual(step.reclaimedBlockIds, [first.blockId], "the superseded block must reclaim");
        checkEqual(await store.getBlock(first.blockId), undefined, "reclaimed bytes must be gone");
        checkEqual(
          (
            await store.listRetiredManifestBlockPage({
              removedThroughVersion: first.version + 1,
              afterBlockId: null,
              limit: 1,
            })
          ).records,
          [],
          "payload reclamation must atomically remove its retired provenance",
        );
        checkEqual(await store.getCurrentManifestVersion(), 1, "the successor manifest was lost");
        const pruned = await store.getManifest(first.version);
        check(pruned?.prunedAt !== undefined, "pruned manifests are tombstoned, not deleted");
        await store.removeGarbageCollectionJob(job.id);
        check(
          (await store.getGarbageCollectionJob(job.id)) === undefined,
          "a completed garbage-collection job was retained",
        );
        store.close();
      },
    },
    {
      name: "temp spill pages are isolated scratch; owner records conflict by exact class",
      async run(target) {
        const store = await target.create();
        await store.createTempOwner({
          ownerId: "o1",
          createdAt: T0,
          expiresAt: LATER,
          revision: 0,
        });
        await store.putTempRunPage({
          ownerId: "o1",
          runId: "r1",
          pageIndex: 0,
          bytes: Uint8Array.of(1),
        });
        const batched = store.putTempRunPages?.bind(store);
        if (batched !== undefined) {
          await batched([
            { ownerId: "o1", runId: "r1", pageIndex: 1, bytes: Uint8Array.of(2) },
            { ownerId: "o1", runId: "r2", pageIndex: 0, bytes: Uint8Array.of(3) },
          ]);
          checkEqual(
            await store.getTempRunPage("o1", "r1", 1),
            Uint8Array.of(2),
            "batched pages must read back identically",
          );
        }
        await store.removeTempRun("o1", "r1");
        checkEqual(
          await store.getTempRunPage("o1", "r1", 0),
          undefined,
          "a removed run must be gone",
        );
        await checkThrows(
          () =>
            store.renewTempOwner({
              ownerId: "o1",
              expectedRevision: 9,
              expiresAtCutoff: T0,
              expiresAt: LATER,
            }),
          TempOwnerConflictError,
          "renewing a temp owner with a stale revision",
        );
        checkEqual(
          await store.removeTempOwnerIfExpired("o1", PAST),
          false,
          "an unexpired owner must survive the sweep",
        );
        const listed = await store.listTempOwnerIdsPage(null, 10);
        check(listed.records.includes("o1"), "owners must be listable");
        store.close();
      },
    },
    {
      name: "compaction job records update by CAS with the exact conflict class",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t"));
        const { version, blockId } = await commitOne(store, "one");
        await store.createCompactionJob({
          id: "job-1",
          tableId: "table-t",
          sourceManifestVersion: version,
          sourceSegmentIds: ["segment-one"],
          sourceBlockIds: [blockId],
          outputBlockIds: [],
          cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
          processedRows: 0,
          sourceStoredBytes: 0,
          outputStoredBytes: 0,
          logicalBytes: 0,
          rewritePlan: { kind: "copy-v1" },
          outputCursor: null,
          memoryBudgetBytes: 0,
          minimumMemoryBytes: 0,
          level0SourceStoredBytes: 0,
          anchorSourceStoredBytes: 0,
          peakWorkingBytes: 0,
          outputLogicalBytes: 0,
          targetLevel: 1,
          state: "planned",
          transactionId: null,
          outputSegmentId: null,
          publishedVersion: null,
          revision: 0,
          createdAt: T0,
          updatedAt: T0,
        });
        await checkThrows(
          () => store.removeCompactionJob("job-1"),
          Error,
          "removing a nonterminal compaction job",
        );
        check(
          (await store.getCompactionJob("job-1"))?.state === "planned",
          "a refused compaction-job removal mutated the record",
        );
        await checkThrows(
          () =>
            store.updateCompactionJob("job-1", 9, {
              state: "running",
              transactionId: "txn-one",
              updatedAt: LATER,
            }),
          CompactionJobConflictError,
          "updating a compaction job with a stale revision",
        );
        const updated = await store.updateCompactionJob("job-1", 0, { updatedAt: LATER });
        check(updated.revision === 1, "a job update must advance the revision");
        await store.cancelCompactionJob("job-1", updated.revision, LATER);
        check(await store.removeCompactionJob("job-1"), "terminal job was not removed");
        check((await store.getCompactionJob("job-1")) === undefined, "terminal job was retained");

        await store.createTransaction(transaction("txn-orphan-output", version));
        await store.stageTransactionArtifacts({
          transactionId: "txn-orphan-output",
          expectedRevision: 0,
          blocks: [{ id: "orphan-compaction-output", bytes: Uint8Array.of(9) }],
          segments: [],
          updatedAt: T0,
        });
        await store.updateTransaction("txn-orphan-output", 1, {
          status: "aborted",
          updatedAt: LATER,
        });
        await store.createCompactionJob({
          id: "job-orphan-output",
          tableId: "table-t",
          sourceManifestVersion: version,
          sourceSegmentIds: ["segment-one"],
          sourceBlockIds: [blockId],
          outputBlockIds: ["orphan-compaction-output"],
          cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
          processedRows: 0,
          sourceStoredBytes: 0,
          outputStoredBytes: 1,
          logicalBytes: 1,
          rewritePlan: { kind: "copy-v1" },
          outputCursor: null,
          memoryBudgetBytes: 0,
          minimumMemoryBytes: 0,
          level0SourceStoredBytes: 0,
          anchorSourceStoredBytes: 0,
          peakWorkingBytes: 0,
          outputLogicalBytes: 1,
          targetLevel: 1,
          state: "cancelled",
          transactionId: null,
          outputSegmentId: null,
          publishedVersion: null,
          revision: 0,
          createdAt: T0,
          updatedAt: LATER,
        });
        check(
          await store.removeCompactionJob("job-orphan-output"),
          "terminal compaction metadata with independent transaction provenance was retained",
        );
        check(
          (await store.getBlock("orphan-compaction-output")) !== undefined,
          "removing terminal metadata deleted an independently proven output",
        );
        store.close();
      },
    },
    {
      name: "full-text bases serve exact and prefix candidates with honest coverage",
      async run(target) {
        const store = await target.create();
        await store.addTable({
          ...table("t"),
          ftsColumns: {
            "col-v": {
              storage: "fts-chunks-v1",
              tokenizerVersion: 1,
              state: "ready",
              buildFromVersion: 0,
            },
          },
        });
        await store.writeFtsBase("table-t", "col-v", {
          coversVersion: 0,
          chunks: [
            [
              { term: "minnow", rowIds: [1n, 3n], tf: [1, 2] },
              { term: "minnows", rowIds: [2n], tf: [1] },
            ],
            [{ term: "shark", rowIds: [4n], tf: [1] }],
          ],
          totalTokens: 5,
        });
        const exact = await store.readFtsCandidates(
          "table-t",
          "col-v",
          [{ term: "minnow", prefix: false }],
          10,
        );
        checkEqual(exact.rowIdsByTerm, [[1n, 3n]], "exact terms must match exactly");
        check(
          exact.hasBase && exact.coversVersion === 0 && exact.totalTokens === 5,
          "coverage and token totals must report the stored base",
        );
        const prefix = await store.readFtsCandidates(
          "table-t",
          "col-v",
          [{ term: "minnow", prefix: true }],
          10,
        );
        checkEqual(
          prefix.rowIdsByTerm,
          [[1n, 2n, 3n]],
          "prefix terms must match the term range, row ids ascending and unique",
        );
        const ordered = await store.readFtsPostings("table-t", "col-v", 10);
        checkEqual(
          ordered.postings,
          [
            { term: "minnow", rowIds: [1n, 3n], tf: [1, 2] },
            { term: "minnows", rowIds: [2n], tf: [1] },
            { term: "shark", rowIds: [4n], tf: [1] },
          ],
          "ordered postings must merge into canonical term and row-ID order",
        );
        store.close();
      },
    },
    {
      name: "postings builds publish bounded generations atomically and reclaim replacements",
      async run(target) {
        let store = await target.create();
        await store.addTable({
          ...table("postings"),
          ftsColumns: {
            "col-v": {
              storage: "fts-chunks-v1",
              tokenizerVersion: 1,
              state: "building",
              buildFromVersion: -1,
            },
          },
        });
        await store.beginFtsBaseBuild({
          tableId: "table-postings",
          columnId: "col-v",
          buildId: "abandoned",
          ownerId: "owner-abandoned",
          createdAt: "2099-01-01T00:00:00.000Z",
          expiresAt: "2099-01-01T00:10:00.000Z",
        });
        await store.writeFtsBaseBuildChunk({
          tableId: "table-postings",
          columnId: "col-v",
          buildId: "abandoned",
          ownerId: "owner-abandoned",
          expiresAtCutoff: "2099-01-01T00:00:01.000Z",
          expiresAt: "2099-01-01T00:10:00.000Z",
          updatedAt: "2099-01-01T00:00:01.000Z",
          ordinal: 0,
          chunk: [{ term: "old", rowIds: [1n], tf: [1] }],
        });
        await store.beginFtsBaseBuild({
          tableId: "table-postings",
          columnId: "col-v",
          buildId: "replacement",
          ownerId: "owner-replacement",
          createdAt: "2099-01-01T00:10:00.000Z",
          expiresAt: "2099-01-01T00:20:00.000Z",
        });
        const firstChunk: Parameters<BlockStore["writeFtsBaseBuildChunk"]>[0] = {
          tableId: "table-postings",
          columnId: "col-v",
          buildId: "replacement",
          ownerId: "owner-replacement",
          expiresAtCutoff: "2099-01-01T00:10:01.000Z",
          expiresAt: "2099-01-01T00:20:00.000Z",
          updatedAt: "2099-01-01T00:10:01.000Z",
          ordinal: 0,
          chunk: [{ term: "alpha", rowIds: [2n], tf: [1] }],
        };
        await store.writeFtsBaseBuildChunk(firstChunk);
        // Lost-ack replay is idempotent: retrying the identical chunk neither duplicates it nor
        // advances the ordinal, while changing acknowledged bytes is an ownership conflict.
        await store.writeFtsBaseBuildChunk(firstChunk);
        await checkThrows(
          () =>
            store.writeFtsBaseBuildChunk({
              ...firstChunk,
              chunk: [{ term: "alpha", rowIds: [999n], tf: [1] }],
            }),
          PostingBuildConflictError,
          "replaying an acknowledged postings ordinal with changed bytes",
        );
        if (target.reopen !== undefined) {
          store = await target.reopen(store);
          // The active owner and completed ordinal are durable too. Replaying after a process
          // restart must still be a no-op, not a duplicate allocation or a vanished build.
          await store.writeFtsBaseBuildChunk(firstChunk);
        }
        await store.writeFtsBaseBuildChunk({
          tableId: "table-postings",
          columnId: "col-v",
          buildId: "replacement",
          ownerId: "owner-replacement",
          expiresAtCutoff: "2099-01-01T00:10:02.000Z",
          expiresAt: "2099-01-01T00:20:00.000Z",
          updatedAt: "2099-01-01T00:10:02.000Z",
          ordinal: 1,
          chunk: [{ term: "omega", rowIds: [3n], tf: [1] }],
        });
        await store.finishFtsBaseBuild({
          tableId: "table-postings",
          columnId: "col-v",
          buildId: "replacement",
          ownerId: "owner-replacement",
          expiresAtCutoff: "2099-01-01T00:10:03.000Z",
          coversVersion: 4,
          chunkCount: 2,
          totalTokens: 2,
          completedAt: "2099-01-01T00:10:03.000Z",
        });
        let candidates = await store.readFtsCandidates(
          "table-postings",
          "col-v",
          [
            { term: "old", prefix: false },
            { lower: "alpha", lowerInclusive: true, upper: "omega", upperInclusive: true },
          ],
          4,
        );
        checkEqual(
          candidates.rowIdsByTerm,
          [[], [2n, 3n]],
          "only a finished replacement generation may become visible",
        );
        check(candidates.hasBase, "a finished generation must report a published base");

        if (target.reopen !== undefined) {
          store = await target.reopen(store);
          candidates = await store.readFtsCandidates(
            "table-postings",
            "col-v",
            [{ term: "omega", prefix: false }],
            4,
          );
          checkEqual(candidates.rowIdsByTerm, [[3n]], "a finished generation must survive reopen");
          check(candidates.hasBase, "a reopened generation must still report its base");
        }
        await checkThrows(
          () => store.removeFtsColumn("table-postings", "col-v"),
          Error,
          "removing postings still owned by a ready catalog accelerator",
        );
        const indexed = await store.getTable("table-postings");
        check(indexed !== undefined, "the postings table disappeared");
        await store.updateTable(indexed.id, indexed.revision, {
          ftsColumns: {
            ...indexed.ftsColumns,
            "col-v": {
              ...(indexed.ftsColumns?.["col-v"] ?? {
                storage: "fts-chunks-v1" as const,
                tokenizerVersion: 1,
                buildFromVersion: -1,
              }),
              state: "invalid",
            },
          },
        });
        await store.removeFtsColumn("table-postings", "col-v");
        candidates = await store.readFtsCandidates(
          "table-postings",
          "col-v",
          [{ term: "alpha", prefix: false }],
          4,
        );
        checkEqual(candidates.rowIdsByTerm, [[]], "removing an index must remove its candidates");
        check(!candidates.hasBase, "a removed base must be distinguishable from an empty base");
        store.close();
      },
    },
    {
      name: "UNIQUE-index membership seeds and drops atomically with its catalog record",
      async run(target) {
        const store = await target.create();
        const record = table("unique-secondary");
        const indexId = "unique-v";
        const namespaceId = secondaryUniqueKeyNamespace(record.id, indexId);
        const building = {
          name: "unique_secondary_v",
          columnId: "col-v",
          columnIds: ["col-v"],
          directions: ["asc" as const],
          unique: true as const,
          termEncoding: "tuple-v1" as const,
          storage: "postings-v1" as const,
          storageColumnId: "secondary-index:unique-v",
          locator: "row-id" as const,
          state: "building" as const,
          buildId: "builder",
          buildFromVersion: -1,
        };
        const { buildId: _buildId, ...withoutBuilder } = building;
        void _buildId;
        const ready = {
          ...withoutBuilder,
          state: "ready" as const,
          uniqueEnforced: true as const,
        };
        await store.addTable({ ...record, secondaryIndexes: { [indexId]: building } });
        try {
          await store.updateTable(record.id, 0, {
            secondaryIndexes: { [indexId]: ready },
            expectedManifestVersion: { value: null },
            uniqueKeySeed: { namespaceId: `${namespaceId}-wrong`, keyTokens: ["x"] },
          });
          throw new Error("a seed for the wrong namespace was accepted");
        } catch (error) {
          check(error instanceof TypeError, "a mismatched UNIQUE seed must be rejected");
        }
        check(
          (await store.getTable(record.id))?.revision === 0,
          "a mismatched seed must not mutate the catalog",
        );
        await checkThrows(
          () =>
            store.updateTable(record.id, 0, {
              secondaryIndexes: {
                [indexId]: ready,
              },
              expectedManifestVersion: { value: null },
              uniqueKeySeed: { namespaceId, keyTokens: ["x", "x"] },
            }),
          UniqueKeyConflictError,
          "a duplicate UNIQUE seed",
        );
        check(
          (await store.getTable(record.id))?.secondaryIndexes?.[indexId]?.state === "building",
          "a refused seed must not publish the ready catalog state",
        );
        await store.updateTable(record.id, 0, {
          secondaryIndexes: {
            [indexId]: ready,
          },
          expectedManifestVersion: { value: null },
          uniqueKeySeed: { namespaceId, keyTokens: ["x", "y"] },
        });
        checkEqual(
          await store.getExistingUniqueKeys(namespaceId, ["x", "z"]),
          ["x"],
          "the ready catalog and membership must publish together",
        );
        await store.updateTable(record.id, 1, { secondaryIndexes: null });
        checkEqual(
          await store.getExistingUniqueKeys(namespaceId, ["x", "y"]),
          [],
          "dropping the index must reclaim its membership namespace",
        );
        store.close();
      },
    },
    {
      name: "atomic staging and rollback keep journals and artifacts inseparable",
      async run(target) {
        let store = await target.create();
        await store.addTable(table("t"));
        {
          const begun = await store.beginTransaction({
            record: {
              id: "txn-b",
              ownerId: "txn-b/owner",
              expiresAt: LATER,
              pendingBlockIds: [],
              pendingSegmentIds: [],
              status: "active",
              revision: 0,
              startedAt: T0,
              updatedAt: T0,
              committedVersion: null,
            },
            reserveRowIds: { tableId: "table-t", count: 4 },
          });
          check(
            begun.record.snapshotVersion === (await store.getCurrentManifestVersion()),
            "beginTransaction must pin the current version",
          );
          check(
            begun.rowIds !== undefined && begun.rowIds.endExclusive - begun.rowIds.start === 4n,
            "beginTransaction must honor the reservation",
          );
        }
        const stage = store.stageTransactionArtifacts.bind(store);
        await store.createTransaction(
          transaction("txn-s", await store.getCurrentManifestVersion()),
        );
        const duplicate = segment("segment-duplicate", "table-t", "txn-s", "block-none");
        await checkThrows(
          () =>
            stage({
              transactionId: "txn-s",
              expectedRevision: 0,
              blocks: [],
              segments: [duplicate, duplicate],
              updatedAt: LATER,
            }),
          Error,
          "staging duplicate segment IDs",
        );
        await checkThrows(
          () =>
            stage({
              transactionId: "txn-s",
              expectedRevision: 0,
              blocks: [],
              segments: [segment("segment-foreign", "table-t", "another-txn", "block-none")],
              updatedAt: LATER,
            }),
          Error,
          "staging a segment owned by another transaction",
        );
        check(
          (await store.getSegment("segment-duplicate")) === undefined &&
            (await store.getSegment("segment-foreign")) === undefined &&
            (await store.getTransaction("txn-s"))?.revision === 0,
          "refused staging must leave the transaction and segment store unchanged",
        );
        const first = await stage({
          transactionId: "txn-s",
          expectedRevision: 0,
          blocks: [{ id: "block-z", bytes: Uint8Array.of(7) }],
          segments: [segment("segment-z", "table-t", "txn-s", "block-z")],
          updatedAt: LATER,
        });
        await checkThrows(
          () =>
            stage({
              transactionId: "txn-s",
              expectedRevision: first.revision,
              blocks: [{ id: "block-gap", bytes: Uint8Array.of(9) }],
              segments: [segment("segment-gap", "table-t", "txn-s", "block-gap", 0)],
              updatedAt: LATER,
            }),
          TypeError,
          "staging a segment whose commit ordinal does not continue the journal",
        );
        check(
          (await store.getTransaction("txn-s"))?.revision === first.revision &&
            (await store.getBlock("block-gap")) === undefined &&
            (await store.getSegment("segment-gap")) === undefined,
          "an ordinal refusal must leave the record, block, and segment unchanged",
        );
        const staged = await stage({
          transactionId: "txn-s",
          expectedRevision: first.revision,
          blocks: [{ id: "block-a", bytes: Uint8Array.of(8) }],
          segments: [segment("segment-a", "table-t", "txn-s", "block-a", 1)],
          updatedAt: LATER,
        });
        checkEqual(staged.pendingBlockIds, ["block-z", "block-a"], "staging journals blocks");
        checkEqual(
          staged.pendingSegmentIds,
          ["segment-z", "segment-a"],
          "staging preserves the canonical segment journal order",
        );
        check(
          (await store.getBlock("block-a")) !== undefined &&
            (await store.getSegment("segment-a")) !== undefined,
          "staging must write bytes and segments",
        );
        const rollbackInput = {
          transactionId: "txn-s",
          expectedRevision: staged.revision,
          pendingBlockIds: ["block-z"],
          pendingSegmentIds: ["segment-z"],
          removeBlockIds: ["block-a"],
          removeSegmentIds: ["segment-a"],
          updatedAt: LATER,
        } as const;
        await checkThrows(
          () =>
            store.rollbackTransactionArtifacts({
              ...rollbackInput,
              expectedRevision: staged.revision + 1,
            }),
          TransactionRecordConflictError,
          "rollback with a stale transaction revision",
        );
        await checkThrows(
          () =>
            store.rollbackTransactionArtifacts({
              ...rollbackInput,
              removeBlockIds: [],
            }),
          TypeError,
          "rollback whose lists do not exactly partition the journal",
        );
        await checkThrows(
          () =>
            store.rollbackTransactionArtifacts({
              ...rollbackInput,
              pendingBlockIds: ["block-a"],
              pendingSegmentIds: ["segment-a"],
              removeBlockIds: ["block-z"],
              removeSegmentIds: ["segment-z"],
            }),
          TypeError,
          "rollback that retains a non-prefix journal partition",
        );
        check(
          (await store.getBlock("block-a")) !== undefined &&
            (await store.getSegment("segment-a")) !== undefined,
          "a refused rollback must leave every artifact intact",
        );
        const rolledBack = await store.rollbackTransactionArtifacts(rollbackInput);
        checkEqual(
          rolledBack.pendingBlockIds,
          ["block-z"],
          "rollback must retain checkpoint block",
        );
        check(
          (await store.getBlock("block-a")) === undefined &&
            (await store.getSegment("segment-a")) === undefined &&
            (await store.getBlock("block-z")) !== undefined &&
            (await store.getSegment("segment-z")) !== undefined,
          "rollback must delete only its exact removed partition",
        );
        if (target.reopen !== undefined) {
          const reopened = await target.reopen(store);
          checkEqual(
            (await reopened.getTransaction("txn-s"))?.pendingBlockIds,
            ["block-z"],
            "rollback journal must survive reopen",
          );
          check(
            (await reopened.getBlock("block-a")) === undefined &&
              (await reopened.getBlock("block-z")) !== undefined,
            "rollback artifact removal must survive reopen",
          );
          store = reopened;
        }
        const before = await store.getCatalogProbe();
        await store.addTable(table("probe-check"));
        const after = await store.getCatalogProbe();
        check(
          after.catalogEpoch > before.catalogEpoch,
          "a catalog mutation must advance the probe's epoch",
        );
        store.close();
      },
    },
    {
      name: "the single-shot write, when present, commits atomically and refuses typed",
      async run(target) {
        const store = await target.create();
        const write = store.writeTransaction?.bind(store);
        if (write === undefined) {
          store.close();
          return;
        }
        await store.addTable(table("t", true));
        const schemaEpoch = (await store.getCatalogProbe()).schemaEpoch;
        const fresh = (id: string): Omit<TransactionRecord, "snapshotVersion"> => {
          const { snapshotVersion: _pinned, ...record } = transaction(id, null);
          void _pinned;
          return { ...record, schemaEpochGuard: schemaEpoch };
        };
        // A fresh record: begun, staged, and committed in the one step.
        const first = await write({
          transaction: { record: fresh("txn-w1") },
          blocks: [{ id: "block-w1", bytes: Uint8Array.of(1) }],
          segments: [segment("segment-w1", "table-t", "txn-w1", "block-w1")],
          expectedManifestVersion: null,
          levelZeroSegmentLimits: [{ tableId: "table-t", limit: MAX_LEVEL_ZERO_SEGMENTS }],
          uniqueKeyChanges: [{ tableId: "table-t", keyTokens: ["number:1"], requireAbsent: true }],
          committedAt: LATER,
        });
        checkEqual(first.version, 0, "the first single-shot write must publish version 0");
        const committed = await store.getTransaction("txn-w1");
        check(
          committed?.status === "committed" &&
            committed.committedVersion === 0 &&
            committed.snapshotVersion === null,
          "the fresh record must end committed, pinned at the expected version",
        );
        checkEqual(
          committed.pendingBlockIds,
          ["block-w1"],
          "the single-shot write must journal its blocks on the record",
        );
        check(
          (await store.getBlock("block-w1")) !== undefined &&
            (await store.getSegment("segment-w1"))?.logicalOrder === 0,
          "the single-shot write must land bytes and finalized segments",
        );
        checkEqual(
          await store.getExistingUniqueKeys("table-t", ["number:1"]),
          ["number:1"],
          "the single-shot write must apply unique-key changes",
        );
        // A stale version refuses as WriteConflictError and leaves nothing behind — not even
        // the record.
        await checkThrows(
          () =>
            write({
              transaction: { record: fresh("txn-stale") },
              blocks: [{ id: "block-stale", bytes: Uint8Array.of(2) }],
              segments: [segment("segment-stale", "table-t", "txn-stale", "block-stale")],
              expectedManifestVersion: null,
              levelZeroSegmentLimits: [{ tableId: "table-t", limit: MAX_LEVEL_ZERO_SEGMENTS }],
              committedAt: LATER,
            }),
          WriteConflictError,
          "a single-shot write against a moved manifest",
        );
        check(
          (await store.getTransaction("txn-stale")) === undefined &&
            (await store.getBlock("block-stale")) === undefined &&
            (await store.getSegment("segment-stale")) === undefined,
          "a refused single-shot write must leave no record, block, or segment",
        );
        // So does a duplicate unique key.
        await checkThrows(
          () =>
            write({
              transaction: { record: fresh("txn-dup") },
              blocks: [{ id: "block-dup", bytes: Uint8Array.of(3) }],
              segments: [segment("segment-dup", "table-t", "txn-dup", "block-dup")],
              expectedManifestVersion: 0,
              levelZeroSegmentLimits: [{ tableId: "table-t", limit: MAX_LEVEL_ZERO_SEGMENTS }],
              uniqueKeyChanges: [
                { tableId: "table-t", keyTokens: ["number:1"], requireAbsent: true },
              ],
              committedAt: LATER,
            }),
          UniqueKeyConflictError,
          "a single-shot write inserting an existing unique key",
        );
        check(
          (await store.getTransaction("txn-dup")) === undefined &&
            (await store.getBlock("block-dup")) === undefined,
          "a key conflict must leave no record or block",
        );
        checkEqual(await store.getCurrentManifestVersion(), 0, "refusals must not advance");
        // A begun transaction — the shape a reservation needs — continues into the same step.
        await store.createTransaction(transaction("txn-w2", 0));
        await checkThrows(
          () =>
            write({
              transaction: { id: "txn-w2", expectedRevision: 4 },
              blocks: [{ id: "block-w2", bytes: Uint8Array.of(4) }],
              segments: [segment("segment-w2", "table-t", "txn-w2", "block-w2")],
              expectedManifestVersion: 0,
              levelZeroSegmentLimits: [{ tableId: "table-t", limit: MAX_LEVEL_ZERO_SEGMENTS }],
              committedAt: LATER,
            }),
          TransactionRecordConflictError,
          "a single-shot write with a stale transaction revision",
        );
        check(
          (await store.getBlock("block-w2")) === undefined,
          "a revision conflict must leave no block",
        );
        const second = await write({
          transaction: { id: "txn-w2", expectedRevision: 0 },
          blocks: [{ id: "block-w2", bytes: Uint8Array.of(4) }],
          segments: [segment("segment-w2", "table-t", "txn-w2", "block-w2")],
          expectedManifestVersion: 0,
          levelZeroSegmentLimits: [{ tableId: "table-t", limit: MAX_LEVEL_ZERO_SEGMENTS }],
          changedTableIds: ["table-t"],
          committedAt: LATER,
        });
        checkEqual(second.version, 1, "the continued write must publish the next version");
        checkEqual(second.changedTableIds, ["table-t"], "the summary must carry the change set");
        checkEqual(
          await manifestBlockIds(store, 1),
          ["block-w1", "block-w2"],
          "the continued write must preserve visible blocks and add its own",
        );
        check(
          (await store.getTransaction("txn-w2"))?.status === "committed",
          "the begun record must end committed",
        );
        store.close();
      },
    },
    {
      name: "compaction commit proves exact output provenance before publication",
      async run(target) {
        const store = await target.create();
        await store.addTable({
          id: "table-compact",
          name: "compact",
          columns: [{ id: "col-value", name: "value", type: "number", nullable: false }],
          managed: false,
          revision: 0,
          createdAt: T0,
        });
        await store.createTransaction(transaction("txn-compact-source", null));
        const source = await store.stageTransactionArtifacts({
          transactionId: "txn-compact-source",
          expectedRevision: 0,
          blocks: [{ id: "compact-source-block", bytes: Uint8Array.of(1) }],
          segments: [
            {
              ...segment(
                "compact-source-segment",
                "table-compact",
                "txn-compact-source",
                "compact-source-block",
              ),
              columnBlockIds: { "col-value": ["compact-source-block"] },
            },
          ],
          updatedAt: T0,
        });
        await store.commitTransaction({
          transactionId: source.id,
          expectedTransactionRevision: source.revision,
          expectedManifestVersion: null,
          levelZeroSegmentLimits: [{ tableId: "table-compact", limit: MAX_LEVEL_ZERO_SEGMENTS }],
          committedAt: T0,
        });

        const jobId = "compact-proof-job";
        const outputBlockId = `${jobId}/output/segment/000000/column/000000/part/000000`;
        await store.createTransaction(transaction("txn-compact-output", 0));
        const output = await store.stageTransactionArtifacts({
          transactionId: "txn-compact-output",
          expectedRevision: 0,
          blocks: [{ id: outputBlockId, bytes: Uint8Array.of(2) }],
          segments: [
            {
              ...segment(
                "compact-output-segment",
                "table-compact",
                "txn-compact-output",
                outputBlockId,
              ),
              columnBlockIds: { "col-value": [outputBlockId] },
              // Structurally valid, but copy-v1 is required to publish an insert segment.
              kind: "base",
              level: 1,
            },
          ],
          updatedAt: LATER,
        });
        const job: CompactionJobRecord = {
          id: jobId,
          tableId: "table-compact",
          sourceManifestVersion: 0,
          sourceSegmentIds: ["compact-source-segment"],
          sourceBlockIds: ["compact-source-block"],
          outputBlockIds: [outputBlockId],
          cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
          processedRows: 2,
          sourceStoredBytes: 1,
          outputStoredBytes: 1,
          logicalBytes: 1,
          rewritePlan: { kind: "copy-v1" },
          outputCursor: null,
          memoryBudgetBytes: 0,
          minimumMemoryBytes: 0,
          level0SourceStoredBytes: 1,
          anchorSourceStoredBytes: 0,
          peakWorkingBytes: 0,
          outputLogicalBytes: 1,
          targetLevel: 1,
          state: "ready",
          transactionId: output.id,
          outputSegmentId: "compact-output-segment",
          publishedVersion: null,
          revision: 0,
          createdAt: T0,
          updatedAt: LATER,
        };
        await store.createCompactionJob(job);
        await checkThrows(
          () =>
            store.commitTransaction({
              transactionId: output.id,
              expectedTransactionRevision: output.revision,
              expectedManifestVersion: 0,
              compactionJobId: job.id,
              removedBlockIds: ["compact-source-block"],
              committedAt: LATER,
            }),
          Error,
          "a structurally valid compaction segment that differs from the immutable plan",
        );
        checkEqual(
          await store.getCurrentManifestVersion(),
          0,
          "a provenance refusal must not publish a manifest",
        );
        check(
          (await store.getTransaction(output.id))?.status === "active" &&
            (await store.getSegment("compact-output-segment"))?.kind === "base" &&
            (await store.getBlock(outputBlockId)) !== undefined &&
            (await store.getCompactionJob(job.id))?.state === "ready",
          "a provenance refusal must leave the transaction, artifacts, and job unchanged",
        );
        checkEqual(
          await manifestBlockIds(store, 0),
          ["compact-source-block"],
          "a provenance refusal must retain every source block",
        );
        store.close();
      },
    },
    {
      name: "the lease move, when present, re-pins in place and refuses typed",
      async run(target) {
        const store = await target.create();
        const move = store.moveLease?.bind(store);
        if (move === undefined) {
          store.close();
          return;
        }
        await store.addTable(table("t"));
        const first = await commitOne(store, "one");
        const second = await commitOne(store, "two");
        await store.createLease({
          id: "lease-m",
          kind: "reader",
          manifestVersion: first.version,
          ownerId: "owner-m",
          createdAt: T0,
          expiresAt: LATER,
          revision: 0,
        });
        await checkThrows(
          () =>
            move({
              id: "lease-m",
              expectedRevision: 9,
              manifestVersion: second.version,
              expiresAtCutoff: T0,
              expiresAt: LATER,
            }),
          LeaseConflictError,
          "moving with a stale revision",
        );
        await checkThrows(
          () =>
            move({
              id: "lease-m",
              expectedRevision: 0,
              manifestVersion: 999,
              expiresAtCutoff: T0,
              expiresAt: LATER,
            }),
          SnapshotManifestMissingError,
          "moving to a version with no manifest",
        );
        checkEqual(
          await store.getLease("lease-m"),
          {
            id: "lease-m",
            kind: "reader",
            manifestVersion: first.version,
            ownerId: "owner-m",
            createdAt: T0,
            expiresAt: LATER,
            revision: 0,
          },
          "a refused move must leave the lease exactly as it was",
        );
        const moved = await move({
          id: "lease-m",
          expectedRevision: 0,
          manifestVersion: second.version,
          expiresAtCutoff: T0,
          expiresAt: LATER,
        });
        checkEqual(
          moved,
          {
            id: "lease-m",
            kind: "reader",
            manifestVersion: second.version,
            ownerId: "owner-m",
            createdAt: T0,
            expiresAt: LATER,
            revision: 1,
          },
          "a move must re-pin, renew, and advance the revision",
        );
        checkEqual(
          (await store.listLeases()).map((lease) => lease.id),
          ["lease-m"],
          "a move must keep the one record, not create a second",
        );
        store.close();
      },
    },
    {
      name: "everything committed survives a reopen",
      async run(target) {
        if (target.reopen === undefined) return;
        let store = await target.create();
        await store.addTable(table("t", true));
        let { version, blockId } = await commitOne(store, "one", { keyTokens: ["number:1"] });
        const reserved = await store.reserveRowIds("table-t", 10);
        // The single-shot write and the lease move, when present, must be just as durable.
        const write = store.writeTransaction?.bind(store);
        if (write !== undefined) {
          const { snapshotVersion: _pinned, ...record } = transaction("txn-single", null);
          void _pinned;
          const summary = await write({
            transaction: {
              record: {
                ...record,
                schemaEpochGuard: (await store.getCatalogProbe()).schemaEpoch,
              },
            },
            blocks: [{ id: "block-single", bytes: Uint8Array.of(1, 2, 3, 4) }],
            segments: [segment("segment-single", "table-t", "txn-single", "block-single")],
            expectedManifestVersion: version,
            levelZeroSegmentLimits: [{ tableId: "table-t", limit: MAX_LEVEL_ZERO_SEGMENTS }],
            uniqueKeyChanges: [
              { tableId: "table-t", keyTokens: ["number:1"], requireAbsent: false },
            ],
            committedAt: LATER,
          });
          version = summary.version;
          blockId = "block-single";
        }
        const move = store.moveLease?.bind(store);
        if (move !== undefined) {
          await store.createLease({
            id: "lease-durable",
            kind: "reader",
            manifestVersion: null,
            ownerId: "owner",
            createdAt: T0,
            expiresAt: LATER,
            revision: 0,
          });
          await move({
            id: "lease-durable",
            expectedRevision: 0,
            manifestVersion: version,
            expiresAtCutoff: T0,
            expiresAt: LATER,
          });
        }
        store = await target.reopen(store);

        checkEqual(
          await store.getCurrentManifestVersion(),
          version,
          "the committed version must survive a reopen",
        );
        if (move !== undefined) {
          checkEqual(
            (await store.getLease("lease-durable"))?.manifestVersion,
            version,
            "a moved lease must survive a reopen at its new version",
          );
        }
        checkEqual(
          await store.getBlock(blockId),
          Uint8Array.of(1, 2, 3, 4),
          "committed bytes must survive a reopen",
        );
        checkEqual(
          await store.getExistingUniqueKeys("table-t", ["number:1"]),
          ["number:1"],
          "unique-key membership must survive a reopen",
        );
        check((await store.getTableByName("t")) !== undefined, "the catalog must survive a reopen");
        const next = await store.reserveRowIds("table-t", 1);
        check(
          next.start >= reserved.endExclusive,
          "counters must continue past the reserved high-water mark after a reopen",
        );
        store.close();
      },
    },
  ];
}
