import {
  CompactionJobConflictError,
  LeaseConflictError,
  SnapshotManifestMissingError,
  TableRecordConflictError,
  TempOwnerConflictError,
  TransactionRecordConflictError,
  UniqueKeyConflictError,
  WriteConflictError,
  type BlockStore,
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

// ------------------------------------------------------------------------------------------
// Fixtures — the smallest records that exercise each rule.
// ------------------------------------------------------------------------------------------

const T0 = "2026-01-01T00:00:00.000Z";
const LATER = "2026-01-01T01:00:00.000Z";
const FAR_FUTURE = "2999-01-01T00:00:00.000Z";
const PAST = "2000-01-01T00:00:00.000Z";

function table(name: string, keyed = false): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [
      { id: "col-id", name: "id", type: "number", nullable: false },
      { id: "col-v", name: "v", type: "string", nullable: true },
    ],
    revision: 0,
    createdAt: T0,
    ...(keyed ? { uniqueKeyColumnId: "col-id" } : {}),
  };
}

function transaction(id: string, snapshotVersion: number | null): TransactionRecord {
  return {
    id,
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
): SegmentRecord {
  return {
    id,
    tableId,
    transactionId,
    rowCount: 2,
    rowIdStart: 1n,
    rowIdEndExclusive: 3n,
    columnBlockIds: { "col-id": [blockId], "col-v": [blockId] },
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
  await store.addBlock(blockId, Uint8Array.of(1, 2, 3, 4));
  await store.addSegment(
    segment(`segment-${suffix}`, options.tableId ?? "table-t", transactionId, blockId),
  );
  await store.updateTransaction(transactionId, 0, {
    pendingBlockIds: [blockId],
    pendingSegmentIds: [`segment-${suffix}`],
    updatedAt: T0,
  });
  const summary = await store.commitTransaction({
    transactionId,
    expectedTransactionRevision: 1,
    expectedManifestVersion: version,
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
      name: "blocks are immutable, copied both directions, and duplicates are refused",
      async run(target) {
        const store = await target.create();
        const bytes = Uint8Array.of(1, 2, 3);
        await store.addBlock("block-a", bytes);
        bytes[0] = 99; // The store must not alias the caller's buffer.
        const first = await store.getBlock("block-a");
        check(first?.[0] === 1, "stored bytes changed with the caller's buffer");
        first[1] = 99; // Nor may the caller's mutations reach the store.
        const second = await store.getBlock("block-a");
        check(second?.[1] === 2, "returned bytes alias the store's copy");
        try {
          await store.addBlock("block-a", Uint8Array.of(9));
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
      name: "a block batch with any duplicate writes nothing at all",
      async run(target) {
        const store = await target.create();
        await store.addBlock("block-a", Uint8Array.of(1));
        try {
          await store.addBlocks([
            { id: "block-b", bytes: Uint8Array.of(2) },
            { id: "block-a", bytes: Uint8Array.of(3) },
            { id: "block-c", bytes: Uint8Array.of(4) },
          ]);
          throw new Error("a batch containing an existing id was accepted");
        } catch {
          // Expected; what matters is that nothing landed.
        }
        checkEqual(await store.getBlock("block-b"), undefined, "a failed batch leaked block-b");
        checkEqual(await store.getBlock("block-c"), undefined, "a failed batch leaked block-c");
        checkEqual(
          await store.listBlockIds(),
          ["block-a"],
          "listBlockIds must be sorted and exact",
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
      name: "removing a table removes everything keyed to it",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t", true));
        const { blockId } = await commitOne(store, "one", {
          keyTokens: ["number:1"],
          tableId: "table-t",
        });
        await store.reserveRowIds("table-t", 10);
        await store.removeTable("table-t", 0);
        checkEqual(await store.listSegments("table-t"), [], "segments must go with the table");
        checkEqual(
          await store.getExistingUniqueKeys("table-t", ["number:1"]),
          [],
          "unique keys must go with the table",
        );
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
      name: "counter reservations never overlap, even issued concurrently",
      async run(target) {
        const store = await target.create();
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
          manifest?.blockIds.includes(blockId) === true,
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
        await store.addBlock("block-dup", Uint8Array.of(9));
        await store.updateTransaction("txn-dup", 0, {
          pendingBlockIds: ["block-dup"],
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
      name: "publishManifest compare-and-swaps and refuses missing blocks",
      async run(target) {
        const store = await target.create();
        await store.addBlock("block-a", Uint8Array.of(1));
        await checkThrows(
          () => store.publishManifest({ expectedVersion: 3, blockIds: ["block-a"], createdAt: T0 }),
          WriteConflictError,
          "publishing against the wrong expected version",
        );
        try {
          await store.publishManifest({
            expectedVersion: null,
            blockIds: ["missing"],
            createdAt: T0,
          });
          throw new Error("a manifest referencing a missing block was published");
        } catch (error) {
          check(
            !(error instanceof WriteConflictError),
            "missing blocks are not a version conflict",
          );
        }
        const manifest = await store.publishManifest({
          expectedVersion: null,
          blockIds: ["block-a"],
          createdAt: T0,
        });
        check(manifest.version === 0, "the first published version must be 0");
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
          expiresAt: FAR_FUTURE,
          revision: 0,
        });
        await checkThrows(
          () => store.renewLease("lease-a", 9, FAR_FUTURE),
          LeaseConflictError,
          "renewing with a stale revision",
        );
        const renewed = await store.renewLease("lease-a", 0, FAR_FUTURE);
        check(renewed.revision === 1, "a renewal must advance the revision");
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
        store.close();
      },
    },
    {
      name: "garbage collection reclaims only unpinned, superseded state — atomically",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t"));
        const first = await commitOne(store, "one");
        // Supersede the first commit's block so version 0 becomes collectable.
        const transactionId = "txn-two";
        await store.createTransaction(transaction(transactionId, first.version));
        await store.addBlock("block-two", Uint8Array.of(5));
        await store.addSegment(segment("segment-two", "table-t", transactionId, "block-two"));
        await store.updateTransaction(transactionId, 0, {
          pendingBlockIds: ["block-two"],
          pendingSegmentIds: ["segment-two"],
          updatedAt: T0,
        });
        await store.commitTransaction({
          transactionId,
          expectedTransactionRevision: 1,
          expectedManifestVersion: first.version,
          removedBlockIds: [first.blockId],
          committedAt: LATER,
        });
        const job = await store.createGarbageCollectionJob({
          id: "gc-1",
          candidateManifestVersions: [first.version],
          candidateSegmentIds: ["segment-one"],
          candidateBlockIds: [first.blockId],
          leaseCutoff: LATER,
          createdAt: LATER,
        });
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
        check((await store.getBlock("block-two")) !== undefined, "live bytes must survive");
        const pruned = await store.getManifest(first.version);
        check(pruned?.prunedAt !== undefined, "pruned manifests are tombstoned, not deleted");
        store.close();
      },
    },
    {
      name: "temp spill pages are isolated scratch; owner records conflict by exact class",
      async run(target) {
        const store = await target.create();
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
        await store.createTempOwner({ ownerId: "o1", expiresAt: FAR_FUTURE, revision: 0 });
        await checkThrows(
          () => store.renewTempOwner("o1", 9, FAR_FUTURE),
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
          () =>
            store.updateCompactionJob("job-1", 9, {
              state: "running",
              transactionId: "txn-one",
              updatedAt: LATER,
            }),
          CompactionJobConflictError,
          "updating a compaction job with a stale revision",
        );
        const updated = await store.updateCompactionJob("job-1", 0, {
          state: "running",
          transactionId: "txn-one",
          updatedAt: LATER,
        });
        check(updated.revision === 1, "a job update must advance the revision");
        store.close();
      },
    },
    {
      name: "full-text bases serve exact and prefix candidates with honest coverage",
      async run(target) {
        const store = await target.create();
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
          exact.coversVersion === 0 && exact.totalTokens === 5,
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
        store.close();
      },
    },
    {
      name: "optional atomic methods, when present, equal the sequential calls",
      async run(target) {
        const store = await target.create();
        await store.addTable(table("t"));
        const begin = store.beginTransaction?.bind(store);
        if (begin !== undefined) {
          const begun = await begin({
            record: {
              id: "txn-b",
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
        const stage = store.stageTransactionArtifacts?.bind(store);
        if (stage !== undefined) {
          await store.createTransaction(
            transaction("txn-s", await store.getCurrentManifestVersion()),
          );
          const staged = await stage({
            transactionId: "txn-s",
            expectedRevision: 0,
            blocks: [{ id: "block-staged", bytes: Uint8Array.of(7) }],
            segments: [segment("segment-staged", "table-t", "txn-s", "block-staged")],
            updatedAt: LATER,
          });
          checkEqual(staged.pendingBlockIds, ["block-staged"], "staging must journal the block");
          check(
            (await store.getBlock("block-staged")) !== undefined,
            "staging must write the bytes",
          );
          check(
            (await store.getSegment("segment-staged")) !== undefined,
            "staging must write the segment",
          );
        }
        const probe = store.getCatalogProbe?.bind(store);
        if (probe !== undefined) {
          const before = await probe();
          await store.addTable(table("probe-check"));
          const after = await probe();
          check(
            after.catalogEpoch > before.catalogEpoch,
            "a catalog mutation must advance the probe's epoch",
          );
        }
        const catalogState = store.getQueryCatalogState?.bind(store);
        if (catalogState !== undefined) {
          const state = await catalogState(["t", "missing-table"]);
          check(
            state.tables.length === 2 && state.tables[1] === undefined,
            "getQueryCatalogState must be positional with undefined for missing tables",
          );
          checkEqual(
            state.tables[0]?.id,
            (await store.getTableByName("t"))?.id,
            "getQueryCatalogState must return the same records the individual reads do",
          );
        }
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
        const fresh = (id: string): Omit<TransactionRecord, "snapshotVersion"> => {
          const { snapshotVersion: _pinned, ...record } = transaction(id, null);
          void _pinned;
          return record;
        };
        // A fresh record: begun, staged, and committed in the one step.
        const first = await write({
          transaction: { record: fresh("txn-w1") },
          blocks: [{ id: "block-w1", bytes: Uint8Array.of(1) }],
          segments: [segment("segment-w1", "table-t", "txn-w1", "block-w1")],
          expectedManifestVersion: null,
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
          removedBlockIds: ["block-w1"],
          changedTableIds: ["table-t"],
          committedAt: LATER,
        });
        checkEqual(second.version, 1, "the continued write must publish the next version");
        checkEqual(second.changedTableIds, ["table-t"], "the summary must carry the change set");
        const manifest = await store.getManifest(1);
        checkEqual(
          manifest?.blockIds,
          ["block-w2"],
          "the continued write must add its blocks and apply supersessions",
        );
        check(
          (await store.getTransaction("txn-w2"))?.status === "committed",
          "the begun record must end committed",
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
          expiresAt: LATER,
          revision: 0,
        });
        await checkThrows(
          () => move("lease-m", 9, second.version, FAR_FUTURE),
          LeaseConflictError,
          "moving with a stale revision",
        );
        await checkThrows(
          () => move("lease-m", 0, 999, FAR_FUTURE),
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
            expiresAt: LATER,
            revision: 0,
          },
          "a refused move must leave the lease exactly as it was",
        );
        const moved = await move("lease-m", 0, second.version, FAR_FUTURE);
        checkEqual(
          moved,
          {
            id: "lease-m",
            kind: "reader",
            manifestVersion: second.version,
            ownerId: "owner-m",
            expiresAt: FAR_FUTURE,
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
            transaction: { record },
            blocks: [{ id: "block-single", bytes: Uint8Array.of(1, 2, 3, 4) }],
            segments: [segment("segment-single", "table-t", "txn-single", "block-single")],
            expectedManifestVersion: version,
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
            expiresAt: FAR_FUTURE,
            revision: 0,
          });
          await move("lease-durable", 0, version, FAR_FUTURE);
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
