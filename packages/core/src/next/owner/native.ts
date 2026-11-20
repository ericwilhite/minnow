/** Native owner composition. The host owns the lease; this engine owns sessions/spaces.
 * Replacement runs only under OwnerServer's exclusive snapshot barrier. */
import type { StorageBackend, OwnerLease, StoreSession } from "../storage/contract.js";
import { DATA_ALIGN } from "../storage/contract.js";
import type { MemoryGovernor, Grant } from "../memory/governor.js";
import type { ExtentSpace } from "../storage/extents.js";
import { openExtentSpace } from "../storage/space.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import { WritePathImpl, type EngineInput } from "../write/engine.js";
import { exportSnapshot } from "../write/snapshot-capture.js";
import { loadSnapshot, validateSnapshotScope } from "../write/snapshot-load.js";
import { snapshotSpoolGrants, spoolSnapshotInput } from "../write/snapshot-source.js";
import { SnapshotScopeError } from "../snapshot-errors.js";
import { FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import { ChunkReader } from "../page/reader.js";
import { composeEngine, type CatalogOwnerEngine } from "./compose.js";
import type { StatementPlanner } from "./planner.js";
import { SNAPSHOT_TRANSFER_BYTES } from "./snapshot-api.js";
import type { OwnerSnapshotOperations } from "./snapshot-connection.js";

export interface NativeOwnerOptions {
  readonly backend: StorageBackend;
  readonly lease: OwnerLease;
  readonly governor: MemoryGovernor;
  readonly planner: StatementPlanner;
  readonly options?: EngineInput["options"];
}

export async function openNativeOwnerEngine(
  input: NativeOwnerOptions,
): Promise<CatalogOwnerEngine> {
  const { backend, lease, governor } = input;
  const scratch = backend.scratch(lease);
  if (scratch === null) throw new SnapshotScopeError("Native owner needs persistent scratch");
  let session: StoreSession | undefined, space: ExtentSpace | undefined, startup: Grant | undefined;
  let path: WritePathImpl | undefined, engine: CatalogOwnerEngine | undefined;
  let closed = false;
  const spillCredit = await governor.admit({
    label: "owner scratch control",
    minimumBytes: SCRATCH_CONTROL_BYTES,
    spillable: false,
  });
  const scratchMemory = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
  let spill: ScratchSpillService | undefined;
  const closeStorage = async () => {
    const s = space,
      g = startup,
      io = session;
    space = undefined;
    startup = undefined;
    session = undefined;
    try {
      await s?.close();
    } finally {
      g?.close();
      await io?.close();
    }
  };
  const reopen = async () => {
    await closeStorage();
    session = await backend.open(lease);
    startup = await governor.admit({
      label: "native owner startup",
      minimumBytes: 8 << 20,
      spillable: true,
      // The owner's scratch control grant stays held through this wait.
      holdsCredit: true,
    });
    space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      budget: startup,
      registerCache: (cache) => governor.registerCache(cache),
    });
    const opening = WritePathImpl.open({
      session,
      space,
      governor,
      options: input.options ?? { durability: "strict" },
      // The owner's scratch control is held before and during open and recovery.
      ownerHoldings: () => spillCredit.held,
    });
    startup.release(startup.held - startup.allocated - startup.loaned);
    path = await opening;
    engine?.installWritePath(path);
    return path;
  };
  const operations: OwnerSnapshotOperations = {
    governor,
    fatal: false,
    exportMinimumBytes: 8 << 20,
    importGrants: snapshotSpoolGrants(SNAPSHOT_TRANSFER_BYTES),
    async export(sink, signal, grant) {
      if (path === undefined) throw new SnapshotScopeError("Native owner is not open");
      validateSnapshotScope(path.catalog);
      // Identity bytes belong to the existing complete export grant.
      if (!grant.allocate(80)) throw new SnapshotScopeError("Lost export identity admission");
      try {
        const id = crypto.getRandomValues(new Uint8Array(16));
        await exportSnapshot(path, id, sink, grant, { signal });
      } finally {
        grant.free(80);
      }
    },
    async import(source, signal, funding) {
      if (path === undefined || space === undefined || spill === undefined)
        throw new SnapshotScopeError("Native owner is not open");
      // Server has drained all sessions, pins and output pools. Close maintenance too.
      const previousId = path.databaseId;
      let archive: Awaited<ReturnType<typeof spoolSnapshotInput>> | undefined;
      let failure: { error: unknown } | undefined;
      let recoveryFailure: { error: unknown } | undefined;
      let cleanupFailure: { error: unknown } | undefined;
      try {
        await path.close();
        const currentLsn = path.appliedLsn;
        await spill.sweep();
        archive = await spoolSnapshotInput(
          source,
          {
            scratch,
            offset: 0,
            capacity: Math.floor(Number.MAX_SAFE_INTEGER / DATA_ALIGN) * DATA_ALIGN,
          },
          governor,
          { signal, maxChunkBytes: SNAPSHOT_TRANSFER_BYTES, funding: funding.work },
        );
        await loadSnapshot(
          archive,
          async () => {
            if (space === undefined || session === undefined)
              throw new SnapshotScopeError("Replacement target closed");
            return {
              space,
              epoch: session.epoch,
              currentLsn,
              empty: false,
              close: () => Promise.resolve(),
            };
          },
          governor,
          {
            signal,
            replace: true,
            retainedBytes: () =>
              funding.transport.held +
              spillCredit.held +
              (startup?.held ?? 0) +
              (space?.stats.retainedBytes ?? 0) +
              FIXED_CACHE_BYTES,
          },
        );
      } catch (error) {
        failure = { error };
      } finally {
        try {
          try {
            await archive?.close();
          } finally {
            await spill.sweep();
          }
        } catch (error) {
          cleanupFailure = { error };
          operations.fatal = true;
          operations.fatalError = error;
        } finally {
          // Publication failure is indeterminate. Read physical slots; never keep the old path.
          try {
            await reopen();
          } catch (error) {
            operations.fatal = true;
            operations.fatalError = error;
            recoveryFailure = { error };
          }
        }
      }
      // Native publication is atomic. A recovered new identity proves that this
      // import committed, even if a post-publication I/O or cancellation rejected.
      if (recoveryFailure !== undefined)
        throw recoveryFailure.error instanceof Error
          ? recoveryFailure.error
          : new Error("Native snapshot recovery failed", { cause: recoveryFailure.error });
      if (cleanupFailure !== undefined)
        throw cleanupFailure.error instanceof Error
          ? cleanupFailure.error
          : new Error("Snapshot scratch cleanup failed", { cause: cleanupFailure.error });
      if (failure !== undefined && path.databaseId === previousId)
        throw failure.error instanceof Error
          ? failure.error
          : new Error("Native snapshot load failed", { cause: failure.error });
    },
  };
  try {
    spill = new ScratchSpillService(scratch, { budget: spillCredit, memory: scratchMemory });
    await spill.sweep();
    const opened = await reopen();
    engine = composeEngine({
      writePath: opened,
      planner: input.planner,
      governor,
      spill,
      snapshots: operations,
      reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
      async closeStorage() {
        if (closed) return;
        closed = true;
        try {
          await closeStorage();
        } finally {
          try {
            await spill?.close();
          } finally {
            scratchMemory.unregister?.();
            spillCredit.close();
          }
        }
      },
    });
    return engine;
  } catch (error) {
    try {
      await path?.close();
    } finally {
      try {
        await closeStorage();
      } finally {
        try {
          await spill?.close();
        } finally {
          scratchMemory.unregister?.();
          spillCredit.close();
        }
      }
    }
    throw error;
  }
}
