/** Real native composition for local and worker owners. No simulator/test executor or 0.x
 * engine fallback. Backend selection/namespace preflight belongs to the storage adapter. */
import {
  OwnershipLostError,
  type StorageBackend,
  type OwnerLease,
  type StoreSession,
} from "../storage/contract.js";
import type { Grant } from "../memory/governor.js";
import { Governor } from "../memory/governor.js";
import type { CacheMemory, ExtentSpace } from "../storage/extents.js";
import { openExtentSpace } from "../storage/space.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import { openWritePath } from "../write/engine.js";
import type { WritePath } from "../access/write-path.js";
import { createLowerer } from "../lower/create.js";
import { ChunkReader } from "../page/reader.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import type { SpillService } from "../spill/contract.js";
import { composeEngine } from "./compose.js";
import type { OwnerEngine } from "./engine.js";
import { OwnerServer, type ServerPort } from "./server.js";
import { PROTOCOL_VERSION } from "./protocol.js";
import {
  startOwnerHost,
  type OwnerEngineFactory,
  type OwnerHostInit,
  type OwnerHostDeps,
} from "./host.js";
export interface NativeBootOptions {
  readonly backend: StorageBackend;
  /** Explicit until B4b supplies the reviewed public default. Never an unbounded/null fallback. */
  readonly memoryLimitBytes: number;
  readonly durability?: "strict" | "relaxed";
  readonly transactionIdleTimeoutMs?: number;
  readonly signal?: AbortSignal;
}
function checkLease(lease: OwnerLease): void {
  if (lease.signal.aborted) throw new OwnershipLostError(lease.epoch);
}
function validateDurability(value: unknown): void {
  if (value !== undefined && value !== "strict" && value !== "relaxed")
    throw new TypeError("durability must be strict or relaxed");
}
function validate(options: Omit<NativeBootOptions, "backend">): void {
  if (!Number.isSafeInteger(options.memoryLimitBytes) || options.memoryLimitBytes <= 0)
    throw new RangeError("memoryLimitBytes must be a positive finite safe integer");
  validateDurability(options.durability);
  if (
    options.transactionIdleTimeoutMs !== undefined &&
    (!Number.isSafeInteger(options.transactionIdleTimeoutMs) ||
      options.transactionIdleTimeoutMs <= 0)
  )
    throw new RangeError("transactionIdleTimeoutMs must be a positive finite safe integer");
  options.signal?.throwIfAborted();
}
interface Resources {
  readonly engine: OwnerEngine;
  readonly governor: Governor;
  readonly spill: SpillService;
  closeStorage(): Promise<void>;
  close(): Promise<void>;
}
async function cleanup(steps: ReadonlyArray<() => unknown>): Promise<void> {
  const errors: unknown[] = [];
  for (const step of steps)
    try {
      await step();
    } catch (error) {
      errors.push(error);
    }
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) throw new AggregateError(errors, "Native owner cleanup failed");
}
async function openResources(
  backend: StorageBackend,
  lease: OwnerLease,
  options: Omit<NativeBootOptions, "backend">,
): Promise<Resources> {
  const governor = new Governor(options.memoryLimitBytes);
  let session: StoreSession | null = null,
    startup: Grant | null = null,
    scratchControl: Grant | null = null,
    scratchMemory: CacheMemory | null = null,
    space: ExtentSpace | null = null,
    path: WritePath | null = null,
    spill: SpillService | null = null,
    engine: OwnerEngine | null = null,
    storageClosing: Promise<void> | null = null;
  const closeStorage = () =>
    (storageClosing ??= cleanup([
      () => (spill instanceof ScratchSpillService ? spill.close() : spill?.sweep()),
      () => {
        scratchControl?.close();
        scratchMemory?.unregister?.();
      },
      () => space?.close(),
      () => {
        startup?.close();
      },
      () => session?.close(),
    ]));
  try {
    options.signal?.throwIfAborted();
    checkLease(lease);
    session = await backend.open(lease);
    startup = await governor.admit({
      label: "owner startup",
      minimumBytes: 0,
      spillable: true,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
    // Exact startup allocations grow this grant synchronously; lifetime roots/cache transfer
    // their charges to native handles before ready. There is no hardcoded 8 MiB test reserve.
    space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      budget: startup,
      registerCache: (cache) => governor.registerCache(cache),
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });
    options.signal?.throwIfAborted();
    checkLease(lease);
    const opening = openWritePath({
      session,
      space,
      governor,
      options: { durability: options.durability ?? "strict" },
    });
    startup.release(startup.held - startup.allocated - startup.loaned);
    const openedPath = await opening;
    path = openedPath;
    options.signal?.throwIfAborted();
    checkLease(lease);
    const scratch = backend.scratch(lease);
    if (scratch === null) {
      // The native spill contract explicitly permits this for an in-memory backend only.
      spill = new MemorySpillService();
    } else {
      scratchControl = await governor.admit({
        label: "shared scratch control",
        // The write path is open: its owner minimum stays held through this wait.
        holdsCredit: true,
        minimumBytes: SCRATCH_CONTROL_BYTES,
        spillable: false,
        ...(options.signal === undefined ? {} : { signal: options.signal }),
      });
      scratchMemory = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
      spill = new ScratchSpillService(scratch, { budget: scratchControl, memory: scratchMemory });
      // Permanent beside the write engine: part of every write growth step's complete minimum.
      const control = scratchControl;
      openedPath.retainOwnerHoldings(() => control.held);
    }
    // Native recovery must finish before old temporary ownership is swept.
    await spill.sweep();
    options.signal?.throwIfAborted();
    checkLease(lease);
    engine = composeEngine({
      planner: createLowerer(),
      writePath: path,
      governor,
      spill,
      reads: { open: (pages, chunk, read) => ChunkReader.open(pages, chunk, read) },
      ...(options.transactionIdleTimeoutMs === undefined
        ? {}
        : { transactionIdleTimeoutMs: options.transactionIdleTimeoutMs }),
    });
    const opened = engine,
      originalClose = opened.close.bind(opened);
    let engineClosing: Promise<void> | null = null,
      closing: Promise<void> | null = null;
    opened.close = () => (engineClosing ??= originalClose());
    return {
      engine: opened,
      governor,
      spill,
      closeStorage,
      close: () => (closing ??= cleanup([() => opened.close(), closeStorage])),
    };
  } catch (error) {
    try {
      await cleanup([() => engine?.close() ?? path?.close(), closeStorage]);
    } catch (closing) {
      throw new AggregateError([error, closing], "Native open and cleanup failed", {
        cause: closing,
      });
    }
    throw error;
  }
}
export interface NativeOwner {
  readonly server: OwnerServer;
  readonly engine: OwnerEngine;
  readonly governor: Governor;
  readonly spill: SpillService;
  accept(port: ServerPort): void;
  close(): Promise<void>;
}
export async function openNativeOwner(input: NativeBootOptions): Promise<NativeOwner> {
  const options = { ...input };
  validate(options);
  const lease = await options.backend.acquire();
  let resources: Resources;
  try {
    resources = await openResources(options.backend, lease, options);
  } catch (error) {
    await lease.release();
    throw error;
  }
  const server = new OwnerServer(resources.engine, {
    ownerId: crypto.randomUUID(),
    epoch: lease.epoch,
    build: "native-internal",
    ...(options.transactionIdleTimeoutMs === undefined
      ? {}
      : { transactionIdleTimeoutMs: options.transactionIdleTimeoutMs }),
  });
  let closing: Promise<void> | null = null;
  const close = () =>
    (closing ??= (async () => {
      lease.signal.removeEventListener("abort", lost);
      const drain = server.quiesce("closing");
      try {
        await cleanup([() => drain.drained, () => resources.close(), () => lease.release()]);
      } finally {
        drain.finish();
      }
    })());
  const lost = () => {
    server.stepDown("lockLost");
    void close().catch((error: unknown) => {
      console.error("Native owner close after ownership loss failed", error);
    });
  };
  lease.signal.addEventListener("abort", lost, { once: true });
  if (lease.signal.aborted) {
    await close();
    checkLease(lease);
  }
  return {
    server,
    engine: resources.engine,
    governor: resources.governor,
    spill: resources.spill,
    accept: (port) => {
      if (closing !== null) throw new TypeError("Native owner is closed");
      checkLease(lease);
      server.accept(port);
    },
    close,
  };
}
/** The real worker host uses the same boot resources, plus the existing election/lifecycle
 * machinery. Entry-point side effects and exported worker barrels remain a cutover step. */
export async function startNativeOwnerHost(
  init: OwnerHostInit,
  backendFor: (database: string) => StorageBackend | Promise<StorageBackend>,
  options: Omit<NativeBootOptions, "backend" | "signal">,
  deps: OwnerHostDeps,
) {
  const supplied = { ...init.settings },
    defaults = { ...options };
  const memoryLimitBytes = Object.hasOwn(supplied, "memoryLimitBytes")
    ? supplied.memoryLimitBytes
    : defaults.memoryLimitBytes;
  const durability = Object.hasOwn(supplied, "durability")
    ? supplied.durability
    : defaults.durability;
  const idle = Object.hasOwn(supplied, "transactionIdleTimeoutMs")
    ? supplied.transactionIdleTimeoutMs
    : defaults.transactionIdleTimeoutMs;
  if (typeof memoryLimitBytes !== "number")
    throw new TypeError("memoryLimitBytes must be a number");
  if (durability !== undefined && durability !== "strict" && durability !== "relaxed")
    throw new TypeError("durability must be strict or relaxed");
  if (idle !== undefined && typeof idle !== "number")
    throw new TypeError("transactionIdleTimeoutMs must be a number");
  const settings: Omit<NativeBootOptions, "backend" | "signal"> = {
    memoryLimitBytes,
    ...(durability === undefined ? {} : { durability }),
    ...(idle === undefined ? {} : { transactionIdleTimeoutMs: idle }),
  };
  validate(settings); // Refuse bad settings before the first mutation-authority acquisition.
  interface Fence {
    backend: StorageBackend;
    lease: OwnerLease;
    resources: Resources | null;
  }
  const factory: OwnerEngineFactory<Fence> = {
    async openFence(database) {
      const backend = await backendFor(database);
      return { backend, lease: await backend.acquire(), resources: null };
    },
    async open(fence) {
      const resources = await openResources(fence.backend, fence.lease, settings);
      fence.resources = resources;
      return resources.engine;
    },
    closeSession: (fence) => fence.resources?.closeStorage() ?? Promise.resolve(),
    async closeFence(fence) {
      let failure: { error: unknown } | null = null;
      try {
        await fence.resources?.close();
      } catch (error) {
        failure = { error };
      }
      const report = await fence.lease.release();
      if (failure !== null) throw failure.error;
      return report;
    },
    epochOf: (fence) => fence.lease.epoch,
  };
  return startOwnerHost(
    { ...init, settings: { ...supplied, ...settings, protocol: PROTOCOL_VERSION } },
    factory,
    deps,
  );
}
