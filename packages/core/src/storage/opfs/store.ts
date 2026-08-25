import {
  assertStorageBulkReadItems,
  assertTempRunPageBatchLimits,
  OpfsUncertainOutcomeError,
  StorageCorruptionError,
  StorageFormatVersionError,
  validateStorageDatabaseName,
  type BlockStore,
  type TempRunPage,
} from "../types.js";
import { validateTempRunPage, validateTempRunPageIdentity } from "../toolkit/record-core.js";
import { OpfsTree, encodeSegment, isDomError } from "./files.js";
import { LOG_FORMAT_VERSION } from "../toolkit/wire.js";
import { OpfsLeader } from "./leader.js";
import {
  rehydrateStoreError,
  estimateRpcValueBytes,
  fingerprintStoreRequest,
  MAX_OPFS_RPC_MESSAGE_BYTES,
  parseStoreRpcMessage,
  serializeStoreError,
  type SerializedStoreError,
  type StoreRpcMessage,
} from "./rpc.js";

/** How long a follower waits for a leader's answer before assuming it is gone. */
const RPC_TIMEOUT_MS = 1_000;
/** How long a ping waits for a leadership announcement. */
const DISCOVERY_WAIT_MS = 150;
/** Attempts across the discover → call → elect loop before an operation gives up. */
const DISPATCH_ATTEMPTS = 10;
/** A leader yields to a foreground bidder at most this often. */
const YIELD_COOLDOWN_MS = 3_000;
/** Results remembered for retried requests whose acknowledgement was lost. */
const DEDUPE_CACHE_SIZE = 512;
/** Requests admitted concurrently by either side of the follower protocol. */
const RPC_IN_FLIGHT_LIMIT = 512;
const RPC_IN_FLIGHT_MUTATION_BYTES = 128 * 1024 * 1024;
const RPC_IN_FLIGHT_READ_BYTES = 256 * 1024 * 1024;
const RPC_SETTLED_OUTCOME_BYTES = 128 * 1024 * 1024;
const RPC_SERVER_ADMISSION_LIMIT = 1024;
const RPC_SERVER_ADMISSION_BYTES = 256 * 1024 * 1024;
/** Channels into requesters' inboxes a leader keeps open between answers. */
const ANSWER_CHANNEL_CACHE_SIZE = 64;
const BULK_READ_ARGUMENT_INDEX = {
  getBlocks: 0,
  getTransactions: 0,
  getExistingUniqueKeys: 1,
  hasManifestBlocks: 1,
} as const;

export interface OpfsBlockStoreOptions {
  /** Databases live under `minnowdb/<name>` in the origin's private file system. */
  name: string;
  /**
   * `"strict"` (the default) flushes the payload before flushing its publishing WAL frame,
   * both before the operation resolves. `"relaxed"` writes the complete payload and
   * WAL frame before resolving but lets the operating system schedule their final flush; a
   * power loss may roll back a fully consistent suffix.
   */
  durability?: "relaxed" | "strict";
  /** The storage root; defaults to `navigator.storage.getDirectory()`. Tests inject a shim. */
  root?: FileSystemDirectoryHandle;
  /** @internal Test seam: checkpoint after this many WAL entries (default 1024). */
  checkpointEntries?: number;
  /** @internal Test seam: cleanup-debt backpressure limit (default 64 MiB). */
  cleanupLimitBytes?: number;
  /** @internal Test seam: how long a follower waits for the leader (default 1000ms). */
  rpcTimeoutMs?: number;
}

/** Methods a follower may invoke on the leader. Temp-page IO is instance-local by design. */
const RPC_METHODS = new Set([
  "getBlock",
  "getBlocks",
  "readManifestBlock",
  "hasManifestBlocks",
  "listManifestBlockPage",
  "listRetiredManifestBlockPage",
  "addTable",
  "getTable",
  "getTableByName",
  "listTables",
  "updateTable",
  "removeTable",
  "dropTable",
  "dropTableColumn",
  "removeFtsColumn",
  "writeFtsBase",
  "beginFtsBaseBuild",
  "renewFtsBaseBuild",
  "writeFtsBaseBuildChunk",
  "finishFtsBaseBuild",
  "abortFtsBaseBuild",
  "readFtsCandidates",
  "readFtsPostings",
  "getSegment",
  "listSegmentPage",
  "listTableSegmentPage",
  "removeAbortedSegment",
  "adoptAbortedSegment",
  "reserveRowIds",
  "reserveAutoIncrement",
  "getExistingUniqueKeys",
  "beginUniqueKeyBuild",
  "getUniqueKeyBuild",
  "renewUniqueKeyBuild",
  "appendUniqueKeyBuildChunk",
  "finishUniqueKeyBuild",
  "abortUniqueKeyBuild",
  "getCurrentManifest",
  "getCurrentManifestVersion",
  "getCatalogProbe",
  "getManifest",
  "listManifestPage",
  "createTransaction",
  "renewTransaction",
  "abortTransactionIfExpired",
  "beginTransaction",
  "getTransaction",
  "getTransactions",
  "listTransactionPage",
  "updateTransaction",
  "stageTransactionArtifacts",
  "rollbackTransactionArtifacts",
  "commitTransaction",
  "writeTransaction",
  "createLease",
  "getLease",
  "listLeases",
  "listExpiredLeasePage",
  "renewLease",
  "moveLease",
  "removeLeaseIfExpired",
  "removeLease",
  "createCompactionJob",
  "getCompactionJob",
  "listCompactionJobs",
  "listCompactionJobPage",
  "updateCompactionJob",
  "cancelCompactionJob",
  "removeCompactionJob",
  "createGarbageCollectionJob",
  "updateGarbageCollectionPlanning",
  "getGarbageCollectionJob",
  "listGarbageCollectionJobs",
  "listGarbageCollectionJobPage",
  "runGarbageCollectionStep",
  "removePrunedManifestRecords",
  "removeGarbageCollectionJob",
  "createTempOwner",
  "getTempOwner",
  "putTempRunPage",
  "putTempRunPages",
  "getTempRunPage",
  "removeTempRun",
  "renewTempOwner",
  "removeTempOwnerIfExpired",
  "removeTempOwner",
  "listTempOwnerIdsPage",
  "listExpiredTempOwnerPage",
  "getLogicalStorageBytes",
  "getStorageStats",
  "checkIntegrity",
  "beginSnapshotFrameExport",
  "readSnapshotExportFrame",
  "closeSnapshotFrameExport",
  "beginSnapshotFrameImport",
  "renewSnapshotFrameImport",
  "appendSnapshotImportFrames",
  "finishSnapshotFrameImport",
  "cancelSnapshotFrameImport",
]);

/**
 * Reads are idempotent — a lost-acknowledgement retry can simply run again — so only
 * mutations enter the dedupe cache, and read results (block bytes included) are never
 * retained on the leader.
 */
const READ_METHODS = new Set([
  "getBlock",
  "getBlocks",
  "readManifestBlock",
  "hasManifestBlocks",
  "listManifestBlockPage",
  "listRetiredManifestBlockPage",
  "getTable",
  "getTableByName",
  "listTables",
  "readFtsCandidates",
  "readFtsPostings",
  "getSegment",
  "listSegmentPage",
  "listTableSegmentPage",
  "getExistingUniqueKeys",
  "getUniqueKeyBuild",
  "getCurrentManifest",
  "getCurrentManifestVersion",
  "getCatalogProbe",
  "getManifest",
  "listManifestPage",
  "getTransaction",
  "getTransactions",
  "listTransactionPage",
  "getLease",
  "listLeases",
  "listExpiredLeasePage",
  "getCompactionJob",
  "listCompactionJobs",
  "listCompactionJobPage",
  "getGarbageCollectionJob",
  "listGarbageCollectionJobs",
  "listGarbageCollectionJobPage",
  "getTempOwner",
  "getTempRunPage",
  "listTempOwnerIdsPage",
  "listExpiredTempOwnerPage",
  "getLogicalStorageBytes",
  "getStorageStats",
  "checkIntegrity",
  "readSnapshotExportFrame",
]);

interface ServedOutcome {
  ok: boolean;
  value?: unknown;
  error?: SerializedStoreError;
}

interface ServedMutation<Outcome> {
  method: string;
  signature: string;
  requestBytes: number;
  outcome: Outcome;
  outcomeBytes?: number;
}

type OpMessage = Extract<StoreRpcMessage, { kind: "op" }>;

function sameServedRequest(
  remembered: Pick<ServedMutation<unknown>, "method" | "signature">,
  message: OpMessage,
  signature: string,
): boolean {
  return remembered.method === message.method && remembered.signature === signature;
}

interface PendingRpc {
  message: OpMessage;
  retainedBytes: number;
  /** Which leader this request was last posted to, so announces only trigger real re-sends. */
  sentTo: string | undefined;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * The OPFS block store: one leader per database holds every file handle and does all storage
 * work at held-handle speed; other connections are thin followers whose operations travel a
 * `BroadcastChannel` to it.
 *
 * Two kinds of channel carry the protocol. One shared channel per database carries what every
 * connection must hear: leader announcements, pings, bids, yields, releases. Every connection
 * also owns an inbox — a channel named for its instance id — where the messages meant for it
 * alone arrive: operations at the leader, results and busy notices at the requester. A block
 * read's bytes are therefore structured-cloned once, into the tab that asked, rather than into
 * every tab of the origin.
 *
 * Leadership is the write-ahead log's own exclusive sync-access handle — enforced by the
 * browser against the actual resource, released the instant its holder dies. Elections are
 * simply attempts to open it. Correctness never rides on a message: an operation is
 * acknowledged only after the leader's WAL holds it. Request ids deduplicate delivery retries
 * while the same leader serves. A dead leader costs a failover in which the next acquirer
 * replays checkpoint-plus-WAL; an in-flight read moves to it, while an in-flight mutation fails
 * with `OpfsUncertainOutcomeError` instead of risking a second execution after a lost reply.
 * The channel affects how fast multi-tab work moves, never whether it is right.
 *
 * `setForeground(true)` marks this connection as the one the user is looking at; a background
 * leader yields to a foreground bidder, so the tab doing the work is normally the tab holding
 * the microsecond-fast path.
 */
// The runtime methods in `RPC_METHODS` share one generated dispatch body below. This interface
// supplies their exact public types without emitting one repetitive wrapper per operation.
// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
export interface OpfsBlockStore extends Required<BlockStore> {}
// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
export class OpfsBlockStore {
  readonly liveQueryChannelName: string;
  readonly #tree: OpfsTree;
  readonly #durability: "relaxed" | "strict";
  readonly #checkpointEntries: number | undefined;
  readonly #cleanupLimitBytes: number | undefined;
  readonly #rpcTimeoutMs: number;
  readonly #instanceId = crypto.randomUUID();
  readonly #channelName: string;
  /** The database-wide channel: leadership traffic every connection listens to. */
  #channel: BroadcastChannel | undefined;
  /** This connection's own channel: operations when leading, results when following. */
  #inbox: BroadcastChannel | undefined;
  /** While leading: open channels into requesters' inboxes, by instance id. */
  readonly #answerChannels = new Map<string, BroadcastChannel>();
  #leader: OpfsLeader | undefined;
  #knownLeader: string | undefined;
  #foreground = false;
  #closed = false;
  #lastYieldAt = 0;
  #electing: Promise<boolean> | undefined;
  readonly #pending = new Map<string, PendingRpc>();
  readonly #inFlightMutations = new Map<string, ServedMutation<Promise<ServedOutcome>>>();
  readonly #settledMutations = new Map<string, ServedMutation<ServedOutcome>>();
  readonly #servedRequestLocks = new Map<string, Promise<void>>();
  #inFlightMutationBytes = 0;
  #inFlightReadBytes = 0;
  #settledMutationBytes = 0;
  #pendingRpcBytes = 0;
  #servedRequestCount = 0;
  #servedRequestBytes = 0;
  #servedMutationGateForTests: Promise<void> | undefined;
  #dropNextRpcResultForTests = false;
  #reacquireTimer: ReturnType<typeof setTimeout> | undefined;

  private constructor(tree: OpfsTree, options: OpfsBlockStoreOptions) {
    this.#tree = tree;
    this.#durability = options.durability ?? "strict";
    this.#checkpointEntries = options.checkpointEntries;
    this.#cleanupLimitBytes = options.cleanupLimitBytes;
    this.#rpcTimeoutMs = options.rpcTimeoutMs ?? RPC_TIMEOUT_MS;
    this.#channelName = `minnowdb-store:${options.name}`;
    this.liveQueryChannelName = `minnowdb-live:opfs:${options.name}`;
  }

  static async open(options: OpfsBlockStoreOptions): Promise<OpfsBlockStore> {
    const tree = new OpfsTree(await resolveDatabaseRoot(options));
    const store = new OpfsBlockStore(tree, options);
    await store.#ensureFormatMarker();
    if (typeof BroadcastChannel === "function") {
      const onMessage = (event: MessageEvent<unknown>) => {
        const message = parseStoreRpcMessage(event.data, RPC_METHODS);
        if (message !== undefined) store.#onMessage(message);
      };
      const channel = new BroadcastChannel(store.#channelName);
      channel.onmessage = onMessage;
      const inbox = new BroadcastChannel(store.#inboxName(store.#instanceId));
      inbox.onmessage = onMessage;
      store.#channel = channel;
      store.#inbox = inbox;
    }
    await store.#tryBecomeLeader();
    return store;
  }

  // ---------------------------------------------------------------------------------------
  // Leadership.
  // ---------------------------------------------------------------------------------------

  async #tryBecomeLeader(): Promise<boolean> {
    if (this.#closed) return false;
    if (this.#leader !== undefined) return true;
    if (this.#electing !== undefined) return this.#electing;
    const election = this.#elect();
    this.#electing = election;
    try {
      return await election;
    } finally {
      this.#electing = undefined;
    }
  }

  async #elect(): Promise<boolean> {
    let wal: FileSystemSyncAccessHandle;
    try {
      wal = await this.#tree.openHandle(["wal"], { create: true });
    } catch (error) {
      if (isLockContention(error)) return false;
      throw error;
    }
    let slotA: FileSystemSyncAccessHandle | undefined;
    let slotB: FileSystemSyncAccessHandle | undefined;
    try {
      // A dying ex-leader releases all its handles at once, but a graceful demotion closes
      // them in sequence; the brief retry covers the gap.
      slotA = await this.#openWithRetry(["checkpoint-a"]);
      slotB = await this.#openWithRetry(["checkpoint-b"]);
      this.#leader = await OpfsLeader.recover(
        this.#tree,
        this.#durability === "strict",
        { wal, slotA, slotB },
        this.#checkpointEntries,
        this.#cleanupLimitBytes,
      );
    } catch (error) {
      wal.close();
      slotA?.close();
      slotB?.close();
      if (isLockContention(error)) return false;
      throw error;
    }
    if (this.#closed) {
      // close() ran while this election was in flight; a leader installed now would hold the
      // browser's file lock with no owner to ever release it.
      const leader = this.#leader;
      this.#leader = undefined;
      await leader.shutdown().catch(() => {
        leader.crash();
      });
      return false;
    }
    this.#knownLeader = this.#instanceId;
    this.#post({ kind: "leader", leaderId: this.#instanceId });
    return true;
  }

  async #openWithRetry(path: string[]): Promise<FileSystemSyncAccessHandle> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.#tree.openHandle(path, { create: true });
      } catch (error) {
        if (isLockContention(error) && attempt < 40) {
          await sleep(5 + Math.random() * 10);
          continue;
        }
        throw error;
      }
    }
  }

  /** Marks this connection as the one the user is looking at — a leadership preference. */
  setForeground(foreground: boolean): void {
    this.#foreground = foreground;
    if (foreground && this.#leader === undefined && !this.#closed) {
      this.#post({ kind: "bid", bidderId: this.#instanceId, foreground: true });
    }
  }

  async #yieldLeadership(to: string): Promise<void> {
    const leader = this.#leader;
    if (leader === undefined) return;
    this.#leader = undefined;
    this.#knownLeader = undefined;
    this.#lastYieldAt = Date.now();
    try {
      await leader.shutdown();
    } catch {
      // Whatever failed, the handles must not outlive the leadership; crash-close is
      // idempotent and releases them.
      leader.crash();
    }
    // An open channel into a follower's inbox would hear the next leader's answers to it.
    this.#closeAnswerChannels();
    this.#post({ kind: "yield", to });
    // If the bidder loses the race or vanishes, someone must still hold the database.
    if (this.#reacquireTimer !== undefined) clearTimeout(this.#reacquireTimer);
    this.#reacquireTimer = setTimeout(() => {
      if (this.#knownLeader === undefined && !this.#closed) void this.#tryBecomeLeader();
    }, 1_500);
  }

  #onMessage(message: StoreRpcMessage): void {
    if (this.#closed) return;
    switch (message.kind) {
      case "op": {
        if (this.#leader !== undefined) {
          const requestBytes = estimateRpcValueBytes(message.args);
          if (
            this.#servedRequestCount >= RPC_SERVER_ADMISSION_LIMIT ||
            this.#servedRequestBytes + requestBytes > RPC_SERVER_ADMISSION_BYTES
          ) {
            this.#answer(message.from, {
              kind: "result",
              requestId: message.requestId,
              ok: false,
              error: { name: "Error", message: "The OPFS leader RPC queue is full" },
            });
            return;
          }
          this.#servedRequestCount += 1;
          this.#servedRequestBytes += requestBytes;
          void this.#serveOp(message).finally(() => {
            this.#servedRequestCount = Math.max(0, this.#servedRequestCount - 1);
            this.#servedRequestBytes = Math.max(0, this.#servedRequestBytes - requestBytes);
          });
        }
        return;
      }
      case "result": {
        const pending = this.#takePending(message.requestId);
        if (pending === undefined) return;
        clearTimeout(pending.timer);
        if (message.ok) pending.resolve(message.value);
        else pending.reject(rehydrateStoreError(message.error));
        return;
      }
      case "busy": {
        const pending = this.#pending.get(message.requestId);
        if (pending === undefined) return;
        clearTimeout(pending.timer);
        pending.timer = setTimeout(() => {
          const expired = this.#takePending(message.requestId);
          expired?.reject(RPC_TIMED_OUT);
        }, this.#rpcTimeoutMs);
        return;
      }
      case "leader": {
        this.#knownLeader = message.leaderId;
        if (this.#reacquireTimer !== undefined) {
          clearTimeout(this.#reacquireTimer);
          this.#reacquireTimer = undefined;
        }
        // Reads can move to the new leader. A mutation may already be durable on the old one;
        // never execute it twice merely because its acknowledgement was lost.
        for (const [requestId, pending] of this.#pending) {
          if (pending.sentTo === message.leaderId) continue;
          if (!READ_METHODS.has(pending.message.method)) {
            this.#takePending(requestId);
            clearTimeout(pending.timer);
            pending.reject(new OpfsUncertainOutcomeError(pending.message.method));
            continue;
          }
          pending.sentTo = message.leaderId;
          this.#send(message.leaderId, pending.message);
        }
        // The freshly announced leader may be background while we are what the user sees.
        if (this.#foreground && this.#leader === undefined) {
          this.#post({ kind: "bid", bidderId: this.#instanceId, foreground: true });
        }
        return;
      }
      case "ping": {
        if (this.#leader !== undefined) {
          this.#post({ kind: "leader", leaderId: this.#instanceId });
        }
        return;
      }
      case "bid": {
        if (
          this.#leader !== undefined &&
          !this.#foreground &&
          message.foreground &&
          message.bidderId !== this.#instanceId &&
          Date.now() - this.#lastYieldAt > YIELD_COOLDOWN_MS
        ) {
          void this.#yieldLeadership(message.bidderId);
        }
        return;
      }
      case "yield": {
        if (message.to === this.#instanceId) void this.#tryBecomeLeader();
        return;
      }
      case "released": {
        if (this.#knownLeader === message.leaderId) this.#knownLeader = undefined;
        return;
      }
    }
  }

  #takePending(requestId: string): PendingRpc | undefined {
    const pending = this.#pending.get(requestId);
    if (pending === undefined) return undefined;
    this.#pending.delete(requestId);
    this.#pendingRpcBytes -= pending.retainedBytes;
    return pending;
  }

  async #serveOp(message: OpMessage): Promise<void> {
    const requestKey = `${String(message.from.length)}:${message.from}${message.requestId}`;
    // Once an identity is admitted, duplicates can compare their fingerprint immediately and
    // either attach to the exact execution or fail closed. The short lock below exists only for
    // the pre-admission fingerprint race between two first deliveries.
    if (this.#inFlightMutations.has(requestKey) || this.#settledMutations.has(requestKey)) {
      await this.#serveOpLocked(message, requestKey);
      return;
    }
    const previous = this.#servedRequestLocks.get(requestKey) ?? Promise.resolve();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tail = previous.then(() => hold);
    this.#servedRequestLocks.set(requestKey, tail);
    await previous;
    try {
      await this.#serveOpLocked(message, requestKey);
    } finally {
      release();
      if (this.#servedRequestLocks.get(requestKey) === tail) {
        this.#servedRequestLocks.delete(requestKey);
      }
    }
  }

  async #serveOpLocked(message: OpMessage, requestKey: string): Promise<void> {
    const leader = this.#leader;
    if (leader === undefined || !RPC_METHODS.has(message.method)) return;
    const isRead = READ_METHODS.has(message.method);
    let fingerprint: { signature: string; retainedBytes: number };
    try {
      fingerprint = await fingerprintStoreRequest(message.method, message.args);
    } catch {
      return;
    }
    const remembered = isRead ? undefined : this.#settledMutations.get(requestKey);
    let settled: ServedOutcome;
    if (remembered !== undefined) {
      if (!sameServedRequest(remembered, message, fingerprint.signature)) {
        this.#rejectReusedRequestIdentity(message);
        return;
      }
      // Refresh bounded settled outcomes as an LRU. This is the lost-ack retry path.
      this.#settledMutations.delete(requestKey);
      this.#settledMutations.set(requestKey, remembered);
      settled = remembered.outcome;
    } else if (!isRead && this.#inFlightMutations.has(requestKey)) {
      const inFlight = this.#inFlightMutations.get(requestKey);
      if (inFlight === undefined) throw new Error("In-flight RPC identity disappeared");
      if (!sameServedRequest(inFlight, message, fingerprint.signature)) {
        this.#rejectReusedRequestIdentity(message);
        return;
      }
      // Never evict an in-flight mutation. A duplicate attaches to the one execution and resets
      // the requester's patience while it remains queued or running.
      this.#answer(message.from, { kind: "busy", requestId: message.requestId });
      settled = await inFlight.outcome;
    } else {
      if (
        !isRead &&
        (this.#inFlightMutations.size >= RPC_IN_FLIGHT_LIMIT ||
          this.#inFlightMutationBytes + fingerprint.retainedBytes > RPC_IN_FLIGHT_MUTATION_BYTES)
      ) {
        this.#answer(message.from, {
          kind: "result",
          requestId: message.requestId,
          ok: false,
          error: { name: "Error", message: "The OPFS leader mutation queue is full" },
        });
        return;
      }
      if (isRead) {
        const reservation = Math.max(fingerprint.retainedBytes, MAX_OPFS_RPC_MESSAGE_BYTES);
        if (this.#inFlightReadBytes + reservation > RPC_IN_FLIGHT_READ_BYTES) {
          this.#answer(message.from, {
            kind: "result",
            requestId: message.requestId,
            ok: false,
            error: { name: "Error", message: "The OPFS leader read queue is full" },
          });
          return;
        }
        this.#inFlightReadBytes += reservation;
        try {
          settled = await this.#executeServedOpAfterGate(leader, message, true);
        } finally {
          this.#inFlightReadBytes = Math.max(0, this.#inFlightReadBytes - reservation);
        }
      } else {
        this.#inFlightMutationBytes += fingerprint.retainedBytes;
        const execution = this.#executeServedOpAfterGate(leader, message, false);
        const outcome = execution.then((result) => {
          this.#inFlightMutations.delete(requestKey);
          this.#inFlightMutationBytes = Math.max(
            0,
            this.#inFlightMutationBytes - fingerprint.retainedBytes,
          );
          if (!this.#closed) {
            this.#rememberSettledMutation(requestKey, message.method, fingerprint, result);
          }
          return result;
        });
        this.#inFlightMutations.set(requestKey, {
          method: message.method,
          signature: fingerprint.signature,
          requestBytes: fingerprint.retainedBytes,
          outcome,
        });
        settled = await outcome;
      }
    }
    if (this.#dropNextRpcResultForTests) {
      this.#dropNextRpcResultForTests = false;
      return;
    }
    this.#answer(
      message.from,
      settled.ok
        ? { kind: "result", requestId: message.requestId, ok: true, value: settled.value }
        : {
            kind: "result",
            requestId: message.requestId,
            ok: false,
            error: settled.error ?? { name: "Error", message: "unknown" },
          },
    );
  }

  #rememberSettledMutation(
    requestKey: string,
    method: string,
    fingerprint: { signature: string; retainedBytes: number },
    outcome: ServedOutcome,
  ): void {
    const outcomeBytes = estimateRpcValueBytes(outcome);
    while (
      this.#settledMutations.size >= DEDUPE_CACHE_SIZE ||
      this.#settledMutationBytes + outcomeBytes > RPC_SETTLED_OUTCOME_BYTES
    ) {
      const oldest = this.#settledMutations.keys().next().value;
      if (oldest === undefined) break;
      const removed = this.#settledMutations.get(oldest);
      this.#settledMutations.delete(oldest);
      this.#settledMutationBytes -= removed?.outcomeBytes ?? 0;
    }
    if (outcomeBytes > RPC_SETTLED_OUTCOME_BYTES) return;
    this.#settledMutations.set(requestKey, {
      method,
      signature: fingerprint.signature,
      requestBytes: fingerprint.retainedBytes,
      outcome,
      outcomeBytes,
    });
    this.#settledMutationBytes += outcomeBytes;
  }

  #rejectReusedRequestIdentity(message: OpMessage): void {
    this.#answer(message.from, {
      kind: "result",
      requestId: message.requestId,
      ok: false,
      error: {
        name: "Error",
        message: "The OPFS RPC request identity was reused with different contents",
      },
    });
  }

  async #executeServedOpAfterGate(
    leader: OpfsLeader,
    message: OpMessage,
    isRead: boolean,
  ): Promise<ServedOutcome> {
    const gate = this.#servedMutationGateForTests;
    if (!isRead && gate !== undefined) await gate;
    return this.#executeServedOp(leader, message);
  }

  async #executeServedOp(leader: OpfsLeader, message: OpMessage): Promise<ServedOutcome> {
    try {
      const method = (
        leader as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>
      )[message.method];
      if (method === undefined) {
        return { ok: false, error: { name: "Error", message: "Unknown store operation" } };
      }
      return { ok: true, value: await method.apply(leader, message.args) };
    } catch (error) {
      return { ok: false, error: serializeStoreError(error) };
    }
  }

  /** Posts on the shared channel: leadership traffic, heard by every connection. */
  #post(message: StoreRpcMessage): void {
    this.#channel?.postMessage(message);
  }

  /**
   * Posts an operation into the leader's inbox through a channel opened for this one message.
   * Every follower posts into the same inbox, and a `BroadcastChannel` hears whatever any other
   * object of its name posts — an outbound channel kept open would deliver every other
   * follower's operation (block bytes included) to this tab. Opened and closed around a single
   * message, nothing but the leader's inbox is ever listening on that name.
   */
  #send(leaderId: string, message: OpMessage): void {
    // Once the channels are torn down nothing may be posted.
    if (this.#channel === undefined) return;
    this.#postOnce(leaderId, message);
  }

  /**
   * Posts an answer into the requester's inbox. Only the leader posts there, so the leader
   * keeps these channels open across requests and hears nothing through them. They close on
   * demotion: an ex-leader's open channel would hear the next leader's answers to that tab.
   */
  #answer(requesterId: string, message: StoreRpcMessage): void {
    if (this.#channel === undefined) return;
    if (this.#leader === undefined) {
      // An answer finishing after demotion; do not reopen a channel that demotion just closed.
      this.#postOnce(requesterId, message);
      return;
    }
    let outbound = this.#answerChannels.get(requesterId);
    if (outbound === undefined) {
      outbound = new BroadcastChannel(this.#inboxName(requesterId));
      this.#answerChannels.set(requesterId, outbound);
      if (this.#answerChannels.size > ANSWER_CHANNEL_CACHE_SIZE) {
        // Followers leave without a goodbye; the oldest entry is the likeliest to be gone.
        const [oldest] = this.#answerChannels;
        if (oldest !== undefined) {
          this.#answerChannels.delete(oldest[0]);
          oldest[1].close();
        }
      }
    }
    outbound.postMessage(message);
  }

  #closeAnswerChannels(): void {
    for (const outbound of this.#answerChannels.values()) outbound.close();
    this.#answerChannels.clear();
  }

  /** Posts one message into an inbox through a channel that lives only for that message. */
  #postOnce(instanceId: string, message: StoreRpcMessage): void {
    const outbound = new BroadcastChannel(this.#inboxName(instanceId));
    outbound.postMessage(message);
    outbound.close();
  }

  #inboxName(instanceId: string): string {
    return `${this.#channelName}:${instanceId}`;
  }

  /** Closes every channel; nothing is posted or delivered after this. */
  #closeChannels(): void {
    this.#channel?.close();
    this.#channel = undefined;
    this.#inbox?.close();
    this.#inbox = undefined;
    this.#closeAnswerChannels();
  }

  // ---------------------------------------------------------------------------------------
  // Dispatch: local when leading, RPC when following, elect when leaderless.
  // ---------------------------------------------------------------------------------------

  #assertOpen(): void {
    if (this.#closed) throw new Error("This OPFS store connection is closed");
  }

  async #dispatch(method: string, args: unknown[]): Promise<unknown> {
    this.#assertOpen();
    const requestId = crypto.randomUUID();
    for (let attempt = 0; attempt < DISPATCH_ATTEMPTS; attempt += 1) {
      // Re-checked each attempt: the awaits below (elections, RPC round trips) give close()
      // every opportunity to run.
      this.#assertOpen();
      const leader = this.#leader;
      if (leader !== undefined) {
        const bound = (
          leader as unknown as Record<string, (...call: unknown[]) => Promise<unknown>>
        )[method];
        if (bound === undefined) throw new Error(`Unknown store operation: ${method}`);
        return bound.apply(leader, args);
      }
      if (this.#channel === undefined) {
        if (await this.#tryBecomeLeader()) continue;
        throw new Error(
          "Another connection holds this OPFS database, and BroadcastChannel is unavailable to reach it",
        );
      }
      if (!this.#leaderKnown()) {
        this.#post({ kind: "ping" });
        await sleep(DISCOVERY_WAIT_MS);
        if (!this.#leaderKnown()) {
          if (await this.#tryBecomeLeader()) continue;
          await sleep(20 + Math.random() * 50);
          continue;
        }
      }
      try {
        return await this.#rpc(requestId, method, args);
      } catch (error) {
        if (error === RPC_TIMED_OUT) {
          this.#knownLeader = undefined;
          if (!READ_METHODS.has(method)) throw new OpfsUncertainOutcomeError(method);
          continue;
        }
        throw error;
      }
    }
    throw new Error("The OPFS store could not reach or become a leader");
  }

  /** The channel can change #knownLeader between any two awaits; a method defeats narrowing. */
  #leaderKnown(): boolean {
    return this.#knownLeader !== undefined;
  }

  #rpc(requestId: string, method: string, args: unknown[]): Promise<unknown> {
    let retainedBytes: number;
    try {
      retainedBytes = estimateRpcValueBytes(args);
    } catch (error) {
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }
    if (
      this.#pending.size >= RPC_IN_FLIGHT_LIMIT ||
      this.#pendingRpcBytes + retainedBytes > RPC_IN_FLIGHT_MUTATION_BYTES
    ) {
      return Promise.reject(new Error("The OPFS follower request queue is full"));
    }
    return new Promise<unknown>((resolve, reject) => {
      const message: OpMessage = { kind: "op", requestId, from: this.#instanceId, method, args };
      const timer = setTimeout(() => {
        const expired = this.#takePending(requestId);
        expired?.reject(RPC_TIMED_OUT);
      }, this.#rpcTimeoutMs);
      const leaderId = this.#knownLeader;
      this.#pending.set(requestId, {
        message,
        retainedBytes,
        sentTo: leaderId,
        resolve,
        reject,
        timer,
      });
      this.#pendingRpcBytes += retainedBytes;
      // The dispatch loop only gets here with a leader known; should it have slipped away in
      // between, the timeout (or the next leader's announcement, which re-sends) takes over.
      if (leaderId !== undefined) this.#send(leaderId, message);
    });
  }

  // ---------------------------------------------------------------------------------------
  // The BlockStore surface.
  // ---------------------------------------------------------------------------------------

  async putTempRunPage(page: TempRunPage): Promise<void> {
    validateTempRunPage(page);
    await this.#dispatch("putTempRunPage", [page]);
  }

  async putTempRunPages(pages: readonly TempRunPage[]): Promise<void> {
    // Spill pages are instance-local files; the batch is a convenience, not a round-trip win.
    assertTempRunPageBatchLimits(pages);
    for (const page of pages) validateTempRunPage(page);
    await this.#dispatch("putTempRunPages", [pages]);
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    return (await this.#dispatch("getTempRunPage", [ownerId, runId, pageIndex])) as
      Uint8Array | undefined;
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    await this.#dispatch("removeTempRun", [ownerId, runId]);
  }

  /** @internal Shared implementation installed for every ordinary RPC method below. */
  async _dispatchGenerated(method: string, args: unknown[]): Promise<unknown> {
    const bulkIndex = (BULK_READ_ARGUMENT_INDEX as Partial<Record<string, number>>)[method];
    if (bulkIndex !== undefined) {
      const bulkItems = args[bulkIndex];
      if (!Array.isArray(bulkItems)) throw new TypeError(`${method} items must be an array`);
      assertStorageBulkReadItems(bulkItems, `${method} request`);
    }
    return this.#dispatch(method, args);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#reacquireTimer !== undefined) clearTimeout(this.#reacquireTimer);
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error("This OPFS store connection is closed"));
    }
    this.#pending.clear();
    this.#inFlightMutations.clear();
    this.#settledMutations.clear();
    this.#servedRequestLocks.clear();
    this.#pendingRpcBytes = 0;
    this.#inFlightMutationBytes = 0;
    this.#inFlightReadBytes = 0;
    this.#settledMutationBytes = 0;
    this.#servedRequestCount = 0;
    this.#servedRequestBytes = 0;
    const leader = this.#leader;
    this.#leader = undefined;
    if (leader !== undefined) {
      void leader
        .shutdown()
        .catch(() => {
          leader.crash();
        })
        .then(() => {
          this.#post({ kind: "released", leaderId: this.#instanceId });
          this.#closeChannels();
        });
      return;
    }
    this.#closeChannels();
  }

  /** Test-only: whether this connection currently holds the database's handles. */
  _isLeaderForTests(): boolean {
    return this.#leader !== undefined;
  }

  /** Test-only: the id that names this connection's inbox channel. */
  _instanceIdForTests(): string {
    return this.#instanceId;
  }

  /** Test-only: simulates an acknowledgement lost after the served operation settles. */
  _dropNextRpcResultForTests(): void {
    this.#dropNextRpcResultForTests = true;
  }

  /** Test-only: holds newly admitted served mutations until the returned release is called. */
  _holdServedMutationsForTests(): () => void {
    if (this.#servedMutationGateForTests !== undefined) {
      throw new Error("Served mutations are already held");
    }
    let release!: () => void;
    this.#servedMutationGateForTests = new Promise((resolve) => {
      release = resolve;
    });
    return () => {
      this.#servedMutationGateForTests = undefined;
      release();
    };
  }

  /** Test-only: retransmits the oldest request with its stable deduplication identity. */
  _resendOldestPendingForTests(): void {
    const pending = this.#pending.values().next().value;
    if (pending?.sentTo !== undefined) this.#send(pending.sentTo, pending.message);
  }

  /** Test-only counters that pin the connection's bounded RPC state and close-time cleanup. */
  _residentStateForTests(): {
    answerChannels: number;
    dedupeEntries: number;
    inFlightMutations: number;
    pendingRequests: number;
    retainedRpcBytes: number;
    closed: boolean;
  } {
    return {
      answerChannels: this.#answerChannels.size,
      dedupeEntries: this.#settledMutations.size,
      inFlightMutations: this.#inFlightMutations.size,
      pendingRequests: this.#pending.size,
      retainedRpcBytes:
        this.#pendingRpcBytes +
        this.#inFlightMutationBytes +
        this.#inFlightReadBytes +
        this.#settledMutationBytes +
        this.#servedRequestBytes,
      closed: this.#closed,
    };
  }

  /** Test-only: what tab death looks like — locks release, nothing flushes, no goodbyes. */
  _crashForTests(): void {
    this.#closed = true;
    if (this.#reacquireTimer !== undefined) clearTimeout(this.#reacquireTimer);
    for (const pending of this.#pending.values()) clearTimeout(pending.timer);
    this.#pending.clear();
    this.#inFlightMutations.clear();
    this.#settledMutations.clear();
    this.#servedRequestLocks.clear();
    this.#pendingRpcBytes = 0;
    this.#inFlightMutationBytes = 0;
    this.#inFlightReadBytes = 0;
    this.#settledMutationBytes = 0;
    this.#servedRequestCount = 0;
    this.#servedRequestBytes = 0;
    this.#leader?.crash();
    this.#leader = undefined;
    this.#closeChannels();
  }

  async #ensureFormatMarker(): Promise<void> {
    const existing = await this.#tree.readFile(["format.json"], {
      lockedMeansAbsent: true,
      maxBytes: 1024,
    });
    if (existing !== undefined) {
      try {
        this.#validateFormatMarker(existing);
        return;
      } catch (error) {
        if (!(error instanceof SyntaxError)) throw error;
        // A torn marker from a crashed first open is repairable only while no database
        // artifacts exist. Never stamp the current version over an unversioned WAL.
      }
    }
    let artifact: string | undefined;
    for await (const name of this.#tree.iterateNames([])) {
      if (name !== "format.json") {
        artifact = name;
        break;
      }
    }
    if (artifact !== undefined) {
      throw new StorageCorruptionError(
        "opfs",
        "format.json",
        `The OPFS format marker is ${existing === undefined ? "missing" : "torn"}, but the ` +
          `database directory already contains storage artifacts (${artifact}). ` +
          `Refusing to guess their layout version.`,
      );
    }
    const bytes = new TextEncoder().encode(JSON.stringify({ formatVersion: LOG_FORMAT_VERSION }));
    try {
      await this.#tree.writeFile(["format.json"], bytes, { flush: true });
    } catch (error) {
      if (!isLockContention(error)) throw error;
      // Another opener owns the marker write. Do not assume it writes this build's version:
      // wait until its handle closes, then validate the bytes it actually published.
      for (let attempt = 0; attempt < 40; attempt += 1) {
        await sleep(5 + Math.random() * 10);
        const published = await this.#tree.readFile(["format.json"], {
          lockedMeansAbsent: true,
          maxBytes: 1024,
        });
        if (published === undefined) continue;
        this.#validateFormatMarker(published);
        return;
      }
      throw new Error("A concurrent opener did not publish a readable OPFS format marker", {
        cause: error,
      });
    }
  }

  #validateFormatMarker(bytes: Uint8Array): void {
    const text = new TextDecoder().decode(bytes);
    const parsed = JSON.parse(text) as unknown;
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed) ||
      !Number.isSafeInteger((parsed as { formatVersion?: unknown }).formatVersion)
    ) {
      throw new StorageCorruptionError(
        "opfs",
        "format.json",
        "The OPFS format marker is invalid: formatVersion must be a safe integer",
      );
    }
    const formatVersion = (parsed as { formatVersion: number }).formatVersion;
    if (formatVersion !== LOG_FORMAT_VERSION) {
      throw new StorageFormatVersionError(
        "opfs",
        "format.json",
        formatVersion,
        LOG_FORMAT_VERSION,
        formatVersion < LOG_FORMAT_VERSION ? "older" : "newer",
      );
    }
    if (
      Object.keys(parsed).length !== 1 ||
      !Object.hasOwn(parsed, "formatVersion") ||
      text !== JSON.stringify({ formatVersion })
    ) {
      throw new StorageCorruptionError(
        "opfs",
        "format.json",
        "The OPFS format marker is not the canonical layout marker",
      );
    }
  }
}

for (const method of RPC_METHODS) {
  if (method in OpfsBlockStore.prototype) continue;
  Object.defineProperty(OpfsBlockStore.prototype, method, {
    configurable: true,
    value(this: OpfsBlockStore, ...args: unknown[]) {
      return this._dispatchGenerated(method, args);
    },
  });
}

/** Removes every file of a database created by `OpfsBlockStore.open` under this name. */
export async function deleteOpfsDatabase(options: {
  name: string;
  root?: FileSystemDirectoryHandle;
}): Promise<void> {
  const encodedName = encodeSegment(validateStorageDatabaseName(options.name));
  const root = options.root ?? (await navigator.storage.getDirectory());
  try {
    const namespace = await root.getDirectoryHandle("minnowdb");
    await namespace.removeEntry(encodedName, { recursive: true });
  } catch (error) {
    if (!isDomError(error, "NotFoundError")) throw error;
  }
}

const RPC_TIMED_OUT = new Error("The leader did not answer in time");

async function resolveDatabaseRoot(
  options: OpfsBlockStoreOptions,
): Promise<FileSystemDirectoryHandle> {
  const encodedName = encodeSegment(validateStorageDatabaseName(options.name));
  const root = options.root ?? (await navigator.storage.getDirectory());
  const namespace = await root.getDirectoryHandle("minnowdb", { create: true });
  return namespace.getDirectoryHandle(encodedName, { create: true });
}

function isLockContention(error: unknown): boolean {
  return isDomError(error, "NoModificationAllowedError") || isDomError(error, "InvalidStateError");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
