import {
  assertStorageBulkReadItems,
  assertTempRunPageBatchLimits,
  OpfsCoordinationError,
  OpfsDatabaseInUseError,
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
import { OpfsLeader, OpfsLeaderClosedError, type ServedMutationRequest } from "./leader.js";
import {
  rehydrateStoreError,
  estimateRpcValueBytes,
  fingerprintStoreRequest,
  MAX_OPFS_RPC_HOLD_MS,
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
/**
 * How long an operation keeps looking for a leader — discover, call, elect, again — before it
 * gives up. Measured from the last sign that the database is changing hands: a connection
 * that holds the handles while it recovers the log or checkpoints on its way out answers
 * pings with `wait`, and each such answer restarts this budget, so a long recovery of a big
 * database is waited out while a leader that is frozen with the handles is not.
 */
const DISPATCH_BUDGET_MS = 10_000;
/** A leader yields to a foreground bidder at most this often; a bid inside it is deferred. */
const YIELD_COOLDOWN_MS = 3_000;
/** After yielding, the ex-leader stays out of elections this long so the bidder can win. */
const HANDOVER_GRACE_MS = 1_500;
/**
 * A hidden leader with other connections around lets go of its handles after this long with
 * nothing to do. A browser may freeze a hidden tab, worker and all, and a frozen leader can
 * neither serve nor yield; releasing while idle keeps the database available to the tabs that
 * are awake. Its own next operation simply elects again.
 */
const HIDDEN_IDLE_RELEASE_MS = 15_000;
/**
 * A mutation whose leader falls silent is not given up on at once: the follower pings, and a
 * leader that answers the ping is alive and still holds the request. This many extra rounds
 * of patience are allowed before the outcome is declared uncertain.
 */
const MUTATION_PATIENCE_ROUNDS = 3;
/** How long a closed connection keeps declining requests that were already on their way. */
const DECLINE_AFTER_CLOSE_MS = 1_000;
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
  /** @internal Test seam: how long served requests stay answerable (default 10 minutes). */
  servedLedgerAgeMs?: number;
  /** @internal Test seam: the largest served result the ledger retains (default 64 KiB). */
  servedLedgerResultBytes?: number;
  /** @internal Test seam: how long a follower waits for the leader (default 1000ms). */
  rpcTimeoutMs?: number;
  /** @internal Test seam: minimum spacing between foreground yields (default 3000ms). */
  yieldCooldownMs?: number;
  /** @internal Test seam: idle time before a hidden leader releases (default 15000ms). */
  hiddenIdleReleaseMs?: number;
  /** @internal Test seam: how long an ex-leader stays out of elections (default 1500ms). */
  handoverGraceMs?: number;
  /** @internal Test seam: how long an operation looks for a leader (default 10000ms). */
  dispatchBudgetMs?: number;
  /**
   * Hears failures no operation reports: a background checkpoint or cleanup that failed, an
   * election or handover that threw, a served request that could not be answered. The worker
   * host wires it to the client's `onWorkerError`; a direct caller may log it.
   */
  onDiagnostic?: (error: unknown, context: string) => void;
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
  /** Timeouts survived so far by pinging a leader that turned out to be alive. */
  patienceRounds: number;
  /** A ping is out; the leader's own announcement, not a stranger's, restores the timer. */
  awaitingPing: boolean;
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
  readonly #servedLedgerAgeMs: number | undefined;
  readonly #servedLedgerResultBytes: number | undefined;
  readonly #rpcTimeoutMs: number;
  readonly #yieldCooldownMs: number;
  readonly #hiddenIdleReleaseMs: number;
  readonly #handoverGraceMs: number;
  readonly #dispatchBudgetMs: number;
  readonly #onDiagnostic: ((error: unknown, context: string) => void) | undefined;
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
  /** Holding the handles and recovering the log: the moment between winning and leading. */
  #recovering = false;
  /** When a connection holding the handles last said it was still recovering or leaving. */
  #waitHeardAt = 0;
  readonly #pending = new Map<string, PendingRpc>();
  readonly #inFlightMutations = new Map<string, ServedMutation<Promise<ServedOutcome>>>();
  readonly #settledMutations = new Map<string, ServedMutation<ServedOutcome>>();
  readonly #servedRequestLocks = new Map<string, Promise<void>>();
  #inFlightMutationBytes = 0;
  #inFlightReadBytes = 0;
  #readCapacityChanged: { promise: Promise<void>; resolve: () => void } | undefined;
  #settledMutationBytes = 0;
  #pendingRpcBytes = 0;
  #servedRequestCount = 0;
  #servedRequestBytes = 0;
  #servedMutationGateForTests: Promise<void> | undefined;
  #dropNextRpcResultForTests = false;
  #reacquireTimer: ReturnType<typeof setTimeout> | undefined;
  /** While leading: every admitted request, so keepalives and holds reach its requester. */
  readonly #served = new Map<string, string>();
  #keepaliveTimer: ReturnType<typeof setInterval> | undefined;
  /** A foreground bid that arrived inside the yield cooldown, honored when it ends. */
  #deferredBid: { bidderId: string; timer: ReturnType<typeof setTimeout> } | undefined;
  #hiddenIdleTimer: ReturnType<typeof setTimeout> | undefined;
  #inboxLingerTimer: ReturnType<typeof setTimeout> | undefined;
  /** When this connection last did storage work of its own or for a follower. */
  #lastActivityAt = Date.now();
  /** Whether any other connection has ever spoken; a lone leader never releases for idleness. */
  #othersSeen = false;
  /**
   * Leaders that said goodbye. A request they still hold is answered — with its result or a
   * decline — so it waits, where a request to a leader that vanished cannot.
   */
  readonly #gracefulLeaders = new Set<string>();

  private constructor(tree: OpfsTree, options: OpfsBlockStoreOptions) {
    this.#tree = tree;
    this.#durability = options.durability ?? "strict";
    this.#checkpointEntries = options.checkpointEntries;
    this.#cleanupLimitBytes = options.cleanupLimitBytes;
    this.#servedLedgerAgeMs = options.servedLedgerAgeMs;
    this.#servedLedgerResultBytes = options.servedLedgerResultBytes;
    this.#rpcTimeoutMs = options.rpcTimeoutMs ?? RPC_TIMEOUT_MS;
    this.#yieldCooldownMs = options.yieldCooldownMs ?? YIELD_COOLDOWN_MS;
    this.#hiddenIdleReleaseMs = options.hiddenIdleReleaseMs ?? HIDDEN_IDLE_RELEASE_MS;
    this.#handoverGraceMs = options.handoverGraceMs ?? HANDOVER_GRACE_MS;
    this.#dispatchBudgetMs = options.dispatchBudgetMs ?? DISPATCH_BUDGET_MS;
    this.#onDiagnostic = options.onDiagnostic;
    this.#channelName = `minnowdb-store:${options.name}`;
    this.liveQueryChannelName = `minnowdb-live:opfs:${options.name}`;
  }

  #releaseConnectionLock: (() => void) | undefined;

  static async open(options: OpfsBlockStoreOptions): Promise<OpfsBlockStore> {
    const ownedOptions = { ...options };
    validateStorageDatabaseName(ownedOptions.name);
    const release = await holdConnectionLock(ownedOptions.name);
    let store: OpfsBlockStore;
    try {
      const tree = new OpfsTree(await resolveDatabaseRoot(ownedOptions));
      store = new OpfsBlockStore(tree, ownedOptions);
    } catch (error) {
      release?.();
      throw error;
    }
    // Once a store owns the lock, only its handle-cleanup path may release it. In particular,
    // an announcement failure after recovery still has live handles to shut down.
    store.#releaseConnectionLock = release;
    try {
      await store.#ensureFormatMarker();
      if (typeof BroadcastChannel === "function") {
        const onMessage = (event: MessageEvent<unknown>) => {
          const message = parseStoreRpcMessage(event.data, RPC_METHODS);
          if (message !== undefined) store.#onMessage(message);
        };
        const channel = new BroadcastChannel(store.#channelName);
        channel.onmessage = onMessage;
        store.#channel = channel;
        const inbox = new BroadcastChannel(store.#inboxName(store.#instanceId));
        inbox.onmessage = onMessage;
        store.#inbox = inbox;
      }
      await store.#tryBecomeLeader();
      return store;
    } catch (error) {
      store.close();
      throw error;
    }
  }

  // ---------------------------------------------------------------------------------------
  // Leadership.
  // ---------------------------------------------------------------------------------------

  /** Reports a failure no caller awaits; a throwing hook is contained. */
  #diagnostic(error: unknown, context: string): void {
    try {
      this.#onDiagnostic?.(error, context);
    } catch {
      // A diagnostic hook must never turn a background failure into a second one.
    }
  }

  /** Fire-and-forget election: a failure is reported, never left as an unhandled rejection. */
  #electInBackground(context: string, force = false): void {
    this.#tryBecomeLeader(force).catch((error: unknown) => {
      this.#diagnostic(error, context);
    });
  }

  /**
   * `force` skips the handover grace: only the reacquire timer, whose job is to make sure the
   * database is held by someone when the bidder never turns up, may take the handles back
   * this soon after yielding them.
   */
  async #tryBecomeLeader(force = false): Promise<boolean> {
    if (this.#closed) return false;
    if (this.#leader !== undefined) return true;
    if (this.#yielding !== undefined) return false;
    if (!force && Date.now() - this.#lastYieldAt < this.#handoverGraceMs) return false;
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
    // The handles are held from here on: nobody else can lead until recovery ends, so every
    // connection looking for a leader is told to wait rather than run out its patience.
    this.#recovering = true;
    try {
      this.#post({ kind: "wait", leaderId: this.#instanceId });
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
        this.#onDiagnostic,
        this.#servedLedgerAgeMs,
        this.#servedLedgerResultBytes,
      );
    } catch (error) {
      wal.close();
      slotA?.close();
      slotB?.close();
      if (isLockContention(error)) return false;
      throw error;
    } finally {
      this.#recovering = false;
    }
    if (this.#closed) {
      // close() ran while this election was in flight; a leader installed now would hold the
      // browser's file lock with no owner to ever release it.
      const leader = this.#leader;
      this.#leader = undefined;
      await leader.shutdown().catch((error: unknown) => {
        this.#diagnostic(error, "opfs shutdown after close during election");
        leader.crash();
      });
      return false;
    }
    this.#knownLeader = this.#instanceId;
    this.#leader.onBeforeCheckpoint = (expectedMs) => {
      this.#holdServed(expectedMs);
    };
    this.#post({ kind: "leader", leaderId: this.#instanceId });
    this.#post({ kind: "state", leaderId: this.#instanceId, foreground: this.#foreground });
    if (!this.#foreground) this.#armHiddenIdleTimer();
    return true;
  }

  /** Tells every requester the leader holds that a long synchronous step is about to run. */
  #holdServed(expectedMs: number): void {
    if (this.#served.size === 0) return;
    const ms = Math.min(MAX_OPFS_RPC_HOLD_MS, expectedMs * 2 + this.#rpcTimeoutMs);
    for (const [requestId, from] of this.#served)
      this.#answer(from, { kind: "hold", requestId, ms });
  }

  #keepaliveSuppressedForTests = false;

  /** @internal Simulates a leader from a release without keepalives. */
  _suppressKeepaliveForTests(): void {
    this.#keepaliveSuppressedForTests = true;
    this.#stopKeepalive();
  }

  #startKeepalive(): void {
    if (this.#keepaliveTimer !== undefined || this.#keepaliveSuppressedForTests) return;
    const timer = setInterval(
      () => {
        if (this.#served.size === 0) {
          clearInterval(timer);
          if (this.#keepaliveTimer === timer) this.#keepaliveTimer = undefined;
          return;
        }
        for (const [requestId, from] of this.#served) {
          this.#answer(from, { kind: "busy", requestId });
        }
      },
      Math.max(1, Math.floor(this.#rpcTimeoutMs / 2)),
    );
    (timer as { unref?: () => void }).unref?.();
    this.#keepaliveTimer = timer;
  }

  #stopKeepalive(): void {
    if (this.#keepaliveTimer === undefined) return;
    clearInterval(this.#keepaliveTimer);
    this.#keepaliveTimer = undefined;
  }

  /**
   * A hidden leader that has been idle for the whole release window, with other connections
   * around, lets go of its handles. Activity of any kind restarts the window.
   */
  #armHiddenIdleTimer(): void {
    this.#clearHiddenIdleTimer();
    if (this.#foreground || this.#leader === undefined || this.#closed) return;
    const elapsed = Date.now() - this.#lastActivityAt;
    const timer = setTimeout(
      () => {
        this.#hiddenIdleTimer = undefined;
        if (this.#closed || this.#leader === undefined || this.#foreground) return;
        if (
          this.#othersSeen &&
          this.#served.size === 0 &&
          this.#yielding === undefined &&
          Date.now() - this.#lastActivityAt >= this.#hiddenIdleReleaseMs
        ) {
          this.#demote().catch((error: unknown) => {
            this.#diagnostic(error, "opfs idle release");
          });
          return;
        }
        this.#armHiddenIdleTimer();
      },
      Math.max(0, this.#hiddenIdleReleaseMs - elapsed),
    );
    (timer as { unref?: () => void }).unref?.();
    this.#hiddenIdleTimer = timer;
  }

  #clearHiddenIdleTimer(): void {
    if (this.#hiddenIdleTimer === undefined) return;
    clearTimeout(this.#hiddenIdleTimer);
    this.#hiddenIdleTimer = undefined;
  }

  /** Releases the handles with no successor named; whoever next needs the database elects. */
  async #demote(): Promise<void> {
    const leader = this.#leader;
    if (leader === undefined) return;
    this.#leader = undefined;
    this.#knownLeader = undefined;
    const shutdown = this.#shutdownAfterMutations(leader).catch((error: unknown) => {
      this.#diagnostic(error, "opfs idle release shutdown");
      leader.crash();
    });
    this.#yielding = shutdown;
    await shutdown;
    if (this.#yielding === shutdown) this.#yielding = undefined;
    if (this.#closed) return;
    this.#closeAnswerChannels();
    this.#post({ kind: "released", leaderId: this.#instanceId });
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
    if (this.#closed) return;
    const changed = this.#foreground !== foreground;
    this.#foreground = foreground;
    if (this.#leader !== undefined) {
      if (foreground) {
        this.#clearHiddenIdleTimer();
        this.#clearDeferredBid();
      } else {
        this.#lastActivityAt = Date.now();
        this.#armHiddenIdleTimer();
      }
      // Foreground followers bid on hearing a background leader; a leader that just went
      // hidden while another tab stayed visible would otherwise keep the fast path for good.
      if (changed) this.#post({ kind: "state", leaderId: this.#instanceId, foreground });
      return;
    }
    if (foreground) this.#post({ kind: "bid", bidderId: this.#instanceId, foreground: true });
  }

  #clearDeferredBid(): void {
    if (this.#deferredBid === undefined) return;
    clearTimeout(this.#deferredBid.timer);
    this.#deferredBid = undefined;
  }

  /** Yields now when the cooldown allows it, otherwise when the cooldown ends. */
  #considerBid(bidderId: string): void {
    if (this.#leader === undefined || this.#foreground || this.#yielding !== undefined) return;
    const remaining = this.#yieldCooldownMs - (Date.now() - this.#lastYieldAt);
    if (remaining <= 0) {
      this.#clearDeferredBid();
      this.#yieldLeadership(bidderId).catch((error: unknown) => {
        this.#diagnostic(error, "opfs yield");
      });
      return;
    }
    if (this.#deferredBid !== undefined) {
      this.#deferredBid.bidderId = bidderId;
      return;
    }
    const timer = setTimeout(() => {
      const deferred = this.#deferredBid;
      this.#deferredBid = undefined;
      if (deferred !== undefined && !this.#closed) this.#considerBid(deferred.bidderId);
    }, remaining);
    (timer as { unref?: () => void }).unref?.();
    this.#deferredBid = { bidderId, timer };
  }

  async #yieldLeadership(to: string): Promise<void> {
    const leader = this.#leader;
    if (leader === undefined) return;
    this.#clearHiddenIdleTimer();
    this.#clearDeferredBid();
    this.#leader = undefined;
    this.#knownLeader = undefined;
    this.#lastYieldAt = Date.now();
    const shutdown = this.#shutdownAfterMutations(leader).catch((error: unknown) => {
      // Whatever failed, the handles must not outlive the leadership; crash-close is
      // idempotent and releases them.
      this.#diagnostic(error, "opfs yield shutdown");
      leader.crash();
    });
    this.#yielding = shutdown;
    await shutdown;
    if (this.#yielding === shutdown) this.#yielding = undefined;
    if (this.#closed) return;
    // An open channel into a follower's inbox would hear the next leader's answers to it.
    this.#closeAnswerChannels();
    this.#post({ kind: "yield", to });
    // Every other follower must stop posting into this inbox and discover the next leader.
    this.#post({ kind: "released", leaderId: this.#instanceId });
    // If the bidder loses the race or vanishes, someone must still hold the database.
    if (this.#reacquireTimer !== undefined) clearTimeout(this.#reacquireTimer);
    this.#reacquireTimer = setTimeout(() => {
      if (this.#knownLeader === undefined && !this.#closed) {
        this.#electInBackground("opfs reacquire after yield", true);
      }
    }, this.#handoverGraceMs);
  }

  #yielding: Promise<void> | undefined;

  /**
   * Shuts a leader down from its place in the mutation queue, so a served mutation still
   * running finishes whole first — its result frame in the log, its answer on its way — and
   * the checkpoint the shutdown writes carries that request as settled. Shutting down from
   * outside the queue would land the checkpoint between the mutation's frame and its result,
   * and the next leader would have to call a durable, known-value write uncertain.
   */
  #shutdownAfterMutations(leader: OpfsLeader): Promise<void> {
    return this.#withMutationTurn(() => leader.shutdown());
  }

  /** Resolves once every request this connection admitted is answered, or the linger ends. */
  async #servedDrained(): Promise<void> {
    const deadline = Date.now() + DECLINE_AFTER_CLOSE_MS;
    while (this.#served.size > 0 && Date.now() < deadline) await sleep(5);
  }

  #onMessage(message: StoreRpcMessage): void {
    if (this.#closed) {
      // A request that was already on its way when this connection closed never ran here.
      // Saying so lets the requester send it to the next leader instead of waiting out a
      // timeout and reporting an outcome it cannot know.
      if (message.kind === "op") this.#decline(message);
      return;
    }
    if (this.#coordinationPausedForTests) return;
    this.#othersSeen = true;
    switch (message.kind) {
      case "op": {
        if (this.#leader === undefined) {
          this.#decline(message);
          return;
        }
        {
          const requestBytes = estimateRpcValueBytes(message.args);
          if (
            this.#servedRequestCount >= RPC_SERVER_ADMISSION_LIMIT ||
            this.#servedRequestBytes + requestBytes > RPC_SERVER_ADMISSION_BYTES
          ) {
            this.#answer(message.from, {
              kind: "result",
              requestId: message.requestId,
              ok: false,
              error: serializeStoreError(
                new OpfsCoordinationError("leader-queue-full", message.method),
              ),
            });
            return;
          }
          this.#servedRequestCount += 1;
          this.#servedRequestBytes += requestBytes;
          this.#lastActivityAt = Date.now();
          const alreadyServing = this.#served.has(message.requestId);
          this.#served.set(message.requestId, message.from);
          this.#startKeepalive();
          void this.#serveOp(message)
            .catch((error: unknown) => {
              // The requester must not wait out its timeout for an answer that will never come.
              this.#diagnostic(error, `opfs served ${message.method}`);
              this.#answer(message.from, {
                kind: "result",
                requestId: message.requestId,
                ok: false,
                error: serializeStoreError(error),
              });
            })
            .finally(() => {
              this.#servedRequestCount = Math.max(0, this.#servedRequestCount - 1);
              this.#servedRequestBytes = Math.max(0, this.#servedRequestBytes - requestBytes);
              if (!alreadyServing) this.#served.delete(message.requestId);
              this.#lastActivityAt = Date.now();
            });
        }
        return;
      }
      case "declined": {
        const pending = this.#takePending(message.requestId);
        if (pending === undefined) return;
        clearTimeout(pending.timer);
        pending.reject(RPC_DECLINED);
        return;
      }
      case "uncertain": {
        const pending = this.#takePending(message.requestId);
        if (pending === undefined) return;
        clearTimeout(pending.timer);
        pending.reject(new OpfsUncertainOutcomeError(pending.message.method));
        return;
      }
      case "hold": {
        const pending = this.#pending.get(message.requestId);
        if (pending === undefined) return;
        this.#armPendingTimer(pending, Math.max(message.ms, this.#rpcTimeoutMs));
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
        this.#armPendingTimer(pending, this.#rpcTimeoutMs);
        return;
      }
      case "leader": {
        this.#knownLeader = message.leaderId;
        this.#gracefulLeaders.delete(message.leaderId);
        if (this.#reacquireTimer !== undefined) {
          clearTimeout(this.#reacquireTimer);
          this.#reacquireTimer = undefined;
        }
        for (const pending of this.#pending.values()) {
          if (pending.sentTo === message.leaderId) {
            // The leader we were waiting on is alive; it still holds the request.
            if (pending.awaitingPing) this.#armPendingTimer(pending, this.#rpcTimeoutMs);
            continue;
          }
          if (pending.sentTo === undefined) {
            pending.sentTo = message.leaderId;
            this.#send(message.leaderId, pending.message);
            continue;
          }
          // A leader that said goodbye answers everything it holds, with a result or a decline,
          // so a mutation waits for that answer. One on a leader that vanished goes to the new
          // leader with the same identity and first-send time: the log it recovered either
          // holds the outcome, proves the request never ran, or says it cannot tell.
          if (
            !READ_METHODS.has(pending.message.method) &&
            this.#gracefulLeaders.has(pending.sentTo)
          ) {
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
      case "state": {
        if (
          !message.foreground &&
          this.#foreground &&
          this.#leader === undefined &&
          message.leaderId !== this.#instanceId
        ) {
          this.#post({ kind: "bid", bidderId: this.#instanceId, foreground: true });
        }
        return;
      }
      case "ping": {
        if (this.#leader !== undefined) {
          this.#post({ kind: "leader", leaderId: this.#instanceId });
        } else if (this.#recovering || this.#yielding !== undefined) {
          this.#post({ kind: "wait", leaderId: this.#instanceId });
        }
        return;
      }
      case "wait": {
        if (message.leaderId !== this.#instanceId) this.#waitHeardAt = Date.now();
        return;
      }
      case "bid": {
        if (message.foreground && message.bidderId !== this.#instanceId) {
          this.#considerBid(message.bidderId);
        }
        return;
      }
      case "yield": {
        if (message.to === this.#instanceId) {
          this.#knownLeader = undefined;
          this.#electInBackground("opfs election after yield", true);
        }
        return;
      }
      case "released": {
        if (this.#knownLeader === message.leaderId) this.#knownLeader = undefined;
        this.#gracefulLeaders.add(message.leaderId);
        if (this.#gracefulLeaders.size > 16) {
          const [oldest] = this.#gracefulLeaders;
          if (oldest !== undefined) this.#gracefulLeaders.delete(oldest);
        }
        return;
      }
    }
  }

  /** Answers a request this connection will not run: it is not the leader. */
  #decline(message: OpMessage): void {
    this.#postOnce(message.from, {
      kind: "declined",
      requestId: message.requestId,
      reason: "not-leader",
    });
  }

  /** Restarts a pending request's patience from a live signal (busy, hold, announce). */
  #armPendingTimer(pending: PendingRpc, ms: number): void {
    clearTimeout(pending.timer);
    pending.awaitingPing = false;
    pending.timer = setTimeout(() => {
      this.#onPendingTimeout(pending.message.requestId);
    }, ms);
  }

  /**
   * The leader has been silent for a whole timeout. A read simply fails over. A mutation first
   * asks whether the leader is still there: one that answers the ping is alive and still holds
   * the request, so the wait continues; only silence to the ping too, or exhausted patience,
   * makes the outcome uncertain.
   */
  #onPendingTimeout(requestId: string): void {
    const pending = this.#pending.get(requestId);
    if (pending === undefined) return;
    if (
      READ_METHODS.has(pending.message.method) ||
      pending.awaitingPing ||
      pending.patienceRounds >= MUTATION_PATIENCE_ROUNDS ||
      this.#channel === undefined
    ) {
      this.#takePending(requestId);
      pending.reject(RPC_TIMED_OUT);
      return;
    }
    pending.patienceRounds += 1;
    pending.awaitingPing = true;
    this.#post({ kind: "ping" });
    pending.timer = setTimeout(() => {
      this.#onPendingTimeout(requestId);
    }, DISCOVERY_WAIT_MS);
  }

  #takePending(requestId: string): PendingRpc | undefined {
    const pending = this.#pending.get(requestId);
    if (pending === undefined) return undefined;
    this.#pending.delete(requestId);
    this.#pendingRpcBytes -= pending.retainedBytes;
    return pending;
  }

  async #serveOp(message: OpMessage): Promise<void> {
    const requestKey = servedRequestKey(message.from, message.requestId);
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
    } catch (error) {
      this.#answer(message.from, {
        kind: "result",
        requestId: message.requestId,
        ok: false,
        error: serializeStoreError(error),
      });
      return;
    }
    let remembered = isRead ? undefined : this.#settledMutations.get(requestKey);
    const inFlight =
      isRead || remembered !== undefined ? undefined : this.#inFlightMutations.get(requestKey);
    if (remembered === undefined && inFlight === undefined && !isRead) {
      // The log outlives the leader that served it: a re-send after a failover finds its
      // outcome here, or learns that it happened but that its answer died with that leader.
      // A mutation this leader is still running is not "unsettled" in that sense — its first
      // frame is durable while its value is still coming — so the in-flight check came first.
      const logged = leader.servedOutcome(requestKey);
      if (logged !== undefined) {
        if (logged.method !== message.method || logged.signature !== fingerprint.signature) {
          this.#rejectReusedRequestIdentity(message);
          return;
        }
        if (!logged.settled || logged.withheld === true) {
          this.#answer(message.from, { kind: "uncertain", requestId: message.requestId });
          return;
        }
        const outcome: ServedOutcome = { ok: true, value: logged.result };
        this.#rememberSettledMutation(
          requestKey,
          logged.method,
          { signature: logged.signature, retainedBytes: logged.requestBytes },
          outcome,
        );
        remembered = this.#settledMutations.get(requestKey) ?? {
          method: logged.method,
          signature: logged.signature,
          requestBytes: logged.requestBytes,
          outcome,
        };
      }
    }
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
    } else if (inFlight !== undefined) {
      if (!sameServedRequest(inFlight, message, fingerprint.signature)) {
        this.#rejectReusedRequestIdentity(message);
        return;
      }
      // Never evict an in-flight mutation. A duplicate attaches to the one execution and resets
      // the requester's patience while it remains queued or running.
      this.#answer(message.from, { kind: "busy", requestId: message.requestId });
      settled = await inFlight.outcome;
    } else {
      if (!isRead && message.sentAt < leader.servedCoverageSince) {
        // First sent before this leader's ledger begins: an earlier leader may have executed
        // it and the log no longer says. Only the requester can reconcile that.
        this.#answer(message.from, { kind: "uncertain", requestId: message.requestId });
        return;
      }
      if (
        !isRead &&
        (this.#inFlightMutations.size >= RPC_IN_FLIGHT_LIMIT ||
          this.#inFlightMutationBytes + fingerprint.retainedBytes > RPC_IN_FLIGHT_MUTATION_BYTES)
      ) {
        this.#answer(message.from, {
          kind: "result",
          requestId: message.requestId,
          ok: false,
          error: serializeStoreError(
            new OpfsCoordinationError("mutation-queue-full", message.method),
          ),
        });
        return;
      }
      if (isRead) {
        const reservation = Math.max(fingerprint.retainedBytes, MAX_OPFS_RPC_MESSAGE_BYTES);
        while (this.#inFlightReadBytes + reservation > RPC_IN_FLIGHT_READ_BYTES) {
          // Incoming requests already have count/byte admission bounds. Reserve the maximum
          // response only while executing, so four tiny metadata reads do not overload a leader.
          this.#answer(message.from, { kind: "busy", requestId: message.requestId });
          if (this.#readCapacityChanged === undefined) {
            let resolve!: () => void;
            const promise = new Promise<void>((done) => {
              resolve = done;
            });
            this.#readCapacityChanged = { promise, resolve };
          }
          await this.#readCapacityChanged.promise;
          if (this.#closed || this.#leader !== leader) {
            this.#decline(message);
            return;
          }
        }
        if (this.#closed || this.#leader !== leader) {
          this.#decline(message);
          return;
        }
        this.#inFlightReadBytes += reservation;
        try {
          settled = await this.#executeServedOpAfterGate(leader, message, true);
        } finally {
          this.#inFlightReadBytes = Math.max(0, this.#inFlightReadBytes - reservation);
          this.#wakeReadWaiters();
        }
      } else {
        this.#inFlightMutationBytes += fingerprint.retainedBytes;
        const execution = this.#executeServedOpAfterGate(leader, message, false, {
          key: requestKey,
          method: message.method,
          signature: fingerprint.signature,
          requestBytes: fingerprint.retainedBytes,
          // The requester's clock decides coverage; one running ahead must not evict the ledger
          // and pin coverage in the future, so it is read no later than now.
          sentAt: Math.min(message.sentAt, Date.now()),
        });
        const outcome = execution.then((result) => {
          this.#inFlightMutations.delete(requestKey);
          this.#inFlightMutationBytes = Math.max(
            0,
            this.#inFlightMutationBytes - fingerprint.retainedBytes,
          );
          if (!this.#closed && result !== DECLINED_OUTCOME) {
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
    if (settled === DECLINED_OUTCOME) return;
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

  #wakeReadWaiters(): void {
    const waiting = this.#readCapacityChanged;
    this.#readCapacityChanged = undefined;
    waiting?.resolve();
  }

  async #executeServedOpAfterGate(
    leader: OpfsLeader,
    message: OpMessage,
    isRead: boolean,
    request?: ServedMutationRequest,
  ): Promise<ServedOutcome> {
    const gate = this.#servedMutationGateForTests;
    if (!isRead && gate !== undefined) await gate;
    return this.#executeServedOp(leader, message, request);
  }

  /** The channel can replace #leader between any two awaits; a method defeats narrowing. */
  #leads(leader: OpfsLeader): boolean {
    return this.#leader === leader;
  }

  #mutationTail: Promise<void> = Promise.resolve();

  /**
   * Mutations run through the leader one at a time, from call to completion, whether a
   * follower sent them or this connection issued them. The leader appends each one's frame
   * from its own queue anyway; what the turn adds is that the request identity the leader is
   * handed belongs to exactly one operation, so no frame can ever carry another's.
   */
  #withMutationTurn<T>(run: () => Promise<T>): Promise<T> {
    const turn = this.#mutationTail.then(run);
    this.#mutationTail = turn.then(
      () => undefined,
      () => undefined,
    );
    return turn;
  }

  async #executeServedOp(
    leader: OpfsLeader,
    message: OpMessage,
    request?: ServedMutationRequest,
  ): Promise<ServedOutcome> {
    if (leader.isClosed() || !this.#leads(leader)) {
      // Leadership moved while this request waited at the gate; nothing ran.
      this.#decline(message);
      return DECLINED_OUTCOME;
    }
    try {
      const method = (
        leader as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>
      )[message.method];
      if (method === undefined) {
        return { ok: false, error: { name: "Error", message: "Unknown store operation" } };
      }
      if (request === undefined) {
        return { ok: true, value: await method.apply(leader, message.args) };
      }
      return await this.#withMutationTurn(async () => {
        if (leader.isClosed() || !this.#leads(leader)) {
          this.#decline(message);
          return DECLINED_OUTCOME;
        }
        leader.servingRequest = request;
        try {
          const value: unknown = await method.apply(leader, message.args);
          // The identity is consumed by the first frame the mutation appends; one that appended
          // nothing (a refusal, a no-op) leaves it, and the log then holds nothing to answer.
          if (leader.servingRequest !== request) {
            await leader.completeServed(request.key, value).catch((error: unknown) => {
              this.#diagnostic(error, `opfs served result for ${message.method}`);
            });
          }
          return { ok: true, value };
        } finally {
          if (leader.servingRequest === request) leader.servingRequest = undefined;
        }
      });
    } catch (error) {
      if (leader.isClosed() && !this.#leads(leader)) {
        // The leader shut down before this request's turn on its queue came; work queued
        // behind a shutdown is refused before it runs, so nothing happened.
        this.#decline(message);
        return DECLINED_OUTCOME;
      }
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

  /**
   * Closes the shared channel now and the inbox a moment later. A request posted into this
   * inbox just before it closed would otherwise vanish, and its follower could only wait out
   * a timeout; for that moment the inbox stays open to answer each one with a decline.
   */
  #closeChannels(): void {
    this.#channel?.close();
    this.#channel = undefined;
    this.#closeAnswerChannels();
    const inbox = this.#inbox;
    this.#inbox = undefined;
    if (inbox === undefined) return;
    if (this.#inboxLingerTimer !== undefined) clearTimeout(this.#inboxLingerTimer);
    const timer = setTimeout(() => {
      this.#inboxLingerTimer = undefined;
      inbox.close();
    }, DECLINE_AFTER_CLOSE_MS);
    (timer as { unref?: () => void }).unref?.();
    (inbox as { unref?: () => void }).unref?.();
    this.#inboxLingerTimer = timer;
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
    const sentAt = Date.now();
    const isRead = READ_METHODS.has(method);
    let sentRemotely = false;
    // Set once an attempt may have executed somewhere: a timed-out delivery. A decline is a
    // proof that it did not.
    let mayHaveRun = false;
    this.#lastActivityAt = sentAt;
    // The budget runs from the last sign of a handover in progress, not from the first attempt.
    while (Date.now() - Math.max(sentAt, this.#waitHeardAt) < this.#dispatchBudgetMs) {
      // Re-checked each attempt: the awaits below (elections, RPC round trips) give close()
      // every opportunity to run.
      this.#assertOpen();
      const leader = this.#leader;
      if (leader !== undefined) {
        const bound = (
          leader as unknown as Record<string, (...call: unknown[]) => Promise<unknown>>
        )[method];
        if (bound === undefined) throw new Error(`Unknown store operation: ${method}`);
        try {
          if (isRead) return await bound.apply(leader, args);
          if (sentRemotely) {
            // This connection became the leader with its own request outstanding on a leader
            // that vanished. Its recovered log answers exactly like it would a follower's.
            const key = servedRequestKey(this.#instanceId, requestId);
            const logged = leader.servedOutcome(key);
            if (logged !== undefined) {
              if (!logged.settled || logged.withheld === true) {
                throw new OpfsUncertainOutcomeError(method);
              }
              return logged.result;
            }
            if (sentAt < leader.servedCoverageSince) throw new OpfsUncertainOutcomeError(method);
          }
          return await this.#withMutationTurn(() => {
            if (leader.isClosed() || !this.#leads(leader)) throw new OpfsLeaderClosedError();
            return bound.apply(leader, args);
          });
        } catch (error) {
          // The leader shut down — yielding to a foreground tab, releasing idle handles —
          // while this operation waited for its turn on the queue, and the queue refused it
          // before anything ran; a read on closed handles ran nothing either. Whoever leads
          // next answers it, possibly this connection once it holds the handles again.
          if (!this.#leads(leader) && (isRead || error instanceof OpfsLeaderClosedError)) {
            continue;
          }
          throw error;
        } finally {
          this.#lastActivityAt = Date.now();
        }
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
        sentRemotely = true;
        return await this.#rpc(requestId, method, args, sentAt);
      } catch (error) {
        if (error === RPC_DECLINED || error === RPC_TIMED_OUT) {
          // Declined: provably never ran there. Timed out: the leader is gone; whoever leads
          // next recovered its log and answers the same request from it.
          if (error === RPC_TIMED_OUT) mayHaveRun = true;
          this.#knownLeader = undefined;
          continue;
        }
        throw error;
      }
    }
    // A mutation whose delivery went silent may have run there; a read cannot have changed
    // anything, and neither can a mutation every leader declined.
    if (!isRead && mayHaveRun) throw new OpfsUncertainOutcomeError(method);
    throw new OpfsCoordinationError("leader-unavailable", method);
  }

  /** The channel can change #knownLeader between any two awaits; a method defeats narrowing. */
  #leaderKnown(): boolean {
    return this.#knownLeader !== undefined;
  }

  #rpc(requestId: string, method: string, args: unknown[], sentAt: number): Promise<unknown> {
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
      return Promise.reject(new OpfsCoordinationError("follower-queue-full", method));
    }
    return new Promise<unknown>((resolve, reject) => {
      const message: OpMessage = {
        kind: "op",
        requestId,
        from: this.#instanceId,
        method,
        args,
        sentAt,
      };
      const timer = setTimeout(() => {
        this.#onPendingTimeout(requestId);
      }, this.#rpcTimeoutMs);
      const leaderId = this.#knownLeader;
      this.#pending.set(requestId, {
        message,
        retainedBytes,
        sentTo: leaderId,
        resolve,
        reject,
        timer,
        patienceRounds: 0,
        awaitingPing: false,
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
    this.#clearCoordinationTimers();
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
    this.#wakeReadWaiters();
    this.#settledMutationBytes = 0;
    this.#servedRequestCount = 0;
    this.#servedRequestBytes = 0;
    const leader = this.#leader;
    this.#leader = undefined;
    if (leader !== undefined) {
      // A served mutation still running finishes and is answered before the goodbye: the
      // shutdown takes its turn on the mutation queue, and the channels stay open — keepalives
      // included — until every admitted request has its result or decline.
      void this.#shutdownAfterMutations(leader)
        .catch((error: unknown) => {
          this.#diagnostic(error, "opfs close shutdown");
          leader.crash();
        })
        .then(() => this.#servedDrained())
        .then(() => {
          this.#stopKeepalive();
          this.#post({ kind: "released", leaderId: this.#instanceId });
          this.#closeChannels();
          this.#releaseWhenHandlesClose();
        });
      return;
    }
    if (this.#yielding !== undefined) {
      // Closing mid-handover: the followers still need to hear that this leader is gone.
      void this.#yielding
        .then(() => this.#servedDrained())
        .then(() => {
          this.#stopKeepalive();
          this.#post({ kind: "released", leaderId: this.#instanceId });
          this.#closeChannels();
          this.#releaseWhenHandlesClose();
        });
      return;
    }
    this.#stopKeepalive();
    this.#closeChannels();
    this.#releaseWhenHandlesClose();
  }

  /** Stops the timers that would elect, yield, or release; keepalives stop when served drains. */
  #clearCoordinationTimers(): void {
    if (this.#reacquireTimer !== undefined) clearTimeout(this.#reacquireTimer);
    this.#reacquireTimer = undefined;
    this.#clearDeferredBid();
    this.#clearHiddenIdleTimer();
  }

  #releaseWhenHandlesClose(): void {
    const release = this.#releaseConnectionLock;
    this.#releaseConnectionLock = undefined;
    if (this.#electing === undefined && this.#yielding === undefined) release?.();
    else void Promise.allSettled([this.#electing, this.#yielding]).then(() => release?.());
  }

  /** Test-only: whether this connection currently holds the database's handles. */
  _isLeaderForTests(): boolean {
    return this.#leader !== undefined;
  }

  #coordinationPausedForTests = false;

  /** @internal Simulates a suspended leader without releasing its file handles. */
  _pauseCoordinationForTests(): () => void {
    this.#coordinationPausedForTests = true;
    return () => {
      this.#coordinationPausedForTests = false;
    };
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

  /** Test-only: the request id of the oldest pending request, for hand-posted answers. */
  _oldestPendingRequestIdForTests(): string | undefined {
    return this.#pending.keys().next().value;
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
    this.#clearCoordinationTimers();
    this.#stopKeepalive();
    this.#served.clear();
    for (const pending of this.#pending.values()) clearTimeout(pending.timer);
    this.#pending.clear();
    this.#inFlightMutations.clear();
    this.#settledMutations.clear();
    this.#servedRequestLocks.clear();
    this.#pendingRpcBytes = 0;
    this.#inFlightMutationBytes = 0;
    this.#inFlightReadBytes = 0;
    this.#wakeReadWaiters();
    this.#settledMutationBytes = 0;
    this.#servedRequestCount = 0;
    this.#servedRequestBytes = 0;
    this.#leader?.crash();
    this.#leader = undefined;
    // Tab death closes every channel at once; nothing lingers to decline.
    this.#channel?.close();
    this.#channel = undefined;
    this.#inbox?.close();
    this.#inbox = undefined;
    this.#closeAnswerChannels();
    this.#releaseWhenHandlesClose();
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

/**
 * How long deletion waits for a closing connection to let go of its lock. A store releases the
 * lock only once its leader has shut down and its handles are closed, which finishes after
 * `close()` returns and after a worker's dispose reply, so a delete that follows a close by a
 * few milliseconds must not read the lingering lock as an open connection. A connection that is
 * genuinely open holds the lock for its whole life, and is refused once the wait runs out.
 */
const DELETE_LOCK_WAIT_MS = 1_000;

/** Removes every file of a database created by `OpfsBlockStore.open` under this name. */
export async function deleteOpfsDatabase(options: {
  name: string;
  root?: FileSystemDirectoryHandle;
}): Promise<void> {
  const encodedName = encodeSegment(validateStorageDatabaseName(options.name));
  const locks = (globalThis as { navigator?: { locks?: LockManager } }).navigator?.locks;
  if (locks === undefined) throw new Error("Deleting an OPFS database requires Web Locks");
  const abort = new AbortController();
  const timer = setTimeout(() => {
    abort.abort();
  }, DELETE_LOCK_WAIT_MS);
  try {
    await locks.request(
      connectionLockName(options.name),
      { mode: "exclusive", signal: abort.signal },
      async () => {
        const root = options.root ?? (await navigator.storage.getDirectory());
        try {
          const namespace = await root.getDirectoryHandle("minnowdb");
          await namespace.removeEntry(encodedName, { recursive: true });
        } catch (error) {
          if (!isDomError(error, "NotFoundError")) throw error;
        }
      },
    );
  } catch (error) {
    // Aborting a request that was never granted rejects it; one granted before the abort runs
    // to completion. So an abort here means the wait ran out with the lock still held.
    if (abort.signal.aborted && isDomError(error, "AbortError")) {
      throw new OpfsDatabaseInUseError(options.name);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function connectionLockName(name: string): string {
  return `minnowdb-opfs-connections:${name}`;
}

/** Shared lifetime locks keep deletion exclusive with both leaders and idle followers. */
async function holdConnectionLock(name: string): Promise<(() => void) | undefined> {
  const locks = (globalThis as { navigator?: { locks?: LockManager } }).navigator?.locks;
  if (locks === undefined) return undefined;
  return new Promise<() => void>((resolve, reject) => {
    void locks
      .request(
        connectionLockName(name),
        { mode: "shared" },
        () =>
          new Promise<void>((release) => {
            resolve(release);
          }),
      )
      .catch(reject);
  });
}

/** The dedupe and ledger key of a request: requester instance id plus request id, unambiguous. */
function servedRequestKey(from: string, requestId: string): string {
  return `${String(from.length)}:${from}${requestId}`;
}

const RPC_TIMED_OUT = new Error("The leader did not answer in time");
const RPC_DECLINED = new Error("The connection asked is not the leader");
/** A served request that was declined rather than executed; never answered as a result. */
const DECLINED_OUTCOME: ServedOutcome = { ok: false };

async function resolveDatabaseRoot(
  options: OpfsBlockStoreOptions,
): Promise<FileSystemDirectoryHandle> {
  const encodedName = encodeSegment(validateStorageDatabaseName(options.name));
  const root = options.root ?? (await navigator.storage.getDirectory());
  const namespace = await root.getDirectoryHandle("minnowdb", { create: true });
  return namespace.getDirectoryHandle(encodedName, { create: true });
}

/**
 * Whether a database directory of this name exists in the origin's private file system (or
 * under `root`). Nothing is created: an `auto` descriptor asks this before choosing a store for
 * a name it has no record of, so a database an explicit `opfs` descriptor created is found
 * rather than shadowed by an empty one.
 */
export async function opfsDatabaseExists(options: {
  name: string;
  root?: FileSystemDirectoryHandle;
}): Promise<boolean> {
  const encodedName = encodeSegment(validateStorageDatabaseName(options.name));
  try {
    const root = options.root ?? (await navigator.storage.getDirectory());
    const namespace = await root.getDirectoryHandle("minnowdb");
    await namespace.getDirectoryHandle(encodedName);
    return true;
  } catch {
    return false;
  }
}

function isLockContention(error: unknown): boolean {
  return isDomError(error, "NoModificationAllowedError") || isDomError(error, "InvalidStateError");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
