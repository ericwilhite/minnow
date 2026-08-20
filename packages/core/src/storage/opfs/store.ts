import {
  type BeginTransactionInput,
  type BeginTransactionResult,
  type BlockStore,
  type BlockWrite,
  type CatalogProbe,
  type CommitTransactionInput,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type CreateGarbageCollectionJobInput,
  type FtsCandidates,
  type FtsColumnIndexRecord,
  type GarbageCollectionJobRecord,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  type Manifest,
  type ManifestSummary,
  type PublishManifestInput,
  type QueryCatalogState,
  type RowIdRange,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type TableColumnRecord,
  type TableRecord,
  type TempOwnerRecord,
  type TempRunPage,
  type TransactionRecord,
  type TransactionRecordUpdate,
  type TriggerRecord,
  type FtsPosting,
} from "../types.js";
import type { DatabaseSnapshot, SnapshotLoadProgress } from "../snapshot.js";
import {
  validateId,
  validateTempRunPage,
  validateTempRunPageIdentity,
} from "../toolkit/record-core.js";
import { OpfsTree, encodeSegment, isDomError } from "./files.js";
import { LOG_FORMAT_VERSION } from "../toolkit/wire.js";
import { OpfsLeader } from "./leader.js";
import {
  rehydrateStoreError,
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
/** Channels into requesters' inboxes a leader keeps open between answers. */
const ANSWER_CHANNEL_CACHE_SIZE = 64;

export interface OpfsBlockStoreOptions {
  /** Databases live under `minnowdb/<name>` in the origin's private file system. */
  name: string;
  /**
   * `"relaxed"` (the default, matching the IndexedDB store) lets the operating system
   * schedule the final flush to disk; `"strict"` flushes every log frame and payload write
   * before the operation resolves.
   */
  durability?: "relaxed" | "strict";
  /** The storage root; defaults to `navigator.storage.getDirectory()`. Tests inject a shim. */
  root?: FileSystemDirectoryHandle;
  /** @internal Test seam: checkpoint after this many WAL entries (default 1024). */
  checkpointEntries?: number;
  /** @internal Test seam: how long a follower waits for the leader (default 1000ms). */
  rpcTimeoutMs?: number;
}

/** Methods a follower may invoke on the leader. Temp-page IO is instance-local by design. */
const RPC_METHODS = new Set([
  "addBlock",
  "addBlocks",
  "getBlock",
  "getBlocks",
  "removeBlock",
  "listBlockIds",
  "addTable",
  "getTable",
  "getTableByName",
  "listTables",
  "updateTable",
  "removeTable",
  "writeFtsBase",
  "readFtsCandidates",
  "addSegment",
  "getSegment",
  "listSegments",
  "removeSegment",
  "reserveRowIds",
  "reserveAutoIncrement",
  "getExistingUniqueKeys",
  "getCurrentManifest",
  "getCurrentManifestVersion",
  "getCatalogProbe",
  "getQueryCatalogState",
  "getManifest",
  "listManifests",
  "listManifestPage",
  "publishManifest",
  "createTransaction",
  "beginTransaction",
  "getTransaction",
  "getTransactions",
  "listTransactions",
  "listTransactionPage",
  "updateTransaction",
  "stageTransactionArtifacts",
  "commitTransaction",
  "createLease",
  "getLease",
  "listLeases",
  "renewLease",
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
  "getGarbageCollectionJob",
  "listGarbageCollectionJobs",
  "runGarbageCollectionStep",
  "removeGarbageCollectionJob",
  "createTempOwner",
  "getTempOwner",
  "renewTempOwner",
  "removeTempOwnerIfExpired",
  "removeTempOwner",
  "listTempOwnerIdsPage",
  "getLogicalStorageBytes",
  "exportSnapshot",
  "importSnapshot",
]);

/**
 * Reads are idempotent — a lost-acknowledgement retry can simply run again — so only
 * mutations enter the dedupe cache, and read results (block bytes included) are never
 * retained on the leader.
 */
const READ_METHODS = new Set([
  "getBlock",
  "getBlocks",
  "listBlockIds",
  "getTable",
  "getTableByName",
  "listTables",
  "readFtsCandidates",
  "getSegment",
  "listSegments",
  "getExistingUniqueKeys",
  "getCurrentManifest",
  "getCurrentManifestVersion",
  "getCatalogProbe",
  "getQueryCatalogState",
  "getManifest",
  "listManifests",
  "listManifestPage",
  "getTransaction",
  "getTransactions",
  "listTransactions",
  "listTransactionPage",
  "getLease",
  "listLeases",
  "getCompactionJob",
  "listCompactionJobs",
  "listCompactionJobPage",
  "getGarbageCollectionJob",
  "listGarbageCollectionJobs",
  "getTempOwner",
  "listTempOwnerIdsPage",
  "getLogicalStorageBytes",
  "exportSnapshot",
]);

interface ServedOutcome {
  ok: boolean;
  value?: unknown;
  error?: SerializedStoreError;
}

type OpMessage = Extract<StoreRpcMessage, { kind: "op" }>;

interface PendingRpc {
  message: OpMessage;
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
 * acknowledged only after the leader's WAL holds it, a lost message costs a retry (request
 * ids deduplicate retries while the same leader serves), and a dead leader costs a failover
 * in which the next acquirer replays checkpoint-plus-WAL. A retry that crosses a failover is
 * the one case a duplicate can execute — the same "committed but the response was lost"
 * class the IndexedDB store has — and it resolves the same way: compare-and-swap operations
 * surface a conflict the engine's recovery already reconciles. The channel affects how fast
 * multi-tab work moves, never whether it is right.
 *
 * `setForeground(true)` marks this connection as the one the user is looking at; a background
 * leader yields to a foreground bidder, so the tab doing the work is normally the tab holding
 * the microsecond-fast path.
 */
export class OpfsBlockStore implements BlockStore {
  readonly #tree: OpfsTree;
  readonly #durability: "relaxed" | "strict";
  readonly #checkpointEntries: number | undefined;
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
  readonly #dedupe = new Map<string, Promise<ServedOutcome>>();
  #reacquireTimer: ReturnType<typeof setTimeout> | undefined;

  private constructor(tree: OpfsTree, options: OpfsBlockStoreOptions) {
    this.#tree = tree;
    this.#durability = options.durability ?? "relaxed";
    this.#checkpointEntries = options.checkpointEntries;
    this.#rpcTimeoutMs = options.rpcTimeoutMs ?? RPC_TIMEOUT_MS;
    this.#channelName = `minnowdb-store:${options.name}`;
  }

  static async open(options: OpfsBlockStoreOptions): Promise<OpfsBlockStore> {
    const tree = new OpfsTree(await resolveDatabaseRoot(options));
    const store = new OpfsBlockStore(tree, options);
    await store.#ensureFormatMarker();
    if (typeof BroadcastChannel === "function") {
      const onMessage = (event: MessageEvent<unknown>) => {
        store.#onMessage(event.data as StoreRpcMessage);
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
        if (this.#leader !== undefined) void this.#serveOp(message);
        return;
      }
      case "result": {
        const pending = this.#pending.get(message.requestId);
        if (pending === undefined) return;
        this.#pending.delete(message.requestId);
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
          this.#pending.delete(message.requestId);
          pending.reject(RPC_TIMED_OUT);
        }, this.#rpcTimeoutMs);
        return;
      }
      case "leader": {
        this.#knownLeader = message.leaderId;
        if (this.#reacquireTimer !== undefined) {
          clearTimeout(this.#reacquireTimer);
          this.#reacquireTimer = undefined;
        }
        // A new leader never saw our in-flight requests; send those again (ids deduplicate).
        // Requests already sitting with this same leader stay put — every ping triggers an
        // announce, and blind re-sends would race their own in-flight executions.
        for (const pending of this.#pending.values()) {
          if (pending.sentTo === message.leaderId) continue;
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

  async #serveOp(message: OpMessage): Promise<void> {
    const leader = this.#leader;
    if (leader === undefined || !RPC_METHODS.has(message.method)) return;
    let outcome = this.#dedupe.get(message.requestId);
    if (outcome === undefined) {
      outcome = this.#executeServedOp(leader, message);
      // Only mutations need at-most-once; reads simply run again on a retried request, and
      // caching their results (block bytes included) would be pure memory waste.
      if (!READ_METHODS.has(message.method)) {
        this.#dedupe.set(message.requestId, outcome);
        if (this.#dedupe.size > DEDUPE_CACHE_SIZE) {
          const [oldest] = this.#dedupe.keys();
          if (oldest !== undefined) this.#dedupe.delete(oldest);
        }
      }
    } else {
      // A duplicate of a request that is still running (or already remembered): attach to it
      // rather than executing twice, and reset the caller's patience meanwhile — the original
      // may legitimately take longer than one timeout (a snapshot import, a checkpoint pause).
      this.#answer(message.from, { kind: "busy", requestId: message.requestId });
    }
    const settled = await outcome;
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
    return new Promise<unknown>((resolve, reject) => {
      const message: OpMessage = { kind: "op", requestId, from: this.#instanceId, method, args };
      const timer = setTimeout(() => {
        this.#pending.delete(requestId);
        reject(RPC_TIMED_OUT);
      }, this.#rpcTimeoutMs);
      const leaderId = this.#knownLeader;
      this.#pending.set(requestId, { message, sentTo: leaderId, resolve, reject, timer });
      // The dispatch loop only gets here with a leader known; should it have slipped away in
      // between, the timeout (or the next leader's announcement, which re-sends) takes over.
      if (leaderId !== undefined) this.#send(leaderId, message);
    });
  }

  // ---------------------------------------------------------------------------------------
  // The BlockStore surface.
  // ---------------------------------------------------------------------------------------

  async addBlock(id: string, bytes: Uint8Array): Promise<void> {
    await this.#dispatch("addBlock", [id, bytes]);
  }

  async addBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    await this.#dispatch("addBlocks", [blocks]);
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    return (await this.#dispatch("getBlock", [id])) as Uint8Array | undefined;
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    return (await this.#dispatch("getBlocks", [ids])) as Array<Uint8Array | undefined>;
  }

  async removeBlock(id: string): Promise<void> {
    await this.#dispatch("removeBlock", [id]);
  }

  async listBlockIds(): Promise<string[]> {
    return (await this.#dispatch("listBlockIds", [])) as string[];
  }

  async putTempRunPage(page: TempRunPage): Promise<void> {
    validateTempRunPage(page);
    await this.#tree.writeFile(
      ["temp", encodeSegment(page.ownerId), encodeSegment(page.runId), String(page.pageIndex)],
      page.bytes,
    );
  }

  async putTempRunPages(pages: readonly TempRunPage[]): Promise<void> {
    // Spill pages are instance-local files; the batch is a convenience, not a round-trip win.
    for (const page of pages) await this.putTempRunPage(page);
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    return this.#tree.readFile([
      "temp",
      encodeSegment(ownerId),
      encodeSegment(runId),
      String(pageIndex),
    ]);
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    await this.#tree.deleteTree(["temp", encodeSegment(ownerId), encodeSegment(runId)]);
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    validateId(ownerId);
    await this.#dispatch("removeTempOwner", [ownerId]);
    await this.#tree.deleteTree(["temp", encodeSegment(ownerId)]);
  }

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    await this.#dispatch("createTempOwner", [record]);
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    return (await this.#dispatch("getTempOwner", [ownerId])) as TempOwnerRecord | undefined;
  }

  async renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord> {
    return (await this.#dispatch("renewTempOwner", [
      ownerId,
      expectedRevision,
      expiresAt,
    ])) as TempOwnerRecord;
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    const removed = (await this.#dispatch("removeTempOwnerIfExpired", [
      ownerId,
      expiresAtCutoff,
    ])) as boolean;
    if (removed) await this.#tree.deleteTree(["temp", encodeSegment(ownerId)]);
    return removed;
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    return (await this.#dispatch("listTempOwnerIdsPage", [afterOwnerId, limit])) as StoragePage<
      string,
      string
    >;
  }

  async addTable(record: TableRecord): Promise<void> {
    await this.#dispatch("addTable", [record]);
  }

  async getTable(id: string): Promise<TableRecord | undefined> {
    return (await this.#dispatch("getTable", [id])) as TableRecord | undefined;
  }

  async getTableByName(name: string): Promise<TableRecord | undefined> {
    return (await this.#dispatch("getTableByName", [name])) as TableRecord | undefined;
  }

  async listTables(): Promise<TableRecord[]> {
    return (await this.#dispatch("listTables", [])) as TableRecord[];
  }

  async updateTable(
    id: string,
    expectedRevision: number,
    update: {
      columns?: TableColumnRecord[];
      ftsColumns?: Record<string, FtsColumnIndexRecord> | null;
      triggers?: TriggerRecord[] | null;
    },
  ): Promise<TableRecord> {
    return (await this.#dispatch("updateTable", [id, expectedRevision, update])) as TableRecord;
  }

  async removeTable(id: string, expectedRevision: number): Promise<void> {
    await this.#dispatch("removeTable", [id, expectedRevision]);
  }

  async writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void> {
    await this.#dispatch("writeFtsBase", [tableId, columnId, input]);
  }

  async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: ReadonlyArray<{ term: string; prefix: boolean }>,
    upToVersion: number,
  ): Promise<
    FtsCandidates & { deltaChunkCount: number; totalTokens: number; coversVersion: number }
  > {
    return (await this.#dispatch("readFtsCandidates", [
      tableId,
      columnId,
      terms,
      upToVersion,
    ])) as FtsCandidates & { deltaChunkCount: number; totalTokens: number; coversVersion: number };
  }

  async addSegment(record: SegmentRecord): Promise<void> {
    await this.#dispatch("addSegment", [record]);
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    return (await this.#dispatch("getSegment", [id])) as SegmentRecord | undefined;
  }

  async listSegments(tableId?: string): Promise<SegmentRecord[]> {
    return (await this.#dispatch("listSegments", [tableId])) as SegmentRecord[];
  }

  async removeSegment(id: string): Promise<void> {
    await this.#dispatch("removeSegment", [id]);
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    return (await this.#dispatch("reserveRowIds", [tableId, count])) as RowIdRange;
  }

  async reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    return (await this.#dispatch("reserveAutoIncrement", [
      tableId,
      columnId,
      count,
      atLeast,
    ])) as RowIdRange;
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    return (await this.#dispatch("getExistingUniqueKeys", [tableId, keyTokens])) as string[];
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    return (await this.#dispatch("getCurrentManifest", [])) as Manifest | undefined;
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    return (await this.#dispatch("getCurrentManifestVersion", [])) as number | null;
  }

  async getCatalogProbe(): Promise<CatalogProbe> {
    return (await this.#dispatch("getCatalogProbe", [])) as CatalogProbe;
  }

  async getQueryCatalogState(tableNames: readonly string[]): Promise<QueryCatalogState> {
    return (await this.#dispatch("getQueryCatalogState", [tableNames])) as QueryCatalogState;
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    return (await this.#dispatch("getManifest", [version])) as Manifest | undefined;
  }

  async listManifests(): Promise<Manifest[]> {
    return (await this.#dispatch("listManifests", [])) as Manifest[];
  }

  async listManifestPage(
    afterVersion: number | null,
    limit: number,
  ): Promise<StoragePage<Manifest, number>> {
    return (await this.#dispatch("listManifestPage", [afterVersion, limit])) as StoragePage<
      Manifest,
      number
    >;
  }

  async publishManifest(input: PublishManifestInput): Promise<Manifest> {
    return (await this.#dispatch("publishManifest", [input])) as Manifest;
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    await this.#dispatch("createTransaction", [record]);
  }

  async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    return (await this.#dispatch("beginTransaction", [input])) as BeginTransactionResult;
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    return (await this.#dispatch("getTransaction", [id])) as TransactionRecord | undefined;
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    return (await this.#dispatch("getTransactions", [ids])) as Array<TransactionRecord | undefined>;
  }

  async listTransactions(): Promise<TransactionRecord[]> {
    return (await this.#dispatch("listTransactions", [])) as TransactionRecord[];
  }

  async listTransactionPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<TransactionRecord, string>> {
    return (await this.#dispatch("listTransactionPage", [afterId, limit])) as StoragePage<
      TransactionRecord,
      string
    >;
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    return (await this.#dispatch("updateTransaction", [
      id,
      expectedRevision,
      update,
    ])) as TransactionRecord;
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return (await this.#dispatch("stageTransactionArtifacts", [input])) as TransactionRecord;
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    return (await this.#dispatch("commitTransaction", [input])) as ManifestSummary;
  }

  async createLease(record: LeaseRecord): Promise<void> {
    await this.#dispatch("createLease", [record]);
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    return (await this.#dispatch("getLease", [id])) as LeaseRecord | undefined;
  }

  async listLeases(): Promise<LeaseRecord[]> {
    return (await this.#dispatch("listLeases", [])) as LeaseRecord[];
  }

  async renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord> {
    return (await this.#dispatch("renewLease", [id, expectedRevision, expiresAt])) as LeaseRecord;
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    return (await this.#dispatch("removeLeaseIfExpired", [
      id,
      expectedRevision,
      expiresAtCutoff,
    ])) as boolean;
  }

  async removeLease(id: string): Promise<void> {
    await this.#dispatch("removeLease", [id]);
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    await this.#dispatch("createCompactionJob", [record]);
  }

  async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    return (await this.#dispatch("getCompactionJob", [id])) as CompactionJobRecord | undefined;
  }

  async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    return (await this.#dispatch("listCompactionJobs", [tableId])) as CompactionJobRecord[];
  }

  async listCompactionJobPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<CompactionJobRecord, string>> {
    return (await this.#dispatch("listCompactionJobPage", [afterId, limit])) as StoragePage<
      CompactionJobRecord,
      string
    >;
  }

  async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
    return (await this.#dispatch("updateCompactionJob", [
      id,
      expectedRevision,
      update,
    ])) as CompactionJobRecord;
  }

  async cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    return (await this.#dispatch("cancelCompactionJob", [
      id,
      expectedRevision,
      cancelledAt,
    ])) as CompactionJobRecord;
  }

  async removeCompactionJob(id: string): Promise<void> {
    await this.#dispatch("removeCompactionJob", [id]);
  }

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    return (await this.#dispatch("createGarbageCollectionJob", [
      input,
    ])) as GarbageCollectionJobRecord;
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    return (await this.#dispatch("getGarbageCollectionJob", [id])) as
      GarbageCollectionJobRecord | undefined;
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return (await this.#dispatch("listGarbageCollectionJobs", [])) as GarbageCollectionJobRecord[];
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    return (await this.#dispatch("runGarbageCollectionStep", [
      input,
    ])) as GarbageCollectionStepResult;
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    await this.#dispatch("removeGarbageCollectionJob", [id]);
  }

  async getLogicalStorageBytes(): Promise<number> {
    return (await this.#dispatch("getLogicalStorageBytes", [])) as number;
  }

  async exportSnapshot(): Promise<DatabaseSnapshot> {
    return (await this.#dispatch("exportSnapshot", [])) as DatabaseSnapshot;
  }

  async importSnapshot(
    snapshot: DatabaseSnapshot,
    options: { onProgress?: (progress: SnapshotLoadProgress) => void } = {},
  ): Promise<void> {
    const leader = this.#leader;
    if (leader !== undefined) {
      return leader.importSnapshot(snapshot, options);
    }
    // The progress callback cannot cross the channel; report the two ends locally.
    const totalBytes = snapshot.blocks.reduce((total, block) => total + block.bytes.byteLength, 0);
    options.onProgress?.({ phase: "blocks", writtenBytes: 0, totalBytes });
    await this.#dispatch("importSnapshot", [snapshot, {}]);
    options.onProgress?.({ phase: "done", writtenBytes: totalBytes, totalBytes });
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

  /** Test-only: what tab death looks like — locks release, nothing flushes, no goodbyes. */
  _crashForTests(): void {
    this.#closed = true;
    if (this.#reacquireTimer !== undefined) clearTimeout(this.#reacquireTimer);
    for (const pending of this.#pending.values()) clearTimeout(pending.timer);
    this.#pending.clear();
    this.#leader?.crash();
    this.#leader = undefined;
    this.#closeChannels();
  }

  async #ensureFormatMarker(): Promise<void> {
    const existing = await this.#tree.readFile(["format.json"], { lockedMeansAbsent: true });
    if (existing !== undefined) {
      try {
        const parsed = JSON.parse(new TextDecoder().decode(existing)) as {
          formatVersion?: number;
        };
        if (
          typeof parsed.formatVersion === "number" &&
          parsed.formatVersion !== LOG_FORMAT_VERSION
        ) {
          throw new Error(
            `This database uses OPFS layout version ${String(parsed.formatVersion)}; this build ` +
              `reads version ${String(LOG_FORMAT_VERSION)}. Delete it with deleteOpfsDatabase() — ` +
              `earlier layouts were experimental and carry no compatibility promise.`,
          );
        }
        return;
      } catch (error) {
        if (!(error instanceof SyntaxError)) throw error;
        // A torn marker from a crashed first open: rewrite it below.
      }
    }
    const bytes = new TextEncoder().encode(JSON.stringify({ formatVersion: LOG_FORMAT_VERSION }));
    try {
      await this.#tree.writeFile(["format.json"], bytes, { flush: true });
    } catch (error) {
      // A concurrent open won the marker; theirs says the same thing.
      if (!isLockContention(error)) throw error;
    }
  }
}

/** Removes every file of a database created by `OpfsBlockStore.open` under this name. */
export async function deleteOpfsDatabase(options: {
  name: string;
  root?: FileSystemDirectoryHandle;
}): Promise<void> {
  const root = options.root ?? (await navigator.storage.getDirectory());
  try {
    const namespace = await root.getDirectoryHandle("minnowdb");
    await namespace.removeEntry(encodeSegment(options.name), { recursive: true });
  } catch (error) {
    if (!isDomError(error, "NotFoundError")) throw error;
  }
}

const RPC_TIMED_OUT = new Error("The leader did not answer in time");

async function resolveDatabaseRoot(
  options: OpfsBlockStoreOptions,
): Promise<FileSystemDirectoryHandle> {
  if (options.name.length === 0) throw new TypeError("Database name cannot be empty");
  const root = options.root ?? (await navigator.storage.getDirectory());
  const namespace = await root.getDirectoryHandle("minnowdb", { create: true });
  return namespace.getDirectoryHandle(encodeSegment(options.name), { create: true });
}

function isLockContention(error: unknown): boolean {
  return isDomError(error, "NoModificationAllowedError") || isDomError(error, "InvalidStateError");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
