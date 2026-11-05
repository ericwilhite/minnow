/**
 * Exactly-once requests: the owner's per-client ledger of terminal outcomes (review finding 5).
 *
 * ## Identity
 * - A client has a `clientId` of 16 random bytes for its whole page lifetime. It keeps it across
 *   reconnects and owner changes; a reload is a new client.
 * - A client runs one or more engine **sessions** (`sessionId`, unique within the client: each has
 *   its own transaction state). A session's connection to the owner is a **generation**: it
 *   starts at 1 and a reconnect is the next one. A transaction lives in one generation.
 * - Each request carries `(clientId, sessionId, generation, sequence, sentAtMs, resolvedThrough)`
 *   and its `scope` (below); a retry resends them unchanged. Sequences are per client, increase as
 *   the client issues them, may arrive out of order (up to `MAX_OUTSTANDING_REQUESTS` in flight,
 *   the worker protocol's limit), and may have gaps. `resolvedThrough` is the highest S such that
 *   the client holds the outcome of every request ≤ S.
 * - `scope` is `transaction` for a statement the client sent inside an explicit transaction it
 *   opened (after BEGIN, before COMMIT or ROLLBACK), and `standalone` for everything else: an
 *   autocommit statement, BEGIN, COMMIT, ROLLBACK, a read. The client knows it; the owner cannot
 *   tell after a reconnect, when the transaction is gone.
 *
 * ## Admission (`RequestLedger.admit`), checked by the receiver before executing anything
 * - `expired`: `now − sentAtMs > CLIENT_RETRY_WINDOW_MS`. Never executed; the receiver answers
 *   `RequestExpiredError` (next/errors.ts).
 * - `stale`: sequence ≤ the client's `resolvedThrough`: the client already has the outcome; a
 *   late duplicate is dropped, never executed.
 * - `window`: sequence > resolvedThrough + MAX_OUTSTANDING_REQUESTS: a protocol violation.
 * - `inFlight`: the same request is executing: the duplicate waits for that execution's outcome.
 * - `duplicate`: the outcome is recorded: a committed write answers "already committed" with its
 *   original affected-row counts (RETURNING rows are not replayed: the duplicate gets counts
 *   only); a failure answers with the same error code.
 * - `sessionLost`: a `transaction`-scope request of a generation that has ended (and no outcome
 *   is held for it). Never executed: its transaction rolled back with the generation; the receiver
 *   answers `SessionLostError` (next/errors.ts).
 * - `execute`: first sight; run it.
 *
 * ## Outcomes by kind of request
 * - **Commit** (an autocommit write, or COMMIT): exactly once. A durable `committed` outcome
 *   (below), recorded once the commit is durable.
 * - **Statement inside an open transaction** (`transaction` scope): it changed the transaction's
 *   state, so running it twice would apply it twice. It records a `statement` (or `failed`)
 *   outcome, in memory only, keyed by its `(clientId, sessionId, generation)` (ruling R16.1). A
 *   duplicate gets it back instead of running again.
 * - **Standalone queries** (reads, including `SELECT nextval(...)`, and anything else that is not
 *   a commit) are **at-least-once** (ruling R16.2): they are `settle`d, which records nothing, so a
 *   retry runs them again. A `nextval` whose answer was lost is a sequence gap, which PostgreSQL
 *   allows. Only commits are exactly-once.
 * - **Failure**: a `failed` outcome, in memory only.
 *
 * ## Sessions (`endSession`, review A1-5 finding 2)
 * `endSession(clientId, sessionId, generation, nowMs)` closes that generation of that session (and any
 * earlier one): when the connection drops, the client's Web Lock is released, or the session is
 * closed. Its open transaction rolls back. Then:
 * - its `transaction`-scope requests leave `inFlight` and their outcomes are dropped; a late
 *   completion for them (`record` after the close) is ignored and never recorded, and a retry is
 *   `sessionLost`;
 * - its pending `standalone` requests are untouched: a commit in progress completes and records
 *   its durable outcome, so its retry gets `duplicate`;
 * - every other session of the client, and every other client, is untouched: their retry
 *   protection stays.
 * The owner ends every admitted request with `record` or `settle`, so nothing stays in flight.
 * The ledger remembers an ended generation for two windows: any request of it was sent before the
 * client saw that connection end (within the owner liveness timeout of `endSession`, far below a
 * window), so by then its retries are `expired`. A client's ended sessions go with the client.
 *
 * ## Durability and bounds
 * - Committed outcomes are durable: the commit's log frame carries the request (log-frame.ts
 *   `CommitFrameBody.request`), and the root's ledger checkpoints them (`globalLsn`, replay.ts).
 *   Failed outcomes live in memory only: after an owner restart a retried failure re-executes,
 *   which is safe because the failed attempt changed nothing.
 * - An outcome is kept until the client acknowledges it (`resolvedThrough`) or
 *   `CLIENT_RETRY_WINDOW_MS` after it was recorded, whichever is first. Any retry the receiver
 *   accepts was sent within the window before now, and its original was recorded after it was
 *   sent, so its outcome is still present. A client with no outcome and no request for a window
 *   is forgotten. So the ledger holds at most `MAX_OUTSTANDING_REQUESTS` outcomes per active
 *   client.
 * - The clock is the device clock shared by the client's tab and the owner's worker. Setting the
 *   clock back by more than the window can make an expired retry look fresh: documented, not
 *   defended.
 */
export const CLIENT_RETRY_WINDOW_MS = 10 * 60 * 1000;
export const MAX_OUTSTANDING_REQUESTS = 256;

/** One generation of one engine session of a client. */
export interface SessionRef {
  readonly sessionId: string;
  /** 1 for the first connection; a reconnect is the next. */
  readonly generation: number;
}

export interface RequestIdentity extends SessionRef {
  /** Hex of the 16-byte client id. */
  readonly clientId: string;
  readonly sequence: number;
  readonly sentAtMs: number;
  readonly resolvedThrough: number;
  /** "Identity": `transaction` inside an explicit transaction the client opened. */
  readonly scope: "transaction" | "standalone";
}

export type Outcome =
  | {
      readonly kind: "committed";
      readonly lsn: number;
      readonly counts: Readonly<Record<string, number>>;
      readonly recordedAtMs: number;
    }
  | { readonly kind: "failed"; readonly code: string; readonly recordedAtMs: number }
  /** A statement inside an open transaction: session-scoped, never durable. */
  | {
      readonly kind: "statement";
      readonly status: "ok" | "failed";
      readonly counts: Readonly<Record<string, number>>;
      readonly code?: string;
      readonly recordedAtMs: number;
    };

export type Admission =
  | { readonly kind: "execute" }
  | { readonly kind: "inFlight" }
  | { readonly kind: "duplicate"; readonly outcome: Outcome }
  | { readonly kind: "expired" | "stale" | "window" | "sessionLost" };

/** The durable form, in the manifest root (committed outcomes only). */
export interface ClientLedgerEntry {
  readonly clientId: string;
  readonly resolvedThrough: number;
  readonly outcomes: ReadonlyArray<{
    readonly sequence: number;
    readonly lsn: number;
    readonly counts: Readonly<Record<string, number>>;
    readonly recordedAtMs: number;
  }>;
}

/** What a request or outcome belongs to: its session generation for `transaction` scope. */
interface Owner {
  readonly sessionId: string;
  readonly generation: number;
  readonly scope: "transaction" | "standalone";
}

interface Held {
  readonly outcome: Outcome;
  /** Null for outcomes restored from a checkpoint or replayed (committed, session-free). */
  readonly owner: Owner | null;
}

interface ClientState {
  resolvedThrough: number;
  lastSeenMs: number;
  readonly outcomes: Map<number, Held>;
  readonly inFlight: Map<number, Owner>;
  /** Per session: generations ≤ `generation` have ended, at `atMs`. */
  readonly ended: Map<string, { generation: number; atMs: number }>;
}

export class RequestLedger {
  readonly #clients = new Map<string, ClientState>();

  #client(id: string, nowMs: number): ClientState {
    let client = this.#clients.get(id);
    if (client === undefined) {
      client = {
        resolvedThrough: 0,
        lastSeenMs: nowMs,
        outcomes: new Map(),
        inFlight: new Map(),
        ended: new Map(),
      };
      this.#clients.set(id, client);
    }
    return client;
  }

  admit(request: RequestIdentity, nowMs: number): Admission {
    if (nowMs - request.sentAtMs > CLIENT_RETRY_WINDOW_MS) return { kind: "expired" };
    const client = this.#client(request.clientId, nowMs);
    client.lastSeenMs = nowMs;
    this.#acknowledge(client, request.resolvedThrough);
    if (request.sequence <= client.resolvedThrough) return { kind: "stale" };
    if (request.sequence > client.resolvedThrough + MAX_OUTSTANDING_REQUESTS)
      return { kind: "window" };
    const held = client.outcomes.get(request.sequence);
    if (held !== undefined) return { kind: "duplicate", outcome: held.outcome };
    if (client.inFlight.has(request.sequence)) return { kind: "inFlight" };
    if (
      request.scope === "transaction" &&
      request.generation <= (client.ended.get(request.sessionId)?.generation ?? 0)
    )
      return { kind: "sessionLost" };
    client.inFlight.set(request.sequence, {
      sessionId: request.sessionId,
      generation: request.generation,
      scope: request.scope,
    });
    return { kind: "execute" };
  }

  /**
   * Ends an admitted request that records nothing (a standalone query: at-least-once, R16.2), so
   * a retry runs it again. Never for a statement inside an open transaction or a commit.
   */
  settle(clientId: string, sequence: number): void {
    this.#clients.get(clientId)?.inFlight.delete(sequence);
  }

  /**
   * Ends generations ≤ `generation` of one session of a client ("Sessions"): drops that session's
   * `transaction`-scope requests and outcomes; its standalone requests (commits in progress) and
   * every other session keep theirs.
   */
  endSession(clientId: string, sessionId: string, generation: number, nowMs: number): void {
    const client = this.#clients.get(clientId);
    if (client === undefined) return;
    if (generation > (client.ended.get(sessionId)?.generation ?? 0))
      client.ended.set(sessionId, { generation, atMs: nowMs });
    const ended = (owner: Owner | null) =>
      owner !== null &&
      owner.scope === "transaction" &&
      owner.sessionId === sessionId &&
      owner.generation <= generation;
    for (const [sequence, owner] of client.inFlight)
      if (ended(owner)) client.inFlight.delete(sequence);
    for (const [sequence, held] of client.outcomes)
      if (ended(held.owner)) client.outcomes.delete(sequence);
  }

  /**
   * Records the terminal outcome of an admitted request (a commit only once it is durable), and
   * returns whether it was kept. A completion that is no longer in flight is kept only if it is
   * a durable commit (replay records those too); otherwise it belongs to an ended generation (or
   * was never admitted) and is ignored ("Sessions").
   */
  record(clientId: string, sequence: number, outcome: Outcome): boolean {
    const client = this.#client(clientId, outcome.recordedAtMs);
    const owner = client.inFlight.get(sequence) ?? null;
    if (owner === null && outcome.kind !== "committed") return false;
    client.inFlight.delete(sequence);
    if (sequence <= client.resolvedThrough) return false;
    client.outcomes.set(sequence, { outcome, owner });
    return true;
  }

  #acknowledge(client: ClientState, resolvedThrough: number): void {
    if (resolvedThrough <= client.resolvedThrough) return;
    client.resolvedThrough = resolvedThrough;
    for (const sequence of client.outcomes.keys())
      if (sequence <= resolvedThrough) client.outcomes.delete(sequence);
  }

  /** Drops outcomes older than the window and clients idle for it. */
  prune(nowMs: number): void {
    for (const [id, client] of this.#clients) {
      for (const [sequence, held] of client.outcomes)
        if (nowMs - held.outcome.recordedAtMs > CLIENT_RETRY_WINDOW_MS)
          client.outcomes.delete(sequence);
      for (const [sessionId, ended] of client.ended)
        if (nowMs - ended.atMs > 2 * CLIENT_RETRY_WINDOW_MS) client.ended.delete(sessionId);
      if (
        client.outcomes.size === 0 &&
        client.inFlight.size === 0 &&
        nowMs - client.lastSeenMs > CLIENT_RETRY_WINDOW_MS
      )
        this.#clients.delete(id);
    }
  }

  /** Outcomes held (for the bound). */
  get size(): number {
    let size = 0;
    for (const client of this.#clients.values()) size += client.outcomes.size;
    return size;
  }

  /** Requests admitted and not yet recorded or settled (an owner leak shows here). */
  get inFlight(): number {
    let count = 0;
    for (const client of this.#clients.values()) count += client.inFlight.size;
    return count;
  }

  /** The durable checkpoint: committed outcomes only. */
  checkpoint(): ClientLedgerEntry[] {
    return [...this.#clients].map(([clientId, client]) => ({
      clientId,
      resolvedThrough: client.resolvedThrough,
      outcomes: [...client.outcomes].flatMap(([sequence, { outcome }]) =>
        outcome.kind === "committed"
          ? [
              {
                sequence,
                lsn: outcome.lsn,
                counts: outcome.counts,
                recordedAtMs: outcome.recordedAtMs,
              },
            ]
          : [],
      ),
    }));
  }

  /** Rebuilds from a root checkpoint; replayed commit frames are then `record`ed in LSN order. */
  static restore(entries: readonly ClientLedgerEntry[], nowMs: number): RequestLedger {
    const ledger = new RequestLedger();
    for (const entry of entries) {
      const client = ledger.#client(entry.clientId, nowMs);
      client.resolvedThrough = entry.resolvedThrough;
      for (const outcome of entry.outcomes)
        client.outcomes.set(outcome.sequence, {
          outcome: {
            kind: "committed",
            lsn: outcome.lsn,
            counts: outcome.counts,
            recordedAtMs: outcome.recordedAtMs,
          },
          owner: null,
        });
    }
    return ledger;
  }
}
