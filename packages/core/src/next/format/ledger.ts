/**
 * Exactly-once requests: the owner's per-client ledger of terminal outcomes (review finding 5).
 *
 * ## Identity
 * - A client has a `clientId` of 16 random bytes for its whole page lifetime. It keeps it across
 *   reconnects and owner changes; a reload is a new client.
 * - Each request carries `(clientId, sequence, sentAtMs, resolvedThrough)`. Sequences increase as
 *   the client issues them, may arrive out of order (up to `MAX_OUTSTANDING_REQUESTS` in flight,
 *   the worker protocol's limit), and may have gaps. `resolvedThrough` is the highest S such that
 *   the client holds the outcome of every request ≤ S.
 *
 * ## Admission (`RequestLedger.admit`), checked by the receiver before executing anything
 * - `expired`: `now − sentAtMs > CLIENT_RETRY_WINDOW_MS`. Never executed (`RequestExpiredError`).
 * - `stale`: sequence ≤ the client's `resolvedThrough`: the client already has the outcome; a
 *   late duplicate is dropped, never executed.
 * - `window`: sequence > resolvedThrough + MAX_OUTSTANDING_REQUESTS: a protocol violation.
 * - `inFlight`: the same request is executing: the duplicate waits for that execution's outcome.
 * - `duplicate`: the outcome is recorded: a committed write answers "already committed" with its
 *   original affected-row counts (RETURNING rows are not replayed: the duplicate gets counts
 *   only); a failure answers with the same error code.
 * - `execute`: first sight; run it.
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

export interface RequestIdentity {
  /** Hex of the 16-byte client id. */
  readonly clientId: string;
  readonly sequence: number;
  readonly sentAtMs: number;
  readonly resolvedThrough: number;
}

export type Outcome =
  | {
      readonly kind: "committed";
      readonly lsn: number;
      readonly counts: Readonly<Record<string, number>>;
      readonly recordedAtMs: number;
    }
  | { readonly kind: "failed"; readonly code: string; readonly recordedAtMs: number };

export type Admission =
  | { readonly kind: "execute" }
  | { readonly kind: "inFlight" }
  | { readonly kind: "duplicate"; readonly outcome: Outcome }
  | { readonly kind: "expired" | "stale" | "window" };

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

interface ClientState {
  resolvedThrough: number;
  lastSeenMs: number;
  readonly outcomes: Map<number, Outcome>;
  readonly inFlight: Set<number>;
}

export class RequestLedger {
  readonly #clients = new Map<string, ClientState>();

  #client(id: string, nowMs: number): ClientState {
    let client = this.#clients.get(id);
    if (client === undefined) {
      client = { resolvedThrough: 0, lastSeenMs: nowMs, outcomes: new Map(), inFlight: new Set() };
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
    const outcome = client.outcomes.get(request.sequence);
    if (outcome !== undefined) return { kind: "duplicate", outcome };
    if (client.inFlight.has(request.sequence)) return { kind: "inFlight" };
    client.inFlight.add(request.sequence);
    return { kind: "execute" };
  }

  /** Records the terminal outcome of an admitted request (a commit only once it is durable). */
  record(clientId: string, sequence: number, outcome: Outcome): void {
    const client = this.#client(clientId, outcome.recordedAtMs);
    client.inFlight.delete(sequence);
    if (sequence > client.resolvedThrough) client.outcomes.set(sequence, outcome);
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
      for (const [sequence, outcome] of client.outcomes)
        if (nowMs - outcome.recordedAtMs > CLIENT_RETRY_WINDOW_MS) client.outcomes.delete(sequence);
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

  /** The durable checkpoint: committed outcomes only. */
  checkpoint(): ClientLedgerEntry[] {
    return [...this.#clients].map(([clientId, client]) => ({
      clientId,
      resolvedThrough: client.resolvedThrough,
      outcomes: [...client.outcomes]
        .filter(
          (entry): entry is [number, Extract<Outcome, { kind: "committed" }>] =>
            entry[1].kind === "committed",
        )
        .map(([sequence, outcome]) => ({
          sequence,
          lsn: outcome.lsn,
          counts: outcome.counts,
          recordedAtMs: outcome.recordedAtMs,
        })),
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
          kind: "committed",
          lsn: outcome.lsn,
          counts: outcome.counts,
          recordedAtMs: outcome.recordedAtMs,
        });
    }
    return ledger;
  }
}
