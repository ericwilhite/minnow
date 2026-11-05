import { describe, expect, it } from "vitest";
import {
  CLIENT_RETRY_WINDOW_MS,
  MAX_OUTSTANDING_REQUESTS,
  RequestLedger,
  type RequestIdentity,
} from "./ledger.js";

const T0 = 1_000_000;
const request = (sequence: number, extra: Partial<RequestIdentity> = {}): RequestIdentity => ({
  clientId: "c1",
  sessionId: "s1",
  generation: 1,
  scope: "standalone",
  sequence,
  sentAtMs: T0,
  resolvedThrough: 0,
  ...extra,
});
const committed = (lsn: number, rowCount: number, at = T0) =>
  ({ kind: "committed", lsn, counts: { rowCount }, recordedAtMs: at }) as const;

describe("exactly-once request ledger", () => {
  it("answers a committed duplicate with its original counts and never re-executes it", () => {
    const ledger = new RequestLedger();
    expect(ledger.admit(request(1), T0).kind).toBe("execute");
    ledger.record("c1", 1, committed(10, 3));
    expect(ledger.admit(request(1), T0 + 5)).toEqual({
      kind: "duplicate",
      outcome: committed(10, 3),
    });
  });

  it("does not report a failed request as committed when a later one commits (review probe)", () => {
    const ledger = new RequestLedger();
    ledger.admit(request(1), T0);
    ledger.admit(request(2), T0);
    ledger.record("c1", 1, { kind: "failed", code: "UNIQUE", recordedAtMs: T0 });
    ledger.record("c1", 2, committed(11, 1));
    expect(ledger.admit(request(1), T0)).toMatchObject({
      kind: "duplicate",
      outcome: { kind: "failed" },
    });
    expect(ledger.admit(request(2), T0)).toMatchObject({
      kind: "duplicate",
      outcome: { kind: "committed" },
    });
  });

  it("handles concurrent, out-of-order and in-flight requests within the window", () => {
    const ledger = new RequestLedger();
    for (const sequence of [5, 2, 256, 1])
      expect(ledger.admit(request(sequence), T0).kind).toBe("execute");
    expect(ledger.admit(request(2), T0).kind).toBe("inFlight");
    expect(ledger.admit(request(MAX_OUTSTANDING_REQUESTS + 1), T0).kind).toBe("window");
    ledger.record("c1", 2, committed(12, 1));
    expect(ledger.admit(request(2), T0).kind).toBe("duplicate");
  });

  it("drops acknowledged outcomes and refuses their late duplicates", () => {
    const ledger = new RequestLedger();
    ledger.admit(request(1), T0);
    ledger.record("c1", 1, committed(10, 1));
    expect(ledger.admit(request(2, { resolvedThrough: 1 }), T0).kind).toBe("execute");
    expect(ledger.size).toBe(0);
    expect(ledger.admit(request(1, { resolvedThrough: 1 }), T0).kind).toBe("stale");
    expect(ledger.admit(request(300, { resolvedThrough: 1 }), T0).kind).toBe("window");
  });

  it("enforces expiry at the receiver and keeps every outcome an accepted retry needs", () => {
    const ledger = new RequestLedger();
    ledger.admit(request(1), T0);
    ledger.record("c1", 1, committed(10, 1, T0 + 50));
    // The last accepted retry, sent at T0, arrives as late as the window allows.
    const lastAccepted = T0 + CLIENT_RETRY_WINDOW_MS;
    ledger.prune(lastAccepted);
    expect(ledger.admit(request(1), lastAccepted).kind).toBe("duplicate");
    expect(ledger.admit(request(1), lastAccepted + 1).kind).toBe("expired");
    ledger.prune(T0 + 50 + CLIENT_RETRY_WINDOW_MS + 1);
    expect(ledger.size).toBe(0);
    ledger.prune(T0 + 3 * CLIENT_RETRY_WINDOW_MS);
    expect(ledger.checkpoint()).toEqual([]);
  });

  it("checkpoints committed outcomes only and restores them", () => {
    const ledger = new RequestLedger();
    ledger.admit(request(1), T0);
    ledger.admit(request(2), T0);
    ledger.record("c1", 1, committed(10, 4));
    ledger.record("c1", 2, { kind: "failed", code: "X", recordedAtMs: T0 });
    const restored = RequestLedger.restore(ledger.checkpoint(), T0);
    expect(restored.admit(request(1), T0)).toEqual({
      kind: "duplicate",
      outcome: committed(10, 4),
    });
    // A failure is not durable: after a restart the retry runs (the failed attempt changed nothing).
    expect(restored.admit(request(2), T0).kind).toBe("execute");
  });
});

describe("settling requests with no durable effect (Phase B ruling)", () => {
  it("frees a read without recording an outcome, so its retry runs again", () => {
    const ledger = new RequestLedger();
    expect(ledger.admit(request(1), T0).kind).toBe("execute");
    expect(ledger.admit(request(1), T0).kind).toBe("inFlight");
    ledger.settle("c1", 1);
    expect(ledger.size).toBe(0);
    // A retried read is executed again, never answered with a stored outcome.
    expect(ledger.admit(request(1), T0).kind).toBe("execute");
  });

  it("does not let a settled request linger in flight or block pruning", () => {
    const ledger = new RequestLedger();
    ledger.admit(request(1), T0); // a statement inside an open transaction
    ledger.admit(request(2), T0); // its commit
    ledger.settle("c1", 1);
    ledger.record("c1", 2, committed(10, 1));
    expect(ledger.admit(request(2), T0).kind).toBe("duplicate");
    ledger.admit(request(3, { resolvedThrough: 2 }), T0); // a rollback
    ledger.settle("c1", 3);
    ledger.prune(T0 + 3 * CLIENT_RETRY_WINDOW_MS);
    expect(ledger.checkpoint()).toEqual([]);
  });
});

describe("statements inside an open transaction (review A1-4 finding 3)", () => {
  const statement = (rowCount: number) =>
    ({ kind: "statement", status: "ok", counts: { rowCount }, recordedAtMs: T0 }) as const;
  const inTransaction = (sequence: number, extra: Partial<RequestIdentity> = {}) =>
    request(sequence, { scope: "transaction", ...extra });

  it("answers a retried in-transaction statement from its session outcome, never twice (review probe)", () => {
    const ledger = new RequestLedger();
    let executed = 0;
    for (let attempt = 0; attempt < 2; attempt++) {
      const admission = ledger.admit(inTransaction(1), T0 + attempt);
      if (admission.kind === "execute") {
        executed += 1;
        expect(ledger.record("c1", 1, statement(4))).toBe(true);
      } else expect(admission).toEqual({ kind: "duplicate", outcome: statement(4) });
    }
    expect(executed).toBe(1);
  });

  it("keeps session outcomes out of the durable checkpoint and drops them when the session ends", () => {
    const ledger = new RequestLedger();
    ledger.admit(inTransaction(1), T0);
    ledger.record("c1", 1, statement(2));
    ledger.admit(request(2), T0);
    ledger.record("c1", 2, committed(10, 2));
    expect(ledger.checkpoint()[0]?.outcomes.map((o) => o.sequence)).toEqual([2]);
    ledger.endSession("c1", "s1", 1, T0);
    expect(ledger.admit(request(2), T0).kind).toBe("duplicate");
    // Its transaction rolled back with the generation: the retry is refused, never run.
    expect(ledger.admit(inTransaction(1), T0).kind).toBe("sessionLost");
  });
});

describe("session generations (review A1-5 finding 2, rulings R16.1 and R16.2)", () => {
  const statement = {
    kind: "statement",
    status: "ok",
    counts: { rows: 1 },
    recordedAtMs: T0 + 1,
  } as const;
  const inTransaction = (sequence: number, extra: Partial<RequestIdentity> = {}) =>
    request(sequence, { scope: "transaction", ...extra });

  it("drops an ended generation's pending statements, so pruning forgets the client (review probe)", () => {
    const ledger = new RequestLedger();
    expect(ledger.admit(inTransaction(1), T0).kind).toBe("execute");
    ledger.endSession("c1", "s1", 1, T0);
    expect(ledger.inFlight).toBe(0);
    ledger.prune(T0 + 2 * CLIENT_RETRY_WINDOW_MS);
    expect(ledger.checkpoint()).toEqual([]);
    // The probe's reconnect: the old statement, retried, is refused while the window allows it.
    const ledger2 = new RequestLedger();
    ledger2.admit(inTransaction(1), T0);
    ledger2.endSession("c1", "s1", 1, T0 + 5);
    expect(ledger2.admit(inTransaction(1), T0 + 10).kind).toBe("sessionLost");
    // The reconnected generation runs its own requests.
    expect(ledger2.admit(inTransaction(2, { generation: 2 }), T0 + 10).kind).toBe("execute");
  });

  it("ignores a completion that arrives after its generation ended (review probe)", () => {
    const ledger = new RequestLedger();
    ledger.admit(inTransaction(1), T0);
    ledger.endSession("c1", "s1", 1, T0);
    expect(ledger.record("c1", 1, statement)).toBe(false);
    expect(ledger.record("c1", 1, { kind: "failed", code: "X", recordedAtMs: T0 })).toBe(false);
    expect(ledger.size).toBe(0);
    // Not "duplicate": nothing was recorded for the dead transaction.
    expect(ledger.admit(inTransaction(1), T0 + 2).kind).toBe("sessionLost");
  });

  it("lets a commit pending at the end complete and record its durable outcome", () => {
    const ledger = new RequestLedger();
    ledger.admit(request(1), T0); // COMMIT (standalone), in progress
    ledger.endSession("c1", "s1", 1, T0);
    expect(ledger.inFlight).toBe(1);
    expect(ledger.record("c1", 1, committed(10, 3))).toBe(true);
    expect(ledger.admit(request(1), T0)).toEqual({ kind: "duplicate", outcome: committed(10, 3) });
    expect(ledger.checkpoint()[0]?.outcomes.map((o) => o.sequence)).toEqual([1]);
  });

  it("ends one session of a client without touching another's retry protection (R16.1)", () => {
    const ledger = new RequestLedger();
    const s2 = (sequence: number) => inTransaction(sequence, { sessionId: "s2" });
    ledger.admit(inTransaction(1), T0);
    ledger.record("c1", 1, statement);
    ledger.admit(s2(2), T0);
    ledger.record("c1", 2, statement);
    ledger.admit(s2(3), T0); // still executing in s2
    ledger.admit(inTransaction(4), T0); // still executing in s1
    ledger.endSession("c1", "s1", 1, T0);
    expect(ledger.admit(inTransaction(1), T0).kind).toBe("sessionLost");
    expect(ledger.admit(inTransaction(4), T0).kind).toBe("sessionLost");
    expect(ledger.admit(s2(2), T0)).toEqual({ kind: "duplicate", outcome: statement });
    expect(ledger.admit(s2(3), T0).kind).toBe("inFlight");
    expect(ledger.record("c1", 3, statement)).toBe(true);
    expect(ledger.admit(s2(3), T0).kind).toBe("duplicate");
    // Ending an older generation of s2 than the one in use changes nothing for it.
    ledger.endSession("c1", "s2", 0, T0);
    expect(ledger.admit(s2(2), T0).kind).toBe("duplicate");
  });

  it("runs standalone queries at least once: a settled nextval is re-executed (R16.2)", () => {
    const ledger = new RequestLedger();
    expect(ledger.admit(request(1), T0).kind).toBe("execute"); // SELECT nextval('s')
    ledger.settle("c1", 1);
    expect(ledger.admit(request(1), T0 + 1).kind).toBe("execute");
    ledger.settle("c1", 1);
    expect(ledger.size).toBe(0);
  });

  it("expires an ended generation's retries before the ledger forgets the generation", () => {
    const ledger = new RequestLedger();
    ledger.admit(inTransaction(1), T0);
    ledger.endSession("c1", "s1", 1, T0);
    ledger.admit(request(2), T0 + 2 * CLIENT_RETRY_WINDOW_MS); // the client stays active
    ledger.settle("c1", 2);
    ledger.prune(T0 + 2 * CLIENT_RETRY_WINDOW_MS + 1);
    expect(ledger.admit(inTransaction(1), T0 + 2 * CLIENT_RETRY_WINDOW_MS + 1).kind).toBe(
      "expired",
    );
  });
});
