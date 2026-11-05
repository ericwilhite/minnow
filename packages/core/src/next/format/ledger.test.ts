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
