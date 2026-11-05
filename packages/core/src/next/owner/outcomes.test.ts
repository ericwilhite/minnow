import { afterEach, describe, expect, it, vi } from "vitest";
import { RequestLedger, type Outcome, type RequestIdentity } from "../format/ledger.js";
import { IdleTransactionTimer } from "./engine.js";
import { ledgerAction, type AnsweredRequest, type OwnerLedger } from "./outcomes.js";

const base: AnsweredRequest = {
  kind: "statement",
  plan: "query",
  control: null,
  before: "idle",
  opened: false,
  reachedCommit: false,
  commits: false,
  failed: false,
};

describe("ledgerAction", () => {
  it("makes only commits exactly-once; standalone queries, nextval included, are at-least-once", () => {
    expect(ledgerAction({ ...base, plan: "modify", commits: true, reachedCommit: true })).toBe(
      "writePath",
    );
    expect(
      ledgerAction({ ...base, kind: "commit", before: "open", commits: true, reachedCommit: true }),
    ).toBe("writePath");
    // SELECT nextval(…) is a query: a retry runs it again (a gap, as PostgreSQL allows).
    expect(ledgerAction(base)).toBe("settle");
    expect(ledgerAction({ ...base, failed: true })).toBe("settle");
    for (const kind of ["prepare", "subscribe", "admin"] as const)
      expect(ledgerAction({ ...base, kind, plan: null })).toBe("settle");
  });

  it("records failures of would-be commits, but not past the commit boundary", () => {
    expect(ledgerAction({ ...base, plan: "modify", commits: true, failed: true })).toBe("failed");
    expect(
      ledgerAction({ ...base, plan: "modify", commits: true, failed: true, reachedCommit: true }),
    ).toBe("settle");
  });

  it("records a session-scoped outcome for statements that change or abort a transaction", () => {
    const inside = { ...base, before: "open" } as const;
    expect(ledgerAction({ ...inside, plan: "modify" })).toBe("statement");
    expect(ledgerAction({ ...inside, plan: "ddl" })).toBe("statement");
    expect(ledgerAction({ ...inside, kind: "write", plan: null })).toBe("statement");
    expect(ledgerAction({ ...inside, plan: "transactionControl", control: "other" })).toBe(
      "statement",
    );
    expect(ledgerAction({ ...inside, failed: true })).toBe("statement");
    expect(ledgerAction({ ...base, kind: "begin", plan: null, opened: true })).toBe("statement");
    expect(
      ledgerAction({ ...base, plan: "transactionControl", control: "begin", opened: true }),
    ).toBe("statement");
    expect(ledgerAction({ ...base, before: "timedOut", failed: true })).toBe("statement");
  });

  it("settles queries inside a transaction, ROLLBACK, and a read-only COMMIT", () => {
    expect(ledgerAction({ ...base, before: "open" })).toBe("settle");
    expect(ledgerAction({ ...base, kind: "rollback", plan: null, before: "open" })).toBe("settle");
    expect(
      ledgerAction({ ...base, plan: "transactionControl", control: "rollback", before: "failed" }),
    ).toBe("settle");
    expect(ledgerAction({ ...base, kind: "commit", plan: null, before: "open" })).toBe("settle");
  });
});

describe("the reviewer's double-execute probe (R12)", () => {
  /**
   * A live session runs `BEGIN; UPDATE counter SET n = n + 1` and the UPDATE arrives twice on the
   * same session (a duplicate before the client acknowledged it). The owner's loop: admit, run on
   * `execute`, then apply `ledgerAction`; a duplicate is answered from the ledger.
   */
  const inTransaction: RequestIdentity = {
    clientId: "c1",
    sessionId: "1",
    generation: 1,
    scope: "transaction",
    sequence: 0,
    sentAtMs: 0,
    resolvedThrough: 0,
  };

  function serve(ledger: OwnerLedger, counter: { n: number }, sequence: number, nowMs: number) {
    const identity: RequestIdentity = { ...inTransaction, sequence };
    const admission = ledger.admit(identity, nowMs);
    if (admission.kind === "duplicate") return admission.outcome;
    expect(admission.kind).toBe("execute");
    counter.n++;
    const answered: AnsweredRequest = { ...base, plan: "modify", before: "open" };
    const action = ledgerAction(answered);
    const outcome: Outcome = {
      kind: "statement",
      status: "ok",
      counts: { counter: 1 },
      recordedAtMs: nowMs,
    };
    if (action === "statement") ledger.record("c1", sequence, outcome);
    else if (action === "settle") ledger.settle("c1", sequence);
    return outcome;
  }

  it("runs the UPDATE once and answers the duplicate with its outcome", () => {
    const ledger = new RequestLedger();
    const counter = { n: 0 };
    const first = serve(ledger, counter, 2, 10);
    const duplicate = serve(ledger, counter, 2, 20);
    expect(counter.n).toBe(1);
    expect(duplicate).toEqual(first);
  });

  it("would run it twice if the statement were settled (the defect R12 fixes)", () => {
    const ledger = new RequestLedger();
    const identity: RequestIdentity = { ...inTransaction, sequence: 2 };
    expect(ledger.admit(identity, 10).kind).toBe("execute");
    ledger.settle("c1", 2);
    expect(ledger.admit(identity, 20).kind).toBe("execute");
  });

  it("endSession clears only that session's statement outcomes; other sessions' stay", () => {
    const ledger: OwnerLedger = new RequestLedger();
    const statement: Outcome = {
      kind: "statement",
      status: "ok",
      counts: { t: 1 },
      recordedAtMs: 5,
    };
    const s1 = { ...inTransaction, sessionId: "1" };
    const s2 = { ...inTransaction, sessionId: "2" };
    const other = { ...s1, clientId: "c2" };
    // Session 1 and session 2 of client c1 each hold a statement outcome; so does client c2's
    // session 1. Session 1 also has a statement and a COMMIT (standalone scope) still running.
    for (const identity of [
      { ...s1, sequence: 1 },
      { ...s2, sequence: 2 },
      { ...other, sequence: 1 },
    ]) {
      expect(ledger.admit(identity, 5).kind).toBe("execute");
      expect(ledger.record(identity.clientId, identity.sequence, statement)).toBe(true);
    }
    expect(ledger.admit({ ...s1, sequence: 3 }, 5).kind).toBe("execute");
    expect(ledger.admit({ ...s1, scope: "standalone", sequence: 4 }, 5).kind).toBe("execute");

    ledger.endSession("c1", "1", 1, 6);

    // Session 1's outcome is gone and its retry is refused, never run again.
    expect(ledger.admit({ ...s1, sequence: 1 }, 7).kind).toBe("sessionLost");
    // Its in-flight statement's late completion is ignored, and its retry is refused too.
    expect(ledger.record("c1", 3, statement)).toBe(false);
    expect(ledger.admit({ ...s1, sequence: 3 }, 7).kind).toBe("sessionLost");
    // The COMMIT in progress completes and records; its retry gets the commit.
    const committed: Outcome = { kind: "committed", lsn: 9, counts: { t: 1 }, recordedAtMs: 7 };
    expect(ledger.record("c1", 4, committed)).toBe(true);
    expect(ledger.admit({ ...s1, scope: "standalone", sequence: 4 }, 8)).toEqual({
      kind: "duplicate",
      outcome: committed,
    });
    // Session 2 of the same client, and the other client's session 1, keep theirs.
    expect(ledger.admit({ ...s2, sequence: 2 }, 8)).toEqual({
      kind: "duplicate",
      outcome: statement,
    });
    expect(ledger.admit({ ...other, sequence: 1 }, 8)).toEqual({
      kind: "duplicate",
      outcome: statement,
    });
    // The next generation of session 1 runs normally.
    expect(ledger.admit({ ...s1, generation: 2, sequence: 5 }, 8).kind).toBe("execute");
  });
});

describe("IdleTransactionTimer (R16.7)", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("fires only while a transaction is open and no request runs, and resets on activity", () => {
    vi.useFakeTimers();
    const expired = vi.fn();
    const timer = new IdleTransactionTimer(1_000, expired);
    vi.advanceTimersByTime(5_000);
    expect(expired).not.toHaveBeenCalled();

    timer.setOpen(true);
    vi.advanceTimersByTime(900);
    timer.start();
    vi.advanceTimersByTime(5_000);
    expect(expired).not.toHaveBeenCalled();
    timer.end();
    vi.advanceTimersByTime(900);
    timer.touch();
    vi.advanceTimersByTime(900);
    expect(expired).not.toHaveBeenCalled();
    vi.advanceTimersByTime(100);
    expect(expired).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(5_000);
    expect(expired).toHaveBeenCalledTimes(1);
    timer.setOpen(true);
    timer.dispose();
    vi.advanceTimersByTime(5_000);
    expect(expired).toHaveBeenCalledTimes(1);
  });
});
