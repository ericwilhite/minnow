import { afterEach, describe, expect, it, vi } from "vitest";
import { RequestLedger, type Outcome } from "../format/ledger.js";
import { IdleTransactionTimer } from "./engine.js";
import { ledgerAction, type AnsweredRequest } from "./outcomes.js";

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
  function serve(ledger: RequestLedger, counter: { n: number }, sequence: number, nowMs: number) {
    const identity = { clientId: "c1", sequence, sentAtMs: 0, resolvedThrough: 0 };
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
    const identity = { clientId: "c1", sequence: 2, sentAtMs: 0, resolvedThrough: 0 };
    expect(ledger.admit(identity, 10).kind).toBe("execute");
    ledger.settle("c1", 2);
    expect(ledger.admit(identity, 20).kind).toBe("execute");
  });

  // Needs contracts round 5 (R16.1): `record(…, session)` and `endSession(clientId, session)`.
  it.todo("endSession clears only that session's statement outcomes; other sessions' stay");
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
