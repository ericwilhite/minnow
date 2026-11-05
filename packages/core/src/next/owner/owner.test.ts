import { describe, expect, it } from "vitest";
import { CLIENT_RETRY_WINDOW_MS, RequestLedger, type RequestIdentity } from "../format/ledger.js";
import {
  canHost,
  clientLockName,
  compareProtocol,
  hostLockName,
  lifecycleAction,
  ownerLockName,
  type LifecycleEvent,
} from "./election.js";
import {
  CommittedWithoutResultError,
  TransactionIdleTimeoutError,
  OutcomeUnknownError,
  OwnerUnresponsiveError,
  SessionLostError,
  fromWireError,
  ownerErrorRegistry,
  toWireError,
} from "./errors.js";
import {
  DEFAULT_RESULT_WINDOW_BYTES,
  PROTOCOL_VERSION,
  ProtocolError,
  parseClientMessage,
  type StatementRequest,
} from "./protocol.js";
import { RequestSequencer, isStandalone, mayControlTransaction } from "./sequence.js";

/** Autocommit requests on the connection's default session, first generation. */
const SESSION = { sessionId: "0", generation: 1, scope: "standalone" } as const;

describe("RequestSequencer", () => {
  it("tracks resolvedThrough across out-of-order replies", () => {
    const sequencer = new RequestSequencer(8);
    const seqs = [1, 2, 3, 4].map(() => sequencer.issue());
    expect(seqs).toEqual([1, 2, 3, 4]);
    sequencer.resolve(3);
    sequencer.resolve(2);
    expect(sequencer.resolvedThrough).toBe(0);
    sequencer.resolve(1);
    expect(sequencer.resolvedThrough).toBe(3);
    expect(sequencer.outstanding).toBe(1);
    sequencer.resolve(3);
    expect(sequencer.resolvedThrough).toBe(3);
    expect(() => {
      sequencer.resolve(5);
    }).toThrow(RangeError);
  });

  it("stops issuing at the window and resumes when the oldest resolves", () => {
    const sequencer = new RequestSequencer(3);
    expect([sequencer.issue(), sequencer.issue(), sequencer.issue()]).toEqual([1, 2, 3]);
    expect(sequencer.issue()).toBeNull();
    sequencer.resolve(3);
    expect(sequencer.issue()).toBeNull();
    sequencer.resolve(1);
    expect(sequencer.issue()).toBe(4);
  });

  it("never trips the ledger's window, duplicate or stale rules when it follows the protocol", () => {
    // A seeded random schedule: issue, deliver (possibly twice, out of order), resolve.
    const ledger = new RequestLedger();
    const sequencer = new RequestSequencer();
    let state = 0x2545f491;
    const random = () => {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      return (state >>> 0) / 2 ** 32;
    };
    const inFlight: number[] = [];
    const now = 1_000_000;
    let executed = 0;
    for (let step = 0; step < 5_000; step++) {
      if (random() < 0.55) {
        const seq = sequencer.issue();
        if (seq !== null) inFlight.push(seq);
        continue;
      }
      if (inFlight.length === 0) continue;
      const index = Math.floor(random() * inFlight.length);
      const seq = inFlight[index] ?? 0;
      const identity: RequestIdentity = {
        ...SESSION,
        clientId: "c",
        sequence: seq,
        sentAtMs: now,
        resolvedThrough: sequencer.resolvedThrough,
      };
      const admission = ledger.admit(identity, now);
      expect(admission.kind).not.toBe("window");
      expect(admission.kind).not.toBe("stale");
      expect(admission.kind).toBe("execute");
      executed++;
      ledger.record("c", seq, { kind: "committed", lsn: seq, counts: { t: 1 }, recordedAtMs: now });
      // The reply was lost once in a while: the retry is answered from the ledger.
      if (random() < 0.2) {
        const retry = ledger.admit(
          { ...identity, resolvedThrough: sequencer.resolvedThrough },
          now,
        );
        expect(retry.kind).toBe("duplicate");
      }
      sequencer.resolve(seq);
      inFlight.splice(index, 1);
    }
    expect(executed).toBeGreaterThan(1_000);
    expect(ledger.size).toBeLessThanOrEqual(256);
  });

  it("re-runs a settled no-effect request on retry, and answers a recorded failure", () => {
    const ledger = new RequestLedger();
    const read: RequestIdentity = {
      ...SESSION,
      clientId: "c",
      sequence: 1,
      sentAtMs: 0,
      resolvedThrough: 0,
    };
    expect(ledger.admit(read, 1).kind).toBe("execute");
    ledger.settle("c", 1);
    expect(ledger.admit(read, 2).kind).toBe("execute");
    ledger.record("c", 1, { kind: "failed", code: "23505", recordedAtMs: 3 });
    const retry = ledger.admit(read, 4);
    expect(retry.kind === "duplicate" ? retry.outcome.kind : retry.kind).toBe("failed");
  });

  it("is refused as expired past the retry window, so the client resolves it locally", () => {
    const ledger = new RequestLedger();
    const admission = ledger.admit(
      { ...SESSION, clientId: "c", sequence: 1, sentAtMs: 0, resolvedThrough: 0 },
      CLIENT_RETRY_WINDOW_MS + 1,
    );
    expect(admission.kind).toBe("expired");
  });
});

describe("standalone requests", () => {
  const idle = { transaction: "idle", unresolvedControl: 0 } as const;

  it("keeps concurrent autocommit statements standalone", () => {
    expect(isStandalone({ t: "statement", session: 0 }, idle)).toBe(true);
    expect(isStandalone({ t: "write", session: 0 }, idle)).toBe(true);
    expect(isStandalone({ t: "prepare" }, idle)).toBe(true);
  });

  it("is never standalone inside, or possibly inside, a transaction", () => {
    expect(isStandalone({ t: "statement", session: 1 }, idle)).toBe(false);
    expect(
      isStandalone({ t: "statement", session: 0 }, { transaction: "open", unresolvedControl: 0 }),
    ).toBe(false);
    expect(
      isStandalone({ t: "statement", session: 0 }, { transaction: "idle", unresolvedControl: 1 }),
    ).toBe(false);
    for (const t of ["begin", "commit", "rollback"] as const)
      expect(isStandalone({ t, session: 2 }, idle)).toBe(false);
  });

  it("classifies transaction control conservatively", () => {
    for (const text of [
      "BEGIN",
      "  begin;",
      "-- note\nCOMMIT",
      "/* a /* nested */ c */ rollback",
      "START TRANSACTION",
      "end",
      "SAVEPOINT a",
      "RELEASE SAVEPOINT a",
      "SELECT 1; BEGIN",
      "select ';'",
    ])
      expect(mayControlTransaction(text), text).toBe(true);
    for (const text of [
      "SELECT 1",
      "select 1;  ;\n",
      "INSERT INTO t VALUES (1)",
      "-- begin\nSELECT 2",
      "WITH x AS (SELECT 1) SELECT * FROM x",
      "beginning",
    ])
      expect(mayControlTransaction(text), text).toBe(false);
  });
});

describe("parseClientMessage", () => {
  const statement: StatementRequest = {
    t: "statement",
    seq: 3,
    sentAtMs: 1,
    resolvedThrough: 2,
    live: false,
    standalone: true,
    retry: false,
    session: 0,
    text: "SELECT 1",
    prepared: null,
    knownColumnsVersion: -1,
    params: [],
    result: { windowBytes: DEFAULT_RESULT_WINDOW_BYTES, frameRows: null, wantRows: true },
  };

  it("accepts a well-formed request and signal", () => {
    expect(parseClientMessage(structuredClone(statement))).toEqual(statement);
    expect(parseClientMessage({ t: "cancel", target: 3, close: false }).t).toBe("cancel");
    expect(parseClientMessage({ t: "release", target: 3, through: 0 }).t).toBe("release");
    expect(parseClientMessage({ t: "liveness" }).t).toBe("liveness");
    expect(parseClientMessage({ t: "hello", protocol: PROTOCOL_VERSION }).t).toBe("hello");
  });

  it("rejects unknown kinds and broken headers", () => {
    expect(() => parseClientMessage(null)).toThrow(ProtocolError);
    expect(() => parseClientMessage({ t: "rpc-call" })).toThrow(ProtocolError);
    expect(() => parseClientMessage({ ...statement, seq: 0 })).toThrow(ProtocolError);
    expect(() => parseClientMessage({ ...statement, resolvedThrough: 3 })).toThrow(ProtocolError);
    expect(() => parseClientMessage({ ...statement, retry: undefined })).toThrow(ProtocolError);
  });
});

describe("errors on the wire", () => {
  it("keep their class, code and fields through a structured clone", () => {
    for (const error of [
      new OwnerUnresponsiveError(10_000),
      new OutcomeUnknownError("unresponsive"),
      new TransactionIdleTimeoutError(30_000),
      new SessionLostError("transaction"),
      new CommittedWithoutResultError({ orders: 2 }),
    ]) {
      const back = fromWireError(structuredClone(toWireError(error)), ownerErrorRegistry);
      expect(back).toBeInstanceOf(error.constructor);
      expect(back.name).toBe(error.name);
      expect(back.message).toBe(error.message);
      const fields = (value: Error) =>
        Object.fromEntries(Object.entries(value).filter(([key]) => key !== "name"));
      expect(fields(back)).toEqual(fields(error));
    }
  });
});

describe("election and lifecycle", () => {
  it("names distinct locks per database and client", () => {
    const names = [
      ownerLockName("a"),
      hostLockName("a"),
      clientLockName("a", "00ff"),
      ownerLockName("b"),
    ];
    expect(new Set(names).size).toBe(names.length);
  });

  it("terminates on freeze and pagehide, flushes on hidden, and steps down on hidden only on mobile", () => {
    const table = (mobile: boolean) =>
      Object.fromEntries(
        (["freeze", "resume", "pagehide", "pageshow", "hidden", "visible"] as LifecycleEvent[]).map(
          (event) => [event, lifecycleAction(event, { mobile })],
        ),
      );
    expect(table(false)).toEqual({
      freeze: "terminate",
      resume: "none",
      pagehide: "terminate",
      pageshow: "none",
      hidden: "flush",
      visible: "none",
    });
    expect(table(true)).toMatchObject({
      hidden: "stepDown",
      freeze: "terminate",
      pagehide: "terminate",
    });
  });

  it("lets pages and dedicated workers host, unless superseded or hidden on mobile", () => {
    const page = {
      kind: "page",
      visible: true,
      platform: { mobile: true },
      superseded: false,
    } as const;
    expect(canHost(page)).toBe(true);
    expect(canHost({ ...page, visible: false })).toBe(false);
    expect(canHost({ ...page, visible: false, platform: { mobile: false } })).toBe(true);
    expect(canHost({ ...page, kind: "dedicatedWorker" })).toBe(true);
    expect(canHost({ ...page, kind: "sharedWorker" })).toBe(false);
    expect(canHost({ ...page, kind: "serviceWorker" })).toBe(false);
    expect(canHost({ ...page, superseded: true })).toBe(false);
  });

  it("orders protocol versions for supersession", () => {
    expect(compareProtocol(2, 2)).toBe("same");
    expect(compareProtocol(3, 2)).toBe("clientNewer");
    expect(compareProtocol(1, 2)).toBe("clientOlder");
  });
});
