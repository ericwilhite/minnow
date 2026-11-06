/**
 * The third review of increment 2 (B5-p2b-review-3): its probes, kept as tests.
 *
 * 1. A pending request's fate comes from the owner's ledger, never from the client's guess: what
 *    the server's refusal says its absence proves (`effect`), and "none" locally only for a
 *    request that was never sent. (The durable-hit and miss paths through real owner deaths, and
 *    the generated property, are in owner-election.test.ts.)
 * 2. Every early rejection reaches a cursor's consumer.
 * 3. A lost session's barrier clears only when the owner confirms a ROLLBACK, never on send.
 */
import { describe, expect, it } from "vitest";
import { MAX_OUTSTANDING_REQUESTS } from "../format/ledger.js";
import { ProtocolClient } from "../owner/client.js";
import { PROTOCOL_VERSION, type OwnerMessage } from "../owner/protocol.js";
import { classifyStatementText } from "../owner/sequence.js";
import { harness, raw, seed } from "./owner-harness.js";

const delay = (ms = 40) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

type Sent = Record<string, unknown>;

function fakeOwner() {
  const sent: Sent[] = [];
  const port = {
    onmessage: null as ((event: { data: unknown }) => void) | null,
    close: () => undefined,
    postMessage(message: Sent) {
      sent.push(message);
      if (message.t === "hello")
        queueMicrotask(() =>
          port.onmessage?.({ data: { t: "welcome", ownerId: "o", protocol: PROTOCOL_VERSION } }),
        );
    },
  };
  const reply = (data: unknown) => {
    port.onmessage?.({ data });
  };
  const done = (seq: number, command: string, transaction: string) => {
    reply({
      t: "done",
      seq,
      columns: [],
      frames: [],
      summary: {
        command,
        rowCount: null,
        counts: null,
        lsn: null,
        replayed: false,
        closedEarly: false,
        transaction,
        catalogVersion: 1,
      },
    });
  };
  const lastSeq = () => sent.at(-1)?.seq as number;
  return { port: port as unknown as MessagePort, sent, reply, done, lastSeq };
}

const outcome = (promise: Promise<unknown>) =>
  promise.then(
    () => "ok",
    (error: unknown) => (error as Error).name,
  );

/** A client whose session `session` was lost by a reconnect after an acknowledged BEGIN. */
async function lostSession(session: number) {
  const client = new ProtocolClient();
  const first = fakeOwner();
  await client.connect(first.port);
  const begin = client.begin(session);
  first.done(first.lastSeq(), "BEGIN", "inTransaction");
  await begin;
  const second = fakeOwner();
  await client.reconnect(second.port);
  expect(client.lostSessions.has(session)).toBe(true);
  return { client, second };
}

describe("finding 2: every early rejection reaches the cursor", () => {
  it("a cursor opened on an already lost session rejects with SessionLostError", async () => {
    const { client } = await lostSession(1);
    const cursor = client.cursor("SELECT 1", [], { session: 1 });
    expect(await Promise.race([outcome(cursor.frames.next()), delay().then(() => "hung")])).toBe(
      "SessionLostError",
    );
    client.close();
  });

  it("a cursor opened after close rejects with ClientClosedError", async () => {
    const client = new ProtocolClient();
    client.close();
    const cursor = client.cursor("SELECT 1");
    expect(await Promise.race([outcome(cursor.frames.next()), delay().then(() => "hung")])).toBe(
      "ClientClosedError",
    );
  });

  it("a cursor opened on a full outstanding window rejects", async () => {
    const client = new ProtocolClient();
    await client.connect(fakeOwner().port);
    const silent: Array<Promise<unknown>> = [];
    for (let i = 0; i < MAX_OUTSTANDING_REQUESTS; i++)
      silent.push(client.query("SELECT 1").catch(() => undefined));
    const cursor = client.cursor("SELECT 1");
    expect(await Promise.race([outcome(cursor.frames.next()), delay().then(() => "hung")])).toBe(
      "RangeError",
    );
    client.close();
    await Promise.all(silent);
  });
});

describe("finding 3: a lost session's barrier clears only on a confirmed rollback", () => {
  it("classifies every ROLLBACK ... TO spelling as savepoint, never as rollback", () => {
    for (const text of [
      "ROLLBACK TO a",
      "ROLLBACK TO SAVEPOINT a",
      "ROLLBACK WORK TO SAVEPOINT a",
      "rollback transaction to a",
      '/* x */ ROLLBACK "WORK" "to" a',
    ])
      expect(classifyStatementText(text), text).toBe("savepoint");
    for (const text of ["ROLLBACK", "ROLLBACK WORK", "ROLLBACK TRANSACTION", "ABORT"])
      expect(classifyStatementText(text), text).toBe("rollback");
  });

  it("a ROLLBACK the owner refuses leaves the barrier, and the next statement is refused", async () => {
    const { client, second } = await lostSession(0);
    const rollback = client.query("ROLLBACK");
    expect(client.lostSessions.has(0)).toBe(true);
    second.reply({
      t: "error",
      seq: second.lastSeq(),
      error: { name: "PlanningError", message: "syntax error", props: { sqlState: "42601" } },
    });
    expect(await outcome(rollback)).toBe("PlanningError");
    expect(client.lostSessions.has(0)).toBe(true);
    await expect(client.query("UPDATE t SET n = 1")).rejects.toMatchObject({
      name: "SessionLostError",
      effect: "none",
    });
    client.close();
  });

  it("a ROLLBACK answered with the session still in a transaction does not clear the barrier", async () => {
    const { client, second } = await lostSession(1);
    const rollback = client.query("ROLLBACK", [], { session: 1 });
    // Not a confirmation that the session ended (a future ROLLBACK AND CHAIN, say).
    second.done(second.lastSeq(), "ROLLBACK", "inTransaction");
    await rollback;
    expect(client.lostSessions.has(1)).toBe(true);
    client.close();
  });

  it("work pipelined right behind a ROLLBACK is refused until the owner confirms it", async () => {
    const { client, second } = await lostSession(1);
    const rollback = client.query("ROLLBACK", [], { session: 1 });
    const rollbackSeq = second.lastSeq();
    const early = client.query("UPDATE t SET n = 1", [], { session: 1 }).then(
      () => "ran",
      (error: unknown) =>
        `${(error as Error).name}:${String((error as { effect?: string }).effect)}`,
    );
    expect(await Promise.race([early, delay().then(() => "sent and pending")])).toBe(
      "SessionLostError:none",
    );
    expect(second.sent.some((message) => message.text === "UPDATE t SET n = 1")).toBe(false);
    second.done(rollbackSeq, "ROLLBACK", "idle");
    await rollback;
    expect(client.lostSessions.has(1)).toBe(false);
    client.close();
  });

  it("through the composed engine: ROLLBACK WORK TO SAVEPOINT keeps the barrier (the review's probe)", async () => {
    const h = harness();
    const client = await h.connect();
    await seed(client, 1);
    await client.begin(0);
    await h.reconnect(client);
    expect(client.lostSessions.has(0)).toBe(true);
    await expect(client.query("ROLLBACK WORK TO SAVEPOINT a")).rejects.toBeDefined();
    expect(client.lostSessions.has(0)).toBe(true);
    await expect(client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1])).rejects.toMatchObject({
      name: "SessionLostError",
    });
    expect(h.writePath.rows("t")[0]?.[1]).toBe(0);
    // A real ROLLBACK, confirmed, makes it usable.
    await client.query("ROLLBACK");
    expect(client.lostSessions.has(0)).toBe(false);
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    expect(h.writePath.rows("t")[0]?.[1]).toBe(1);
  });
});

describe("finding 1: the owner's refusal says what the ledger's absence proves", () => {
  const retry = (seq: number, extra: object) => ({
    seq,
    sentAtMs: Date.now(),
    resendable: false,
    retry: true,
    live: false,
    generation: 1,
    resolvedThrough: 0,
    ...extra,
  });
  const effectOf = (message: OwnerMessage) =>
    message.t === "error" ? (message.error.props as { effect?: string } | undefined)?.effect : null;

  it("a BEGIN, COMMIT or ROLLBACK it never saw: none; a statement: unknown", async () => {
    const { server, connect, writePath } = harness();
    await seed(await connect(), 1);
    const port = raw(server, "8".repeat(32), 2);
    port.deliver(retry(1, { t: "begin", session: 1, readOnly: false }));
    port.deliver(retry(2, { t: "commit", session: 1 }));
    port.deliver(retry(3, { t: "rollback", session: 1 }));
    const statement = (seq: number, text: string, session: number) =>
      retry(seq, {
        t: "statement",
        session,
        text,
        prepared: null,
        knownColumnsVersion: -1,
        params: [],
        result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
      });
    port.deliver(statement(4, "COMMIT", 0));
    port.deliver(statement(5, "UPDATE t SET n = n + 1 WHERE id = 1", 1));
    port.deliver(statement(6, "UPDATE t SET n = n + 1 WHERE id = 1", 0));
    const effects = [];
    for (let seq = 1; seq <= 6; seq++) effects.push(effectOf(await port.reply(seq)));
    expect(effects).toEqual(["none", "none", "none", "none", "unknown", "unknown"]);
    // None of them ran here: the row is untouched.
    expect(writePath.rows("t")[0]?.[1]).toBe(0);
  });

  it("a statement of an ended generation's transaction: unknown (its transaction may have committed)", async () => {
    const { server, connect } = harness();
    await seed(await connect(), 1);
    const first = raw(server, "7".repeat(32), 1);
    first.deliver({
      t: "begin",
      seq: 1,
      session: 1,
      readOnly: false,
      sentAtMs: Date.now(),
      resolvedThrough: 0,
      live: false,
      resendable: false,
      retry: false,
      generation: 1,
    });
    await first.reply(1);
    const update = {
      t: "statement",
      seq: 2,
      session: 1,
      text: "UPDATE t SET n = n + 1 WHERE id = 1",
      prepared: null,
      knownColumnsVersion: -1,
      params: [],
      result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
      sentAtMs: Date.now(),
      resolvedThrough: 1,
      live: false,
      resendable: false,
      retry: false,
      generation: 1,
    };
    first.deliver(update);
    await first.reply(2);
    // The next generation ends the first; the statement's retry meets the ended session.
    const second = raw(server, "7".repeat(32), 2);
    second.deliver({ ...update, retry: true });
    const answer = await second.reply(2);
    expect(answer).toMatchObject({ t: "error", error: { name: "SessionLostError" } });
    expect(effectOf(answer)).toBe("unknown");
  });
});

describe("the session queue (found by the generated ledger property)", () => {
  it("a synchronous answer behind an asynchronous request does not stall the requests behind it", async () => {
    const { server, connect } = harness();
    await seed(await connect(), 1);
    const port = raw(server, "6".repeat(32), 1);
    const statement = (seq: number, text: string, extra: object = {}) => ({
      t: "statement",
      seq,
      session: 0,
      text,
      prepared: null,
      knownColumnsVersion: -1,
      params: [1],
      result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
      sentAtMs: Date.now(),
      resolvedThrough: seq - 1,
      live: false,
      resendable: true,
      retry: false,
      generation: 1,
      ...extra,
    });
    // A strict commit (asynchronous: it waits for its flush), then a retry the owner refuses at
    // once, then another statement: all three are answered.
    port.deliver(statement(1, "UPDATE t SET n = n + 1 WHERE id = $1"));
    port.deliver({
      ...statement(2, "UPDATE t SET n = n + 1 WHERE id = $1"),
      retry: true,
      resendable: false,
    });
    port.deliver(statement(3, "SELECT * FROM t WHERE id = $1"));
    const answers = await Promise.race([
      Promise.all([port.reply(1), port.reply(2), port.reply(3)]).then((all) => all.map((m) => m.t)),
      delay(2_000).then(() => "stalled"),
    ]);
    expect(answers).toEqual(["done", "error", "done"]);
  });
});

describe("ending a generation (found on the restacked p2c by timing)", () => {
  it("a retry arriving while the ended generation's session is still busy meets an ended session", async () => {
    const { server, connect } = harness();
    await seed(await connect(), 1);
    const header = (seq: number, extra: object) => ({
      seq,
      sentAtMs: Date.now(),
      resolvedThrough: seq - 1,
      live: false,
      resendable: false,
      retry: false,
      generation: 1,
      ...extra,
    });
    const statement = (seq: number, text: string, params: unknown[]) =>
      header(seq, {
        t: "statement",
        session: 1,
        text,
        prepared: null,
        knownColumnsVersion: -1,
        params,
        result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
      });
    const first = raw(server, "5".repeat(32), 1);
    first.deliver(header(1, { t: "begin", session: 1, readOnly: false }));
    await first.reply(1);
    const update = statement(2, "UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    first.deliver(update);
    await first.reply(2);
    // The session is busy (a long statement) when the next generation says hello.
    first.deliver(statement(3, "SELECT spin($1)", [50]));
    const second = raw(server, "5".repeat(32), 2);
    second.deliver({ ...update, retry: true });
    const answer = await second.reply(2);
    expect(answer).toMatchObject({
      t: "error",
      error: { name: "SessionLostError", props: { effect: "unknown" } },
    });
  });
});

describe("B1 (B5-p2d review 1): a confirmed end discharges only the losses before it", () => {
  it("a replayed earlier COMMIT does not clear a later unanswered BEGIN (wire)", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    const begin = client.begin(0);
    first.done(first.lastSeq(), "BEGIN", "inTransaction");
    await begin;
    const commit = client.commit(0).catch((error: unknown) => error);
    const commitSeq = first.lastSeq();
    const second = client.begin(0).catch((error: unknown) => error);
    const beginSeq = first.lastSeq();
    const owner = fakeOwner();
    await client.reconnect(owner.port);
    expect(client.lostSessions.has(0)).toBe(true);
    owner.reply({
      t: "done",
      seq: commitSeq,
      columns: null,
      frames: [],
      summary: {
        command: "COMMIT",
        rowCount: null,
        counts: { t: 1 },
        lsn: 1,
        replayed: true,
        closedEarly: false,
        transaction: "idle",
        catalogVersion: 1,
      },
    });
    owner.reply({
      t: "error",
      seq: beginSeq,
      error: {
        name: "SessionLostError",
        message: "lost",
        props: {
          code: "MINNOW_SESSION_LOST",
          lost: "transaction",
          sequence: beginSeq,
          effect: "none",
        },
      },
    });
    await Promise.all([commit, second]);
    // The COMMIT ended the first transaction; the refused BEGIN after it keeps the session lost.
    expect(client.lostSessions.has(0)).toBe(true);
    await expect(client.query("UPDATE t SET n = 1")).rejects.toMatchObject({
      name: "SessionLostError",
      effect: "none",
    });
    // An explicit ROLLBACK the owner confirms discharges every loss before it.
    const rollback = client.query("ROLLBACK");
    owner.done(owner.lastSeq(), "ROLLBACK", "idle");
    await rollback;
    expect(client.lostSessions.has(0)).toBe(false);
    client.close();
  });

  it("through the composed engine: COMMIT replayed, later BEGIN refused, next UPDATE refused (the review's probe)", async () => {
    const h = harness();
    const { port1, port2 } = new MessageChannel();
    const post = port1.postMessage.bind(port1);
    let armed = false;
    let dropped = 0;
    port1.postMessage = ((message: { t?: string }) => {
      if (armed && message.t === "done") {
        dropped++;
        return;
      }
      post(message);
    }) as typeof port1.postMessage;
    h.server.accept(port1);
    const client = new ProtocolClient();
    await client.connect(port2);
    await seed(client, 1);
    await client.begin(0);
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    armed = true;
    const commit = client.commit(0).catch((error: unknown) => error);
    const begin = client.begin(0).catch((error: unknown) => error);
    for (let i = 0; i < 500 && dropped < 2; i++) await delay(2);
    expect(dropped).toBe(2);
    await h.reconnect(client);
    await Promise.all([commit, begin]);
    expect(client.lostSessions.has(0)).toBe(true);
    const next = await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]).then(
      () => "executed",
      (error: unknown) => (error as Error).name,
    );
    expect(next).toBe("SessionLostError");
    // The committed increment, and nothing after it.
    expect(h.writePath.rows("t")[0]?.[1]).toBe(1);
    client.close();
    port1.close();
  });
});

describe("p2d: an owner whose timers do not run still says it is alive on the request path", () => {
  it("sends an overdue alive to a busy connection when any message arrives", async () => {
    // No interval timer at all, as in a WebKit worker whose long statement yields through
    // message tasks: the request path is the only source of `alive`.
    const realSetInterval = globalThis.setInterval;
    globalThis.setInterval = (() => 0) as unknown as typeof setInterval;
    try {
      const { server, connect } = harness({ aliveIntervalMs: 30 });
      await seed(await connect(), 1);
      const busy = raw(server, "4".repeat(32), 1);
      busy.deliver({
        t: "statement",
        seq: 1,
        session: 0,
        text: "SELECT spin($1)",
        prepared: null,
        knownColumnsVersion: -1,
        params: [400],
        result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
        sentAtMs: Date.now(),
        resolvedThrough: 0,
        live: false,
        resendable: true,
        retry: false,
        generation: 1,
      });
      await delay(100);
      const alive = () => busy.replies.filter((message) => message.t === "alive").length;
      expect(alive()).toBe(0);
      // Another client's request: the owner handles a message, and owes the busy client an alive.
      const other = raw(server, "3".repeat(32), 1);
      other.deliver({ t: "release", target: 99, through: 0 });
      expect(alive()).toBe(1);
      await busy.reply(1);
    } finally {
      globalThis.setInterval = realSetInterval;
    }
  });
});
