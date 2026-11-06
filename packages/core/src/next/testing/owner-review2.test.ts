/**
 * The second review of increment 2 (B5-p2b-review-2): its probes, kept as tests, and the ruling's
 * cases around them.
 *
 * - Statement kinds come from authority: prepared statements from the owner's `prepared` reply,
 *   SQL text from the client's classifier (sequence.ts, checked against the front end in
 *   owner-statement-kind.test.ts). A lost reply for a begin or unknown statement marks its session
 *   lost and is never resent (session 0 too); commit goes to the ledger; rollback ends the session.
 * - Reconnect failures reach cursor consumers.
 * - `close()` settles every pending startup and discovery wait.
 */
import { describe, expect, it } from "vitest";
import { CLIENT_RETRY_WINDOW_MS } from "../format/ledger.js";
import { Broker } from "../owner/broker.js";
import { ProtocolClient } from "../owner/client.js";
import { OwnerConnector, workerOwner } from "../owner/connector.js";
import { hostLockName } from "../owner/election.js";
import { PROTOCOL_VERSION, type PreparedStatementKind } from "../owner/protocol.js";
import { FakeLockRegistry } from "./fake-locks.js";
import { harness, raw, seed } from "./owner-harness.js";

const delay = (ms = 40) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

type Sent = Record<string, unknown>;

/** A fake owner port: welcomes every hello, records what the client sends, replies on demand. */
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
  const lastSeq = () => sent.at(-1)?.seq as number;
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
  const prepared = (seq: number, id: number, statementKind: PreparedStatementKind) => {
    reply({
      t: "prepared",
      seq,
      id,
      columns: [],
      parameters: [],
      catalogVersion: 1,
      controlsTransaction: statementKind !== "other",
      statementKind,
      readOnly: false,
    });
  };
  return { port: port as unknown as MessagePort, sent, reply, lastSeq, done, prepared };
}

const outcome = (promise: Promise<unknown>) =>
  promise.then(
    () => "ok",
    (error: unknown) => (error as Error).name,
  );

describe("finding 1: a lost BEGIN binds its session, however it was spelled", () => {
  const spellings = [
    "BEGIN",
    "start transaction",
    "  Begin Work ;",
    "/* lead */ START TRANSACTION READ ONLY",
    '-- note\n"begin"',
    "/* a /* b */ BEGIN",
  ];

  for (const session of [1, 0])
    for (const text of spellings)
      it(`SQL ${JSON.stringify(text)} in session ${String(session)}`, async () => {
        const client = new ProtocolClient();
        const first = fakeOwner();
        await client.connect(first.port);
        const begun = outcome(client.query(text, [], { session }));
        const second = fakeOwner();
        await client.reconnect(second.port);
        // Never resent: it may have opened a transaction on the old owner.
        expect(second.sent.filter((message) => message.retry === true)).toEqual([]);
        expect(await begun).toBe("SessionLostError");
        expect(client.lostSessions.has(session)).toBe(true);
        // What follows on the session is refused, unsent, until it is ended explicitly.
        const update = outcome(client.query("UPDATE t SET n = n + 1", [], { session }));
        expect(await update).toBe("SessionLostError");
        expect(second.sent.some((message) => message.text === "UPDATE t SET n = n + 1")).toBe(
          false,
        );
        const rollback = client.query("ROLLBACK", [], { session });
        expect(second.sent.at(-1)?.text).toBe("ROLLBACK");
        second.done(second.lastSeq(), "ROLLBACK", "idle");
        await rollback;
        expect(client.lostSessions.has(session)).toBe(false);
        client.close();
      });

  for (const session of [1, 0])
    it(`a prepared BEGIN in session ${String(session)}, by the owner's statement kind`, async () => {
      const client = new ProtocolClient();
      const first = fakeOwner();
      await client.connect(first.port);
      const prepare = client.prepare("BEGIN");
      first.prepared(first.lastSeq(), 1, "begin");
      const { id } = await prepare;
      const begun = outcome(client.execute(id, [], session));
      const second = fakeOwner();
      await client.reconnect(second.port);
      expect(second.sent.filter((message) => message.retry === true)).toEqual([]);
      expect(await begun).toBe("SessionLostError");
      expect(client.lostSessions.has(session)).toBe(true);
      client.close();
    });

  it("a statement in flight after an unanswered BEGIN is part of the lost transaction", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    const begun = outcome(client.query("BEGIN", [], { session: 1 }));
    const update = outcome(client.query("UPDATE t SET n = 1", [], { session: 1 }));
    const second = fakeOwner();
    await client.reconnect(second.port);
    expect(second.sent.filter((message) => message.retry === true)).toEqual([]);
    expect([await begun, await update]).toEqual(["SessionLostError", "SessionLostError"]);
    client.close();
  });

  it("a multi-statement text is unknown: refused, the session lost, its effect not claimed", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    const multi = client.query("UPDATE t SET n = 1; COMMIT", [], { session: 0 });
    const second = fakeOwner();
    await client.reconnect(second.port);
    expect(second.sent.filter((message) => message.retry === true)).toEqual([]);
    await expect(multi).rejects.toMatchObject({ name: "SessionLostError", effect: "unknown" });
    await multi.catch((error: unknown) => {
      expect((error as Error).message).not.toContain("nothing took effect");
    });
    expect(client.lostSessions.has(0)).toBe(true);
    client.close();
  });

  it("an autocommit statement on idle session 0 is still resent", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    const update = client.query("UPDATE t SET n = n + 1 WHERE id = 1");
    const seq = first.lastSeq();
    const second = fakeOwner();
    await client.reconnect(second.port);
    expect(second.sent.filter((message) => message.retry === true).map((m) => m.seq)).toEqual([
      seq,
    ]);
    second.done(seq, "UPDATE", "idle");
    await update;
    expect(client.lostSessions.size).toBe(0);
    client.close();
  });
});

describe("finding 2: a prepared COMMIT goes through the ledger like a typed one", () => {
  it("is resent after a reconnect, and its replayed answer ends the lost session", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    const prepare = client.prepare("COMMIT");
    first.prepared(first.lastSeq(), 1, "commit");
    await prepare;
    const begin = client.begin(1);
    first.done(first.lastSeq(), "BEGIN", "inTransaction");
    await begin;
    const commit = client.execute(1, [], 1);
    const seq = first.lastSeq();
    const second = fakeOwner();
    await client.reconnect(second.port);
    expect(
      second.sent.filter((message) => message.retry === true).map((message) => message.prepared),
    ).toEqual([1]);
    expect(client.lostSessions.has(1)).toBe(true);
    second.reply({
      t: "done",
      seq,
      columns: [],
      frames: [],
      summary: {
        command: "COMMIT",
        rowCount: null,
        counts: { t: 1 },
        lsn: 7,
        replayed: true,
        closedEarly: false,
        transaction: "idle",
        catalogVersion: 1,
      },
    });
    expect((await commit).summary.replayed).toBe(true);
    expect(client.lostSessions.has(1)).toBe(false);
    client.close();
  });

  it("a prepared statement whose kind the owner never sent is unknown, never resent", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    // Prepared id 9 was never answered by this owner: nothing is known about it.
    const run = client.execute(9, [], 1);
    const second = fakeOwner();
    await client.reconnect(second.port);
    expect(second.sent.filter((message) => message.retry === true)).toEqual([]);
    await expect(run).rejects.toMatchObject({ name: "SessionLostError", effect: "unknown" });
    client.close();
  });
});

describe("finding 3: reconnect failures reach cursor consumers", () => {
  it("wakes a cursor of a lost transaction waiting for its first frame", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    const begin = client.begin(1);
    first.done(first.lastSeq(), "BEGIN", "inTransaction");
    await begin;
    const cursor = client.cursor("SELECT * FROM t", [], { session: 1 });
    const waiting = outcome(cursor.frames.next());
    await client.reconnect(fakeOwner().port);
    expect(await Promise.race([waiting, delay().then(() => "hung")])).toBe("SessionLostError");
    client.close();
  });

  it("wakes a cursor whose frames were already handed out", async () => {
    const client = new ProtocolClient();
    const first = fakeOwner();
    await client.connect(first.port);
    const cursor = client.cursor("SELECT * FROM t");
    const seq = first.lastSeq();
    first.reply({
      t: "rows",
      seq,
      columns: [],
      frames: [{ sequence: 0, rowCount: 0, dictionaries: [], columns: [] }],
    });
    await cursor.frames.next();
    const waiting = outcome(cursor.frames.next());
    await client.reconnect(fakeOwner().port);
    expect(await Promise.race([waiting, delay().then(() => "hung")])).toBe("SessionLostError");
    client.close();
  });

  it("wakes a cursor when the client closes", async () => {
    const client = new ProtocolClient();
    await client.connect(fakeOwner().port);
    const cursor = client.cursor("SELECT * FROM t");
    const waiting = outcome(cursor.frames.next());
    client.close();
    expect(await Promise.race([waiting, delay().then(() => "hung")])).toBe("ClientClosedError");
  });
});

describe("finding 4: close() settles every startup and discovery wait", () => {
  const page = () => ({
    kind: "page" as const,
    visible: true,
    platform: { mobile: false },
    superseded: false,
  });

  it("settles a worker startup whose ready never arrives", async () => {
    const spawned = { started: false };
    let terminated = 0;
    const worker = {
      onmessage: null,
      onerror: null,
      postMessage() {
        spawned.started = true;
      },
      terminate() {
        terminated++;
      },
    } as unknown as Worker;
    const connector = new OwnerConnector({
      database: "close",
      client: new ProtocolClient(),
      locks: new FakeLockRegistry().context(),
      openBroker: null,
      context: page,
      startOwner: workerOwner(() => worker, { mobile: false }),
    });
    const starting = outcome(connector.start());
    while (!spawned.started) await delay(1);
    connector.close();
    expect(await Promise.race([starting, delay().then(() => "hung")])).toBe("ClientClosedError");
    expect(terminated).toBe(1);
  });

  it("settles a broker discovery pending at shutdown", async () => {
    const broker = fakeOwner();
    const connector = new OwnerConnector({
      database: "close-broker",
      client: new ProtocolClient(),
      locks: new FakeLockRegistry().context(),
      openBroker: () => broker.port,
      startOwner: null,
      context: page,
    });
    const starting = outcome(connector.start());
    await delay(1);
    expect(broker.sent.some((message) => message.t === "connect")).toBe(true);
    connector.close();
    expect(await Promise.race([starting, delay().then(() => "hung")])).toBe("ClientClosedError");
  });

  it("settles a handshake pending at shutdown at once, not at the handshake bound", async () => {
    const registry = new FakeLockRegistry();
    // A live owner holds the host lock but never answers hello: the handshake stays pending.
    const owner = registry.context();
    void owner.request(hostLockName("close-hello"), {}, () => new Promise<void>(() => undefined));
    const silent: MessagePort[] = [];
    let asks = 0;
    const connector = new OwnerConnector({
      database: "close-hello",
      client: new ProtocolClient(),
      locks: registry.context(),
      openBroker: () => {
        const { port1, port2 } = new MessageChannel();
        silent.push(port1);
        port1.onmessage = (event: MessageEvent) => {
          const message = event.data as { t: string; nonce: number };
          if (message.t !== "connect") return;
          asks++;
          const channel = new MessageChannel();
          silent.push(channel.port1);
          port1.postMessage(
            { t: "port", nonce: message.nonce, ownerId: "x", port: channel.port2 },
            [channel.port2],
          );
        };
        return port2;
      },
      startOwner: null,
      context: page,
      handshakeTimeoutMs: 60_000,
    });
    const starting = outcome(connector.start());
    await delay(20);
    // One handshake, still waiting for its welcome.
    expect(asks).toBe(1);
    connector.close();
    expect(await Promise.race([starting, delay().then(() => "hung")])).toBe("ClientClosedError");
    for (const port of silent) port.close();
    owner.kill();
  });
});

describe("checked sound by the review (kept as regression tests)", () => {
  it("a missing, old-generation or expired COMMIT never reports success", async () => {
    const { server, connect } = harness();
    await seed(await connect(), 1);
    const port = raw(server, "9".repeat(32), 2);
    const commit = {
      t: "commit",
      session: 1,
      seq: 1,
      sentAtMs: Date.now(),
      resolvedThrough: 0,
      live: false,
      resendable: false,
      retry: true,
      generation: 1,
    };
    port.deliver(commit);
    expect(await port.reply(1)).toMatchObject({
      t: "error",
      error: { name: "SessionLostError", props: { effect: "none" } },
    });
    port.deliver({ ...commit, seq: 2, sentAtMs: Date.now() - CLIENT_RETRY_WINDOW_MS - 100 });
    expect(await port.reply(2)).toMatchObject({
      t: "error",
      error: { name: "RequestExpiredError" },
    });
  });

  it("the broker releases a port's waits per database on registration and all on close", async () => {
    const broker = new Broker({ query: () => Promise.resolve({ held: [] }) });
    const closers: Array<() => void> = [];
    const port = {
      onmessage: null as ((event: MessageEvent) => void) | null,
      postMessage: () => undefined,
      addEventListener(type: string, listener: () => void) {
        if (type === "close") closers.push(listener);
      },
    };
    broker.accept(port as unknown as MessagePort);
    for (let i = 0; i < 100; i++)
      port.onmessage?.({
        data: { t: "connect", v: 1, db: `d${String(i)}`, clientId: "c", nonce: i },
      } as MessageEvent);
    await delay(1);
    expect(broker.heldPorts).toBe(100);
    port.onmessage?.({
      data: {
        t: "register",
        v: 1,
        db: "d0",
        ownerId: "o",
        protocol: 1,
        build: "",
        lockHolder: null,
      },
    } as MessageEvent);
    expect(broker.heldPorts).toBe(99);
    for (const close of closers) close();
    expect(broker.heldPorts).toBe(0);
  });

  it("a late welcome after an aborted handshake cannot answer the next one", async () => {
    const client = new ProtocolClient();
    const first = new MessageChannel();
    const abort = new AbortController();
    const pending = outcome(client.connect(first.port1, abort.signal));
    abort.abort();
    expect(await pending).toBe("OwnerUnreachableError");
    const second = new MessageChannel();
    second.port2.onmessage = () => {
      second.port2.postMessage({ t: "welcome", ownerId: "new", protocol: PROTOCOL_VERSION });
    };
    const connecting = client.connect(second.port1);
    first.port2.postMessage({ t: "welcome", ownerId: "old", protocol: PROTOCOL_VERSION });
    expect((await connecting).ownerId).toBe("new");
    client.close();
    first.port2.close();
    second.port2.close();
  });
});
