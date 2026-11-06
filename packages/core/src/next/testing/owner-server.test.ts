/**
 * The owner server end to end in process: protocol client ⇄ MessageChannel ⇄ server ⇄ composed
 * engine ⇄ stand-ins for the write path, executor and planner (testing/owner-standins.ts).
 */
import { describe, expect, it } from "vitest";
import { SessionLostError } from "../errors.js";
import {
  CancelledError,
  InFailedTransactionError,
  RecordedFailureError,
  TransactionIdleTimeoutError,
} from "../owner/errors.js";
import { PROTOCOL_VERSION, type OwnerMessage } from "../owner/protocol.js";
import type { ServerPort } from "../owner/server.js";
import { taskClock } from "../exec/pipeline.js";
import { flushTasks, flushUntil, harness, raw, seed, sleep } from "./owner-harness.js";

describe("the task clock (ruling R23.5)", () => {
  it("a client message begins a task on the worker's clock", async () => {
    const { server, connect } = harness();
    await connect();
    const port = raw(server);
    const before = taskClock.epoch;
    port.port.onmessage?.({ data: { t: "ping" } } as MessageEvent);
    expect(taskClock.epoch).toBe(before + 1);
  });
});

describe("handshake", () => {
  it("welcomes the current protocol and refuses another", async () => {
    const { server, connect } = harness();
    const client = await connect();
    expect(client.generation).toBe(1);
    const port = raw(server);
    expect(port.replies[0]?.t).toBe("welcome");
    const old: ServerPort = { onmessage: null, postMessage: () => undefined };
    const seen: OwnerMessage[] = [];
    old.postMessage = (message) => seen.push(message as OwnerMessage);
    server.accept(old);
    old.onmessage?.({
      data: {
        t: "hello",
        protocol: PROTOCOL_VERSION + 1,
        build: "x",
        clientId: "a".repeat(32),
        livenessLock: "",
        previousOwnerId: null,
        generation: 1,
      },
    } as MessageEvent);
    expect(seen[0]).toMatchObject({
      t: "refused",
      reason: "version",
      ownerProtocol: PROTOCOL_VERSION,
    });
  });
});

describe("statements end to end", () => {
  it("creates, writes and reads through the composed engine", async () => {
    const { connect, writePath } = harness();
    const client = await connect();
    await seed(client);
    const point = await client.query("SELECT * FROM t WHERE id = $1", [2]);
    expect(point.rows()).toEqual([[2, 0, "row 2"]]);
    expect(point.columns.map((column) => column.name)).toEqual(["id", "n", "name"]);
    const all = await client.query("SELECT * FROM t");
    expect(all.rows()).toHaveLength(3);
    const update = await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [3]);
    expect(update.summary).toMatchObject({
      command: "UPDATE",
      rowCount: 1,
      counts: { t: 1 },
      replayed: false,
    });
    expect(update.summary.lsn).toBeGreaterThan(0);
    expect(writePath.rows("t")[2]).toEqual([3, 1, "row 3"]);
    expect((await client.query("SHOW timezone")).rows()).toEqual([["UTC"]]);
  });

  it("answers a warm point read inside its message event: one post, no await (R6)", async () => {
    const { server, connect } = harness();
    await seed(await connect());
    const port = raw(server);
    const before = port.port.posted;
    const synchronous = server.stats.synchronous;
    port.deliver(port.statement(1, "SELECT * FROM t WHERE id = $1", [1]));
    // Answered before `deliver` returned: the reply is already posted.
    expect(port.port.posted).toBe(before + 1);
    expect(port.replies.at(-1)).toMatchObject({ t: "done", seq: 1, summary: { rowCount: 1 } });
    expect(server.stats.synchronous).toBe(synchronous + 1);
  });

  it("omits columns a prepared statement's client already holds", async () => {
    const { connect } = harness();
    const client = await connect();
    await seed(client);
    const prepared = await client.prepare("SELECT * FROM t WHERE id = $1");
    expect(prepared.columns.map((column) => column.name)).toEqual(["id", "n", "name"]);
    const first = await client.execute(prepared.id, [1]);
    expect(first.columns).toHaveLength(3);
    const again = await client.execute(prepared.id, [1], 0, first.summary.catalogVersion);
    expect(again.columns).toEqual([]);
    expect(again.rows()).toEqual([[1, 0, "row 1"]]);
  });

  it("maps engine errors onto the wire with their code and SQLSTATE", async () => {
    const { connect } = harness();
    const client = await connect();
    await expect(client.query("SELECT 1 / 0")).rejects.toMatchObject({
      name: "ExecutionError",
      code: "MINNOW_DIVISION_BY_ZERO",
      sqlState: "22012",
    });
    // The front end's own syntax error, passed through (it carries a position, no SQLSTATE yet).
    await expect(client.query("SELEKT 1")).rejects.toMatchObject({ name: "SqlCompileError" });
    await expect(client.query("SELECT id FROM missing")).rejects.toMatchObject({
      name: "PlanningError",
      sqlState: "42P01",
    });
    await seed(client, 1);
    await expect(
      client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "dup"]),
    ).rejects.toMatchObject({
      sqlState: "23505",
    });
  });
});

describe("RETURNING and the commit order (R2)", () => {
  it("delivers an autocommit write's RETURNING rows only after its commit is durable", async () => {
    const { connect, server, writePath, executor } = harness({ autoFlush: false });
    const acknowledgedDurable: boolean[] = [];
    executor.onAcknowledge = () =>
      acknowledgedDurable.push(writePath.durableLsn >= writePath.appliedLsn);
    const client = await connect();
    const created = client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await flushUntil(writePath, created);
    const port = raw(server);
    port.deliver(port.statement(1, "INSERT INTO t VALUES ($1, $2, $3) RETURNING *", [7, 1, "x"]));
    await sleep(10);
    // Committed in memory, not yet durable: nothing of the request has reached the client.
    expect(writePath.appliedLsn).toBeGreaterThan(writePath.durableLsn);
    expect(port.replies.filter((message) => "seq" in message && message.seq === 1)).toEqual([]);
    await writePath.flush();
    const done = await port.reply(1);
    const frames = port.replies.flatMap((message) =>
      (message.t === "rows" || message.t === "done") && message.seq === 1 ? message.frames : [],
    );
    expect(frames.map((frame) => frame.rowCount)).toEqual([1]);
    expect(done).toMatchObject({
      t: "done",
      summary: { lsn: writePath.durableLsn, counts: { t: 1 } },
    });
    // Completion 2 reached the executor only once the commit was durable.
    expect(acknowledgedDurable).toEqual([true]);
  });

  it("delivers RETURNING inside an explicit transaction when the statement has executed (R11)", async () => {
    const { connect, writePath } = harness({ autoFlush: false });
    const client = await connect();
    const created = client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await flushUntil(writePath, created);
    await client.begin(1);
    const inside = await client.query(
      "INSERT INTO t VALUES ($1, $2, $3) RETURNING *",
      [1, 0, "a"],
      { session: 1 },
    );
    expect(inside.rows()).toEqual([[1, 0, "a"]]);
    expect(inside.summary.transaction).toBe("open");
    const commit = client.commit(1);
    expect((await flushUntil(writePath, commit)).summary).toMatchObject({
      command: "COMMIT",
      counts: { t: 1 },
    });
  });
});

describe("exactly-once and resends (R12, R16, R18.2)", () => {
  it("runs a duplicated in-transaction UPDATE once: the reviewer's probe, end to end", async () => {
    const { connect, server, writePath } = harness();
    await seed(await connect(), 1);
    const port = raw(server);
    port.deliver({ ...port.statement(1, "BEGIN"), session: 1 });
    await port.reply(1);
    const update = {
      ...port.statement(2, "UPDATE t SET n = n + 1 WHERE id = $1", [1]),
      session: 1,
      resendable: false,
    };
    port.deliver(update);
    port.deliver(update);
    const first = await port.reply(2);
    expect(first).toMatchObject({ t: "done", summary: { rowCount: 1, replayed: false } });
    await flushTasks();
    const answers = port.replies.filter((message) => "seq" in message && message.seq === 2);
    expect(answers).toHaveLength(2);
    expect(answers[1]).toMatchObject({ t: "done", summary: { replayed: true, counts: { t: 1 } } });
    port.deliver({ ...port.statement(3, "COMMIT"), session: 1, resendable: false });
    await port.reply(3);
    await flushTasks();
    expect(writePath.rows("t")[0]).toEqual([1, 1, "row 1"]);
  });

  it("answers a lost COMMIT from the ledger after a reconnect, and refuses the transaction's statements", async () => {
    const { server, connect, writePath } = harness();
    await seed(await connect(), 1);
    const clientId = "c".repeat(32);
    const first = raw(server, clientId, 1);
    first.deliver({ ...first.statement(1, "BEGIN"), session: 1 });
    await first.reply(1);
    first.deliver({
      ...first.statement(2, "UPDATE t SET n = n + 1 WHERE id = $1", [1]),
      session: 1,
      resendable: false,
    });
    await first.reply(2);
    const commit = { ...first.statement(3, "COMMIT"), session: 1, resendable: false };
    first.deliver(commit);
    await first.reply(3); // The reply is "lost": the client reconnects without having seen it.
    const statementInTx = {
      ...first.statement(4, "UPDATE t SET n = n + 1 WHERE id = $1", [1]),
      session: 2,
      resendable: false,
    };
    first.deliver({ ...first.statement(5, "BEGIN"), session: 2 });
    await first.reply(5);

    const second = raw(server, clientId, 2);
    // The resent COMMIT is answered from the ledger: committed, with its counts, not run again.
    second.deliver({ ...commit, retry: true });
    expect(await second.reply(3)).toMatchObject({
      t: "done",
      summary: { replayed: true, counts: { t: 1 } },
    });
    // A statement of the old generation's open transaction is refused and never runs.
    second.deliver({ ...statementInTx, retry: true });
    const refused = await second.reply(4);
    expect(refused).toMatchObject({ t: "error", error: { name: "SessionLostError" } });
    expect(writePath.rows("t")[0]).toEqual([1, 1, "row 1"]);
    // The old generation's open transaction rolled back once its last request settled.
    await flushTasks();
    expect(writePath.writerBusy).toBe(false);
  });

  it("never runs a non-resendable retry a new owner has not seen (its old owner died)", async () => {
    const { server, connect, writePath } = harness();
    await seed(await connect(), 1);
    const clientId = "b".repeat(32);
    // The old owner is gone without ending the generation: this owner's ledger knows nothing of it.
    const port = raw(server, clientId, 2);
    const lost = {
      ...port.statement(9, "UPDATE t SET n = n + 1 WHERE id = $1", [1]),
      session: 1,
      generation: 1,
      resendable: false,
      retry: true,
    };
    port.deliver(lost);
    expect(await port.reply(9)).toMatchObject({ t: "error", error: { name: "SessionLostError" } });
    await flushTasks();
    expect(writePath.rows("t")[0]).toEqual([1, 0, "row 1"]);
  });

  it("re-runs a resent standalone nextval: at-least-once (R16.2)", async () => {
    const { server, connect } = harness();
    await (await connect()).query("CREATE SEQUENCE s");
    const clientId = "d".repeat(32);
    const first = raw(server, clientId, 1);
    first.deliver(first.statement(1, "SELECT nextval('s')"));
    const one = await first.reply(1);
    const second = raw(server, clientId, 2);
    second.deliver({ ...first.statement(1, "SELECT nextval('s')"), retry: true });
    const two = await second.reply(1);
    const value = (message: OwnerMessage) =>
      message.t === "done" &&
      message.frames[0]?.columns[0]?.form === "flat" &&
      message.frames[0].columns[0].data.layout === "int64"
        ? message.frames[0].columns[0].data.values.values[0]
        : null;
    expect(value(one)).toBe(1);
    expect(value(two)).toBe(2);
  });

  it("replays a recorded autocommit failure instead of running it again", async () => {
    const { server, connect } = harness();
    await seed(await connect(), 1);
    const port = raw(server);
    const duplicate = port.statement(1, "INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "dup"]);
    port.deliver(duplicate);
    expect(await port.reply(1)).toMatchObject({
      t: "error",
      error: { props: { sqlState: "23505" } },
    });
    port.replies.length = 0;
    port.deliver(duplicate);
    const again = await port.reply(1);
    expect(again).toMatchObject({
      t: "error",
      error: { name: "RecordedFailureError", props: { code: "MINNOW_UNIQUE_VIOLATION" } },
    });
    expect(RecordedFailureError).toBeDefined();
  });
});

describe("transaction state", () => {
  it("aborts a transaction on a failing statement (25P02) until ROLLBACK", async () => {
    const { connect } = harness();
    const client = await connect();
    await seed(client, 1);
    await client.query("BEGIN");
    await expect(client.query("SELECT 1 / 0")).rejects.toMatchObject({ sqlState: "22012" });
    await expect(client.query("SELECT * FROM t WHERE id = $1", [1])).rejects.toBeInstanceOf(
      InFailedTransactionError,
    );
    expect((await client.query("ROLLBACK")).summary.transaction).toBe("idle");
    expect((await client.query("SELECT * FROM t WHERE id = $1", [1])).rows()).toHaveLength(1);
  });

  it("rolls back an idle transaction after transactionIdleTimeoutMs and frees the writer slot (R16.7)", async () => {
    const { connect, writePath } = harness({ idleMs: 30 });
    const client = await connect();
    await seed(client, 1);
    const aborted: number[] = [];
    client.onSessionAborted = (session) => aborted.push(session);
    await client.begin(1);
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    expect(writePath.writerBusy).toBe(true);
    await sleep(80);
    expect(aborted).toEqual([1]);
    expect(writePath.writerBusy).toBe(false);
    await expect(
      client.query("SELECT * FROM t WHERE id = $1", [1], { session: 1 }),
    ).rejects.toBeInstanceOf(TransactionIdleTimeoutError);
    await client.rollback(1);
    expect(writePath.rows("t")[0]).toEqual([1, 0, "row 1"]);
    // Another writer gets the slot meanwhile; the session works again after ROLLBACK.
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    expect(writePath.rows("t")[0]).toEqual([1, 1, "row 1"]);
  });

  it("keeps a transaction with a request running past the timeout", async () => {
    const { connect } = harness({ rowsPerFrame: 10, idleMs: 20 });
    const client = await connect();
    await seed(client, 40);
    await client.query("BEGIN");
    const cursor = client.cursor("SELECT * FROM t", [], { windowBytes: 1 });
    let taken = 0;
    for await (const frame of cursor.frames) {
      taken += frame.rowCount;
      await sleep(15);
    }
    expect(taken).toBe(40);
    expect((await client.query("COMMIT")).summary.command).toBe("COMMIT");
  });
});

describe("result windows and releases (R3)", () => {
  it("bounds the frames in flight by the window and releases them as the cursor takes them", async () => {
    const { connect, server } = harness({ rowsPerFrame: 10 });
    const client = await connect();
    await seed(client, 100);
    const cursor = client.cursor("SELECT * FROM t", [], { windowBytes: 1 });
    await sleep(20);
    // A 1-byte window: one frame outstanding at a time.
    expect(server.stats.outstandingFrames).toBe(1);
    let rows = 0;
    let most = 0;
    for await (const frame of cursor.frames) {
      rows += frame.rowCount;
      await flushTasks();
      most = Math.max(most, server.stats.outstandingFrames);
    }
    expect(rows).toBe(100);
    expect(most).toBeLessThanOrEqual(2);
    await sleep(5);
    expect(server.stats.outstandingFrames).toBe(0);
  });

  it("sends the next frame only when the client releases one (a 1-byte window)", async () => {
    const { connect, server } = harness({ rowsPerFrame: 10 });
    await seed(await connect(), 100);
    const port = raw(server);
    port.deliver(
      port.statement(1, "SELECT * FROM t", [], {
        result: { windowBytes: 1, frameRows: null, wantRows: true },
      }),
    );
    const received = () =>
      port.replies.flatMap((message) =>
        (message.t === "rows" || message.t === "done") && message.seq === 1 ? message.frames : [],
      );
    await sleep(20);
    expect(received()).toHaveLength(1);
    for (let through = 0; through < 9; through++) {
      port.deliver({ t: "release", target: 1, through });
      await sleep(3);
      expect(received()).toHaveLength(through + 2);
    }
    await port.reply(1);
    port.deliver({ t: "release", target: 1, through: 9 });
    expect(server.stats.outstandingFrames).toBe(0);
  });

  it("releases every outstanding frame when the connection ends", async () => {
    const { connect, server, executor } = harness({ rowsPerFrame: 10 });
    const client = await connect();
    await seed(client, 50);
    client.cursor("SELECT * FROM t", [], { windowBytes: 1 << 20 });
    await sleep(20);
    expect(server.stats.outstandingFrames).toBeGreaterThan(0);
    expect(executor.outstandingBytes).toBeGreaterThan(0);
    client.close();
    await sleep(10);
    expect(server.stats.outstandingFrames).toBe(0);
    // The executor's result grants got every frame back (B4's accounting, not only the server's).
    expect(executor.outstandingBytes).toBe(0);
    expect(server.stats.connections).toBe(0);
  });

  it("ends a cursor early with done { closedEarly }", async () => {
    const { connect } = harness({ rowsPerFrame: 10 });
    const client = await connect();
    await seed(client, 50);
    const cursor = client.cursor("SELECT * FROM t", [], { windowBytes: 1 });
    const first = await cursor.frames.next();
    expect(first.done).toBe(false);
    cursor.close();
    const rest: number[] = [];
    for await (const frame of cursor.frames) rest.push(frame.rowCount);
    expect(rest.length).toBeLessThan(4);
  });

  it("cancels an abandoned non-live stream", async () => {
    const { connect, server } = harness({ rowsPerFrame: 10, abandonedStreamMs: 20 });
    const client = await connect();
    await seed(client, 50);
    const cursor = client.cursor("SELECT * FROM t", [], { windowBytes: 1 });
    await sleep(80);
    expect(server.stats.outstandingFrames).toBe(0);
    await expect(async () => {
      for await (const frame of cursor.frames) void frame;
    }).rejects.toBeInstanceOf(SessionLostError);
  });
});

describe("cancellation and the commit boundary (R9)", () => {
  it("cancels a write waiting for the writer slot, with no effect", async () => {
    const { connect, writePath } = harness();
    const client = await connect();
    await seed(client, 1);
    await client.begin(1); // holds the writer slot
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    const waiting = client.send({
      t: "statement",
      session: 0,
      text: "UPDATE t SET n = n + 1 WHERE id = $1",
      prepared: null,
      knownColumnsVersion: -1,
      params: [1],
      result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
    });
    await sleep(5);
    client.cancel(waiting.seq);
    await expect(waiting.promise).rejects.toBeInstanceOf(CancelledError);
    await client.rollback(1);
    expect(writePath.rows("t")[0]).toEqual([1, 0, "row 1"]);
  });

  it("completes a write cancelled after its commit() call, RETURNING rows included", async () => {
    const { connect, writePath } = harness({ autoFlush: false });
    const client = await connect();
    const created = client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await flushUntil(writePath, created);
    const insert = client.send({
      t: "statement",
      session: 0,
      text: "INSERT INTO t VALUES ($1, $2, $3) RETURNING *",
      prepared: null,
      knownColumnsVersion: -1,
      params: [1, 0, "a"],
      result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
    });
    await sleep(5); // executed, committed in memory, waiting for the flush
    expect(writePath.appliedLsn).toBeGreaterThan(writePath.durableLsn);
    client.cancel(insert.seq);
    await sleep(5);
    await writePath.flush();
    const result = await insert.promise;
    expect(result.summary).toMatchObject({ command: "INSERT", counts: { t: 1 } });
    expect(result.rows()).toEqual([[1, 0, "a"]]);
    expect(writePath.rows("t")).toHaveLength(1);
  });
});

describe("connections ending", () => {
  it("rolls back and releases a client whose liveness lock was released", async () => {
    const liveness = new Map<string, AbortController>();
    const { connect, writePath, server } = harness({ liveness });
    const client = await connect();
    await seed(client, 1);
    await client.begin(1);
    expect(writePath.writerBusy).toBe(true);
    // Not watched until the client declares its lock held.
    expect(liveness.size).toBe(0);
    client.liveness(true);
    await flushTasks();
    expect(liveness.size).toBe(1);
    [...liveness.values()][0]?.abort();
    await flushTasks();
    expect(writePath.writerBusy).toBe(false);
    expect(server.stats.connections).toBe(0);
  });

  it("ends an older generation when the client reconnects, and steps down cleanly", async () => {
    const { connect, reconnect, server, writePath } = harness();
    const client = await connect();
    await seed(client, 1);
    await client.begin(1);
    await reconnect(client);
    expect(server.stats.connections).toBe(1);
    expect(writePath.writerBusy).toBe(false);
    const reasons: string[] = [];
    client.onStepDown = (reason) => reasons.push(reason);
    server.stepDown("pagehide");
    await sleep(5);
    expect(reasons).toEqual(["pagehide"]);
    expect(server.stats.connections).toBe(0);
  });
});
