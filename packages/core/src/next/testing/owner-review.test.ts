/**
 * The adversarial probes of review B5-p2a (Astra 6, 2026-11-05), adopted into the suite. Each
 * probe states the defect it found; all must pass for the reason it names.
 */
import { describe, expect, it } from "vitest";
import { ProtocolClient } from "../owner/client.js";
import { flushUntil, harness, raw, seed, sleep } from "./owner-harness.js";

/** A fake port for client-only probes: answers hello (and prepare) and records what is sent. */
function fakePort(onSend: (message: Record<string, unknown>) => void = () => undefined) {
  const port = {
    onmessage: null as ((event: { data: unknown }) => void) | null,
    close: () => undefined,
    postMessage(message: Record<string, unknown>) {
      onSend(message);
      if (message.t === "hello")
        queueMicrotask(() => port.onmessage?.({ data: { t: "welcome", ownerId: "o" } }));
      if (message.t === "prepare")
        queueMicrotask(() =>
          port.onmessage?.({
            data: {
              t: "prepared",
              seq: message.seq,
              id: message.id,
              columns: [],
              parameters: [],
              catalogVersion: 1,
              controlsTransaction: true,
            },
          }),
        );
    },
  };
  return port;
}

const asPort = (port: ReturnType<typeof fakePort>): MessagePort => port as unknown as MessagePort;

describe("transaction state (findings 6, 7, 8)", () => {
  it("a read-only refusal aborts the transaction (25P02)", async () => {
    const { connect } = harness();
    const client = await connect();
    await seed(client, 1);
    await client.query("BEGIN READ ONLY");
    await expect(client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1])).rejects.toMatchObject({
      sqlState: "25006",
    });
    await expect(client.query("SHOW timezone")).rejects.toMatchObject({ sqlState: "25P02" });
  });

  it("a missing savepoint aborts the transaction (3B001, then 25P02)", async () => {
    const { connect } = harness();
    const client = await connect();
    await client.query("BEGIN");
    await expect(client.query("RELEASE missing")).rejects.toMatchObject({ sqlState: "3B001" });
    await expect(client.query("SHOW timezone")).rejects.toMatchObject({ sqlState: "25P02" });
  });

  it("restores the transaction's counts on ROLLBACK TO", async () => {
    const { connect } = harness();
    const client = await connect();
    await seed(client, 1);
    await client.query("BEGIN");
    await client.query("SAVEPOINT a");
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    await client.query("ROLLBACK TO a");
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    expect((await client.query("COMMIT")).summary.counts).toEqual({ t: 1 });
  });

  it("allows savepoints in a read-only transaction", async () => {
    const { connect } = harness();
    const client = await connect();
    await client.query("BEGIN READ ONLY");
    expect((await client.query("SAVEPOINT a")).summary.command).toBe("SAVEPOINT");
  });

  it("restores the shadowed savepoint after RELEASE of the newer one with its name", async () => {
    const { connect } = harness();
    const client = await connect();
    await client.query("BEGIN");
    await client.query("SAVEPOINT a");
    await client.query("SAVEPOINT a");
    await client.query("RELEASE a");
    expect((await client.query("ROLLBACK TO a")).summary.command).toBe("ROLLBACK");
  });

  it("plans a transaction's statements against its own DDL, privately", async () => {
    const { connect, writePath } = harness();
    const client = await connect();
    await client.query("BEGIN");
    await client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    const insert = await client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "a"]);
    expect(insert.summary.rowCount).toBe(1);
    await client.query("ROLLBACK");
    expect(writePath.catalog.tableByName("t")).toBeUndefined();
    // Another table t reaches the same catalog version, committed, with other columns. A plan
    // made inside the rolled-back transaction must not be reused for it.
    await client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT)");
    await client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "a"]);
    expect(writePath.rows("t")).toEqual([[1, 0]]);
  });
});

describe("replies and resends (findings 1, 3, 5, 11)", () => {
  it("refuses a lost RETURNING reply's retry with CommittedWithoutResultError, never an empty result", async () => {
    const { server, connect } = harness();
    await seed(await connect(), 1);
    const port = raw(server);
    const update = port.statement(1, "UPDATE t SET n = n + 1 WHERE id = $1 RETURNING *", [1]);
    port.deliver(update);
    await port.reply(1);
    port.replies.length = 0;
    port.deliver({ ...update, retry: true });
    expect(await port.reply(1)).toMatchObject({
      t: "error",
      error: { name: "CommittedWithoutResultError", props: { counts: { t: 1 } } },
    });
  });

  it("refuses a hello older than one already seen from the client", () => {
    const { server } = harness();
    raw(server, "f".repeat(32), 2);
    const old = raw(server, "f".repeat(32), 1);
    expect(old.replies[0]).toMatchObject({ t: "refused", reason: "staleGeneration" });
  });

  it("marks requests behind a prepared BEGIN not resendable", async () => {
    const sent: Array<Record<string, unknown>> = [];
    const client = new ProtocolClient();
    await client.connect(asPort(fakePort((message) => sent.push(message))));
    const prepared = await client.prepare("BEGIN");
    const begin = client.execute(prepared.id).catch(() => undefined);
    const update = client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]).catch(() => undefined);
    const resendable = sent.at(-1)?.resendable;
    client.close();
    await Promise.all([begin, update]);
    expect(resendable).toBe(false);
  });

  it("starts a resent request's stream dictionaries afresh", async () => {
    const client = new ProtocolClient();
    let statementSeq = 0;
    const capture = (message: Record<string, unknown>) => {
      if (message.t === "statement") statementSeq = message.seq as number;
    };
    const first = fakePort(capture);
    await client.connect(asPort(first));
    const query = client.query("SELECT * FROM t");
    const frame = { sequence: 0, rowCount: 0, columns: [], dictionaries: [] };
    first.onmessage?.({ data: { t: "rows", seq: statementSeq, columns: [], frames: [frame] } });
    const second = fakePort(capture);
    await client.reconnect(asPort(second));
    // The resent request's stream starts again at frame 0 and completes normally.
    second.onmessage?.({ data: { t: "rows", seq: statementSeq, columns: [], frames: [frame] } });
    second.onmessage?.({
      data: {
        t: "done",
        seq: statementSeq,
        columns: null,
        frames: [],
        summary: {
          command: "SELECT",
          rowCount: 0,
          counts: null,
          lsn: null,
          replayed: false,
          closedEarly: false,
          transaction: "idle",
          catalogVersion: 1,
        },
      },
    });
    const result = await query;
    client.close();
    expect(result.frames).toHaveLength(1);
  });
});

describe("frames and their charge (findings 9, 10, 12)", () => {
  it("frees a finished cursor's frames when the client closes it", async () => {
    const { connect, server, executor, governor } = harness();
    const client = await connect();
    await seed(client, 3);
    const cursor = client.cursor("SELECT * FROM t");
    await sleep(20);
    cursor.close();
    await sleep(20);
    expect(server.stats.outstandingFrames).toBe(0);
    expect(executor.outstandingBytes).toBe(0);
    expect(governor.usedBytes).toBe(0);
  });

  it("keeps watching a live client while a finished stream's frames are outstanding", async () => {
    const liveness = new Map<string, AbortController>();
    const { connect, server } = harness({ liveness });
    const client = await connect();
    await seed(client, 3);
    client.liveness(true);
    client.cursor("SELECT * FROM t");
    await sleep(20);
    expect(server.stats.outstandingFrames).toBeGreaterThan(0);
    expect(liveness.size).toBe(1);
  });

  it("frees a live point read's frame when its cursor is closed", async () => {
    const { connect, executor, governor } = harness();
    const client = await connect();
    await seed(client, 1);
    client.liveness(true);
    const cursor = client.cursor("SELECT * FROM t WHERE id = $1", [1]);
    await sleep(20);
    cursor.close();
    await sleep(20);
    expect(executor.outstandingBytes).toBe(0);
    expect(governor.usedBytes).toBe(0);
  });

  it("frees a frame held behind a durability wait when the client disconnects", async () => {
    const { connect, engine, executor } = harness();
    const client = await connect();
    await seed(client, 1);
    let open: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      open = resolve;
    });
    engine.durable = () => gate;
    const query = client.query("SELECT * FROM t").catch(() => undefined);
    await sleep(10);
    expect(executor.outstandingBytes).toBeGreaterThan(0);
    client.close();
    await sleep(10);
    open();
    await sleep(10);
    await query;
    expect(executor.outstandingBytes).toBe(0);
  });

  it("frees the frame pulled before a failed durability wait", async () => {
    const { connect, engine, executor } = harness();
    const client = await connect();
    await seed(client, 1);
    engine.durable = () => Promise.reject(new Error("flush failed"));
    await expect(client.query("SELECT * FROM t")).rejects.toThrow("flush failed");
    expect(executor.outstandingBytes).toBe(0);
  });

  it("answers a strict nextval only once its sequence block is durable", async () => {
    const { connect, writePath } = harness({ autoFlush: false });
    const client = await connect();
    let answered = false;
    const query = client.query("SELECT nextval('s')").then(() => {
      answered = true;
    });
    await sleep(10);
    const premature = answered;
    await flushUntil(writePath, query);
    expect(premature).toBe(false);
  });

  it("charges an autocommit write's RETURNING spool until it is delivered", async () => {
    const { connect, writePath, governor } = harness({ autoFlush: false });
    const client = await connect();
    const created = client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await flushUntil(writePath, created);
    const insert = client.query("INSERT INTO t VALUES ($1, $2, $3) RETURNING *", [1, 0, "a"]);
    await sleep(10);
    expect(governor.usedBytes).toBeGreaterThan(0); // the spool, before the commit is durable
    expect((await flushUntil(writePath, insert)).rows()).toEqual([[1, 0, "a"]]);
    await sleep(5);
    expect(governor.usedBytes).toBe(0); // released on receipt, grant closed
  });
});

describe("cancellation and failed commits (findings 2, 4)", () => {
  it("never runs a write cancelled while it waited behind its session", async () => {
    const { connect, writePath } = harness();
    const client = await connect();
    await seed(client, 20);
    const cursor = client.cursor("SELECT * FROM t STREAM BY 1", [], { windowBytes: 1 });
    await sleep(10);
    const update = client.send({
      t: "statement",
      session: 0,
      text: "UPDATE t SET n = n + 1 WHERE id = $1",
      prepared: null,
      knownColumnsVersion: -1,
      params: [1],
      result: { windowBytes: 1024, frameRows: null, wantRows: true },
    });
    const outcome = update.promise.then(
      () => "committed",
      () => "cancelled",
    );
    client.cancel(update.seq);
    for await (const frame of cursor.frames) void frame;
    expect(await outcome).toBe("cancelled");
    expect(writePath.rows("t")[0]?.[1]).toBe(0);
  });

  it("ends an autocommit RETURNING request whose durability fails", async () => {
    const { connect, writePath, governor } = harness();
    const client = await connect();
    await seed(client, 1);
    const commit = writePath.commit.bind(writePath);
    writePath.commit = (tx, request) => {
      const receipt = commit(tx, request);
      const durable = Promise.reject(new Error("durability failed"));
      durable.catch(() => undefined);
      return { ...receipt, durable };
    };
    let state = "pending";
    const query = client.query("UPDATE t SET n = n + 1 WHERE id = $1 RETURNING *", [1]).then(
      () => {
        state = "resolved";
      },
      (error: unknown) => {
        state = (error as Error).message;
      },
    );
    await sleep(40);
    const observed = state;
    client.close();
    await query;
    expect(observed).toBe("durability failed");
    await sleep(5);
    expect(governor.usedBytes).toBe(0);
  });
});
