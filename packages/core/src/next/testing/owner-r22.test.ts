/**
 * Ruling R22: tiny result messages are cloned, not transferred (R22.1), and releases ride on the
 * next request or go alone after a short delay or at half the window (R22.2).
 */
import { describe, expect, it } from "vitest";
import { ProtocolClient } from "../owner/client.js";
import { TRANSFER_MIN_BYTES, transferFor } from "../owner/frame.js";
import { PROTOCOL_VERSION, RELEASE_DELAY_MS, type OwnerMessage } from "../owner/protocol.js";
import type { ServerPort } from "../owner/server.js";
import { harness, seed, sleep } from "./owner-harness.js";

/** A server-side port that records each message with the transfer list it was posted with. */
function recordingPort(server: ReturnType<typeof harness>["server"]) {
  const posted: Array<{ message: OwnerMessage; transfer: readonly unknown[] }> = [];
  const port: ServerPort = {
    onmessage: null,
    postMessage(message: unknown, transfer: Transferable[]) {
      posted.push({ message: message as OwnerMessage, transfer });
    },
  };
  server.accept(port);
  const deliver = (data: unknown) => {
    port.onmessage?.({ data: structuredClone(data) } as MessageEvent);
  };
  deliver({
    t: "hello",
    protocol: PROTOCOL_VERSION,
    build: "t",
    clientId: "a".repeat(32),
    livenessLock: "",
    previousOwnerId: null,
    generation: 1,
  });
  const statement = (seq: number, text: string, params: unknown[] = [], extra: object = {}) => ({
    t: "statement",
    seq,
    sentAtMs: Date.now(),
    resolvedThrough: seq - 1,
    live: false,
    resendable: true,
    retry: false,
    generation: 1,
    session: 0,
    text,
    prepared: null,
    knownColumnsVersion: -1,
    params,
    result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
    ...extra,
  });
  return { posted, deliver, statement };
}

/** A client-side fake owner: answers hello, records what the client sends. */
function fakeOwner() {
  const sent: Array<Record<string, unknown>> = [];
  const port = {
    onmessage: null as ((event: { data: unknown }) => void) | null,
    close: () => undefined,
    postMessage(message: Record<string, unknown>) {
      sent.push(message);
      if (message.t === "hello")
        queueMicrotask(() =>
          port.onmessage?.({ data: { t: "welcome", ownerId: "o", protocol: PROTOCOL_VERSION } }),
        );
    },
  };
  const done = (seq: number, frames: unknown[]) => {
    port.onmessage?.({
      data: {
        t: "done",
        seq,
        columns: [],
        frames,
        summary: {
          command: "SELECT",
          rowCount: 1,
          counts: null,
          lsn: null,
          replayed: false,
          closedEarly: false,
          transaction: "idle",
          catalogVersion: 1,
        },
      },
    });
  };
  return { port: port as unknown as MessagePort, sent, done };
}

const frame = (sequence: number, bytes: number) => ({
  sequence,
  rowCount: 1,
  dictionaries: [],
  columns: [
    {
      form: "flat",
      length: 1,
      validity: null,
      data: { layout: "f64", values: new Float64Array(bytes / 8) },
    },
  ],
});

describe("R22.1: tiny frames are cloned", () => {
  it("pins the threshold", () => {
    expect(TRANSFER_MIN_BYTES).toBe(1024);
    const small = [new ArrayBuffer(512), new ArrayBuffer(511)];
    const large = [new ArrayBuffer(512), new ArrayBuffer(512)];
    expect(transferFor(small)).toEqual([]);
    expect(transferFor(large)).toEqual(large);
  });

  it("clones a point read's frame and transfers a large result", async () => {
    const { server, connect } = harness();
    const client = await connect();
    await seed(client, 1);
    await client.query("INSERT INTO t VALUES ($1, $2, $3)", [2, 0, "x".repeat(4096)]);
    const port = recordingPort(server);
    port.deliver(port.statement(1, "SELECT * FROM t WHERE id = $1", [1]));
    port.deliver(port.statement(2, "SELECT * FROM t"));
    await sleep(10);
    const of = (seq: number) =>
      port.posted.filter(
        ({ message }) => "seq" in message && message.seq === seq && message.t === "done",
      );
    expect(of(1)[0]?.transfer).toEqual([]);
    expect(of(2)[0]?.transfer.length).toBeGreaterThan(0);
  });
});

describe("R22.2: releases ride on the next request", () => {
  it("attaches a pending release to the next request instead of sending it alone", async () => {
    const owner = fakeOwner();
    const client = new ProtocolClient();
    await client.connect(owner.port);
    const first = client.query("SELECT 1");
    const firstSeq = owner.sent.at(-1)?.seq as number;
    owner.done(firstSeq, [frame(0, 64)]);
    await first;
    const second = client.query("SELECT 2").catch(() => undefined);
    const request = owner.sent.at(-1);
    expect(request?.releases).toEqual([{ target: firstSeq, through: 0 }]);
    await sleep(RELEASE_DELAY_MS + 10);
    expect(owner.sent.filter((message) => message.t === "release")).toEqual([]);
    client.close();
    await second;
  });

  it("sends a release alone after the delay when no request follows", async () => {
    const owner = fakeOwner();
    const client = new ProtocolClient();
    await client.connect(owner.port);
    const query = client.query("SELECT 1");
    const seq = owner.sent.at(-1)?.seq as number;
    owner.done(seq, [frame(0, 64)]);
    await query;
    expect(owner.sent.filter((message) => message.t === "release")).toEqual([]);
    await sleep(RELEASE_DELAY_MS + 10);
    expect(owner.sent.filter((message) => message.t === "release")).toEqual([
      { t: "release", target: seq, through: 0 },
    ]);
    client.close();
  });

  it("sends a release at once when half the window is unreleased", async () => {
    const owner = fakeOwner();
    const client = new ProtocolClient();
    await client.connect(owner.port);
    const query = client.query("SELECT 1", [], { windowBytes: 4096 });
    const seq = owner.sent.at(-1)?.seq as number;
    owner.done(seq, [frame(0, 2048)]);
    await query;
    expect(owner.sent.filter((message) => message.t === "release")).toEqual([
      { t: "release", target: seq, through: 0 },
    ]);
    client.close();
  });

  it("applies releases carried by a request on the owner", async () => {
    const { server, connect } = harness();
    await seed(await connect(), 1);
    const port = recordingPort(server);
    port.deliver(port.statement(1, "SELECT * FROM t WHERE id = $1", [1]));
    expect(server.stats.outstandingFrames).toBe(1);
    port.deliver(
      port.statement(2, "SELECT * FROM t WHERE id = $1", [1], {
        releases: [{ target: 1, through: 0 }],
      }),
    );
    // Frame 0 of request 1 is released; request 2's own frame is now the one outstanding.
    expect(server.stats.outstandingFrames).toBe(1);
    port.deliver({ t: "release", target: 2, through: 0 });
    expect(server.stats.outstandingFrames).toBe(0);
  });
});
