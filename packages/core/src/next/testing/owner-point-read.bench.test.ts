/**
 * The point-read path's owner-side cost (ruling R6: owner-side warm PK read ≤ 10 µs), measured in
 * Node over the composition with stand-in engines, so it measures what this track adds: envelope
 * check, plan-cache hit, admission, the session queue, `runNow`, settle, the reply. The stand-in
 * executor's own work is a Map lookup. The cross-tab round trip is the browser lane's (increment
 * 2); the in-process MessageChannel round trip here is reported for reference only.
 *
 * Every read must take the synchronous path; the timing bound is loose (shared machines), the
 * printed numbers are the measurement.
 */
import { describe, expect, it } from "vitest";
import type { ChunkReads } from "../exec/scan.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  StandinExecutor,
  StandinPlanner,
  StandinWritePath,
  standinRules,
} from "./owner-standins.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import type { EngineSession, OwnerEngine, PlannedStatement, SyncRun } from "../owner/engine.js";
import { RequestLedger } from "../format/ledger.js";
import { PROTOCOL_VERSION } from "../owner/protocol.js";
import { OwnerServer, type ServerPort } from "../owner/server.js";

const ROWS = 1_000;
const READS = 20_000;

function quantile(sorted: readonly number[], q: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? Number.NaN;
}

async function setup() {
  const writePath = new StandinWritePath();
  const engine = composeEngine({
    planner: new StandinPlanner(),
    executor: new StandinExecutor(),
    writePath,
    governor: new Governor(64 * 1024 * 1024),
    spill: new MemorySpillService(),
    reads: {} as ChunkReads,
    rules: standinRules,
  });
  const server = new OwnerServer(engine, { ownerId: "bench", epoch: 1, build: "bench" });
  const { port1, port2 } = new MessageChannel();
  server.accept(port1);
  const client = new ProtocolClient();
  await client.connect(port2);
  await client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
  for (let id = 1; id <= ROWS; id++)
    await client.query("INSERT INTO t VALUES ($1, $2, $3)", [id, id, `name ${String(id)}`]);
  return {
    server,
    client,
    close: () => {
      client.close();
      port1.close();
      port2.close();
    },
  };
}

/** An engine whose point read costs nothing: what remains is the server's own work. */
function stubEngine(): OwnerEngine {
  const columns = [
    {
      name: "id",
      type: { kind: "int32", bits: 32 } as const,
      nullable: false,
      sqlType: "integer",
      render: null,
    },
  ];
  const planned = {
    text: "",
    kind: "query",
    columns,
    parameters: [],
    controlsTransaction: false,
    control: null,
    catalogVersion: 1,
    pointRead: true,
    lowered: {} as PlannedStatement["lowered"],
  } satisfies PlannedStatement;
  const values = new Int32Array(1);
  const answer: SyncRun = {
    columns,
    outcome: { command: "SELECT", rowCount: 1, counts: null, lsn: null },
    frames: [
      {
        payload: {
          sequence: 0,
          rowCount: 1,
          dictionaries: [],
          columns: [{ form: "flat", length: 1, validity: null, data: { layout: "i32", values } }],
        },
        transfer: [],
        byteLength: 4,
        requiresLsn: 0,
        release: () => undefined,
      },
    ],
  };
  const session = {
    transaction: "idle",
    readOnly: false,
    lastCommitLsn: null,
    catalogVersion: 1,
    plan: () => planned,
    runNow: () => answer,
    close: () => undefined,
  } as unknown as EngineSession;
  return {
    durability: "strict",
    catalogVersion: 1,
    ledger: new RequestLedger(),
    plan: () => planned,
    durable: () => null,
    openSession: () => session,
    readSet: () => [],
    onCommit: () => () => undefined,
    admin: () => Promise.resolve(null),
    flushPending: () => Promise.resolve(),
    close: () => Promise.resolve(),
  };
}

function ownerSideMicros(
  server: OwnerServer,
  preparedText: string,
  catalogVersion: number,
  keys: number,
): number[] {
  const port: ServerPort = { onmessage: null, postMessage: () => undefined };
  server.accept(port);
  const deliver = (data: unknown) => {
    port.onmessage?.({ data } as MessageEvent);
  };
  deliver({
    t: "hello",
    protocol: PROTOCOL_VERSION,
    build: "b",
    clientId: "f".repeat(32),
    livenessLock: "",
    previousOwnerId: null,
    generation: 1,
  });
  deliver({
    t: "prepare",
    seq: 1,
    sentAtMs: Date.now(),
    resolvedThrough: 0,
    live: false,
    resendable: true,
    retry: false,
    generation: 1,
    id: 1,
    text: preparedText,
  });
  const times: number[] = [];
  for (let seq = 2; seq < READS + 2; seq++) {
    const message = {
      t: "statement",
      seq,
      sentAtMs: Date.now(),
      resolvedThrough: seq - 1,
      live: false,
      resendable: true,
      retry: false,
      generation: 1,
      session: 0,
      text: null,
      prepared: 1,
      knownColumnsVersion: catalogVersion,
      params: [1 + (seq % keys)],
      result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
    };
    const start = performance.now();
    deliver(message);
    times.push((performance.now() - start) * 1000);
    deliver({ t: "release", target: seq, through: 0 });
  }
  return times.slice(READS / 4).sort((a, b) => a - b);
}

describe("point read (R6)", () => {
  it("adds a few µs of server work per point read, all on the synchronous path", () => {
    const server = new OwnerServer(stubEngine(), { ownerId: "bench", epoch: 1, build: "bench" });
    const times = ownerSideMicros(server, "SELECT", 1, 1);
    expect(server.stats.synchronous).toBe(READS);
    expect(server.stats.outstandingFrames).toBe(0);
    console.log(
      `server alone (stub engine): p50 ${quantile(times, 0.5).toFixed(2)} µs, p99 ${quantile(times, 0.99).toFixed(2)} µs`,
    );
    expect(quantile(times, 0.5)).toBeLessThan(10);
  }, 60_000);

  it("measures the full composition over the stand-ins, and the in-process round trip", async () => {
    const { server, client, close } = await setup();
    const prepared = await client.prepare("SELECT * FROM t WHERE id = $1");

    // Owner side: a hand-driven port, messages delivered synchronously; replies released at once.
    const replies: unknown[] = [];
    const port: ServerPort = {
      onmessage: null,
      postMessage: (message) => {
        replies.push(message);
      },
    };
    server.accept(port);
    const deliver = (data: unknown) => {
      port.onmessage?.({ data } as MessageEvent);
    };
    const clientId = "e".repeat(32);
    deliver({
      t: "hello",
      protocol: PROTOCOL_VERSION,
      build: "b",
      clientId,
      livenessLock: "",
      previousOwnerId: null,
      generation: 1,
    });
    deliver({
      t: "prepare",
      seq: 1,
      sentAtMs: Date.now(),
      resolvedThrough: 0,
      live: false,
      resendable: true,
      retry: false,
      generation: 1,
      id: 1,
      text: "SELECT * FROM t WHERE id = $1",
    });
    void prepared;
    const synchronousBefore = server.stats.synchronous;
    const times: number[] = [];
    let seq = 1;
    for (let i = 0; i < READS; i++) {
      seq++;
      const message = {
        t: "statement",
        seq,
        sentAtMs: Date.now(),
        resolvedThrough: seq - 1,
        live: false,
        resendable: true,
        retry: false,
        generation: 1,
        session: 0,
        text: null,
        prepared: 1,
        knownColumnsVersion: server.engine.catalogVersion,
        params: [1 + (i % ROWS)],
        result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
      };
      const start = performance.now();
      deliver(message);
      times.push((performance.now() - start) * 1000);
      deliver({ t: "release", target: seq, through: 0 });
      replies.length = 0;
    }
    expect(server.stats.synchronous - synchronousBefore).toBe(READS);
    expect(server.stats.outstandingFrames).toBe(0);
    const warm = times.slice(READS / 4).sort((a, b) => a - b);
    const ownerP50 = quantile(warm, 0.5);
    const ownerP99 = quantile(warm, 0.99);

    // Reference: a whole request/response over an in-process MessageChannel.
    const rtt: number[] = [];
    for (let i = 0; i < 2_000; i++) {
      const start = performance.now();
      await client.execute(prepared.id, [1 + (i % ROWS)], 0, server.engine.catalogVersion);
      rtt.push((performance.now() - start) * 1000);
    }
    rtt.sort((a, b) => a - b);
    console.log(
      `owner-side point read: p50 ${ownerP50.toFixed(1)} µs, p99 ${ownerP99.toFixed(1)} µs; ` +
        `in-process MessageChannel round trip: p50 ${quantile(rtt, 0.5).toFixed(1)} µs, p99 ${quantile(rtt, 0.99).toFixed(1)} µs`,
    );
    expect(ownerP50).toBeLessThan(50);
    close();
  }, 60_000);
});
