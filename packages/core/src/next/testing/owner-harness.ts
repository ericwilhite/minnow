/**
 * The in-process owner harness shared by the owner suites: the composed engine over the stand-ins
 * (owner-standins.ts), the protocol server, protocol clients over MessageChannels, and
 * hand-driven raw connections.
 */
import { afterEach } from "vitest";
import type { ChunkReads } from "../exec/scan.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { PROTOCOL_VERSION, type OwnerMessage, type WireValue } from "../owner/protocol.js";
import { OwnerServer, type ServerPort } from "../owner/server.js";
import {
  StandinExecutor,
  StandinPlanner,
  StandinWritePath,
  standinRules,
} from "./owner-standins.js";

export const cleanups: Array<() => void> = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
});

export interface HarnessOptions {
  readonly durability?: "strict" | "relaxed";
  readonly autoFlush?: boolean;
  readonly idleMs?: number;
  readonly abandonedStreamMs?: number;
  readonly liveness?: Map<string, AbortController>;
}

export function harness(options: HarnessOptions = {}) {
  const writePath = new StandinWritePath({
    durability: options.durability ?? "strict",
    autoFlush: options.autoFlush ?? true,
  });
  const executor = new StandinExecutor();
  const planner = new StandinPlanner();
  const governor = new Governor(64 * 1024 * 1024);
  const engine = composeEngine({
    planner,
    executor,
    writePath,
    governor,
    spill: new MemorySpillService(),
    reads: {} as ChunkReads,
    rules: standinRules,
    ...(options.idleMs === undefined ? {} : { transactionIdleTimeoutMs: options.idleMs }),
  });
  const liveness = options.liveness;
  const server = new OwnerServer(engine, {
    ownerId: "owner-1",
    epoch: 1,
    build: "test",
    ...(options.idleMs === undefined ? {} : { transactionIdleTimeoutMs: options.idleMs }),
    ...(options.abandonedStreamMs === undefined
      ? {}
      : { abandonedStreamMs: options.abandonedStreamMs }),
    ...(liveness === undefined
      ? {}
      : {
          watchLiveness: (lock: string) => {
            const controller = new AbortController();
            liveness.set(lock, controller);
            return { gone: controller.signal, stop: () => liveness.delete(lock) };
          },
        }),
  });
  const connect = async (client = new ProtocolClient()): Promise<ProtocolClient> => {
    const { port1, port2 } = new MessageChannel();
    server.accept(port1);
    cleanups.push(() => {
      port1.close();
      port2.close();
    });
    await client.connect(port2);
    cleanups.push(() => {
      client.close();
    });
    return client;
  };
  const reconnect = async (client: ProtocolClient): Promise<void> => {
    const { port1, port2 } = new MessageChannel();
    server.accept(port1);
    cleanups.push(() => {
      port1.close();
      port2.close();
    });
    await client.reconnect(port2);
  };
  return { writePath, executor, planner, governor, engine, server, connect, reconnect };
}

/** A hand-driven connection: messages delivered synchronously, replies recorded in order. */
export function raw(server: OwnerServer, clientId = "0".repeat(31) + "1", generation = 1) {
  const replies: OwnerMessage[] = [];
  const waiters: Array<() => void> = [];
  const port: ServerPort & { posted: number } = {
    posted: 0,
    onmessage: null,
    postMessage(message: unknown) {
      this.posted++;
      replies.push(message as OwnerMessage);
      for (const wake of waiters.splice(0)) wake();
    },
  };
  server.accept(port);
  const deliver = (message: unknown) => {
    port.onmessage?.({ data: structuredClone(message) } as MessageEvent);
  };
  deliver({
    t: "hello",
    protocol: PROTOCOL_VERSION,
    build: "test",
    clientId,
    livenessLock: `client:${clientId}`,
    previousOwnerId: null,
    generation,
  });
  let resolvedThrough = 0;
  const statement = (
    seq: number,
    text: string,
    params: WireValue[] = [],
    extra: Record<string, unknown> = {},
  ) => ({
    t: "statement",
    seq,
    sentAtMs: Date.now(),
    resolvedThrough,
    live: false,
    resendable: true,
    retry: false,
    generation,
    session: 0,
    text,
    prepared: null,
    knownColumnsVersion: -1,
    params,
    result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
    ...extra,
  });
  const reply = async (seq: number): Promise<OwnerMessage> => {
    for (;;) {
      const found = replies.find(
        (message) =>
          "seq" in message &&
          message.seq === seq &&
          (message.t === "done" ||
            message.t === "error" ||
            message.t === "prepared" ||
            message.t === "value"),
      );
      if (found !== undefined) return found;
      await new Promise<void>((resolve) => waiters.push(resolve));
    }
  };
  return {
    port,
    replies,
    deliver,
    statement,
    reply,
    acknowledge(through: number) {
      resolvedThrough = through;
    },
  };
}

export const flushTasks = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export async function seed(client: ProtocolClient, rows = 3): Promise<void> {
  await client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
  for (let id = 1; id <= rows; id++)
    await client.query("INSERT INTO t VALUES ($1, $2, $3)", [id, 0, `row ${String(id)}`]);
}
