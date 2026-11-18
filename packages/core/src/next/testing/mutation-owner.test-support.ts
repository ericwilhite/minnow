/** Real native storage/executor/ledger; only transport loss and phase scheduling are injected. */
import { expect } from "vitest";
import { StorageCore } from "./storage-core.js";
import { composeEngine } from "../owner/compose.js";
import { ProtocolClient, type ProtocolClientOptions } from "../owner/client.js";
import { OwnerServer, type ServerPort } from "../owner/server.js";
import type { OwnerMessage } from "../owner/protocol.js";
import { chunkReads } from "../exec/scan.js";

export function latch() {
  let release: () => void = () => undefined;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}
export function phaseGate() {
  const entered = latch(),
    resume = latch();
  return {
    entered: entered.promise,
    release: resume.release,
    async wait() {
      entered.release();
      await resume.promise;
    },
  };
}
export async function mutationOwner(
  transport: "direct" | "channel",
  count: number,
  options: ProtocolClientOptions = {},
) {
  const core = await new StorageCore(32).open();
  await core.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT NOT NULL CHECK(v>0))");
  if (count)
    await core.execute(
      `INSERT INTO t VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},1)`).join(",")}`,
    );
  const engine = composeEngine({
    planner: core.lowerer,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: chunkReads,
  });
  let now = Date.now();
  const server = new OwnerServer(engine, {
    ownerId: "mutation-native",
    epoch: 1,
    build: "test",
    now: () => now,
  });
  const client = new ProtocolClient({ ...options, now: () => now });
  let annotateError: ((error: unknown) => void) | null = null;
  const openSession = engine.openSession.bind(engine);
  engine.openSession = (...input) => {
    const session = openSession(...input),
      run = session.run.bind(session);
    session.run = (...input) => {
      const result = run(...input);
      if (!annotateError) return result;
      const poll = result.results.poll.bind(result.results),
        next = result.results.next.bind(result.results);
      result.results.poll = () => {
        try {
          return poll();
        } catch (error) {
          annotateError?.(error);
          throw error;
        }
      };
      result.results.next = async () => {
        try {
          return await next();
        } catch (error) {
          annotateError?.(error);
          throw error;
        }
      };
      void result.done.catch((error: unknown) => {
        annotateError?.(error);
      });
      return result;
    };
    return session;
  };
  const packets: OwnerMessage[] = [];
  const clientTrace: Array<{
    connection: number;
    t: string;
    sequence: number | null;
    target: number | null;
    terminalAtReturn: boolean;
  }> = [];
  const clients: Array<{ close(): void }> = [];
  let drop: ((message: OwnerMessage) => boolean) | null = null;
  let observer: ((message: OwnerMessage) => void) | null = null;
  let fence: ReturnType<typeof latch> | null = null;
  let armed: { phase: "end" | "commit"; gate: ReturnType<typeof phaseGate> } | null = null;
  const gates: Array<ReturnType<typeof phaseGate>> = [];
  const begin = core.harness.path.begin.bind(core.harness.path);
  core.harness.path.begin = async (input) => {
    const tx = await begin(input),
      statement = tx.statement.bind(tx),
      commit = tx.commit.bind(tx);
    tx.statement = (input) => {
      const s = statement(input),
        end = s.end.bind(s);
      s.end = async (signal) => {
        if (armed?.phase === "end") {
          const g = armed.gate;
          armed = null;
          await g.wait();
        }
        return end(signal);
      };
      return s;
    };
    tx.commit = async (request) => {
      if (armed?.phase === "commit") {
        const g = armed.gate;
        armed = null;
        await g.wait();
      }
      return commit(request);
    };
    return tx;
  };
  function link() {
    let ownerHandler: ServerPort["onmessage"] = null;
    let clientHandler: MessagePort["onmessage"] = null;
    let closed = false;
    let deliveredTerminal: number | null = null;
    const connection = clients.length + 1;
    const channel = transport === "channel" ? new MessageChannel() : null;
    const receive = (event: MessageEvent) => {
      clientHandler?.call(peer, event);
      const message = event.data as OwnerMessage;
      if (message.t === "done" || message.t === "error") deliveredTerminal = message.seq;
      // Fence actual client delivery, not the earlier owner-side postMessage call.
      if ((event.data as OwnerMessage).t === "alive") fence?.release();
    };
    const post = (value: unknown, transfer: Transferable[]) => {
      const message = value as OwnerMessage;
      packets.push(structuredClone(message));
      observer?.(message);
      if (drop?.(message)) {
        structuredClone(message, { transfer });
        return;
      }
      if (channel) channel.port1.postMessage(message, transfer);
      else if (!closed) receive({ data: structuredClone(message, { transfer }) } as MessageEvent);
    };
    const owner: ServerPort = {
      postMessage: post,
      get onmessage() {
        return ownerHandler;
      },
      set onmessage(value) {
        ownerHandler = value;
        if (channel) channel.port1.onmessage = value;
      },
      close() {
        closed = true;
        channel?.port1.close();
        channel?.port2.close();
      },
    };
    const peer = {
      get onmessage() {
        return clientHandler;
      },
      set onmessage(value: MessagePort["onmessage"]) {
        clientHandler = value;
        if (channel) channel.port2.onmessage = receive;
      },
      postMessage(message: unknown) {
        const header = message as { t: string; seq?: number; target?: number },
          trace = {
            connection,
            t: header.t,
            sequence: header.seq ?? null,
            target: header.target ?? null,
            terminalAtReturn: false,
          };
        clientTrace.push(trace);
        if (channel) channel.port2.postMessage(message);
        else if (!closed) ownerHandler?.({ data: structuredClone(message) } as MessageEvent);
        trace.terminalAtReturn = header.seq !== undefined && deliveredTerminal === header.seq;
      },
      close() {
        owner.close?.();
      },
    } as MessagePort;
    clients.push(peer);
    server.accept(owner);
    return peer;
  }
  return {
    core,
    engine,
    server,
    client,
    packets,
    clientTrace,
    connect: () => client.connect(link()),
    reconnect: () => client.reconnect(link()),
    drop(filter: ((message: OwnerMessage) => boolean) | null) {
      drop = filter;
    },
    observe(callback: ((message: OwnerMessage) => void) | null) {
      observer = callback;
    },
    /** Add test context to a REAL native failure, without replacing planning/execution. */
    annotateError(callback: ((error: unknown) => void) | null) {
      annotateError = callback;
    },
    advance(ms: number) {
      now += ms;
    },
    arm(phase: "end" | "commit") {
      const gate = phaseGate();
      gates.push(gate);
      armed = { phase, gate };
      return gate;
    },
    async fence() {
      const wait = latch();
      fence = wait;
      client.ping();
      await wait.promise;
      if (fence === wait) fence = null;
    },
    async rows() {
      const result = await core.execute("SELECT id,v FROM t ORDER BY id");
      if (!("batches" in result)) throw Error("rows");
      return result.batches.flatMap((b) =>
        Array.from({ length: b.length }, (_, i) =>
          b.columns.map((c) => {
            if (c.form !== "flat" || c.data.layout !== "i32") throw Error("integer row");
            return c.data.values[i];
          }),
        ),
      );
    },
    async close() {
      for (const g of gates) g.release();
      client.close();
      const stop = server.quiesce("closing");
      await stop.drained;
      stop.finish();
      for (const c of clients) c.close();
      await engine.close();
      await core.close();
      expect(core.harness.governor.usedBytes).toBe(0);
    },
  };
}
