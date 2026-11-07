/**
 * The point-read path's owner-side cost (ruling R6), measured in Node.
 *
 * - **Server alone** (a stub engine whose point read costs nothing): what the protocol server adds
 *   (envelope check, admission, the session queue, `runNow`, settle, the reply). Gated at 10 µs.
 * - **The full composition** over real lowering and the stand-in executor and write path, split per
 *   read into lowering, execution (the executor and what it calls), the write path (snapshot pins),
 *   and the rest: the server plus the composition (plan-cache hit, statistics check, admission,
 *   frames to the wire). The owner's share (the rest plus lowering) is gated at 50 µs; warm
 *   prepared and parameterized reads must not lower at all. Execution is reported, not gated: the
 *   stand-in has no key index (B3's is not on main), so its key lookup scans the table and its
 *   cost is not the real executor's. R6's absolute target for the whole read is judged once B3's
 *   write path and B4's executor replace the stand-ins.
 * - **Literal statements** (`WHERE id = 7`, a new text per key) are reported: past the plan cache
 *   every one is lowered again, which is what the split shows.
 *
 * Every read must take the synchronous path; the timing bounds are loose (shared machines), the
 * printed numbers are the measurement. The timing wrappers add well under a µs per call.
 */
import { describe, expect, it } from "vitest";
import type { ChunkReads } from "../exec/scan.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { createLowerer } from "../lower/create.js";
import { StandinExecutor, StandinWritePath, withTestStatements } from "./owner-standins.js";
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

/** Time spent inside a component's calls (the outermost call only, so nesting never counts twice). */
interface Meter {
  ms: number;
  calls: number;
}
const depth = { value: 0 };

function hasMethods(value: object): boolean {
  for (
    let o: object | null = value;
    o !== null && o !== Object.prototype;
    o = Object.getPrototypeOf(o) as object | null
  )
    for (const key of Object.getOwnPropertyNames(o))
      if (
        key !== "constructor" &&
        typeof Object.getOwnPropertyDescriptor(o, key)?.value === "function"
      )
        return true;
  return false;
}

/**
 * Wraps `target`'s methods to charge `meter`. `deep`: objects with methods that they return (a
 * prepared plan, an execution, its result channel) are wrapped too. Data (arrays, typed arrays,
 * maps, promises, bound parameters) is never wrapped: callers key WeakMaps by it.
 */
function metered<T extends object>(target: T, meter: Meter, deep: boolean): T {
  return new Proxy(target, {
    get(object, prop) {
      const value: unknown = Reflect.get(object, prop, object);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        const outer = depth.value === 0;
        depth.value++;
        const start = performance.now();
        try {
          const result: unknown = (value as (...values: unknown[]) => unknown).apply(object, args);
          const wrap =
            deep &&
            result !== null &&
            typeof result === "object" &&
            !(result instanceof Promise) &&
            !Array.isArray(result) &&
            !ArrayBuffer.isView(result) &&
            !(result instanceof Map) &&
            !(result instanceof Set) &&
            hasMethods(result);
          return wrap ? metered(result, meter, true) : result;
        } finally {
          depth.value--;
          if (outer) {
            meter.ms += performance.now() - start;
            meter.calls++;
          }
        }
      };
    },
  });
}

async function setup() {
  const meters = {
    lowering: { ms: 0, calls: 0 },
    execution: { ms: 0, calls: 0 },
    writePath: { ms: 0, calls: 0 },
  };
  const lowerer = withTestStatements(createLowerer());
  const writePath = new StandinWritePath();
  const executor = new StandinExecutor();
  const engine = composeEngine({
    planner: metered(lowerer, meters.lowering, false),
    executor: metered(executor, meters.execution, true),
    writePath: metered(writePath, meters.writePath, false),
    governor: new Governor(64 * 1024 * 1024),
    spill: new MemorySpillService(),
    reads: {} as ChunkReads,
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
    meters,
    executor,
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
    createReturnPool: () => {
      throw new Error("Point benchmark stub has no streaming output pool");
    },
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

  it("splits the composition's point read into lowering, execution, the write path and the owner's share", async () => {
    const { server, client, meters, executor, close } = await setup();
    const prepared = await client.prepare("SELECT * FROM t WHERE id = $1");

    // Owner side: a hand-driven port, messages delivered synchronously; replies released at once.
    let errors = 0;
    const port: ServerPort = {
      onmessage: null,
      postMessage: (message) => {
        if ((message as { t?: string }).t === "error") errors++;
      },
    };
    server.accept(port);
    const deliver = (data: unknown) => {
      port.onmessage?.({ data } as MessageEvent);
    };
    deliver({
      t: "hello",
      protocol: PROTOCOL_VERSION,
      build: "b",
      clientId: "e".repeat(32),
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
    let seq = 1;

    /** Runs `count` reads; returns per-read µs quantiles of each part (the warm last three quarters). */
    const run = (count: number, form: "prepared" | "text" | "literal") => {
      const parts = {
        total: [] as number[],
        lowering: [] as number[],
        execution: [] as number[],
        writePath: [] as number[],
        owner: [] as number[],
      };
      let lowerCalls = 0;
      let executorPrepares = 0;
      const synchronousBefore = server.stats.synchronous;
      for (let i = 0; i < count; i++) {
        seq++;
        const key = 1 + (i % ROWS);
        const before = {
          lowering: meters.lowering.ms,
          calls: meters.lowering.calls,
          execution: meters.execution.ms,
          writePath: meters.writePath.ms,
          prepares: executor.prepared,
        };
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
          text:
            form === "prepared"
              ? null
              : form === "text"
                ? "SELECT * FROM t WHERE id = $1"
                : `SELECT * FROM t WHERE id = ${String(key)}`,
          prepared: form === "prepared" ? 1 : null,
          knownColumnsVersion: server.engine.catalogVersion,
          params: form === "literal" ? [] : [key],
          result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
        };
        const start = performance.now();
        deliver(message);
        const total = (performance.now() - start) * 1000;
        deliver({ t: "release", target: seq, through: 0 });
        if (i < count / 4) continue;
        const lowering = (meters.lowering.ms - before.lowering) * 1000;
        const execution = (meters.execution.ms - before.execution) * 1000;
        const writePath = (meters.writePath.ms - before.writePath) * 1000;
        lowerCalls += meters.lowering.calls - before.calls;
        executorPrepares += executor.prepared - before.prepares;
        parts.total.push(total);
        parts.lowering.push(lowering);
        parts.execution.push(execution);
        parts.writePath.push(writePath);
        parts.owner.push(total - execution - writePath);
      }
      const p50 = (values: number[]) =>
        quantile(
          [...values].sort((a, b) => a - b),
          0.5,
        );
      return {
        synchronous: server.stats.synchronous - synchronousBefore,
        lowerCalls,
        executorPrepares,
        warm: parts.total.length,
        total: p50(parts.total),
        p99: quantile(
          [...parts.total].sort((a, b) => a - b),
          0.99,
        ),
        lowering: p50(parts.lowering),
        execution: p50(parts.execution),
        writePath: p50(parts.writePath),
        owner: p50(parts.owner),
      };
    };
    const line = (name: string, r: ReturnType<typeof run>) =>
      `${name}: p50 ${r.total.toFixed(1)} µs (p99 ${r.p99.toFixed(1)}) = owner's share ${r.owner.toFixed(1)} ` +
      `(lowering ${r.lowering.toFixed(1)}, ${String(r.lowerCalls)} lower calls in ${String(r.warm)} warm reads) ` +
      `+ execution ${r.execution.toFixed(1)} (stand-in, scans ${String(ROWS)} rows) + write path ${r.writePath.toFixed(1)}`;

    const preparedReads = run(READS, "prepared");
    const textReads = run(READS / 2, "text");
    const literalReads = run(READS / 4, "literal");
    expect(errors).toBe(0);
    // Every prepared read takes the synchronous path; no warm read is lowered again.
    expect(preparedReads.synchronous).toBe(READS);
    expect(preparedReads.lowerCalls).toBe(0);
    expect(textReads.lowerCalls).toBe(0);
    // …and none is prepared again by the executor (the composition caches it per plan).
    expect(preparedReads.executorPrepares).toBe(0);
    expect(textReads.executorPrepares).toBe(0);
    // A literal per key outgrows the plan cache: every warm read is lowered.
    expect(literalReads.lowerCalls).toBe(literalReads.warm);
    expect(server.stats.outstandingFrames).toBe(0);

    // Reference: a whole request/response over an in-process MessageChannel.
    const rtt: number[] = [];
    for (let i = 0; i < 2_000; i++) {
      const start = performance.now();
      await client.execute(prepared.id, [1 + (i % ROWS)], 0, server.engine.catalogVersion);
      rtt.push((performance.now() - start) * 1000);
    }
    rtt.sort((a, b) => a - b);
    console.log(
      [
        line("prepared point read", preparedReads),
        line("parameterized text", textReads),
        line("literal per key", literalReads),
        `in-process MessageChannel round trip (prepared): p50 ${quantile(rtt, 0.5).toFixed(1)} µs, p99 ${quantile(rtt, 0.99).toFixed(1)} µs`,
      ].join("\n"),
    );
    expect(preparedReads.owner).toBeLessThan(50);
    expect(textReads.owner).toBeLessThan(50);
    close();
  }, 120_000);
});
