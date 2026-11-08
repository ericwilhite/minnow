/** Worker for owner-point-read-cpu.mts; synchronous requests, no wall-clock observations. */
import { Session } from "node:inspector/promises";
import { writeFileSync } from "node:fs";
import assert from "node:assert/strict";
import { Governor } from "../packages/core/src/next/memory/governor.js";
import { MemorySpillService } from "../packages/core/src/next/spill/memory-spill.js";
import { createLowerer } from "../packages/core/src/next/lower/create.js";
import {
  StandinExecutor,
  StandinWritePath,
  withTestStatements,
} from "../packages/core/src/next/testing/owner-standins.js";
import { ProtocolClient } from "../packages/core/src/next/owner/client.js";
import { composeEngine } from "../packages/core/src/next/owner/compose.js";
import { OwnerServer, type ServerPort } from "../packages/core/src/next/owner/server.js";
import type { ChunkReads } from "../packages/core/src/next/exec/scan.js";
import { frameTransferList } from "../packages/core/src/next/exec/frame-buffers.js";
import { PROTOCOL_VERSION } from "../packages/core/src/next/owner/protocol.js";
const ROWS = 100;
async function setup() {
  const lowerer = withTestStatements(createLowerer());
  const writePath = new StandinWritePath();
  const executor = new StandinExecutor();
  const engine = composeEngine({
    planner: lowerer,
    executor: executor,
    writePath: writePath,
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
    executor,
    close: () => {
      client.close();
      port1.close();
      port2.close();
    },
  };
}

if (typeof process.threadCpuUsage !== "function")
  throw new Error("CPU benchmark needs Node with threadCpuUsage (22.19+)");
const mode = process.argv[2] ?? "full";
const { server, close } = await setup();
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
function one() {
  seq++;
  deliver({
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
    params: [1 + (seq % 100)],
    result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
  });
  deliver({ t: "release", target: seq, through: 0 });
}

const payload = {
  rowCount: 1,
  dictionaries: [],
  columns: [
    {
      form: "flat" as const,
      length: 1,
      validity: null,
      data: { layout: "i32" as const, values: new Int32Array([1]) },
    },
    {
      form: "flat" as const,
      length: 1,
      validity: null,
      data: {
        layout: "varlen" as const,
        bytes: new Uint8Array([120]),
        offsets: new Int32Array([0, 1]),
      },
    },
  ],
};
const wide = {
  ...payload,
  rowCount: 16,
  columns: Array.from({ length: 200 }, () => ({
    form: "flat" as const,
    length: 16,
    validity: null,
    data: { layout: "i32" as const, values: new Int32Array(16) },
  })),
};
const framePayload = mode === "frame-wide" ? wide : payload;
const step = mode.startsWith("frame")
  ? () => {
      const b = frameTransferList(framePayload);
      assert.equal(b.length, mode === "frame-wide" ? 200 : 3);
    }
  : one;
for (let i = 0; i < 10000; i++) step();
const samples: number[] = [];
const iterations = mode.startsWith("frame") ? 10000 : 1000;
for (let block = 0; block < 30; block++) {
  const before = process.threadCpuUsage();
  for (let i = 0; i < iterations; i++) step();
  const c = process.threadCpuUsage(before);
  samples.push((c.user + c.system) / iterations);
}
samples.sort((a, b) => a - b);
let components: Record<string, number> | null = null;
if (process.argv[3]) {
  const inspector = new Session();
  inspector.connect();
  await inspector.post("Profiler.enable");
  await inspector.post("Profiler.setSamplingInterval", { interval: 100 });
  await inspector.post("Profiler.start");
  for (let i = 0; i < 100000; i++) one();
  const { profile } = await inspector.post("Profiler.stop");
  writeFileSync(process.argv[3], JSON.stringify(profile));
  inspector.disconnect();
  const nodes = new Map(profile.nodes.map((n) => [n.id, n]));
  const parents = new Map<number, number>();
  for (const n of profile.nodes) for (const child of n.children ?? []) parents.set(child, n.id);
  components = { bind: 0, "write-path-probe": 0, "compiled-plan-cache": 0 };
  for (const id of profile.samples ?? []) {
    const names: string[] = [];
    let at: number | undefined = id;
    while (at !== undefined) {
      names.push(nodes.get(at)?.callFrame.functionName ?? "");
      at = parents.get(at);
    }
    const has = (pattern: RegExp) => names.some((n) => pattern.test(n));
    const bucket = has(/garbage collector/)
      ? "gc-unattributed"
      : has(/frameTransferList|frameByteLength|^#frame$|^flats$/)
        ? "frame"
        : has(/columnVector|payloadOf|#encode/)
          ? "fixture-frame-build"
          : has(/boundParameters|bindParameterized|bindParameters/)
            ? "bind"
            : has(/^pin$|StandinSnapshot|releasePin|#pinOptions/)
              ? "write-path-probe"
              : has(/^prepare$/)
                ? "compiled-plan-cache"
                : has(/rowsOf|interpret|evaluate|compareValues|StandinExecution|^execute$/)
                  ? "standin-execution"
                  : "owner-dispatch-compose";
    components[bucket] = (components[bucket] ?? 0) + 1;
  }
  components["SQL-text-plan-cache"] = 0;
}
assert.equal(errors, 0);
assert.equal(server.stats.outstandingFrames, 0);
console.log(
  JSON.stringify({
    mode,
    cpuUsPerOperationP50: samples[15],
    samples,
    components,
    errors,
    synchronous: server.stats.synchronous,
    limits:
      "p50 of amortized CPU batches, not individual-call latency; profile sample counts not wall durations; stand-in execution and binding, no real index/parameter-vector execution",
  }),
);
close();
process.exit(0);
