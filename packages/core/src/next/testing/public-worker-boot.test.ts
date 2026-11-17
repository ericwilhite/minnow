/** Genuine worker + MessageChannel over the same production native host factory. Tests use
 * a Web Locks platform double in Node; storage, recovery, spill and execution are real. */
import { Worker, MessageChannel as NodeMessageChannel } from "node:worker_threads";
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_WORKER_BOOT_REACH } from "../../testing/reach.js";
import { ProtocolClient } from "../owner/client.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
const workers = new Set<Worker>();
afterEach(async () => {
  for (const worker of workers) await worker.terminate();
  workers.clear();
});
const source = `
(async () => {
 const {parentPort,workerData} = await import('node:worker_threads');
 const {installNativeOwnerWorker} = await import(workerData.boot);
 const {MemoryBackend} = await import(workerData.memory);
 const {FakeLockRegistry} = await import(workerData.locks);
 const backend = new MemoryBackend(), registry = new FakeLockRegistry();
 const scope={onmessage:null,postMessage:status=>parentPort.postMessage(status)};
 const service=installNativeOwnerWorker(scope,()=>backend,{memoryLimitBytes:32<<20},{locks:registry.context()});
 parentPort.on('message', async message => {
  try {
   scope.onmessage?.({data:message});
   if(message.t === 'minnow:stepDown') {
    await service.close();const host=await service.ready;
    parentPort.postMessage({t:'drained',used:host.engine.options.governor.usedBytes,
      spill:host.engine.options.spill.bytesInUse,locks:registry.query()});
   }
  } catch(error) {parentPort.postMessage({t:'failure',message:String(error),stack:error.stack});}
 });
})().catch(error=>{throw error;});`;

for (const seed of seedsFor("next-public-worker-boot", [0xb742, 0xb743]))
  it(`serves canonical native results from a real worker (${String(seed)})`, async () => {
    const reach = createReach("native worker boot", PUBLIC_WORKER_BOOT_REACH);
    const worker = new Worker(source, {
      eval: true,
      workerData: {
        boot: new URL("../../../dist/next/owner/native-worker.js", import.meta.url).href,
        memory: new URL("../../../dist/next/storage/memory-store.js", import.meta.url).href,
        locks: new URL("../../../dist/next/testing/fake-locks.js", import.meta.url).href,
      },
    });
    workers.add(worker);
    const channel = new NodeMessageChannel(),
      protocol = new ProtocolClient();
    let readyResolve: () => void = () => undefined,
      readyReject: (error: Error) => void = () => undefined;
    const ready = new Promise<void>((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
    });
    let drainedResolve: (result: unknown) => void = () => undefined;
    const drained = new Promise<unknown>((resolve) => {
      drainedResolve = resolve;
    });
    worker.on("error", (error) => {
      readyReject(error instanceof Error ? error : new Error(String(error)));
    });
    worker.on("message", (message: unknown) => {
      const event = message as { t?: string; message?: string };
      if (event.t === "minnow:ready") readyResolve();
      if (event.t === "drained") drainedResolve(message);
      if (event.t === "failure" || event.t === "minnow:failed")
        readyReject(new Error(event.message ?? JSON.stringify(message)));
    });
    ready.catch(() => undefined);
    const db = new MinnowDatabaseClient(protocol, { ready });
    try {
      worker.postMessage(
        {
          t: "minnow:host",
          database: `native-worker-${String(seed)}`,
          build: "test",
          clientPort: channel.port2,
          brokerPort: null,
          settings: { memoryLimitBytes: 32 << 20 },
        },
        [channel.port2],
      );
      await Promise.all([protocol.connect(channel.port1 as unknown as MessagePort), ready]);
      await db.execute("CREATE TABLE worker_rows(id INT PRIMARY KEY,n BIGINT,v TEXT)");
      await fc.assert(
        fc.asyncProperty(
          fc.bigInt({ min: -(1n << 60n), max: 1n << 60n }),
          fc.string({ maxLength: 40 }),
          async (value, text) => {
            reach.beginHistory();
            await db.execute("DELETE FROM worker_rows");
            await db.execute("INSERT INTO worker_rows VALUES(1,$1,$2)", [value, text]);
            const answer = await db.query(
              "SELECT n,v,TIMESTAMP '2000-01-01 00:00:00.123456' ts FROM worker_rows",
            );
            expect(answer.tuples()).toEqual([[value, text, "2000-01-01T00:00:00.123456"]]);
            reach.reach("worker native SQL and exact decode");
            await db.transaction(async (tx) => {
              await tx.execute("INSERT INTO worker_rows VALUES(2,$1,$2)", [value + 1n, text]);
              expect((await db.query("SELECT count(*) FROM worker_rows")).values(0)).toEqual([1]);
            });
            reach.reach("worker isolated transaction");
            const cursor = db.cursor("SELECT i::bigint FROM generate_series(1,7) s(i)", [], {
              frameRows: 1,
              prefetchBytes: 1,
            });
            const first = await cursor.next();
            await cursor.close();
            expect(first.done ? null : first.value.values(0)).toEqual([1n]);
            reach.reach("worker cursor close retains views");
          },
        ),
        { seed, numRuns: 8, examples: [[9007199254740993n, "水"]] },
      );
      await db.close();
      worker.postMessage({ t: "minnow:stepDown" });
      expect(await drained).toMatchObject({ used: 0, spill: 0, locks: { held: [], pending: [] } });
      reach.reach("worker complete native teardown");
      reach.assertReached();
    } finally {
      await db.close();
      channel.port1.close();
      await worker.terminate();
      workers.delete(worker);
    }
  }, 60000);
