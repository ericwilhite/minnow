/** Explicit export release is not result credit. Capabilities stay on their original connection;
 * cancellation/reconnect may discard queued frames, never consumer-held frames. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, OWNER_EXPORT_REACH } from "../../testing/reach.js";
import { ProtocolClient, type ExportFrame } from "../owner/client.js";
import { OwnerServer } from "../owner/server.js";
import type { ReturnPool } from "../exec/return-pool.js";
import { frameTransferList } from "../exec/result.js";
import { harness } from "./owner-harness.js";

async function link(server: OwnerServer, client: ProtocolClient, reconnect = false) {
  const { port1, port2 } = new MessageChannel();
  const frameSequences: number[] = [];
  const sent: string[] = [],
    waits: Array<() => void> = [];
  const post = port2.postMessage.bind(port2);
  port2.postMessage = (message: unknown, options?: Transferable[] | StructuredSerializeOptions) => {
    sent.push((message as { t?: string }).t ?? "");
    if (Array.isArray(options)) post(message, options);
    else post(message, options);
  };
  const send = port1.postMessage.bind(port1);
  port1.postMessage = (message: unknown, options?: Transferable[] | StructuredSerializeOptions) => {
    const reply = message as { frames?: Array<{ sequence: number }> };
    frameSequences.push(...(reply.frames ?? []).map((frame) => frame.sequence));
    if (Array.isArray(options)) send(message, options);
    else send(message, options);
  };
  server.accept(port1);
  const receive = port1.onmessage?.bind(port1);
  port1.onmessage = (event) => {
    receive?.(event);
    if ((event.data as { t?: string }).t === "ping") waits.shift()?.();
  };
  if (reconnect) await client.reconnect(port2);
  else await client.connect(port2);
  return {
    sent,
    frameSequences,
    barrier: () =>
      new Promise<void>((resolve) => {
        waits.push(resolve);
        post({ t: "ping" });
      }),
    close: () => {
      port1.close();
      port2.close();
    },
  };
}
function snapshot(frame: ExportFrame) {
  return frameTransferList(frame.payload).map((b) => new Uint8Array(b).slice());
}
function equal(frame: ExportFrame, expected: readonly Uint8Array[]) {
  const buffers = frameTransferList(frame.payload);
  expect(buffers.length).toBe(expected.length);
  buffers.forEach((b, i) => expect(new Uint8Array(b)).toEqual(expected[i]));
}
async function first(stream: ReturnType<ProtocolClient["exportStream"]>) {
  const entry = await stream.frames.next();
  if (entry.done) throw new Error("missing export frame");
  return entry.value;
}
async function fill(client: ProtocolClient, count: number, width: number, sparse: boolean) {
  await client.query("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, name TEXT)");
  for (let id = 1; id <= count; id++)
    await client.query("INSERT INTO t VALUES ($1, $2, $3)", [
      id,
      id * 3,
      sparse && id % 2 === 0 ? null : `水:${String(id)}:${"x".repeat(width)}`,
    ]);
}

for (const seed of seedsFor("owner-export-lifecycle", [0xb2e8, 0xb2e9]))
  it(`explicit export return histories (${String(seed)})`, async () => {
    const reach = createReach("owner export", OWNER_EXPORT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 7 }),
        fc.integer({ min: 0, max: 2048 }),
        fc.boolean(),
        fc.constantFrom("complete", "cancel", "close", "reconnect", "owner-change"),
        fc.boolean(),
        async (count, width, sparse, transition, loseReturn) => {
          reach.beginHistory();
          const original = harness({ rowsPerFrame: 1 });
          const environments = [original],
            client = new ProtocolClient();
          const links: Array<Awaited<ReturnType<typeof link>>> = [];
          const pools: ReturnPool[] = [];
          const observe = (environment: typeof original) => {
            const create = environment.engine.createReturnPool.bind(environment.engine);
            vi.spyOn(environment.engine, "createReturnPool").mockImplementation((cap) => {
              const pool = create(cap);
              pools.push(pool);
              return pool;
            });
          };
          observe(original);
          let active = original;
          let wire = await link(active.server, client);
          links.push(wire);
          const held: ExportFrame[] = [];
          try {
            await fill(client, count, width, sparse);
            const ordinary = await client.query("SELECT * FROM t");
            expect(ordinary.rows()).toHaveLength(count);
            expect(pools).toHaveLength(0);
            reach.reach("ordinary lifetime");
            const beforeFrames = wire.frameSequences.length;
            const stream = client.exportStream("SELECT * FROM t", [], { windowBytes: 1 });
            const old = await first(stream);
            held.push(old);
            const bytes = snapshot(old);
            expect(await stream.columns).toHaveLength(3);
            await wire.barrier(); // old frame has credit, but its views are still consumer-owned
            equal(old, bytes);
            expect(wire.frameSequences.slice(beforeFrames)).toContain(1);
            reach.reach("credit independent of return");
            reach.reach("held after credit");
            if (transition === "cancel") {
              stream.cancel();
              await expect(stream.done).rejects.toMatchObject({ code: "MINNOW_CANCELLED" });
              reach.reach("release after cancel");
            } else if (transition === "close") {
              stream.close();
              await expect(stream.done).resolves.toMatchObject({ closedEarly: true });
              reach.reach("early close");
            } else if (transition === "reconnect" || transition === "owner-change") {
              if (transition === "owner-change") {
                const stop = active.server.quiesce("closing");
                await stop.drained;
                stop.finish();
                active = harness({ rowsPerFrame: 1 });
                active.server = new OwnerServer(active.engine, {
                  ownerId: "owner-2",
                  epoch: 2,
                  build: "test",
                });
                environments.push(active);
                observe(active);
              }
              wire = await link(active.server, client, true);
              links.push(wire);
              await expect(stream.done).rejects.toMatchObject({ code: "MINNOW_SESSION_LOST" });
              expect(client.ownerId).toBe(transition === "owner-change" ? "owner-2" : "owner-1");
              if (transition === "owner-change") await fill(client, count, width, sparse);
              const fresh = client.exportStream("SELECT * FROM t", [], { windowBytes: 1 });
              const current = await first(fresh);
              held.push(current);
              await wire.barrier();
              const before = wire.sent.filter((t) => t === "returnBuffers").length;
              equal(old, bytes);
              old.release();
              old.release();
              await wire.barrier();
              expect(wire.sent.filter((t) => t === "returnBuffers")).toHaveLength(before);
              expect(frameTransferList(old.payload).every((b) => b.byteLength === 0)).toBe(true);
              reach.reach(
                transition === "reconnect"
                  ? "release after reconnect"
                  : "release after owner change",
              );
              current.release();
              for await (const frame of fresh.frames) frame.release();
              await fresh.done;
            } else {
              let lost: ExportFrame | undefined;
              for await (const frame of stream.frames) {
                if (loseReturn && lost === undefined) {
                  lost = frame;
                  held.push(frame);
                } else frame.release();
              }
              await expect(stream.done).resolves.toMatchObject({ rowCount: count });
              if (lost) {
                expect(frameTransferList(lost.payload).some((b) => b.byteLength > 0)).toBe(true);
                reach.reach("lost return does not block");
              }
            }
            if (transition !== "reconnect" && transition !== "owner-change") {
              equal(old, bytes);
              const oldViews = frameTransferList(old.payload).map((b) => new Uint8Array(b));
              old.release();
              await wire.barrier();
              const returns = wire.sent.filter((t) => t === "returnBuffers").length;
              old.release();
              await wire.barrier();
              expect(wire.sent.filter((t) => t === "returnBuffers")).toHaveLength(returns);
              for (const pool of pools) {
                const reused = pool.take(4);
                if (reused) new Uint8Array(reused).fill(0xa5);
              }
              expect(oldViews.every((view) => view.length === 0)).toBe(true);
              reach.reach("detached across reuse");
              reach.reach("double release");
            }
            expect(ordinary.rows()).toHaveLength(count);
            expect(pools.every((pool) => pool.evictableBytes <= pool.capBytes)).toBe(true);
            reach.reach(width < 100 ? "small frames" : "large frames");
          } finally {
            client.close();
            for (const frame of held) frame.release();
            for (const environment of environments) {
              const stop = environment.server.quiesce("closing");
              await stop.drained;
              stop.finish();
              await environment.engine.close();
              expect(environment.governor.usedBytes).toBe(0);
            }
            links.forEach((wire) => wire.close());
          }
        },
      ),
      { seed, numRuns: 64 },
    );
    reach.assertReached();
  }, 60_000);
