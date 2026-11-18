/** Byte ownership and suspended/cancelled handle histories over real MessageChannels.
 * Native format semantics live in snapshot-owner.test.ts; this isolates transport ownership. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import type { Grant } from "../memory/governor.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_TRANSPORT_REACH } from "../../testing/reach.js";
import { OwnerServer, type ServerPort } from "../owner/server.js";
import { ProtocolClient } from "../owner/client.js";
import type { SnapshotSink } from "../format/snapshot.js";
import { harness } from "./owner-harness.js";

function equalBytes(a: Uint8Array, b: Uint8Array | undefined): void {
  if (b === undefined) throw new Error("Source was not initialized");
  let same = a.length === b.length;
  for (let i = 0; same && i < a.length; i++) same = a[i] === b[i];
  expect(same).toBe(true);
}

for (const seed of seedsFor("next-snapshot-transport", [0xb2e7, 0xb2e8]))
  it(`dedicated bounded archive chunks and handle cleanup ${String(seed)}`, async () => {
    const reach = createReach("snapshot transport", SNAPSHOT_TRANSPORT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: (1 << 20) + 31 }),
        fc.constantFrom("all", "break", "cancel", "reconnect", "owner change"),
        async (length, mode) => {
          reach.beginHistory();
          const h = harness(),
            ports: MessagePort[] = [],
            servers: OwnerServer[] = [];
          let borrowed: Uint8Array | undefined, signalClose!: () => void;
          const closed = new Promise<void>((resolve) => {
            signalClose = resolve;
          });
          let transfers = 0;
          let transport: Grant | undefined;
          const admit = h.governor.admitGroup.bind(h.governor);
          const admission = vi
            .spyOn(h.governor, "admitGroup")
            .mockImplementation(async (options) => {
              const grants = await admit(options);
              transport = grants.find((grant) => grant.label === "snapshot transport");
              return grants;
            });
          // Explicitly invoked with .call(this) below, preserving the native receiver.
          // eslint-disable-next-line @typescript-eslint/unbound-method
          const slice = Uint8Array.prototype.slice;
          const allocation = vi.spyOn(Uint8Array.prototype, "slice").mockImplementation(function (
            this: Uint8Array,
            start,
            end,
          ) {
            if (this === borrowed) {
              expect(transport?.allocated).toBeGreaterThanOrEqual((1 << 20) + 1024);
              reach.reach("precharged transfer copies");
            }
            return slice.call(this, start, end);
          });

          const operations = {
            governor: h.governor,
            importGrants: [],
            exportMinimumBytes: length + 64,
            async export(sink: SnapshotSink, signal: AbortSignal, grant: Grant) {
              signal.throwIfAborted();
              try {
                expect(grant.allocate(length + 64)).toBe(true);
                borrowed = Uint8Array.from({ length }, (_, n) => (n * 31 + length) & 255);
                await sink.write(borrowed);
              } finally {
                grant.free(length + 64);
                signalClose();
              }
            },
            import: async () => {
              throw new Error("Not used by this ownership law");
            },
          };
          const server = (epoch: number) => {
            const server = new OwnerServer(h.engine, {
              ownerId: `transport-${String(epoch)}`,
              epoch,
              build: "test",
              snapshots: operations,
            });
            servers.push(server);
            return server;
          };
          const firstServer = server(1);
          const port = (server: OwnerServer) => {
            const pair = new MessageChannel();
            ports.push(pair.port1, pair.port2);
            const wire: ServerPort = {
              get onmessage() {
                return pair.port1.onmessage;
              },
              set onmessage(value) {
                pair.port1.onmessage = value;
              },
              close: () => pair.port1.close(),
              postMessage(message, buffers) {
                const reply = message as { t: string; value?: unknown };
                if (reply.t === "value" && reply.value instanceof Uint8Array) {
                  expect(buffers).toHaveLength(1);
                  expect(buffers[0] === reply.value.buffer).toBe(true);
                  expect(reply.value.buffer === borrowed?.buffer).toBe(false);
                  expect(borrowed?.byteLength).toBe(length);
                  transfers++;
                }
                pair.port1.postMessage(message, buffers);
                if (reply.t === "value" && reply.value instanceof Uint8Array)
                  expect(reply.value.byteLength).toBe(0);
              },
            };
            server.accept(wire);
            return pair.port2;
          };
          const client = new ProtocolClient();
          try {
            expect(
              typeof client.exportSnapshotStream,
              "next/ snapshot API must be implemented",
            ).toBe("function");
            await client.connect(port(firstServer));
            const phases: string[] = [];
            const abort = new AbortController(),
              iterator = client.exportSnapshotStream({
                signal: abort.signal,
                onProgress: (p) => phases.push(p.phase),
              });
            const first = await iterator.next();
            expect(first.done).toBe(false);
            if (first.done) throw new Error("Missing snapshot chunk");
            const held = first.value;
            expect(held.byteOffset).toBe(0);
            expect(held.length).toBe(held.buffer.byteLength);
            equalBytes(held, borrowed?.subarray(0, Math.min(length, 1 << 20)));
            reach.reach(length > 1 << 20 ? "split native write" : "single chunk");
            if (mode === "all") {
              const chunks = [held];
              for await (const chunk of iterator) chunks.push(chunk);
              const output = new Uint8Array(length);
              let at = 0;
              for (const chunk of chunks) {
                expect(chunk.length).toBeLessThanOrEqual(1 << 20);
                output.set(chunk, at);
                at += chunk.length;
              }
              expect(at).toBe(length);
              equalBytes(output, borrowed);
            } else if (mode === "break") await iterator.return();
            else if (mode === "cancel") {
              abort.abort();
              await closed;
              // A suspended generator need not advance for cancellation to release client liveness.
              await client.send({ t: "admin", op: "storageStats", args: [] }).promise;
              expect(client.hasPins).toBe(false);
              await iterator.return();
            } else {
              const nextServer = mode === "reconnect" ? firstServer : server(2);
              if (mode === "owner change") firstServer.stepDown("closing");
              await client.reconnect(port(nextServer));
              await expect(iterator.next()).rejects.toMatchObject({
                code: "MINNOW_SNAPSHOT_HANDLE",
              });
            }
            await closed;
            expect(phases.includes("done")).toBe(mode === "all");
            equalBytes(held, borrowed?.subarray(0, held.length));
            expect(borrowed?.byteLength).toBe(length);
            expect(transfers).toBeGreaterThan(0);
            reach.reach(mode);
            reach.reach("borrowed source never transferred");
            reach.reach("held output stays valid");
          } finally {
            allocation.mockRestore();
            admission.mockRestore();
            client.close();
            for (const server of servers) {
              const draining = server.quiesce("closing");
              await draining.drained;
              draining.finish();
            }
            await h.engine.close();
            for (const p of ports) p.close();
            expect(h.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [(1 << 20) + 31, "all"],
          [1, "break"],
          [17, "cancel"],
          [33, "reconnect"],
          [64, "owner change"],
        ],
      },
    );
    reach.assertReached();
  }, 180000);
