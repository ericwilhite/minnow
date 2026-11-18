/** Restore removes old effects: never acknowledge/replay an old incarnation's request.
 * Both real native adapters and the real composed executor, across MessageChannels.
 */
import fc from "fast-check";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { harness, raw, seed as seedRows, flushTasks, flushUntil } from "./owner-harness.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  OWNER_DATABASE_IDENTITY_REACH,
  OWNER_DATABASE_IDENTITY_QUEUE_REACH,
} from "../../testing/reach.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { SimLocks } from "../storage/sim-disk.js";
import { composeEngine } from "../owner/compose.js";
import { ProtocolClient } from "../owner/client.js";
import { OwnerServer, type ServerPort } from "../owner/server.js";
import { PROTOCOL_VERSION, type OwnerMessage } from "../owner/protocol.js";
import { createLowerer } from "../lower/create.js";
import { ChunkReader } from "../page/reader.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { exportSnapshot } from "../write/snapshot-capture.js";
import { loadSnapshot } from "../write/snapshot-load.js";
import { snapshotBlob } from "../write/snapshot-source.js";
import { snapshotTestTarget } from "../write/snapshot-test-support.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  tableInput,
} from "../write/test-support.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

async function archive(value: number): Promise<Blob> {
  const h = new Harness("strict", "memory"),
    path = await h.open();
  const grant = await h.governor.admit({
    label: "owner archive",
    minimumBytes: 8 << 20,
    spillable: false,
  });
  try {
    await autocommit(path, (s) =>
      s.changeCatalog({
        kind: "createTable",
        table: tableInput("t", [column("v", INT)], { primaryKey: null }),
      }),
    );
    await autocommit(path, (s) => s.table(1).insert(batch(ints([value])), null));
    const parts: Array<Uint8Array<ArrayBuffer>> = [];
    await exportSnapshot(
      path,
      new Uint8Array(16).fill(3),
      {
        write: (bytes) => {
          parts.push(bytes.slice());
        },
      },
      grant,
    );
    return new Blob(parts);
  } finally {
    grant.close();
    await h.close();
    expect(h.governor.usedBytes).toBe(0);
  }
}

for (const seed of seedsFor("next-owner-database-identity", [0xb2df, 0xb2e0]))
  it(`old request/session refusal across native replacement ${String(seed)}`, async () => {
    const reach = createReach("owner database identity", OWNER_DATABASE_IDENTITY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 999 }),
        fc.boolean(),
        async (value, receiptLost) => {
          reach.beginHistory();
          const source = await archive(value);
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const h = new Harness(),
              locks = new SimLocks(),
              disk = new MemoryOpfs(),
              factory = new IDBFactory();
            let epoch = 0;
            const backend =
              kind === "OPFS"
                ? createOpfsBackend({ name: "owner-identity", directory: disk.root, locks })
                : createIndexedDbBackend({
                    name: "owner-identity",
                    locks,
                    database: () => openIndexedDb(factory, "owner-identity"),
                  });
            const open = backend.open.bind(backend);
            backend.open = async (lease) => {
              const session = await open(lease);
              epoch = session.epoch;
              return session;
            };
            Object.assign(h, { backend });
            const ports: MessagePort[] = [],
              clients: ProtocolClient[] = [];
            let owner:
              { server: OwnerServer; engine: ReturnType<typeof composeEngine> } | undefined;
            const openOwner = async () => {
              await h.open();
              const engine = composeEngine({
                writePath: h.path,
                planner: createLowerer(),
                governor: h.governor,
                spill: new MemorySpillService(),
                reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
              });
              owner = {
                engine,
                server: new OwnerServer(engine, {
                  ownerId: `owner-${String(epoch)}`,
                  epoch,
                  build: "identity",
                }),
              };
              return owner;
            };
            const stopOwner = async () => {
              if (owner === undefined) return;
              const stopped = owner.server.quiesce("closing");
              await stopped.drained;
              stopped.finish();
              await owner.engine.close();
              await h.close();
              owner = undefined;
              expect(h.governor.usedBytes).toBe(0);
            };
            try {
              const first = await openOwner(),
                id = h.path.databaseId;
              const pair = new MessageChannel();
              ports.push(pair.port1, pair.port2);
              let hold = false,
                saved: OwnerMessage | undefined;
              let held!: () => void;
              const heldDone = new Promise<void>((resolve) => {
                held = resolve;
              });
              const wire: ServerPort = {
                get onmessage() {
                  return pair.port1.onmessage;
                },
                set onmessage(value) {
                  pair.port1.onmessage = value;
                },
                close: () => pair.port1.close(),
                postMessage(message, transfer) {
                  const reply = message as OwnerMessage;
                  if (hold && reply.t === "done") {
                    saved = reply;
                    held();
                    return;
                  }
                  pair.port1.postMessage(message, transfer);
                },
              };
              first.server.accept(wire);
              const old = new ProtocolClient({ clientId: "11".repeat(16) });
              clients.push(old);
              await old.connect(pair.port2);
              await old.query("CREATE TABLE t (v INT)");
              hold = receiptLost;
              const oldWrite = old.query("INSERT INTO t (v) VALUES ($1)", [-value]).then(
                () => ({ error: null }),
                (error: unknown) => ({ error }),
              );
              if (receiptLost) {
                await Promise.race([
                  heldDone,
                  oldWrite.then(({ error }) => {
                    throw error instanceof Error
                      ? error
                      : new Error("Write completed without intercepted reply", { cause: error });
                  }),
                ]);
                expect(saved?.t).toBe("done");
                reach.reach("lost committed reply");
              } else {
                expect(await oldWrite).toEqual({ error: null });
                reach.reach("committed receipt delivered");
              }
              const lsn = h.path.appliedLsn;
              await stopOwner();
              const prepaid = await h.governor.admit({
                label: "owner replacement startup",
                minimumBytes: 8 << 20,
                spillable: true,
              });
              try {
                await loadSnapshot(
                  snapshotBlob(source),
                  () => snapshotTestTarget(h, lsn, false, undefined, prepaid),
                  h.governor,
                  { replace: true },
                );
              } finally {
                prepaid.close();
              }
              const second = await openOwner();
              expect(h.path.databaseId).not.toBe(id);
              const replacement = new MessageChannel();
              ports.push(replacement.port1, replacement.port2);
              second.server.accept(replacement.port1);
              await expect(old.reconnect(replacement.port2)).rejects.toMatchObject({
                code: "MINNOW_DATABASE_REPLACED",
              });
              if (receiptLost)
                expect(await oldWrite).toMatchObject({
                  error: { code: "MINNOW_DATABASE_REPLACED" },
                });
              await expect(
                old.query("INSERT INTO t (v) VALUES ($1)", [-value]),
              ).rejects.toMatchObject({ code: "MINNOW_DATABASE_REPLACED" });
              reach.reach("SDK retry refused");
              reach.reach("sticky old session");

              // The stale hello below must refuse BEFORE claiming a larger generation
              // and killing this live session of a genuinely fresh client.
              const freshPair = new MessageChannel();
              ports.push(freshPair.port1, freshPair.port2);
              second.server.accept(freshPair.port1);
              const fresh = new ProtocolClient({ clientId: "22".repeat(16) });
              clients.push(fresh);
              await fresh.connect(freshPair.port2);
              expect((await fresh.query("SELECT v FROM t")).rows()).toEqual([[value]]);
              const stale = new MessageChannel();
              ports.push(stale.port1, stale.port2);
              second.server.accept(stale.port1);
              const refused = new Promise<OwnerMessage>((resolve) => {
                stale.port2.onmessage = (event) => resolve(event.data as OwnerMessage);
              });
              stale.port2.postMessage({
                t: "hello",
                databaseId: id,
                protocol: PROTOCOL_VERSION,
                build: "stale",
                clientId: fresh.clientId,
                generation: 2,
                previousOwnerId: "old",
                livenessLock: "",
              });
              expect(await refused).toMatchObject({ t: "refused", reason: "databaseReplaced" });
              expect((await fresh.query("SELECT v FROM t")).rows()).toEqual([[value]]);
              await fresh.query("INSERT INTO t (v) VALUES ($1)", [value + 1]);
              expect((await fresh.query("SELECT v FROM t ORDER BY v")).rows()).toEqual([
                [value],
                [value + 1],
              ]);
              reach.reach("stale hello before supersede");
              reach.reach("new session writes");
              reach.reach(kind);
            } finally {
              for (const client of clients) client.close();
              await stopOwner();
              for (const port of ports) port.close();
              expect(h.governor.usedBytes).toBe(0);
            }
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, true],
          [1, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

// Deterministic scheduling at the server boundary: a request already waiting in a
// session must recheck the incarnation before planning or consulting the ledger.
for (const seed of seedsFor("next-owner-database-identity-queued", [0xb2e1, 0xb2e2]))
  it(`queued and newly arriving requests check identity before the ledger ${String(seed)}`, async () => {
    const reach = createReach("owner identity queued", OWNER_DATABASE_IDENTITY_QUEUE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 999 }), async (value) => {
        reach.beginHistory();
        const h = harness({ autoFlush: false });
        try {
          await flushUntil(h.writePath, seedRows(await h.connect(), 1));
          let id = 1n;
          Object.defineProperty(h.writePath, "databaseId", { get: () => id });
          const port = raw(h.server);
          port.deliver(port.statement(1, "UPDATE t SET n = $1 WHERE id = 1", [value]));
          await flushTasks();
          expect(h.writePath.appliedLsn).toBeGreaterThan(h.writePath.durableLsn);
          port.deliver(port.statement(2, "UPDATE t SET n = n + 1 WHERE id = 1"));
          reach.reach("queued before replacement");
          id = 2n;
          await h.writePath.flush();
          expect(await flushUntil(h.writePath, port.reply(2))).toMatchObject({
            t: "error",
            error: { props: { code: "MINNOW_DATABASE_REPLACED" } },
          });
          port.deliver({
            ...port.statement(1, "UPDATE t SET n = $1 WHERE id = 1", [value]),
            retry: true,
          });
          expect(port.replies.at(-1)).toMatchObject({
            t: "error",
            seq: 1,
            error: { props: { code: "MINNOW_DATABASE_REPLACED" } },
          });
          reach.reach("arrived after replacement");
          reach.reach("no stale replay");
          expect(h.writePath.rows("t")[0]).toEqual([1, value, "row 1"]);
          reach.reach("no queued write");
        } finally {
          const closed = h.server.quiesce("closing");
          await closed.drained;
          closed.finish();
          await h.engine.close();
          expect(h.governor.usedBytes).toBe(0);
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 120000);
