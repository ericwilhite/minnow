/** Restore cannot replay/remove a write while still reporting its old committed outcome. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_OWNER_INFLIGHT_REACH } from "../../testing/reach.js";
import { snapshotOwner } from "./snapshot-owner-support.js";

for (const seed of seedsFor("next-snapshot-owner-inflight", [0xb2e9, 0xb2ea]))
  it(`replacement drains live native writes and invalidates old clients ${String(seed)}`, async () => {
    const reach = createReach("snapshot owner in flight", SNAPSHOT_OWNER_INFLIGHT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 999 }),
        async (atCommit, loseReply, heldExport, value) => {
          reach.beginHistory();
          for (const kind of ["OPFS", "IndexedDB"] as const)
            for (const contractOnly of [false, true]) {
              const source = await snapshotOwner(kind, "inflight-source"),
                target = await snapshotOwner(kind, "inflight-target");
              let release: () => void = () => undefined;
              let paused: AsyncGenerator<Uint8Array, void, void> | undefined;
              let held: Uint8Array | undefined;
              let preparedClosed = 0;
              if (contractOnly) {
                const create = target.current.engine.createPreparedCache.bind(
                  target.current.engine,
                );
                target.current.engine.createPreparedCache = () => {
                  const cache = create();
                  let closed = false;
                  const check = () => {
                    if (closed) throw new Error("Prepared cache used after restore drain");
                  };
                  // Only the OwnerEngine contract; no concrete PlanCache.clear() extension.
                  return {
                    get(id) {
                      check();
                      return cache.get(id);
                    },
                    set(id, value) {
                      check();
                      return cache.set(id, value);
                    },
                    delete(id) {
                      cache.delete(id);
                    },
                    close() {
                      if (closed) return;
                      closed = true;
                      preparedClosed++;
                      cache.close();
                    },
                  };
                };
              }
              try {
                const from = await source.current.connect(),
                  into = await target.current.connect(),
                  blocker = await target.current.connect();
                await from.query("CREATE TABLE t (v INT)");
                await from.query("INSERT INTO t VALUES ($1)", [value]);
                const bytes = await from.exportSnapshot();
                await into.query("CREATE TABLE t (v INT)");
                await into.query("INSERT INTO t VALUES ($1)", [-value]);
                await into.prepare("SELECT v FROM t");
                await target.current.path.writePath.recycleLog();
                const oldId = target.current.engine.databaseId;
                if (heldExport) {
                  const exporting = await target.current.connect();
                  paused = exporting.exportSnapshotStream();
                  const first = await paused.next();
                  expect(first.done).toBe(false);
                  if (first.done) throw new Error("Missing export header");
                  held = first.value;
                }
                if (!atCommit) await blocker.begin(1);
                const gate = atCommit ? target.blockFlush() : undefined;
                release = gate?.release ?? release;
                const writer = await target.current.connect(loseReply && atCommit);
                const write = writer.query("INSERT INTO t VALUES ($1)", [-value - 1]).then(
                  (result) => ({ result, error: null }),
                  (error: unknown) => ({ result: null, error }),
                );
                if (gate !== undefined) {
                  await gate.reached;
                  expect(target.current.path.writePath.appliedLsn).toBeGreaterThan(
                    target.current.path.writePath.durableLsn,
                  );
                  reach.reach("durable commit blocked");
                } else {
                  // FIFO reply barrier on the writer proves its INSERT is waiting for the held slot.
                  await writer.send({ t: "admin", op: "storageStats", args: [] }).promise;
                  reach.reach("writer slot blocked");
                }
                let inputs = 0;
                const loading = into.importSnapshotStream(
                  (async function* () {
                    inputs++;
                    yield bytes;
                  })(),
                );
                const loaded = loading.then(
                  () => ({ error: null }),
                  (error: unknown) => ({ error }),
                );
                if (atCommit) {
                  // A later message on the importer's port proves exclusion is already active.
                  await expect(
                    into.send({ t: "admin", op: "storageStats", args: [] }).promise,
                  ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_HANDLE" });
                  expect(inputs).toBe(0);
                  reach.reach("no input before drain");
                  release();
                }
                expect(await loaded).toEqual({ error: null });
                if (contractOnly) {
                  expect(preparedClosed).toBeGreaterThan(0);
                  reach.reach("contract-only prepared cache closed");
                } else reach.reach("concrete prepared cache closed");
                if (paused !== undefined) {
                  await expect(paused.next()).rejects.toMatchObject({
                    code: "MINNOW_DATABASE_REPLACED",
                  });
                  expect(held?.length).toBeGreaterThan(0);
                  reach.reach("suspended export cancelled");
                }
                const outcome = await write;
                if (!atCommit) expect(outcome.error).toMatchObject({ code: "MINNOW_SESSION_LOST" });
                else if (loseReply) {
                  expect(outcome.error).toMatchObject({ code: "MINNOW_DATABASE_REPLACED" });
                  reach.reach("committed reply lost");
                } else {
                  expect(outcome.result?.summary.command).toBe("INSERT");
                  reach.reach("committed reply received");
                }
                expect(target.current.engine.databaseId).not.toBe(oldId);
                await expect(
                  writer.query("INSERT INTO t VALUES ($1)", [-value - 1]),
                ).rejects.toMatchObject({ code: "MINNOW_DATABASE_REPLACED" });
                await expect(blocker.commit(1)).rejects.toMatchObject({
                  code: "MINNOW_DATABASE_REPLACED",
                });
                const fresh = await target.current.connect();
                expect((await fresh.query("SELECT v FROM t")).rows()).toEqual([[value]]);
                await fresh.query("INSERT INTO t VALUES ($1)", [value + 1]);
                expect((await fresh.query("SELECT v FROM t ORDER BY v")).rows()).toEqual([
                  [value],
                  [value + 1],
                ]);
                reach.reach("no stale retry");
                reach.reach("new session writes");
                reach.reach(kind);
              } finally {
                release();
                try {
                  await target.close();
                } finally {
                  await paused?.return().catch(() => undefined);
                  await source.close();
                }
              }
            }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, false, true, 1],
          [true, true, false, 2],
          [true, false, true, 3],
        ],
      },
    );
    reach.assertReached();
  }, 180000);
