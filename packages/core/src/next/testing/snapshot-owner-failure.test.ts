/** Cancellation/producer cleanup, quota, corruption and scope through the owner snapshot API. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_OWNER_FAILURE_REACH } from "../../testing/reach.js";
import { snapshotOwner } from "./snapshot-owner-support.js";

for (const seed of seedsFor("next-snapshot-owner-failures", [0xb2e5, 0xb2e6]))
  it(`failed public transfers preserve the target and retry ${String(seed)}`, async () => {
    const reach = createReach("snapshot owner failures", SNAPSHOT_OWNER_FAILURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("cancel", "quota", "corrupt", "scope"),
        fc.integer({ min: 1, max: 999 }),
        async (mode, value) => {
          reach.beginHistory();
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const source = await snapshotOwner(kind, "failure-source"),
              target = await snapshotOwner(kind, "failure-target");
            try {
              const from = await source.current.connect(),
                into = await target.current.connect();
              await from.query("CREATE TABLE t (v INT)");
              await from.query("INSERT INTO t VALUES ($1)", [value]);
              await into.query("CREATE TABLE t (v INT)");
              await into.query("INSERT INTO t VALUES ($1)", [-value]);
              await target.current.path.writePath.recycleLog();
              const oldId = target.current.engine.databaseId,
                bytes = await from.exportSnapshot(),
                copy = bytes.slice();
              if (mode === "cancel") {
                const abort = new AbortController(),
                  reason = new Error("stop public load");
                let started!: () => void,
                  returned = false;
                const entered = new Promise<void>((resolve) => {
                  started = resolve;
                });
                const input: AsyncIterable<Uint8Array> = {
                  [Symbol.asyncIterator]() {
                    return {
                      next() {
                        started();
                        return new Promise<IteratorResult<Uint8Array>>(() => undefined);
                      },
                      return() {
                        returned = true;
                        return new Promise<IteratorResult<Uint8Array>>(() => undefined);
                      },
                    };
                  },
                };
                const pending = into.importSnapshotStream(input, { signal: abort.signal });
                const outcome = pending.then(
                  () => ({ error: null }),
                  (error: unknown) => ({ error }),
                );
                await entered;
                abort.abort(reason);
                expect(await outcome).toEqual({ error: reason });
                expect(returned).toBe(true);
                reach.reach("stalled input cancelled");
                reach.reach("unbounded return ignored");
                await target.current.reconnect(into);
              } else if (mode === "quota") {
                target.failSpool();
                await expect(into.importSnapshot(bytes)).rejects.toMatchObject({
                  code: "MINNOW_STORE_QUOTA",
                });
                reach.reach("quota refused");
                await target.current.reconnect(into);
              } else if (mode === "corrupt") {
                const corrupt = bytes.slice();
                corrupt[corrupt.length - 1] = (corrupt[corrupt.length - 1] ?? 0) ^ 1;
                await expect(into.importSnapshot(corrupt)).rejects.toMatchObject({
                  code: "MINNOW_SNAPSHOT_FORMAT",
                });
                reach.reach("corruption refused");
                await target.current.reconnect(into);
              } else {
                await from.query("CREATE TABLE checked (n INT CHECK (n > 0))");
                await expect(from.exportSnapshot()).rejects.toMatchObject({
                  code: "MINNOW_SNAPSHOT_SCOPE",
                });
                reach.reach("scope refused");
              }
              expect(target.current.engine.databaseId).toBe(oldId);
              expect((await into.query("SELECT v FROM t")).rows()).toEqual([[-value]]);
              expect((await from.query("SELECT v FROM t")).rows()).toEqual([[value]]);
              expect(bytes).toEqual(copy);
              reach.reach("old target and source intact");
              await into.importSnapshot(bytes);
              const fresh = await target.current.connect();
              expect((await fresh.query("SELECT v FROM t")).rows()).toEqual([[value]]);
              reach.reach("retry completes");
              reach.reach(kind);
            } finally {
              await target.close();
              await source.close();
            }
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["cancel", 1],
          ["quota", 2],
          ["corrupt", 3],
          ["scope", 4],
        ],
      },
    );
    reach.assertReached();
  }, 180000);
