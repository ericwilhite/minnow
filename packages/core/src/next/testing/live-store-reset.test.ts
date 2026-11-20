/** Terminal database identity reset clears receiving bindings independently of decoder delivery. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_STORE_RESET_REACH } from "../../testing/reach.js";
import { NativeLiveStore } from "../owner/live-store.js";
import { snapshotOwner } from "./snapshot-owner-support.js";
import { until, publicationTurn } from "./live-runtime.test-support.js";

interface Row {
  id: number;
  payload: string;
}
for (const seed of seedsFor("next-live-store-reset", [8904, 8905]))
  it(`clears receiving baselines on replacement seed=${String(seed)}`, async () => {
    const reach = createReach("receiving identity reset", LIVE_STORE_RESET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 99 }),
        async (keyed, outstanding, streamed, key) => {
          reach.beginHistory();
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const source = await snapshotOwner(kind, "binding-source");
            const target = await snapshotOwner(kind, "binding-target");
            let store: NativeLiveStore<Row> | undefined;
            let fresh: NativeLiveStore<Row> | undefined;
            let unsubscribe: (() => void) | undefined;
            let release: (() => void) | undefined;
            try {
              const from = await source.current.connect();
              const into = await target.current.connect();
              const observer = await target.current.connect();
              await from.query("CREATE TABLE t (id INT PRIMARY KEY, payload TEXT)");
              await from.query("INSERT INTO t VALUES ($1, 'new')", [key]);
              const bytes = await from.exportSnapshot();
              await into.query("CREATE TABLE t (id INT PRIMARY KEY, payload TEXT)");
              await into.query("INSERT INTO t VALUES ($1, 'old')", [key]);
              let hold = false,
                decoding = false,
                aborted = false;
              store = new NativeLiveStore<Row>(
                observer,
                "SELECT id, payload FROM t ORDER BY id",
                [],
                {
                  ...(keyed ? { key: "id" as const, table: "t" } : {}),
                  decode: async (result, signal) => {
                    if (hold) {
                      decoding = true;
                      await new Promise<void>((resolve) => {
                        release = resolve;
                        signal.addEventListener(
                          "abort",
                          () => {
                            aborted = true;
                            resolve();
                          },
                          { once: true },
                        );
                      });
                    }
                    return result.rows();
                  },
                },
              );
              await store.refresh();
              expect(store.getSnapshot().rows).toEqual([{ id: key, payload: "old" }]);
              unsubscribe = store.subscribe(() => undefined);
              const runtime = target.current.engine.live;
              if (runtime === undefined) throw new Error("Missing native runtime");
              let refresh: Promise<void> | undefined;
              if (outstanding) {
                hold = true;
                refresh = store.refresh();
                await until(() => decoding);
                expect(store.getSnapshot().rows).toEqual([{ id: key, payload: "old" }]);
              }
              if (streamed)
                await into.importSnapshotStream(
                  (async function* () {
                    yield bytes;
                  })(),
                );
              else await into.importSnapshot(bytes);
              await expect(observer.query("SELECT 1")).rejects.toMatchObject({
                code: "MINNOW_DATABASE_REPLACED",
              });
              await publicationTurn();
              const terminal = store.getSnapshot();
              expect(terminal).toMatchObject({
                status: "error",
                rows: [],
                version: null,
                error: { code: "MINNOW_DATABASE_REPLACED" },
              });
              expect(terminal.result).toBeUndefined();
              expect(terminal.changes).toBeUndefined();
              expect(terminal.keyChanges).toBeUndefined();
              expect(terminal.patch).toBeUndefined();
              expect(runtime.retainedBytes).toBe(0);
              if (outstanding) {
                expect(aborted).toBe(true);
                reach.reach("pending decoder aborted");
              }
              await refresh;
              await store.refresh();
              await publicationTurn();
              expect(store.getSnapshot()).toBe(terminal);
              reach.reach("stable typed terminal error");
              reach.reach("empty old baseline");
              const reopened = await target.current.connect();
              fresh = new NativeLiveStore<Row>(
                reopened,
                "SELECT id, payload FROM t ORDER BY id",
                [],
                keyed ? { key: "id", table: "t" } : {},
              );
              await fresh.refresh();
              expect(fresh.getSnapshot()).toMatchObject({
                status: "ready",
                rows: [{ id: key, payload: "new" }],
                reset: true,
              });
              expect(fresh.getSnapshot().keyChanges ?? []).toEqual([]);
              reach.reach("explicit reopen full snapshot");
              reach.reach(keyed ? "keyed" : "unkeyed");
              reach.reach(outstanding ? "outstanding" : "settled");
              reach.reach(streamed ? "stream" : "array");
              reach.reach(kind);
              await store.close();
              await fresh.close();
              expect(store.usedBytes).toBe(0);
              expect(fresh.usedBytes).toBe(0);
              expect(NativeLiveStore.clientUsedBytes(observer)).toBe(0);
              reach.reach("zero receiving charge after close");
            } finally {
              release?.();
              unsubscribe?.();
              await store?.close();
              await fresh?.close();
              await target.close();
              await source.close();
            }
            expect(target.current.governor.usedBytes).toBe(0);
            expect(source.current.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, false, false, 1],
          [true, true, true, 2],
          [true, false, false, 3],
          [false, true, true, 4],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
