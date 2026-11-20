/** Actual snapshot RPC + native import/recovery meets the live protocol, on both stores. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_SNAPSHOT_IMPORT_REACH } from "../../testing/reach.js";
import { ProtocolClient } from "../owner/client.js";
import { snapshotOwner } from "./snapshot-owner-support.js";
import { until, publicationTurn } from "./live-runtime.test-support.js";

for (const seed of seedsFor("next-live-snapshot-import", [8900, 8901])) {
  it(`resets live identity and retires old state on native import seed=${String(seed)}`, async () => {
    const reach = createReach("live snapshot import", LIVE_SNAPSHOT_IMPORT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 100 }),
        async (outstanding, streamed, throwsReset, reusedKey) => {
          reach.beginHistory();
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const source = await snapshotOwner(kind, "live-source");
            const target = await snapshotOwner(kind, "live-target");
            const stale = new ProtocolClient();
            const ports: MessagePort[] = [];
            let release!: () => void;
            const gate = new Promise<void>((resolve) => {
              release = resolve;
            });
            try {
              const from = await source.current.connect();
              const into = await target.current.connect();
              const observer = await target.current.connect();
              const oldRows = [
                [reusedKey, "old"],
                [reusedKey + 3, "target"],
              ];
              const importedRows = [
                [reusedKey, "new"],
                [reusedKey + 2, "source"],
              ];
              await from.query("CREATE TABLE t (id INT PRIMARY KEY, payload TEXT)");
              await from.query("INSERT INTO t VALUES ($1, 'new'), ($2, 'source')", [
                reusedKey,
                reusedKey + 2,
              ]);
              const bytes = await from.exportSnapshot();
              await into.query("CREATE TABLE t (id INT PRIMARY KEY, payload TEXT)");
              await into.query("INSERT INTO t VALUES ($1, 'old'), ($2, 'target')", [
                reusedKey,
                reusedKey + 3,
              ]);
              // Lose replacement notification on a third connection. Its next real handshake
              // must be refused by the owner, independently of the SDK's sticky local guard.
              const attach = (dropReplacement: boolean) => {
                const pair = new MessageChannel();
                ports.push(pair.port1, pair.port2);
                target.current.server.accept({
                  get onmessage() {
                    return pair.port1.onmessage;
                  },
                  set onmessage(value) {
                    pair.port1.onmessage = value;
                  },
                  postMessage(message, transfer) {
                    if (!dropReplacement || (message as { t?: string }).t !== "databaseReplaced")
                      pair.port1.postMessage(message, transfer);
                  },
                  close() {
                    pair.port1.close();
                  },
                });
                return pair.port2;
              };
              await stale.connect(attach(true));
              await stale.query("SELECT 7");
              const runtime = target.current.engine.live;
              if (runtime === undefined) throw new Error("Live runtime missing");
              const observed: unknown[][][] = [];
              const resets: string[] = [];
              const errors: Error[] = [];
              let visible: unknown[][] | null = null;
              let initialVersion = 0;
              const keyChanges: unknown[][] = [];
              let replacementRefusal: unknown;
              const sub = observer.subscribeLive("SELECT id, payload FROM t ORDER BY id", [], {
                onResult(result, version, _generation, delivery) {
                  observed.push(result.rows());
                  visible = result.rows();
                  keyChanges.push([...delivery.keyChanges]);
                  if (observed.length === 1) initialVersion = version;
                  if (outstanding && observed.length === 1) return gate;
                },
                onReset(reason) {
                  resets.push(reason);
                  visible = null;
                  try {
                    observer
                      .subscribeLive("SELECT 1", [], {
                        onResult: () => undefined,
                        onError: () => undefined,
                      })
                      .close();
                  } catch (error) {
                    replacementRefusal = error;
                  }
                  if (throwsReset) throw new Error("isolated reset consumer failure");
                },
                onError(error) {
                  errors.push(error);
                },
              });
              await until(() => observed.length === 1);
              if (!outstanding) await until(() => runtime.stats.waiting === 0);
              let importerReset = 0;
              let importerDeliveries = 0;
              const ownSub = into.subscribeLive("SELECT id FROM t ORDER BY id", [], {
                onResult() {
                  importerDeliveries++;
                },
                onReset() {
                  importerReset++;
                },
                onError(error) {
                  errors.push(error);
                },
              });
              await until(() => observed.length === 1 && importerDeliveries === 1);
              await until(() => runtime.stats.waiting === (outstanding ? 1 : 0));
              expect(observed[0]).toEqual(oldRows);
              expect(runtime.retainedBytes).toBeGreaterThan(0);
              expect(runtime.stats.waiting).toBe(outstanding ? 1 : 0);
              reach.reach(outstanding ? "outstanding delivery" : "acknowledged delivery");
              if (streamed) {
                await into.importSnapshotStream(
                  (async function* () {
                    for (let at = 0; at < bytes.length; at += 137)
                      yield bytes.subarray(at, at + 137);
                  })(),
                );
                reach.reach("stream import");
              } else {
                await into.importSnapshot(bytes);
                reach.reach("array import");
              }
              // This callback must bypass an unacknowledged onResult; releasing the gate
              // first would hide precisely the stalled-consumer reset bug.
              await expect(observer.query("SELECT 1")).rejects.toMatchObject({
                code: "MINNOW_DATABASE_REPLACED",
              });
              await expect(into.query("SELECT 1")).rejects.toMatchObject({
                code: "MINNOW_DATABASE_REPLACED",
              });
              expect(resets).toEqual(["databaseReplaced"]);
              expect(importerReset).toBe(1);
              expect(replacementRefusal).toMatchObject({ code: "MINNOW_DATABASE_REPLACED" });
              expect(visible, "old ready rows are invalid after identity reset").toBeNull();
              expect(runtime.stats).toEqual({
                subscriptions: 0,
                dirty: 0,
                running: 0,
                waiting: 0,
                bytes: 0,
              });
              reach.reach("explicit reset before consumer resumes");
              reach.reach("zero old live charge");
              if (throwsReset) reach.reach("reset callback isolated");
              await expect(observer.query("SELECT 1")).rejects.toMatchObject({
                code: "MINNOW_DATABASE_REPLACED",
              });
              await expect(stale.reconnect(attach(false))).rejects.toMatchObject({
                code: "MINNOW_DATABASE_REPLACED",
              });
              sub.refresh();
              release();
              await until(() =>
                errors.some((e) => Reflect.get(e, "code") === "MINNOW_DATABASE_REPLACED"),
              );
              await publicationTurn();
              expect(observed).toEqual([oldRows]);
              expect(keyChanges).toEqual([[]]);
              expect(observer.liveCount).toBe(0);
              expect(runtime.retainedBytes).toBe(0);
              reach.reach("typed old session refusal");
              reach.reach("stale handshake refused");
              const fresh = await target.current.connect();
              expect((await fresh.query("SELECT id, payload FROM t ORDER BY id")).rows()).toEqual(
                importedRows,
              );
              const newRuntime = target.current.engine.live;
              if (newRuntime === undefined) throw new Error("Live runtime missing after import");
              expect(newRuntime, "new path needs a new publication feed").not.toBe(runtime);
              let freshRows: unknown[][] | undefined;
              const freshDeliveries: Array<{ reset: boolean; keys: unknown[]; version: number }> =
                [];
              const freshSub = fresh.subscribeLive("SELECT id, payload FROM t ORDER BY id", [], {
                onResult(result, version, _generation, delivery) {
                  freshRows = result.rows();
                  freshDeliveries.push({
                    reset: delivery.reset,
                    keys: [...delivery.keyChanges],
                    version,
                  });
                },
                onError(error) {
                  errors.push(error);
                },
              });
              await until(() => freshRows !== undefined && newRuntime.stats.waiting === 0);
              expect(freshRows).toEqual(importedRows);
              expect(freshDeliveries[0]).toMatchObject({ reset: true, keys: [] });
              expect(initialVersion).toBeGreaterThan(0);
              reach.reach("key reuse has no rekey");
              await fresh.query("INSERT INTO t VALUES ($1, 'after')", [reusedKey + 4]);
              await publicationTurn();
              await until(
                () =>
                  newRuntime.stats.dirty === 0 &&
                  newRuntime.stats.running === 0 &&
                  newRuntime.stats.waiting === 0,
              );
              expect(freshRows, "commits must invalidate through the recovered path feed").toEqual([
                ...importedRows,
                [reusedKey + 4, "after"],
              ]);
              expect(freshDeliveries.every((d) => d.version > 0 && d.keys.length === 0)).toBe(true);
              expect(
                errors.every((error) => Reflect.get(error, "code") === "MINNOW_DATABASE_REPLACED"),
              ).toBe(true);
              reach.reach("recovered feed delivers commits");
              sub.close();
              ownSub.close();
              freshSub.close();
              reach.reach(kind);
            } finally {
              release();
              stale.close();
              for (const port of ports) port.close();
              await target.close();
              await source.close();
            }
            expect(target.current.governor.usedBytes).toBe(0);
            expect(source.current.governor.usedBytes).toBe(0);
            reach.reach("zero owner charge after close");
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [false, false, false, 1],
          [true, true, true, 2],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
}
