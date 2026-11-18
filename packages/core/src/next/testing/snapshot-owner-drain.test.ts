/** Started cleanup remains owner work after its connection drops; restore must drain it. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { ABANDONED_STREAM_MS } from "../owner/protocol.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_OWNER_DRAIN_REACH } from "../../testing/reach.js";
import { snapshotOwner } from "./snapshot-owner-support.js";

for (const seed of seedsFor("next-snapshot-owner-drain", [0xb2f1, 0xb2f2]))
  it(`replacement drains disconnected export cleanup ${String(seed)}`, async () => {
    const reach = createReach("snapshot owner drain", SNAPSHOT_OWNER_DRAIN_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("disconnect", "reconnect", "cancel", "abandon"),
        fc.integer({ min: 1, max: 999 }),
        async (mode, value) => {
          reach.beginHistory();
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const source = await snapshotOwner(kind, "drain-source"),
              target = await snapshotOwner(kind, "drain-target");
            let release!: () => void, entered!: () => void;
            const waiting = new Promise<void>((resolve) => {
              release = resolve;
            });
            const cleaning = new Promise<void>((resolve) => {
              entered = resolve;
            });
            let loading: Promise<{ error: unknown }> | undefined;
            let output: AsyncGenerator<Uint8Array, void, void> | undefined;
            try {
              const from = await source.current.connect(),
                into = await target.current.connect(),
                exporting = await target.current.connect();
              await from.query("CREATE TABLE t (v INT)");
              await from.query("INSERT INTO t VALUES ($1)", [value]);
              const archive = await from.exportSnapshot();
              await into.query("CREATE TABLE t (v INT)");
              await into.query("INSERT INTO t VALUES ($1)", [-value]);
              const operations = target.current.engine.snapshots;
              if (operations === undefined) throw new Error("Missing native snapshot operations");
              const nativeExport = operations.export.bind(operations),
                nativeImport = operations.import.bind(operations);
              let cleanupDone = false,
                importStarted = false,
                inputs = 0;
              operations.export = async (sink, signal, work) => {
                const grant = await target.current.governor.admit({
                  label: "started export cleanup witness",
                  minimumBytes: 1024,
                  spillable: false,
                });
                expect(grant.allocate(1024)).toBe(true);
                try {
                  await nativeExport(sink, signal, work);
                } finally {
                  entered();
                  await waiting;
                  cleanupDone = true;
                  grant.close();
                }
              };
              operations.import = async (input, signal, funding) => {
                importStarted = true;
                await nativeImport(input, signal, funding);
              };
              let now = Date.now();
              if (mode === "abandon") {
                // Native IDB task timers stay real; only the owner sweep clock is driven.
                vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
                vi.spyOn(target.current.server, "now").mockImplementation(() => now);
              }
              const abort = new AbortController();
              output = exporting.exportSnapshotStream({ signal: abort.signal });
              expect((await output.next()).done).toBe(false);
              if (mode === "disconnect") exporting.close();
              else if (mode === "reconnect") await target.current.reconnect(exporting);
              else if (mode === "cancel")
                abort.abort(new DOMException("stop export", "AbortError"));
              else {
                now += ABANDONED_STREAM_MS;
                vi.advanceTimersByTime(Math.max(1, ABANDONED_STREAM_MS / 2));
              }
              await cleaning;
              reach.reach(mode);
              expect(cleanupDone).toBe(false);
              expect(target.current.governor.usedBytes).toBeGreaterThanOrEqual(1024);
              reach.reach("started cleanup charged");
              loading = into
                .importSnapshotStream(
                  (async function* () {
                    inputs++;
                    yield archive;
                  })(),
                )
                .then(
                  () => ({ error: null }),
                  (error: unknown) => ({ error }),
                );
              // FIFO reply: the import's exclusion interval has started, without a timer.
              await expect(
                into.send({ t: "admin", op: "storageStats", args: [] }).promise,
              ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_HANDLE" });
              expect(importStarted).toBe(false);
              expect(inputs).toBe(0);
              reach.reach("import waits for cleanup");
              release();
              expect(await loading).toEqual({ error: null });
              expect(cleanupDone).toBe(true);
              expect(importStarted).toBe(true);
              reach.reach("release unblocks import");
              const fresh = await target.current.connect();
              expect((await fresh.query("SELECT v FROM t")).rows()).toEqual([[value]]);
              await fresh.query("INSERT INTO t VALUES ($1)", [value + 1]);
              expect((await fresh.query("SELECT v FROM t ORDER BY v")).rows()).toEqual([
                [value],
                [value + 1],
              ]);
              reach.reach("fresh session writes");
              reach.reach(kind);
            } finally {
              release();
              await loading;
              try {
                await target.close();
              } finally {
                await output?.return().catch(() => undefined);
                vi.restoreAllMocks();
                vi.useRealTimers();
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
          ["disconnect", 1],
          ["reconnect", 2],
          ["cancel", 3],
          ["abandon", 4],
        ],
      },
    );
    reach.assertReached();
  }, 180000);
