import { FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import { FTS_APPEND_WORKSPACE_BYTES } from "../format/fts-append.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  FTS_PUBLICATION_FAULT_REACH,
  FTS_PUBLICATION_CANCEL_REACH,
} from "../../testing/reach.js";
import { Harness, batch, texts, rules, rowsOf, watchGrants } from "../write/test-support.js";
import { setupFts, ftsParts } from "./fts-transaction.test-support.js";
import type { Grant } from "../memory/governor.js";
import { yieldTask } from "../exec/pipeline.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

for (const seed of seedsFor("next-fts-publication-faults", [0xb845, 0xb846]))
  it(`keeps row/FTS publication atomic across staged-root and append faults ${String(seed)}`, async () => {
    const reach = createReach("FTS publication faults", FTS_PUBLICATION_FAULT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (n) => {
        reach.beginHistory();
        for (const fault of ["prepare", "append", "none"] as const) {
          let enabled = false;
          const h = new Harness("strict", "sim", undefined, undefined, {
            afterDeltaPrepared(kind) {
              if (enabled && fault === "prepare" && kind === "fts")
                throw new Error("FTS prepare fault");
            },
          });
          const open = h.backend.open.bind(h.backend);
          h.backend.open = async (...args) => {
            const session = await open(...args);
            const append = session.appendLog.bind(session);
            session.appendLog = (bytes) =>
              enabled && fault === "append"
                ? Promise.reject(new Error("FTS append fault"))
                : append(bytes);
            return session;
          };
          try {
            const path = await setupFts(h);
            const tx = await path.begin(),
              s = tx.statement({ rules });
            const values = Array.from({ length: n }, (_, i) => "alpha " + String(i));
            await s.table(1).insert(batch(texts(values)), null);
            await s.end();
            enabled = true;
            if (fault === "none") {
              const receipt = await tx.commit(null);
              await receipt.durable;
              expect((await ftsParts(h)).entries.filter((e) => e.kind === "insert")).toHaveLength(
                n,
              );
              reach.reach("row and FTS commit");
            } else {
              await expect(
                tx.commit(null).then(async (receipt) => {
                  await receipt.durable;
                  return receipt;
                }),
              ).rejects.toThrow(
                fault === "prepare" ? /FTS prepare fault/ : /could not be made durable/,
              );
              reach.reach(fault === "prepare" ? "prepare fault" : "append fault");
            }
            enabled = false;
            await h.reopen();
            const pin = h.path.pin();
            try {
              expect(await rowsOf(pin, 1)).toEqual(fault === "none" ? values.map((x) => [x]) : []);
            } finally {
              pin.release();
            }
            await h.path.recycleLog();
            if (fault !== "none") {
              expect(h.path.state().root.fts).toEqual({ checkpoint: null, staged: null });
              reach.reach("no uncommitted FTS");
            } else {
              expect((await ftsParts(h)).lsn).not.toBeNull();
              reach.reach("committed FTS survives");
            }
            reach.reach("orphan retirement");
          } finally {
            enabled = false;
            await h.close();
          }
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);

for (const seed of seedsFor("next-fts-publication-cancel", [0xb847, 0xb848]))
  it(`drains an in-flight prepared-page write before close ${String(seed)}`, async (context) => {
    let stopped = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      stopped = true;
      retire?.();
    });
    const reach = createReach("FTS publication cancellation", FTS_PUBLICATION_CANCEL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray([0, 1, 2, 3], { minLength: 4, maxLength: 4 }),
        async (order) => {
          reach.beginHistory();
          for (const k of order) {
            if (stopped) throw new Error("FTS test body retired");
            const h = new Harness(),
              gate = deferred(),
              reached = deferred();
            let writes = 0,
              active = true,
              metadata: Grant | undefined,
              closing: Promise<void> | undefined,
              committing: Promise<unknown> | undefined,
              closed = false;
            let opened: Awaited<ReturnType<typeof setupFts>> | undefined;
            retire = () => {
              active = false;
              closing ??= opened?.close();
              void closing?.catch(() => undefined);
              gate.resolve();
            };
            watchGrants(h.governor, (g) => {
              if (active && g.label === "write path metadata") metadata = g;
            });
            h.onSpace = (space) => {
              const begin = space.beginWrite.bind(space);
              space.beginWrite = (label, options) => {
                const w = begin(label, options);
                if (label !== "FTS preparation") return w;
                const write = w.write.bind(w);
                w.write = async (parts) => {
                  const result = await write(parts);
                  if (active && writes++ === k) {
                    reached.resolve();
                    await gate.promise;
                  }
                  return result;
                };
                return w;
              };
            };
            try {
              const path = await setupFts(h);
              opened = path;
              const tx = await path.begin(),
                s = tx.statement({ rules });
              await s.table(1).insert(batch(texts(["alpha beta"])), null);
              await s.end();
              committing = tx.commit(null);
              void committing.catch(() => undefined);
              await Promise.race([
                reached.promise,
                committing.then(() => {
                  throw new Error("Expected a gated FTS write");
                }),
              ]);
              expect(metadata?.allocated).toBeGreaterThan(
                FTS_WORKSPACE_BYTES + FTS_APPEND_WORKSPACE_BYTES,
              );
              closing = path.close().then(() => {
                closed = true;
              });
              void closing.catch(() => undefined);
              await yieldTask();
              await yieldTask();
              expect(closed).toBe(false);
              expect(metadata?.allocated).toBeGreaterThan(
                FTS_WORKSPACE_BYTES + FTS_APPEND_WORKSPACE_BYTES,
              );
              gate.resolve();
              await expect(committing).rejects.toMatchObject({ code: "MINNOW_WRITE_PATH_CLOSED" });
              await closing;
              expect(metadata?.allocated).toBe(0);
              reach.reach(("write " + String(k)) as "write 0" | "write 1" | "write 2" | "write 3");
              reach.reach("retained across close");
              reach.reach("drained");
            } finally {
              active = false;
              closing ??= opened?.close();
              void closing?.catch(() => undefined);
              gate.resolve();
              await Promise.allSettled(
                [committing, closing].filter((p): p is Promise<unknown> => p !== undefined),
              );
              await h.close();
              retire = undefined;
            }
          }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);
