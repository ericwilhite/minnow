/** The append LSN, not preparation completion, seals optional FTS capabilities. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_PUBLICATION_SEAL_REACH } from "../../testing/reach.js";
import { Harness, batch, texts, rules, rowsOf } from "../write/test-support.js";
import { setupFts, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import { LogAppender } from "../write/log.js";
import { SMALL_COMMIT_BYTES } from "../write/commit-buffers.js";

for (const boundary of ["preflight", "streaming", "deferred append"] as const)
  for (const seed of seedsFor("next-fts-publication-seal", [0xb87d, 0xb87e]))
    it(`seals ${boundary} at serialized publication seed ${String(seed)}`, async (context) => {
      const reach = createReach("FTS publication seal", FTS_PUBLICATION_SEAL_REACH);
      let stopped = false,
        retire: (() => void) | undefined;
      context.onTestFinished(() => {
        stopped = true;
        retire?.();
      });
      const checkActive = () => {
        if (stopped) throw new Error("Retired FTS publication history");
      };
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("renderer", "tokenizer", "scorer", "config", "workspace"),
          fc.integer({ min: 1, max: 3 }),
          fc.boolean(),
          async (field, n, stale) => {
            checkActive();
            reach.beginHistory();
            const h = new Harness(),
              adapter = {
                ...ftsSemantics(),
                config: { version: 1 },
                workspaceBytes: FTS_WORKSPACE_BYTES,
                render: ftsValue,
              };
            let changed = false,
              armed = false;
            const change = () => {
              if (!armed || stopped || changed) return;
              changed = true;
              if (!stale) return;
              if (field === "renderer") adapter.rendererVersion++;
              else if (field === "tokenizer") adapter.tokenizerVersion++;
              else if (field === "scorer") adapter.scorerVersion++;
              else if (field === "config") adapter.config.version++;
              else adapter.workspaceBytes *= 2;
            };
            let restore = () => {
              armed = false;
            };
            const cleanup = () => {
              const undo = restore;
              restore = () => {
                armed = false;
              };
              undo();
            };
            retire = () => {
              cleanup();
              void h.close().catch(() => undefined);
            };
            try {
              const path = await setupFts(h, false);
              path.installFtsRenderer(adapter);
              const tx = await path.begin(),
                statement = tx.statement({ rules });
              const values = Array.from({ length: n }, (_, i) => `alpha beta ${String(i)}`);
              await statement.table(1).insert(batch(texts(values)), null);
              await statement.end();
              checkActive();
              const workspace = tx.commitWorkspaceBytes.bind(tx);
              if (boundary === "preflight") {
                tx.commitWorkspaceBytes = () => {
                  armed = true;
                  queueMicrotask(change);
                  return workspace();
                };
                restore = () => {
                  armed = false;
                  tx.commitWorkspaceBytes = workspace;
                };
              } else if (boundary === "streaming") {
                // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked only with explicit original.call(this)
                const original = LogAppender.prototype.appendStream;
                const spy = vi
                  .spyOn(LogAppender.prototype, "appendStream")
                  .mockImplementation(function (this: LogAppender, length, fill, seal) {
                    return original.call(
                      this,
                      length,
                      async (out, offset) => {
                        await fill(out, offset);
                        armed = true;
                        change();
                      },
                      seal,
                    );
                  });
                // Use a real large request to force native streaming, not an alternate codec.
                restore = () => {
                  armed = false;
                  spy.mockRestore();
                };
              } else {
                // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked only with explicit original.call(this)
                const original = LogAppender.prototype.append;
                const spy = vi.spyOn(LogAppender.prototype, "append").mockImplementation(function (
                  this: LogAppender,
                  body,
                  seal,
                ) {
                  return Promise.resolve().then(() => {
                    armed = true;
                    change();
                    return original.call(this, body, seal);
                  });
                });
                restore = () => {
                  armed = false;
                  spy.mockRestore();
                };
              }
              const request =
                boundary === "deferred append"
                  ? null
                  : {
                      clientId: new Uint8Array(16),
                      sequence: 1,
                      sentAtMs: 0,
                      counts: {
                        ["x".repeat(boundary === "streaming" ? SMALL_COMMIT_BYTES + 64 : 100000)]:
                          n,
                      },
                    };
              const receipt = await tx.commit(request);
              cleanup();
              await receipt.durable;
              expect(changed, "selected publication boundary must run").toBe(true);
              const commit = await lastFtsCommit(h);
              const claim = commit.body.effects.find((e) => e.kind === "fts");
              reach.reach(stale ? "late retirement invalidated" : "valid claim preserved");
              expect(commit.body.effects.some((e) => e.kind === "rows")).toBe(true);
              reach.reach("same row body");
              expect(path.state().root.fts.checkpoint).toBeNull();
              await h.reopen(); // Native close drains IO, but this live table is not checkpointed.
              expect(
                h.path.state().root.fts.staged,
                "replay must see the uncheckpointed claim",
              ).not.toBeNull();
              await h.path.recycleLog();
              const entries = (await ftsParts(h)).entries;
              expect(
                entries.some((e) => e.kind === "invalidate"),
                "stale renderer must not replay an insert-only claim",
              ).toBe(stale);
              // Independently pin the durable WAL disposition as well as checkpoint output.
              if (stale)
                expect(
                  claim,
                  "retirement must be sealed in the SAME durable row claim",
                ).toMatchObject({ invalidated: true });
              reach.reach("replayed disposition");
              await h.reopen();
              const pin = h.path.pin();
              try {
                expect(await rowsOf(pin, 1)).toEqual(values.map((v) => [v]));
              } finally {
                pin.release();
              }
              expect((await ftsParts(h)).entries.some((e) => e.kind === "invalidate")).toBe(stale);
              reach.reach("checkpointed disposition");
            } finally {
              cleanup();
              await h.close();
              retire = undefined;
            }
            expect(h.governor.usedBytes).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            ["renderer", 1, true],
            ["workspace", 3, true],
            ["config", 2, false],
          ],
        },
      );
      reach.assertReached();
    }, 60_000);
