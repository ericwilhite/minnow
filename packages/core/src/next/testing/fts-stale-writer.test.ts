/** Missing/stale semantic write capability invalidates acceleration, not accepted rows.
 * Stored unsupported versions still refuse on open; this law is about a writer adapter.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_STALE_WRITER_REACH } from "../../testing/reach.js";
import { Harness, batch, texts, rules, rowsOf } from "../write/test-support.js";
import { setupFts, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import { ftsDef } from "../access/fts-def.js";
for (const seed of seedsFor("next-fts-stale-writer", [0xb859, 0xb85a]))
  it(`invalidates stale writer capabilities without refusing accepted rows ${String(seed)}`, async () => {
    const reach = createReach("FTS stale writer", FTS_STALE_WRITER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray(["tokenizer", "renderer", "scorer", "config", "workspace"] as const, {
          minLength: 4,
          maxLength: 4,
        }),
        async (order) => {
          reach.beginHistory();
          for (const mode of [
            "before install",
            "before commit",
            "during prepare",
            "late declaration",
          ] as const)
            for (const field of order) {
              const h = new Harness();
              try {
                const path = await setupFts(h, false),
                  adapter = {
                    ...ftsSemantics(),
                    config: { version: 1 },
                    workspaceBytes: FTS_WORKSPACE_BYTES,
                    render: ftsValue,
                  };
                const stale = () => {
                  if (field === "workspace") adapter.workspaceBytes = -1;
                  else if (field === "config") adapter.config.version++;
                  else if (field === "tokenizer") adapter.tokenizerVersion++;
                  else if (field === "renderer") adapter.rendererVersion++;
                  else adapter.scorerVersion++;
                };
                if (mode === "before install") stale();
                path.installFtsRenderer(adapter);
                const tx = await path.begin(),
                  s = tx.statement({ rules }),
                  writer = s.table(1);
                if (mode === "late declaration") {
                  await s.changeCatalog({ kind: "dropFts", ftsId: 1 });
                  const { id, ...fts } = ftsDef(2, 1, 1);
                  void id;
                  await s.changeCatalog({ kind: "createFts", fts });
                }
                await writer.insert(batch(texts(["alpha alpha beta"])), null);
                await s.end();
                if (mode === "before commit") stale();
                let changed = false;
                if (mode === "during prepare") {
                  const begin = h.space.beginWrite.bind(h.space);
                  h.space.beginWrite = (label) => {
                    const out = begin(label),
                      write = out.write.bind(out);
                    if (label === "FTS preparation")
                      out.write = async (parts) => {
                        const result = await write(parts);
                        if (!changed) {
                          changed = true;
                          stale();
                        }
                        return result;
                      };
                    return out;
                  };
                }
                const receipt = await tx.commit(null);
                await receipt.durable;
                const entries = (await ftsParts(h)).entries;
                if (mode === "late declaration") {
                  expect(entries).toHaveLength(1);
                  expect(entries[0]).toMatchObject({
                    kind: "insert",
                    ftsId: 2,
                    generation: 1,
                    mutationEpoch: 0,
                  });
                  reach.reach("late declaration");
                } else {
                  expect(
                    entries.some((e) => e.kind === "invalidate" && e.ftsId === 1),
                    "stale writer must invalidate in its row commit",
                  ).toBe(true);
                  reach.reach(mode);
                  reach.reach(field);
                  if (mode === "during prepare") {
                    expect(changed).toBe(true);
                    expect(entries.some((e) => e.kind === "insert")).toBe(true);
                    reach.reach("partial parts dominated");
                  }
                }
                const logged = await lastFtsCommit(h);
                expect(logged.body.effects.filter((e) => e.kind === "fts")).toHaveLength(1);
                reach.reach("same body");
                await path.recycleLog();
                await h.reopen();
                const pin = h.path.pin();
                try {
                  expect(await rowsOf(pin, 1)).toEqual([["alpha alpha beta"]]);
                } finally {
                  pin.release();
                }
                expect((await ftsParts(h)).entries).toEqual(entries);
                reach.reach("rows and history survive");
              } finally {
                await h.close();
              }
            }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);
