/** Accepted notification/rollback law; table history is kept even before an index exists. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_MUTATION_HISTORY_REACH } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  batch,
  texts,
  locators,
  rules,
  rowsOf,
} from "../write/test-support.js";
import { setupFts, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";
import type { Grant } from "../memory/governor.js";
for (const seed of seedsFor("next-fts-mutation-history", [0xb851, 0xb852]))
  it(`keeps accepted mutation history through rollbacks and missing declarations ${String(seed)}`, async (context) => {
    const reach = createReach("FTS mutation history", FTS_MUTATION_HISTORY_REACH);
    let stopped = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      stopped = true;
      retire?.();
    });
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.shuffledSubarray(["upsert", "savepoint", "statement", "empty", "delete"] as const, {
          minLength: 5,
          maxLength: 5,
        }),
        async (pressure, order) => {
          reach.beginHistory();
          for (const hasDefinition of [false, true])
            for (const mode of order) {
              if (stopped) throw new Error("Retired FTS mutation history");
              const h = new Harness();
              let competitor: Grant | undefined;
              let opened: Harness["path"] | undefined;
              const release = () => {
                if (competitor) {
                  competitor.free(competitor.allocated);
                  competitor.close();
                  competitor = undefined;
                }
              };
              retire = () => {
                release();
                void opened?.close().catch(() => undefined);
              };
              try {
                const path = await setupFts(h);
                opened = path;
                if (!hasDefinition)
                  await autocommit(path, (s) => s.changeCatalog({ kind: "dropFts", ftsId: 1 }));
                await autocommit(path, (s) => s.table(1).insert(batch(texts(["alpha"])), null));
                const before = path.state().root.fts.staged,
                  tx = await path.begin(),
                  point = tx.savepoint();
                const s = tx.statement({ rules });
                if (mode === "delete") await s.table(1).delete(batch(locators([0], 1)), null);
                else if (mode !== "empty") await s.table(1).insert(batch(texts(["beta"])), null);
                s.noteFtsMutation(1, mode === "delete" ? "delete" : "upsert");
                s.noteFtsMutation(1, "upsert");
                if (mode === "statement") s.rollback();
                else await s.end();
                if (mode === "savepoint") tx.rollbackTo(point);
                if (pressure) {
                  competitor = await h.governor.admit({
                    label: "FTS mutation competitor",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  while (competitor.tryGrow(64 * 1024)) {
                    // Exhaust ordinary credit after the complete publication preflight.
                  }
                  expect(competitor.allocate(competitor.held)).toBe(true);
                }
                const receipt = await tx.commit(null);
                await receipt.durable;
                const mutated = mode === "upsert" || mode === "delete";
                expect(receipt.wrote).toBe(mutated);
                if (mutated) {
                  const logged = await lastFtsCommit(h);
                  expect(
                    logged.body.effects.some((effect) => effect.kind === "ftsMutation"),
                    "reference-free keyed history is protected by its atomic WAL body",
                  ).toBe(true);
                  expect((await ftsParts(h)).entries).toMatchObject([
                    { kind: "keyed", tableId: 1, ftsId: 0 },
                  ]);
                  expect(
                    (await lastFtsCommit(h)).body.effects.filter((e) => e.kind === "ftsMutation"),
                  ).toHaveLength(1);
                  reach.reach("accepted");
                  if (!hasDefinition) reach.reach("no definition");
                  else reach.reach("definition");
                } else {
                  expect(path.state().root.fts.staged).toEqual(before);
                  reach.reach("no publication");
                }
                release();
                await path.recycleLog();
                await h.reopen();
                if (mutated)
                  expect((await ftsParts(h)).entries.some((e) => e.kind === "keyed")).toBe(true);
                const pin = h.path.pin();
                try {
                  expect(await rowsOf(pin, 1)).toEqual(
                    mode === "delete"
                      ? []
                      : mode === "upsert"
                        ? [["alpha"], ["beta"]]
                        : [["alpha"]],
                  );
                } finally {
                  pin.release();
                }
                reach.reach(mode);
                if (pressure) reach.reach("competing grant");
                reach.reach("reopen");
              } finally {
                release();
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
