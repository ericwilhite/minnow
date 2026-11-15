/** Keyed invalidation and accepted row effects share the same irreversible WAL boundary. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_KEYED_FAULT_REACH } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  batch,
  texts,
  locators,
  rules,
  rowsOf,
  watchGrants,
} from "../write/test-support.js";
import { setupFts, ftsParts } from "./fts-transaction.test-support.js";
for (const seed of seedsFor("next-fts-keyed-faults", [0xb85b, 0xb85c]))
  it(`keeps keyed rows and FTS invalidation atomic across publication faults ${String(seed)}`, async (context) => {
    let retired = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      retired = true;
      retire?.();
    });
    const reach = createReach("FTS keyed faults", FTS_KEYED_FAULT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 3 }), async (n) => {
        reach.beginHistory();
        for (const kind of ["update", "delete"] as const)
          for (const fault of ["prepare", "append", "none"] as const) {
            if (retired) throw new Error("Retired keyed FTS fault history");
            let armed = false;
            const h = new Harness("strict", "sim");
            const restores: Array<() => void> = [];
            watchGrants(h.governor, (grant) => {
              if (retired || grant.label !== "write path metadata") return;
              const allocate = grant.allocate.bind(grant);
              grant.allocate = (bytes) => {
                if (
                  armed &&
                  fault === "prepare" &&
                  (new Error().stack ?? "").includes("fts-mutations")
                )
                  throw new Error("FTS keyed preparation fault");
                return allocate(bytes);
              };
              restores.push(() => {
                grant.allocate = allocate;
              });
            });
            const restore = () => {
              armed = false;
              for (const undo of restores.splice(0)) undo();
            };
            retire = () => {
              restore();
              void h.close().catch(() => undefined);
            };
            const open = h.backend.open.bind(h.backend);
            h.backend.open = async (...args) => {
              const session = await open(...args),
                append = session.appendLog.bind(session);
              session.appendLog = (bytes) =>
                armed && fault === "append"
                  ? Promise.reject(new Error("FTS keyed append fault"))
                  : append(bytes);
              return session;
            };
            try {
              const path = await setupFts(h),
                values = Array.from({ length: n }, (_, i) => `alpha ${String(i)}`);
              await autocommit(path, (s) => s.table(1).insert(batch(texts(values)), null));
              const tx = await path.begin(),
                s = tx.statement({ rules });
              if (kind === "update")
                await s
                  .table(1, { assigned: [1] })
                  .update(batch(locators([0], 1), texts(["beta"])), null);
              else await s.table(1).delete(batch(locators([0], 1)), null);
              await s.end();
              armed = true;
              if (fault === "none") {
                const receipt = await tx.commit(null);
                await receipt.durable;
              } else
                await expect(tx.commit(null).then(async (r) => r.durable)).rejects.toThrow(
                  fault === "prepare" ? /FTS keyed preparation fault/ : /could not be made durable/,
                );
              armed = false;
              await h.reopen();
              await h.path.recycleLog();
              const pin = h.path.pin();
              try {
                expect(await rowsOf(pin, 1)).toEqual(
                  (fault !== "none"
                    ? values
                    : kind === "update"
                      ? [...values.slice(1), "beta"]
                      : values.slice(1)
                  ).map((v) => [v]),
                );
              } finally {
                pin.release();
              }
              expect((await ftsParts(h)).entries.some((e) => e.kind === "keyed")).toBe(
                fault === "none",
              );
              reach.reach(kind);
              reach.reach(fault);
              reach.reach("rows and invalidation agree");
              reach.reach("orphan retirement");
            } finally {
              restore();
              await h.close();
              retire = undefined;
            }
          }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);
