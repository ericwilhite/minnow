/** Statement signal cancels newly required FTS metadata admission, not only row/input waits. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_ADMISSION_CANCEL_REACH } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  TEXT,
  batch,
  texts,
  locators,
  rules,
  rowsOf,
} from "../write/test-support.js";
import { yieldTask } from "../task.js";
import type { Grant } from "../memory/governor.js";
for (const seed of seedsFor("next-fts-admission-cancel", [0xb861, 0xb862]))
  it(`cancels FTS admission while competing credit remains held ${String(seed)}`, async (context) => {
    const reach = createReach("FTS admission cancel", FTS_ADMISSION_CANCEL_REACH);
    let stopped = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      stopped = true;
      retire?.();
    });
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (n) => {
        reach.beginHistory();
        for (const kind of ["update", "delete", "upsert"] as const)
          for (const mode of ["wait", "resume"] as const)
            for (const source of ["options", "end"] as const) {
              if (stopped) throw new Error("Retired FTS admission cancellation history");
              const h = new Harness(),
                optionController = new AbortController(),
                endController = new AbortController(),
                controller = source === "options" ? optionController : endController;
              let competitor: Grant | undefined, pending: Promise<void> | undefined;
              let opened: Harness["path"] | undefined;
              const release = () => {
                if (competitor) {
                  competitor.free(competitor.allocated);
                  competitor.close();
                  competitor = undefined;
                }
              };
              retire = () => {
                controller.abort(new Error("Retired FTS admission"));
                release();
                void opened?.close().catch(() => undefined);
              };
              try {
                const path = await h.open();
                opened = path;
                await autocommit(path, (s) =>
                  s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("body", TEXT)], { primaryKey: null }),
                  }),
                );
                const values = Array.from({ length: n }, (_, i) => `alpha ${String(i)}`);
                await autocommit(path, (s) => s.table(1).insert(batch(texts(values)), null));
                const tx = await path.begin(),
                  s = tx.statement({ rules, signal: optionController.signal });
                try {
                  if (kind === "update")
                    await s
                      .table(1, { assigned: [1] })
                      .update(batch(locators([0], 1), texts(["beta"])), null);
                  else if (kind === "delete")
                    await s.table(1).delete(batch(locators([0], 1)), null);
                  else {
                    await s.table(1).insert(batch(texts(["beta"])), null);
                    s.noteFtsMutation(1, "upsert");
                  }
                  competitor = await h.governor.admit({
                    label: "FTS end cancellation competitor",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  while (competitor.tryGrow(64 * 1024)) {
                    /* Hold ordinary credit through cancellation. */
                  }
                  expect(competitor.allocate(competitor.held)).toBe(true);
                  const abort = new Error("FTS admission cancelled");
                  const nativePrepare = path.prepareFtsMutation.bind(path);
                  if (mode === "resume")
                    path.prepareFtsMutation = (signal) =>
                      Promise.resolve(nativePrepare(signal)).then(() => {
                        controller.abort(abort);
                      });
                  let state = "pending",
                    reason: unknown;
                  pending = s.end(endController.signal);
                  void pending.then(
                    () => {
                      state = "ended";
                    },
                    (e: unknown) => {
                      state = "rejected";
                      reason = e;
                    },
                  );
                  await yieldTask();
                  expect(state).toBe("pending");
                  reach.reach("actual admission wait");
                  if (mode === "wait") {
                    controller.abort(abort);
                    await yieldTask();
                    await yieldTask();
                    expect(
                      state,
                      "statement abort must reject before competing credit is released",
                    ).toBe("rejected");
                    expect(competitor.allocated).toBeGreaterThan(0);
                    reach.reach("abort while credit held");
                  } else {
                    release();
                    await yieldTask();
                    await yieldTask();
                    expect(
                      state,
                      "statement abort after admission must reject before successful end",
                    ).toBe("rejected");
                    reach.reach("abort on admission resume");
                  }
                  expect(reason).toBe(abort);
                  path.prepareFtsMutation = nativePrepare;
                  await Promise.allSettled([pending]);
                  s.rollback();
                  // Failed admission must not leave an UNFUNDED minimum in the same transaction.
                  // Reject an unexpected fresh-credit attempt immediately, never by timeout.
                  const nativeAdmit = h.governor.admitGroup.bind(h.governor);
                  let renewals = 0,
                    continuationError: unknown;
                  h.governor.admitGroup = () => {
                    renewals++;
                    return Promise.reject(
                      new Error("Unfunded cancelled FTS minimum requested credit"),
                    );
                  };
                  try {
                    const next = tx.statement({ rules });
                    try {
                      await next.table(1).insert(batch(texts(["continuation"])), null);
                      await next.end();
                    } catch (error) {
                      continuationError = error;
                    }
                    expect(
                      renewals,
                      "ordinary work after rollback must not readmit the cancelled FTS minimum",
                    ).toBe(0);
                    expect(continuationError).toBeUndefined();
                    reach.reach("same transaction usable");
                  } finally {
                    h.governor.admitGroup = nativeAdmit;
                  }
                  tx.rollback();
                  release();
                  const pin = path.pin();
                  try {
                    expect(await rowsOf(pin, 1)).toEqual(values.map((v) => [v]));
                  } finally {
                    pin.release();
                  }
                  expect(path.state().root.fts).toEqual({ checkpoint: null, staged: null });
                  reach.reach("no FTS preparation");
                  await autocommit(path, (st) => st.table(1).insert(batch(texts(["gamma"])), null));
                  expect(path.failure).toBeNull();
                  reach.reach("writer usable");
                  reach.reach(kind);
                  reach.reach(source);
                } finally {
                  release();
                  await Promise.allSettled(pending ? [pending] : []);
                  tx.rollback();
                }
              } finally {
                controller.abort();
                release();
                await Promise.allSettled(pending ? [pending] : []);
                await h.close();
                retire = undefined;
              }
            }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 120_000);
