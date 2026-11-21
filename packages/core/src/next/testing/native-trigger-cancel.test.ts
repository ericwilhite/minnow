/** Cancellation at actual native root checkpoints: no unretired effects may commit. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_CANCEL_REACH } from "../../testing/reach.js";
import { StatementImpl } from "../write/transaction.js";
import { runTriggerStatement } from "../owner/trigger-statement.js";
import { commandRootFixture } from "./command-root.test-support.js";
import { orderingScript } from "./trigger-ordering.test-support.js";
import { oracleRows } from "./differential-adapter.js";
function containsReason(value: unknown, reason: unknown): boolean {
  return (
    value === reason ||
    (value instanceof AggregateError &&
      value.errors.some((error: unknown) => containsReason(error, reason))) ||
    (value instanceof Error && containsReason(value.cause, reason))
  );
}

async function freshScope(f: Awaited<ReturnType<typeof commandRootFixture>>) {
  await f.root.close();
  f.statement.rollback();
  const statement = f.tx.statement({
    signal: f.scope.signal,
    rules: (id) => {
      const found = f.scope.tables.find((entry) => entry.table.id === id);
      if (!found) throw new Error("Missing native cancellation rules");
      return found.rules;
    },
  });
  if (!(statement instanceof StatementImpl)) throw new Error("Native statement required");
  return { ...f.scope, statement };
}
function source(rows: number, published: boolean) {
  const h = orderingScript(
    {
      rows,
      event: "update",
      beforeNames: ["p_before"],
      afterNames: ["zz_p_after"],
      twoBodies: true,
      reverseCreation: false,
      published,
    },
    "accumulator",
  );
  return { setup: [...h.nativeSetup, ...h.seed], sql: h.mutation };
}
const queries = [
  "SELECT * FROM p ORDER BY id",
  "SELECT * FROM c ORDER BY id",
  "SELECT label,n FROM log ORDER BY label,n",
  "SELECT label,n FROM audit ORDER BY label,n",
  "SELECT * FROM trace",
];
for (const seed of seedsFor("next-native-trigger-cancel", [0xb7ec, 0xb7ed])) {
  const reach = createReach("native trigger cancellation", NATIVE_TRIGGER_CANCEL_REACH);
  for (const cut of ["first", "middle", "last"] as const)
    it(`undoes native cancellation seed ${String(seed)}, ${cut}`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 4 }),
          fc.boolean(),
          fc.boolean(),
          async (rows, published, ioError) => {
            reach.beginHistory();
            const h = source(rows, published);
            const baseline = await commandRootFixture(h.setup, h.sql, published, 64 << 20, true);
            let checkpoints = 0;
            try {
              const scope = await freshScope(baseline);
              await runTriggerStatement(
                {
                  ...scope,
                  checkpoint: () => {
                    checkpoints++;
                  },
                },
                { slots: [] },
              );
              expect(checkpoints).toBeGreaterThan(2);
            } finally {
              await baseline.close();
            }
            const target =
                cut === "first" ? 1 : cut === "last" ? checkpoints : Math.ceil(checkpoints / 2),
              f = await commandRootFixture(h.setup, h.sql, published, 64 << 20, true),
              expected = new Map<string, unknown>(),
              reason = new Error(
                ioError ? "injected checkpoint failure" : "injected native cancellation",
              );
            let visited = 0;
            try {
              const scope = await freshScope(f);
              f.scope.funding.releaseIdleStep();
              for (const sql of queries) {
                const result = await f.core.execute(sql);
                if (!("batches" in result)) throw new Error("Expected baseline SELECT");
                expected.set(sql, oracleRows(result));
              }
              await f.scope.funding.prepareMetadata(f.setupBytes + (2 << 20), f.scope.signal);
              const work = runTriggerStatement(
                {
                  ...scope,
                  checkpoint: () => {
                    if (++visited === target) {
                      if (ioError) throw reason;
                      f.stop.abort(reason);
                    }
                    f.scope.signal.throwIfAborted();
                  },
                },
                { slots: [] },
              );
              const failure = await work.catch((error: unknown) => error);
              // Cleanup can report an AggregateError while preserving its primary cause.
              expect(
                containsReason(failure, reason),
                "cancellation reason survives cleanup aggregates",
              ).toBe(true);
              expect(visited).toBe(target);
              const after = f.core.harness.path.stats();
              expect(
                f.core.harness.path.shared.readers.count,
                "retired native reader registrations",
              ).toBe(0);
              expect(f.core.harness.path.shared.readers.heldPins).toBe(0);
              expect(after.borrowedChunkBytes).toBe(0);
              expect(after.repointedReaders).toBe(0);
              expect(after.repointPins).toBe(0);
              const commit = f.tx.commit(null);
              await expect(
                commit,
                "failed root has no surviving undo/effect",
              ).resolves.toMatchObject({ wrote: false });
              await (
                await commit
              ).durable;
              f.scope.funding.releaseIdleStep();
              for (const sql of queries) {
                const result = await f.core.execute(sql);
                if (!("batches" in result)) throw new Error("Expected rollback SELECT");
                expect(oracleRows(result), `cancel exact rows ${sql}`).toEqual(expected.get(sql));
              }
              reach.reach(cut);
              reach.reach(ioError ? "checkpoint error" : "AbortSignal cancellation");
              reach.reach(published ? "published" : "resident");
              reach.reach("empty commit after statement undo");
              reach.reach("exact parent RI and nested rows");
              reach.reach("prefix registrations retired");
            } finally {
              await f.close();
              expect(f.core.harness.path.stats()).toMatchObject({
                memtableBytes: 0,
                frozenMemtableBytes: 0,
                borrowedChunkBytes: 0,
                repointPins: 0,
                pins: 0,
              });
              expect(
                f.core.harness.path.shared.readers.count,
                "retired native reader registrations",
              ).toBe(0);
              expect(f.core.harness.path.shared.readers.heldPins).toBe(0);
            }
            reach.reach("zero native ownership");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [2, false, false],
            [4, true, true],
          ],
        },
      );
    }, 120000);
  it(`meets native cancellation Reach seed ${String(seed)}`, () => reach.assertReached());
}
