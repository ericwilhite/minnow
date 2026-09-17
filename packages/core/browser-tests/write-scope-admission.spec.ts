import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

type Runner = typeof import("../browser/write-scope-admission-run.js");

for (const store of ["indexeddb", "opfs"] as const) {
  test(`${store}: concurrent scopes and autocommit load every table and survive reopening`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    if (store === "opfs") await requireWorkerOpfs(page);
    const result = await page.evaluate(async (kind) => {
      const url = "/packages/core/browser/write-scope-admission-run.ts";
      return (
        (await import(url)) as typeof import("../browser/write-scope-admission-run.js")
      ).runWriteScopeAdmission(kind);
    }, store);
    expect(result.callbacks).toBe(45);
    expect(result.completed).toEqual([{ n: 45 }]);
    expect(result.counts).toEqual(
      Array.from({ length: 45 }, (_, table) => ({
        n: 867,
        ids: (867 * 866) / 2,
        values: table * 867,
      })),
    );
  });

  test(`${store}: three tabs write, migrate and hold turns over one database without conflicts`, async ({
    storageContext,
  }) => {
    test.setTimeout(180_000);
    const tabs = await Promise.all([1, 2, 3].map(() => storageContext.newPage()));
    await Promise.all(tabs.map((tab) => tab.goto("/packages/core/browser/")));
    const probe = tabs[0];
    if (probe === undefined) throw new Error("Expected three tabs");
    if (store === "opfs") await requireWorkerOpfs(probe);
    const name = `shared-admission-${crypto.randomUUID()}`;
    const TABLES = 24;
    const ROWS = 400;
    for (const tab of tabs) {
      await tab.evaluate(
        async ({ kind, name }) => {
          const url = "/packages/core/browser/write-scope-admission-run.ts";
          await ((await import(url)) as Runner).openShared(kind, name);
        },
        { kind: store, name },
      );
    }
    await tabs[0]?.evaluate(async (tables) => {
      const url = "/packages/core/browser/write-scope-admission-run.ts";
      await ((await import(url)) as Runner).initializeShared(tables);
    }, TABLES);

    // Every tab bursts at once over disjoint table ranges; the middle tab also changes the
    // schema mid-burst. Zero commit retries: any conflict is a coordination failure.
    const shares = [0, 1, 2].map((tab) =>
      Array.from({ length: TABLES / 3 }, (_, index) => tab * (TABLES / 3) + index),
    );
    const [bursts, migration] = await Promise.all([
      Promise.all(
        tabs.map((tab, index) =>
          tab.evaluate(
            async ({ tab, tables, rows }) => {
              const url = "/packages/core/browser/write-scope-admission-run.ts";
              return ((await import(url)) as Runner).burstShared({
                tab,
                concurrency: 4,
                tables,
                rowsPerTable: rows,
              });
            },
            { tab: index, tables: shares[index] ?? [], rows: ROWS },
          ),
        ),
      ),
      (async () => {
        await new Promise((resolve) => setTimeout(resolve, 300));
        return tabs[1]?.evaluate(async () => {
          const url = "/packages/core/browser/write-scope-admission-run.ts";
          return ((await import(url)) as Runner).migrateShared("note");
        });
      })(),
    ]);
    expect(migration).toBeNull();
    for (const burst of bursts) {
      expect(burst.conflicts).toEqual([]);
      expect(burst.callbacks).toBe(TABLES / 3);
    }
    // Every counter read-modify-write saw a distinct value: no lost update anywhere.
    const reads = bursts.flatMap((burst) => burst.counterReads).sort((a, b) => a - b);
    expect(reads).toEqual(Array.from({ length: reads.length }, (_, index) => index));

    const verified = await tabs[2]?.evaluate(
      async ({ tables, rows }) => {
        const url = "/packages/core/browser/write-scope-admission-run.ts";
        return ((await import(url)) as Runner).verifyShared({ tables, rowsPerTable: rows });
      },
      { tables: TABLES, rows: ROWS },
    );
    expect(verified?.counter).toEqual({ value: reads.length });
    expect(verified?.ledger).toEqual({ n: TABLES, applied: TABLES });
    expect(verified?.tables).toEqual(
      Array.from({ length: TABLES }, (_, table) => ({
        n: ROWS,
        ids: (ROWS * (ROWS - 1)) / 2,
        values: table * ROWS,
        tabs: 1,
      })),
    );
    expect(verified?.columns).toEqual(["id", "tab", "n", "note"]);
    expect(verified?.stalls).toBe(0);

    // A tab that parks inside a scope holds the turn: another tab's write waits rather than
    // conflicting, and closing that waiting tab lets go at once without waiting for the holder.
    await tabs[0]?.evaluate(async () => {
      const url = "/packages/core/browser/write-scope-admission-run.ts";
      await ((await import(url)) as Runner).holdSharedScope();
    });
    const held = await tabs[1]?.evaluate(async () => {
      const url = "/packages/core/browser/write-scope-admission-run.ts";
      return ((await import(url)) as Runner).writeWhileHeld(1_500);
    });
    expect(held?.waited).toBe(true);
    expect(held?.outcome).not.toBe("published");
    expect(held?.closeMs).toBeLessThan(5_000);
    await tabs[0]?.evaluate(async () => {
      const url = "/packages/core/browser/write-scope-admission-run.ts";
      await ((await import(url)) as Runner).releaseSharedScope();
    });
    const after = await tabs[2]?.evaluate(async () => {
      const url = "/packages/core/browser/write-scope-admission-run.ts";
      const runner = (await import(url)) as Runner;
      const rows = (await runner.verifyShared({ tables: 0, rowsPerTable: 0 })).ledger;
      await runner.closeShared();
      return rows;
    });
    // The held scope's row landed; the cancelled write from the closed tab did not.
    expect(after).toEqual({ n: TABLES + 1, applied: TABLES });
    await tabs[0]?.evaluate(async () => {
      const url = "/packages/core/browser/write-scope-admission-run.ts";
      await ((await import(url)) as Runner).closeShared();
    });
  });
}
