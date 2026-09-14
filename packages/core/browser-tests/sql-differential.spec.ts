import { expect } from "@playwright/test";
import { seedsFor } from "../src/testing/seeds.js";
import { requireWorkerOpfs, test } from "./fixtures.js";

type StoreKind = "indexeddb" | "opfs";

interface BrowserSqlDifferentialResult {
  readonly seeds: readonly number[];
  readonly versions: Record<"minnow" | "sqlite" | "pglite", string>;
  readonly generatedQueries: number;
  readonly fixedQueries: number;
  readonly mutations: number;
  readonly oracleComparisons: number;
  readonly matrix: {
    readonly entries: number;
    readonly supported: number;
    readonly compatibleReadsCompared: number;
    readonly compatibleReadAcceptance: number;
    readonly nonportableReadsAccepted: number;
    readonly compatibleMutationsCompared: number;
    readonly compatibleWritesAccepted: number;
    readonly nonportableWritesAccepted: number;
    readonly unsupportedRejected: number;
  };
  readonly failures: readonly string[];
  readonly workerErrors: readonly string[];
  readonly elapsedMs: number;
}

/**
 * Runs Minnow, SQLite Wasm, and PGlite inside each supported browser engine.
 *
 * The custom corpus sends the same seeded fixtures, parameters, ordered reads, and stateful
 * RETURNING mutations to all three engines. The feature-matrix pass then executes every supported
 * read in a published Minnow worker; PostgreSQL-compatible deterministic examples are compared
 * with PGlite, while documented differences and extensions are acceptance checked. Every
 * supported write runs against a fresh durable fixture; compatible mutations also compare their
 * affected count, RETURNING rows, and resulting state with PGlite. Every documented unsupported
 * example must produce its recorded error without changing the fixture.
 */
for (const store of ["indexeddb", "opfs"] as const satisfies readonly StoreKind[]) {
  for (const seed of seedsFor("sql-conformance", [0xc0ffee])) {
    test(`${store}: native SQL differential and feature matrix (seed ${String(seed)})`, async ({
      storageContext,
    }, info) => {
      test.setTimeout(360_000);
      const page = await storageContext.newPage();
      const browserErrors: string[] = [];
      page.on("console", (message) => {
        if (message.type() === "error") browserErrors.push(message.text());
      });
      page.on("pageerror", (error) => browserErrors.push(error.message));

      await page.goto("/packages/core/browser/sql-differential/");
      await expect(page.locator("#ready")).toHaveText("Browser SQL differential runner ready");
      if (store === "opfs") await requireWorkerOpfs(page);
      const result = await page.evaluate(
        async (input) => {
          const target = window as typeof window & {
            runBrowserSqlDifferential(request: {
              seeds: readonly number[];
              store: StoreKind;
            }): Promise<BrowserSqlDifferentialResult>;
          };
          return target.runBrowserSqlDifferential(input);
        },
        { seeds: [seed], store },
      );

      await info.attach("sql-differential.json", {
        contentType: "application/json",
        body: JSON.stringify(
          {
            browser: info.project.name,
            store,
            replay: `MINNOW_SEED=${String(seed)} npm run test:browser:conformance -- sql-differential.spec.ts --project=${info.project.name}`,
            ...result,
            elapsedMs: Math.round(result.elapsedMs),
          },
          undefined,
          2,
        ),
      });

      expect(browserErrors).toEqual([]);
      expect(result.workerErrors).toEqual([]);
      expect(result.failures).toEqual([]);
      expect(result.seeds).toEqual([seed]);
      expect(result.versions.minnow).not.toBe("");
      expect(result.versions.sqlite).toMatch(/^3\./);
      expect(result.versions.pglite).not.toBe("");
      expect(result.fixedQueries).toBe(10);
      expect(result.generatedQueries).toBe(42);
      expect(result.mutations).toBe(4);
      // 7 fixed cases hit both oracles, 3 PostgreSQL-only cases hit PGlite, and every generated
      // query, mutation, and per-mutation post-image hits both independent engines.
      expect(result.oracleComparisons).toBe(117);
      // These are the committed matrix/profile populations. Exact counts make removing examples
      // or routing one around execution a visible review change instead of silently weakening CI.
      expect(result.matrix).toEqual({
        entries: 366,
        supported: 305,
        compatibleReadsCompared: 209,
        compatibleReadAcceptance: 1,
        nonportableReadsAccepted: 28,
        compatibleMutationsCompared: 24,
        compatibleWritesAccepted: 36,
        nonportableWritesAccepted: 7,
        unsupportedRejected: 61,
      });
      expect(result.matrix.supported + result.matrix.unsupportedRejected).toBe(
        result.matrix.entries,
      );
      // Every supported feature must land in exactly one verification category. This fails if a
      // matrix example is silently skipped before its category's counter advances.
      expect(
        result.matrix.compatibleReadsCompared +
          result.matrix.compatibleReadAcceptance +
          result.matrix.nonportableReadsAccepted +
          result.matrix.compatibleMutationsCompared +
          result.matrix.compatibleWritesAccepted +
          result.matrix.nonportableWritesAccepted,
      ).toBe(result.matrix.supported);
      await page.close();
    });
  }
}
