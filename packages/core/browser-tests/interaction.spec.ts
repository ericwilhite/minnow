import { expect } from "@playwright/test";
import type { Page } from "@playwright/test";
import {
  generateInteractionPlan,
  runInteractionPlan,
  type InteractionPlan,
  type PlanValue,
  type SimulatedConnection,
  type SimulatedExecuteResult,
  type SimulatedQueryResult,
  type SimulationDriver,
} from "@minnowdb/core/testing";
import {
  rehydrateError,
  serializeError,
  type SerializedError,
} from "@minnowdb/core/worker-protocol";
import { requireWorkerOpfs, test } from "./fixtures.js";

/**
 * The interaction-plan simulator across real tabs.
 *
 * The Node suite runs every seeded plan in one process over the memory, fake-IndexedDB and
 * OPFS-shim stores. This runs the same generator and the same shadow model with each plan
 * connection being a real page on the same origin, holding its own published worker over one
 * shared IndexedDB or OPFS database: concurrent rounds are genuinely concurrent across
 * processes, reopens are real worker restarts, and crash faults terminate a worker with a
 * statement in flight. What the plan checks -- statement atomicity, three-valued predicates,
 * LIMIT, TLP partitions, UNION ALL cardinality, transaction isolation across tabs, explicable
 * reads under concurrent writes, durability after a crash, cross-tab agreement -- is judged the
 * same way in both places; what differs is that here the browser is the one being judged.
 */
type StoreKind = "indexeddb" | "opfs";

const conformance = process.env.MINNOW_BROWSER_CONFORMANCE === "1";
const configuredSeedBase = process.env.MINNOW_BROWSER_INTERACTION_SEED_BASE;

function parseSeedBase(value: string | undefined): number | undefined {
  if (value === undefined) return undefined;
  const seed = Number(value);
  if (!Number.isSafeInteger(seed) || seed < 0 || seed > 0xffff_ffff) {
    throw new RangeError(
      "MINNOW_BROWSER_INTERACTION_SEED_BASE must be an integer from 0 through 4294967295",
    );
  }
  return seed;
}

const seedBase = parseSeedBase(configuredSeedBase);
const browserSeeds: Record<string, number> = {
  chromium: 0x5eed,
  firefox: 0xf1ef,
  webkit: 0x3eb5,
};
const browserSeedOffsets: Record<string, number> = {
  chromium: 0x9e37_79b9,
  firefox: 0x243f_6a88,
  webkit: 0xb7e1_5163,
};

function campaignSeed(browserName: string, campaign: "complete" | "crash"): number {
  const base =
    seedBase === undefined
      ? (browserSeeds[browserName] ?? browserSeeds.chromium ?? 0x5eed)
      : (seedBase + (browserSeedOffsets[browserName] ?? 0)) >>> 0;
  return campaign === "complete" ? (base ^ 0xa5a5_a5a5) >>> 0 : base;
}

function withoutFaults(plan: InteractionPlan): InteractionPlan {
  return {
    ...plan,
    interactions: plan.interactions.filter((interaction) => interaction.kind !== "fault"),
  };
}

interface TabFailure extends SerializedError {
  failed: true;
}

interface TabError {
  source: "window-error" | "unhandled-rejection" | "worker";
  error: SerializedError;
  kind?:
    | "uncaught"
    | "unhandled-rejection"
    | "messageerror"
    | "maintenance"
    | "coordination"
    | "transport";
  context?: string;
}

type PageDiagnostic =
  | { readonly source: "console"; readonly message: string }
  | { readonly source: "pageerror"; readonly error: SerializedError };

interface DiagnosticCollectionFailure {
  readonly phase: "read-tab-errors" | "close-tab";
  readonly page: number;
  readonly error: SerializedError;
}

function rethrow<T>(value: T | TabFailure): T {
  if (typeof value === "object" && value !== null && "failed" in value) {
    throw rehydrateError(value, new Map());
  }
  return value;
}

interface Tab {
  execute(sql: string, params?: PlanValue[]): Promise<SimulatedExecuteResult | TabFailure>;
  query(sql: string, params?: PlanValue[]): Promise<SimulatedQueryResult | TabFailure>;
  reopen(): Promise<TabFailure | undefined>;
  crash(): Promise<void>;
  maintain(table: string): Promise<TabFailure | undefined>;
  close(): Promise<TabFailure | undefined>;
  pageErrors(): TabError[];
}
type TabWindow = typeof window & { simulatorTab: Tab };

async function openTab(
  page: Page,
  kind: StoreKind,
  name: string,
  collect: (errors: TabError[]) => void,
): Promise<SimulatedConnection> {
  const load = async (): Promise<void> => {
    await page.goto("/packages/core/browser/interaction/");
    await expect(page.locator("#ready")).toHaveText("Interaction simulator tab ready");
    await page
      .evaluate(
        async ({ kind, name }) => {
          const target = window as typeof window & {
            simulatorTab: { open(k: string, n: string): Promise<TabFailure | undefined> };
          };
          return target.simulatorTab.open(kind, name);
        },
        { kind, name },
      )
      .then(rethrow);
  };
  await load();
  let crashed = false;
  // Each callback runs inside the page, so it must reach the tab through `window` itself.
  return {
    execute: async (sql, params) =>
      rethrow(
        await page.evaluate(
          ({ sql, params }) => (window as TabWindow).simulatorTab.execute(sql, params),
          { sql, params: params === undefined ? undefined : [...params] },
        ),
      ),
    query: async (sql, params) =>
      rethrow(
        await page.evaluate(
          ({ sql, params }) => (window as TabWindow).simulatorTab.query(sql, params),
          { sql, params: params === undefined ? undefined : [...params] },
        ),
      ),
    /**
     * A crash is recovered the way an application must recover one: by reloading the document.
     * A new worker in the same document is enough on Chromium and Firefox, but WebKit keeps the
     * terminated worker's IndexedDB connection -- and its unfinished transaction -- registered
     * until the document goes away, and until then every connection to that database blocks,
     * new ones in other tabs included. The engine bounds that wait and reports
     * `StorageUnresponsiveError` rather than hanging; the remedy it names is this reload.
     */
    reopen: async () => {
      if (!crashed) {
        rethrow(await page.evaluate(() => (window as TabWindow).simulatorTab.reopen()));
        return;
      }
      crashed = false;
      collect(await page.evaluate(() => (window as TabWindow).simulatorTab.pageErrors()));
      await load();
    },
    crash: async () => {
      crashed = true;
      await page.evaluate(() => (window as TabWindow).simulatorTab.crash());
    },
    maintain: async (table) =>
      rethrow(
        await page.evaluate((table) => (window as TabWindow).simulatorTab.maintain(table), table),
      ),
  };
}

for (const store of ["indexeddb", "opfs"] as const satisfies readonly StoreKind[]) {
  for (const campaign of ["complete", "crash"] as const) {
    test(`${store}: the ${campaign} seeded interaction plan holds across real tabs`, async ({
      storageContext,
      browserName,
    }, info) => {
      test.setTimeout(600_000);
      const name = `interaction-${crypto.randomUUID()}`;
      const pages: Page[] = [];
      const pageDiagnostics: PageDiagnostic[] = [];
      // A reload after a crash wipes the tab's own record, so take it before every reload.
      const tabErrors: TabError[] = [];
      const driver: SimulationDriver = {
        open: async () => {
          const page = await storageContext.newPage();
          page.on("console", (message) => {
            if (message.type() === "error") {
              pageDiagnostics.push({ source: "console", message: message.text() });
            }
          });
          page.on("pageerror", (error) =>
            pageDiagnostics.push({ source: "pageerror", error: serializeError(error) }),
          );
          pages.push(page);
          if (store === "opfs" && pages.length === 1) {
            await page.goto("/packages/core/browser/interaction/");
            await requireWorkerOpfs(page);
          }
          return openTab(page, store, name, (errors) => tabErrors.push(...errors));
        },
      };
      // The complete campaign removes fault steps so every browser and store must judge the whole
      // plan. Crash recovery remains a separate campaign, where a browser-level storage wedge can
      // be classified without letting it substitute for ordinary SQL and concurrency coverage.
      const seed = campaignSeed(browserName, campaign);
      const generated = generateInteractionPlan(seed, {
        length: conformance ? 800 : 220,
        connections: conformance ? 4 : 3,
        tables: 2,
        keySpace: conformance ? 32 : 16,
        faultPoints: ["crash"],
      });
      const plan = campaign === "complete" ? withoutFaults(generated) : generated;
      await info.attach(`${store}-${campaign}-plan.json`, {
        contentType: "application/json",
        body: JSON.stringify(
          {
            browser: browserName,
            store,
            campaign,
            conformance,
            seed,
            seedBase,
            plan,
          },
          undefined,
          2,
        ),
      });
      let result: Awaited<ReturnType<typeof runInteractionPlan>> | undefined;
      let runFailure: { readonly error: unknown } | undefined;
      try {
        result = await runInteractionPlan(plan, driver);
      } catch (error) {
        runFailure = { error };
      }
      const collectionFailures: DiagnosticCollectionFailure[] = [];
      const collectTabErrors = async (): Promise<void> => {
        for (const [pageIndex, page] of pages.entries()) {
          try {
            tabErrors.push(
              ...(await page.evaluate(() => (window as TabWindow).simulatorTab.pageErrors())),
            );
          } catch (error) {
            collectionFailures.push({
              phase: "read-tab-errors",
              page: pageIndex,
              error: serializeError(error),
            });
          }
        }
      };
      const attachDiagnostics = (explicitCloseAttempted: boolean): Promise<void> =>
        info.attach(`${store}-${campaign}-diagnostics.json`, {
          contentType: "application/json",
          body: JSON.stringify(
            {
              browser: browserName,
              store,
              campaign,
              conformance,
              seed,
              result,
              runFailure: runFailure === undefined ? undefined : serializeError(runFailure.error),
              explicitCloseAttempted,
              pageDiagnostics,
              tabErrors,
              collectionFailures,
            },
            undefined,
            2,
          ),
        });
      if (runFailure !== undefined) {
        await collectTabErrors();
        await attachDiagnostics(false);
        throw runFailure.error;
      }
      const closeOutcomes = await Promise.allSettled(
        pages.map(async (page) =>
          rethrow(await page.evaluate(() => (window as TabWindow).simulatorTab.close())),
        ),
      );
      for (const [pageIndex, outcome] of closeOutcomes.entries()) {
        if (outcome.status === "rejected") {
          collectionFailures.push({
            phase: "close-tab",
            page: pageIndex,
            error: serializeError(outcome.reason),
          });
        }
      }
      await collectTabErrors();
      await attachDiagnostics(true);
      expect(collectionFailures).toEqual([]);
      if (result === undefined) throw new Error("Interaction plan returned no result");
      // A complete campaign has no fault steps. The crash campaign draws only faults this real-tab
      // driver can inject, so it must not skip any either.
      expect(result.faultsSkipped).toBe(0);
      if (campaign === "complete") {
        expect(result.faultsInjected).toBe(0);
      } else {
        expect(result.faultsInjected).toBeGreaterThan(0);
      }
      if (result.transientsAccepted > 0) {
        // A browser that wedges a whole database is beyond the engine's reach; bounding the wait and
        // naming it is the accepted outcome for WebKit's IndexedDB worker-crash behavior. It is not
        // an accepted substitute for a complete plan or for any other browser/store combination.
        expect(campaign).toBe("crash");
        expect(browserName).toBe("webkit");
        expect(store).toBe("indexeddb");
        expect(result.stoppedBy).toMatch(/StorageUnresponsive/u);
      } else {
        expect(result.interactions).toBe(plan.interactions.length);
        expect(result.acceptedWrites).toBeGreaterThan(10);
        expect(result.checkpoints).toBeGreaterThan(0);
      }
      // Real tabs take turns as the database's writer through Web Locks, so a complete campaign
      // never loses a commit race; only a crash can leave a defensive conflict behind.
      if (campaign === "complete") expect(result.rejectedConflicts).toBe(0);
      expect(pageDiagnostics).toEqual([]);
      // A deliberate crash is classified on the mutation's typed failure. The error sink is for
      // unsolicited window and worker diagnostics, none of which may be hidden by message text.
      expect(tabErrors).toEqual([]);
    });
  }
}
