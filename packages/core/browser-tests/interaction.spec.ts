import { expect } from "@playwright/test";
import type { Page } from "@playwright/test";
import {
  generateInteractionPlan,
  runInteractionPlan,
  type PlanValue,
  type SimulatedConnection,
  type SimulatedExecuteResult,
  type SimulatedQueryResult,
  type SimulationDriver,
} from "@minnowdb/core/testing";
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

interface TabFailure {
  failed: true;
  name: string;
  message: string;
}

function rethrow<T>(value: T | TabFailure): T {
  if (typeof value === "object" && value !== null && "failed" in value) {
    const error = new Error(value.message);
    error.name = value.name;
    throw error;
  }
  return value;
}

async function openTab(
  page: Page,
  kind: StoreKind,
  name: string,
  crashes: boolean,
): Promise<SimulatedConnection> {
  await page.goto("/packages/core/browser/interaction/");
  await expect(page.locator("#ready")).toHaveText("Interaction simulator tab ready");
  await page.evaluate(
    async ({ kind, name }) => {
      const target = window as typeof window & {
        simulatorTab: { open(k: string, n: string): Promise<void> };
      };
      await target.simulatorTab.open(kind, name);
    },
    { kind, name },
  );
  interface Tab {
    execute(sql: string, params?: PlanValue[]): Promise<SimulatedExecuteResult | TabFailure>;
    query(sql: string, params?: PlanValue[]): Promise<SimulatedQueryResult | TabFailure>;
    reopen(): Promise<void>;
    crash(): Promise<void>;
    maintain(table: string): Promise<void>;
  }
  type TabWindow = typeof window & { simulatorTab: Tab };
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
    reopen: () => page.evaluate(() => (window as TabWindow).simulatorTab.reopen()),
    // WebKit once stalled for the whole test timeout on the first read after a worker was
    // terminated mid-write, and the stall did not reproduce in isolation; until it is
    // understood, WebKit runs the plan without crash faults so the rest of it still judges
    // WebKit (the Node suites and the other two browsers keep exercising crashes).
    ...(crashes
      ? { crash: () => page.evaluate(() => (window as TabWindow).simulatorTab.crash()) }
      : {}),
    maintain: (table) =>
      page.evaluate((table) => (window as TabWindow).simulatorTab.maintain(table), table),
  };
}

for (const store of ["indexeddb", "opfs"] as const satisfies readonly StoreKind[]) {
  test(`${store}: a seeded interaction plan holds across real tabs`, async ({
    storageContext,
    browserName,
  }) => {
    test.setTimeout(600_000);
    const name = `interaction-${crypto.randomUUID()}`;
    const pages: Page[] = [];
    const consoleErrors: string[] = [];
    const driver: SimulationDriver = {
      open: async () => {
        const page = await storageContext.newPage();
        page.on("pageerror", (error) => consoleErrors.push(error.message));
        pages.push(page);
        if (store === "opfs" && pages.length === 1) {
          await page.goto("/packages/core/browser/interaction/");
          await requireWorkerOpfs(page);
        }
        return openTab(page, store, name, browserName !== "webkit");
      },
    };
    // Seeds differ per browser so three engines explore three plans on every run.
    const seeds: Record<string, number> = { chromium: 0x5eed, firefox: 0xf1ef, webkit: 0x3eb5 };
    const seed = seeds[browserName] ?? 0x5eed;
    const plan = generateInteractionPlan(seed, {
      length: 220,
      connections: 3,
      tables: 2,
      keySpace: 16,
      faultPoints: ["crash"],
    });
    const result = await runInteractionPlan(plan, driver);
    const tabErrors = (
      await Promise.all(
        pages.map((page) =>
          page.evaluate(() =>
            (
              window as typeof window & { simulatorTab: { pageErrors(): string[] } }
            ).simulatorTab.pageErrors(),
          ),
        ),
      )
    ).flat();
    await Promise.all(
      pages.map((page) =>
        page.evaluate(() =>
          (
            window as typeof window & { simulatorTab: { close(): Promise<void> } }
          ).simulatorTab.close(),
        ),
      ),
    );
    expect(result.interactions).toBe(plan.interactions.length);
    expect(result.acceptedWrites).toBeGreaterThan(10);
    expect(result.checkpoints).toBeGreaterThan(0);
    const faults = plan.interactions.filter((interaction) => interaction.kind === "fault").length;
    if (browserName === "webkit") {
      expect(result.faultsSkipped).toBe(faults);
    } else {
      expect(result.faultsInjected).toBeGreaterThan(0);
      // The plan draws crash faults only, so every fault step was a real worker termination.
      expect(result.faultsSkipped).toBe(0);
    }
    expect(consoleErrors).toEqual([]);
    // Worker terminations surface through the client's error sink; the connection-lost report
    // for a deliberate crash is expected, anything else is not.
    expect(tabErrors.filter((message) => !/terminated|lost|closed|crash/iu.test(message))).toEqual(
      [],
    );
  });
}
