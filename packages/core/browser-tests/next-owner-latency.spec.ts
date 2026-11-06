import { expect, type Page } from "@playwright/test";
import { test } from "./fixtures.js";

/**
 * The cross-tab point-read latency lane (ruling R6): warm prepared point reads from one tab
 * against the owner in another, uncontended and beside an analytic load in the owner (stand-in
 * `SELECT spin($1)`, which works in 4 ms slices with a task yield between them, as the executor's
 * driver must, R16.4). Timed per request, from the call to its resolution, on cross-origin isolated
 * pages (fine timers). The engines are the stand-ins: owner-side work is the server's and the
 * stand-ins', not the real executor's. `baseline` is a bare round trip to the broker SharedWorker
 * under the same conditions: what the platform costs before this protocol adds anything.
 */
type Run = typeof import("../browser/next-owner/run.js");
const RUN = "/packages/core/browser/next-owner/run.ts";

function call<K extends keyof Run>(
  page: Page,
  name: K,
  ...args: Parameters<Run[K]>
): Promise<Awaited<ReturnType<Run[K]>>> {
  const values: unknown[] = [...args];
  return page.evaluate(
    async ({ url, name, args }) => {
      const run = (await import(url)) as Record<string, (...values: unknown[]) => unknown>;
      return run[name]?.(...args);
    },
    { url: RUN, name, args: values },
  ) as Promise<Awaited<ReturnType<Run[K]>>>;
}

/** R6: uncontended p50 per engine, and p95 beside an analytic query. */
const TARGET_P50_US = { chromium: 250, firefox: 400, webkit: 600 } as const;
const TARGET_LOADED_P95_US = 5_000;
/**
 * Web Lock calls (page and owner worker together) allowed during the whole uncontended sample:
 * a few for starting the watches, never one per read. Lock calls cross processes; one per request
 * made reads 5.7× a bare round trip. Deterministic, unlike timings on a shared host.
 */
const MAX_LOCK_CALLS_PER_SAMPLE = 8;
/** A quiet perf host (the browser perf lane): enforce R6's absolute targets too. */
const ENFORCE_TARGETS = process.env.MINNOW_PERF_LANE === "1";

test.describe.configure({ timeout: 120_000 });

test("cross-tab warm point reads meet R6", async ({ page, context, browserName }, info) => {
  const name = `latency-${String(Date.now())}`;
  const owner = page;
  await owner.goto("/packages/core/browser/");
  await call(owner, "wipe", name);
  await call(owner, "open", name);
  await call(owner, "query", "CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
  for (let id = 1; id <= 100; id++)
    await call(owner, "query", "INSERT INTO t VALUES ($1, $2, $3)", [id, id, `name ${String(id)}`]);
  const reader = await context.newPage();
  await reader.goto("/packages/core/browser/");
  expect(await call(reader, "open", name)).toEqual({ hosting: false });
  const timers = await call(reader, "timerResolution");
  const baseline = await call(reader, "brokerRoundTrips", 3_000, 500);
  await call(reader, "pointReads", 200, 0); // the watches start here
  const locksBefore = await call(reader, "lockCalls");
  const uncontended = await call(reader, "pointReads", 3_000, 500);
  const locksAfter = await call(reader, "lockCalls");
  const lockCalls =
    locksAfter.page - locksBefore.page + ((locksAfter.owner ?? 0) - (locksBefore.owner ?? 0));
  // An analytic query in the owner for longer than the loaded sample takes.
  await call(owner, "send", "load", "SELECT spin($1)", [20_000]);
  await reader.waitForTimeout(50);
  const loaded = await call(reader, "pointReads", 1_000, 50);
  const ratio = uncontended.p50 / baseline.p50;
  const crossover = await call(reader, "transferCrossover");
  const result = {
    browser: browserName,
    timers,
    baseline,
    uncontended,
    ratio,
    lockCalls,
    loaded,
    crossover,
  };
  console.log(JSON.stringify(result));
  await info.attach("next-owner-latency", {
    body: JSON.stringify(result, null, 2),
    contentType: "application/json",
  });
  expect(timers.isolated).toBe(true);
  expect(locksAfter.owner).not.toBeNull();
  expect(lockCalls).toBeLessThanOrEqual(MAX_LOCK_CALLS_PER_SAMPLE);
  if (ENFORCE_TARGETS) {
    expect(uncontended.p50).toBeLessThanOrEqual(TARGET_P50_US[browserName]);
    expect(loaded.p95).toBeLessThanOrEqual(TARGET_LOADED_P95_US);
  } else
    info.annotations.push({
      type: "R6",
      description: `targets reported, not enforced (MINNOW_PERF_LANE unset): p50 ${uncontended.p50.toFixed(0)} µs vs ${String(TARGET_P50_US[browserName])} (${ratio.toFixed(1)}× a bare round trip); loaded p95 ${loaded.p95.toFixed(0)} µs vs ${String(TARGET_LOADED_P95_US)}`,
    });
});
