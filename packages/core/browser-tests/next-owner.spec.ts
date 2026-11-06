import { expect, type Page } from "@playwright/test";
import { PROTOCOL_VERSION } from "../dist/next/owner/protocol.js";
import { test } from "./fixtures.js";

/**
 * The next core's owner across real tabs (B5 increment 2): election through the Web Lock, the
 * broker SharedWorker, the owner worker with its fence, owner death and resends, lifecycle
 * step-down and flush, version skew. Statements go through real lowering; the executor and write
 * path are the stand-ins (testing/owner-standins.ts) over OPFS, or IndexedDB where workers have no
 * OPFS.
 */
type Run = typeof import("../browser/next-owner/run.js");
const RUN = "/packages/core/browser/next-owner/run.ts";

async function tab(page: Page): Promise<Page> {
  await page.goto("/packages/core/browser/");
  return page;
}

/**
 * Calls an export of run.ts in `page`. A call that does not settle in 20 s fails with what the page
 * knows: its connector's status and the origin's Web Locks, so a hang names its cause.
 */
async function call<K extends keyof Run>(
  page: Page,
  name: K,
  ...args: Parameters<Run[K]>
): Promise<Awaited<ReturnType<Run[K]>>> {
  const values: unknown[] = [...args];
  const pending = page.evaluate(
    async ({ url, name, args }) => {
      const run = (await import(url)) as Record<string, (...values: unknown[]) => unknown>;
      return run[name]?.(...args);
    },
    { url: RUN, name, args: values },
  ) as Promise<Awaited<ReturnType<Run[K]>>>;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      void page
        .evaluate(async (url) => {
          const run = (await import(url)) as Record<string, () => unknown>;
          let status: unknown;
          try {
            status = run.status?.();
          } catch (error) {
            status = String(error);
          }
          return { status, locks: await navigator.locks.query() };
        }, RUN)
        .then(
          (state) => {
            reject(new Error(`${name} did not settle in 20 s: ${JSON.stringify(state)}`));
          },
          (error: unknown) => {
            reject(new Error(`${name} did not settle in 20 s (${String(error)})`));
          },
        );
    }, 20_000);
  });
  try {
    return await Promise.race([pending, deadline]);
  } finally {
    clearTimeout(timer);
  }
}

const dbName = (label: string) =>
  `${label}-${String(Date.now())}-${String(Math.random()).slice(2, 8)}`;

async function seed(page: Page): Promise<void> {
  await call(page, "query", "CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
  await call(page, "query", "INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
}

const n = async (page: Page) => {
  const result = await call(page, "query", "SELECT * FROM t WHERE id = $1", [1]);
  return "rows" in result ? result.rows[0]?.[1] : result;
};

test.describe.configure({ timeout: 60_000 });

test("two tabs share one owner, elected through the host lock", async ({ page, context }) => {
  const name = dbName("share");
  const a = await tab(page);
  await call(a, "wipe", name);
  expect(await call(a, "open", name)).toEqual({ hosting: true });
  const b = await tab(await context.newPage());
  expect(await call(b, "open", name)).toEqual({ hosting: false });
  await call(a, "query", "CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
  await call(b, "query", "INSERT INTO t VALUES ($1, $2, $3)", [1, 5, "from b"]);
  expect(await call(a, "query", "SELECT * FROM t")).toMatchObject({ rows: [[1, 5, "from b"]] });
});

test("a tab closed with a write in flight: the next owner runs it exactly once", async ({
  page,
  context,
}) => {
  const name = dbName("kill-write");
  const a = await tab(page);
  await call(a, "wipe", name);
  await call(a, "open", name);
  await seed(a);
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  // A's owner keeps the UPDATE applied in memory, never durable.
  await call(b, "knob", name, { t: "hold", value: true });
  await call(b, "send", "update", "UPDATE t SET n = n + 1 WHERE id = $1", [1]);
  await b.waitForTimeout(200);
  await a.close();
  // B saw the host lock free, elected itself, recovered the durable state and resent the UPDATE.
  expect(await call(b, "settle", "update")).toMatchObject({
    summary: { command: "UPDATE", counts: { t: 1 } },
  });
  expect(await call(b, "status")).toMatchObject({ hosting: true });
  expect(await n(b)).toBe(1);
});

test("a COMMIT whose reply died with its owner is answered from the ledger", async ({
  page,
  context,
}) => {
  const name = dbName("kill-commit");
  const a = await tab(page);
  await call(a, "wipe", name);
  await call(a, "open", name);
  await seed(a);
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  await call(b, "begin", 1);
  await call(b, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1], 1);
  // The owner saves the commit (frame and ledger outcome), then hangs before replying.
  await call(b, "knob", name, { t: "dieOnSave" });
  await call(b, "send", "commit", "COMMIT", [], 1);
  await b.waitForTimeout(300);
  await a.close();
  expect(await call(b, "settle", "commit")).toMatchObject({
    summary: { replayed: true, counts: { t: 1 } },
  });
  expect(await n(b)).toBe(1);
});

test("pagehide terminates the owner; another tab takes over on its next request", async ({
  page,
  context,
}) => {
  const name = dbName("pagehide");
  const a = await tab(page);
  await call(a, "wipe", name);
  await call(a, "open", name);
  await seed(a);
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  await a.evaluate(() => {
    window.dispatchEvent(new PageTransitionEvent("pagehide", { persisted: false }));
  });
  expect(await call(a, "status")).toMatchObject({ hosting: false });
  expect(await n(b)).toBe(0);
  // A synthetic pagehide leaves a's page alive, so either tab may win the election; whichever
  // does, b is served (an idle b whose owner was replaced follows the announcement, p2d).
  const hosts = [
    ((await call(a, "status")) as { hosting: boolean }).hosting,
    ((await call(b, "status")) as { hosting: boolean }).hosting,
  ];
  expect(hosts.filter(Boolean)).toHaveLength(1);
});

test("hidden flushes pending relaxed commits before the owner can be lost", async ({
  page,
  context,
}) => {
  for (const flushOnHidden of [true, false]) {
    const name = dbName(`hidden-${String(flushOnHidden)}`);
    const a = await tab(flushOnHidden ? page : await context.newPage());
    await call(a, "wipe", name);
    // Relaxed, and no timed flush for ten minutes: acknowledged commits sit in memory.
    await call(a, "open", name, { settings: { durability: "relaxed", flushDelayMs: 600_000 } });
    await seed(a);
    if (flushOnHidden) await call(a, "lifecycle", "hidden");
    await a.waitForTimeout(100);
    await a.close();
    const b = await tab(await context.newPage());
    await call(b, "open", name);
    const result = await call(b, "query", "SELECT id, n, name FROM t");
    // With the flush the rows survived the owner; without it (the control) even the table is gone.
    if (flushOnHidden) expect(result).toMatchObject({ rows: [[1, 0, "x"]] });
    else expect(result).toMatchObject({ error: "PlanningError" });
    await b.close();
  }
});

test("a newer build supersedes the older owner and its tabs", async ({ page, context }) => {
  const name = dbName("skew");
  const a = await tab(page);
  await call(a, "wipe", name);
  await call(a, "open", name);
  await seed(a);
  const newer = await tab(await context.newPage());
  expect(await call(newer, "open", name, { protocol: PROTOCOL_VERSION + 1 })).toEqual({
    hosting: true,
  });
  await expect.poll(async () => (await call(a, "status")).superseded).toBe(true);
  expect(await n(newer)).toBe(0);
});

test("a closed tab's open transaction is rolled back and the writer slot freed", async ({
  page,
  context,
}) => {
  const name = dbName("liveness");
  const a = await tab(page);
  await call(a, "wipe", name);
  await call(a, "open", name);
  await seed(a);
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  await call(b, "begin", 1);
  await call(b, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1], 1);
  await b.waitForTimeout(100); // B holds and declared its liveness lock
  await b.close();
  expect(await call(a, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1])).toMatchObject({
    summary: { counts: { t: 1 } },
  });
  expect(await n(a)).toBe(1);
});

// ---- the review of increment 2 (B5-p2b-review-1) ----

test("freeze, which fires on the document, terminates the owner", async ({ page }) => {
  await tab(page);
  expect(await call(page, "open", dbName("freeze-document"))).toEqual({ hosting: true });
  await page.evaluate(() => document.dispatchEvent(new Event("freeze")));
  await page.waitForTimeout(100);
  expect(await call(page, "status")).toMatchObject({ hosting: false });
});

test("an older client is superseded by a newer owner", async ({ page, context }) => {
  await tab(page);
  const name = dbName("older");
  await call(page, "open", name, { protocol: PROTOCOL_VERSION + 1 });
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  expect(await call(b, "status")).toMatchObject({ superseded: true, hosting: false });
});

test("a transaction lost with its owner stays lost until ROLLBACK, never autocommit", async ({
  page,
  context,
}) => {
  const name = dbName("lost-session");
  const a = await tab(page);
  await call(a, "open", name);
  await seed(a);
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  await call(b, "begin", 1);
  await call(b, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1], 1);
  await a.close();
  // Session 0 reconnects b, which takes over.
  expect(await n(b)).toBe(0);
  const update = () => call(b, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1], 1);
  expect(await update()).toMatchObject({ error: "SessionLostError" });
  expect(await call(b, "query", "COMMIT", [], 1)).toMatchObject({ error: "SessionLostError" });
  expect(await n(b)).toBe(0);
  expect(await call(b, "query", "ROLLBACK", [], 1)).toMatchObject({
    summary: { transaction: "idle" },
  });
  expect(await update()).toMatchObject({ summary: { transaction: "idle" } });
  expect(await n(b)).toBe(1);
});

// ---- the second review of increment 2 (B5-p2b-review-2) ----

/**
 * Leaves a request unanswered by hanging tab `a`'s owner: an autocommit write is saved, then the
 * worker spins forever (owner-worker.ts `dieOnSave`), so what `b` sends next is in flight when
 * `a` closes.
 */
async function hangOwner(b: Page, name: string): Promise<void> {
  await call(b, "knob", name, { t: "dieOnSave" });
  await call(b, "send", "hang", "INSERT INTO t VALUES ($1, $2, $3)", [2, 0, "hang"]);
  await b.waitForTimeout(300);
}

for (const [session, text] of [
  [1, "BEGIN"],
  [0, "/* lead */ start transaction"],
] as const)
  test(`an unanswered ${JSON.stringify(text)} in session ${String(session)} loses the session, never autocommit`, async ({
    page,
    context,
  }) => {
    const name = dbName(`lost-begin-${String(session)}`);
    const a = await tab(page);
    await call(a, "wipe", name);
    await call(a, "open", name);
    await seed(a);
    const b = await tab(await context.newPage());
    await call(b, "open", name);
    await hangOwner(b, name);
    await call(b, "send", "begin", text, [], session);
    await a.close();
    // The saved write is answered from the ledger; the BEGIN is refused, never resent.
    expect(await call(b, "settle", "hang")).toMatchObject({ summary: { replayed: true } });
    expect(await call(b, "settle", "begin")).toMatchObject({ error: "SessionLostError" });
    const update = () => call(b, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1], session);
    expect(await update()).toMatchObject({ error: "SessionLostError" });
    expect(await call(b, "query", "SELECT * FROM t WHERE id = $1", [1], 1)).toMatchObject(
      session === 1 ? { error: "SessionLostError" } : { rows: [[1, 0, "x"]] },
    );
    expect(await call(b, "query", "ROLLBACK", [], session)).toMatchObject({
      summary: { transaction: "idle" },
    });
    expect(await update()).toMatchObject({ summary: { transaction: "idle" } });
    expect(await n(b)).toBe(1);
  });

test("a prepared COMMIT whose reply died with its owner is answered from the ledger", async ({
  page,
  context,
}) => {
  const name = dbName("kill-prepared-commit");
  const a = await tab(page);
  await call(a, "wipe", name);
  await call(a, "open", name);
  await seed(a);
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  const commit = await call(b, "prepare", "COMMIT");
  await call(b, "begin", 1);
  await call(b, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1], 1);
  await call(b, "knob", name, { t: "dieOnSave" });
  await call(b, "sendPrepared", "commit", commit, [], 1);
  await b.waitForTimeout(300);
  await a.close();
  expect(await call(b, "settle", "commit")).toMatchObject({
    summary: { replayed: true, counts: { t: 1 } },
  });
  expect(await n(b)).toBe(1);
});

test("closing before the owner worker is ready settles start() and frees the host lock", async ({
  page,
}) => {
  await tab(page);
  expect(await call(page, "closeBeforeReady", dbName("close-early"))).toBe("ClientClosedError");
});

// ---- p2d: a client notices that its owner was replaced ----

test("an idle tab whose owner was replaced reconnects at once (the owner's tab re-elected first)", async ({
  page,
  context,
}) => {
  const name = dbName("replaced");
  const a = await tab(page);
  await call(a, "wipe", name);
  await call(a, "open", name);
  await seed(a);
  const b = await tab(await context.newPage());
  await call(b, "open", name);
  // a's owner goes; a's page survives a synthetic pagehide and elects a new owner first.
  await a.evaluate(() => {
    window.dispatchEvent(new PageTransitionEvent("pagehide", { persisted: false }));
  });
  expect(await n(a)).toBe(0);
  // b was idle, its port to the dead owner silent: the announcement moves it.
  const started = Date.now();
  expect(await call(b, "query", "UPDATE t SET n = n + 1 WHERE id = $1", [1])).toMatchObject({
    summary: { counts: { t: 1 } },
  });
  expect(Date.now() - started).toBeLessThan(10_000);
  expect(await n(a)).toBe(1);
});
