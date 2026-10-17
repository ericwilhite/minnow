import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

type Runner = typeof import("../browser/index-takeover-run.js");
const RUNNER = "/packages/core/browser/index-takeover-run.ts";

/**
 * A tab closes in the middle of `CREATE UNIQUE INDEX`, leaving the index marked, its build
 * session open for an hour, and its key set staged. Another tab's `CREATE UNIQUE INDEX IF NOT
 * EXISTS` waits while the first tab lives -- success then would hand back an index nothing
 * enforces -- and finishes the index within seconds of the tab closing: the browser releases the
 * closed tab's Web Locks, which is how the engine knows its build will never finish.
 */
for (const store of ["indexeddb", "opfs"] as const) {
  test(`${store}: a tab closed mid CREATE UNIQUE INDEX hands the build to another tab at once`, async ({
    storageContext,
  }) => {
    test.setTimeout(120_000);
    const [builder, other] = await Promise.all([
      storageContext.newPage(),
      storageContext.newPage(),
    ]);
    await Promise.all([builder, other].map((tab) => tab.goto("/packages/core/browser/")));
    if (store === "opfs") await requireWorkerOpfs(builder);
    const name = `index-takeover-${crypto.randomUUID()}`;
    await other.evaluate(
      async ({ url, kind, name }) => {
        const runner = (await import(url)) as Runner;
        await runner.openTab(kind, name, false);
        const created = await runner.execute(
          "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT NOT NULL)",
        );
        if (created.error !== undefined) throw new Error(created.error);
        for (let start = 0; start < 2_000; start += 500) {
          const rows = Array.from(
            { length: 500 },
            (_, offset) => `(${String(start + offset)}, 'e${String(start + offset)}')`,
          );
          const inserted = await runner.execute(`INSERT INTO people VALUES ${rows.join(", ")}`);
          if (inserted.error !== undefined) throw new Error(inserted.error);
        }
      },
      { url: RUNNER, kind: store, name },
    );
    await builder.evaluate(
      async ({ url, kind, name }) => {
        const runner = (await import(url)) as Runner;
        await runner.openTab(kind, name, true);
        runner.start("create", "CREATE UNIQUE INDEX people_email ON people (email)");
        await runner.untilParked();
      },
      { url: RUNNER, kind: store, name },
    );

    // The building tab is alive: the other tab waits for it rather than report the index.
    const whileAlive = await other.evaluate(async (url) => {
      const runner = (await import(url)) as Runner;
      runner.start("ifne", "CREATE UNIQUE INDEX IF NOT EXISTS people_email ON people (email)");
      return runner.settled("ifne", 1_500);
    }, RUNNER);
    expect(whileAlive).toBe("pending");

    await builder.close();
    const closedAt = Date.now();
    const finished = await other.evaluate(async (url) => {
      const runner = (await import(url)) as Runner;
      return runner.settled("ifne", 60_000);
    }, RUNNER);
    // Within seconds, not the build session's one-hour lease.
    expect(finished).not.toBe("pending");
    expect(finished).not.toHaveProperty("error");
    expect(Date.now() - closedAt).toBeLessThan(20_000);

    const duplicate = await other.evaluate(async (url) => {
      const runner = (await import(url)) as Runner;
      return runner.execute("INSERT INTO people VALUES (5000, 'e42')");
    }, RUNNER);
    expect(duplicate.error).toMatch(/unique|duplicate/i);
    const fresh = await other.evaluate(async (url) => {
      const runner = (await import(url)) as Runner;
      return runner.execute("INSERT INTO people VALUES (5001, 'fresh')");
    }, RUNNER);
    expect(fresh.error).toBeUndefined();
  });
}
