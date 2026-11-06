/**
 * Review B3-p2a-2 probes for the write path (the catalog and divergence probes live with their
 * modules: lower/memory-catalog.test.ts, testing/divergences.test.ts).
 */
import { describe, expect, it } from "vitest";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

describe("review B3-p2a-2", () => {
  it("3: a group root read that failed for budget is retried by the same segment", async () => {
    const harness = new Harness("strict", "memory", 2);
    const caches: Array<{ evict(bytes: number): number }> = [];
    const register = harness.governor.registerCache.bind(harness.governor);
    harness.governor.registerCache = (cache) => {
      caches.push(cache);
      return register(cache);
    };
    let path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    for (const pair of [
      [1, 2],
      [3, 4],
      [5, 6],
    ])
      await autocommit(path, (s) => s.table(t).insert(batch(ints(pair)), null));
    await path.quiesce();
    path = await harness.reopen();
    const view = path.pin();
    let allow = false;
    const table = view.table(t, { budget: { allocate: () => allow, free: () => undefined } });
    const segment = table.segments.find((candidate) => candidate.kind === "group");
    if (segment?.kind !== "group") throw new Error("no group segment");
    // A cold, asynchronous read: nothing cached, nothing resident.
    for (const cache of caches) cache.evict(Number.POSITIVE_INFINITY);
    harness.space.peek = () => null;
    const first = Promise.resolve().then(() => segment.root());
    const concurrent = Promise.resolve().then(() => segment.root());
    await expect(first).rejects.toThrow();
    await expect(concurrent).rejects.toThrow();
    allow = true;
    await expect(Promise.resolve().then(() => segment.root())).resolves.toBeDefined();
    view.release();
    await harness.close();
  });
});
