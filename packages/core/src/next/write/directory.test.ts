/**
 * The paged group directory under the write path (format/manifest-codec.ts "paged group
 * directory"): a table past one full page of groups keeps that page across flushes, rewrites it
 * when a deletion folds into one of its groups, retires exactly the directory extents no longer
 * used (the extent audit), and reopens with every row.
 */
import { GroupDirectory } from "./groups.js";
import { describe, expect, it } from "vitest";
import { GROUPS_PER_PAGE, ParseBudgetError, parseCharge } from "../format/manifest-codec.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  tableInput,
} from "./test-support.js";

const ROWS_PER_GROUP = 2;

describe("paged group directory", () => {
  it("a snapshot holds the group roots it read, charged to its budget, until released", async () => {
    const harness = new Harness("strict", "memory", ROWS_PER_GROUP);
    const path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("h", [column("id", INT, false)]),
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
    let charged = 0;
    const budget = {
      allocate: (bytes: number) => {
        charged += bytes;
        return true;
      },
      free: (bytes: number) => {
        charged -= bytes;
      },
    };
    const view = path.pin();
    const table = view.table(t, { budget });
    const groups = table.segments.filter((segment) => segment.kind === "group");
    expect(groups.length).toBeGreaterThanOrEqual(2);
    for (const segment of groups) await segment.root();
    expect(charged).toBeGreaterThan(0);
    view.release();
    expect(charged).toBe(0);
    await harness.close();
  });

  it("keeps full pages, rewrites a changed one, retires the old blobs, and reopens", async () => {
    const harness = new Harness("relaxed", "memory", ROWS_PER_GROUP);
    let path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("g", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    const groups = GROUPS_PER_PAGE + 3;
    const rows = groups * ROWS_PER_GROUP;
    // One group per flush: each commit fills a memtable.
    for (let id = 0; id < rows; id += ROWS_PER_GROUP) {
      const tx = await path.begin();
      const s = tx.statement({ rules: () => ({ check: () => null, generate: () => undefined }) });
      await s.table(t).insert(batch(ints([id, id + 1])), null);
      await s.end();
      await tx.commit(null);
    }
    await path.quiesce();
    const root = () => path.statistics(t);
    expect(root()?.rowCount).toBe(rows);
    await harness.space.audit();
    // A deletion in the first page's first group: that page is rewritten at the next flush.
    await autocommit(path, (s) => s.table(t).delete(batch(locators([0], t)), null));
    await autocommit(path, (s) => s.table(t).insert(batch(ints([rows, rows + 1])), null));
    await path.quiesce();
    await harness.space.audit();
    await harness.close();
    const started = performance.now();
    path = await harness.open();
    // Gate A tracks reopen time against directory size (target: ≤ 500 ms at 5M rows).
    console.info(
      `reopen with ${String(groups + 1)} groups: ${(performance.now() - started).toFixed(0)} ms, resident metadata ${String(path.residentMetadataBytes)} B`,
    );
    const view = path.pin();
    const ids = (await rowsOf(view, t)).map((row) => Number(row[0]));
    view.release();
    expect(ids.length).toBe(rows + 1);
    expect(ids.includes(0)).toBe(false);
    expect(ids.includes(rows + 1)).toBe(true);
    await harness.space.audit();
    await harness.close();
  }, 300_000);
});

describe("resident directory memory (review B3-p2a-2 #1)", () => {
  it("charges the directory a table keeps resident, and a cap too tight for it refuses the open", async () => {
    const harness = new Harness("relaxed", "memory", ROWS_PER_GROUP);
    // Every cache registered with the governor, so evictable bytes can be told from the rest.
    const caches: Array<{ readonly evictableBytes: number }> = [];
    const register = harness.governor.registerCache.bind(harness.governor);
    harness.governor.registerCache = (cache) => {
      caches.push(cache);
      return register(cache);
    };
    // The engine's resident-metadata grant, to check its charge is real (not only reported).
    const metadata: { grant: { readonly allocated: number } | null } = { grant: null };
    const admit = harness.governor.admit.bind(harness.governor);
    harness.governor.admit = async (options) => {
      const grant = await admit(options);
      if (options.label === "write path metadata") metadata.grant = grant;
      return grant;
    };
    const fixed = () =>
      harness.governor.usedBytes - caches.reduce((sum, cache) => sum + cache.evictableBytes, 0);
    let path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("m", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    const groups = 600;
    for (let id = 0; id < groups * ROWS_PER_GROUP; id += ROWS_PER_GROUP) {
      const tx = await path.begin();
      const s = tx.statement({ rules: () => ({ check: () => null, generate: () => undefined }) });
      await s.table(t).insert(batch(ints([id, id + 1])), null);
      await s.end();
      await tx.commit(null);
    }
    await path.quiesce();
    const grown = path.residentMetadataBytes;
    // At least the directory's exact charge (groups.ts) on top of the root's.
    expect(grown).toBeGreaterThanOrEqual(GroupDirectory.buildCharge(groups));
    // Each publish charged its growth on the grant itself.
    expect(metadata.grant?.allocated).toBe(grown);
    await harness.close();
    const baseline = fixed();
    path = await harness.open();
    const opened = path.residentMetadataBytes;
    // The same directory, charged again at open, before it was materialized.
    expect(opened).toBe(grown);
    expect(metadata.grant?.allocated).toBe(opened);
    const used = fixed() - baseline;
    await harness.close();
    // Ballast leaves room for everything the open needs except half the directory's charge.
    const governor = harness.governor;
    // Caches give way under pressure: only the fixed holdings count.
    const room = governor.capBytes - governor.reserveBytes - fixed();
    const ballast = await governor.admit({
      label: "ballast",
      minimumBytes: room - used + opened,
      spillable: false,
    });
    // Refused with the typed metadata error (the resident charge or the parse that feeds it),
    // never by growing the heap past the cap.
    await expect(harness.open()).rejects.toThrow(ParseBudgetError);
    ballast.close();
    path = await harness.open();
    expect(path.residentMetadataBytes).toBe(opened);
    expect(metadata.grant?.allocated).toBe(opened);
    // Dropping the table returns its directory's charge (no older reader holds it).
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
    expect(metadata.grant?.allocated).toBe(path.residentMetadataBytes);
    // What stays is the current root's parse bound alone.
    await path.quiesce();
    expect(path.residentMetadataBytes).toBe(parseCharge(harness.space.root?.length ?? 0));
    expect(metadata.grant?.allocated).toBe(path.residentMetadataBytes);
    await harness.close();
  }, 300_000);
});
