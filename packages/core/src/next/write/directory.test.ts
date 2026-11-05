/**
 * The paged group directory under the write path (format/manifest-codec.ts "paged group
 * directory"): a table past one full page of groups keeps that page across flushes, rewrites it
 * when a deletion folds into one of its groups, retires exactly the directory extents no longer
 * used (the extent audit), and reopens with every row.
 */
import { describe, expect, it } from "vitest";
import { GROUPS_PER_PAGE } from "../format/manifest-codec.js";
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
    path = await harness.reopen();
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
