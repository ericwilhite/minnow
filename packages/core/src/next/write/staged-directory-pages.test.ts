/**
 * A transaction that stages more than `GROUPS_PER_PAGE` memtables of one table: its staged
 * directory spans two pages, which one write places in one extent. The next staging and the fold
 * at commit each release that directory, and must name the shared extent once (ruling R25:
 * storage/extent-list.ts refuses a release the list does not name). The generated histories in
 * staged-generated.test.ts cover the same release paths with small directory pages; this case
 * runs at the real page size, without that tuning.
 */
import { expect, it } from "vitest";
import { decodeGroupIndex, GROUPS_PER_PAGE } from "../format/manifest-codec.js";
import { need } from "./bytes.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
const MEMTABLE_ROWS = 8;
const CALL_ROWS = 512;

it("a staged directory past one page is released once by the next staging and the fold", async () => {
  const harness = new Harness("strict", "memory", MEMTABLE_ROWS);
  let path = await harness.open();
  let t = 0;
  await autocommit(path, async (statement) => {
    t =
      (
        await statement.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", INT), column("b", TEXT)]),
        })
      ).id ?? 0;
  });
  const rows = (GROUPS_PER_PAGE + 256) * MEMTABLE_ROWS;
  const tx = await path.begin();
  const statement = tx.statement({ rules });
  let pages = 0;
  let stagesAtTwoPages = 0;
  for (let first = 1; first <= rows; first += CALL_ROWS) {
    const ids = Array.from({ length: Math.min(CALL_ROWS, rows - first + 1) }, (_, k) => first + k);
    await statement
      .table(t)
      .insert(batch(ints(ids), ints(ids), texts(ids.map((id) => `v${String(id)}`))), null);
    // The durable staged directory, once it has two pages (sharing one extent).
    const entry = path.state().root.tables.find((table) => table.tableId === t)?.staged[0];
    if (pages < 2 && entry !== undefined) {
      const lease = await harness.space.read([entry.groups], { budget: FREE });
      try {
        const index = decodeGroupIndex(need(lease.bytes[0]), FREE).value;
        pages = index.length;
        if (pages >= 2) {
          expect(new Set(index.map((page) => page.ref.extent.offset)).size).toBe(1);
          stagesAtTwoPages = path.maintenance.stages;
        }
      } finally {
        lease.release();
      }
    }
  }
  await statement.end();
  expect(pages).toBe(2);
  expect(path.maintenance.stages, "a later staging retired the two-page directory").toBeGreaterThan(
    stagesAtTwoPages,
  );
  await (
    await tx.commit(null)
  ).durable;
  await path.quiesce();
  expect(path.state().root.tables.find((table) => table.tableId === t)?.staged).toEqual([]);
  await harness.space.audit();
  path = await harness.reopen();
  const view = path.pin();
  try {
    expect((await rowsOf(view, t)).length).toBe(rows);
  } finally {
    view.release();
  }
  await harness.space.audit();
  await harness.close();
}, 300_000);
