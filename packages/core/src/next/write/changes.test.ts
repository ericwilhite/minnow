import { afterAll, describe, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { ChangeFeed, CHANGE_FEED_BYTES, CHANGE_TABLE_LIMIT } from "../access/changes.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
} from "./test-support.js";

import { createReach, LIVE_PUBLICATION_REACH } from "../../testing/reach.js";
const reach = createReach("next-live-publication", LIVE_PUBLICATION_REACH);
afterAll(() => reach.assertReached());

// Authority: access/changes.ts; native accepted effects, not planner writes, define invalidation.
describe("native committed changes", () => {
  for (const seed of seedsFor("next-live-publication", [8121, 8122, 8123])) {
    it(`surviving native transaction history seed=${String(seed)}`, async () => {
      reach.beginHistory();
      const h = new Harness("strict", "memory");
      const path = await h.open();
      try {
        const events: Array<{ lsn: number; tables: number[]; catalog: boolean }> = [];
        const feed = path.changes;
        const stop = feed.observe((e) =>
          events.push({ lsn: e.lsn, tables: e.tables.slice(0, e.tableCount), catalog: e.catalog }),
        );
        const bad = feed.observe(() => {
          throw new Error("consumer failure");
        });
        const ids: number[] = [];
        for (const name of ["a", "b", "c"]) {
          await autocommit(path, async (st) => {
            const table = await st.changeCatalog({
              kind: "createTable",
              table: tableInput(name, [column("id", INT)]),
            });
            if (table.id === null) throw new Error("Table ID missing");
            ids.push(table.id);
          });
          expect(events.at(-1)?.catalog).toBe(true);
          reach.reach("catalog publication");
        }
        const random = mulberry32(seed);
        for (let step = 0; step < 40; step++) {
          const tx = await path.begin();
          const kept = ids[Math.floor(random() * ids.length)] ?? 0;
          const dropped = ids[(ids.indexOf(kept) + 1) % ids.length] ?? 0;
          let st = tx.statement({ rules });
          await st.table(kept).insert(batch(ints([step])), null);
          await st.end();
          const point = tx.savepoint();
          st = tx.statement({ rules });
          await st.table(dropped).insert(batch(ints([step])), null);
          await st.end();
          tx.rollbackTo(point);
          reach.reach("savepoint rollback");
          const count = events.length;
          if (random() < 0.25) {
            tx.rollback();
            expect(events).toHaveLength(count);
            reach.reach("transaction rollback");
          } else {
            const receipt = await tx.commit(null);
            await receipt.durable;
            expect(events).toHaveLength(count + 1);
            expect(events.at(-1)).toEqual({ lsn: receipt.lsn, tables: [kept], catalog: false });
            expect(feed.probe()).toBe(receipt.lsn);
            reach.reach("explicit commit");
            reach.reach("observer isolation");
          }
        }
        stop();
        bad();
      } finally {
        await h.close();
      }
      expect(h.governor.usedBytes).toBe(0);
      reach.reach("zero charges");
    });
  }
  it("bounds summaries, catches missed notifications and marks replacement distinctly", () => {
    let bytes = 0;
    const feed = ChangeFeed.create(
      {
        allocate: (n) => {
          bytes += n;
          return true;
        },
        free: (n) => {
          bytes -= n;
        },
      },
      0,
    );
    const tables = Array.from({ length: CHANGE_TABLE_LIMIT + 1 }, (_, tableId) => ({
      kind: "rows",
      tableId,
    }));
    feed.prepare(1, tables);
    feed.install(1, 10);
    expect(feed.probe()).toBe(10); // No listener received this publication: global catch-up.
    let replacement = false;
    const lineage: Array<[number, number]> = [];
    const stop = feed.observe((e) => {
      expect(e.unknown).toBe(true);
      expect(e.tableCount).toBeLessThanOrEqual(CHANGE_TABLE_LIMIT);
      replacement = e.replacement;
      lineage.push([e.previousLsn, e.lsn]);
    });
    feed.notify();
    feed.replace(11);
    expect(replacement).toBe(true);
    expect(lineage).toEqual([
      [0, 10],
      [10, 11],
    ]);
    stop();
    feed.prepare(2, [{ kind: "catalog" }]);
    feed.install(2, 12);
    expect(feed.rekeysSince(11, 12)).toEqual({ reset: true, rekeys: [] });
    expect(bytes).toBe(CHANGE_FEED_BYTES);
    feed.close();
    expect(bytes).toBe(0);
  });
});
