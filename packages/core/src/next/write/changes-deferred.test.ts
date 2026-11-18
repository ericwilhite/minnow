import { afterAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, LIVE_DISPATCH_REACH } from "../../testing/reach.js";
import { ChangeFeed, CHANGE_FEED_BYTES } from "../access/changes.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  batch,
  ints,
  rules,
} from "./test-support.js";

const reach = createReach("next-live-dispatch", LIVE_DISPATCH_REACH);
afterAll(() => reach.assertReached());
const turn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
for (const seed of seedsFor("next-live-dispatch", [8862, 8863])) {
  it(`releases native commit bookkeeping before observer callout seed=${String(seed)}`, async () => {
    reach.beginHistory();
    const h = new Harness("relaxed", "memory");
    const path = await h.open();
    try {
      await autocommit(path, (st) =>
        st.changeCatalog({ kind: "createTable", table: tableInput("t", [column("id", INT)]) }),
      );
      await turn();
      const tx = await path.begin();
      const st = tx.statement({ rules });
      await st.table(1).insert(batch(ints([1 + Math.floor(mulberry32(seed)() * 100)])), null);
      await st.end();
      let writerGranted = false,
        response = false;
      const seen: Array<{ writerGranted: boolean; response: boolean }> = [];
      const stop = path.changes.observe(() => {
        seen.push({ writerGranted, response });
      });
      const queued = path.begin().then((next) => {
        writerGranted = true;
        next.rollback();
      });
      const receipt = await tx.commit(null);
      response = true;
      await queued;
      await receipt.durable;
      await turn();
      expect(seen).toEqual([{ writerGranted: true, response: true }]);
      stop();
      reach.reach("writer released");
      reach.reach("response available");
    } finally {
      await h.close();
    }
    expect(h.governor.usedBytes).toBe(0);
    reach.reach("zero charges");
  });
  it(`coalesces published effects in fixed storage and isolates reentrant publication seed=${String(seed)}`, async () => {
    reach.beginHistory();
    let bytes = 0;
    const feed = ChangeFeed.create(
      {
        allocate(n) {
          bytes += n;
          return true;
        },
        free(n) {
          bytes -= n;
        },
      },
      0,
    );
    const random = mulberry32(seed),
      count = 3 + Math.floor(random() * 12);
    const snapshots: Array<{ lsn: number; previousLsn: number; tables: number[] }> = [];
    const firstLsns: number[] = [];
    let published = false;
    const publish = (lsn: number, tableId: number) => {
      feed.prepare(lsn, [{ kind: "rows", tableId }]);
      feed.install(lsn, lsn);
      feed.notify();
    };
    const first = feed.observe((e) => {
      if (!published) {
        published = true;
        publish(count + 1, 99);
      }
      firstLsns.push(e.lsn);
    });
    const second = feed.observe((e) => {
      snapshots.push({
        lsn: e.lsn,
        previousLsn: e.previousLsn,
        tables: e.tables.slice(0, e.tableCount),
      });
    });
    try {
      for (let i = 1; i <= count; i++) publish(i, i);
      // Preparation for an uncommitted writer must never change the queued publication.
      feed.prepare("uncommitted", [{ kind: "rows", tableId: 1234 }]);
      expect(firstLsns).toEqual([]);
      expect(snapshots).toEqual([]);
      await turn();
      await turn();
      expect(firstLsns).toEqual([count, count + 1]);
      expect(snapshots).toEqual([
        { lsn: count, previousLsn: 0, tables: Array.from({ length: count }, (_, i) => i + 1) },
        { lsn: count + 1, previousLsn: count, tables: [99] },
      ]);
      first();
      second();
      expect(bytes).toBe(CHANGE_FEED_BYTES);
      reach.reach("bounded coalescing");
      reach.reach("uncommitted excluded");
      reach.reach("borrow stable during callout");
    } finally {
      feed.close();
    }
    expect(bytes).toBe(0);
    reach.reach("zero charges");
  });
}
