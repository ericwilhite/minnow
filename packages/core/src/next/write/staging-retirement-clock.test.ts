/**
 * R23.5: short cached walks share the worker task budget, including work before the first walk.
 * Derived from review 3's many-tables-clock-probe. Real tasks/timers; only monotonic work time is
 * injected: each observed group root costs 0.25 ms. Each table stays below one 4 ms slice.
 * The first publication after recovery must yield during cumulative retirement work.
 */
import { expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import * as merge from "./merge.js";
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

const reach = createReach("next-write-retirement-clock", [
  "short walks accumulate",
  "prior task work",
]);
const isRoot = (bytes: Uint8Array | null) =>
  bytes?.[0] === 77 && bytes[1] === 71 && bytes[2] === 82 && bytes[3] === 49;

it.each([0, 10])(
  "short unclaimed walks share the task deadline (prior work %i ms)",
  async (priorWork) => {
    const h = new Harness("strict", "sim", 8, undefined, { merges: { mode: "off" } });
    let handle: ReturnType<typeof setTimeout> | undefined;
    try {
      const p = await h.open();
      const tables: number[] = [];
      for (let n = 0; n < 100; n++)
        await autocommit(p, async (s) => {
          const result = await s.changeCatalog({
            kind: "createTable",
            table: tableInput(`t${String(n)}`, [column("id", INT, false)]),
          });
          tables.push(result.id ?? 0);
        });
      await p.quiesce();
      const tx = await p.begin();
      const statement = tx.statement({ rules });
      for (const id of tables)
        for (let from = 0; from < 24; from += 8)
          await statement
            .table(id)
            .insert(batch(ints(Array.from({ length: 8 }, (_, k) => from + k))), null);
      await statement.end();
      const q = await h.crash();
      const t = tables[tables.length - 1] ?? 0;
      let clock = priorWork;
      let timer = false;
      let firstRootAt = 0;
      let reads = 0;
      let yielded = false;
      const rootsAtYield: number[] = [];
      const realYield = merge.yieldTask;
      vi.spyOn(merge, "yieldTask").mockImplementation(async () => {
        rootsAtYield.push(reads);
        await realYield();
      });
      vi.spyOn(performance, "now").mockImplementation(() => clock);
      const observedRoot = () => {
        clock += 0.25;
        if (reads++ === 0) {
          firstRootAt = clock;
          handle = setTimeout(() => {
            timer = true;
          }, 0);
        } else yielded ||= timer;
      };
      const read = h.space.read.bind(h.space);
      h.space.read = async (refs, options) => {
        const lease = await read(refs, options);
        if (lease.bytes.some(isRoot)) observedRoot();
        return lease;
      };
      const peek = h.space.peek.bind(h.space);
      h.space.peek = (ref) => {
        const bytes = peek(ref);
        if (isRoot(bytes)) observedRoot();
        return bytes;
      };
      reach.beginHistory();
      // As in the review probe: the second commit frees the live tail for the first publication.
      await autocommit(q, (s) =>
        s.table(t).insert(batch(ints(Array.from({ length: 40 }, (_, k) => k))), null),
      );
      await autocommit(q, (s) => s.table(t).insert(batch(ints([40])), null));
      await q.quiesce();
      expect(reads).toBeGreaterThan(100);
      expect(clock - firstRootAt).toBeGreaterThan(40);
      reach.reach("short walks accumulate");
      if (priorWork > 0) reach.reach("prior task work");
      expect(yielded, "a timer runs before the last short walk completes").toBe(true);
      if (priorWork > 0)
        expect(rootsAtYield[0], "work before the first walk is not forgotten").toBe(0);
    } finally {
      if (handle !== undefined) clearTimeout(handle);
      vi.restoreAllMocks();
      await h.close();
    }
  },
  300_000,
);

it("reached every declared retirement-clock state", () => {
  reach.assertReached();
});
