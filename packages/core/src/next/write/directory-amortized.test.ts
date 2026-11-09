/** The directory writer's append contract: keep full pages, carry a geometric partial suffix,
 * and encode O(N log GROUPS_PER_PAGE) references across N singleton flushes. This uses the
 * existing arbitrary-page-size codec; deletions still rewrite from their first changed page.
 */
import fc from "fast-check";
import type { Statement } from "../access/modify.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { expect, it } from "vitest";
import { createReach, DIRECTORY_APPEND_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { GROUPS_PER_PAGE, decodeGroupPage, decodeGroupIndex } from "../format/manifest-codec.js";
import { need } from "./bytes.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  ints,
  locators,
  batch,
  rowsOf,
} from "./test-support.js";
const FREE = { allocate: () => true, free: () => undefined };
// Independent structural predicate, not the writer's Math.log2 partition helper.
function canonical(counts: readonly number[]): void {
  let tail = false,
    previous = GROUPS_PER_PAGE;
  for (const count of counts) {
    if (count === GROUPS_PER_PAGE) {
      expect(tail, "full pages precede the tail").toBe(false);
      continue;
    }
    tail = true;
    expect(count > 0 && (count & (count - 1)) === 0, "partial pages have power-of-two sizes").toBe(
      true,
    );
    expect(count, "partial sizes descend without repeats").toBeLessThan(previous);
    previous = count;
  }
}
const reach = createReach("next-directory-append", DIRECTORY_APPEND_REACH);
for (const seed of seedsFor("next-directory-append", [27951, 27952, 27953, 27954]))
  it(`singleton flushes amortize directory encoding (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 32, max: 128 }),
        fc.integer({ min: 1, max: 3 }),
        async (count, width) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", width, undefined, { merges: { mode: "off" } });
          let path = await h.open();
          try {
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
            let encoded = 0;
            const begin = h.space.beginWrite.bind(h.space);
            h.space.beginWrite = (...args) => {
              const w = begin(...args),
                write = w.write.bind(w);
              w.write = async (parts) => {
                for (const bytes of parts)
                  if (bytes[0] === 77 && bytes[1] === 71 && bytes[2] === 80 && bytes[3] === 49)
                    encoded += decodeGroupPage(bytes, FREE).value.length;
                return write(parts);
              };
              return w;
            };
            for (let k = 0; k <= count; k++) {
              await autocommit(path, (s) =>
                s
                  .table(t)
                  .insert(
                    batch(ints(Array.from({ length: width }, (_, i) => k * width + i))),
                    null,
                  ),
              );
              await path.quiesce();
            }
            const table = need(path.state().root.tables.find((x) => x.tableId === t));
            expect(table.groupCount, "every requested group was flushed").toBe(count);
            const lease = await h.space.read([table.groups], { budget: FREE });
            let pageCount = 0;
            try {
              const pages = decodeGroupIndex(need(lease.bytes[0]), FREE).value;
              canonical(pages.map((page) => page.groupCount));
              pageCount = pages.length;
            } finally {
              lease.release();
            }
            expect(
              encoded,
              "append must not repeatedly encode the growing prefix",
            ).toBeLessThanOrEqual(count * (2 + Math.ceil(Math.log2(count))));
            expect(
              pageCount,
              "partial-page reuse must stay bounded by a geometric tail",
            ).toBeLessThanOrEqual(1 + Math.ceil(Math.log2(count)));
            reach.reach("singleton append");
            reach.reach("geometric tail");
            await h.space.audit();
            reach.reach("exact extent ownership");
            // Rewriting an early partial page must fold the deletion and retire old shared blobs.
            await autocommit(path, (s) => s.table(t).delete(batch(locators([0], t)), null));
            await autocommit(path, (s) =>
              s
                .table(t)
                .insert(
                  batch(ints(Array.from({ length: width }, (_, i) => (count + 1) * width + i))),
                  null,
                ),
            );
            await path.quiesce();
            await h.space.audit();
            reach.reach("changed page");
            path = await h.reopen();
            const view = path.pin();
            let ids: number[];
            try {
              ids = (await rowsOf(view, t)).map((row) => Number(row[0])).sort((a, b) => a - b);
            } finally {
              view.release();
            }
            expect(ids).toEqual(Array.from({ length: (count + 2) * width - 1 }, (_, k) => k + 1));
            reach.reach("reopen preserves rows");
            await h.space.audit();
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("exact cleanup");
        },
      ),
      { seed, numRuns: 8 },
    );
  });

async function locateKey(statement: Statement, table: number, id: number): Promise<number> {
  const keys = new KeyBatch(FREE);
  try {
    encodeKeyBatch(
      "equality",
      [{ vector: ints([id]), direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      keys,
    );
    const output = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
    await statement.view
      .index(table, statement.view.catalog.table(table)?.primaryKey ?? 0)
      .probe(keys, output);
    expect(output.count).toBe(1);
    return need(output.locators[0]);
  } finally {
    keys.close();
  }
}
for (const seed of seedsFor("next-directory-mixed", [27961, 27962, 27963, 27964]))
  it(`mixed edits cannot accumulate a linear partial tail (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4, max: 5 }),
        fc.integer({ min: 1, max: 2 }),
        fc.integer({ min: 9, max: 11 }),
        async (power, width, rounds) => {
          reach.beginHistory();
          const base = 2 ** power,
            h = new Harness("strict", "memory", width, undefined, { merges: { mode: "off" } });
          let path = await h.open();
          try {
            let t = 0,
              nextGroup = 0;
            const live = new Set<number>();
            await autocommit(path, async (s) => {
              t =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("mixed", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            const append = async () => {
              const ids = Array.from({ length: width }, (_, k) => nextGroup * width + k);
              await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
              for (const id of ids) live.add(id);
              nextGroup++;
              await path.quiesce();
            };
            const bounded = async () => {
              const table = need(path.state().root.tables.find((x) => x.tableId === t));
              const lease = await h.space.read([table.groups], { budget: FREE });
              try {
                const pages = decodeGroupIndex(need(lease.bytes[0]), FREE).value;
                canonical(pages.map((page) => page.groupCount));
                expect(
                  pages.length,
                  "mixed mutations must not accumulate a linear partial tail",
                ).toBeLessThanOrEqual(1 + Math.ceil(Math.log2(Math.max(1, table.groupCount))));
              } finally {
                lease.release();
              }
            };
            // One full live group remains for the first delete admission; recycling flushes it too.
            for (let group = 0; group < 2 * base - 1; group++) await append();
            for (let round = 0; round < rounds; round++) {
              const first = round === 0 ? base * width : nextGroup * width;
              if (round > 0) for (let n = 0; n < base - 1 - round; n++) await append();
              await bounded();
              await autocommit(path, async (s) =>
                s.table(t).delete(batch(locators([await locateKey(s, t, first)], t)), null),
              );
              live.delete(first);
              await path.recycleLog();
              await path.quiesce();
              expect(need(path.state().root.tables.find((x) => x.tableId === t)).groupCount).toBe(
                nextGroup,
              );
              await bounded();
              await h.space.audit();
              reach.reach("mixed mutations");
              reach.reach("bounded tail after mutation");
            }
            path = await h.reopen();
            const view = path.pin();
            try {
              expect(
                (await rowsOf(view, t)).map((row) => Number(row[0])).sort((a, b) => a - b),
              ).toEqual([...live].sort((a, b) => a - b));
            } finally {
              view.release();
            }
            await h.space.audit();
            reach.reach("reopen preserves rows");
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("exact cleanup");
        },
      ),
      { seed, numRuns: 8 },
    );
  }, 60_000);

for (const seed of seedsFor("next-directory-batches", [27971, 27972, 27973, 27974]))
  it(`variable batch flushes retain a canonical bounded tail (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4, max: 5 }),
        fc.integer({ min: 1, max: 2 }),
        fc.integer({ min: 3, max: 6 }),
        async (power, width, batches) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", width, undefined, { merges: { mode: "off" } });
          let p = await h.open();
          try {
            let t = 0,
              next = 0;
            await autocommit(p, async (s) => {
              t =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("batches", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            for (let call = 0; call < batches; call++) {
              const beforeGroups =
                p.state().root.tables.find((table) => table.tableId === t)?.groupCount ?? 0;
              const beforeFlushes = p.maintenance.flushes;
              // Whole transactions span many frozen memtables; shrinking batches used to leave
              // nearly equal partial pages even without a deletion.
              const rows = (2 ** power - 1 - 2 * call) * width;
              const ids = Array.from({ length: rows }, (_, k) => next + k);
              next += rows;
              await autocommit(p, (s) => s.table(t).insert(batch(ints(ids)), null));
              await autocommit(p, (s) =>
                s.table(t).insert(batch(ints(Array.from({ length: width }, () => next++))), null),
              );
              await p.quiesce();
              const table = need(p.state().root.tables.find((x) => x.tableId === t));
              expect(table.groupCount).toBe(next / width - 1);
              expect(
                table.groupCount - beforeGroups,
                "at least one flush carries multiple groups",
              ).toBeGreaterThan(p.maintenance.flushes - beforeFlushes);
              const lease = await h.space.read([table.groups], { budget: FREE });
              try {
                const pages = decodeGroupIndex(need(lease.bytes[0]), FREE).value;
                canonical(pages.map((page) => page.groupCount));
                expect(pages.length).toBeLessThanOrEqual(
                  1 + Math.ceil(Math.log2(table.groupCount)),
                );
              } finally {
                lease.release();
              }
              await h.space.audit();
            }
            reach.reach("variable batches");
            reach.reach("multi-group flush");
            p = await h.reopen();
            const v = p.pin();
            try {
              expect((await rowsOf(v, t)).map((x) => Number(x[0])).sort((a, b) => a - b)).toEqual(
                Array.from({ length: next }, (_, k) => k),
              );
            } finally {
              v.release();
            }
            await h.space.audit();
            reach.reach("reopen preserves rows");
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("exact cleanup");
        },
      ),
      { seed, numRuns: 8 },
    );
  }, 60_000);

it("reached directory append states", () => reach.assertReached());
