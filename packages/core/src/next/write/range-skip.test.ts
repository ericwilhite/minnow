/** OFFSET law: PostgreSQL supplies ordered results; snapshot copies freeze its single connection.
 * Minnow pins stay open across later commits, staging, rollback and publication.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RANGE_SKIP_REACH } from "../../testing/reach.js";
import type { Snapshot } from "../access/snapshot.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { SnapshotReleasedError } from "../access/snapshot.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  locators,
  rules,
  tableInput,
} from "./test-support.js";
import { need } from "./bytes.js";
import { RunRangeInput } from "./range-input.js";

const ALL = { lower: null, upper: null };
const FREE = { allocate: () => true, free: () => undefined };
const reach = createReach("visible index skip", WRITE_RANGE_SKIP_REACH);
let pg: PGlite | null = null;
afterAll(async () => {
  await pg?.close();
  reach.assertReached();
});

async function locate(view: Snapshot, table: number, ids: number[]): Promise<number[]> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(ids), direction: "asc", nulls: "first" }],
    { length: ids.length, selection: null },
    keys,
  );
  const out = {
    probes: new Int32Array(ids.length),
    locators: new Float64Array(ids.length),
    count: 0,
  };
  try {
    await view.index(table, view.catalog.table(table)?.primaryKey ?? 0).probe(keys, out);
    expect(out.count).toBe(ids.length);
    return Array.from(out.locators.subarray(0, out.count));
  } finally {
    keys.close();
  }
}

async function check(view: Snapshot, table: number, source: string, offset: number): Promise<void> {
  for (const direction of ["asc", "desc"] as const) {
    reach.reach(direction);
    if (direction === "desc" && offset >= 546) reach.reach("descending multiple pages");
    const result = await need(pg).query<{ id: number }>(
      `SELECT id FROM ${source} ORDER BY id ${direction} OFFSET $1`,
      [offset],
    );
    const total = need(
      (await need(pg).query<{ n: number }>(`SELECT count(*)::int AS n FROM ${source}`)).rows[0],
    ).n;
    const expected = await locate(
      view,
      table,
      result.rows.map((r) => r.id),
    );
    const cursor = view
      .index(table, view.catalog.table(table)?.primaryKey ?? 0)
      .range(ALL, direction);
    try {
      expect(await cursor.skip(0)).toBe(0);
      const first = Math.floor(offset / 2);
      expect(await cursor.skip(first)).toBe(Math.min(total, first));
      expect(await cursor.skip(offset - first)).toBe(
        Math.min(Math.max(0, total - first), offset - first),
      );
      const actual: number[] = [],
        out = new Float64Array(3);
      for (;;) {
        const n = await cursor.next(out);
        if (n === 0) break;
        actual.push(...out.subarray(0, n));
      }
      expect(actual).toEqual(expected);
      expect(await cursor.skip(1)).toBe(0);
      reach.reach("repeated skip");
      reach.reach("end");
      if (offset === 0) reach.reach("zero");
      if (offset > total) reach.reach("past end");
    } finally {
      cursor.close();
    }
  }
}

for (const seed of seedsFor("next-write-range-skip", [0xb701, 0xb702]))
  it(`agrees with PostgreSQL OFFSET across concurrent pins and pending staging (seed ${String(seed)})`, async () => {
    pg ??= await PGlite.create();
    await fc.assert(
      fc.asyncProperty(
        fc.uniqueArray(fc.integer({ min: -100, max: 100 }), { minLength: 16, maxLength: 40 }),
        fc.array(
          fc.record({
            kind: fc.constantFrom("insert", "delete", "update"),
            key: fc.integer({ min: 0, max: 100 }),
          }),
          { minLength: 5, maxLength: 12 },
        ),
        fc.integer({ min: 0, max: 60 }),
        fc.boolean(),
        async (values, actions, offset, stored) => {
          reach.beginHistory();
          await need(pg).exec(
            "DROP TABLE IF EXISTS t, old, pending; CREATE TABLE t(id int PRIMARY KEY)",
          );
          await need(pg).query("INSERT INTO t SELECT unnest($1::int[])", [values]);
          const h = new Harness("strict", "memory", 4),
            p = await h.open();
          let table = 0;
          try {
            await autocommit(p, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            await autocommit(p, (s) => s.table(table).insert(batch(ints(values)), null));
            if (stored) {
              await p.recycleLog();
              await p.quiesce();
              reach.reach("stored");
            } else reach.reach("memtable");
            const old = p.pin();
            await need(pg).exec("CREATE TEMP TABLE old AS SELECT * FROM t");
            try {
              // Every history exercises delete and key-changing update, plus randomized operations.
              for (const [step, a] of [
                { kind: "delete", key: 0 },
                { kind: "update", key: 1 },
                ...actions,
              ].entries()) {
                const ids = (
                  await need(pg).query<{ id: number }>("SELECT id FROM t ORDER BY id")
                ).rows.map((r) => r.id);
                const chosen = need(ids[a.key % ids.length]),
                  added = 1000 + step;
                const pin = p.pin();
                try {
                  if (a.kind === "insert" || ids.length === 0) {
                    await autocommit(p, (s) => s.table(table).insert(batch(ints([added])), null));
                    await need(pg).query("INSERT INTO t VALUES ($1)", [added]);
                    reach.reach("insert");
                  } else if (a.kind === "delete") {
                    const at = await locate(pin, table, [chosen]);
                    await autocommit(p, (s) =>
                      s.table(table).delete(batch(locators(at, table)), null),
                    );
                    await need(pg).query("DELETE FROM t WHERE id=$1", [chosen]);
                    reach.reach("delete");
                  } else {
                    const at = await locate(pin, table, [chosen]);
                    await autocommit(p, (s) =>
                      s
                        .table(table, {
                          assigned: [need(need(p.catalog.table(table)).columns[0]).id],
                        })
                        .update(batch(locators(at, table), ints([added])), null),
                    );
                    await need(pg).query("UPDATE t SET id=$1 WHERE id=$2", [added, chosen]);
                    reach.reach("update");
                  }
                } finally {
                  pin.release();
                }
              }
              if (stored) {
                await p.recycleLog();
                await p.quiesce();
              }
              const current = p.pin(),
                tx = await p.begin();
              await need(pg).exec("CREATE TEMP TABLE pending AS SELECT * FROM t");
              try {
                for (let from = 0; from < 20; from += 4) {
                  const s = tx.statement({ rules }),
                    ids = Array.from({ length: 4 }, (_, k) => -200 + from + k);
                  await s.table(table).insert(batch(ints(ids)), null);
                  await s.end();
                  await need(pg).query("INSERT INTO pending SELECT unnest($1::int[])", [ids]);
                }
                expect(p.maintenance.stages).toBeGreaterThan(0);
                const outsider = p.pin();
                try {
                  expect(
                    outsider
                      .internalTable(table)
                      .tableView.memtables.some(
                        (m) => m.staged !== null && m.visibleRowIdEnd(outsider.bound) !== Infinity,
                      ),
                  ).toBe(true);
                  for (const n of [0, offset, 100]) await check(outsider, table, "t", n);
                  reach.reach("other staged rows");
                } finally {
                  outsider.release();
                }
                reach.reach("old pin");
                const own = tx.statement({ rules });
                try {
                  await check(own.view, table, "pending", offset);
                  reach.reach("own earlier statement");
                } finally {
                  await own.end();
                }
                for (const [view, source] of [
                  [old, "old"],
                  [current, "t"],
                ] as const) {
                  for (const n of [0, offset, 100]) await check(view, table, source, n);
                }
                const late = p.pin();
                try {
                  if (offset % 2 === 0) {
                    await (
                      await tx.commit(null)
                    ).durable;
                    await p.quiesce();
                    reach.reach("pending commit while pinned");
                  } else {
                    tx.rollback();
                    reach.reach("rollback");
                  }
                  await check(late, table, "t", offset);
                  const after = p.pin();
                  try {
                    await check(after, table, offset % 2 === 0 ? "pending" : "t", offset);
                  } finally {
                    after.release();
                  }
                  await check(current, table, "t", offset);
                } finally {
                  late.release();
                }
              } finally {
                tx.rollback();
                current.release();
              }
            } finally {
              old.release();
            }
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
  }, 120_000);

it("drains disjoint stored blocks, mixes next/skip, and refuses bad counts", async () => {
  const h = new Harness("strict", "memory"),
    p = await h.open();
  let t = 0;
  try {
    await autocommit(p, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(p, (s) =>
      s.table(t).insert(batch(ints(Array.from({ length: 4000 }, (_, i) => i))), null),
    );
    await p.recycleLog();
    await p.quiesce();
    const pin = p.pin(),
      cursor = pin.index(t, need(need(p.catalog.table(t)).primaryKey)).range(ALL, "asc");
    const spy = vi.spyOn(RunRangeInput.prototype, "skipBlock");
    try {
      for (const n of [-1, 0.5, NaN, Infinity, 2 ** 53])
        expect(() => cursor.skip(n)).toThrow(RangeError);
      const out = new Float64Array(2);
      expect(await cursor.next(out)).toBe(2);
      expect(await cursor.skip(3995)).toBe(3995);
      expect(spy).toHaveBeenCalled();
      reach.reach("disjoint blocks");
      expect(await cursor.next(out)).toBe(2);
      expect(Array.from(out)).toEqual(await locate(pin, t, [3997, 3998]));
      expect(await cursor.skip(4)).toBe(1);
      expect(await cursor.next(out)).toBe(0);
      for (const direction of ["asc", "desc"] as const) {
        const seek = pin.index(t, need(need(p.catalog.table(t)).primaryKey)).range(ALL, direction);
        const pages = vi.spyOn(RunRangeInput.prototype, "skipPages");
        try {
          expect(await seek.skip(3995)).toBe(3995);
          expect(pages).toHaveBeenCalled();
          reach.reach("fixed page counts");
          expect(await seek.next(out)).toBe(2);
          expect(Array.from(out)).toEqual(
            await locate(pin, t, direction === "asc" ? [3995, 3996] : [4, 3]),
          );
        } finally {
          seek.close();
          pages.mockRestore();
        }
      }
      const whole = pin.index(t, need(need(p.catalog.table(t)).primaryKey)).range(ALL, "asc");
      const advance = vi.spyOn(RunRangeInput.prototype, "advance");
      try {
        expect(await whole.skip(4000)).toBe(4000);
        expect(advance).not.toHaveBeenCalled();
        reach.reach("whole run count");
      } finally {
        whole.close();
        advance.mockRestore();
      }
      pin.release();
      expect(() => cursor.skip(1)).toThrow(SnapshotReleasedError);
    } finally {
      spy.mockRestore();
      cursor.close();
      pin.release();
    }
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

for (const seed of seedsFor("next-write-range-skip-pages", [0xb703]))
  it(`uses fixed page counts across generated boundaries (seed ${String(seed)})`, async () => {
    pg ??= await PGlite.create();
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 300, max: 2000 }),
        fc.integer({ min: 0, max: 100000 }),
        fc.boolean(),
        async (count, choice, reverse) => {
          reach.beginHistory();
          const ids = Array.from({ length: count }, (_, i) => i - 1000);
          if (reverse) {
            ids.reverse();
            reach.reach("reverse insertion");
          }
          await need(pg).exec("DROP TABLE IF EXISTS fixed; CREATE TABLE fixed(id int PRIMARY KEY)");
          await need(pg).query("INSERT INTO fixed SELECT unnest($1::int[])", [ids]);
          const h = new Harness("strict", "memory"),
            p = await h.open();
          let table = 0;
          try {
            await autocommit(p, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            await autocommit(p, (s) => s.table(table).insert(batch(ints(ids)), null));
            await p.recycleLog();
            await p.quiesce();
            const view = p.pin();
            try {
              await check(view, table, "fixed", choice % (count + 20));
            } finally {
              view.release();
            }
            reach.reach("fixed page counts generated");
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12, examples: [[1089, 6091, false]] },
    );
  }, 120_000);
