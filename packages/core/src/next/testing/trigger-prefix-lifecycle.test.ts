/** Phase pins may survive publication and writes only for spooled locator resolution. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_PREFIX_LIFECYCLE_REACH } from "../../testing/reach.js";
import {
  Harness,
  INT,
  autocommit,
  batch,
  column,
  ints,
  locators,
  rowsOf,
  tableInput,
} from "../write/test-support.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import type { Snapshot } from "../access/snapshot.js";

for (const seed of seedsFor("next-trigger-prefix-lifecycle", [0xb7cc, 0xb7cd]))
  it(`seals prefix reads and retires native retention across batches seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("trigger prefix lifecycle", TRIGGER_PREFIX_LIFECYCLE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 8, max: 12 }),
          fc.integer({ min: 1, max: 3 }),
          fc.integer({ min: 2, max: 3 }),
          fc.constantFrom("commit", "cancel read", "cancel write"),
          fc.boolean(),
          async (count, size, bodies, finish, merge) => {
            reach.beginHistory();
            let releasePublish: () => void = () => undefined,
              entered: () => void = () => undefined;
            const gate = new Promise<void>((r) => {
                releasePublish = r;
              }),
              started = new Promise<void>((r) => {
                entered = r;
              }),
              h = new Harness("strict", "memory", 2, undefined, {
                beforeFlushPublish: () => {
                  entered();
                  return gate;
                },
                merges: { mode: "manual", fanIn: 2, stallRuns: 100 },
              });
            const pins: Snapshot[] = [],
              cancel = new AbortController(),
              reason = new Error("cancel prefix body");
            let tx: Awaited<ReturnType<Harness["path"]["begin"]>> | undefined;
            await h.open();
            const grant = await h.governor.admit({
                label: "prefix lifecycle keys",
                minimumBytes: 65536,
                spillable: true,
              }),
              keys = new KeyBatch(grant);
            try {
              await pg.exec("DROP TABLE IF EXISTS t;CREATE TABLE t(id INT PRIMARY KEY,v INT)");
              await autocommit(h.path, (s) =>
                s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("t", [column("id", INT, false), column("v", INT, false)]),
                }),
              );
              const table = h.path.catalog.tableByName("t");
              if (table?.primaryKey == null || !table.columns[1]) throw new Error("missing target");
              const primary = table.primaryKey,
                value = table.columns[1].id,
                ids = Array.from({ length: count }, (_, i) => i + 1);
              await autocommit(h.path, async (s) => {
                const w = s.table(table.id);
                for (let from = 0; from < count; from += 2) {
                  const part = ids.slice(from, from + 2);
                  await w.insert(batch(ints(part), ints(part.map((n) => n * 10))), null);
                }
              });
              await pg.exec(
                `INSERT INTO t VALUES ${ids.map((n) => `(${String(n)},${String(n * 10)})`).join(",")}`,
              );
              await started; // a real committed flush is paused before publication
              encodeKeyBatch(
                "equality",
                [{ vector: ints(ids), direction: "asc", nulls: "first" }],
                { length: count, selection: null },
                keys,
              );
              tx = await h.path.begin();
              const root = tx.statement({
                rules: () => ({ check: () => null, generate: () => undefined }),
              });
              await pg.exec("BEGIN");
              const locate = async (pin: Snapshot) => {
                const out = {
                  probes: new Int32Array(count),
                  locators: new Float64Array(count),
                  count: 0,
                };
                await pin.index(table.id, primary).probe(keys, out);
                expect(out.count).toBe(count);
                const at = new Array<number>(count);
                for (let k = 0; k < out.count; k++) at[out.probes[k] ?? 0] = out.locators[k] ?? 0;
                return at;
              };
              let cancelled = false;
              try {
                for (let body = 0; body < bodies; body++) {
                  const pin = root.readOwn();
                  pins.push(pin);
                  // Keep a second non-borrowing owner alive: retiring the last reader would
                  // otherwise free all rows and hide a missing per-pin borrow release.
                  const twin = body === 0 ? root.readOwn() : null;
                  if (twin) pins.push(twin);
                  const observed = await rowsOf(pin, table.id),
                    expected = (
                      await pg.query<{ id: number; v: number }>("SELECT * FROM t ORDER BY id")
                    ).rows.map((r) => [r.id, r.v]);
                  expect(observed).toEqual(expected);
                  const source = pin.table(table.id, { budget: grant }),
                    segment = source.segments[0],
                    cachedIndex = pin.index(table.id, primary),
                    cachedCursor = cachedIndex.range({ lower: null, upper: null }, "asc"),
                    at = await locate(pin);
                  if (!segment) throw new Error("missing source");
                  expect(h.path.shared.readers.count).toBeGreaterThan(0);
                  if (body === 0) {
                    releasePublish();
                    await h.path.quiesce();
                    expect(h.path.maintenance.flushes).toBeGreaterThan(0);
                    expect(h.path.stats().borrowedChunkBytes).toBeGreaterThan(0);
                    reach.reach("publication with borrowed prefix");
                    // Maintenance changes the physical root, not the current-prefix read phase.
                    expect(await rowsOf(pin, table.id)).toEqual(expected);
                    if (merge) {
                      await h.path.runMaintenance();
                      expect(h.path.maintenance.merges).toBeGreaterThan(0);
                      expect(await rowsOf(pin, table.id)).toEqual(expected);
                      reach.reach("merge with prefix alive");
                    } else reach.reach("flush only");
                    if (finish === "cancel read") {
                      cancel.abort(reason);
                      cancel.signal.throwIfAborted();
                    }
                  }
                  const writer = root.table(table.id, { assigned: [value], inputView: pin });
                  let batches = 0;
                  for (let from = 0; from < count; from += size) {
                    cancel.signal.throwIfAborted();
                    const end = Math.min(count, from + size);
                    await writer.update(
                      batch(
                        locators(at.slice(from, end), table.id),
                        ints(expected.slice(from, end).map((r) => (r[1] ?? 0) + 1)),
                      ),
                      null,
                    );
                    batches++;
                    if (from === 0) {
                      expect(() => pin.table(table.id, { budget: grant })).toThrow("locator-only");
                      expect(() => pin.index(table.id, primary)).toThrow("locator-only");
                      expect(() => source.segments).toThrow("locator-only");
                      expect(() => segment.deletions()).toThrow("locator-only");
                      expect(() =>
                        cachedIndex.probe(keys, {
                          probes: new Int32Array(count),
                          locators: new Float64Array(count),
                          count: 0,
                        }),
                      ).toThrow("locator-only");
                      try {
                        await expect(
                          Promise.resolve().then(() => cachedCursor.next(new Float64Array(1))),
                        ).rejects.toThrow("locator-only");
                        reach.reach("cached cursor read refused");
                      } finally {
                        cachedCursor.close();
                      }
                      reach.reach("cached and fresh rereads refused");
                      if (body === 0 && finish === "cancel write") {
                        cancel.abort(reason);
                        cancel.signal.throwIfAborted();
                      }
                      await h.path.runMaintenance(); // later writer batches still use the original locators
                    }
                  }
                  expect(batches).toBeGreaterThan(1);
                  reach.reach("multiple write batches");
                  await pg.exec("UPDATE t SET v=v+1");
                  pin.release();
                  if (twin) {
                    expect(twin.released).toBe(false);
                    expect(h.path.shared.readers.count).toBeGreaterThan(0);
                    expect(h.path.stats().borrowedChunkBytes).toBe(0);
                    reach.reach("borrow ends before independent pin");
                    twin.release();
                  }
                }
              } catch (error) {
                if (error !== reason) throw error;
                if (finish === "commit") throw error;
                cancelled = true;
                reach.reach(finish);
              }
              if (cancelled) {
                root.rollback();
                tx.rollback();
                await pg.exec("ROLLBACK");
              } else {
                await root.end();
                await (
                  await tx.commit(null)
                ).durable;
                await pg.exec("COMMIT");
                reach.reach("committed bodies");
              }
              tx = undefined;
              for (const pin of pins) expect(pin.released).toBe(true);
              const final = h.path.pin();
              try {
                expect(await rowsOf(final, table.id)).toEqual(
                  (
                    await pg.query<{ id: number; v: number }>("SELECT * FROM t ORDER BY id")
                  ).rows.map((r) => [r.id, r.v]),
                );
              } finally {
                final.release();
              }
              await h.path.quiesce();
              expect(h.path.shared.readers.count).toBe(0);
              expect(h.path.shared.readers.heldPins).toBe(0);
              expect(h.path.stats().borrowedChunkBytes).toBe(0);
              keys.close();
              expect(grant.allocated).toBe(0);
              grant.close();
              await h.path.close();
              expect(h.path.stats()).toMatchObject({
                memtableBytes: 0,
                frozenMemtableBytes: 0,
                borrowedChunkBytes: 0,
              });
              expect(h.path.shared.budget).toMatchObject({ allocated: 0, loaned: 0 });
              expect(h.path.shared.readers.count).toBe(0);
              expect(h.path.stats()).toMatchObject({
                borrowedChunkBytes: 0,
                repointPins: 0,
                pins: 0,
              });
              reach.reach("zero native retention and registrations");
              reach.reach(size === 1 ? "single row batches" : "several row batches");
            } finally {
              releasePublish();
              tx?.rollback();
              for (const pin of pins) pin.release();
              keys.close();
              grant.close();
              await h.close();
              await pg.exec("ROLLBACK;DROP TABLE IF EXISTS t");
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [8, 1, 2, "commit", false],
            [10, 3, 3, "commit", true],
            [9, 2, 2, "cancel read", true],
            [12, 1, 2, "cancel write", false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
