/** Limits belong to catalog paths. Live range admission and FK fallback must remain complete. */
import fc from "fast-check";
import { historyBody } from "../../testing/history-body.test-support.js";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, LONG_KEY_ACTION_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { encodeEqualityKey } from "../keys/key-encoding.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { ChunkReader } from "../page/reader.js";
import { Governor } from "../memory/governor.js";
import { RunSink } from "./run-output.js";
import { scanIndexHolders } from "./index-scan.js";
import { need } from "./bytes.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  TEXT,
  INT,
  ints,
  texts,
  locators,
  tableInput,
  rowsOf,
  rules,
} from "./test-support.js";
const reach = createReach("long key actions and admission", LONG_KEY_ACTION_STATES);
afterAll(() => reach.assertReached());
const value = (units: number, n: number, glyph = "\u0800") =>
  glyph.repeat(units - 1) + String.fromCharCode(0x0800 + n);
const key = (s: string) => encodeEqualityKey([{ type: TEXT, value: new TextEncoder().encode(s) }]);
async function target(h: Harness, t: number, pk: number, value: string) {
  const view = h.path.pin();
  try {
    const rows: number[] = [];
    await view.index(t, pk).resolve(key(value), rows);
    expect(rows).toHaveLength(1);
    return view.internalTable(t).locator(need(rows[0]));
  } finally {
    view.release();
  }
}
for (const seed of seedsFor("next-long-live-range", [0xb515]))
  it(`admits a live memtable's future key width before publication (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(13107, 16384),
        fc.constantFrom("asc", "desc"),
        fc.constantFrom("\u0800", "\0"),
        async (units, direction, glyph) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
          await h.open();
          let old: ReturnType<typeof h.path.pin> | undefined;
          try {
            let t = 0;
            await autocommit(h.path, async (s) => {
              t = need(
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("k", TEXT, false), column("n", INT, false)]),
                  })
                ).id,
              );
            });
            await autocommit(h.path, (s) =>
              s.table(t).insert(batch(texts(["a", "b"]), ints([1, 2])), null),
            );
            old = h.path.pin();
            const index = old.index(t, need(old.catalog.table(t)?.primaryKey));
            const firstRow = need(old.internalTable(t).tableView.memtables[0]).firstRowId;
            const plan = index.planRange({ lower: null, upper: null }, direction);
            await autocommit(h.path, (s) =>
              s
                .table(t)
                .insert(
                  batch(texts([value(units, 3, glyph), value(units, 4, glyph)]), ints([3, 4])),
                  null,
                ),
            );
            await autocommit(h.path, (s) =>
              s.table(t).insert(batch(texts(["z"]), ints([5])), null),
            );
            await h.path.quiesce();
            const ref = need(
              old.internalTable(t).tableView.memtables[0]?.published?.runs.get(index.def.id),
            );
            expect(ref.encoding).toBe(2);
            expect(ref.keyStorage?.maxKeyBytes).toBe(
              glyph === "\0" ? 2 * units + 4 : 3 * units + 3,
            );
            const current = h.path.pin();
            try {
              for (const n of [3, 4]) {
                const found: number[] = [];
                await current.index(t, index.def.id).resolve(key(value(units, n, glyph)), found);
                expect(found).toHaveLength(1);
              }
            } finally {
              current.release();
            }
            if (glyph === "\0") reach.reach("NUL published key");
            // Independent lower bound: two pair buffers and at least one full input head must fit,
            // independently of the two verified IO slabs and parser room.
            expect(plan.minimumBytes - 2 * MAX_BLOB_BYTES - 128 * 1024).toBeGreaterThanOrEqual(
              3 * need(ref.keyStorage).maxKeyBytes,
            );
            const grant = await h.governor.admit({
                label: "planned before long invisible rows",
                minimumBytes: plan.minimumBytes,
                spillable: false,
              }),
              held = grant.held;
            const cursor = plan.open({ budget: grant, signal: new AbortController().signal }),
              out = new Float64Array(8);
            try {
              expect(await cursor.next(out)).toBe(2);
              expect(await cursor.next(out)).toBe(0);
              const table = old.internalTable(t);
              expect(Array.from(out.subarray(0, 2), (n) => table.rowId(n))).toEqual(
                direction === "asc" ? [firstRow, firstRow + 1] : [firstRow + 1, firstRow],
              );
              expect(grant.held).toBe(held);
              expect(grant.loaned).toBe(0);
            } finally {
              cursor.close();
              expect(grant.allocated).toBe(0);
              grant.close();
            }
            reach.reach(direction === "asc" ? "forward planned" : "reverse planned");
            reach.reach("future width at fixed admission");
            reach.reach("automatic overflow selection");
          } finally {
            old?.release();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 16 },
    );
  }, 120_000);
for (const seed of seedsFor("next-long-fk-fallback", [0xb516]))
  it(`keeps FK actions exact after their support index becomes invalid (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(13107, 16384),
        fc.constantFrom("cascade", "restrict", "setNull"),
        async (units, action) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 2, undefined, { merges: { mode: "off" } });
          await h.open();
          let p = 0,
            c = 0,
            pk = 0;
          const a = value(units, 0),
            b = value(units, 1);
          const begin = h.space.beginWrite.bind(h.space),
            estimates = vi.spyOn(h.space, "beginWrite").mockImplementation((...args) => {
              const writer = begin(...args),
                open = writer.open.bind(writer);
              vi.spyOn(writer, "open").mockImplementation((capacity) => {
                if (capacity > 8 * 1024 * 1024)
                  throw new Error("Known-row extent estimate exceeds 8 MiB");
                return open(capacity);
              });
              return writer;
            });
          try {
            await autocommit(h.path, async (s) => {
              p = need(
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("p", [column("k", TEXT, false), column("n", INT, false)]),
                  })
                ).id,
              );
            });
            pk = need(h.path.catalog.table(p)?.primaryKey);
            await autocommit(h.path, async (s) => {
              c = need(
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput(
                      "c",
                      [
                        column("id", INT, false),
                        column("k", TEXT, true),
                        column("body", TEXT, false),
                      ],
                      {
                        foreignKeys: [
                          {
                            name: "fk",
                            columns: [1],
                            parentTableId: p,
                            parentIndexId: pk,
                            onDelete: action,
                            enforced: true,
                          },
                        ],
                      },
                    ),
                  })
                ).id,
              );
            });
            await autocommit(h.path, (s) => s.table(p).insert(batch(texts([a]), ints([1])), null));
            await autocommit(h.path, (s) =>
              s
                .table(c)
                .insert(
                  batch(ints([1, 2]), texts([a, a]), texts(["payload".repeat(20_000), "short"])),
                  null,
                ),
            );
            const support = need(h.path.catalog.table(c)?.foreignKeys[0]?.supportIndexId);
            expect(h.path.catalog.index(support)?.invalid).toBe(true);
            const locator = await target(h, p, pk, a),
              parentColumn = need(h.path.catalog.table(p)?.columns[0]).id;
            await autocommit(h.path, (s) =>
              s
                .table(p, { assigned: [parentColumn] })
                .update(batch(locators([locator], p), texts([b])), null),
            );
            let view = h.path.pin();
            try {
              expect((await rowsOf(view, c)).map((r) => r[1])).toEqual([b, b]);
            } finally {
              view.release();
            }
            reach.reach("invalid support rekey");
            await h.path.recycleLog();
            await h.path.quiesce();
            expect(
              h.space.stats.dataBytes,
              "known-row extents stay proportional to actual rows",
            ).toBeLessThan(8 * 1024 * 1024);
            await h.reopen();
            view = h.path.pin();
            try {
              const table = view.internalTable(c),
                bodyId = need(table.columns.find((col) => col.name === "body")).id;
              const open = ChunkReader.open.bind(ChunkReader),
                spy = vi.spyOn(ChunkReader, "open").mockImplementation((space, stored, options) => {
                  expect(stored.columnId, "fallback never reads covering payload").not.toBe(bodyId);
                  return open(space, stored, options);
                });
              let found: Awaited<ReturnType<typeof scanIndexHolders>> | undefined;
              try {
                found = await scanIndexHolders(
                  table,
                  [need(table.columns.find((col) => col.name === "k")).id],
                  key(b),
                );
                expect(found.value).toHaveLength(2);
              } finally {
                found?.release();
                spy.mockRestore();
              }
              reach.reach("selective durable fallback");
              // Each constructor/allocation failure must unwind even a partially built KeyBatch.
              const original = view.shared,
                base = original.budget;
              for (let fail = 1; fail <= 6; fail++) {
                let calls = 0,
                  balance = 0;
                const fault = new Error("injected FK scan allocation");
                const budget = {
                  allocate: (n: number) => {
                    if (++calls === fail) throw fault;
                    const yes = base.allocate(n);
                    if (yes) balance += n;
                    return yes;
                  },
                  free: (n: number) => {
                    balance -= n;
                    base.free(n);
                  },
                };
                Object.assign(view, { shared: { ...original, budget } });
                let held: Awaited<ReturnType<typeof scanIndexHolders>> | undefined;
                try {
                  held = await scanIndexHolders(
                    table,
                    [need(table.columns.find((col) => col.name === "k")).id],
                    key(b),
                  );
                } catch (error) {
                  expect(error).toBe(fault);
                } finally {
                  held?.release();
                  Object.assign(view, { shared: original });
                }
                expect(balance).toBe(0);
              }
              reach.reach("fallback allocation cleanup");
            } finally {
              view.release();
            }
            const next = await target(h, p, pk, b),
              remove = () =>
                autocommit(h.path, (s) => s.table(p).delete(batch(locators([next], p)), null));
            if (action === "restrict")
              await expect(remove()).rejects.toMatchObject({
                code: "MINNOW_FOREIGN_KEY_VIOLATION",
              });
            else await remove();
            view = h.path.pin();
            try {
              const rows = await rowsOf(view, c);
              if (action === "cascade") expect(rows).toHaveLength(0);
              else {
                expect(rows).toHaveLength(2);
                expect(rows.map((r) => r[1])).toEqual(action === "setNull" ? [null, null] : [b, b]);
              }
            } finally {
              view.release();
            }
            reach.reach(
              action === "cascade"
                ? "foreign cascade"
                : action === "restrict"
                  ? "foreign restrict"
                  : "foreign set null",
            );
          } finally {
            estimates.mockRestore();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
  }, 180_000);

const flushHistories = 24;
for (const seed of seedsFor("next-long-flush-bytes", [0xb51a]))
  for (let ordinal = 0; ordinal < flushHistories; ordinal++)
    it(`drains wide flushes by bytes under a constrained cap (seed ${String(seed)}, history ${String(ordinal + 1)})`, async () => {
      const enter = historyBody(ordinal);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(769, 16384),
          fc.constantFrom(32, 64),
          fc.constantFrom("auto", "commit", "rollback", "savepoint"),
          fc.constantFrom(8, 80, 160),
          fc.constantFrom("\u0800", "\0"),
          async (units, cap, outcome, batchRows, glyph) =>
            enter(async () => {
              reach.beginHistory();
              const h = new Harness("strict", "memory", 160, undefined, {
                merges: { mode: "off" },
              });
              Object.assign(h, { governor: new Governor(cap << 20) });
              await h.open();
              const method = Object.getOwnPropertyDescriptor(RunSink.prototype, "drain")
                ?.value as unknown;
              if (typeof method !== "function") throw new Error("RunSink drain missing");
              const drain = method as (
                this: RunSink,
                ...args: Parameters<RunSink["drain"]>
              ) => ReturnType<RunSink["drain"]>;
              let calls = 0;
              let pressure: Awaited<ReturnType<Governor["admit"]>> | undefined;
              let foreignBuffer: Uint8Array | undefined;
              const observed = vi.spyOn(RunSink.prototype, "drain").mockImplementation(function (
                this: RunSink,
                ...args
              ) {
                expect(
                  this.builder.queuedBytes,
                  "overflow payload queue is bounded before append",
                ).toBeLessThanOrEqual(512 * 1024);
                calls++;
                return drain.apply(this, args);
              });
              try {
                let t = 0;
                await autocommit(h.path, async (s) => {
                  t = need(
                    (
                      await s.changeCatalog({
                        kind: "createTable",
                        table: tableInput("wide", [
                          column("k", TEXT, false),
                          column("n", INT, false),
                        ]),
                      })
                    ).id,
                  );
                });
                if (units === 16384 && cap === 32 && outcome !== "auto") {
                  await h.path.quiesce();
                  const bytes =
                    h.governor.capBytes -
                    h.governor.reserveBytes -
                    h.governor.usedBytes -
                    6 * 1024 * 1024;
                  expect(bytes).toBeGreaterThan(0);
                  pressure = await h.governor.admit({
                    label: "another execution",
                    minimumBytes: bytes,
                    spillable: false,
                  });
                  expect(pressure.allocate(bytes)).toBe(true);
                  foreignBuffer = new Uint8Array(bytes).fill(0x5a);
                  reach.reach("pre-full pending pressure");
                }
                const tx = outcome === "auto" ? null : await h.path.begin();
                let save: ReturnType<NonNullable<typeof tx>["savepoint"]> | undefined;
                try {
                  for (let from = 0; from < 160;) {
                    const boundary = outcome === "savepoint" ? 80 : 160;
                    const take = Math.min(batchRows, boundary - (from % boundary));
                    const input = batch(
                      texts(Array.from({ length: take }, (_, i) => value(units, from + i, glyph))),
                      ints(Array.from({ length: take }, (_, i) => from + i)),
                    );
                    if (tx === null)
                      await autocommit(h.path, (s) => s.table(t).insert(input, null));
                    else {
                      const statement = tx.statement({ rules });
                      try {
                        await statement.table(t).insert(input, null);
                        await statement.end();
                      } catch (error) {
                        statement.rollback();
                        throw error;
                      }
                      if (from + take === 80) save = tx.savepoint();
                    }
                    from += take;
                  }
                  if (tx !== null) {
                    if (units === 16384 && cap === 32) {
                      expect(h.path.maintenance.stages).toBeGreaterThan(0);
                      reach.reach("pending byte freeze");
                    }
                    if (outcome === "rollback") {
                      tx.rollback();
                      reach.reach("byte freeze rollback");
                    } else {
                      if (outcome === "savepoint") {
                        tx.rollbackTo(need(save));
                        reach.reach("byte freeze savepoint");
                      }
                      const receipt = await tx.commit(null);
                      await receipt.durable;
                    }
                  }
                } catch (error) {
                  tx?.rollback();
                  throw error;
                }
                await autocommit(h.path, (s) =>
                  s.table(t).insert(batch(texts(["sentinel"]), ints([160])), null),
                );
                await h.path.quiesce();
                // quiesce waits runnable flushes; it does not freeze a commit's live tail.
                // Escaped-NUL byte boundaries can therefore remain WAL-only. The original BMP
                // cases still require a drain; every actual drain is checked for every glyph.
                if (glyph === "\u0800" && (outcome === "auto" || outcome === "commit"))
                  expect(calls).toBeGreaterThan(0);
                await h.reopen();
                const view = h.path.pin();
                try {
                  for (const n of [0, 79, 159]) {
                    const out: number[] = [];
                    await view
                      .index(t, need(view.catalog.table(t)?.primaryKey))
                      .resolve(key(value(units, n, glyph)), out);
                    expect(out).toHaveLength(
                      outcome === "rollback" || (outcome === "savepoint" && n >= 80) ? 0 : 1,
                    );
                  }
                } finally {
                  view.release();
                }
                if (calls > 0) reach.reach("bounded overflow drain");
                reach.reach(cap === 32 ? "thirty-two MiB flush" : "sixty-four MiB flush");
                if (cap === 32 && batchRows === 80) reach.reach("wide ingress at thirty-two MiB");
                if (cap === 32 && batchRows === 160 && outcome !== "savepoint")
                  reach.reach("whole wide ingress below cap");
                if (glyph === "\0") reach.reach("escaped key expansion at admission");
              } finally {
                observed.mockRestore();
                if (foreignBuffer !== undefined) expect(foreignBuffer[0]).toBe(0x5a);
                pressure?.close();
                await h.close();
              }
              expect(h.governor.usedBytes).toBe(0);
            }),
        ),
        {
          seed,
          numRuns: flushHistories,
          examples: [
            [16384, 32, "commit", 160, "\0"],
            [16384, 32, "rollback", 160, "\0"],
            [16384, 32, "savepoint", 80, "\0"],
            [16384, 32, "auto", 160, "\u0800"],
            [16384, 32, "commit", 160, "\u0800"],
            [16384, 32, "rollback", 160, "\u0800"],
            [16384, 32, "commit", 8, "\u0800"],
            [16384, 32, "commit", 80, "\u0800"],
            [16384, 32, "rollback", 8, "\u0800"],
            [16384, 32, "rollback", 80, "\u0800"],
            [16384, 32, "savepoint", 8, "\u0800"],
            [16384, 32, "savepoint", 80, "\u0800"],
          ],
        },
      );
    }, 180_000);
