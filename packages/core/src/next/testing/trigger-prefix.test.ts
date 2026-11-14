/** Native trigger substatements read their root's write prefix and own their locator pin. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_PREFIX_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import type { Snapshot } from "../access/snapshot.js";
import type { Statement } from "../access/modify.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { batch, ints, locators, rowsOf } from "../write/test-support.js";
import { oracleRows } from "./differential-adapter.js";
const own = (s: Statement) => (typeof s.readOwn === "function" ? s.readOwn() : s.view);
const release = (view: Snapshot, s: Statement) => {
  if (view !== s.view) view.release();
};
for (const seed of seedsFor("next-trigger-prefix", [0xb7c2, 0xb7c3]))
  it(`uses current-prefix locators without sharing the check pin seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("trigger prefix", TRIGGER_PREFIX_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 4 }),
          fc.integer({ min: -100, max: 100 }),
          fc.boolean(),
          fc.boolean(),
          async (bodies, base, rollback, published) => {
            reach.beginHistory();
            const core = await new StorageCore(published ? 1 : undefined).open();
            const pins: Snapshot[] = [];
            const prefix = (s: Statement) => {
              const p = own(s);
              if (p !== s.view) pins.push(p);
              return p;
            };
            let tx: import("../access/write-path.js").WriteTransaction | undefined;
            let grant: import("../memory/governor.js").Grant | undefined;
            try {
              await pg.exec("DROP TABLE IF EXISTS t;CREATE TABLE t(id INT PRIMARY KEY,v INT)");
              await core.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)");
              await core.write((s) =>
                s
                  .table(s.catalog.tableByName("t")?.id ?? 0)
                  .insert(batch(ints([1]), ints([base])), null),
              );
              await pg.exec(`INSERT INTO t VALUES(1,${String(base)})`);
              if (published) {
                await core.harness.path.recycleLog();
                await core.reopen();
                reach.reach("published");
              } else reach.reach("resident");
              const table = core.harness.path.catalog.tableByName("t");
              if (table?.primaryKey == null) throw new Error("missing target");
              grant = await core.harness.governor.admit({
                label: "trigger-prefix",
                minimumBytes: 65536,
                spillable: true,
              });
              const charge = grant;
              const keys = new KeyBatch(grant);
              try {
                encodeKeyBatch(
                  "equality",
                  [{ vector: ints([1]), direction: "asc", nulls: "first" }],
                  { length: 1, selection: null },
                  keys,
                );
                const locate = async (view: Snapshot) => {
                  const out = {
                    probes: new Int32Array(1),
                    locators: new Float64Array(1),
                    count: 0,
                  };
                  await view.index(table.id, table.primaryKey ?? 0).probe(keys, out);
                  expect(out.count).toBe(1);
                  return out.locators[0] ?? 0;
                };
                tx = await core.harness.path.begin();
                const root = tx.statement({
                  rules: () => ({ check: () => null, generate: () => undefined }),
                });
                const initial = await locate(root.view),
                  v = table.columns[1];
                if (!v) throw new Error("missing v");
                await root
                  .table(table.id, { assigned: [v.id] })
                  .update(batch(locators([initial], table.id), ints([base + 1])), null);
                await pg.exec(`BEGIN;UPDATE t SET v=${String(base + 1)}`);
                expect(await rowsOf(root.view, table.id)).toEqual([[1, base]]);
                reach.reach("start view unchanged");
                for (let i = 0; i < bodies; i++) {
                  const pin = prefix(root);
                  try {
                    const twin = prefix(root);
                    try {
                      expect(twin).not.toBe(pin);
                    } finally {
                      release(twin, root);
                    }
                    reach.reach("independent pins");
                    expect(await rowsOf(pin, table.id)).toEqual(
                      (await pg.query<{ id: number; v: number }>("SELECT * FROM t")).rows.map(
                        (r) => [r.id, r.v],
                      ),
                    );
                    // Both writers have identical assignments: their input pins must not alias
                    // the cached start-view writer, nor the previous body's writer.
                    const at = await locate(pin);
                    await expect(
                      Promise.resolve().then(() =>
                        root
                          .table(table.id, { assigned: [v.id], inputView: pin })
                          .update(batch(locators([at], table.id), ints([base + i + 2])), null),
                      ),
                    ).resolves.toBe(1);
                    await pg.exec(`UPDATE t SET v=${String(base + i + 2)}`);
                    reach.reach("prefix rewrite");
                  } finally {
                    release(pin, root);
                  }
                }
                const last = prefix(root);
                expect(await rowsOf(last, table.id)).toEqual([[1, base + bodies + 1]]);
                const foreign = core.harness.path.pin();
                try {
                  expect(() => root.table(table.id, { inputView: foreign })).toThrow(
                    "inputView must come from this statement",
                  );
                  expect(() => root.table(table.id, { inputView: root.view })).toThrow(
                    "inputView must come from this statement",
                  );
                  reach.reach("foreign pin refused");
                } finally {
                  foreign.release();
                }
                const expired = prefix(root),
                  expiredAt = await locate(expired),
                  expiredWriter = root.table(table.id, { assigned: [v.id], inputView: expired });
                release(expired, root);
                expect(() => root.table(table.id, { inputView: expired })).toThrow(
                  expect.objectContaining({ code: "MINNOW_SNAPSHOT_RELEASED" }),
                );
                reach.reach("released pin refused");
                if (rollback) {
                  expect(() =>
                    expiredWriter.update(batch(locators([expiredAt], table.id), ints([999])), null),
                  ).toThrow(expect.objectContaining({ code: "MINNOW_SNAPSHOT_RELEASED" }));
                  reach.reach("expired writer refused");
                  root.rollback();
                  tx.rollback();
                  await pg.exec("ROLLBACK");
                  reach.reach("rollback");
                } else {
                  await root.end();
                  await (
                    await tx.commit(null)
                  ).durable;
                  await pg.exec("COMMIT");
                  reach.reach("commit");
                }
                tx = undefined;
                expect(() => last.table(table.id, { budget: charge })).toThrow(
                  expect.objectContaining({ code: "MINNOW_SNAPSHOT_RELEASED" }),
                );
                last.release();
                reach.reach("end releases pin");
              } finally {
                keys.close();
              }
              expect(grant.allocated).toBe(0);
              grant.close();
              grant = undefined;
              const result = await core.execute("SELECT * FROM t");
              if (!("batches" in result)) throw new Error("missing result");
              expect(oracleRows(result)).toEqual((await pg.query("SELECT * FROM t")).rows);
              reach.reach(bodies > 1 ? "multiple bodies" : "one body");
            } finally {
              for (const p of pins) p.release();
              tx?.rollback();
              grant?.close();
              await core.close();
              await pg.exec("ROLLBACK;DROP TABLE IF EXISTS t");
            }
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [1, 1, false, false],
            [3, 2, true, true],
            [4, 3, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
