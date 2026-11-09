/** Statement rollback removes every index effect, including a prefix added before allocation failed. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_PARTIAL_DELTA_STATES } from "../../testing/reach.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import type { Statement } from "../access/modify.js";
import { MemtableMemoryError, need } from "./bytes.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  locators,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";
const reach = createReach("partial index addition rollback", WRITE_PARTIAL_DELTA_STATES);
const FREE = { allocate: () => true, free: () => undefined };
for (const seed of seedsFor("next-write-partial-delta-rollback", [0xb3d0]))
  it(`undoes failed key prefixes and accepts their retry (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          count: fc.integer({ min: 2, max: 12 }),
          after: fc.integer({ min: 0, max: 20 }),
          index: fc.constantFrom(0, 1, 2),
          native: fc.boolean(),
          remove: fc.boolean(),
          backend: fc.constantFrom("memory", "indexeddb"),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.backend);
          reach.reach(s.remove ? "delete prefix" : "insert prefix");
          reach.reach(s.native ? "native failure" : "budget denial");
          reach.reach(s.index === 0 ? "primary" : s.index === 1 ? "unique" : "nonunique");
          const h = new Harness("strict", "memory");
          if (s.backend === "indexeddb")
            Object.assign(h, {
              backend: createIndexedDbBackend({
                database: () => simIndexedDb(h.disk),
                name: "partial-delta",
                locks: new SimLocks(),
                memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
              }),
            });
          const p = await h.open();
          let table = 0;
          let cleanup: (() => void) | undefined;
          const ids = Array.from({ length: s.count }, (_, i) => i + 1),
            values = ids.map((n) => n * 17 + 3);
          try {
            await autocommit(p, async (st) => {
              table =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false), column("v", INT, false)]),
                  })
                ).id ?? 0;
            });
            for (const unique of [true, false])
              await autocommit(p, (st) =>
                st.changeCatalog({
                  kind: "createIndex",
                  index: {
                    name: unique ? "unique_v" : "by_v",
                    tableId: table,
                    columnIds: [p.catalog.table(table)?.columns[1]?.id ?? 0],
                    unique,
                    role: "secondary",
                  },
                }),
              );
            await autocommit(p, (st) =>
              st
                .table(table)
                .insert(
                  batch(ints(s.remove ? [0, ...ids] : [0]), ints(s.remove ? [0, ...values] : [0])),
                  null,
                ),
            );
            const pin = p.pin(),
              memtable = need(pin.internalTable(table).tableView.memtables[0]);
            const defs = need(p.catalog.table(table)).indexes,
              selected = need(defs[s.index]);
            const delta = need(memtable.deltas.get(selected.id)),
              before = new Map([...memtable.deltas].map(([id, d]) => [id, d.entryCount]));
            const targets = s.remove
              ? ids.map((_, i) => pin.internalTable(table).locator(i + 2))
              : [];
            pin.release();
            cleanup = () => {
              for (const [id, d] of memtable.deltas)
                while (d.entryCount > (before.get(id) ?? 0)) d.pop(d.entryCount - 1);
            };
            const original = delta.add.bind(delta),
              at = 2 + (s.after % (s.count - 1)),
              error = s.native
                ? new Error("native allocation failed")
                : new MemtableMemoryError(256);
            let calls = 0;
            const spy = vi.spyOn(delta, "add").mockImplementation((...args) => {
              if (++calls === at) throw error;
              return original(...args);
            });
            const attempt = (st: Statement) =>
              s.remove
                ? st.table(table).delete(batch(locators(targets, table)), null)
                : st.table(table).insert(batch(ints(ids), ints(values)), null);
            try {
              await expect(autocommit(p, attempt)).rejects.toBe(error);
            } finally {
              spy.mockRestore();
            }
            reach.reach("partial key prefix");
            // Pending markers are reused by later transactions. Its second statement must still
            // see only the committed rows, through every public index reader.
            const checkTx = await p.begin();
            await checkTx.statement({ rules }).end();
            const check = checkTx.statement({ rules });
            try {
              const initial = s.remove
                ? [[0, 0], ...ids.map((id, i) => [id, values[i]])]
                : [[0, 0]];
              expect(await rowsOf(check.view, table)).toEqual(initial);
              for (const def of defs) {
                const keys = new KeyBatch(FREE),
                  input = def.role === "primary" ? [0, ...ids] : [0, ...values];
                encodeKeyBatch(
                  "equality",
                  [{ vector: ints(input), direction: "asc", nulls: "first" }],
                  { length: input.length, selection: null },
                  keys,
                );
                const out = {
                  probes: new Int32Array(input.length + 1),
                  locators: new Float64Array(input.length + 1),
                  count: 0,
                };
                try {
                  expect((await check.view.index(table, def.id).probe(keys, out)).done).toBe(true);
                  expect(
                    out.count,
                    "aborted key effects must not become visible in a later transaction",
                  ).toBe(initial.length);
                } finally {
                  keys.close();
                }
              }
            } finally {
              check.rollback();
              checkTx.rollback();
            }
            reach.reach("all deltas rolled back");
            cleanup = undefined;
            await autocommit(p, attempt);
            reach.reach("retry accepted");
            const expected = s.remove ? [[0, 0]] : [[0, 0], ...ids.map((id, i) => [id, values[i]])];
            const view = p.pin();
            try {
              expect(await rowsOf(view, table)).toEqual(expected);
              for (const def of defs) {
                const keys = new KeyBatch(FREE),
                  input = s.remove ? [0] : def.role === "primary" ? [0, ...ids] : [0, ...values];
                encodeKeyBatch(
                  "equality",
                  [{ vector: ints(input), direction: "asc", nulls: "first" }],
                  { length: input.length, selection: null },
                  keys,
                );
                const out = {
                  probes: new Int32Array(input.length + 1),
                  locators: new Float64Array(input.length + 1),
                  count: 0,
                };
                try {
                  expect((await view.index(table, def.id).probe(keys, out)).done).toBe(true);
                  expect(out.count).toBe(input.length);
                } finally {
                  keys.close();
                }
              }
            } finally {
              view.release();
            }
            await h.reopen();
            const reopened = h.path.pin();
            try {
              expect(await rowsOf(reopened, table)).toEqual(expected);
            } finally {
              reopened.release();
            }
            reach.reach("durable reopen");
          } finally {
            vi.restoreAllMocks();
            // RED-only cleanup after the assertion: do not let leaked pending markers obstruct close.
            cleanup?.();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);
it("reaches partial key rollback states", () => reach.assertReached());
