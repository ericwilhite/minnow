/** Batch/Vector ownership, validity and selection laws (vector/vector.ts); insert atomicity
 * (access/modify.ts). A physical range and an explicit selection denote the same rows. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_ROW_PLAN_REACH } from "../../testing/reach.js";
import { NotNullViolationError } from "../access/modify.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import type { Batch, Vector } from "../vector/vector.js";
import { need } from "./bytes.js";
import { Memtable } from "./memtable.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

const reach = createReach("write row plans", WRITE_ROW_PLAN_REACH);
const free = { allocate: () => true, free: () => undefined };
for (const seed of seedsFor("next-write-row-plan", [0xb312, 0x91])) {
  it(`preserves selected values, keys, ownership and reopen (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          count: fc.oneof(
            fc.constantFrom(0, 1, 31, 2048, 2049, 4097),
            fc.integer({ min: 2, max: 200 }),
          ),
          capacity: fc.constantFrom(31, 127, 2048),
          shape: fc.constantFrom("range", "selected", "nullable", "dictionary", "constant"),
          salt: fc.integer({ min: 0, max: 10000 }),
        }),
        async ({ count, capacity, shape, salt }) => {
          reach.beginHistory();
          reach.reach(shape);
          if (count === 0) reach.reach("empty");
          if (count > 2048) reach.reach("admission boundary");
          const h = new Harness("strict", "sim", capacity);
          let p = await h.open();
          try {
            let table = 0;
            await autocommit(p, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [
                      column("id", INT, false),
                      column("v", TEXT, shape === "nullable"),
                    ]),
                  })
                ).id ?? 0;
            });
            const def = need(p.catalog.table(table));
            await autocommit(p, (s) =>
              s.changeCatalog({
                kind: "createIndex",
                index: {
                  name: "by_v",
                  tableId: table,
                  columnIds: [need(def.columns[1]).id],
                  unique: false,
                  role: "secondary",
                },
              }),
            );
            // A preceding row makes each later physical/admission range begin in a partial memtable.
            await autocommit(p, (s) =>
              s.table(table).insert(batch(ints([-1]), texts(["prefix"])), null),
            );
            const ids = Array.from({ length: count }, (_, i) => i + salt * 10000);
            const dictionary = ["", "é中😀", "p".repeat(137)];
            const values = ids.map((_, i) =>
              shape === "constant"
                ? need(dictionary[1])
                : shape === "nullable" && i % 7 === 0
                  ? null
                  : need(dictionary[i % 3]) + String(i),
            );
            const v = texts(values);
            let vector: Vector = v;
            if (shape === "dictionary")
              vector = {
                form: "dictionary",
                type: TEXT,
                length: count,
                validity: null,
                codes: Int32Array.from(ids, (_, i) => i % 3),
                dictionary: texts(dictionary),
                dictionaryId: 1,
              };
            if (shape === "constant")
              vector = {
                form: "constant",
                type: TEXT,
                length: count,
                value: texts([need(dictionary[1])]),
              };
            const selected =
              shape === "selected"
                ? ids.map((_, i) => i).filter((i) => i % 3 !== 1)
                : ids.map((_, i) => i);
            const input: Batch = {
              length: count,
              columns: [ints(ids), vector],
              selection:
                shape === "selected"
                  ? { rows: Uint16Array.from([...selected, 65535]), count: selected.length }
                  : null,
            };
            const written = { rows: new Uint16Array(count), count: 0 };
            await autocommit(p, async (s) =>
              expect(await s.table(table).insert(input, written)).toBe(selected.length),
            );
            expect(Array.from(written.rows.subarray(0, written.count))).toEqual(selected);
            reach.reach("written selection");
            const expected = [
              [-1, "prefix"],
              ...selected.map((i) => [
                ids[i],
                shape === "dictionary" ? dictionary[i % 3] : values[i],
              ]),
            ];
            // The producer may reuse mutable flat buffers once insert returns.
            const first = need(input.columns[0]);
            if (first.form === "flat" && first.data.layout === "i32") first.data.values.fill(-99);
            if (v.data.layout === "varlen") v.data.bytes.fill(255);
            reach.reach("producer reused");
            const verify = async () => {
              const view = p.pin();
              const keys = new KeyBatch(free);
              try {
                const actual = await rowsOf(view, table);
                expect(actual).toEqual(expected);
                encodeKeyBatch(
                  "equality",
                  [
                    {
                      vector: ints([-1, ...selected.map((i) => need(ids[i]))]),
                      direction: "asc",
                      nulls: "first",
                    },
                  ],
                  { length: selected.length + 1, selection: null },
                  keys,
                );
                const out = {
                  probes: new Int32Array(selected.length + 1),
                  locators: new Float64Array(selected.length + 1),
                  count: 0,
                };
                expect((await view.index(table, need(def.primaryKey)).probe(keys, out)).done).toBe(
                  true,
                );
                expect(out.count).toBe(selected.length + 1);
                reach.reach("primary keys");
                const probeValues = [
                  "prefix",
                  ...selected.map((i) =>
                    shape === "dictionary" ? (dictionary[i % 3] ?? "") : (values[i] ?? null),
                  ),
                ];
                const distinct = [...new Set(probeValues)];
                encodeKeyBatch(
                  "equality",
                  [{ vector: texts(distinct), direction: "asc", nulls: "first" }],
                  { length: distinct.length, selection: null },
                  keys,
                );
                out.count = 0;
                const secondary = need(
                  p.catalog.table(table)?.indexes.find((index) => index.name === "by_v"),
                );
                expect((await view.index(table, secondary.id).probe(keys, out)).done).toBe(true);
                expect(out.count).toBe(probeValues.filter((value) => value !== null).length);
                reach.reach("secondary null omission");
              } finally {
                keys.close();
                view.release();
              }
            };
            await verify();
            await p.quiesce();
            p = await h.reopen();
            await verify();
            reach.reach("reopen");
          } finally {
            await h.close();
          }
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120000);
  it(`retains nullable NOT NULL rejection and ignores inactive nulls (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 70 }), fc.nat(9999), async (count, salt) => {
        const h = new Harness();
        const p = await h.open();
        try {
          let table = 0;
          await autocommit(p, async (s) => {
            table =
              (
                await s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("t", [column("id", INT, false), column("v", TEXT, false)]),
                })
              ).id ?? 0;
          });
          const bad = salt % count;
          const input = batch(
            ints(Array.from({ length: count }, (_, i) => i)),
            texts(Array.from({ length: count }, (_, i) => (i === bad ? null : "ok"))),
          );
          await expect(
            autocommit(p, (s) => s.table(table).insert(input, null)),
          ).rejects.toBeInstanceOf(NotNullViolationError);
          const rows = Uint16Array.from(
            Array.from({ length: count }, (_, i) => i).filter((i) => i !== bad),
          );
          await autocommit(p, (s) =>
            s.table(table).insert({ ...input, selection: { rows, count: rows.length } }, null),
          );
          const view = p.pin();
          try {
            expect((await rowsOf(view, table)).length).toBe(count - 1);
          } finally {
            view.release();
          }
          reach.beginHistory();
          reach.reach("inactive null");
          reach.reach("NOT NULL refusal");
        } finally {
          await h.close();
        }
      }),
      { seed, numRuns: 24 },
    );
  });
}
it("reaches row-plan input and ownership states", () => reach.assertReached());

// This work law uses only parent APIs; its RED is an actual per-row selection handoff.
for (const seed of seedsFor("next-write-row-plan-work", [0xb312, 0x91]))
  it(`hands contiguous ranges to the existing memtable copier (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2049, max: 4200 }), async (count) => {
        const h = new Harness("strict", "memory", 127);
        const p = await h.open();
        const spy = vi.spyOn(Memtable.prototype, "append");
        try {
          let table = 0;
          await autocommit(p, async (s) => {
            table =
              (
                await s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("t", [column("id", INT, false)]),
                })
              ).id ?? 0;
          });
          await autocommit(p, (s) =>
            s.table(table).insert(batch(ints(Array.from({ length: count }, (_, i) => i))), null),
          );
          const calls = spy.mock.calls;
          expect(calls.length).toBeGreaterThan(1);
          let next = 0;
          for (const args of calls) {
            expect(args[1], "contiguous input must not materialize a row selection").toBeNull();
            expect(args[2]).toBe(next);
            next += args[3];
          }
          expect(next).toBe(count);
        } finally {
          spy.mockRestore();
          await h.close();
        }
      }),
      { seed, numRuns: 8 },
    );
  }, 60000);
