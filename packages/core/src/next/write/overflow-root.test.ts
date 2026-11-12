/** Opt-in encoder2 through ordinary write/snapshot APIs; production selection is stage5. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, OVERFLOW_ROOT_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { need, UNCHARGED } from "./bytes.js";
import * as runs from "./runs.js";
import type { SnapshotImpl } from "./view.js";
import { readRows } from "./rows.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  TEXT,
  INT,
  ints,
  texts,
  tableInput,
  rowsOf,
} from "./test-support.js";
const reach = createReach("ordinary overflow root", OVERFLOW_ROOT_STATES);
afterAll(() => reach.assertReached());
function value(units: number, id: number): string {
  return "\u0800".repeat(units - 1) + String.fromCharCode(0x0800 + id);
}
async function check(view: SnapshotImpl, t: number, units: number, count: number): Promise<void> {
  const pk = need(view.catalog.table(t)?.primaryKey),
    index = view.index(t, pk),
    table = view.internalTable(t);
  expect(
    (await rowsOf(view, t)).map((row) => row[1]).sort((a, b) => Number(a) - Number(b)),
  ).toEqual(Array.from({ length: count }, (_, i) => i));
  const keys = new KeyBatch(UNCHARGED);
  try {
    for (let i = 0; i < count; i++) {
      const vector = texts([value(units, i)]);
      expect(
        encodeKeyBatch(
          "equality",
          [{ vector, direction: "asc", nulls: "first" }],
          { length: 1, selection: null },
          keys,
        ).done,
      ).toBe(true);
      const found: number[] = [];
      await index.resolve(keys.key(0), found);
      expect(found).toHaveLength(1);
      const vectors = await readRows(table, found),
        n = need(vectors[1]);
      if (n.data.layout !== "i32") throw new Error("integer witness expected");
      expect(n.data.values[0]).toBe(i);
    }
  } finally {
    keys.close();
  }
  reach.reach("ordinary point reads");
  for (const direction of ["asc", "desc"] as const) {
    const plan = index.planRange({ lower: null, upper: null }, direction);
    const grant = await view.shared.governor.admit({
      label: "exact planned overflow range",
      minimumBytes: plan.minimumBytes,
      spillable: false,
    });
    const held = grant.held,
      cursor = plan.open({ budget: grant, signal: new AbortController().signal }),
      out = new Float64Array(3),
      locators: number[] = [];
    try {
      for (;;) {
        const n = await cursor.next(out);
        if (n === 0) break;
        for (let i = 0; i < n; i++) locators.push(need(out[i]));
      }
      const rows = await readRows(
          table,
          locators.map((locator) => table.rowId(locator)),
        ),
        v = need(rows[1]);
      if (v.data.layout !== "i32") throw new Error("integer witness expected");
      const wanted = Array.from({ length: count }, (_, i) => i);
      if (direction === "desc") wanted.reverse();
      expect(Array.from(v.data.values.subarray(0, count))).toEqual(wanted);
      expect(grant.held).toBe(held);
      expect(grant.loaned).toBe(0);
      reach.reach(direction === "asc" ? "admitted ascending" : "admitted descending");
    } finally {
      cursor.close();
      expect(grant.allocated).toBe(0);
      grant.close();
    }
  }
}
for (const seed of seedsFor("next-overflow-root", [0xb50c]))
  it(`publishes, reads, continues and reopens long keys (seed ${String(seed)})`, async () => {
    const Real = runs.RunBuilder;
    const factory = vi.spyOn(runs, "RunBuilder").mockImplementation(function (
      ...args: ConstructorParameters<typeof Real>
    ) {
      return new Real(args[0], args[1], args[2], args[3] ?? 2);
    });
    try {
      await fc.assert(
        fc.asyncProperty(fc.constantFrom(769, 4096, 16384), async (units) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 4, undefined, {
            merges: { fanIn: 2, splitEntries: 3, sliceEntries: 8 },
          });
          let path = await h.open(),
            old: SnapshotImpl | undefined;
          try {
            let t = 0;
            await autocommit(path, async (s) => {
              t = need(
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("long", [column("k", TEXT, false), column("n", INT, false)]),
                  })
                ).id,
              );
            });
            const insert = async (from: number, to: number) =>
              autocommit(path, (s) =>
                s
                  .table(t)
                  .insert(
                    batch(
                      texts(Array.from({ length: to - from }, (_, i) => value(units, from + i))),
                      ints(Array.from({ length: to - from }, (_, i) => from + i)),
                    ),
                    null,
                  ),
              );
            await insert(0, 8);
            old = path.pin();
            const oldIndex = old.index(t, need(old.catalog.table(t)?.primaryKey));
            const Native = Uint8Array;
            let plan: ReturnType<typeof oldIndex.planRange>;
            try {
              globalThis.Uint8Array = new Proxy(Native, {
                construct(target, args) {
                  const size: unknown = args[0];
                  if (typeof size === "number" && size > 0)
                    throw new Error("planning allocated native scratch");
                  return Reflect.construct(target, args) as Uint8Array;
                },
              });
              plan = oldIndex.planRange({ lower: null, upper: null }, "desc");
            } finally {
              globalThis.Uint8Array = Native;
            }
            reach.reach("allocation-free planning");
            await insert(8, 9);
            await path.quiesce();
            await path.runMaintenance();
            await path.quiesce();
            const stored = path
              .state()
              .root.tables.flatMap((table) => [
                ...(table.keyIndex?.runs ?? []),
                ...table.secondaryIndexes.flatMap((i) => i.runs),
              ]);
            expect(stored.some((r) => r.encoding === 2)).toBe(true);
            for (const ref of stored) {
              expect(ref.minKey.buffer.byteLength).toBeLessThanOrEqual(2048);
              expect(ref.maxKey.buffer.byteLength).toBeLessThanOrEqual(2048);
            }
            reach.reach("published overflow root");
            const grant = await h.governor.admit({
              label: "planned before publication",
              minimumBytes: plan.minimumBytes,
              spillable: false,
            });
            const held = grant.held,
              cursor = plan.open({ budget: grant, signal: new AbortController().signal }),
              out = new Float64Array(16);
            try {
              expect(await cursor.next(out)).toBe(8);
              expect(await cursor.next(out)).toBe(0);
              expect(grant.held).toBe(held);
            } finally {
              cursor.close();
              expect(grant.allocated).toBe(0);
              grant.close();
            }
            await check(old, t, units, 8);
            reach.reach("old snapshot after publication");
            old.release();
            old = undefined;
            let current = path.pin();
            try {
              await check(current, t, units, 9);
            } finally {
              current.release();
            }
            await h.space.audit();
            path = await h.reopen();
            current = path.pin();
            try {
              await check(current, t, units, 9);
            } finally {
              current.release();
            }
            reach.reach("durable reopen");
            await insert(9, 14);
            await path.quiesce();
            await path.runMaintenance();
            await path.quiesce();
            path = await h.reopen();
            current = path.pin();
            try {
              await check(current, t, units, 14);
            } finally {
              current.release();
            }
            await h.space.audit();
            reach.reach("continued writes");
            reach.reach(units === 16384 ? "shipped UTF16 maximum" : "other key lengths");
          } finally {
            old?.release();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        }),
        { seed, numRuns: 6 },
      );
    } finally {
      factory.mockRestore();
    }
  }, 120_000);
