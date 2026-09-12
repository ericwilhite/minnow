import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import type { BatchValue } from "./batch.js";
import { MinnowDatabase } from "./database.js";
import { column, schema, table } from "./schema.js";

/**
 * A table whose history holds upsert segments used to fall off every fast read path until
 * compaction folded them. Now an upsert of an existing key retires the earlier row and appends
 * the new one on the streamed scan, the materialized replay, and the keyed point read alike.
 * This drives all three against one in-memory model through mixed histories.
 */

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    create: async (): Promise<BlockStore> =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

interface Row extends Record<string, BatchValue> {
  id: number;
  group: string;
  amount: number;
  note: string | null;
}

function random(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

function sortRows(rows: readonly Row[]): Row[] {
  return [...rows].sort((left, right) => left.id - right.id);
}

describe.each(implementations)("upsert histories on $name", ({ create }) => {
  it.each([3, 11, 29])(
    "reads exactly the model through scans, point reads, and IN lists (seed %i)",
    async (seed) => {
      const store = await create();
      const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      await database.migrate(
        schema([
          table("items", {
            id: column.number().unique(),
            group: column.string(),
            amount: column.number(),
            note: column.string().nullable(),
          }),
        ]),
      );
      const next = random(seed);
      const model = new Map<number, Row>();
      const row = (id: number): Row => ({
        id,
        group: `g${String(Math.floor(next() * 4))}`,
        amount: Math.floor(next() * 100),
        note: next() < 0.2 ? null : `n${String(Math.floor(next() * 1000))}`,
      });
      const seeded = Array.from({ length: 60 }, (_, index) => row(index + 1));
      for (const item of seeded) model.set(item.id, item);
      await database.insertBatch("items", seeded);
      let nextId = 61;
      for (let step = 0; step < 24; step += 1) {
        const roll = next();
        if (roll < 0.45) {
          // An upsert mixing existing keys and new ones, some existing keys twice over steps.
          const batch: Row[] = [];
          const count = 1 + Math.floor(next() * 12);
          const used = new Set<number>();
          for (let index = 0; index < count; index += 1) {
            const id = next() < 0.7 ? 1 + Math.floor(next() * 70) : nextId++;
            if (used.has(id)) continue;
            used.add(id);
            batch.push(row(id));
          }
          for (const item of batch) model.set(item.id, item);
          await database.upsertBatch("items", batch);
        } else if (roll < 0.7) {
          const keys = [...model.keys()].filter(() => next() < 0.15);
          if (keys.length === 0) continue;
          const amounts = keys.map(() => Math.floor(next() * 100));
          keys.forEach((id, index) => {
            const current = model.get(id);
            if (current !== undefined) model.set(id, { ...current, amount: amounts[index] ?? 0 });
          });
          await database.updateBatch("items", { keys, changes: { amount: amounts } });
        } else if (roll < 0.85) {
          const keys = [...model.keys()].filter(() => next() < 0.1);
          if (keys.length === 0) continue;
          for (const id of keys) model.delete(id);
          await database.deleteBatch("items", { keys });
        } else {
          const fresh = Array.from({ length: 5 }, () => row(nextId++));
          for (const item of fresh) model.set(item.id, item);
          await database.insertBatch("items", fresh);
        }
        const expected = sortRows([...model.values()]);
        const scanned = (
          await database.query('SELECT "id", "group", "amount", "note" FROM "items"', {
            memoize: false,
          })
        ).rows as Row[];
        expect(sortRows(scanned), `scan after step ${String(step)}`).toEqual(expected);
        const filtered = (
          await database.query(
            'SELECT "id", "amount" FROM "items" WHERE "amount" >= 50 ORDER BY "id" LIMIT 7',
            { memoize: false },
          )
        ).rows;
        expect(filtered, `window after step ${String(step)}`).toEqual(
          expected
            .filter((item) => item.amount >= 50)
            .slice(0, 7)
            .map(({ id, amount }) => ({ id, amount })),
        );
        const probeIds = [...model.keys()].slice(0, 3).concat([999_999]);
        for (const id of probeIds) {
          const point = (
            await database.query(
              'SELECT "id", "group", "amount", "note" FROM "items" WHERE "id" = $1',
              { params: [id], memoize: false },
            )
          ).rows as Row[];
          expect(point, `point read ${String(id)} after step ${String(step)}`).toEqual(
            model.has(id) ? [model.get(id)] : [],
          );
        }
        const inList = (
          await database.query(
            `SELECT "id", "amount" FROM "items" WHERE "id" IN (${probeIds.map((_, index) => `$${String(index + 1)}`).join(", ")})`,
            { params: probeIds, memoize: false },
          )
        ).rows as Array<{ id: number; amount: number }>;
        expect(
          [...inList].sort((left, right) => left.id - right.id),
          `IN list after step ${String(step)}`,
        ).toEqual(
          probeIds
            .filter((id) => model.has(id))
            .sort((left, right) => left - right)
            .map((id) => ({ id, amount: model.get(id)?.amount ?? 0 })),
        );
        const count = (
          await database.query('SELECT count(*) AS n FROM "items"', { memoize: false })
        ).rows[0]?.n;
        expect(count, `count after step ${String(step)}`).toBe(expected.length);
      }
      await database.close();
      store.close();
    },
  );
});
