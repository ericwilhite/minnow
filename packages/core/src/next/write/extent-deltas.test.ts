/**
 * Ruling R25: every publication's extent delta (access/write-path.ts `PublishDelta`) is exact.
 * Replaying the deltas from an empty set gives, after each publication, exactly the extents a full
 * reachability walk of the published root finds - across flushes, folded deletions, merges,
 * index and table DDL, log recycles, idle publishes and reopens. Each delta adds only extents the
 * set lacks and releases only extents it holds.
 */
import { describe, expect, it } from "vitest";
import type { PublishDelta } from "../access/write-path.js";
import { reachableExtents } from "../format/reachable.js";
import type { ExtentSpace } from "../storage/extents.js";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  tableInput,
} from "./test-support.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { Statement } from "../access/modify.js";

const FREE = { allocate: () => true, free: () => undefined };

/** Every extent the space's current root reaches, by offset → length. */
async function walk(space: ExtentSpace): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  const root = need(space.root);
  for await (const reached of reachableExtents()(root, space, FREE)) {
    const extent = "extent" in reached ? reached.extent : reached;
    if (extent.length === 0) continue; // an empty directory names no extent
    out.set(extent.offset, extent.length);
  }
  return out;
}

const REACH = [
  "added",
  "released",
  "merge",
  "recycle",
  "idle-publish",
  "drop-table",
  "drop-index",
  "reopen",
] as const;
const reach = createReach("next-write-extent-deltas", REACH);

async function locate(statement: Statement, table: number, id: number): Promise<number> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints([id]), direction: "asc", nulls: "first" }],
    { length: 1, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
  await statement.view
    .index(table, statement.view.catalog.table(table)?.primaryKey ?? 0)
    .probe(keys, out);
  keys.close();
  return out.locators[0] ?? 0;
}

describe("extent deltas (ruling R25)", () => {
  const SEEDS = seedsFor("next-write-extent-deltas", [701, 702, 703, 704]);
  for (const seed of SEEDS)
    it(`replaying the deltas gives each root's reachable extents (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      const harness = new Harness("strict", "sim", 6, undefined, {
        merges: { fanIn: 2 },
        logRecycleBytes: 1500 + int(2000),
        idleGarbageBytes: 1,
      });
      reach.beginHistory();
      const set = new Map<number, number>();
      let sequence = 0;
      const deltas: PublishDelta[] = [];
      harness.extentDeltas = (delta) => {
        expect(delta.sequence, "deltas come in publish order").toBe(sequence + 1);
        sequence = delta.sequence;
        for (const extent of delta.released) {
          expect(set.has(extent.offset), `released ${String(extent.offset)} is reached`).toBe(true);
          set.delete(extent.offset);
        }
        for (const extent of delta.added) {
          expect(set.has(extent.offset), `added ${String(extent.offset)} is new`).toBe(false);
          set.set(extent.offset, extent.length);
        }
        deltas.push(delta);
        if (delta.added.length > 0) reach.reach("added");
        if (delta.released.length > 0) reach.reach("released");
        return null;
      };
      let path: WritePathImpl = await harness.open();
      const check = async (label: string) => {
        await path.quiesce();
        // Let an armed idle publish land, then compare with a full walk of the current root.
        await new Promise((resolve) => setTimeout(resolve, 150));
        await path.quiesce();
        expect(need(harness.space.root).length).toBeGreaterThan(0);
        const walked = await walk(harness.space);
        const extra = [...set].filter(([offset]) => !walked.has(offset));
        const missing = [...walked].filter(([offset]) => !set.has(offset));
        expect({ extra, missing }, `seed ${String(seed)} ${label}`).toEqual({
          extra: [],
          missing: [],
        });
        expect(set).toEqual(walked);
      };
      const tables: number[] = [];
      let named = 0;
      const rows = new Map<number, Set<number>>();
      const create = async () => {
        let t = 0;
        await autocommit(path, async (s) => {
          t =
            (
              await s.changeCatalog({
                kind: "createTable",
                table: tableInput(`t${String(++named)}`, [
                  column("id", INT, false),
                  column("v", INT),
                ]),
              })
            ).id ?? 0;
        });
        tables.push(t);
        rows.set(t, new Set());
      };
      await create();
      for (let step = 0; step < 60; step++) {
        const t = need(tables[int(tables.length)]);
        const live = need(rows.get(t));
        const roll = int(100);
        if (roll < 50) {
          const ids = Array.from({ length: 1 + int(8) }, () => int(500)).filter(
            (id) => !live.has(id),
          );
          const unique = [...new Set(ids)];
          if (unique.length > 0) {
            await autocommit(path, (s) =>
              s.table(t).insert(batch(ints(unique), ints(unique)), null),
            );
            for (const id of unique) live.add(id);
          }
        } else if (roll < 65 && live.size > 0) {
          const id = need([...live][int(live.size)]);
          await autocommit(path, async (s) =>
            s.table(t).delete(batch(locators([await locate(s, t, id)], t)), null),
          );
          live.delete(id);
        } else if (roll < 72) {
          const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
          const existing = path.catalog
            .table(t)
            ?.indexes.find((index) => index.name.endsWith("_v"));
          await autocommit(path, (s) =>
            existing === undefined
              ? s.changeCatalog({
                  kind: "createIndex",
                  index: {
                    tableId: t,
                    name: `t${String(t)}_v`,
                    columnIds: [v],
                    unique: false,
                    role: "secondary",
                  },
                })
              : s.changeCatalog({ kind: "dropIndex", indexId: existing.id }),
          );
          if (existing !== undefined) reach.reach("drop-index");
        } else if (roll < 78) {
          await create();
        } else if (roll < 82 && tables.length > 1) {
          tables.splice(tables.indexOf(t), 1);
          rows.delete(t);
          await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
          reach.reach("drop-table");
        } else if (roll < 90) {
          await check(`step ${String(step)}`);
        } else {
          await path.quiesce();
          path = await harness.reopen();
          reach.reach("reopen");
          // A reopen publishes nothing; the space's root is the last delta's.
          await check(`reopen at step ${String(step)}`);
        }
        if (path.maintenance.merges > 0) reach.reach("merge");
        if (path.maintenance.recycles > 0) reach.reach("recycle");
        if (path.maintenance.idlePublishes > 0) reach.reach("idle-publish");
      }
      await check("the end");
      expect(deltas.length).toBeGreaterThan(5);
      await harness.close();
    }, 300_000);

  it("reached every declared state", () => {
    reach.assertReached();
  });
});
