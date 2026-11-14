import { catalogStorageParts, CatalogBuffers } from "./catalog-storage.js";
import { decodeCatalogParts } from "./catalog-stream.js";
/**
 * manifest.ts "Checkpoints and replay": the catalog at globalLsn and every table's indexes
 * agree. DROP cannot erase an earlier catalog's uncheckpointed CREATE INDEX obligation.
 * access/key-index.ts requires the stored index to map its live rows, not merely have an ID.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import type { Snapshot } from "../access/snapshot.js";
import type { SpacePin } from "../access/space.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import type { ManifestRoot } from "../format/manifest.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { Governor } from "../memory/governor.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { createReach, WRITE_CATALOG_DROP_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import { Catalog } from "./catalog.js";
import { RunReader } from "./runs.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  ints,
  INT,
  rowsOf,
  tableInput,
  watchGrants,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
const reach = createReach(
  "catalog checkpoints retain cancelled index builds",
  WRITE_CATALOG_DROP_STATES,
);
type Backend = "memory" | "sim" | "indexeddb";
interface Plan {
  readonly rows: number;
  readonly second: number | null;
  readonly lateBlocker: boolean;
  readonly reverse: boolean;
  readonly recover: boolean;
  readonly replace: boolean;
}
interface TableEvent {
  readonly table: number;
  readonly primary: number;
  readonly lsn: number;
}
interface IndexEvent {
  readonly table: number;
  readonly index: number;
  readonly create: number;
  drop: number | null;
  readonly values: readonly number[];
}

async function checkIndex(view: Snapshot, table: number, index: number, values: readonly number[]) {
  const keys = new KeyBatch(FREE);
  const cursor = view.index(table, index).range({ lower: null, upper: null }, "asc");
  try {
    encodeKeyBatch(
      "equality",
      [{ vector: ints(values), direction: "asc", nulls: "first" }],
      { length: values.length, selection: null },
      keys,
    );
    const out = {
      probes: new Int32Array(values.length + 1),
      locators: new Float64Array(values.length + 1),
      count: 0,
    };
    await view.index(table, index).probe(keys, out);
    expect(out.count).toBe(values.length);
    expect(Array.from(out.probes.subarray(0, out.count))).toEqual(values.map((_, k) => k));
    const locators: number[] = [],
      buffer = new Float64Array(3);
    for (;;) {
      const n = await cursor.next(buffer);
      if (!n) break;
      locators.push(...buffer.subarray(0, n));
    }
    expect(locators).toEqual(Array.from(out.locators.subarray(0, out.count)));
  } finally {
    cursor.close();
    keys.close();
  }
}

async function scenario(backend: Backend, plan: Plan) {
  reach.beginHistory();
  reach.reach(backend);
  reach.reach(plan.rows === 0 ? "empty cancelled build" : "nonempty cancelled build");
  reach.reach(plan.second === null ? "single cancellation" : "multiple cancellations");
  reach.reach(plan.lateBlocker ? "later table creation" : "existing blocker table");
  reach.reach(plan.reverse ? "reverse drop order" : "creation drop order");
  reach.reach(plan.recover ? "interrupted publication" : "ordinary publication");
  let armed = false,
    fail = false;
  let entered!: () => void, release!: () => void;
  const atFlush = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const injected = new Error("interrupted cancelled-build publication");
  const h = new Harness("strict", backend === "sim" ? "sim" : "memory", 2, undefined, {
    merges: { mode: "off" },
    beforeFlushPublish: async () => {
      if (armed) {
        armed = false;
        entered();
        await gate;
      }
    },
    afterDeltaPrepared: (kind) => {
      if (fail && kind === "flush") {
        fail = false;
        throw injected;
      }
    },
  });
  Object.assign(h, { governor: new Governor(32 * 1024 * 1024) });
  reach.reach("32 MiB owner cap");
  if (backend === "indexeddb")
    Object.assign(h, {
      backend: createIndexedDbBackend({
        database: () => simIndexedDb(h.disk),
        name: "catalog-drop",
        locks: new SimLocks(),
        memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
      }),
    });
  const tables: TableEvent[] = [],
    indexes: IndexEvent[] = [];
  const observations: Array<{ root: ManifestRoot; pin: SpacePin }> = [];
  h.extentDeltas = {
    prepare: () => null,
    abort: () => undefined,
    commit: () => {
      observations.push({
        root: decodeManifestRoot(need(h.space.root), FREE).value,
        pin: h.space.pin("catalog prefix oracle", FREE),
      });
    },
  };
  let chargedPending = 0,
    remembered = 0;
  watchGrants(h.governor, (grant) => {
    if (grant.label !== "write path metadata") return;
    let owned = 0;
    const allocate = grant.allocate.bind(grant),
      free = grant.free.bind(grant),
      close = grant.close.bind(grant);
    grant.allocate = (bytes) => {
      const accepted = allocate(bytes);
      if (accepted && new Error().stack?.includes("#rememberIndex")) {
        owned += bytes;
        chargedPending += bytes;
        remembered++;
      }
      return accepted;
    };
    grant.free = (bytes) => {
      if (new Error().stack?.includes("#forgetIndex")) {
        owned -= bytes;
        chargedPending -= bytes;
        expect(owned).toBeGreaterThanOrEqual(0);
      }
      free(bytes);
    };
    grant.close = () => {
      close();
      chargedPending -= owned;
      owned = 0;
    };
  });
  let p = await h.open();
  const create = async (name: string) => {
    let table = 0;
    const lsn = await autocommit(p, async (s) => {
      table =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput(name, [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    tables.push({ table, primary: need(p.catalog.table(table)).primaryKey ?? 0, lsn });
    return table;
  };
  const index = async (table: number, values: readonly number[], name = `idx_${String(table)}`) => {
    let id = 0;
    const create = await autocommit(p, async (s) => {
      id =
        (
          await s.changeCatalog({
            kind: "createIndex",
            index: {
              tableId: table,
              name,
              columnIds: [need(need(p.catalog.table(table)).columns[0]).id],
              unique: false,
              role: "secondary",
            },
          })
        ).id ?? 0;
    });
    const event: IndexEvent = { table, index: id, create, drop: null, values };
    indexes.push(event);
    return event;
  };
  const put = (table: number, values: readonly number[]) =>
    autocommit(p, (s) => s.table(table).insert(batch(ints(values)), null));
  const validateRoots = async () => {
    for (const { root } of observations) {
      const buffers = new CatalogBuffers(FREE);
      let catalog: Catalog;
      try {
        catalog = Catalog.fromStored(
          await decodeCatalogParts(catalogStorageParts(root.catalog, h.space, FREE), buffers),
        );
      } finally {
        buffers.close();
      }
      const born = tables.filter((t) => t.lsn <= root.globalLsn);
      expect(catalog.tables.map((t) => t.id).sort((a, b) => a - b)).toEqual(
        born.map((t) => t.table).sort((a, b) => a - b),
      );
      expect(root.tables.map((t) => t.tableId).sort((a, b) => a - b)).toEqual(
        born.map((t) => t.table).sort((a, b) => a - b),
      );
      for (const event of born) {
        const required = [
          event.primary,
          ...indexes
            .filter(
              (i) =>
                i.table === event.table &&
                i.create <= root.globalLsn &&
                (i.drop === null || i.drop > root.globalLsn),
            )
            .map((i) => i.index),
        ];
        expect(
          need(catalog.table(event.table))
            .indexes.map((i) => i.id)
            .sort((a, b) => a - b),
        ).toEqual(required.sort((a, b) => a - b));
        const table = need(root.tables.find((t) => t.tableId === event.table));
        const actual = [table.keyIndex, ...table.secondaryIndexes].filter((i) => i !== null);
        for (const id of required)
          expect(
            actual.some((i) => i.indexId === id),
            "every required historical index has a root",
          ).toBe(true);
        for (const extra of actual.filter((i) => !required.includes(i.indexId))) {
          expect(indexes.some((i) => i.index === extra.indexId && i.create > root.globalLsn)).toBe(
            true,
          );
          expect(extra.runs, "only an empty future index may precede its CREATE").toEqual([]);
        }
      }
      reach.reach("catalog prefix checked");
      for (const event of indexes) {
        if (event.drop === null || root.globalLsn < event.create || root.globalLsn >= event.drop)
          continue;
        const stored = need(
          need(root.tables.find((t) => t.tableId === event.table)).secondaryIndexes.find(
            (i) => i.indexId === event.index,
          ),
        );
        const found: Array<{ row: number; key: number[]; tombstone: boolean }> = [];
        for (const ref of stored.runs) {
          const run = await RunReader.open(h.space, ref, FREE, FREE);
          await run.cursor(h.space, FREE, (key, row, tombstone) => {
            found.push({ row, key: Array.from(key), tombstone });
            return false;
          });
        }
        const keys = new KeyBatch(FREE);
        try {
          encodeKeyBatch(
            "equality",
            [{ vector: ints(event.values), direction: "asc", nulls: "first" }],
            { length: event.values.length, selection: null },
            keys,
          );
          expect(
            found.sort((a, b) => a.row - b.row),
            "historical backfill has the original keys and row IDs",
          ).toEqual(
            event.values.map((_, k) => ({
              row: k + 1,
              key: Array.from(keys.key(k)),
              tombstone: false,
            })),
          );
        } finally {
          keys.close();
        }
        reach.reach(
          event.values.length ? "historical backfill stored" : "historical empty index stored",
        );
      }
    }
    for (const { pin } of observations) pin.release();
    observations.length = 0;
  };
  try {
    const source = await create("source"),
      target = await create("target");
    const other = plan.second === null ? null : await create("other");
    let blocker = plan.lateBlocker ? 0 : await create("blocker");
    const fence = await create("fence");
    await put(fence, [6000]);
    const targetValues = Array.from({ length: plan.rows }, (_, i) => 100 + i);
    const otherValues = Array.from({ length: plan.second ?? 0 }, (_, i) => 200 + i);
    if (targetValues.length) await put(target, targetValues);
    if (other !== null && otherValues.length) await put(other, otherValues);
    if (blocker) await put(blocker, [5000]);
    await put(source, [-1, -2]);
    await put(source, [-3]);
    await p.quiesce();
    armed = true;
    const sourceThrough = await put(source, [1, 2]);
    await put(source, [3]);
    await atFlush;
    const cancelled = [await index(target, targetValues)];
    if (plan.lateBlocker) {
      blocker = await create("blocker");
      await put(blocker, [5000]);
    }
    if (other !== null) cancelled.push(await index(other, otherValues));
    const survivor = await index(blocker, [5000]);
    const fenceIndex = await index(fence, [6000]);
    for (const event of plan.reverse ? [...cancelled].reverse() : cancelled)
      event.drop = await autocommit(p, (s) =>
        s.changeCatalog({ kind: "dropIndex", indexId: event.index }),
      );
    // The blocker must flush, so this checks progress, not merely avoidance of publication.
    await put(blocker, [5001, 5002]);
    await put(blocker, [5003]);
    await put(fence, [6001, 6002]);
    await put(fence, [6003]);
    fail = plan.recover;
    release();
    if (plan.recover) {
      await expect(p.quiesce()).rejects.toBe(injected);
      expect(p.failure).toBe(injected);
      await validateRoots();
      p = await h.reopen();
      reach.reach("replay uncheckpointed drop");
    }
    await p.quiesce();
    expect(p.failure).toBeNull();
    expect(remembered, "each build record takes native metadata credit").toBeGreaterThanOrEqual(
      cancelled.length + 2,
    );
    expect(chargedPending, "published builds/drops return their record charges").toBe(0);
    reach.reach("build records charged and returned");
    // quiesce drains runnable work, not every live/overlapping memtable. These checkpoint
    // assertions require the builds to finish and the DROP prefix to become publishable.
    const checkpoint = decodeManifestRoot(need(h.space.root), FREE).value;
    expect(
      need(checkpoint.tables.find((t) => t.tableId === blocker)).flushedThroughLsn,
    ).toBeGreaterThanOrEqual(survivor.create);
    expect(checkpoint.globalLsn).toBeGreaterThanOrEqual(
      Math.max(...cancelled.map((i) => need(i.drop))),
    );
    if (
      plan.rows > 0 &&
      observations.some(
        ({ root }) =>
          root.globalLsn < need(cancelled[0]).create &&
          (root.tables.find((t) => t.tableId === source)?.flushedThroughLsn ?? 0) >= sourceThrough,
      )
    )
      reach.reach("checkpoint retreats before cancelled CREATE");
    const check = async (extra: boolean, replacement: IndexEvent | null) => {
      const view = p.pin();
      try {
        for (const [table, values] of [
          [source, [-3, -2, -1, 1, 2, 3, ...(extra ? [4] : [])]],
          [target, [...targetValues, ...(extra ? [999] : [])]],
          [blocker, [5000, 5001, 5002, 5003, ...(extra ? [5004] : [])]],
        ] as const) {
          expect(
            (await rowsOf(view, table)).map((r) => r[0]).sort((a, b) => Number(a) - Number(b)),
          ).toEqual(values);
          await checkIndex(view, table, need(view.catalog.table(table)).primaryKey ?? 0, values);
        }
        if (other !== null)
          expect(
            (await rowsOf(view, other)).map((r) => r[0]).sort((a, b) => Number(a) - Number(b)),
          ).toEqual(otherValues);
        for (const event of cancelled)
          expect(
            need(view.catalog.table(event.table)).indexes.some((i) => i.id === event.index),
          ).toBe(false);
        expect(
          (await rowsOf(view, fence)).map((row) => row[0]).sort((a, b) => Number(a) - Number(b)),
        ).toEqual([6000, 6001, 6002, 6003]);
        await checkIndex(view, fence, fenceIndex.index, [6000, 6001, 6002, 6003]);
        await checkIndex(view, blocker, survivor.index, [
          5000,
          5001,
          5002,
          5003,
          ...(extra ? [5004] : []),
        ]);
        if (replacement) await checkIndex(view, target, replacement.index, [...targetValues, 999]);
      } finally {
        view.release();
      }
    };
    await check(false, null);
    await validateRoots();
    await h.space.audit();
    const transient = await autocommit(p, async (statement) => {
      const id =
        (
          await statement.changeCatalog({
            kind: "createIndex",
            index: {
              tableId: target,
              name: "transient",
              columnIds: [need(need(p.catalog.table(target)).columns[0]).id],
              unique: false,
              role: "secondary",
            },
          })
        ).id ?? 0;
      await statement.changeCatalog({ kind: "dropIndex", indexId: id });
    });
    expect(decodeManifestRoot(need(h.space.root), FREE).value.globalLsn).toBeLessThan(transient);
    const allocationsBeforeReplay = remembered;
    p = await h.reopen();
    expect(remembered, "replay reached the same-transaction CREATE/DROP").toBeGreaterThan(
      allocationsBeforeReplay,
    );
    expect(chargedPending, "no prefix needs a build dropped in its own transaction").toBe(0);
    reach.reach("atomic create drop replay");
    await p.quiesce();
    expect(p.failure).toBeNull();
    await check(false, null);
    await put(source, [4]);
    await put(target, [999]);
    await put(blocker, [5004]);
    reach.reach("continued durable writes");
    const replacement = plan.replace ? await index(target, [...targetValues, 999]) : null;
    if (replacement) reach.reach("replacement after reopen");
    await p.recycleLog();
    await p.quiesce();
    expect(p.failure).toBeNull();
    await check(true, replacement);
    await validateRoots();
    await h.space.audit();
    p = await h.reopen();
    await p.quiesce();
    expect(p.failure).toBeNull();
    await check(true, replacement);
    await validateRoots();
    reach.reach("durable reopen");
  } finally {
    release();
    for (const { pin } of observations) pin.release();
    observations.length = 0;
    await h.close();
    expect(h.governor.usedBytes).toBe(0);
    reach.reach("all credit returned");
  }
}

for (const backend of ["memory", "sim"] as const)
  it(`does not lose a quiet build at DROP, including recovery (${backend})`, async () => {
    await scenario(backend, {
      rows: 0,
      second: null,
      lateBlocker: false,
      reverse: false,
      recover: false,
      replace: false,
    });
    await scenario(backend, {
      rows: 0,
      second: null,
      lateBlocker: false,
      reverse: false,
      recover: true,
      replace: true,
    });
    await scenario(backend, {
      rows: 3,
      second: 3,
      lateBlocker: true,
      reverse: true,
      recover: false,
      replace: true,
    });
  });
for (const backend of ["memory", "sim", "indexeddb"] as const)
  for (const seed of seedsFor("next-write-catalog-drop", [702, 704]))
    it(`keeps catalogue prefixes usable (${backend}, seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            rows: fc.constantFrom(0, 1, 3, 5),
            second: fc.constantFrom(null, 0, 3),
            lateBlocker: fc.boolean(),
            reverse: fc.boolean(),
            recover: fc.boolean(),
            replace: fc.boolean(),
          }),
          (plan) => scenario(backend, plan),
        ),
        { seed, numRuns: 24 },
      );
    }, 300_000);
it("reaches the cancelled-build checkpoint dimensions", () => reach.assertReached());
