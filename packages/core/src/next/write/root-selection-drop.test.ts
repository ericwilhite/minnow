/** R25 / PublishDelta.added: every added extent is reached by the exact root being installed.
 * The publication lock excludes other publications, not ordinary catalog/WAL commits.
 * A delayed flush may therefore select a cut after DROP, or install a coherent older cut.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createReach, ROOT_SELECTION_DROP_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Snapshot } from "../access/snapshot.js";
import type { ExtentWriter } from "../storage/extents.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { SimLocks } from "../storage/sim-disk.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "./test-support.js";

Object.assign(globalThis, { IDBKeyRange });
const kinds = ["sim", "memory", "indexeddb", "opfs"] as const;
for (const seed of seedsFor("next-root-selection-drop", [704, 705]))
  it(`keeps delayed flush ownership exact across DROP seed ${String(seed)}`, async () => {
    const reach = createReach("root selection versus DROP", ROOT_SELECTION_DROP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 12 }),
        fc.integer({ min: -30, max: 30 }),
        async (width, salt) => {
          reach.beginHistory();
          for (const kind of kinds)
            for (const phase of ["before", "after"] as const)
              for (const nullable of [false, true]) {
                let armed = false,
                  entered!: () => void,
                  resume!: () => void,
                  target = 0;
                const targetFlush: { writer: ExtentWriter | null } = { writer: null };
                const atGate = new Promise<void>((resolve) => {
                  entered = resolve;
                });
                const gate = new Promise<void>((resolve) => {
                  resume = resolve;
                });
                const pause = () => {
                  armed = false;
                  entered();
                  return gate;
                };
                const harness = new Harness(
                  "strict",
                  kind === "sim" ? "sim" : "memory",
                  width,
                  undefined,
                  {
                    merges: { mode: "off" },
                    rootSelectionBoundary: (boundary, operation) =>
                      armed && phase === boundary && operation === "flush"
                        ? pause()
                        : Promise.resolve(),
                  },
                );
                if (kind === "indexeddb") {
                  const factory = new IDBFactory();
                  Object.assign(harness, {
                    backend: createIndexedDbBackend({
                      name: "root-select",
                      locks: new SimLocks(),
                      database: () => openIndexedDb(factory, "root-select"),
                    }),
                  });
                }
                if (kind === "opfs") {
                  const native = new MemoryOpfs();
                  Object.assign(harness, {
                    backend: createOpfsBackend({
                      name: "root-select",
                      directory: native.root,
                      locks: new SimLocks(),
                    }),
                  });
                }
                const preparing = new Set<number>();
                let installed = 0;
                harness.extentDeltas = {
                  prepare(attempt, delta) {
                    if (delta.writer === targetFlush.writer) preparing.add(attempt);
                    return null;
                  },
                  commit(attempt) {
                    if (preparing.delete(attempt)) installed++;
                  },
                  abort(attempt) {
                    preparing.delete(attempt);
                  },
                };
                harness.onSpace = (space) => {
                  const begin = space.beginWrite.bind(space);
                  space.beginWrite = (label, options) => {
                    const writer = begin(label, options);
                    if (label === `flush t${String(target)}`) targetFlush.writer = writer;
                    return writer;
                  };
                };
                let old: Snapshot | null = null;
                try {
                  let path = await harness.open();
                  await autocommit(path, async (s) => {
                    target =
                      (
                        await s.changeCatalog({
                          kind: "createTable",
                          table: tableInput("target", [column("id", INT, false), column("v", INT)]),
                        })
                      ).id ?? 0;
                  });
                  let kept = 0;
                  await autocommit(path, async (s) => {
                    kept =
                      (
                        await s.changeCatalog({
                          kind: "createTable",
                          table: tableInput("kept", [column("id", INT, false)]),
                        })
                      ).id ?? 0;
                  });
                  await autocommit(path, (s) => s.table(kept).insert(batch(ints([salt])), null));
                  await path.quiesce();
                  armed = true;
                  const ids = Array.from({ length: width }, (_, i) => salt + i + 1);
                  const values = ids.map((n, i) => (nullable && i % 2 === 0 ? null : n * 3));
                  await autocommit(path, (s) =>
                    s.table(target).insert(batch(ints(ids), ints(values)), null),
                  );
                  await autocommit(path, (s) =>
                    s.table(target).insert(batch(ints([1000]), ints([nullable ? null : 17])), null),
                  );
                  await atGate;
                  const targetWriter = targetFlush.writer;
                  if (targetWriter === null) throw new Error("Missing provisional flush writer");
                  expect(targetWriter.extents.length).toBeGreaterThan(0);
                  reach.reach("sealed output exists");
                  old = path.pin();
                  const expected = ids.map((n, i) => [n, values[i] ?? null]);
                  expected.push([1000, nullable ? null : 17]);
                  expect(await rowsOf(old, target)).toEqual(expected);
                  await autocommit(path, (s) =>
                    s.changeCatalog({ kind: "dropTable", tableId: target }),
                  );
                  expect(path.catalog.table(target)).toBeUndefined();
                  resume();
                  await path.quiesce();
                  expect(harness.space.stats.writerBytes).toBe(0);
                  reach.reach("no orphan writer");
                  await harness.space.audit();
                  reach.reach("exact native inventory");
                  expect(await rowsOf(old, target)).toEqual(expected);
                  reach.reach("older pin values");
                  if (phase === "before") {
                    expect(installed).toBe(0);
                    expect(path.maintenance.flushesAbandoned).toBeGreaterThan(0);
                    reach.reach("drop before selection");
                    reach.reach("missing target abandoned");
                  } else {
                    expect(installed).toBe(1);
                    reach.reach("drop after selection");
                    reach.reach("coherent older root kept");
                  }
                  old.release();
                  old = null;
                  path = await harness.reopen();
                  expect(path.catalog.table(target)).toBeUndefined();
                  const view = path.pin();
                  try {
                    expect(await rowsOf(view, kept)).toEqual([[salt]]);
                  } finally {
                    view.release();
                  }
                  await autocommit(path, (s) =>
                    s.table(kept).insert(batch(ints([salt + 1000])), null),
                  );
                  await path.quiesce();
                  await harness.space.audit();
                  reach.reach("reopen and keep writing");
                  reach.reach(kind);
                  reach.reach(nullable ? "NULL payload" : "all valid");
                } finally {
                  resume();
                  old?.release();
                  await harness.close();
                  expect(harness.governor.usedBytes).toBe(0);
                  reach.reach("zero final charge");
                }
              }
        },
      ),
      { seed, numRuns: 8, examples: [[6, 42]] },
    );
    reach.assertReached();
  });
