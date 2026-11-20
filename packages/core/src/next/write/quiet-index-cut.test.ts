/** Ruling 1 / format/manifest.ts: a root's catalog only names covered index builds.
 * Quiet-build eligibility and the chosen cut are one observation. Writes after it stay
 * in their ordinary indexed memtables/WAL, even while root assembly awaits storage.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { createReach, QUIET_INDEX_CUT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Snapshot } from "../access/snapshot.js";
import type { WriteTransaction } from "../access/write-path.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { SimLocks } from "../storage/sim-disk.js";
import { need } from "./bytes.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";

Object.assign(globalThis, { IDBKeyRange });
const kinds = ["sim", "memory", "indexeddb", "opfs"] as const;
const modes = ["committed", "pending commit", "pending rollback"] as const;
function sorted(rows: ReadonlyArray<readonly unknown[]>) {
  return rows.map((row) => [...row]).sort((a, b) => Number(a[0]) - Number(b[0]));
}
async function checkIndex(
  view: Snapshot,
  table: number,
  index: number,
  values: readonly number[],
  h: Harness,
) {
  const grant = await h.governor.admit({
    label: "quiet-cut probe",
    minimumBytes: 64 * 1024,
    spillable: true,
  });
  const keys = new KeyBatch(grant);
  try {
    encodeKeyBatch(
      "equality",
      [{ vector: ints(values), direction: "asc", nulls: "first" }],
      { length: values.length, selection: null },
      keys,
    );
    const output = {
      probes: new Int32Array(values.length),
      locators: new Float64Array(values.length),
      count: 0,
    };
    await view.index(table, index).probe(keys, output);
    expect(output.count).toBe(values.length);
  } finally {
    keys.close();
    grant.close();
  }
}
for (const kind of kinds)
  for (const seed of seedsFor("next-quiet-index-cut", [703, 706]))
    it(`keeps index coverage across the quiet cut ${kind} seed ${String(seed)}`, async () => {
      const reach = createReach(`quiet index cut ${kind}`, QUIET_INDEX_CUT_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 3, max: 9 }),
          fc.integer({ min: 1, max: 2 }),
          fc.integer({ min: -50, max: 50 }),
          fc.boolean(),
          async (width, count, salt, nullable) => {
            reach.beginHistory();
            for (const populated of [false, true])
              for (const mode of populated ? [modes[0]] : modes) {
                let armed = false,
                  phase = "before",
                  before!: () => void,
                  after!: () => void,
                  resumeBefore!: () => void,
                  resumeAfter!: () => void;
                const atBefore = new Promise<void>((r) => {
                  before = r;
                });
                const atAfter = new Promise<void>((r) => {
                  after = r;
                });
                const gateBefore = new Promise<void>((r) => {
                  resumeBefore = r;
                });
                const gateAfter = new Promise<void>((r) => {
                  resumeAfter = r;
                });
                const h = new Harness(
                  "strict",
                  kind === "sim" ? "sim" : "memory",
                  width,
                  undefined,
                  {
                    merges: { mode: "off" },
                    rootSelectionBoundary: async (boundary, operation) => {
                      if (!armed || operation !== "flush") return;
                      if (boundary === "before" && phase === "before") {
                        phase = "after";
                        before();
                        await gateBefore;
                      } else if (boundary === "after" && phase === "after") {
                        armed = false;
                        after();
                        await gateAfter;
                      }
                    },
                  },
                );
                if (kind === "indexeddb") {
                  const factory = new IDBFactory();
                  Object.assign(h, {
                    backend: createIndexedDbBackend({
                      name: "quiet-cut",
                      locks: new SimLocks(),
                      database: () => openIndexedDb(factory, "quiet-cut"),
                    }),
                  });
                }
                if (kind === "opfs")
                  Object.assign(h, {
                    backend: createOpfsBackend({
                      name: "quiet-cut",
                      directory: new MemoryOpfs().root,
                      locks: new SimLocks(),
                    }),
                  });
                let old: Snapshot | null = null,
                  tx: WriteTransaction | null = null;
                try {
                  let p = await h.open(),
                    quiet = 0,
                    busy = 0;
                  for (const name of ["quiet", "busy"])
                    await autocommit(p, async (s) => {
                      const id =
                        (
                          await s.changeCatalog({
                            kind: "createTable",
                            table: tableInput(name, [column("id", INT, false), column("v", INT)]),
                          })
                        ).id ?? 0;
                      if (name === "quiet") quiet = id;
                      else busy = id;
                    });
                  const priorIds = populated
                    ? [...Array.from({ length: width }, (_, i) => -width + i), -100]
                    : [];
                  const prior = priorIds.map((id) => [id, id * 2]);
                  if (populated) {
                    await autocommit(p, (s) =>
                      s
                        .table(quiet)
                        .insert(
                          batch(
                            ints(priorIds.slice(0, width)),
                            ints(priorIds.slice(0, width).map((id) => id * 2)),
                          ),
                          null,
                        ),
                    );
                    await autocommit(p, (s) =>
                      s.table(quiet).insert(batch(ints([-100]), ints([-200])), null),
                    );
                  }
                  for (const ids of [Array.from({ length: width }, (_, i) => -width + i), [-100]])
                    await autocommit(p, (s) =>
                      s.table(busy).insert(batch(ints(ids), ints(ids)), null),
                    );
                  await p.quiesce();
                  const initial = need(p.state().root.tables.find((t) => t.tableId === quiet));
                  expect(initial.groupCount > 0).toBe(populated);
                  reach.reach(populated ? "stored rows before build" : "persisted empty root");
                  armed = true;
                  for (const ids of [Array.from({ length: width }, (_, i) => i + 1), [100]])
                    await autocommit(p, (s) =>
                      s.table(busy).insert(batch(ints(ids), ints(ids)), null),
                    );
                  await atBefore;
                  const valueColumn = need(p.catalog.table(quiet)?.columns[1]).id;
                  let index = 0;
                  await autocommit(p, async (s) => {
                    index =
                      (
                        await s.changeCatalog({
                          kind: "createIndex",
                          index: {
                            tableId: quiet,
                            name: "quiet_v",
                            columnIds: [valueColumn],
                            unique: false,
                            role: "secondary",
                          },
                        })
                      ).id ?? 0;
                  });
                  const buildLsn = p.appliedLsn;
                  old = p.pin();
                  expect(sorted(await rowsOf(old, quiet))).toEqual(sorted(prior));
                  resumeBefore();
                  await atAfter;
                  reach.reach("rows after cut before assembly");
                  const ids = Array.from({ length: count }, (_, i) => 1000 + i);
                  const values = ids.map((_, i) => (nullable && i === 0 ? null : salt + i));
                  tx = await p.begin();
                  const statement = tx.statement({ rules });
                  await statement.table(quiet).insert(batch(ints(ids), ints(values)), null);
                  await statement.end();
                  if (mode === "committed") {
                    await (
                      await tx.commit(null)
                    ).durable;
                    tx = null;
                  }
                  resumeAfter();
                  await p.quiesce();
                  expect(p.failure).toBeNull();
                  const checkpoint = need(p.state().root.tables.find((t) => t.tableId === quiet));
                  if (!populated) {
                    const storedIndex = need(
                      checkpoint.secondaryIndexes.find((i) => i.indexId === index),
                    );
                    expect(checkpoint.flushedThroughLsn).toBe(buildLsn);
                    expect(storedIndex.runs).toHaveLength(0);
                    reach.reach("empty index settled at original cut");
                  } else {
                    // An uncovered build on existing rows must still hold the catalog cut.
                    expect(p.state().root.globalLsn).toBeLessThan(buildLsn);
                    expect(checkpoint.secondaryIndexes.some((i) => i.indexId === index)).toBe(
                      false,
                    );
                    reach.reach("nonquiet build holds cut");
                  }
                  expect(sorted(await rowsOf(old, quiet))).toEqual(sorted(prior));
                  reach.reach("older pin unchanged");
                  if (tx !== null) {
                    if (mode === "pending commit") await (await tx.commit(null)).durable;
                    else tx.rollback();
                    tx = null;
                  }
                  reach.reach(mode);
                  const expected =
                    mode === "pending rollback"
                      ? prior
                      : [...prior, ...ids.map((id, i) => [id, values[i] ?? null])];
                  old.release();
                  old = null;
                  for (let attempt = 0; attempt < 2; attempt++) {
                    if (attempt > 0) p = await h.reopen();
                    const view = p.pin();
                    try {
                      expect(sorted(await rowsOf(view, quiet))).toEqual(sorted(expected));
                      await checkIndex(
                        view,
                        quiet,
                        index,
                        expected.flatMap((row) => (typeof row[1] === "number" ? [row[1]] : [])),
                        h,
                      );
                    } finally {
                      view.release();
                    }
                  }
                  reach.reach("reopen indexed values");
                  await autocommit(p, (s) =>
                    s.table(quiet).insert(batch(ints([2000]), ints([5000])), null),
                  );
                  await p.quiesce();
                  const view = p.pin();
                  try {
                    await checkIndex(view, quiet, index, [5000], h);
                  } finally {
                    view.release();
                  }
                  await p.recycleLog();
                  const complete = need(p.state().root.tables.find((t) => t.tableId === quiet));
                  expect(
                    need(complete.secondaryIndexes.find((i) => i.indexId === index)).runs.length,
                  ).toBeGreaterThan(0);
                  await h.space.audit();
                  reach.reach("exact inventory and continued writes");
                  reach.reach(nullable ? "NULL payload" : "all valid");
                } finally {
                  resumeBefore();
                  resumeAfter();
                  tx?.rollback();
                  old?.release();
                  await h.close();
                  expect(h.governor.usedBytes).toBe(0);
                  reach.reach("zero final charge");
                }
              }
          },
        ),
        {
          seed,
          numRuns: 4,
          // The first example pins the captured seed703 ordering.
          examples: [
            [6, 1, 22, false],
            [6, 2, -3, true],
          ],
        },
      );
      reach.assertReached();
    });
