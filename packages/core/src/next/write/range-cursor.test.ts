/** Streaming range law vs the retained eager reference, under real and forced task schedules. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  WRITE_STREAM_RANGE_STATES,
  WRITE_STREAM_DELTA_STATES,
  WRITE_STREAM_ORDER_STATES,
} from "../../testing/reach.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { seedsFor } from "../../testing/seeds.js";
import { IndexCorruptionError, inKeyRange, type KeyRange } from "../access/key-index.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { MemtableMemoryError } from "./bytes.js";
import { KeyDelta } from "./key-hash.js";
import { Governor } from "../memory/governor.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { DeltaRangeInput, RunRangeInput } from "./range-input.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  locators,
  rules,
  rowsOf,
  texts,
  TEXT,
  tableInput,
} from "./test-support.js";
import type { SnapshotImpl } from "./view.js";

const reach = createReach("streaming range visibility", WRITE_STREAM_RANGE_STATES);
const mutationReach = createReach("ordered cursor mutation", WRITE_STREAM_DELTA_STATES);
const orderReach = createReach("ordered leaf funding", WRITE_STREAM_ORDER_STATES);
const ALL = { lower: null, upper: null };
const FREE = { allocate: () => true, free: () => undefined };
function key(n: number): Uint8Array {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints([n]), direction: "asc", nulls: "first" }],
    { length: 1, selection: null },
    keys,
  );
  const result = keys.key(0).slice();
  keys.close();
  return result;
}
async function check(
  view: SnapshotImpl,
  table: number,
  index: number,
  range: KeyRange,
  capacity: number,
): Promise<void> {
  const reader = view.index(table, index),
    eager = await reader.collect(range);
  reach.reach(reader.def.unique ? "unique" : "nonunique");
  if (range.lower !== null || range.upper !== null) reach.reach("bounded range");
  if (eager.length === 0) reach.reach("empty range");
  if (eager.length > capacity) reach.reach("output continuation");
  const layers = reader.table.tableView;
  if ((layers.runs.get(index)?.length ?? 0) > 0) reach.reach("stored input");
  if (layers.memtables.some((m) => (m.deltas.get(index)?.entryCount ?? 0) > 0))
    reach.reach("memtable input");
  if (layers.memtables.some((m) => (m.staged?.runs.get(index)?.length ?? 0) > 0))
    reach.reach("staged input");
  for (const direction of ["asc", "desc"] as const) {
    reach.reach(direction === "asc" ? "ascending" : "descending");
    const expected = eager.map((e) => view.internalTable(table).locator(e.rowId));
    if (direction === "desc") expected.reverse();
    const cursor = reader.range(range, direction),
      out = new Float64Array(capacity),
      actual: number[] = [];
    try {
      for (let calls = 0; ; calls++) {
        expect(calls).toBeLessThan(eager.length + 2);
        const count = await cursor.next(out);
        expect(count).toBeLessThanOrEqual(capacity);
        actual.push(...out.subarray(0, count));
        if (!count) break;
      }
      expect(actual).toEqual(expected);
      const skipping = reader.range(range, direction);
      try {
        const offset = Math.floor(expected.length / 2);
        expect(await skipping.skip(offset)).toBe(offset);
        const rest: number[] = [];
        for (;;) {
          const n = await skipping.next(out);
          if (!n) break;
          rest.push(...out.subarray(0, n));
        }
        expect(rest).toEqual(expected.slice(offset));
      } finally {
        skipping.close();
      }
    } finally {
      cursor.close();
      cursor.close();
    }
  }
}
for (const seed of seedsFor("next-write-stream-range", [0xb3c8]))
  it(`merges ranges against eager visibility (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          values: fc.uniqueArray(fc.integer({ min: 0, max: 90 }), { minLength: 8, maxLength: 50 }),
          capacity: fc.constantFrom(1, 2, 7),
          stored: fc.boolean(),
          staged: fc.boolean(),
          hot: fc.boolean(),
          capMiB: fc.constantFrom(32, 64),
          backend: fc.constantFrom("memory", "indexeddb"),
          lower: fc.integer({ min: 0, max: 90 }),
          upper: fc.integer({ min: 0, max: 90 }),
          after: fc.boolean(),
          through: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.backend);
          if (s.hot) reach.reach("hot nonunique");
          reach.reach(s.capMiB === 32 ? "32 MiB cap" : "64 MiB cap");
          const h = new Harness("strict", "memory", s.staged ? 4 : 64);
          Object.assign(h, { governor: new Governor(s.capMiB * 1024 * 1024) });
          if (s.backend === "indexeddb")
            Object.assign(h, {
              backend: createIndexedDbBackend({
                database: () => simIndexedDb(h.disk),
                name: "stream-range",
                locks: new SimLocks(),
                memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
              }),
            });
          const p = await h.open();
          let table = 0,
            nonunique = 0;
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
            await autocommit(p, async (st) => {
              nonunique =
                (
                  await st.changeCatalog({
                    kind: "createIndex",
                    index: {
                      name: "by_v",
                      tableId: table,
                      columnIds: [p.catalog.table(table)?.columns[1]?.id ?? 0],
                      unique: false,
                      role: "secondary",
                    },
                  })
                ).id ?? 0;
            });
            if (s.staged) {
              const load = await p.begin();
              for (let from = 0; from < s.values.length; from += 4) {
                const st = load.statement({ rules }),
                  values = s.values.slice(from, from + 4);
                await st
                  .table(table)
                  .insert(batch(ints(values), ints(values.map((n) => (s.hot ? 1 : n % 4)))), null);
                await st.end();
              }
              const read = load.statement({ rules });
              for (const id of [p.catalog.table(table)?.primaryKey ?? 0, nonunique])
                await check(read.view, table, id, ALL, s.capacity);
              await read.end();
              await (
                await load.commit(null)
              ).durable;
            } else
              await autocommit(p, (st) =>
                st
                  .table(table)
                  .insert(
                    batch(ints(s.values), ints(s.values.map((n) => (s.hot ? 1 : n % 4)))),
                    null,
                  ),
              );
            if (s.stored) await p.recycleLog();
            const old = p.pin(),
              currentKey = p.catalog.table(table)?.primaryKey ?? 0;
            try {
              const tx = await p.begin(),
                st = tx.statement({ rules });
              await st
                .table(table)
                .delete(batch(locators([old.internalTable(table).locator(1)], table)), null);
              await st.end();
              const next = tx.statement({ rules });
              for (const id of [currentKey, nonunique])
                await check(next.view, table, id, ALL, s.capacity);
              await next.table(table).insert(batch(ints([100]), ints([1])), null);
              // Its own row is invisible, earlier statement's deletion is visible.
              await check(next.view, table, currentKey, ALL, s.capacity);
              reach.reach("earlier statement");
              reach.reach("own writes invisible");
              await next.end();
              await (
                await tx.commit(null)
              ).durable;
              const view = p.pin();
              try {
                const range: KeyRange = {
                  lower: { kind: s.after ? "after" : "atLeast", key: key(s.lower) },
                  upper: { kind: s.through ? "through" : "before", key: key(s.upper) },
                };
                reach.reach("older pin");
                reach.reach("committed delete");
                for (const pin of [old, view])
                  for (const id of [currentKey, nonunique]) {
                    await check(pin, table, id, ALL, s.capacity);
                    await check(pin, table, id, range, s.capacity);
                  }
              } finally {
                view.release();
              }
            } finally {
              old.release();
            }
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);

for (const seed of seedsFor("next-write-stream-delta", [0xb3c9]))
  it(`walks maintained order without sorting on read (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            key: fc.uint8Array({ minLength: 1, maxLength: 4, min: 0, max: 4 }),
            row: fc.integer({ min: 1, max: 30 }),
            seq: fc.integer({ min: 1, max: 5 }),
            dead: fc.boolean(),
          }),
          { maxLength: 400 },
        ),
        fc.integer({ min: 0, max: 4 }),
        fc.integer({ min: 0, max: 20 }),
        async (entries, bound, pops) => {
          const delta = new KeyDelta();
          try {
            for (const e of entries) delta.add(e.key, 0, e.key.length, e.row, e.seq, e.dead);
            for (let i = 0; i < Math.min(pops, delta.entryCount); i++)
              delta.pop(delta.entryCount - 1);
            const range = {
              lower: { kind: "atLeast" as const, key: new Uint8Array([1]) },
              upper: { kind: "through" as const, key: new Uint8Array([3]) },
            };
            const expected = delta
              .sorted(bound)
              .filter((e) => inKeyRange(range, e.key))
              .flatMap((e) =>
                e.entries.map(
                  (r) => `${e.key.join(".")}:${String(r.rowId)}:${String(r.tombstone)}`,
                ),
              );
            for (const direction of [1, -1]) {
              const input = new DeltaRangeInput(delta, bound, range, direction),
                got: string[] = [];
              try {
                while (await input.advance())
                  got.push(
                    `${input.key().join(".")}:${String(input.rowId)}:${String(input.tombstone)}`,
                  );
              } finally {
                input.close();
              }
              expect(got).toEqual(direction > 0 ? expected : [...expected].reverse());
            }
          } finally {
            delta.releaseCharge();
          }
        },
      ),
      { seed, numRuns: 400 },
    );
  });

it("stops at the requested prefix and closes without collecting the remainder", async () => {
  const h = new Harness("strict", "memory"),
    p = await h.open();
  let t = 0;
  try {
    await autocommit(p, async (st) => {
      t =
        (
          await st.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(p, (st) =>
      st.table(t).insert(batch(ints(Array.from({ length: 10000 }, (_, i) => 10000 - i))), null),
    );
    const view = p.pin(),
      reader = view.index(t, p.catalog.table(t)?.primaryKey ?? 0);
    const eager = vi.spyOn(reader, "collect").mockRejectedValue(new Error("whole range collected"));
    const advance = vi.spyOn(DeltaRangeInput.prototype, "advance");
    const baseline = h.governor.usedBytes;
    const cursor = reader.range(ALL, "asc"),
      out = new Float64Array(1);
    expect(await cursor.next(out)).toBe(1);
    expect(advance.mock.calls.length).toBeLessThanOrEqual(2);
    expect(eager).not.toHaveBeenCalled();
    expect(h.governor.usedBytes).toBeGreaterThan(baseline);
    cursor.close();
    expect(h.governor.usedBytes).toBe(baseline);
    view.release();
    eager.mockRestore();
    advance.mockRestore();
  } finally {
    vi.restoreAllMocks();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it("reads a stored prefix without walking unrelated blocks", async () => {
  const h = new Harness("strict", "memory"),
    p = await h.open();
  let t = 0;
  try {
    await autocommit(p, async (st) => {
      t =
        (
          await st.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(p, (st) =>
      st.table(t).insert(batch(ints(Array.from({ length: 10000 }, (_, i) => i))), null),
    );
    await p.recycleLog();
    await p.quiesce();
    const view = p.pin(),
      reader = view.index(t, p.catalog.table(t)?.primaryKey ?? 0);
    vi.spyOn(h.space, "peek").mockReturnValue(null);
    const read = vi.spyOn(h.space, "read"),
      advance = vi.spyOn(RunRangeInput.prototype, "advance");
    const cursor = reader.range(ALL, "asc"),
      out = new Float64Array(1);
    try {
      expect(await cursor.next(out)).toBe(1);
      expect(advance.mock.calls.length).toBeLessThanOrEqual(2);
      expect(read.mock.calls.reduce((n, [refs]) => n + refs.length, 0)).toBeLessThanOrEqual(2);
      expect(view.internalTable(t).rowId(out[0] ?? -1)).toBe(1);
    } finally {
      cursor.close();
      view.release();
      vi.restoreAllMocks();
    }
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it.each(
  [
    { mode: "cursor", phase: "metadata" },
    { mode: "snapshot", phase: "metadata" },
    { mode: "cursor", phase: "block" },
    { mode: "snapshot", phase: "block" },
  ].flatMap((s) => ["next", "skip"].map((call) => ({ ...s, call }))),
)(
  "settles close during cold I/O ($mode/$phase) without resurrecting scratch",
  async ({ mode, phase, call }) => {
    const h = new Harness("strict", "memory"),
      p = await h.open();
    let t = 0;
    let release: () => void = () => undefined;
    try {
      await autocommit(p, async (st) => {
        t =
          (
            await st.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false)]),
            })
          ).id ?? 0;
      });
      await autocommit(p, (st) => st.table(t).insert(batch(ints([1, 2, 3])), null));
      await p.recycleLog();
      await p.quiesce();
      const view = p.pin(),
        reader = view.index(t, p.catalog.table(t)?.primaryKey ?? 0);
      const gate = new Promise<void>((r) => {
        release = r;
      });
      let entered: () => void = () => undefined;
      const waiting = new Promise<void>((r) => {
        entered = r;
      });
      const read = h.space.read.bind(h.space),
        admit = h.governor.tryAdmit.bind(h.governor);
      let grant: import("../memory/governor.js").Grant | null = null;
      vi.spyOn(h.governor, "tryAdmit").mockImplementation((options) => {
        const got = admit(options);
        if (options.label === "write index range") grant = got;
        return got;
      });
      const metadata = new Set(
        reader.table.tableView.runs
          .get(reader.def.id)
          ?.flatMap((run) => [run.entries, run.fences, run.bloom]),
      );
      vi.spyOn(h.space, "peek").mockReturnValue(null);
      vi.spyOn(h.space, "read").mockImplementation(async (...args) => {
        if (phase === "block" && args[0].some((ref) => metadata.has(ref)))
          return await read(...args);
        entered();
        await gate;
        return await read(...args);
      });
      const cursor = reader.range(ALL, "asc"),
        out = new Float64Array(1);
      let settled = false;
      const pending = Promise.resolve(call === "skip" ? cursor.skip(1) : cursor.next(out)).finally(
        () => {
          settled = true;
        },
      );
      await waiting;
      expect(() => cursor.next(out)).toThrow("Only one outstanding");
      expect(() => cursor.skip(1)).toThrow("Only one outstanding");
      if (mode === "cursor") cursor.close();
      else view.release();
      expect(settled).toBe(false);
      expect((grant as import("../memory/governor.js").Grant | null)?.allocated).toBeGreaterThan(0);
      release();
      expect(await pending).toBe(0);
      expect((grant as import("../memory/governor.js").Grant | null)?.held).toBe(0);
      expect((grant as import("../memory/governor.js").Grant | null)?.allocated).toBe(0);
      cursor.close();
      view.release();
    } finally {
      release();
      vi.restoreAllMocks();
      await h.close();
    }
    expect(h.governor.usedBytes).toBe(0);
  },
);

it.each([false, true])("waits at admission and can close the wait (%s)", async (cancel) => {
  const h = new Harness("strict", "memory"),
    p = await h.open();
  let t = 0;
  try {
    await autocommit(p, async (st) => {
      t =
        (
          await st.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(p, (st) => st.table(t).insert(batch(ints([1, 2])), null));
    const view = p.pin(),
      reader = view.index(t, p.catalog.table(t)?.primaryKey ?? 0);
    const held = await h.governor.admit({
      label: "reader pressure",
      minimumBytes: h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes,
      spillable: false,
    });
    expect(held.allocate(held.held)).toBe(true);
    const cursor = reader.range(ALL, "asc"),
      out = new Float64Array(1);
    let settled = false;
    const pending = Promise.resolve(cursor.next(out)).finally(() => {
      settled = true;
    });
    try {
      await yieldTask();
      expect(settled).toBe(false);
      if (cancel) cursor.close();
      else held.close();
      expect(await pending).toBe(cancel ? 0 : 1);
    } finally {
      held.close();
      cursor.close();
      view.release();
    }
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it.each([false, true])(
  "precharges native reader buffers before constructing them (stored %s)",
  async (stored) => {
    const h = new Harness("strict", "memory"),
      p = await h.open();
    let t = 0;
    try {
      await autocommit(p, async (st) => {
        t =
          (
            await st.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false)]),
            })
          ).id ?? 0;
      });
      await autocommit(p, (st) => st.table(t).insert(batch(ints([1, 2])), null));
      if (stored) {
        await p.recycleLog();
        await p.quiesce();
      }
      const view = p.pin(),
        reader = view.index(t, p.catalog.table(t)?.primaryKey ?? 0),
        admit = h.governor.tryAdmit.bind(h.governor);
      let grant: import("../memory/governor.js").Grant | null = null,
        nativeBytes = 0;
      vi.spyOn(h.governor, "tryAdmit").mockImplementation((options) => {
        const got = admit(options);
        if (options.label === "write index range") grant = got;
        return got;
      });
      const ctor = Uint8Array;
      vi.stubGlobal(
        "Uint8Array",
        new Proxy(ctor, {
          construct(target, args) {
            const caller = new Error().stack?.split("\n")[2] ?? "";
            if (caller.includes("/write/range-cursor.") || caller.includes("/write/range-input.")) {
              nativeBytes += Number(args[0]);
              expect(
                grant?.allocated,
                "native payload is paid before allocation",
              ).toBeGreaterThanOrEqual(nativeBytes);
            }
            return Reflect.construct(target, args, target) as object;
          },
        }),
      );
      const cursor = reader.range(ALL, "asc");
      try {
        expect(await cursor.next(new Float64Array(1))).toBe(1);
        expect(nativeBytes).toBeGreaterThan(0);
      } finally {
        cursor.close();
        view.release();
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
      }
    } finally {
      vi.unstubAllGlobals();
      vi.restoreAllMocks();
      await h.close();
    }
    expect(h.governor.usedBytes).toBe(0);
  },
);

it("reaches streaming range states", () => reach.assertReached());

for (const seed of seedsFor("next-write-stream-range-clock", [0xb3cb]))
  it(`slices invisible prefixes and observes close (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4096, max: 16384 }),
        fc.boolean(),
        async (count, cancel) => {
          const delta = new KeyDelta(),
            bytes = new Uint8Array([1]);
          for (let i = 0; i < count; i++) delta.add(bytes, 0, 1, i + 1, 2, false);
          delta.add(bytes, 0, 1, count + 1, 1, false);
          const input = new DeltaRangeInput(delta, 1, ALL, 1);
          let timerRan = false;
          const timer = setTimeout(() => {
            timerRan = true;
            if (cancel) input.close();
          }, 0);
          vi.spyOn(taskClock, "due").mockReturnValue(true);
          // The law concerns a ready timer. A short prefix may finish before a new 1 ms timer is due.
          const readyAt = performance.now() + 4;
          while (performance.now() < readyAt) {
            /* setup: make the timer due before scanning */
          }
          try {
            const next = input.advance();
            expect(next).toBeInstanceOf(Promise);
            expect(await next).toBe(!cancel);
            expect(timerRan).toBe(true);
          } finally {
            clearTimeout(timer);
            input.close();
            delta.releaseCharge();
            vi.restoreAllMocks();
          }
        },
      ),
      { seed, numRuns: 12 },
    );
  });

for (const seed of seedsFor("next-write-stream-unique-corruption", [0xb3cc]))
  it(`refuses a second live row for a unique key (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.uniqueArray(fc.integer({ min: 1, max: 100 }), { minLength: 2, maxLength: 8 }),
        async (values) => {
          const h = new Harness("strict", "memory"),
            p = await h.open();
          let t = 0;
          try {
            await autocommit(p, async (st) => {
              t =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            await autocommit(p, (st) => st.table(t).insert(batch(ints(values)), null));
            const view = p.pin(),
              id = p.catalog.table(t)?.primaryKey ?? 0;
            const delta = view.internalTable(t).tableView.memtables[0]?.deltas.get(id);
            if (delta === undefined) throw new Error("fixture has no memory delta");
            const bytes = key(values[0] ?? 0),
              added = delta.add(bytes, 0, bytes.length, 2, view.bound, false);
            const cursor = view.index(t, id).range(ALL, "asc");
            try {
              await expect(
                (async () => {
                  while (await cursor.next(new Float64Array(1))) {
                    /* drain to the offending key */
                  }
                })(),
              ).rejects.toBeInstanceOf(IndexCorruptionError);
            } finally {
              cursor.close();
              delta.pop(added);
              view.release();
            }
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 24 },
    );
  });

for (const seed of seedsFor("next-write-stream-delta-mutation", [0xb3ca]))
  it(`resumes past leaf splits and arena growth (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 70, max: 300 }), fc.boolean(), async (count, desc) => {
        mutationReach.beginHistory();
        const delta = new KeyDelta(),
          first = new Uint8Array(4),
          view = new DataView(first.buffer);
        for (let i = 0; i < count; i++) {
          view.setUint32(0, i * 2, false);
          delta.add(first, 0, 4, i + 1, 1, false);
        }
        const input = new DeltaRangeInput(delta, 1, ALL, desc ? -1 : 1),
          actual: number[] = [];
        try {
          expect(await input.advance()).toBe(true);
          actual.push(input.rowId);
          const original = delta.entryCount,
            arena = delta.keyArena();
          mutationReach.reach("ordered inserts");
          mutationReach.reach(desc ? "descending" : "ascending");
          for (let i = 0; i < count; i++) {
            view.setUint32(0, (count - i - 1) * 2 + 1, false);
            delta.add(first, 0, 4, count + i + 1, 2, false);
          }
          expect(delta.keyArena()).not.toBe(arena);
          mutationReach.reach("arena growth");
          mutationReach.reach("unordered pending inserts");
          mutationReach.reach("leaf-sized tail");
          // Grow and split both before and after the cursor's bookmark, then roll back part of it.
          for (let i = 0; i < count >>> 1; i++) delta.pop(delta.entryCount - 1);
          mutationReach.reach("rollback while pinned");
          while (await input.advance()) actual.push(input.rowId);
          mutationReach.reach("resume after mutation");
          mutationReach.reach("invisible tail");
          const expected = Array.from({ length: count }, (_, i) => i + 1);
          if (desc) expected.reverse();
          expect(actual).toEqual(expected);
          while (delta.entryCount > original) delta.pop(delta.entryCount - 1);
        } finally {
          input.close();
          delta.releaseCharge();
        }
      }),
      { seed, numRuns: 80 },
    );
  });

it("reaches ordered cursor mutation states", () => mutationReach.assertReached());

for (const seed of seedsFor("next-write-stream-order-accounting", [0xb3cd]))
  it(`funds ordered leaves before native allocation and retries (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 32 }), fc.boolean(), async (width, failNative) => {
        orderReach.beginHistory();
        let paid = 0,
          limit = Infinity,
          calls = 0,
          constructed = 0;
        const delta = new KeyDelta({
          allocate(n) {
            if (paid + n > limit) return false;
            paid += n;
            return true;
          },
          free(n) {
            paid -= n;
            expect(paid).toBeGreaterThanOrEqual(0);
          },
        });
        const high = new Uint8Array(width).fill(2),
          low = new Uint8Array(width).fill(1);
        delta.add(high, 0, width, 1, 1, false);
        const base = paid,
          ctor = Int32Array,
          failed = new Error("native ordered leaf constructor failed");
        let inject = false;
        vi.stubGlobal(
          "Int32Array",
          new Proxy(ctor, {
            construct(target, args) {
              if (new Error().stack?.split("\n")[2]?.includes("/write/ordered-entries.") === true) {
                calls++;
                expect(
                  paid,
                  "ordered leaf native backing has been paid before construction",
                ).toBeGreaterThanOrEqual(base + Number(args[0]) * 4);
                if (inject) {
                  inject = false;
                  throw failed;
                }
                constructed++;
                orderReach.reach("native allocation witness");
              }
              return Reflect.construct(target, args, target) as object;
            },
          }),
        );
        try {
          // A directory can fit, but a single 256-byte native leaf cannot.
          limit = base + 255;
          expect(() => delta.add(low, 0, width, 2, 1, false)).toThrow(MemtableMemoryError);
          expect(calls).toBe(0);
          orderReach.reach("denied before constructor");
          expect(delta.entryCount).toBe(1);
          limit = Infinity;
          const beforeRetry = paid;
          if (failNative) {
            inject = true;
            expect(() => delta.add(low, 0, width, 2, 1, false)).toThrow(failed);
            expect(paid).toBe(beforeRetry);
            orderReach.reach("constructor failure refund");
            expect(delta.entryCount).toBe(1);
          }
          delta.add(low, 0, width, 2, 1, false);
          expect(constructed).toBe(1);
          orderReach.reach("funded retry");
          expect(delta.entryCount).toBe(2);
          delta.pop(1);
          delta.releaseCharge();
          delta.releaseCharge();
          expect(paid).toBe(0);
          orderReach.reach("release once");
        } finally {
          vi.unstubAllGlobals();
          delta.releaseCharge();
        }
      }),
      { seed, numRuns: 40 },
    );
  });

it("keeps locators fetchable across publication, including out-of-line values", async () => {
  const h = new Harness("strict", "memory", 128),
    p = await h.open();
  let t = 0;
  const ids = Array.from({ length: 32 }, (_, i) => 32 - i),
    large = "x".repeat(65537);
  try {
    await autocommit(p, async (st) => {
      t =
        (
          await st.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", TEXT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(p, (st) =>
      st
        .table(t)
        .insert(batch(ints(ids), texts(ids.map((n) => (n === 32 ? large : String(n))))), null),
    );
    const view = p.pin(),
      cursor = view.index(t, p.catalog.table(t)?.primaryKey ?? 0).range(ALL, "asc"),
      out = new Float64Array(7),
      located: number[] = [];
    try {
      expect(await cursor.next(out)).toBe(7);
      located.push(...out);
      await autocommit(p, (st) => st.table(t).insert(batch(ints([999]), texts(["later"])), null));
      await p.recycleLog();
      await p.quiesce();
      for (;;) {
        const count = await cursor.next(out);
        if (!count) break;
        located.push(...out.subarray(0, count));
      }
      expect(located.map((at) => view.internalTable(t).rowId(at))).toEqual(ids);
      expect(await rowsOf(view, t)).toEqual(ids.map((id) => [id, id === 32 ? large : String(id)]));
    } finally {
      cursor.close();
      view.release();
    }
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it("defers disjoint runs until their bounds can affect the next output", async () => {
  const h = new Harness("strict", "memory", 64),
    p = await h.open();
  let t = 0;
  try {
    await autocommit(p, async (st) => {
      t =
        (
          await st.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    for (let n = 0; n < 5; n++) {
      await autocommit(p, (st) =>
        st.table(t).insert(batch(ints(Array.from({ length: 64 }, (_, i) => n * 64 + i))), null),
      );
      await p.recycleLog();
      await p.quiesce();
    }
    const view = p.pin(),
      reader = view.index(t, p.catalog.table(t)?.primaryKey ?? 0);
    expect(reader.table.tableView.runs.get(reader.def.id)?.length).toBe(5);
    const open = vi.spyOn(RunRangeInput.prototype, "open");
    const cursor = reader.range(ALL, "asc");
    try {
      expect(await cursor.next(new Float64Array(1))).toBe(1);
      expect(open).toHaveBeenCalledTimes(1);
    } finally {
      cursor.close();
      view.release();
      vi.restoreAllMocks();
    }
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it("reaches ordered leaf accounting states", () => orderReach.assertReached());

for (const seed of seedsFor("next-write-stream-memory-cancel", [0xb3cf]))
  it(`cancels a skipped memtable prefix without enumerating it (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 4096, max: 16384 }), async (count) => {
        const h = new Harness("strict", "memory"),
          p = await h.open();
        let t = 0;
        try {
          await autocommit(p, async (st) => {
            t =
              (
                await st.changeCatalog({
                  kind: "createTable",
                  table: tableInput("t", [column("id", INT, false)]),
                })
              ).id ?? 0;
          });
          await autocommit(p, (st) => st.table(t).insert(batch(ints([1000000])), null));
          const view = p.pin(),
            id = p.catalog.table(t)?.primaryKey ?? 0;
          await autocommit(p, (st) =>
            st.table(t).insert(batch(ints(Array.from({ length: count }, (_, i) => i))), null),
          );
          const delta = view.internalTable(t).tableView.memtables[0]?.deltas.get(id);
          if (delta === undefined) throw new Error("fixture lost its original live memtable");
          const seq = delta.seq.bind(delta),
            cursor = view.index(t, id).range(ALL, "asc");
          let visited = 0;
          vi.spyOn(delta, "seq").mockImplementation((e) => {
            if (++visited === 128) queueMicrotask(() => cursor.close());
            return seq(e);
          });
          vi.spyOn(taskClock, "due").mockReturnValue(true);
          try {
            expect(await cursor.next(new Float64Array(1))).toBe(0);
            expect(visited).toBe(128);
          } finally {
            cursor.close();
            view.release();
            vi.restoreAllMocks();
          }
        } finally {
          vi.restoreAllMocks();
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 6 },
    );
  }, 120_000);
