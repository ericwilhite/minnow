/**
 * The hash join's lifecycle under ruling R23.2/R23.3: the table build, the runtime-filter build,
 * probe output and unmatched-row emission run in task-yielding slices (a zero-delay timer runs
 * before the phase completes), resident and spilled; the abort signal is checked before each
 * finish and after downstream backpressure. The sliced jobs themselves yield per store chunk.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { INT, TEXT, batchesOf, type Row } from "./join-fixtures.js";
import { JoinRuntimeFilter } from "./join-filter.js";
import {
  KEY_STEP_BYTES,
  KeyLayout,
  KeyScratch,
  KeyWork,
  extractKeys,
  extractKeysJob,
} from "./join-keys.js";
import * as hashing from "../hash/xxhash32.js";
import { XxHash32Stream } from "../hash/xxhash32.js";
import { vi } from "vitest";
import { JoinTable } from "./join-table.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinKind, type HashJoinSpec } from "./join.js";
import type { Operator, PushResult } from "./pipeline.js";
import { Slice, runToEnd } from "./slices.js";
import { TaskClock, taskClock } from "./pipeline.js";

const FREE = { allocate: () => true, free: () => undefined };

function spec(
  kind: HashJoinKind,
  types: readonly PhysicalType[],
  extra: Partial<HashJoinSpec> = {},
): HashJoinSpec {
  return {
    kind,
    probeTypes: types,
    buildTypes: types,
    probeKeys: [0],
    buildKeys: [0],
    nullSafe: [false],
    residual: null,
    publishes: [],
    ...extra,
  };
}

/** Counts rows (no copies); blocks every `blockEvery` pushes; `onPush` sees each push. */
class CountSink implements Operator {
  rows = 0;
  pushes = 0;
  finished = false;
  aborted = false;
  constructor(
    readonly blockEvery = 0,
    readonly onPush: () => void = () => undefined,
  ) {}
  push(batch: Batch): PushResult {
    this.rows += batch.selection === null ? batch.length : batch.selection.count;
    this.pushes++;
    this.onPush();
    return this.blockEvery > 0 && this.pushes % this.blockEvery === 0 ? "blocked" : "ready";
  }
  /** Resolves at once (a microtask): no time passes, so no slice comes due while blocked. */
  ready(): Promise<void> {
    return Promise.resolve();
  }
  finish(): void {
    this.finished = true;
  }
  abort(): void {
    this.aborted = true;
  }
}

async function setup(s: HashJoinSpec, options: { slack?: number; signal?: AbortSignal } = {}) {
  const minimum = hashJoinMinimumBytes(s);
  const cap =
    options.slack === undefined
      ? 512 << 20
      : Math.max(
          minimum + options.slack + (1 << 20),
          Math.ceil(((minimum + options.slack) * 8) / 7) + 1,
        );
  const governor = new Governor(cap);
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "join", minimumBytes: minimum, spillable: true });
  const join = new HashJoin(s, {
    grant,
    spill,
    queryId: 4,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  });
  return { join, governor, spill };
}

async function pushAll(operator: Operator, types: readonly PhysicalType[], rows: readonly Row[]) {
  for (const batch of batchesOf(types, rows))
    if (operator.push(batch) === "blocked") await operator.ready();
}

/** Runs `phase` with a zero-delay timer set just before it; whether the timer ran first. */
async function servicedDuring(phase: () => unknown): Promise<boolean> {
  let serviced = false;
  const timer = setTimeout(() => {
    serviced = true;
  }, 0);
  await phase();
  clearTimeout(timer);
  return serviced;
}

const sparse = (n: number): Row[] =>
  Array.from({ length: n }, (_, i): Row => [(i * 1_000_003) % 2_000_000_011, i]);

describe("R23.2/R23.3: join phases run in task-yielding slices", () => {
  const builds: Array<{ name: string; s: HashJoinSpec; rows: () => Row[] }> = [
    { name: "hash table (int keys)", s: spec("inner", [INT, INT]), rows: () => sparse(400_000) },
    {
      name: "hash table (text keys)",
      s: spec("inner", [TEXT, INT]),
      rows: () => Array.from({ length: 200_000 }, (_, i): Row => [`k${String(i)}`, i]),
    },
    {
      name: "direct table",
      s: spec("inner", [INT, INT]),
      rows: () => Array.from({ length: 600_000 }, (_, i): Row => [i, i]),
    },
    {
      name: "mark join groups",
      s: spec("mark", [INT, INT, INT], {
        probeKeys: [0, 1],
        buildKeys: [0, 1],
        nullSafe: [false, false],
        correlated: 1,
      }),
      rows: () => Array.from({ length: 300_000 }, (_, i): Row => [i % 1000, i, i]),
    },
    {
      name: "with a runtime filter",
      s: spec("inner", [INT, INT], { publishes: [{ id: 1, key: 0 }] }),
      rows: () => sparse(400_000),
    },
  ];
  for (const b of builds)
    it(`a zero-delay timer runs before finishBuild completes: ${b.name}`, async () => {
      const { join } = await setup(b.s);
      await pushAll(join.build, b.s.buildTypes, b.rows());
      expect(await servicedDuring(() => join.build.finish())).toBe(true);
      const sink = new CountSink();
      await join.probe(sink).finish();
      expect(sink.finished).toBe(true);
      if (b.s.publishes.length > 0) expect(join.runtimeFilters).toHaveLength(1);
    }, 60_000);

  it("a zero-delay timer runs before a heavy probe push is ready", async () => {
    // 2,000 build rows and 2,048 probe rows on one key: 4 M pairs from one push.
    const s = spec("inner", [INT, INT]);
    const { join } = await setup(s);
    await pushAll(
      join.build,
      s.buildTypes,
      Array.from({ length: 2000 }, (_, i): Row => [7, i]),
    );
    await join.build.finish();
    const sink = new CountSink();
    const probe = join.probe(sink);
    const [batch] = [
      ...batchesOf(
        s.probeTypes,
        Array.from({ length: 2048 }, (_, i): Row => [7, i]),
      ),
    ];
    if (batch === undefined) throw new Error("no batch");
    const serviced = await servicedDuring(async () => {
      if (probe.push(batch) === "blocked") await probe.ready();
    });
    expect(serviced).toBe(true);
    await probe.finish();
    expect(sink.rows).toBe(2000 * 2048);
  }, 60_000);

  it("a zero-delay timer runs before finishProbe completes: unmatched build rows, downstream blocked with a resolved ready (R23.4)", async () => {
    const s = spec("right", [INT, INT]);
    const { join } = await setup(s);
    await pushAll(join.build, s.buildTypes, sparse(400_000));
    await join.build.finish();
    const sink = new CountSink(1);
    const probe = join.probe(sink);
    expect(await servicedDuring(() => probe.finish())).toBe(true);
    expect(sink.rows).toBe(400_000);
  }, 60_000);

  it("many short probe pushes, each blocked with a resolved ready, still yield a task (R23.4)", async () => {
    // Each push is far shorter than a slice; a slice restarted at every push never comes due.
    const s = spec("inner", [INT, INT]);
    const { join } = await setup(s);
    await pushAll(
      join.build,
      s.buildTypes,
      Array.from({ length: 64 }, (_, i): Row => [i, i]),
    );
    await join.build.finish();
    const sink = new CountSink(1);
    const probe = join.probe(sink);
    const batch = [
      ...batchesOf(
        s.probeTypes,
        Array.from({ length: 64 }, (_, i): Row => [i, i]),
      ),
    ];
    const serviced = await servicedDuring(async () => {
      const start = performance.now();
      // At least 200 ms of pushes (a slice is 4 ms).
      while (performance.now() - start < 200)
        for (const b of batch) if (probe.push(b) === "blocked") await probe.ready();
    });
    expect(serviced).toBe(true);
    await probe.finish();
  }, 60_000);

  it("a zero-delay timer runs during a probe whose every output blocks with a resolved ready (R23.4)", async () => {
    const s = spec("inner", [INT, INT]);
    const { join } = await setup(s);
    const rows = Array.from({ length: 2048 }, (_, i): Row => [i % 4, i]);
    await pushAll(join.build, s.buildTypes, rows);
    await join.build.finish();
    // Every probe row matches 512 build rows: many output batches per probe batch.
    const sink = new CountSink(1);
    const probe = join.probe(sink);
    const probeRows = Array.from({ length: 4096 }, (_, i): Row => [i % 4, i]);
    expect(
      await servicedDuring(async () => {
        await pushAll(probe, s.probeTypes, probeRows);
        await probe.finish();
      }),
    ).toBe(true);
    expect(sink.rows).toBe(4096 * 512);
  }, 120_000);

  it("a zero-delay timer runs before finishProbe completes: unmatched build rows", async () => {
    const s = spec("right", [INT, INT]);
    const { join } = await setup(s);
    await pushAll(join.build, s.buildTypes, sparse(400_000));
    await join.build.finish();
    const sink = new CountSink();
    const probe = join.probe(sink);
    expect(await servicedDuring(() => probe.finish())).toBe(true);
    expect(sink.rows).toBe(400_000);
  }, 60_000);

  for (const kind of ["inner", "full"] as const)
    it(`a zero-delay timer runs before finishProbe completes: spilled ${kind} join`, async () => {
      const s = spec(kind, [INT, TEXT]);
      const { join, governor, spill } = await setup(s, { slack: 256 * 1024 });
      const rows = (n: number): Row[] =>
        Array.from({ length: n }, (_, i): Row => [
          (i * 7919) % n,
          `v${String(i)}-${"x".repeat(i % 40)}`,
        ]);
      await pushAll(join.build, s.buildTypes, rows(60_000));
      await join.build.finish();
      const sink = new CountSink();
      const probe = join.probe(sink);
      await pushAll(probe, s.probeTypes, rows(60_000));
      expect(join.profile.spilledBytes).toBeGreaterThan(0);
      expect(await servicedDuring(() => probe.finish())).toBe(true);
      expect(sink.rows).toBe(60_000);
      await join.closed;
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    }, 60_000);
});

describe("R23.2/R23.3: the join checks the abort signal", () => {
  it("an abort before finishBuild rejects", async () => {
    const controller = new AbortController();
    const s = spec("inner", [INT, INT]);
    const { join, governor } = await setup(s, { signal: controller.signal });
    await pushAll(join.build, s.buildTypes, sparse(1000));
    controller.abort(new Error("cancel requested"));
    await expect(Promise.resolve().then(() => join.build.finish())).rejects.toThrow(
      "cancel requested",
    );
    await join.closed;
    expect(governor.usedBytes).toBe(0);
  });

  for (const spilled of [false, true])
    it(`an abort before finishProbe rejects (${spilled ? "spilled" : "resident"} right join)`, async () => {
      const controller = new AbortController();
      const s = spec("right", [INT, TEXT]);
      const { join, governor, spill } = await setup(s, {
        signal: controller.signal,
        ...(spilled ? { slack: 256 * 1024 } : {}),
      });
      const rows: Row[] = Array.from({ length: spilled ? 60_000 : 100 }, (_, i): Row => [
        i,
        `v${String(i)}-${"x".repeat(i % 40)}`,
      ]);
      await pushAll(join.build, s.buildTypes, rows);
      await join.build.finish();
      const sink = new CountSink();
      const probe = join.probe(sink);
      await pushAll(probe, s.probeTypes, rows.slice(0, 10));
      expect(join.profile.spilledBytes > 0).toBe(spilled);
      controller.abort(new Error("cancel requested"));
      await expect(Promise.resolve().then(() => probe.finish())).rejects.toThrow(
        "cancel requested",
      );
      expect(sink.finished).toBe(false);
      await join.closed;
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    }, 60_000);

  it("unmatched-row emission checks the signal after downstream backpressure", async () => {
    // Three output batches, so the emission fits one slice: only the check after ready() sees it.
    const controller = new AbortController();
    const s = spec("right", [INT, INT]);
    const { join, governor } = await setup(s, { signal: controller.signal });
    await pushAll(join.build, s.buildTypes, sparse(5000));
    await join.build.finish();
    const sink = new CountSink(1, () => {
      controller.abort(new Error("cancelled while blocked"));
    });
    const probe = join.probe(sink);
    await expect(Promise.resolve().then(() => probe.finish())).rejects.toThrow(
      "cancelled while blocked",
    );
    expect(sink.pushes).toBe(1);
    await join.closed;
    expect(governor.usedBytes).toBe(0);
  });
});

describe("R23.2: the join's sliced jobs yield per store chunk", () => {
  // A slice that is always due: a job yields at every check, so the yields count the checks.
  const count = <T>(
    job: Generator<Promise<void> | void, T, void>,
  ): { yields: number; value: T } => {
    let yields = 0;
    for (;;) {
      const step = job.next();
      if (step.done === true) return { yields, value: step.value };
      yields++;
    }
  };
  const store = (rows: number, values: (i: number) => number) => {
    const s = new ColumnStore([INT], FREE);
    const [batch] = [
      ...batchesOf(
        [INT],
        Array.from({ length: rows }, (_, i): Row => [values(i)]),
        { batchRows: rows },
      ),
    ];
    if (batch === undefined) throw new Error("no batch");
    expect(s.append(batch.columns, null, 0, rows)).toBe(rows);
    return s;
  };

  for (const [name, values] of [
    ["hash", (i: number) => (i * 1_000_003) % 2_000_000_011],
    ["direct", (i: number) => i],
  ] as const)
    it(`JoinTable.buildJob (${name})`, () => {
      const s = store(10 * 2048, values);
      const layout = new KeyLayout([INT], [false]);
      const scratch = new KeyScratch(layout, 2048, FREE);
      const { yields, value } = count(
        JoinTable.buildJob(s, [0], layout, 0, FREE, scratch, new Slice(0)),
      );
      expect(value).not.toBeNull();
      expect(value?.kind).toBe(name === "direct" ? "direct" : "words");
      expect(yields).toBeGreaterThanOrEqual(10);
    });

  it("JoinRuntimeFilter.buildJob", () => {
    const s = store(10 * 2048, (i) => (i * 1_000_003) % 2_000_000_011);
    const { yields, value } = count(
      JoinRuntimeFilter.buildJob(1, s, 0, false, FREE, 2048, new Slice(0)),
    );
    expect(value).not.toBeNull();
    expect(yields).toBeGreaterThanOrEqual(20);
  });
});

describe("review B7-p2c-1 #3: join work is bounded by key bytes", () => {
  // Two alternating 65,537-byte keys: each 2,048-row chunk holds ~128 MiB of key bytes.
  const wide = (i: number) => "x".repeat(65_536) + String(i % 2);
  const rows: Row[] = Array.from({ length: 2048 }, (_, i): Row => [wide(i), i]);
  const types = [TEXT, INT];

  for (const [name, keyColumns] of [
    ["raw keys", [0]],
    ["encoded keys", [0, 1]],
  ] as const)
    it(`the table build's first segment is short (${name}; reviewer: 5,642 ms)`, () => {
      const store = new ColumnStore(types, FREE);
      for (const batch of batchesOf(types, rows, { batchRows: 8 }))
        expect(store.append(batch.columns, null, 0, batch.length)).toBe(batch.length);
      const layout = new KeyLayout(
        keyColumns.map((c) => types[c] ?? TEXT),
        keyColumns.map(() => false),
      );
      const scratch = new KeyScratch(layout, 2048, FREE);
      // A private task clock: the loop below is the runner, each yield beginning a task.
      const slice = new Slice(4, new TaskClock(() => performance.now()));
      const job = JoinTable.buildJob(store, [...keyColumns], layout, 0, FREE, scratch, slice);
      const start = performance.now();
      let step = job.next();
      const first = performance.now() - start;
      let segments = 1;
      while (step.done !== true) {
        // A runner begins a task at each yield (runSliced's yield does).
        slice.clock.begin();
        step = job.next();
        segments++;
      }
      expect(first).toBeLessThan(100);
      // ~128 MiB of key bytes in 4 ms slices: many segments. The count is wall time, so it
      // falls as the work gets cheaper (encoded keys are no longer escaped: review B7-p2c-3 #4,
      // 72 segments on a loaded host); the per-step work bound is the B7-p2c-2 suite below.
      expect(segments).toBeGreaterThan(20);
      expect(step.value?.inserted).toBe(2048);
      step.value?.close();
      store.close();
    }, 120_000);

  it("a dictionary probe batch hashes and looks up each entry once (reviewer: 3,879 ms)", () => {
    const store = new ColumnStore(types, FREE);
    for (const batch of batchesOf(types, rows.slice(0, 2), { batchRows: 2 }))
      store.append(batch.columns, null, 0, batch.length);
    const layout = new KeyLayout([TEXT], [false]);
    const scratch = new KeyScratch(layout, 2048, FREE);
    const table = JoinTable.build(store, [0], layout, 0, FREE, scratch);
    const [batch] = [...batchesOf(types, rows, { dictionary: true })];
    if (batch === undefined || table === null) throw new Error("setup");
    const start = performance.now();
    extractKeys(batch.columns.slice(0, 1), null, 0, 2048, scratch, true);
    const heads = new Int32Array(2048);
    expect(table.lookup(scratch, 2048, heads)).toBe(2048);
    expect(performance.now() - start).toBeLessThan(100);
    table.close();
    store.close();
  });

  it("a flat probe batch past one step of key bytes goes through in parts, yielding", async () => {
    const s = spec("inner", types);
    const { join } = await setup(s);
    await pushAll(join.build, types, rows.slice(0, 2));
    await join.build.finish();
    const sink = new CountSink();
    const probe = join.probe(sink);
    // 64 flat rows of 64 KiB keys: 4 MiB of key bytes in one batch.
    const [batch] = [...batchesOf(types, rows.slice(0, 64), { batchRows: 64 })];
    if (batch === undefined) throw new Error("no batch");
    const serviced = await servicedDuring(async () => {
      const start = performance.now();
      const result = probe.push(batch);
      expect(performance.now() - start).toBeLessThan(100);
      expect(result).toBe("blocked");
      await probe.ready();
    });
    expect(serviced).toBe(true);
    await probe.finish();
    expect(sink.rows).toBe(64);
  }, 60_000);

  it("a build batch of wide keys is hashed in byte steps when the table is built", async () => {
    // A resident raw build only stores rows at arrival; its keys are hashed (and compared) by
    // the table build, in steps of the key-byte budget.
    const s = spec("inner", types);
    const { join } = await setup(s);
    const [batch] = [...batchesOf(types, rows.slice(0, 64), { batchRows: 64 })];
    if (batch === undefined) throw new Error("no batch");
    if (join.build.push(batch) === "blocked") await join.build.ready();
    expect(await servicedDuring(() => join.build.finish())).toBe(true);
    const sink = new CountSink();
    const probe = join.probe(sink);
    await pushAll(probe, types, rows.slice(0, 2));
    await probe.finish();
    expect(sink.rows).toBe(64);
  }, 60_000);
});

describe("review B7-p2c-2: every byte of key work is in a step", () => {
  const types2 = [TEXT, INT];
  /** An always-due slice; the runners below reset it at each yield, as `runSliced` does. */
  // Always due, on a private task clock the runners below begin a task on at each yield.
  const due = new Slice(0, new TaskClock(() => performance.now()));
  /** Runs a job with an always-due slice, returning the most work any one step did. */
  const stepWork = <T>(
    job: Generator<Promise<void> | void, T, void>,
    work: KeyWork,
  ): { max: number; value: T } => {
    let max = 0;
    for (;;) {
      const before = work.total;
      const step = job.next();
      max = Math.max(max, work.total - before);
      if (step.done === true) return { max, value: step.value };
      due.clock.begin();
    }
  };
  // A step may end up to one chunk (4 KiB) past the budget, and a hash step spends two seeds.
  const BOUND = KEY_STEP_BYTES + 2 * 4096;

  it("forced collisions: build and lookup stay within the budget per step (review probe)", () => {
    // Every key hashes to 42 with both seeds: 64 distinct 64 KiB keys, one chain.
    const one = vi.spyOn(hashing, "xxHash32").mockReturnValue(42);
    const digest = vi.spyOn(XxHash32Stream.prototype, "digest").mockReturnValue(42);
    try {
      const keyRows: Row[] = Array.from({ length: 64 }, (_, i): Row => [
        "x".repeat(65_530) + String(i).padStart(6, "0"),
        i,
      ]);
      const store = new ColumnStore(types2, FREE);
      for (const batch of batchesOf(types2, keyRows))
        store.append(batch.columns, null, 0, batch.length);
      const layout = new KeyLayout([TEXT], [false]);
      const scratch = new KeyScratch(layout, 2048, FREE);
      const work = new KeyWork();
      const built = stepWork(
        JoinTable.buildJob(store, [0], layout, 0, FREE, scratch, due, {}, work),
        work,
      );
      expect(built.max).toBeLessThanOrEqual(BOUND);
      const table = built.value;
      if (table === null) throw new Error("no table");
      expect(table.inserted).toBe(64);
      const [batch] = [...batchesOf(types2, keyRows)];
      if (batch === undefined) throw new Error("no batch");
      const probeWork = new KeyWork();
      const extracted = stepWork(
        extractKeysJob(batch.columns.slice(0, 1), null, 0, 64, scratch, probeWork, due),
        probeWork,
      );
      expect(extracted.value).toBe(64);
      const heads = new Int32Array(64);
      const looked = stepWork(table.lookupJob(scratch, 64, heads, probeWork, due), probeWork);
      expect(looked.max).toBeLessThanOrEqual(BOUND);
      expect(looked.value).toBe(64);
      expect([...heads]).toEqual(keyRows.map((_, i) => i + 1));
      table.close();
      store.close();
    } finally {
      one.mockRestore();
      digest.mockRestore();
    }
  }, 60_000);

  it("long keys carry a second hash (64 bits before any byte compare); short ones do not", () => {
    const layout = new KeyLayout([TEXT], [false]);
    const scratch = new KeyScratch(layout, 4, FREE);
    const long = "z".repeat(100);
    const [batch] = [
      ...batchesOf(types2, [
        [long, 1],
        ["short", 2],
      ]),
    ];
    if (batch === undefined) throw new Error("no batch");
    extractKeys(batch.columns.slice(0, 1), null, 0, 2, scratch, true);
    const bytes = new TextEncoder().encode(long);
    expect(scratch.hashes2[0]).toBe(hashing.xxHash32(bytes, 0x6a09_e667));
    expect(scratch.hashes2[1]).toBe(0);
    // The resumable path stores the same pair.
    const again = new KeyScratch(layout, 4, FREE);
    const work = new KeyWork();
    stepWork(extractKeysJob(batch.columns.slice(0, 1), null, 0, 2, again, work, due), work);
    expect([...again.hashes.subarray(0, 2)]).toEqual([...scratch.hashes.subarray(0, 2)]);
    expect([...again.hashes2.subarray(0, 2)]).toEqual([...scratch.hashes2.subarray(0, 2)]);
  });

  it("a 10 MiB key is hashed and compared across many steps", () => {
    const big = "y".repeat(10 << 20);
    const store = new ColumnStore(types2, FREE);
    for (const batch of batchesOf(
      types2,
      [
        [big, 1],
        [big, 2],
      ],
      { batchRows: 1 },
    ))
      store.append(batch.columns, null, 0, batch.length);
    const layout = new KeyLayout([TEXT], [false]);
    const scratch = new KeyScratch(layout, 2048, FREE);
    const work = new KeyWork();
    let steps = 0;
    const job = JoinTable.buildJob(store, [0], layout, 0, FREE, scratch, due, {}, work);
    let max = 0;
    let firstMs = 0;
    for (;;) {
      const before = work.total;
      const start = performance.now();
      const step = job.next();
      if (steps === 0) firstMs = performance.now() - start;
      max = Math.max(max, work.total - before);
      steps++;
      due.clock.begin();
      if (step.done === true) {
        expect(step.value?.inserted).toBe(2);
        step.value?.close();
        break;
      }
    }
    // Two hashes of 10 MiB each (two seeds), and one 10 MiB comparison: over 200 steps.
    expect(max).toBeLessThanOrEqual(BOUND);
    expect(steps).toBeGreaterThan(200);
    // The bound is the work per step above; wall time is only a loose check on a loaded host.
    expect(firstMs).toBeLessThan(100);
    store.close();
  }, 60_000);

  it("a dictionary probe batch hashes each entry once through the operator (review probe)", async () => {
    const x = "x".repeat(65_536);
    const keyRows: Row[] = Array.from({ length: 64 }, (_, i): Row => [x + String(i % 2), i]);
    const s = spec("semi", types2);
    const { join, governor } = await setup(s);
    await pushAll(join.build, types2, keyRows.slice(0, 2));
    await join.build.finish();
    const one = vi.spyOn(hashing, "xxHash32");
    const digest = vi.spyOn(XxHash32Stream.prototype, "digest");
    const sink = new CountSink();
    const probe = join.probe(sink);
    for (const batch of batchesOf(types2, keyRows, { dictionary: true }))
      if (probe.push(batch) === "blocked") await probe.ready();
    // Two entries, two seeds each: four hashes, not one per row.
    const hashes = one.mock.calls.length + digest.mock.calls.length;
    one.mockRestore();
    digest.mockRestore();
    await probe.finish();
    expect(sink.rows).toBe(64);
    expect(hashes).toBeLessThanOrEqual(4);
    await join.closed;
    expect(governor.usedBytes).toBe(0);
  }, 60_000);
});

describe("review B7-p2c-3: no key work past a due step", () => {
  // Always due, on a private task clock the runners below begin a task on at each yield.
  const due = new Slice(0, new TaskClock(() => performance.now()));
  /** The most key work any one step of `job` did (a runner resetting `due` at each yield). */
  const stepWork = <T>(job: Generator<Promise<void> | void, T, void>, work: KeyWork) => {
    let max = 0;
    for (;;) {
      const before = work.total;
      const step = job.next();
      max = Math.max(max, work.total - before);
      if (step.done === true) return { max, value: step.value };
      due.clock.begin();
    }
  };
  // A step may end one chunk (4 KiB) past the budget, plus a slot or two.
  const BOUND = KEY_STEP_BYTES + 8192;

  it("spend's 'yield now' must be honoured: going on without a new slice throws", () => {
    const work = new KeyWork(100);
    expect(work.spend(60, due)).toBe(false);
    expect(work.spend(60, due)).toBe(true);
    expect(() => work.spend(1, due)).toThrow("ignored spend's yield");
    // A slice that is not due never asks for a yield (and owes nothing).
    const later = new Slice(1e9);
    const relaxed = new KeyWork(100);
    for (let i = 0; i < 10; i++) expect(relaxed.spend(60, later)).toBe(false);
    // After the runner's reset the work goes on.
    due.clock.begin();
    expect(work.spend(1, due)).toBe(false);
  });

  // The reviewer's probe, generated over the carried remainder of the budget and the mismatch
  // position: 8 colliding keys (both hashes forced to 42) one byte apart at `at`, inserted (each
  // compared with the chain so far) and looked up (a missing key compared with all of them).
  it("a mismatch that exhausts the budget yields: insertion and lookup, every remainder", () => {
    const one = vi.spyOn(hashing, "xxHash32").mockReturnValue(42);
    const digest = vi.spyOn(XxHash32Stream.prototype, "digest").mockReturnValue(42);
    try {
      const n = KEY_STEP_BYTES - 12;
      const positions = [0, 1, 4095, 4096, n >> 1, n - 4097, n - 2, n - 1];
      let cases = 0;
      for (const at of positions) {
        const key = (c: string) => "x".repeat(at) + c + "x".repeat(n - at - 1);
        const keyRows: Row[] = Array.from({ length: 8 }, (_, i): Row => [key(String(i)), i]);
        for (const left of [1, 12, 13, 4096, at + 1, at + 2, n - at, KEY_STEP_BYTES]) {
          const store = new ColumnStore([TEXT, INT], FREE);
          for (const batch of batchesOf([TEXT, INT], keyRows))
            store.append(batch.columns, null, 0, batch.length);
          const layout = new KeyLayout([TEXT], [false]);
          const scratch = new KeyScratch(layout, 2048, FREE);
          const work = new KeyWork();
          work.left = Math.max(1, left);
          const built = stepWork(
            JoinTable.buildJob(store, [0], layout, 0, FREE, scratch, due, {}, work),
            work,
          );
          expect(built.max, `insert at ${String(at)} left ${String(left)}`).toBeLessThanOrEqual(
            BOUND,
          );
          const table = built.value;
          if (table === null) throw new Error("no table");
          const [probe] = [...batchesOf([TEXT, INT], [[key("z"), 99]])];
          if (probe === undefined) throw new Error("no probe");
          extractKeys([probe.columns[0] as never], null, 0, 1, scratch);
          const lookupWork = new KeyWork();
          lookupWork.left = Math.max(1, left);
          const heads = new Int32Array(1);
          const looked = stepWork(table.lookupJob(scratch, 1, heads, lookupWork, due), lookupWork);
          expect(heads[0]).toBe(0);
          expect(looked.max, `lookup at ${String(at)} left ${String(left)}`).toBeLessThanOrEqual(
            BOUND,
          );
          table.close();
          store.close();
          cases++;
        }
      }
      expect(cases).toBe(64);
    } finally {
      one.mockRestore();
      digest.mockRestore();
    }
  }, 120_000);
});

describe("review B7-p2c-3 #2: mark joins hash and compare in steps", () => {
  const types3 = [TEXT, INT, INT];
  // The reviewer's operator probes: a 10 MiB text key and a nullable int key, build (x, NULL),
  // probe (x, 1): the mark is NULL. No one-shot hash may cover more than a step's bytes.
  for (const correlated of [0, 1])
    it(`no synchronous whole-key hash (correlated ${String(correlated)})`, async () => {
      const x = "x".repeat(10 << 20);
      const s = spec("mark", types3, {
        probeKeys: [0, 1],
        buildKeys: [0, 1],
        nullSafe: [false, false],
        correlated,
      });
      const { join, governor, spill } = await setup(s);
      await pushAll(join.build, types3, [[x, null, 1]]);
      await join.build.finish();
      const spy = vi.spyOn(hashing, "xxHash32");
      const marks: Array<boolean | null> = [];
      const sink = {
        push(batch: Batch): PushResult {
          for (let i = 0; i < batch.length; i++) {
            const mark = batch.columns[3];
            const data = mark?.form === "flat" ? mark.data : null;
            const valid =
              mark?.form === "flat" &&
              (mark.validity === null || ((mark.validity[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0);
            marks.push(valid && data?.layout === "bool" ? (data.values[i] ?? 0) === 1 : null);
          }
          return "ready" as const;
        },
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      };
      const probe = join.probe(sink);
      try {
        await pushAll(probe, types3, [[x, 1, 11]]);
        await probe.finish();
        const sizes = spy.mock.calls.map((c) => (c[3] ?? c[0].length) - (c[2] ?? 0));
        expect(Math.max(0, ...sizes)).toBeLessThanOrEqual(KEY_STEP_BYTES);
        expect(marks).toEqual([null]);
        await join.closed;
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
      } finally {
        spy.mockRestore();
      }
    }, 120_000);

  it("rejected candidates are checkpointed: the walk yields at its deadline checks", async () => {
    // 600,000 build rows in one implicit group (uncorrelated); the probe's second key is NULL,
    // so every build row is a candidate, and every one is rejected (its first key differs).
    const s = spec("mark", [INT, INT], {
      probeKeys: [0, 1],
      buildKeys: [0, 1],
      nullSafe: [false, false],
      correlated: 0,
    });
    const { join, governor } = await setup(s);
    const rows = 600_000;
    await pushAll(
      join.build,
      [INT, INT],
      Array.from({ length: rows }, (_, i): Row => [i + 1, i]),
    );
    await join.build.finish();
    const sink = new CountSink();
    const probe = join.probe(sink);
    // A clock that moves 10 ms per reading: every deadline check is due, so the tasks begun
    // (yields) count the checks the walk makes. Without a checkpoint per candidate the walk
    // makes none: a handful of yields from the other phases.
    // (From the real now: the task's start is a real reading.)
    let t = performance.now();
    const clock = vi.spyOn(performance, "now").mockImplementation(() => (t += 10));
    const before = taskClock.epoch;
    try {
      await pushAll(probe, [INT, INT], [[-5, null]]);
      await probe.finish();
    } finally {
      clock.mockRestore();
    }
    const slices = taskClock.epoch - before;
    // Each step's budget covers KEY_STEP_BYTES / 16 candidates.
    expect(slices).toBeGreaterThanOrEqual(Math.floor((rows * 16) / KEY_STEP_BYTES));
    expect(sink.rows).toBe(1);
    await join.closed;
    expect(governor.usedBytes).toBe(0);
  }, 60_000);
});

describe("review B7-p2c-3 #4: encoded join keys copy text once, in steps", () => {
  const types2 = [TEXT, INT];
  // Always due, on a private task clock the runners below begin a task on at each yield.
  const due = new Slice(0, new TaskClock(() => performance.now()));

  for (const zero of [false, true])
    it(`a 10 MiB ${zero ? "zero-filled " : ""}text key: one copy, never escaped, in bounded steps`, () => {
      const text = (zero ? "\0" : "x").repeat(10 << 20);
      const [batch] = [...batchesOf(types2, [[text, 1]])];
      if (batch === undefined) throw new Error("no batch");
      const scratch = new KeyScratch(new KeyLayout(types2, [false, false]), 2048, FREE);
      const work = new KeyWork();
      const job = extractKeysJob(batch.columns, null, 0, 1, scratch, work, due);
      let max = 0;
      let steps = 0;
      for (;;) {
        const before = work.total;
        const step = job.next();
        max = Math.max(max, work.total - before);
        steps++;
        if (step.done === true) {
          expect(step.value).toBe(1);
          break;
        }
        due.clock.begin();
      }
      // The int's encoding (5 bytes), a header and a length (5), the text as stored: zeros are
      // not doubled (escaping is for order, not equality).
      expect(scratch.encoded?.writer.length).toBe((10 << 20) + 10);
      // Copying (10 MiB) and both hashes (2 × 10 MiB): every step within the budget.
      expect(max).toBeLessThanOrEqual(KEY_STEP_BYTES + 8192);
      expect(steps).toBeGreaterThan((3 * (10 << 20)) / (KEY_STEP_BYTES + 8192));
      // The synchronous form encodes the same bytes.
      const sync = new KeyScratch(new KeyLayout(types2, [false, false]), 2048, FREE);
      expect(extractKeys(batch.columns, null, 0, 1, sync, true)).toBe(1);
      // (Buffer.compare: a deep equality over 10 MiB would take minutes.)
      const mine = scratch.encoded?.key(0) ?? new Uint8Array(0);
      const theirs = sync.encoded?.key(0) ?? new Uint8Array(1);
      expect(Buffer.compare(mine, theirs)).toBe(0);
      expect(sync.hashes[0]).toBe(scratch.hashes[0]);
    }, 60_000);

  // Review B7-p2c-4 (its probe): every copy counts, buffer growth included. Each step's bytes
  // copied by Uint8Array.prototype.set (the writer's pieces, and any regrowth of the buffer
  // copying the written prefix) must stay within the budget.
  it("a 10 MiB key's copies, buffer growth included, stay within each step's budget", () => {
    const [batch] = [...batchesOf(types2, [["x".repeat(10 << 20), 1]])];
    if (batch === undefined) throw new Error("no batch");
    const scratch = new KeyScratch(new KeyLayout(types2, [false, false]), 1, FREE);
    const work = new KeyWork();
    const job = extractKeysJob(batch.columns, null, 0, 1, scratch, work, due);
    let copied = 0;
    let most = 0;
    let largest = 0;
    // eslint-disable-next-line @typescript-eslint/unbound-method -- called with `this` below
    const set = Uint8Array.prototype.set;
    const spy = vi.spyOn(Uint8Array.prototype, "set").mockImplementation(function (
      this: Uint8Array,
      source: ArrayLike<number>,
      offset?: number,
    ) {
      copied += source.length;
      largest = Math.max(largest, source.length);
      set.call(this, source, offset);
    });
    try {
      for (;;) {
        copied = 0;
        const step = job.next();
        most = Math.max(most, copied);
        if (step.done === true) break;
        due.clock.begin();
      }
    } finally {
      spy.mockRestore();
    }
    expect(most).toBeLessThanOrEqual(KEY_STEP_BYTES + 8192);
    expect(largest).toBeLessThanOrEqual(KEY_STEP_BYTES + 8192);
    expect(scratch.encoded?.writer.length).toBe((10 << 20) + 10);
  }, 60_000);

  it("a batch whose keys the grant refuses whole encodes the leading rows that fit", () => {
    // Two 1 MiB keys; the charge holds one of them (plus the scratch), not both.
    let held = 0;
    const charge = {
      allocate: (bytes: number) => {
        if (held + bytes > 1.5 * (1 << 20)) return false;
        held += bytes;
        return true;
      },
      free: (bytes: number) => {
        held -= bytes;
      },
    };
    const rows: Row[] = [
      ["a".repeat(1 << 20), 1],
      ["b".repeat(1 << 20), 2],
    ];
    const [batch] = [...batchesOf(types2, rows)];
    if (batch === undefined) throw new Error("no batch");
    const scratch = new KeyScratch(new KeyLayout(types2, [false, false]), 2, charge);
    const done = runToEnd(
      extractKeysJob(batch.columns, null, 0, 2, scratch, new KeyWork(), due),
      due,
    );
    expect(done).toBe(1);
    expect(scratch.requiredBytes).toBeGreaterThan(2 << 20);
    expect(scratch.encoded?.writer.length).toBe((1 << 20) + 10);
  });

  // Law: two encoded keys are equal exactly when the tuples are equal under SQL equality (the
  // module comment of join-keys.ts). Generated tuples of (text, text, int) with NULLs, empty
  // strings, and 0x00 / 0x01 bytes (the null and value headers) at every boundary, where a
  // concatenation without lengths would confuse ("a\u0001b", "") with ("a", "b\u0001").
  for (const seed of seedsFor("next-join-encoded-keys", [0x3e4, 0x77a1]))
    it(`equal encodings iff equal tuples (seed ${String(seed)})`, () => {
      const types3 = [TEXT, TEXT, INT];
      const layout = new KeyLayout(types3, [true, true, true]);
      const piece = fc.constantFrom(
        "",
        "\0",
        "\u0001",
        "a",
        "\0a",
        "a\0",
        "a\u0001",
        "\u0001b",
        "ab",
        "b\0a",
      );
      const value = fc.option(
        fc.array(piece, { maxLength: 3 }).map((parts) => parts.join("")),
        { nil: null },
      );
      const tuple = fc.tuple(
        value,
        value,
        fc.option(fc.integer({ min: -2, max: 2 }), { nil: null }),
      );
      fc.assert(
        fc.property(fc.array(tuple, { minLength: 2, maxLength: 24 }), (tuples) => {
          const rows = tuples.map((t): Row => [...t]);
          const [batch] = [...batchesOf(types3, rows)];
          if (batch === undefined) return;
          const scratch = new KeyScratch(layout, 2048, FREE);
          const work = new KeyWork();
          runToEnd(extractKeysJob(batch.columns, null, 0, rows.length, scratch, work, due), due);
          const keys = rows.map((_, k) => Array.from(scratch.encoded?.key(k) ?? []).join(","));
          for (let i = 0; i < rows.length; i++)
            for (let j = 0; j < rows.length; j++) {
              const same = JSON.stringify(rows[i]) === JSON.stringify(rows[j]);
              expect(
                keys[i] === keys[j],
                `${JSON.stringify(rows[i])} vs ${JSON.stringify(rows[j])}`,
              ).toBe(same);
              if (same) expect(scratch.hashes[i]).toBe(scratch.hashes[j]);
            }
        }),
        { seed, numRuns: 300 },
      );
    });
});
