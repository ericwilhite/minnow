/**
 * The hash join's lifecycle under ruling R23.2/R23.3: the table build, the runtime-filter build,
 * probe output and unmatched-row emission run in task-yielding slices (a zero-delay timer runs
 * before the phase completes), resident and spilled; the abort signal is checked before each
 * finish and after downstream backpressure. The sliced jobs themselves yield per store chunk.
 */
import { describe, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { INT, TEXT, batchesOf, type Row } from "./join-fixtures.js";
import { JoinRuntimeFilter } from "./join-filter.js";
import { KeyLayout, KeyScratch } from "./join-keys.js";
import { JoinTable } from "./join-table.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinKind, type HashJoinSpec } from "./join.js";
import type { Operator, PushResult } from "./pipeline.js";
import { Slice } from "./slices.js";

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
