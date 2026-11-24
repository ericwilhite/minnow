/**
 * Replay of a crash tail through the native open (owner/native-boot.ts `openNativeOwner`), over
 * generated tails, memory caps, competing memory and outcomes.
 *
 * Law: the governor contract (memory/governor.ts rules 2/4/5/7, PROPOSAL "One memory governor")
 * and docs/engine/memory.mdx "Opening after a crash": below the cap an open completes, waits for
 * a temporary holder (cancellable through the open's signal), or is refused with a typed 53200
 * (`ReplayMemoryError`) whose requirement N exceeds the capacity M it was measured against and is
 * a lower bound of what the open needs; never an untyped failure or a livelock. A refused,
 * cancelled or crashed open leaves the log as it was: every committed row opens at a larger cap.
 *
 * Tails are written through a `Harness` whose flushes never publish (`beforeFlushPublish`), so the
 * rows stay in the log, then abandoned as a crashed process abandons them. The native open runs on
 * its own governor; the test reaches that governor only to add a competing holder or a queued
 * admission once the write owner's minimum is granted, and to watch the replay's growth wait.
 */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { createReach, REPLAY_ADMISSION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import { openNativeOwner, type NativeOwner } from "../owner/native-boot.js";
import type { SimBackend } from "../storage/sim-disk.js";
import { yieldTask } from "../task.js";
import { need } from "../write/bytes.js";
import { ReplayMemoryError } from "../write/engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  tableInput,
  TEXT,
  texts,
  onGroupWait,
  rules,
} from "../write/test-support.js";

const MiB = 1 << 20;
/** The replay's row growth (write/credits.ts `growTo` on the write path's row account). */
const GROWTH = "complete grow write path";

const SHAPES = ["rows", "wide", "commit", "deletes", "index", "gap"] as const;
type Shape = (typeof SHAPES)[number];
type Pressure = "none" | "holder" | "queued";
type Outcome = "complete" | "cancel" | "crash";

interface Case {
  readonly shape: Shape;
  /** Tail size class: about 2 MiB, or about 12 MiB (beyond the owner's idle forecasts). */
  readonly large: boolean;
  readonly capMiB: number;
  readonly pressure: Pressure;
  readonly outcome: Outcome;
}

type Reach = ReturnType<typeof createReach<(typeof REPLAY_ADMISSION_REACH)[number]>>;

/** Table id → row id → text value: what every open must find. */
type Model = Map<number, Map<number, string>>;

const value = (id: number, bytes: number) => String(id).padEnd(bytes, "x");

/**
 * Writes a tail of `shape` and abandons the path (a crashed process). Rows written before the
 * tail are flushed; the tail's are only in the log.
 */
async function writeTail(c: Case, reach: Reach) {
  let holding = false;
  // Small memtables where the shape needs freezes before its tail (row limits; bytes stay below).
  const memtableRows = c.shape === "gap" ? 8 : c.shape === "deletes" ? 64 : undefined;
  const h = new Harness("strict", "sim", memtableRows, undefined, {
    beforeFlushPublish: () => (holding ? new Promise<never>(() => undefined) : Promise.resolve()),
    logRecycleBytes: 1 << 30,
    merges: { mode: "off" },
  });
  const path = await h.open();
  const model: Model = new Map();
  const create = async (name: string, wideKey = false) => {
    let id = 0;
    await autocommit(path, async (s) => {
      id =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput(
              name,
              [column("id", INT, false), column("v", TEXT, false)],
              wideKey ? { primaryKey: [1] } : {},
            ),
          })
        ).id ?? 0;
    });
    model.set(id, new Map());
    return id;
  };
  const insert = async (t: number, ids: number[], bytes: number) => {
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(ids), texts(ids.map((id) => value(id, bytes)))), null),
    );
    for (const id of ids) need(model.get(t)).set(id, value(id, bytes));
  };
  const total = c.large ? 12 * MiB : 2 * MiB;
  const range = (from: number, count: number) => Array.from({ length: count }, (_, k) => from + k);
  switch (c.shape) {
    case "rows": {
      // Multi-row commits of mid-size values over four tables: each memtable stays below its
      // byte limit, so none freezes (a freeze would start a flush, which this tail holds).
      const tables = [await create("a"), await create("b"), await create("c"), await create("d")];
      holding = true;
      const bytes = 16 * 1024;
      for (let k = 0; k * 32 * bytes < total; k++)
        await insert(need(tables[k % tables.length]), range(k * 32, 32), bytes);
      reach.reach("multi-row tail");
      break;
    }
    case "wide": {
      // One large varlen value per row: one 1-row step each.
      const t = await create("w");
      holding = true;
      const bytes = c.large ? 9 * MiB : 2 * MiB;
      await insert(t, [1], bytes);
      reach.reach("large value tail");
      break;
    }
    case "commit": {
      // One commit past 2,048 rows: several replay steps for one logged insert (at most 6 MiB, so
      // its memtable does not freeze).
      const t = await create("c");
      holding = true;
      const count = 3000;
      await insert(t, range(1, count), c.large ? 2048 : 64);
      reach.reach("multi-step commit tail");
      break;
    }
    case "deletes": {
      // Wide-keyed rows flushed first, then a tail of deletes only (tombstones with their keys).
      const t = await create("d", true);
      // Large: twice the other tails, so the tombstones' keys outgrow the owner's idle credit.
      const bytes = 8 * 1024;
      const count = Math.floor((c.large ? 2 * total : total) / bytes);
      for (let from = 0; from < count; from += 64)
        await insert(t, range(from, Math.min(64, count - from)), bytes);
      // A row past them freezes the last full memtable: every row above is flushed, this one not.
      await insert(t, [-1], bytes);
      await path.quiesce();
      const view = path.pin();
      const located: number[] = [];
      try {
        const table = view.internalTable(t);
        for (const segment of table.segments)
          if (segment.kind !== "memory")
            for (let position = 0; position < segment.rowCount; position++)
              located.push(segment.index * 65_536 + position);
      } finally {
        view.release();
      }
      expect(located.length, "every row flushed before the deletes").toBe(count);
      holding = true;
      for (let from = 0; from < located.length; from += 128)
        await autocommit(path, (s) =>
          s.table(t).delete(batch(locators(located.slice(from, from + 128), t)), null),
        );
      for (let id = 0; id < count; id++) need(model.get(t)).delete(id);
      reach.reach("delete-only tail");
      break;
    }
    case "index": {
      // Rows flushed, then CREATE INDEX in the tail: replay backfills it from the stored rows.
      const t = await create("i");
      const bytes = 4 * 1024;
      const count = Math.floor(total / bytes);
      for (let from = 0; from < count; from += 256)
        await insert(t, range(from, Math.min(256, count - from)), bytes);
      await path.quiesce();
      holding = true;
      await autocommit(path, async (s) => {
        await s.changeCatalog({
          kind: "createIndex",
          index: {
            name: "i_v",
            tableId: t,
            columnIds: [need(path.catalog.table(t)?.columns[1]).id],
            unique: false,
            role: "secondary",
          },
        });
      });
      reach.reach("index backfill tail");
      break;
    }
    case "gap": {
      // A transaction stages rows past the small memtable, then rolls back (its flushes publish);
      // the tail's rows then start past the ids it used (a forward gap in the log). Five rows stay
      // below the memtable's row and byte limits, so the held tail never freezes.
      const t = await create("g");
      const tx = await path.begin();
      for (let k = 0; k < 5; k++) {
        const s = tx.statement({ rules });
        const ids = range(1000 + k * 8, 8);
        await s.table(t).insert(batch(ints(ids), texts(ids.map((id) => value(id, 4096)))), null);
        await s.end();
      }
      tx.rollback();
      await path.quiesce();
      holding = true;
      for (let k = 0; k < 5; k++) await insert(t, [k], c.large ? 1536 * 1024 : 256 * 1024);
      if (path.maintenance.stagingRollbacks > 0) reach.reach("staged rollback gap tail");
      break;
    }
  }
  await h.abandon();
  return { h, model };
}

/** Every row of `model`, exactly, in a harness opened at 256 MiB on a fresh governor. */
async function checkRows(h: Harness, model: Model): Promise<void> {
  Object.defineProperty(h, "governor", { value: new Governor(256 * MiB), configurable: true });
  const path = await h.open();
  try {
    const view = path.pin();
    try {
      for (const [t, rows] of model) {
        const found = await rowsOf(view, t);
        expect(new Map(found.map((row) => [Number(row[0]), row[1]])), `table ${String(t)}`).toEqual(
          rows,
        );
      }
    } finally {
      view.release();
    }
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
}

/**
 * Opens `backend` natively at `capMiB` with `pressure` added on the owner's governor once its
 * write owner minimum is granted. `onWait` runs when the replay's row growth waits.
 */
async function nativeOpen(
  h: Harness,
  capMiB: number,
  pressure: Pressure,
  onWait: (release: () => void) => void,
  signal?: AbortSignal,
) {
  const competing: {
    reader: Grant | null;
    queued: Promise<Grant> | null;
    governor: Governor | null;
  } = { reader: null, queued: null, governor: null };
  const queuedAbort = new AbortController();
  let waits: ReturnType<typeof onGroupWait> | null = null;
  const release = () => {
    competing.reader?.close();
    competing.reader = null;
  };
  // eslint-disable-next-line @typescript-eslint/unbound-method -- called with its receiver below
  const admitGroup = Governor.prototype.admitGroup;
  const spy = vi.spyOn(Governor.prototype, "admitGroup").mockImplementation(function (
    this: Governor,
    options,
  ) {
    if (competing.governor === null) {
      // The native owner's governor: its first group admission is the write owner minimum.
      const governor = (competing.governor = this);
      const stop = governor.observeGrants((event) => {
        if (event.label !== "write owner minimum") return;
        stop();
        queueMicrotask(() => {
          const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
          if (pressure === "holder")
            competing.reader = governor.tryAdmit({
              label: "temporary reader",
              minimumBytes: free,
              spillable: false,
            });
          // Reader A leaves the replay room; query B queues for more than A leaves plus what the
          // owner's idle credit can return, so the replay grows while an admission is queued
          // (served as holder growth, ahead of B).
          if (pressure === "queued") {
            competing.reader = governor.tryAdmit({
              label: "reader A",
              minimumBytes: Math.floor(free * 0.45),
              spillable: false,
            });
            const queued = governor.admit({
              label: "queued query B",
              // More than the owner's idle credit could make room for: it stays queued.
              minimumBytes: Math.min(
                governor.capBytes - governor.reserveBytes,
                free - Math.floor(free * 0.45) + 32 * MiB,
              ),
              spillable: false,
              signal: queuedAbort.signal,
            });
            queued.catch(() => undefined);
            competing.queued = queued;
          }
        });
      });
      waits = onGroupWait(governor, GROWTH, () => {
        onWait(release);
      });
    }
    return admitGroup.call(this, options);
  });
  let owner: NativeOwner | null = null;
  let error: unknown = null;
  try {
    owner = await openNativeOwner({
      backend: h.backend,
      memoryLimitBytes: capMiB * MiB,
      ...(signal === undefined ? {} : { signal }),
    });
  } catch (caught) {
    error = caught;
  } finally {
    spy.mockRestore();
  }
  const settle = async () => {
    release();
    queuedAbort.abort();
    const queued = competing.queued;
    if (queued !== null)
      await queued.then(
        (grant) => grant.close(),
        () => undefined,
      );
  };
  return {
    owner,
    error,
    waits: waits as ReturnType<typeof onGroupWait> | null,
    governor: competing.governor,
    settle,
  };
}

/**
 * A cancelled open rejects at once; a read it had in flight returns its buffers when that I/O
 * completes. Waits a bounded number of task turns for that.
 */
async function drained(governor: Governor): Promise<void> {
  for (let turn = 0; turn < 100 && governor.usedBytes > 0; turn++) await yieldTask();
}

/** The capacity `growTo` measures a refusal against: the cap less its reserve. */
const ordinary = (capBytes: number) => {
  const governor = new Governor(capBytes);
  return governor.capBytes - governor.reserveBytes;
};

const EXAMPLES: Array<[Case]> = [
  // The review's P0: a queued admission while the replay grows (a livelock before).
  [{ shape: "rows", large: true, capMiB: 256, pressure: "queued", outcome: "complete" }],
  // One 9 MiB value at 32 MiB: fits (refused before, as 24 MiB).
  [{ shape: "wide", large: true, capMiB: 32, pressure: "none", outcome: "complete" }],
  [{ shape: "deletes", large: true, capMiB: 96, pressure: "holder", outcome: "complete" }],
  [{ shape: "index", large: true, capMiB: 96, pressure: "holder", outcome: "complete" }],
  [{ shape: "gap", large: true, capMiB: 96, pressure: "holder", outcome: "complete" }],
  [{ shape: "commit", large: true, capMiB: 96, pressure: "holder", outcome: "cancel" }],
  [{ shape: "rows", large: true, capMiB: 96, pressure: "holder", outcome: "crash" }],
  [{ shape: "rows", large: true, capMiB: 32, pressure: "none", outcome: "complete" }],
];

const caseArb: fc.Arbitrary<Case> = fc.record({
  shape: fc.constantFrom(...SHAPES),
  large: fc.boolean(),
  capMiB: fc.constantFrom(32, 48, 96, 256),
  pressure: fc.constantFrom<Pressure>("none", "holder", "queued"),
  outcome: fc.constantFrom<Outcome>("complete", "cancel", "crash"),
});

for (const seed of seedsFor("next-replay-admission", [0x70, 0x71]))
  it(`replays generated crash tails through the native open (seed ${String(seed)})`, async () => {
    const reach = createReach("replay admission", REPLAY_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(caseArb, async (c) => {
        reach.beginHistory();
        const { h, model } = await writeTail(c, reach);
        const controller = new AbortController();
        // A holder: the callback's write is invisible to narrowing.
        const crash = { done: false, backstop: false };
        const opened = await nativeOpen(
          h,
          c.capMiB,
          c.pressure,
          (release) => {
            reach.reach("replay waited");
            if (c.outcome === "cancel") {
              controller.abort(new Error("open cancelled while replaying"));
              // Backstop: an open that ignores its signal would wait for this holder forever.
              // The holder leaves later, and the open must not have completed (checked below).
              setTimeout(() => {
                crash.backstop = true;
                release();
              }, 2000);
            } else if (c.outcome === "crash") {
              crash.done = true;
              h.disk.powerLoss();
              release();
            } else release();
          },
          controller.signal,
        );
        onTestFinished(opened.settle);
        const waited = (opened.waits?.waits ?? 0) > 0;
        const { error, owner, governor } = opened;
        if (crash.done) {
          // A dead process's open: whatever it did, the store restarts and keeps its log.
          reach.reach("crashed mid-wait");
          await opened.settle();
          if (owner !== null) await owner.close().catch(() => undefined);
          (h.backend as SimBackend).restart();
        } else if (owner !== null) {
          // Opened: with every committed row (checked below), and nothing held after close.
          if (c.outcome === "cancel")
            expect(waited, "an open aborted while its replay waited does not complete").toBe(false);
          if (c.pressure === "holder" && waited) reach.reach("opened beside a temporary holder");
          if (c.pressure === "queued") reach.reach("opened beside a queued admission");
          await opened.settle();
          await owner.close();
          expect(need(governor).usedBytes, "the closed owner holds nothing").toBe(0);
        } else if (c.outcome === "cancel" && waited) {
          expect(error, "the cancelled open rejects").toBeInstanceOf(Error);
          expect(crash.backstop, "it rejects while the holder still holds its room").toBe(false);
          expect(error).not.toBeInstanceOf(ReplayMemoryError);
          reach.reach("cancelled mid-wait");
          await opened.settle();
          await drained(need(governor));
          expect(need(governor).usedBytes, "the cancelled open holds nothing").toBe(0);
        } else {
          // Refused: typed, N above M, and N a lower bound of what the open needs.
          expect(error, `${c.shape} at ${String(c.capMiB)} MiB`).toBeInstanceOf(ReplayMemoryError);
          const refused = error as ReplayMemoryError;
          expect(refused).toMatchObject({ sqlState: "53200", code: "MINNOW_MEMORY_ADMISSION" });
          expect(refused.bytes, "N exceeds the capacity it was refused at").toBeGreaterThan(
            refused.capacity,
          );
          expect(refused.message).toMatch(
            /^Opening this database needs at least [\d.]+ MiB to replay its recently written rows, more than its memory cap allows for them \([\d.]+ MiB\)\. The data is intact; open it with a larger memory cap\.$/,
          );
          reach.reach("refused 53200");
          await opened.settle();
          expect(need(governor).usedBytes, "the refused open holds nothing").toBe(0);
          // Lower bound: a cap whose ordinary capacity is below N refuses too, without pressure.
          let below = Math.floor((refused.bytes - MiB) / MiB) * MiB;
          while (ordinary(below) >= refused.bytes) below -= MiB;
          if (ordinary(below) > 16 * MiB) {
            const again = await nativeOpen(h, below / MiB, "none", (release) => {
              release();
            });
            await again.settle();
            if (again.owner !== null) await again.owner.close();
            expect(
              again.error,
              `a cap below N (${String(below / MiB)} MiB) refuses too`,
            ).toBeInstanceOf(ReplayMemoryError);
            reach.reach("refusal is a lower bound");
          }
        }
        // The log as it was: a generous open finds every committed row.
        await checkRows(h, model);
        reach.reach("every committed row after");
      }),
      {
        seed,
        numRuns: 64,
        endOnFailure: true,
        examples: EXAMPLES,
      },
    );
    reach.assertReached();
  }, 900_000);

/**
 * A replay that fits is not refused (review of #70, P1/P3). One 9 MiB value replays into one
 * 16 MiB arena (a single row resizes its arena once: write/columns.ts `#putBytes`), and recovery
 * keeps only the log appender's header of the commit window. Opened without competition, it
 * peaks at about 22 MiB of the governor; at a 28 MiB cap (24.5 MiB ordinary) it opens with every
 * row. It was refused there, asking 27.9 MiB (a 1-row step charged a doubling overlap it never
 * makes) or 27.8 MiB (the whole 6 MiB commit window held through recovery).
 */
it("replays one 9 MiB value at a 28 MiB cap", async () => {
  // writeTail records its shape; this pinned case asserts no reach.
  const reach = createReach("replay admission (pinned)", REPLAY_ADMISSION_REACH);
  reach.beginHistory();
  const { h, model } = await writeTail(
    { shape: "wide", large: true, capMiB: 28, pressure: "none", outcome: "complete" },
    reach,
  );
  const opened = await nativeOpen(h, 28, "none", (release) => {
    release();
  });
  expect(opened.error, "the open completes").toBeNull();
  const owner = need(opened.owner);
  expect(owner.governor.peakBytes).toBeLessThanOrEqual(
    owner.governor.capBytes - owner.governor.reserveBytes,
  );
  await opened.settle();
  await owner.close();
  await checkRows(h, model);
}, 120_000);
