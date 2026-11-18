/** R25 / PublishDelta: unpublished output never changes either durable root. A stopped
 * owner may leave provisional catalog/FTS bytes and a partly drained writer; ordinary
 * open recovers the committed DROP and rows, and exact inventory/ownership remains valid.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, ROOT_DROP_ABORT_CRASH_REACH, type Reach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { setupFts, openFtsHarness, ftsParts } from "../owner/fts-transaction.test-support.js";
import { DURABLE_FORMATS } from "../durable-format.js";
import { SimBackend, SimDisk, SimulatedCrash } from "../storage/sim-disk.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  texts,
  tableInput,
  rowsOf,
} from "./test-support.js";

type Coverage = Reach<(typeof ROOT_DROP_ABORT_CRASH_REACH)[number]>;
interface Shape {
  readonly width: number;
  readonly salt: number;
  readonly seed: number;
}
interface StoppedImage {
  readonly steps: number;
  readonly epoch: number;
  readonly files: ReadonlyArray<{
    readonly name: string;
    readonly current: Uint8Array;
    readonly durable: Uint8Array;
  }>;
}
function snapshotStoppedDisk(disk: SimDisk): StoppedImage {
  return {
    steps: disk.steps,
    epoch: disk.epoch,
    files: Array.from(disk.files, ([name, file]) => {
      expect(file.pending).toHaveLength(0);
      expect(file.queued).toHaveLength(0);
      return { name, current: file.current.slice(), durable: file.durable.slice() };
    }),
  };
}
/** Exact comparison without allocating/deep-walking one JS entry per file byte. */
function firstDifference(actual: Uint8Array | undefined, expected: Uint8Array): number {
  if (actual?.length !== expected.length) return -2;
  for (let at = 0; at < expected.length; at++) if (actual[at] !== expected[at]) return at;
  return -1;
}
function expectStoppedDisk(disk: SimDisk, image: StoppedImage): void {
  expect(disk.steps).toBe(image.steps);
  expect(disk.epoch).toBe(image.epoch);
  expect([...disk.files.keys()]).toEqual(image.files.map((file) => file.name));
  for (const saved of image.files) {
    const file = disk.files.get(saved.name);
    expect(firstDifference(file?.current, saved.current), `${saved.name} current`).toBe(-1);
    expect(firstDifference(file?.durable, saved.durable), `${saved.name} durable`).toBe(-1);
    expect(file?.pending).toHaveLength(0);
    expect(file?.queued).toHaveLength(0);
  }
}
type Cut = "abort entry" | "before release" | "after release";
const cuts: readonly Cut[] = ["abort entry", "before release", "after release"];
/** Each stop is a witnessed boundary of the actual abandoned writer, not a step
 * count borrowed from another run with different background metadata work. */
async function history(shape: Shape, cut: Cut, reach: Coverage): Promise<void> {
  let armed = false,
    inAbort = false,
    fired = false,
    target = 0,
    entered!: () => void,
    resume!: () => void;
  const arrived = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    resume = resolve;
  });
  const disk = new SimDisk({ seed: shape.seed ^ shape.salt });
  const h = new Harness("strict", "sim", shape.width, disk, {
    merges: { mode: "off" },
    rootSelectionBoundary: async (phase, kind) => {
      if (armed && phase === "before" && kind === "flush") {
        armed = false;
        entered();
        await gate;
      }
    },
  });
  let mayStop = false,
    stoppedOwner = false;
  const stopped: { image: StoppedImage | undefined } = { image: undefined };
  const stop = (): never => {
    mayStop = false;
    fired = true;
    stoppedOwner = true;
    disk.powerLoss();
    expect(disk.crashes).toBe(1);
    stopped.image = snapshotStoppedDisk(disk);
    throw new SimulatedCrash();
  };
  const owned = new Set<number>();
  const backendOpen = h.backend.open.bind(h.backend);
  h.backend.open = async (...args) => {
    const session = await backendOpen(...args);
    const release = session.releaseData.bind(session);
    session.releaseData = async (ranges) => {
      const matches = inAbort && mayStop && ranges.some((range) => owned.has(range.offset));
      if (matches) reach.reach("owned extent release reached");
      if (matches && cut === "before release") stop();
      await release(ranges);
      if (matches && mayStop && cut === "after release") stop();
    };
    return session;
  };
  const magic = new Set<number>();
  let sawAbort = false;
  h.onSpace = (space) => {
    const begin = space.beginWrite.bind(space);
    space.beginWrite = (label, options) => {
      const writer = begin(label, options);
      if (label !== `flush t${String(target)}`) return writer;
      const open = writer.open.bind(writer),
        abort = writer.abort.bind(writer);
      writer.open = (capacity) => {
        const builder = open(capacity),
          append = builder.append.bind(builder);
        builder.append = async (parts) => {
          for (const part of parts)
            if (part.length >= 4)
              magic.add(
                new DataView(part.buffer, part.byteOffset, part.byteLength).getUint32(0, true),
              );
          return append(parts);
        };
        return builder;
      };
      writer.abort = async () => {
        if (!mayStop) return abort();
        sawAbort = true;
        owned.clear();
        for (const extent of writer.extents) owned.add(extent.offset);
        expect(magic.has(DURABLE_FORMATS.catalogRoot.magic)).toBe(true);
        expect(magic.has(DURABLE_FORMATS.ftsJournal.magic)).toBe(true);
        expect(writer.extents.length).toBeGreaterThan(0);
        reach.reach("provisional catalog output");
        reach.reach("provisional FTS output");
        if (cut === "abort entry") stop();
        inAbort = true;
        try {
          return await abort();
        } finally {
          inAbort = false;
        }
      };
      return writer;
    };
  };
  const closeOwner = async () => {
    try {
      await h.close();
    } catch (error) {
      if (!stoppedOwner) throw error;
      // A dead file handle can reject the final log acknowledgement. Cleanup still
      // releases every owner; an unrelated close error is not silently discarded.
      expect(error).toMatchObject({ name: "InvalidStateError" });
    }
    expect(h.governor.usedBytes).toBe(0);
    reach.reach("zero charges");
  };
  const expected = [[`alpha ${String(shape.salt)}`], [`beta ${String(shape.salt)}`]];
  try {
    let path = await setupFts(h);
    await autocommit(path, (s) => s.table(1).insert(batch(texts([expected[0]?.[0] ?? ""])), null));
    await autocommit(path, async (s) => {
      target =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("target", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    for (const ids of [Array.from({ length: shape.width }, (_, i) => i - shape.width), [-100]])
      await autocommit(path, (s) => s.table(target).insert(batch(ints(ids)), null));
    await path.quiesce();
    // FTS preparation may need publication. Finish this commit before suspending
    // the target flush; its journal checkpoint is still written by that root.
    await autocommit(path, (s) => s.table(1).insert(batch(texts([expected[1]?.[0] ?? ""])), null));
    armed = true;
    magic.clear();
    for (const ids of [Array.from({ length: shape.width }, (_, i) => i + 1), [100]])
      await autocommit(path, (s) => s.table(target).insert(batch(ints(ids)), null));
    await arrived;
    const previous = h.space.root?.slice();
    const rootFiles = ["root-0", "root-1"].map((name) => ({
      name,
      bytes: disk.files.get(name)?.current.slice(),
    }));
    expect(previous).toBeDefined();
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: target }));
    mayStop = true;
    resume();
    // A stopped process has no observable response. Settle its local tasks only
    // to release the test harness; recovery assertions use the persisted images.
    await Promise.allSettled([path.quiesce()]);
    expect(sawAbort).toBe(true);
    expect(fired).toBe(true);
    expect(disk.crashes).toBe(1);
    expect(h.space.root).toEqual(previous);
    reach.reach(cut);
    reach.reach("old root unchanged");
    const stoppedImage = stopped.image;
    if (stoppedImage === undefined) throw new Error("Missing stopped disk image");
    for (const root of rootFiles) expect(disk.files.get(root.name)?.current).toEqual(root.bytes);
    reach.reach("physical roots unchanged");
    expectStoppedDisk(disk, stoppedImage);
    await closeOwner();
    // Draining old JS owners is test cleanup, not recovery: it must not repair
    // persisted state before the new process sees the stopped images.
    expectStoppedDisk(disk, stoppedImage);
    reach.reach("no persistent work after stop");
    if (!(h.backend instanceof SimBackend)) throw new Error("Expected simulated file backend");
    h.backend.restart();
    path = await openFtsHarness(h);
    stoppedOwner = false;
    expect(path.catalog.table(target)).toBeUndefined();
    let pin = path.pin();
    try {
      expect(await rowsOf(pin, 1)).toEqual(expected);
    } finally {
      pin.release();
    }
    reach.reach("committed DROP and rows recovered");
    // Retry publication from the recovered WAL, including its retained FTS history.
    await path.recycleLog();
    expect(path.state().root.tables.some((table) => table.tableId === target)).toBe(false);
    expect((await ftsParts(h)).entries.length).toBeGreaterThan(0);
    await h.space.audit();
    reach.reach("retry and FTS history readable");
    const after = `gamma ${String(shape.salt)}`;
    await autocommit(path, (s) => s.table(1).insert(batch(texts([after])), null));
    path = await h.reopen();
    pin = path.pin();
    try {
      expect(await rowsOf(pin, 1)).toEqual([...expected, [after]]);
    } finally {
      pin.release();
    }
    await h.space.audit();
    reach.reach("exact inventory and continued writes");
  } finally {
    armed = false;
    mayStop = false;
    resume();
    await closeOwner();
  }
}
for (const seed of seedsFor("next-root-drop-abort-crash", [704, 705]))
  it(`recovers abandoned-writer cleanup boundaries seed ${String(seed)}`, async () => {
    const reach = createReach("DROP abort process recovery", ROOT_DROP_ABORT_CRASH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(4, 7),
        fc.integer({ min: 0, max: 31 }),
        async (width, salt) => {
          reach.beginHistory();
          const shape = { width, salt, seed };
          for (const cut of cuts) await history(shape, cut, reach);
          reach.reach("all explicit boundaries");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [4, 0],
          [7, 31],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
