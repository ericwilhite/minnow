/** AFTER children append behind the old frame; every continuation starts without a read borrow. */
import { afterEach, expect, it, vi } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_AFTER_QUEUE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillFile, MemorySpillService } from "../spill/memory-spill.js";
import { SpillCorruptionError } from "../spill/contract.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import {
  TriggerAfterQueue,
  TRIGGER_AFTER_QUEUE_BYTES,
  type TriggerAfterEvent,
} from "./trigger-after-queue.js";
// Capture the production method once: a timed-out body's old callbacks cannot recurse through
// another history's spy. Every history also closes its own queue and signals idempotently.
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the observed IO below.
const releasePage = SpillIo.prototype.releasePage;
afterEach(() => vi.restoreAllMocks());
for (const seed of seedsFor("next-trigger-after-queue", [0xb7ce, 0xb7cf]))
  it(`spills FIFO append-during-drain with fixed credit seed ${String(seed)}`, async () => {
    const reach = createReach("trigger AFTER queue", TRIGGER_AFTER_QUEUE_REACH);
    let active = new Set<Uint8Array>();
    vi.spyOn(SpillIo.prototype, "releasePage").mockImplementation(function (this: SpillIo, page) {
      if ("bytes" in page && page.bytes instanceof Uint8Array) active.delete(page.bytes);
      releasePage.call(this, page);
    });
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 513 }),
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (count, children, wide, cancel) => {
            reach.beginHistory();
            active = new Set();
            const governor = new Governor(8 * 1024 * 1024),
              grant = await governor.admit({
                label: "AFTER root",
                minimumBytes: 512 * 1024,
                spillable: true,
              }),
              charge = new Charge(grant),
              pool = new Pool(charge, TRIGGER_AFTER_QUEUE_BYTES),
              service = new MemorySpillService(),
              signal = new AbortController(),
              reason = new Error("cancel AFTER continuation"),
              files = new SpillFiles(service, 1, charge),
              pages = new PageWriter(pool, 16384),
              io = new SpillIo(
                pool,
                files,
                pages,
                { grant, queryId: 1, signal: signal.signal },
                () => signal.signal.throwIfAborted(),
              ),
              queue = new TriggerAfterQueue(io, service, { rootId: 1, frameId: 2 });
            const create = service.create.bind(service);
            service.create = async (...args) => {
              const file = await create(...args),
                read = file.read.bind(file);
              file.read = async (ref) => {
                const bytes = await read(ref);
                active.add(bytes);
                return bytes;
              };
              return file;
            };
            const credit = governor.capBytes - governor.reserveBytes - governor.usedBytes,
              competing = await governor.admit({
                label: "competing credit",
                minimumBytes: credit,
                spillable: false,
              });
            expect(competing.allocate(credit)).toBe(true);
            const held = grant.held,
              base = wide ? 2 ** 52 : 1;
            const event = (row: number, budget: number, id = base + row): TriggerAfterEvent => ({
              kind: row % 2 ? "trigger" : "referential",
              command: { rootId: 1, id: 3, frameId: 2 },
              tableId: 4,
              event: row % 3 === 0 ? "insert" : row % 3 === 1 ? "update" : "delete",
              images: { spoolId: 5, row },
              budget,
              id,
            });
            const expected: TriggerAfterEvent[] = [],
              actual: TriggerAfterEvent[] = [];
            let closing: Promise<void> | null = null;
            const close = () =>
              (closing ??= (async () => {
                signal.abort(reason);
                await queue.close();
                await files.closeAll();
                pages.close();
                pool.close();
                grant.close();
                competing.close();
              })());
            try {
              for (let row = 0; row < count; row++) {
                const e = event(row, 1);
                expected.push(e);
                await queue.append(e);
                expect(queue.retainedEpochs).toBeLessThanOrEqual(2);
              }
              if (count >= 64) {
                expect(service.bytesInUse).toBeGreaterThan(0);
                reach.reach("spilled pages");
              }
              const action = queue.drain(async (e) => {
                expect(active.size, "no retained native read page at continuation").toBe(0);
                expect(pool.owes).toBe(false);
                actual.push(e);
                if (cancel) {
                  signal.abort(reason);
                  signal.signal.throwIfAborted();
                }
                if (e.kind === "referential" && e.budget === 1 && e.images.row % 7 === 0) {
                  for (let child = 0; child < children; child++) {
                    const next = event(
                      count + e.images.row * children + child,
                      0,
                      base + count + e.images.row * children + child,
                    );
                    expected.push(next);
                    await queue.append(next);
                  }
                  if (children) reach.reach("append during drain");
                }
                expect(queue.retainedEpochs).toBeLessThanOrEqual(2);
              });
              if (cancel && count) {
                await expect(action).rejects.toBe(reason);
                reach.reach("cancel continuation");
              } else {
                await action;
                expect(actual).toEqual(expected);
                expect(queue.queued).toBe(0);
                reach.reach("FIFO order");
              }
              expect(grant.held).toBe(held);
              expect(pool.loans).toBe(0);
              if (wide && count) {
                expect(actual[0]?.id).toBeGreaterThanOrEqual(2 ** 52);
                reach.reach("wide IDs exact");
              }
              reach.reach(
                count === 0 ? "empty" : count === 1 ? "one row" : "multiple blocks or rows",
              );
              reach.reach("fixed metadata and ordinary credit");
            } finally {
              await close();
            }
            expect(service.bytesInUse).toBe(0);
            expect(governor.usedBytes).toBe(0);
            expect(active.size).toBe(0);
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [0, 0, false, false],
            [1, 2, true, false],
            [129, 3, false, false],
            [8192, 1, true, false],
            [65, 2, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      vi.restoreAllMocks();
    }
  }, 120_000);

it("refuses foreign frame packets while cleaning allocated queue state", async () => {
  const governor = new Governor(8 * 1024 * 1024),
    grant = await governor.admit({
      label: "AFTER faults",
      minimumBytes: 512 * 1024,
      spillable: true,
    }),
    pool = new Pool(new Charge(grant), TRIGGER_AFTER_QUEUE_BYTES),
    service = new MemorySpillService(),
    files = new SpillFiles(service, 7, new Charge(grant)),
    pages = new PageWriter(pool, 16384),
    io = new SpillIo(pool, files, pages, { grant, queryId: 7 }, () => undefined),
    queue = new TriggerAfterQueue(io, service, { rootId: 1, frameId: 1 });
  try {
    const e: TriggerAfterEvent = {
      kind: "trigger",
      command: { rootId: 1, id: 1, frameId: 2 },
      images: { spoolId: 1, row: 0 },
      tableId: 1,
      event: "insert",
      budget: 1,
      id: 1,
    };
    await expect(queue.append(e)).rejects.toThrow("foreign frame");
  } finally {
    await queue.close();
    await files.closeAll();
    pages.close();
    pool.close();
    grant.close();
  }
  expect(service.bytesInUse).toBe(0);
  expect(governor.usedBytes).toBe(0);
});

for (const fault of [
  "pending read",
  "pending append",
  "pending continuation",
  "corrupt",
  "close error",
] as const)
  it(`fences AFTER ownership across ${fault}`, async () => {
    const governor = new Governor(8 * 1024 * 1024),
      grant = await governor.admit({
        label: "AFTER IO",
        minimumBytes: 512 * 1024,
        spillable: true,
      }),
      pool = new Pool(new Charge(grant), TRIGGER_AFTER_QUEUE_BYTES),
      service = new MemorySpillService(),
      files = new SpillFiles(service, 8, new Charge(grant)),
      pages = new PageWriter(pool, 16384),
      io = new SpillIo(pool, files, pages, { grant, queryId: 8 }, () => undefined),
      queue = new TriggerAfterQueue(io, service, { rootId: 1, frameId: 1 });
    const originalCreate = service.create.bind(service);
    let unblock: () => void = () => undefined,
      entered: () => void = () => undefined,
      armed = true,
      pending = false;
    const gate = new Promise<void>((r) => {
        unblock = r;
      }),
      seen = new Promise<void>((r) => {
        entered = r;
      });
    const wait = async () => {
      pending = true;
      entered();
      await gate;
      pending = false;
    };
    service.create = async (...args) => {
      const file = await originalCreate(...args);
      if (!(file instanceof MemorySpillFile)) throw new Error("Expected memory fault file");
      const read = file.read.bind(file),
        append = file.append.bind(file),
        close = file.close.bind(file);
      file.read = async (ref) => {
        if (armed) {
          if (fault === "corrupt") {
            armed = false;
            file.corrupt(ref, 0);
          } else if (fault === "pending read") {
            armed = false;
            await wait();
          }
        }
        return read(ref);
      };
      file.append = async (bytes) => {
        if (armed && fault === "pending append") {
          armed = false;
          await wait();
        }
        return append(bytes);
      };
      file.close = async () => {
        expect(pending, "file close must follow pending IO/continuation").toBe(false);
        await close();
        if (fault === "close error") throw new Error("injected AFTER close error");
      };
      return file;
    };
    const e = (row: number): TriggerAfterEvent => ({
      kind: "trigger",
      command: { rootId: 1, id: 1, frameId: 1 },
      tableId: 2,
      event: "insert",
      images: { spoolId: 3, row },
      budget: 1,
      id: 4,
    });
    let work: Promise<void> | null = null;
    try {
      for (let row = 0; row < 63; row++) await queue.append(e(row));
      if (fault === "pending append") work = queue.append(e(63));
      else {
        await queue.append(e(63));
        work = queue.drain(async () => {
          if (fault === "pending continuation") await wait();
        });
      }
      work.catch(() => undefined);
      if (
        fault === "pending read" ||
        fault === "pending append" ||
        fault === "pending continuation"
      ) {
        await seen;
        let closed = false;
        const close = queue.close().then(() => {
          closed = true;
        });
        await Promise.resolve();
        await Promise.resolve();
        expect(closed).toBe(false);
        expect(pool.used).toBeGreaterThan(0);
        unblock();
        await expect(work).rejects.toThrow("closed");
        await close;
      } else if (fault === "corrupt") {
        await expect(work).rejects.toBeInstanceOf(SpillCorruptionError);
        await queue.close();
      } else {
        // Draining itself retires the consumed epoch and must surface its close failure.
        await expect(work).rejects.toThrow("AFTER epoch cleanup failed");
        await expect(queue.close()).rejects.toThrow("AFTER queue cleanup failed");
      }
    } finally {
      unblock();
      await work?.catch(() => undefined);
      await queue.close().catch(() => undefined);
      await files.closeAll();
      pages.close();
      pool.close();
      grant.close();
    }
    expect(service.bytesInUse).toBe(0);
    expect(governor.usedBytes).toBe(0);
  });
