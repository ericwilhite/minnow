/** A queue continuation may allocate/append: its prior spill read must already be retired. */
import { afterEach, expect, it, vi } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_QUEUE_READ_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import { RecursiveTape } from "./recursive-tape.js";
import type { Vector } from "../vector/vector.js";
import { F64, numbers } from "./kernels/test-support.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- called with the original IO.
const releasePage = SpillIo.prototype.releasePage;
afterEach(() => vi.restoreAllMocks());
for (const seed of seedsFor("next-trigger-queue-read", [0xb7d0, 0xb7d1]))
  it(`releases native read/loan before continuation seed ${String(seed)}`, async () => {
    const reach = createReach("AFTER read contract", TRIGGER_QUEUE_READ_REACH);
    let active = new Set<Uint8Array>();
    vi.spyOn(SpillIo.prototype, "releasePage").mockImplementation(function (this: SpillIo, page) {
      if ("bytes" in page && page.bytes instanceof Uint8Array) active.delete(page.bytes);
      releasePage.call(this, page);
    });
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 16 }),
          fc.integer({ min: 1, max: 8 }),
          fc.boolean(),
          async (pagesCount, rows, wide) => {
            reach.beginHistory();
            active = new Set();
            const governor = new Governor(8 * 1024 * 1024),
              grant = await governor.admit({
                label: "AFTER read",
                minimumBytes: 512 * 1024,
                spillable: true,
              }),
              charge = new Charge(grant),
              pool = new Pool(charge, 128 * 1024),
              service = new MemorySpillService(),
              files = new SpillFiles(service, 4, charge),
              pages = new PageWriter(pool, 16384),
              io = new SpillIo(pool, files, pages, { grant, queryId: 4 }, () => undefined),
              tape = new RecursiveTape(io, 1);
            expect(pool.allocate(64)).toBe(true);
            const copy = new Float64Array(8);
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
            const expected: number[] = [],
              actual: unknown[] = [];
            let scratch = 0,
              slack = 0;
            let competing: Awaited<ReturnType<Governor["admit"]>> | undefined;
            try {
              const base = wide ? 2 ** 52 : 0;
              for (let p = 0; p < pagesCount; p++) {
                const values = Array.from({ length: rows }, (_, r) => base + p * rows + r);
                expected.push(...values);
                await tape.append([numbers(F64, values)], rows);
              }
              scratch = pool.room - 8;
              expect(pool.allocate(scratch)).toBe(true);
              slack = grant.held - grant.allocated;
              expect(grant.allocate(slack)).toBe(true);
              const credit = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              competing = await governor.admit({
                label: "competing root",
                minimumBytes: credit,
                spillable: false,
              });
              expect(competing.allocate(credit)).toBe(true);
              let copied = 0;
              const capture = (columns: readonly Vector[], n: number) => {
                const c = columns[0];
                if (c?.form !== "flat" || c.data.layout !== "f64" || n > copy.length)
                  throw new Error("Invalid scalar queue block");
                copy.set(c.data.values.subarray(0, n));
                copied = n;
              };
              const visit = async () => {
                expect(active.size, "no native page retained into a continuation").toBe(0);
                expect(pool.owes, "no read progress held into a continuation").toBe(false);
                for (let r = 0; r < copied; r++) actual.push(copy[r]);
                await Promise.resolve();
                return true;
              };
              // On the parent, the available borrowed-page API models the unsafe copy+continuation
              // design. It fails the ownership assertions, not import/setup. Current native queues
              // use the explicit release-before-continuation API.
              const candidate = tape as {
                eachCopied?: (
                  types: Array<typeof F64>,
                  copy: typeof capture,
                  onVisit: typeof visit,
                ) => Promise<void>;
              };
              if (candidate.eachCopied) await candidate.eachCopied([F64], capture, visit);
              else
                await tape.each([F64], async (columns, n) => {
                  capture(columns, n);
                  return visit();
                });
              expect(actual).toEqual(expected);
              expect(pool.loans).toBeGreaterThan(0);
              reach.reach("actual competing-credit loan");
              reach.reach("progress returned before continuation");
              reach.reach(pagesCount > 1 ? "directory carry" : "one page");
              reach.reach(wide ? "wide scalar IDs" : "ordinary IDs");
            } finally {
              grant.free(slack);
              pool.free(scratch);
              pool.free(64);
              await tape.close();
              await files.closeAll();
              pages.close();
              pool.close();
              grant.close();
              competing?.close();
            }
            expect(governor.usedBytes).toBe(0);
            expect(service.bytesInUse).toBe(0);
            expect(active.size).toBe(0);
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [1, 1, true],
            [2, 3, false],
            [9, 8, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      vi.restoreAllMocks();
    }
  }, 120_000);
