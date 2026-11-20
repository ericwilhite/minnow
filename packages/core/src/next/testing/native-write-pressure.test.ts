/** Governor rules 4/5/7: page and retirement work survives reclaim of idle row credit. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import {
  createReach,
  NATIVE_WRITE_PRESSURE_REACH,
  NATIVE_WRITE_SETTLEMENT_REACH,
} from "../../testing/reach.js";
import type { ExtentWriter, WriterState } from "../storage/extents.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import * as writers from "../page/writer.js";
import { PIN_HEADER_BYTES } from "../storage/root-pins.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import { WriteCredits } from "../write/credits.js";
import { autocommit, batch, rowsOf, texts } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

/** ExtentWriter's lifecycle contract and governor rules4/5/7 require settlement,
 * not a failed attempt. Successful publication also retires provisional ownership. */
function assertSettled(
  state: WriterState | undefined,
  writerBytes: number,
  releasingBytes: number,
) {
  expect(["aborted", "published"]).toContain(state);
  expect(writerBytes).toBe(0);
  expect(releasingBytes).toBe(0);
}

for (const seed of seedsFor("next-native-write-settlement", [47249, 47250]))
  it(`rejects unsettled or still-owned writer work before growth (seed ${String(seed)})`, () => {
    const reach = createReach("native writer settlement", NATIVE_WRITE_SETTLEMENT_REACH);
    fc.assert(
      fc.property(
        fc.constantFrom<WriterState | undefined>(
          "open",
          "publishing",
          "published",
          "aborted",
          "indeterminate",
          undefined,
        ),
        fc.integer({ min: 0, max: 4096 }),
        fc.integer({ min: 0, max: 4096 }),
        (state, writerBytes, releasingBytes) => {
          reach.beginHistory();
          const valid =
            (state === "aborted" || state === "published") &&
            writerBytes === 0 &&
            releasingBytes === 0;
          const check = () => assertSettled(state, writerBytes, releasingBytes);
          if (valid) {
            expect(check).not.toThrow();
            reach.reach(state === "published" ? "published settled" : "aborted settled");
          } else {
            expect(check).toThrow();
            reach.reach(
              state === "open" ||
                state === "publishing" ||
                state === "indeterminate" ||
                state === undefined
                ? "unsettled rejected"
                : "owned bytes rejected",
            );
          }
        },
      ),
      {
        seed,
        numRuns: 48,
        examples: [
          ["published", 0, 0],
          ["aborted", 0, 0],
          ["open", 0, 0],
          ["publishing", 0, 0],
          ["indeterminate", 0, 0],
          [undefined, 0, 0],
          ["published", 4096, 0],
          ["aborted", 0, 4096],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });

const population = Array.from({ length: 12 }, (_, i) => 8814 + i);
const seeds = seedsFor("next-native-write-pressure", population);
for (const boundary of ["retirement", "group"] as const) {
  it.each(seeds)(
    `completes six 1 MiB native inserts after ${boundary} reclaim (seed %i)`,
    async (seed) => {
      await fc.assert(
        fc.asyncProperty(fc.array(fc.boolean(), { minLength: 6, maxLength: 6 }), async (yields) => {
          const reach = createReach(`native ${boundary} pressure`, NATIVE_WRITE_PRESSURE_REACH);
          reach.beginHistory();
          const core = new StorageCore(6);
          const governor = new Governor(32 << 20);
          Object.defineProperty(core.harness, "governor", { value: governor });
          let credits: WriteCredits | undefined;
          let armed = false;
          let borrowed: ExtentWriter | undefined;
          let reclaims = 0;
          let reader: Grant | undefined;
          let held: Uint8Array | undefined;
          let resident: Uint8Array | undefined;
          let residentGrant: Grant | undefined;
          let timer: ReturnType<typeof setTimeout> | undefined;
          const releaseReader = () => {
            if (held !== undefined) expect(held.at(-1)).toBe(7);
            held = undefined;
            if (resident !== undefined) expect(resident.at(-1)).toBe(13);
            const residentBytes = resident?.byteLength ?? 0;
            resident = undefined;
            if (residentBytes > 0) residentGrant?.free(residentBytes);
            residentGrant = undefined;
            if (reader !== undefined && reader.allocated > 0) reader.free(reader.allocated);
            if (reader !== undefined && reader.held > 0) reader.release(reader.held);
          };
          const open = WriteCredits.open.bind(WriteCredits);
          const opened = vi.spyOn(WriteCredits, "open").mockImplementation(async (...args) => {
            credits = await open(...args);
            return credits;
          });
          // Start a real competing reader at a generated writer boundary. It owns real
          // buffers until its task completes; native retries must first unwind their work.
          const reclaim = (writer: ExtentWriter) => {
            if (!armed || credits === undefined) return;
            borrowed = writer;
            credits.reclaimRows();
            reach.reach("idle row credit reclaimed");
            if (reader !== undefined && reclaims === 0) {
              // Model other resident native metadata overlapping the prepaid step.
              // These are real charged bytes, not a lowered cap or a fake denial.
              residentGrant = credits.metadata;
              const live = residentGrant.held - residentGrant.allocated - 4096;
              expect(live).toBeGreaterThan(0);
              expect(residentGrant.allocate(live)).toBe(true);
              resident = new Uint8Array(live).fill(13);
              reach.reach("resident metadata overlaps writer step");
              credits.reclaimMetadata();
              const bytes = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              expect(bytes).toBeGreaterThan(0);
              expect(reader.allocate(bytes)).toBe(true);
              held = new Uint8Array(bytes).fill(7);
              reach.reach("competing reader owns real bytes");
            }
            reclaims++;
          };
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
          const prepare = ExtentListKeeper.prototype.prepare;
          const keeper = vi
            .spyOn(ExtentListKeeper.prototype, "prepare")
            .mockImplementation(function (this: ExtentListKeeper, ...args) {
              if (boundary === "retirement") reclaim(args[1].writer);
              return prepare.apply(this, args);
            });
          const OriginalWriter = writers.GroupWriter;
          const group = vi.spyOn(writers, "GroupWriter").mockImplementation(function (options) {
            if (boundary === "group") reclaim(options.writer);
            const rowBytes = credits?.rows.allocated;
            const writer = new OriginalWriter(options);
            // The active primary row step may retain prepaid idle credit. Page buffers
            // must not consume that reservation, even if this particular step still fits.
            expect(
              credits?.rows.allocated,
              "page workspace must not consume the row account",
            ).toBeLessThanOrEqual(rowBytes ?? 0);
            expect(writer.retainedBytes).toBeGreaterThan(0);
            return writer;
          });
          // A funded attempt may have published rather than aborted. Both retire
          // provisional ownership; open/publishing/indeterminate work may not wait
          // for more credit. Release the competitor on the next real task.
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
          const grow = WriteCredits.prototype.growTo;
          const growth = vi.spyOn(WriteCredits.prototype, "growTo").mockImplementation(function (
            this: WriteCredits,
            grant,
            wanted,
            signal,
          ) {
            if (held !== undefined && grant === this.metadata && wanted > grant.held) {
              const stats = core.harness.space.stats;
              assertSettled(borrowed?.state, stats.writerBytes, stats.releasingBytes);
              reach.reach("admission after provisional settlement");
              timer ??= setTimeout(releaseReader, 5);
            }
            return grow.call(this, grant, wanted, signal);
          });
          let closed = false;
          let restored = false;
          const restore = () => {
            if (restored) return;
            restored = true;
            opened.mockRestore();
            keeper.mockRestore();
            group.mockRestore();
            growth.mockRestore();
          };
          onTestFinished(async () => {
            armed = false;
            if (timer !== undefined) clearTimeout(timer);
            releaseReader();
            reader?.close();
            restore();
            if (!closed) await core.close();
          });
          try {
            await core.open();
            await core.execute("CREATE TABLE pressure(v TEXT)");
            const empty = await core.execute("SELECT v FROM pressure");
            if (!("batches" in empty)) throw new Error("expected an empty SELECT result");
            expect(oracleRows(empty)).toEqual([]);
            reader = await governor.admit({
              label: "concurrent pressure reader",
              minimumBytes: 0,
              spillable: true,
            });
            const table = core.harness.path.catalog.tableByName("pressure");
            if (table === undefined) throw new Error("missing pressure table");
            armed = true;
            const payload = "x".repeat(1 << 20);
            for (let i = 0; i < 6; i++) {
              await autocommit(core.harness.path, (statement) =>
                statement.table(table.id).insert(batch(texts([payload])), null),
              );
              if (yields[i]) await new Promise<void>((resolve) => queueMicrotask(resolve));
            }
            reach.reach("six 1 MiB native commits");
            await core.harness.path.quiesce();
            releaseReader();
            const checkRootFunding = () => {
              expect(core.harness.space.stats.pinCount).toBe(1);
              const label = core.harness.space.stats.oldestPin?.label;
              expect(label).toBe("current state");
              expect(credits?.metadata.allocated).toBe(
                core.harness.path.residentMetadataBytes +
                  PIN_HEADER_BYTES +
                  2 * "current state".length,
              );
            };
            checkRootFunding();
            expect(credits?.metadata.held, "completed phase returns metadata workspace").toBe(
              credits?.metadata.allocated,
            );
            reach.reach("completed metadata workspace returned");
            armed = false;
            await core.harness.reopen();
            checkRootFunding();
            reach.reach("one funded root list through reopen");
            const view = core.harness.path.pin();
            try {
              expect(await rowsOf(view, table.id)).toEqual(
                Array.from({ length: 6 }, () => [payload]),
              );
            } finally {
              view.release();
            }
            reach.reach("exact durable rows");
            expect(reclaims).toBeGreaterThan(0);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            reach.reach("32 MiB peak");
          } finally {
            armed = false;
            if (timer !== undefined) clearTimeout(timer);
            releaseReader();
            reader?.close();
            restore();
            await core.close();
            closed = true;
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
          reach.assertReached();
        }),
        { seed, numRuns: 1, endOnFailure: true },
      );
    },
    60000,
  );
}
