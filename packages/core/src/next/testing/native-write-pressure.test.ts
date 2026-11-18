/** Governor rules 4/5/7: page and retirement work survives reclaim of idle row credit. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { createReach, NATIVE_WRITE_PRESSURE_REACH } from "../../testing/reach.js";
import type { ExtentWriter } from "../storage/extents.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import * as writers from "../page/writer.js";
import { PIN_HEADER_BYTES } from "../storage/root-pins.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import { WriteCredits } from "../write/credits.js";
import { autocommit, batch, rowsOf, texts } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

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
          let timer: ReturnType<typeof setTimeout> | undefined;
          const releaseReader = () => {
            if (held !== undefined) expect(held.at(-1)).toBe(7);
            held = undefined;
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
          // Only safe outer retry may admit more metadata. No provisional writer/lock
          // may survive to this boundary. Release the competitor on the next real task.
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
          const grow = WriteCredits.prototype.growTo;
          const growth = vi.spyOn(WriteCredits.prototype, "growTo").mockImplementation(function (
            this: WriteCredits,
            grant,
            wanted,
            signal,
          ) {
            if (held !== undefined && grant === this.metadata && wanted > grant.held) {
              expect(borrowed?.state).toBe("aborted");
              reach.reach("admission after provisional unwind");
              timer ??= setTimeout(releaseReader, 5);
            }
            return grow.call(this, grant, wanted, signal);
          });
          let closed = false;
          const restore = () => {
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
