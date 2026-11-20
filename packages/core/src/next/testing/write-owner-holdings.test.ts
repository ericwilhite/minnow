/** A native owner's permanent holdings belong to write growth's complete minimum: the engine
 * counts its non-evictable storage floor, and the owner registers its scratch control. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITE_PIN_HOLDINGS_REACH,
  WRITE_RECOVERY_HOLDINGS_REACH,
} from "../../testing/reach.js";
import { openNativeOwner } from "../owner/native-boot.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import { FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import { WriteCredits } from "../write/credits.js";
import { WritePathImpl } from "../write/engine.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "../write/test-support.js";

it("binds the storage floor and the owner's scratch control into write growth", async () => {
  const holdings: Array<() => number> = [];
  const owners: Array<() => number> = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
  const bind = WriteCredits.prototype.bindOwnerHoldings;
  const bound = vi.spyOn(WriteCredits.prototype, "bindOwnerHoldings").mockImplementation(function (
    this: WriteCredits,
    source,
  ) {
    holdings.push(source);
    bind.call(this, source);
  });
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
  const retain = WritePathImpl.prototype.retainOwnerHoldings;
  const retained = vi
    .spyOn(WritePathImpl.prototype, "retainOwnerHoldings")
    .mockImplementation(function (this: WritePathImpl, source) {
      owners.push(source);
      return retain.call(this, source);
    });
  onTestFinished(() => {
    bound.mockRestore();
    retained.mockRestore();
  });
  const owner = await openNativeOwner({
    backend: new SimBackend(new SimDisk()),
    memoryLimitBytes: 64 << 20,
  });
  try {
    expect(owners.map((source) => source())).toEqual([SCRATCH_CONTROL_BYTES]);
    const total = holdings.at(-1)?.() ?? 0;
    // Fixed slabs, the space's retained storage and the registered scratch control.
    expect(total).toBeGreaterThan(FIXED_CACHE_BYTES + SCRATCH_CONTROL_BYTES);
  } finally {
    await owner.close();
  }
});

for (const seed of seedsFor("next-write-recovery-holdings", [47315, 47316]))
  it(`counts owner holdings in every growth of open and recovery (seed ${String(seed)})`, async () => {
    const reach = createReach("recovery holdings", WRITE_RECOVERY_HOLDINGS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.constantFrom("crash", "reopen"),
        fc.constantFrom(SCRATCH_CONTROL_BYTES, 256 << 10),
        async (batches, mode, holdingsBytes) => {
          reach.beginHistory();
          const h = new Harness("strict", "sim", 4);
          const holdings = await h.governor.admit({
            label: "owner scratch control",
            minimumBytes: holdingsBytes,
            spillable: false,
          });
          h.ownerHoldings = () => holdings.held;
          const seen: number[] = [];
          const floors: number[] = [];
          const watch = (name: "growTo" | "prepareMetadata" | "prepareNativeMinimum") => {
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to each owner below.
            const original = WriteCredits.prototype[name] as (...a: unknown[]) => unknown;
            return vi.spyOn(WriteCredits.prototype, name).mockImplementation(function (
              this: WriteCredits,
              ...args: unknown[]
            ) {
              seen.push(this.ownerHoldingsBytes);
              floors.push(FIXED_CACHE_BYTES + h.space.stats.permanentBytes + holdings.held);
              return original.apply(this, args) as never;
            });
          };
          let spies: Array<{ mockRestore(): void }> = [];
          try {
            const path = await h.open();
            await autocommit(path, (s) =>
              s.changeCatalog({
                kind: "createTable",
                table: tableInput("recovery_holdings", [column("id", INT, false)], {
                  primaryKey: [0],
                }),
              }),
            );
            for (let b = 0; b < batches; b++)
              await autocommit(path, (s) =>
                s.table(1).insert(batch(ints([b * 3, b * 3 + 1, b * 3 + 2])), null),
              );
            spies = [watch("growTo"), watch("prepareMetadata"), watch("prepareNativeMinimum")];
            // Open, load and log replay all run before any later owner registration.
            const reopened = mode === "crash" ? await h.crash() : await h.reopen();
            for (const s of spies) s.mockRestore();
            spies = [];
            expect(seen.length, "open and recovery prepared or grew metadata").toBeGreaterThan(0);
            for (let k = 0; k < seen.length; k++)
              expect(seen[k], "holdings bound before open-time growth").toBe(floors[k]);
            reach.reach(mode === "crash" ? "crash recovery growth" : "clean reopen growth");
            const pin = reopened.pin();
            try {
              expect(await rowsOf(pin, 1)).toEqual(
                Array.from({ length: batches * 3 }, (_, id) => [id]),
              );
            } finally {
              pin.release();
            }
            reach.reach("exact rows after recovery");
          } finally {
            for (const s of spies) s.mockRestore();
            await h.close();
            holdings.close();
          }
          // A simulated crash abandons the old engine's grants, as a dead worker would.
          if (mode === "reopen") {
            expect(h.governor.usedBytes).toBe(0);
            reach.reach("zero final charge");
          }
        },
      ),
      { seed, numRuns: 10, examples: [[3, "crash", SCRATCH_CONTROL_BYTES]], endOnFailure: true },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-pin-holdings", [47317, 47318]))
  it(`never counts a reader's releasable pins as permanent (seed ${String(seed)})`, async () => {
    const reach = createReach("releasable pins", WRITE_PIN_HOLDINGS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 1, max: 40 }),
        async (pins, labelLength) => {
          reach.beginHistory();
          const holdings: Array<() => number> = [];
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
          const bind = WriteCredits.prototype.bindOwnerHoldings;
          const bound = vi
            .spyOn(WriteCredits.prototype, "bindOwnerHoldings")
            .mockImplementation(function (this: WriteCredits, source) {
              holdings.push(source);
              bind.call(this, source);
            });
          const h = new Harness("strict", "sim");
          const held: Array<{ release(): void }> = [];
          try {
            await h.open();
            bound.mockRestore();
            const floor = holdings.at(-1);
            if (floor === undefined) throw new Error("No holdings bound at open");
            const before = floor(),
              retained = h.space.stats.retainedBytes;
            for (let p = 0; p < pins; p++) held.push(h.space.pin("r".repeat(labelLength)));
            // Budgetless pins are charged to the space, refunded on release: not a floor.
            expect(h.space.stats.retainedBytes).toBeGreaterThan(retained);
            expect(floor()).toBe(before);
            reach.reach("pins charged to the space");
            reach.reach("owner floor unchanged by pins");
            for (const pin of held.splice(0)) pin.release();
            expect(h.space.stats.retainedBytes).toBe(retained);
            expect(floor()).toBe(before);
            reach.reach("pins refunded on release");
          } finally {
            bound.mockRestore();
            for (const pin of held) pin.release();
            await h.close();
          }
          reach.reach("zero final charge");
        },
      ),
      { seed, numRuns: 8, endOnFailure: true },
    );
    reach.assertReached();
  });
