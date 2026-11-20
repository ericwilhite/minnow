/** Governor rule 3 at the exact cap boundary through a real engine reopen.
 *
 * A native owner restarts over the same disk (clean close or power loss) with a small fresh
 * governor and a real permanent grant beside it (scratch control). The grant is sized so the
 * owner's real holdings decide the outcome of the binding step of open, load and recovery:
 * one byte over refuses at once with 53200 (no queued admission), exactly the cap and one byte
 * under both reopen with every committed row. A refusal leaves nothing behind but the owner's
 * own grant, and a retry with a smaller grant recovers.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITE_OWNER_BOUNDARY_REACH,
  WRITE_PINNED_BOUNDARY_REACH,
} from "../../testing/reach.js";
import { Governor, MemoryAdmissionError, type Grant } from "../memory/governor.js";
import { SimDisk, type SimBackend } from "../storage/sim-disk.js";
import { yieldTask } from "../task.js";
import { WriteCredits, type CompleteMinimumStep } from "../write/credits.js";
import { claimWriteOwner } from "../write/owner-claim.js";
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

const MiB = 1 << 20;
type Mode = "crash" | "clean";

/** The same deterministic history on a fresh disk: the probe and each run start identical. */
async function history(batches: number, tables: number, mode: Mode): Promise<SimDisk> {
  const disk = new SimDisk();
  const h = new Harness("strict", "sim", 4, disk);
  const path = await h.open();
  for (let t = 1; t <= tables; t++)
    await autocommit(path, (s) =>
      s.changeCatalog({
        kind: "createTable",
        table: tableInput(`t${String(t)}`, [column("id", INT, false), column("v", INT, false)], {
          primaryKey: [0],
        }),
      }),
    );
  for (let b = 0; b < batches; b++)
    await autocommit(path, (s) =>
      s.table(1).insert(batch(ints([b * 2, b * 2 + 1]), ints([b, b])), null),
    );
  // Settle background maintenance, so every build of this history leaves identical bytes.
  await path.quiesce();
  if (mode === "crash") {
    await path.flush();
    disk.powerLoss();
    (h.backend as SimBackend).restart();
  } else await h.close();
  return disk;
}

interface Outcome {
  readonly kind: "open" | "refused" | "waited";
  readonly steps: CompleteMinimumStep[];
  readonly error?: unknown;
  readonly harness: Harness;
  readonly grant: Grant | null;
}

/** Reopens with `holdings` bytes of real owner grant; a write admission left queued while
 * nothing else can release is reported as "waited", the defect this boundary rules out. */
async function reopen(disk: SimDisk, capMiB: number, holdings: number): Promise<Outcome> {
  const h = new Harness("strict", "sim", 4, disk);
  Object.defineProperty(h, "governor", { value: new Governor(capMiB * MiB) });
  const grant =
    holdings > 0
      ? await h.governor.admit({
          label: "owner scratch control",
          minimumBytes: holdings,
          spillable: false,
        })
      : null;
  h.ownerHoldings = () => grant?.held ?? 0;
  const steps: CompleteMinimumStep[] = [];
  let pending = 0;
  const admit = h.governor.admitGroup.bind(h.governor);
  h.governor.admitGroup = (options) => {
    const write =
      options.label === "write owner minimum" || options.label.startsWith("complete grow");
    const request = admit(options);
    if (!write) return request;
    pending++;
    return request.finally(() => pending--);
  };
  WriteCredits.observeComplete = (step) => steps.push(step);
  try {
    const state: { settled?: { ok: boolean; error?: unknown } } = {};
    const opening = h.open().then(
      () => (state.settled = { ok: true }),
      (error: unknown) => (state.settled = { ok: false, error }),
    );
    let stalled = 0;
    for (let turn = 0; turn < 20_000 && state.settled === undefined; turn++) {
      await yieldTask();
      stalled = pending > 0 ? stalled + 1 : 0;
      if (stalled > 64) return { kind: "waited", steps, harness: h, grant };
    }
    await opening;
    const settled = state.settled;
    if (settled === undefined) return { kind: "waited", steps, harness: h, grant };
    return settled.ok
      ? { kind: "open", steps, harness: h, grant }
      : { kind: "refused", steps, error: settled.error, harness: h, grant };
  } finally {
    WriteCredits.observeComplete = null;
  }
}

const slackOf = (steps: readonly CompleteMinimumStep[]) =>
  Math.min(...steps.map((step) => step.capacity - step.complete));

for (const seed of seedsFor("next-write-owner-boundary", [47319, 47320]))
  it(`real owner holdings decide reopen at cap - 1, cap and cap + 1 (seed ${String(seed)})`, async () => {
    const reach = createReach("owner holdings boundary at reopen", WRITE_OWNER_BOUNDARY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 48),
        fc.integer({ min: 1, max: 3 }),
        fc.constantFrom(1, 160),
        fc.constantFrom<Mode>("crash", "clean"),
        async (capMiB, batches, tables, mode) => {
          reach.beginHistory();
          const rows = Array.from({ length: batches * 2 }, (_, id) => [id, Math.floor(id / 2)]);
          // Search the binding step: the largest grant with which the reopen still succeeds.
          const probe = await reopen(await history(batches, tables, mode), capMiB, 0);
          expect(probe.kind).toBe("open");
          await probe.harness.close();
          expect(
            probe.steps.map((step) => step.label),
            "the owner minimum is decided with the owner's holdings before construction",
          ).toContain("write owner minimum");
          let holdings = slackOf(probe.steps);
          expect(holdings).toBeGreaterThan(1);
          for (let search = 0; ; search++) {
            expect(search, "the binding search converges").toBeLessThan(8);
            const run = await reopen(await history(batches, tables, mode), capMiB, holdings);
            expect(run.kind, "a reopen never queues an unservable admission").not.toBe("waited");
            if (run.kind === "open") {
              await run.harness.close();
              run.grant?.close();
              break;
            }
            const deficit = -slackOf(run.steps);
            expect(deficit).toBeGreaterThan(0);
            run.grant?.close();
            holdings -= deficit;
          }
          const binding = holdings;
          const decided = (steps: readonly CompleteMinimumStep[]) =>
            steps.find((step) => step.label === "write owner minimum")?.complete;
          const probed = decided(probe.steps) ?? NaN;
          for (const offset of [1, 0, -1] as const) {
            const run = await reopen(
              await history(batches, tables, mode),
              capMiB,
              binding + offset,
            );
            const governor = run.harness.governor;
            try {
              // Determinism: this run decided the same owner minimum plus its own grant.
              expect(decided(run.steps), "identical history, identical decision").toBe(
                probed + binding + offset,
              );
              expect(run.kind, "a reopen never queues an unservable admission").not.toBe("waited");
              const last = run.steps.at(-1);
              if (offset === 1) {
                expect(run.kind).toBe("refused");
                expect(run.error).toBeInstanceOf(MemoryAdmissionError);
                expect((run.error as MemoryAdmissionError).sqlState).toBe("53200");
                // Open, load and replay size their steps inside the owner minimum, so the
                // binding decision is that pre-construction admission (1 to 160 tables).
                expect(last?.label).toBe("write owner minimum");
                reach.reach("refused at the owner minimum (before construction)");
                // Refusal cleanup: only the owner's own grant stays; the governor is free.
                expect(governor.usedBytes).toBe(run.grant?.held ?? 0);
                claimWriteOwner(governor).release();
                reach.reach("refusal left only the owner grant");
                // Retry with a smaller grant on the same disk: it reopens and recovers.
                run.grant?.close();
                const retry = await reopen(run.harness.disk, capMiB, binding);
                expect(retry.kind).toBe("open");
                const pin = retry.harness.path.pin();
                try {
                  expect(await rowsOf(pin, 1)).toEqual(rows);
                } finally {
                  pin.release();
                }
                await retry.harness.close();
                retry.grant?.close();
                reach.reach("retry recovered every row");
              } else {
                expect(run.kind).toBe("open");
                expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
                const pin = run.harness.path.pin();
                try {
                  expect(await rowsOf(pin, 1)).toEqual(rows);
                } finally {
                  pin.release();
                }
                await run.harness.close();
                run.grant?.close();
                expect(governor.usedBytes).toBe(0);
                reach.reach(offset === 0 ? "exact cap reopened" : "one byte under reopened");
              }
            } finally {
              if (run.kind !== "open") run.grant?.close();
            }
          }
          reach.reach(mode === "crash" ? "crash recovery" : "clean restart");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [32, 1, 1, "crash"],
          [48, 3, 160, "clean"],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-pinned-reopen", [47321, 47322]))
  it(`waits for a reader's storage pins instead of refusing (seed ${String(seed)})`, async () => {
    const reach = createReach("storage pins at the boundary", WRITE_PINNED_BOUNDARY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 48),
        fc.integer({ min: 1, max: 3 }),
        fc.integer({ min: 1, max: 4096 }),
        fc.constantFrom<Mode>("crash", "clean"),
        async (capMiB, pinCount, labelLength, mode) => {
          reach.beginHistory();
          const probe = await reopen(await history(2, 1, mode), capMiB, 0);
          expect(probe.kind).toBe("open");
          await probe.harness.close();
          // Exactly the cap without the pins: it fits once the reader releases them.
          const holdings = slackOf(probe.steps);
          const disk = await history(2, 1, mode);
          const h = new Harness("strict", "sim", 4, disk);
          Object.defineProperty(h, "governor", { value: new Governor(capMiB * MiB) });
          const grant = await h.governor.admit({
            label: "owner scratch control",
            minimumBytes: holdings,
            spillable: false,
          });
          h.ownerHoldings = () => grant.held;
          const pins: Array<{ release(): void }> = [];
          // A reader pins the space (budgetless: charged to the space, refunded on release).
          h.onSpace = (space) => {
            for (let p = 0; p < pinCount; p++) pins.push(space.pin("r".repeat(labelLength)));
          };
          let pending = 0;
          const admit = h.governor.admitGroup.bind(h.governor);
          h.governor.admitGroup = (options) => {
            const request = admit(options);
            if (options.label !== "write owner minimum") return request;
            pending++;
            return request.finally(() => pending--);
          };
          const state: { settled?: { ok: boolean; error?: unknown } } = {};
          const opening = h.open().then(
            () => (state.settled = { ok: true }),
            (error: unknown) => (state.settled = { ok: false, error }),
          );
          try {
            for (
              let turn = 0;
              turn < 20_000 && state.settled === undefined && pending === 0;
              turn++
            )
              await yieldTask();
            for (let turn = 0; turn < 64 && state.settled === undefined; turn++) await yieldTask();
            expect(state.settled, "a write that fits after the pins are released must wait").toBe(
              undefined,
            );
            expect(pending).toBe(1);
            expect(h.space.stats.retainedBytes).toBeGreaterThan(h.space.stats.permanentBytes);
            reach.reach("waited while the reader held its pins");
            for (const pin of pins.splice(0)) pin.release();
            await opening;
            expect(state.settled?.ok, "the write proceeds once the pins are released").toBe(true);
            reach.reach("proceeded after the pins were released");
            expect(h.governor.peakBytes).toBeLessThanOrEqual(h.governor.capBytes);
            await h.close();
            reach.reach(mode === "crash" ? "crash recovery" : "clean restart");
          } finally {
            for (const pin of pins) pin.release();
            if (state.settled === undefined) {
              grant.close();
              await opening;
            }
            grant.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [32, 1, 1, "crash"],
          [48, 3, 4096, "clean"],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
