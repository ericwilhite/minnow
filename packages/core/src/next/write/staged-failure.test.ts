/**
 * Review B3-p2c-1 #1: a staging publication that fails at any step (a part's group, its runs, the
 * staged directory, the root's encoding, its metadata charge, a write) gives back everything it
 * held before installation: the parts' space pins and their residual key deltas. Generated
 * (fast-check, `seedsFor`): the step, the part index (k > 0 included), the stage publication it
 * hits and how many times it happens. After every failure and rollback the pin count and the
 * write path's grant equal those of a fault-free rollback of the same transaction; afterwards
 * the path keeps staging and committing, and retired space is reclaimed (R8): no pin holds it.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { Grant } from "../memory/governor.js";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
} from "./test-support.js";

type Step = "group" | "runs" | "directory" | "encode" | "charge" | "write";
const STEPS: readonly Step[] = ["group", "runs", "directory", "encode", "charge", "write"];

const REACH = [
  ...STEPS.map((step) => `failure at ${step}` as const),
  "failure at part 0",
  "failure at part k>0",
  "repeated failures",
] as const;
const reach = createReach("next-write-staged-failure", REACH);

interface Fault {
  readonly step: Step;
  /** The part (memtable index in its publication) a group/runs fault hits. */
  readonly part: number;
  /** Which stage publication of the transaction it hits (0: the first). */
  readonly publication: number;
  /** For `write`: which write call of that publication's writer fails. */
  readonly call: number;
  readonly trials: number;
}

const ALIGN = 4096;
const aligned = (bytes: number) => Math.ceil(bytes / ALIGN) * ALIGN;

async function run(fault: Fault): Promise<void> {
  reach.beginHistory();
  let armed = false;
  let publications = 0;
  const fire = (step: Step, part: number) => {
    if (!armed || step !== fault.step || publications !== fault.publication) return;
    if ((step === "group" || step === "runs") && part !== fault.part) return;
    armed = false;
    reach.reach(`failure at ${step}`);
    if (step === "group" || step === "runs")
      reach.reach(part === 0 ? "failure at part 0" : "failure at part k>0");
    throw new Error(`injected failure at ${step}`);
  };
  const harness = new Harness("strict", "memory", 4, undefined, {
    merges: { mode: "off" },
    stageFault: (step, part) => {
      fire(step, part);
    },
    afterDeltaPrepared: (kind) => {
      if (kind === "stage") fire("encode", -1);
    },
  });
  // A hard failure at the root charge exercises cleanup; temporary refusal now waits/retries
  // (PROPOSAL "One memory governor", covered by metadata-pressure.test.ts).
  let grant: Grant | null = null;
  const admit = harness.governor.admit.bind(harness.governor);
  harness.governor.admit = async (options) => {
    const admitted = await admit(options);
    if (options.label === "write path") grant = admitted;
    if (options.label === "write path metadata") {
      const allocate = admitted.allocate.bind(admitted);
      admitted.allocate = (bytes) => {
        if (
          armed &&
          fault.step === "charge" &&
          publications === fault.publication &&
          (new Error().stack ?? "").includes("#publishStaging")
        ) {
          armed = false;
          reach.reach("failure at charge");
          throw new Error("injected failure at charge");
        }
        return allocate(bytes);
      };
    }
    return admitted;
  };
  // Released bytes per committed publication (the sink sees exactly what each root retires).
  const released: number[] = [];
  let pending = 0;
  harness.extentDeltas = {
    prepare: (_attempt, delta) => {
      pending = delta.released.reduce((sum, extent) => sum + aligned(extent.length), 0);
      return null;
    },
    commit: () => {
      released.push(pending);
      publications += 1;
    },
    abort: () => undefined,
  };
  const path: WritePathImpl = await harness.open();
  // A writer of the stage publication's whole I/O: one of its writes fails.
  const beginWrite = harness.space.beginWrite.bind(harness.space);
  harness.space.beginWrite = (label, options) => {
    const writer = beginWrite(label, options);
    if (label !== "stage" || !armed || fault.step !== "write") return writer;
    if (publications !== fault.publication) return writer;
    let calls = 0;
    const write = writer.write.bind(writer);
    writer.write = (parts) => {
      if (armed && calls++ === fault.call) {
        armed = false;
        reach.reach("failure at write");
        return Promise.reject(new Error("injected failure at write"));
      }
      return write(parts);
    };
    return writer;
  };
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", INT)]),
        })
      ).id ?? 0;
  });
  const v = need(path.catalog.table(t)?.columns[1]).id;
  await autocommit(path, (s) =>
    s.changeCatalog({
      kind: "createIndex",
      index: { tableId: t, name: "t_v", columnIds: [v], unique: false, role: "secondary" },
    }),
  );
  await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2, 3]), ints([1, 2, 3])), null));
  await path.quiesce();
  let next = 1000;
  // Calls of 9 rows over 4-row memtables: each call after the first stages two memtables.
  const large = async (calls: number): Promise<{ tx: Awaited<ReturnType<typeof path.begin>> }> => {
    const tx = await path.begin();
    const s = tx.statement({ rules });
    publications = 0;
    try {
      for (let call = 0; call < calls; call++) {
        const ids = Array.from({ length: 9 }, () => next++);
        await s.table(t).insert(batch(ints(ids), ints(ids)), null);
      }
      await s.end();
    } catch (error) {
      s.rollback();
      tx.rollback();
      throw error;
    }
    return { tx };
  };
  // The fault-free rollback of the same transaction: the baseline.
  (await large(3)).tx.rollback();
  await path.quiesce();
  expect(path.maintenance.stages).toBeGreaterThan(1);
  const pins = harness.space.stats.pinCount;
  const allocated = need(grant as Grant | null).allocated;
  for (let trial = 0; trial < fault.trials; trial++) {
    armed = true;
    const failed = await large(3).then(
      ({ tx }) => {
        tx.rollback();
        return null;
      },
      (error: unknown) => error,
    );
    expect(armed, `the ${fault.step} fault fired`).toBe(false);
    expect(String(failed)).toContain("injected failure");
    await path.quiesce();
    expect(path.failure).toBeNull();
    expect(harness.space.stats.pinCount, `pins after failure ${String(trial)}`).toBe(pins);
    expect(need(grant as Grant | null).allocated, `grant after failure ${String(trial)}`).toBe(
      allocated,
    );
    if (trial > 0) reach.reach("repeated failures");
  }
  // The path goes on: a large commit stages and folds; later publications reclaim (R8).
  const { tx } = await large(3);
  await (
    await tx.commit(null)
  ).durable;
  await path.quiesce();
  for (let k = 0; k < 4; k++) {
    for (const ids of [[next++, next++, next++, next++], [next++]])
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
    await path.quiesce();
  }
  expect(harness.space.stats.pinCount).toBe(pins);
  // Only the last publication's retirements still wait (two-generation reclaim): no pin holds
  // older garbage.
  const last = released.slice(-2).reduce((sum, bytes) => sum + bytes, 0);
  expect(harness.space.stats.waitingBytes).toBeLessThanOrEqual(last);
  await harness.space.audit();
  await harness.close();
}

describe("a failed staging publication gives back what it held (review B3-p2c-1 #1)", () => {
  const SEEDS = seedsFor("next-write-staged-failure", [0x5a6e, 0x5a6f]);
  for (const seed of SEEDS)
    it(`generated failures at every step and part (seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            step: fc.constantFrom(...STEPS),
            part: fc.integer({ min: 0, max: 1 }),
            publication: fc.integer({ min: 0, max: 1 }),
            call: fc.integer({ min: 0, max: 5 }),
            trials: fc.integer({ min: 1, max: 3 }),
          }),
          (fault) => run(fault),
        ),
        { seed, numRuns: 40 },
      );
    }, 600_000);

  it("three failed stage publications leave the pin count at its baseline (the reviewer's probe)", async () => {
    let fail = false;
    const harness = new Harness("strict", "memory", 8, undefined, {
      merges: { mode: "off" },
      afterDeltaPrepared: (kind) => {
        if (kind === "stage" && fail) throw new Error("stage injection");
      },
    });
    harness.extentDeltas = { prepare: () => null, commit: () => undefined, abort: () => undefined };
    const path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await path.quiesce();
    const before = harness.space.stats.pinCount;
    for (let trial = 0; trial < 3; trial++) {
      const tx = await path.begin();
      const s = tx.statement({ rules });
      fail = true;
      await expect(
        (async () => {
          for (let from = 0; from < 24; from += 8)
            await s
              .table(t)
              .insert(batch(ints(Array.from({ length: 8 }, (_, k) => from + k))), null);
        })(),
      ).rejects.toThrow("stage injection");
      s.rollback();
      tx.rollback();
      await path.quiesce();
    }
    expect(harness.space.stats.pinCount).toBe(before);
    await harness.close();
  });

  it("reached every declared state", () => {
    reach.assertReached();
  });
});
