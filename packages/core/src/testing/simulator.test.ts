import { describe, expect, it } from "vitest";
import {
  DeterministicScheduler,
  generateSimulationPlan,
  parseSimulationPlan,
  runSimulation,
} from "./simulator.js";
import { seedFor } from "./seeds.js";

describe("deterministic simulator plans", () => {
  it("generates the same bounded plan from the same seed", () => {
    const options = { rounds: 24, clients: 3, keySpace: 9 } as const;
    const first = generateSimulationPlan(0x51c1, options);
    const second = generateSimulationPlan(0x51c1, options);

    expect(first).toEqual(second);
    expect(first.steps.filter((step) => step.kind === "concurrent")).toHaveLength(24);
    expect(first.steps.some((step) => step.kind === "fault")).toBe(true);
    expect(parseSimulationPlan(JSON.stringify(first))).toEqual(first);
  });

  it.each([
    "null",
    JSON.stringify({ version: 2, seed: 1, clients: 1, keySpace: 1, steps: [] }),
    JSON.stringify({ version: 1, seed: 1.5, clients: 1, keySpace: 1, steps: [] }),
    JSON.stringify({ version: 1, seed: 1, clients: 1, keySpace: 1, steps: [null] }),
    JSON.stringify({ version: 1, seed: 1, clients: 1, keySpace: 1, steps: [{ kind: "unknown" }] }),
    JSON.stringify({
      version: 1,
      seed: 1,
      clients: 1,
      keySpace: 1,
      steps: [{ kind: "concurrent", operations: [{ kind: "unknown", client: 0, key: 1 }] }],
    }),
  ])("rejects malformed replay plans: %s", (source) => {
    expect(() => parseSimulationPlan(source)).toThrow();
  });
});

describe("deterministic completion scheduling", () => {
  it("replays completion order and retains only the configured trace tail", async () => {
    const run = async () => {
      const scheduler = new DeterministicScheduler(47, 2);
      const results = await scheduler.settle([
        scheduler.schedule("first", async () => 1),
        scheduler.schedule("second", async () => 2),
        scheduler.schedule("third", async () => 3),
      ]);
      return { results, trace: [...scheduler.trace], highWater: scheduler.highWater };
    };

    const first = await run();
    expect(await run()).toEqual(first);
    expect(first.results).toEqual([
      { status: "fulfilled", value: 1 },
      { status: "fulfilled", value: 2 },
      { status: "fulfilled", value: 3 },
    ]);
    expect(first.trace).toHaveLength(2);
    expect(first.trace[0]?.sequence).toBe(2);
    expect(first.trace[1]?.sequence).toBe(3);
    expect(first.highWater).toBe(3);
  });
});

describe("deterministic database simulation", () => {
  it("replays concurrency and injected faults to the identical checked result", async () => {
    const plan = generateSimulationPlan(seedFor("deterministic-simulator", 0x5eed), {
      rounds: 20,
      clients: 3,
      keySpace: 8,
    });
    const first = await runSimulation(plan, { traceEvents: 64 });
    const second = await runSimulation(parseSimulationPlan(JSON.stringify(plan)), {
      traceEvents: 64,
    });

    expect(second).toEqual(first);
    expect(first.injectedFaults).toBe(1);
    expect(first.checkpoints).toBeGreaterThan(1);
    expect(first.trace.length).toBeLessThanOrEqual(64);
    expect(first.manifestCount).toBeLessThanOrEqual(2);
    expect(first.blockCount).toBeLessThanOrEqual(plan.keySpace * 16 + 64);
    expect(first.segmentCount).toBeLessThanOrEqual(plan.keySpace + 16);
    expect(first.transactionCount).toBeLessThanOrEqual(plan.keySpace + 32);
  });

  it("keeps final storage bounded by live state rather than run length", async () => {
    const plan = generateSimulationPlan(0x10_000, { rounds: 80, clients: 4, keySpace: 4 });
    const result = await runSimulation(plan, { traceEvents: 16 });

    expect(result.rows.length).toBeLessThanOrEqual(plan.keySpace);
    expect(result.blockCount).toBeLessThanOrEqual(plan.keySpace * 16 + 64);
    expect(result.segmentCount).toBeLessThanOrEqual(plan.keySpace + 16);
    expect(result.transactionCount).toBeLessThanOrEqual(plan.keySpace + 32);
    expect(result.manifestCount).toBeLessThanOrEqual(2);
    expect(result.trace).toHaveLength(16);
  });

  it("reclaims safely after crossing the 64-manifest collection-plan boundary", async () => {
    const plan = generateSimulationPlan(24_301, { rounds: 160, clients: 8, keySpace: 128 });
    const result = await runSimulation(plan, { traceEvents: 8 });

    expect(result.acceptedMutations).toBeGreaterThan(1_000);
    expect(result.collectionPasses).toBeGreaterThan(1);
    expect(result.manifestCount).toBe(1);
    expect(result.leaseCount).toBe(0);
    expect(result.blockCount).toBeLessThanOrEqual(plan.keySpace * 16 + 64);
    expect(result.segmentCount).toBeLessThanOrEqual(plan.keySpace + 16);
    expect(result.transactionCount).toBeLessThanOrEqual(plan.keySpace + 32);
  }, 60_000);
});
