/** The harness must stop worker-local lanes, not just page-side go messages. */
import { afterAll, afterEach, expect, it, vi } from "vitest";
import { createReach, STORAGE_STEP_DOWN_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { runChaos, type StepDownRelease } from "./chaos-workload.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";

const reach = createReach("storage graceful stop", STORAGE_STEP_DOWN_STATES, { minimum: 1 });
afterAll(() => reach.assertReached());
afterEach(() => vi.useRealTimers());

it.each(
  seedsFor(
    "next-storage-stepdown",
    Array.from({ length: 16 }, (_, i) => 0x57e9 + i),
  ),
)(
  "parks the log lane before its post-operation timer touches a closed session (seed %i)",
  async (seed) => {
    const disk = new SimDisk({ seed: 0x57e9 });
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(disk),
      name: "step-down",
      locks: new SimLocks(),
    });
    let release!: () => Promise<StepDownRelease>;
    let done!: () => void;
    const firstDone = new Promise<void>((resolve) => {
      done = resolve;
    });
    let failure: unknown = null;
    let completed = 0;
    const workload = runChaos(
      "step-down",
      seed,
      null,
      {
        backendFor: async () => backend,
        registerRelease: (close) => {
          release = close;
        },
        go: () => (completed === 0 ? Promise.resolve() : new Promise<void>(() => undefined)),
        post: (message) => {
          if (message.chaos === "done" && completed++ === 0) {
            // Freeze exactly between a completed operation and the next lane iteration.
            vi.useFakeTimers();
            done();
          }
        },
      },
      { mode: "concurrent", lanes: ["log"] },
    ).catch((error: unknown) => {
      failure = error;
    });
    void workload;
    await firstDone;
    expect((await release()).aborted).toBe(0);
    await vi.advanceTimersByTimeAsync(100);
    reach.recordHistory(["paused log lane"]);
    expect(failure, "a stopped lane cannot call logLength after session.close").toBeNull();
    expect(completed).toBe(1);
  },
);

it("drains an admitted append before closing the session", async () => {
  const disk = new SimDisk({ seed: 0x57e9 });
  const backend = createIndexedDbBackend({
    database: () => simIndexedDb(disk),
    name: "drain",
    locks: new SimLocks(),
  });
  let entered!: () => void;
  const issued = new Promise<void>((resolve) => {
    entered = resolve;
  });
  let resume!: () => void;
  const held = new Promise<void>((resolve) => {
    resume = resolve;
  });
  let closed = false;
  const open = backend.open.bind(backend);
  backend.open = async (lease) => {
    const session = await open(lease);
    const append = session.appendLog.bind(session);
    session.appendLog = async (bytes) => {
      entered();
      await held;
      return append(bytes);
    };
    const close = session.close.bind(session);
    session.close = async () => {
      closed = true;
      await close();
    };
    return session;
  };
  let release!: () => Promise<StepDownRelease>;
  let failure: unknown = null;
  void runChaos(
    "drain",
    0x57e9,
    null,
    {
      backendFor: async () => backend,
      registerRelease: (close) => {
        release = close;
      },
      post: () => undefined,
      go: () => Promise.resolve(),
    },
    { mode: "concurrent", lanes: ["log"] },
  ).catch((error: unknown) => {
    failure = error;
  });
  await issued;
  const closing = release();
  try {
    for (let n = 0; n < 5; n++) await Promise.resolve();
    reach.recordHistory(["admitted append"]);
    expect(closed, "session close must wait for admitted storage work").toBe(false);
  } finally {
    resume();
    await closing;
  }
  expect(failure).toBeNull();
  expect(closed).toBe(true);
});

it("does not admit a go that arrives after the local stop barrier", async () => {
  const disk = new SimDisk({ seed: 0x57e9 });
  const backend = createIndexedDbBackend({
    database: () => simIndexedDb(disk),
    name: "late-go",
    locks: new SimLocks(),
  });
  let release!: () => Promise<StepDownRelease>;
  let announced!: () => void;
  const intent = new Promise<void>((resolve) => {
    announced = resolve;
  });
  let grant!: () => void;
  const go = new Promise<void>((resolve) => {
    grant = resolve;
  });
  let began = 0;
  let failure: unknown = null;
  void runChaos(
    "late-go",
    0x57e9,
    null,
    {
      backendFor: async () => backend,
      registerRelease: (close) => {
        release = close;
      },
      go: () => go,
      post: (message) => {
        if (message.chaos === "intent") announced();
        if (message.chaos === "began") began++;
      },
    },
    { mode: "concurrent", lanes: ["log"] },
  ).catch((error: unknown) => {
    failure = error;
  });
  await intent;
  await release();
  grant();
  for (let n = 0; n < 10; n++) await Promise.resolve();
  reach.recordHistory(["go in transit"]);
  expect(began, "a go in transit is not admission after stop").toBe(0);
  expect(failure).toBeNull();
});
