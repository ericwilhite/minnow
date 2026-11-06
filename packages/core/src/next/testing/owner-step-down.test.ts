/**
 * The owner's step-down (DECISIONS.md "Step-down", the storage track's protocol): before its
 * worker is terminated, the owner stops starting storage work, lets what is in flight finish,
 * closes its session and awaits the fence's release. WebKit leaves IndexedDB wedged for the rest
 * of the browser session when a worker dies with transactions in flight.
 */
import fc from "fast-check";
import { OWNER_RELEASE_STATES, createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { describe, expect, it, vi } from "vitest";
import { SessionLostError } from "../errors.js";
import { ProtocolClient } from "../owner/client.js";
import { ReleaseUnsettledError } from "../storage/contract.js";
import type { ChunkReads } from "../exec/scan.js";
import { createLowerer } from "../lower/create.js";
import { Governor } from "../memory/governor.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerConnector, workerOwner } from "../owner/connector.js";
import { STEP_DOWN_BOUND_MS, hostLockName, ownerLockName } from "../owner/election.js";
import { startOwnerHost, type OwnerHostStatus, type OwnerEngineFactory } from "../owner/host.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { flushUntil, raw } from "./owner-harness.js";
import { FakeLockRegistry } from "./fake-locks.js";
import { StandinExecutor, StandinWritePath, withTestStatements } from "./owner-standins.js";

const sleep = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** A fake Worker: records what the page posts and whether it was terminated. */
function fakeWorker(answer: "stops" | "silent", ready = true) {
  const posted: Array<{ t: string }> = [];
  let terminated = 0;
  const worker = {
    onmessage: null as ((event: { data: unknown }) => void) | null,
    onerror: null,
    postMessage(message: { t: string }) {
      posted.push(message);
      if (message.t === "minnow:host" && ready)
        queueMicrotask(() => worker.onmessage?.({ data: { t: "minnow:ready", ownerId: "o" } }));
      if (message.t === "minnow:stepDown" && answer === "stops")
        setTimeout(() => {
          worker.onmessage?.({ data: { t: "minnow:stopped", reason: "pagehide", stepDownMs: 30 } });
        }, 30);
    },
    terminate() {
      terminated++;
    },
  };
  return {
    worker: worker as unknown as Worker,
    posted,
    terminated: () => terminated,
    status: (data: unknown) => worker.onmessage?.({ data }),
  };
}

const init = () => {
  const { port2 } = new MessageChannel();
  return { database: "db", build: "", clientPort: port2, brokerPort: null, settings: {} };
};

describe("the page steps its owner worker down before terminating it", () => {
  it("asks the worker to step down and terminates it once it reports stopped", async () => {
    const fake = fakeWorker("stops");
    const handle = workerOwner(() => fake.worker, { mobile: false })(init());
    await handle.ready;
    const stepping = handle.stepDown?.("pagehide");
    // Not at once: the worker drains first.
    expect(fake.terminated()).toBe(0);
    expect(fake.posted.at(-1)).toMatchObject({ t: "minnow:stepDown", reason: "pagehide" });
    await stepping;
    expect(fake.terminated()).toBe(1);
  });

  it("close during startup still awaits the worker release acknowledgement", async () => {
    const fake = fakeWorker("silent", false);
    const spawned = deferred<undefined>();
    const registry = new FakeLockRegistry();
    const start = workerOwner(() => fake.worker, { mobile: false });
    const connector = new OwnerConnector({
      database: "startup-release",
      client: new ProtocolClient(),
      locks: registry.context(),
      openBroker: null,
      startOwner(init) {
        spawned.resolve(undefined);
        return start(init);
      },
      context: () => ({
        kind: "page",
        visible: true,
        superseded: false,
        platform: { mobile: false },
      }),
    });
    const starting = connector.start().catch((error: unknown) => (error as Error).name);
    await spawned.promise;
    connector.close();
    expect(fake.terminated()).toBe(0);
    expect(fake.posted.at(-1)).toMatchObject({ t: "minnow:stepDown", reason: "closing" });
    expect(await starting).toBe("ClientClosedError");
    fake.status({ t: "minnow:stopped", reason: "closing", stepDownMs: 1 });
    await vi.waitFor(() => expect(registry.held(hostLockName("startup-release"))).toBe(false));
    expect(fake.terminated()).toBe(1);
  });

  it.each(["pagehide", "freeze", "deadline"] as const)(
    "%s during startup requests release before terminating the worker",
    async (event) => {
      const fake = fakeWorker("silent", false);
      const spawned = deferred<undefined>();
      const registry = new FakeLockRegistry();
      const start = workerOwner(() => fake.worker, { mobile: false });
      const connector = new OwnerConnector({
        database: "starting-step-down",
        client: new ProtocolClient(),
        locks: registry.context(),
        openBroker: null,
        brokerBoundMs: 50,
        startOwner(init) {
          spawned.resolve(undefined);
          return start(init);
        },
        context: () => ({
          kind: "page",
          visible: true,
          superseded: false,
          platform: { mobile: false },
        }),
      });
      const starting = connector.start().catch((error: unknown) => (error as Error).name);
      try {
        await spawned.promise;
        if (event !== "deadline") connector.lifecycle(event);
        expect(await starting).toBe("OwnerUnreachableError");
        expect(fake.terminated()).toBe(0);
        expect(fake.posted.at(-1)).toMatchObject({
          t: "minnow:stepDown",
          reason: event === "deadline" ? "closing" : event,
        });
        expect(registry.held(hostLockName("starting-step-down"))).toBe(true);
      } finally {
        fake.status({ t: "minnow:stopped", reason: "closing", stepDownMs: 1 });
        connector.close();
      }
      await vi.waitFor(() => expect(registry.held(hostLockName("starting-step-down"))).toBe(false));
      expect(fake.terminated()).toBe(1);
    },
  );

  it("keeps the worker alive through release's abort-settle window and steps down once", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const fake = fakeWorker("silent");
      const handle = workerOwner(() => fake.worker, { mobile: false })(init());
      await handle.ready;
      const stopping = handle.stepDown?.("closing");
      expect(handle.stepDown?.("pagehide")).toBe(stopping);
      await vi.advanceTimersByTimeAsync(1_600);
      expect(fake.terminated()).toBe(0);
      fake.status({ t: "minnow:stopped", reason: "closing", stepDownMs: 1600 });
      await stopping;
      fake.status({ t: "minnow:stopped", reason: "closing", stepDownMs: 1600 });
      expect(fake.terminated()).toBe(1);
      expect(fake.posted.filter((message) => message.t === "minnow:stepDown")).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("terminates a worker that does not answer, after the bound", async () => {
    const fake = fakeWorker("silent");
    const handle = workerOwner(() => fake.worker, { mobile: false })(init());
    await handle.ready;
    const started = performance.now();
    await handle.stepDown?.("pagehide");
    expect(performance.now() - started).toBeGreaterThanOrEqual(STEP_DOWN_BOUND_MS - 20);
    expect(fake.terminated()).toBe(1);
  });
});

/** An owner host over the stand-ins, its fence closing asynchronously in `closeMs`. */
async function host(closeMs: number) {
  const registry = new FakeLockRegistry();
  const locks = registry.context();
  const posted: OwnerHostStatus[] = [];
  const events: string[] = [];
  const channel = new MessageChannel();
  const started = await startOwnerHost(
    { t: "minnow:host", ...init(), database: "step", clientPort: channel.port2 },
    {
      openFence: () => ({ db: "step" }),
      closeFence: () =>
        new Promise<void>((resolve) => {
          events.push("close fence");
          setTimeout(() => {
            events.push("fence closed");
            resolve();
          }, closeMs);
        }),
      open: () =>
        composeEngine({
          planner: withTestStatements(createLowerer()),
          executor: new StandinExecutor(),
          writePath: new StandinWritePath(),
          governor: new Governor(64 * 1024 * 1024),
          spill: new MemorySpillService(),
          reads: {} as ChunkReads,
        }),
    },
    {
      locks,
      post: (status) => {
        posted.push(status);
        if (status.t === "minnow:stopped") events.push("stopped");
      },
    },
  );
  return { registry, posted, events, started, channel };
}

describe("the owner releases its fence before it reports stopped", () => {
  it("awaits release before reporting a startup failure that permits worker termination", async () => {
    const registry = new FakeLockRegistry();
    const release = deferred<undefined>();
    const entered = deferred<undefined>();
    const statuses: OwnerHostStatus[] = [];
    let ended = false;
    const opening = startOwnerHost(
      { t: "minnow:host", ...init(), database: "failed-startup-release" },
      {
        openFence: () => ({}),
        closeFence() {
          entered.resolve(undefined);
          return release.promise;
        },
        open() {
          throw new Error("startup failed");
        },
      },
      {
        locks: registry.context(),
        post: (status) => {
          statuses.push(status);
        },
      },
    )
      .catch((error: unknown) => error)
      .finally(() => {
        ended = true;
      });
    try {
      await entered.promise;
      await sleep(10);
      expect(ended).toBe(false);
      expect(statuses).toEqual([]);
      expect(registry.held(ownerLockName("failed-startup-release"))).toBe(true);
    } finally {
      release.resolve(undefined);
      await opening;
    }
    expect(await opening).toMatchObject({ message: "startup failed" });
    expect(registry.held(ownerLockName("failed-startup-release"))).toBe(false);
    expect(statuses.at(-1)).toMatchObject({ t: "minnow:failed" });
  });

  it("graceful: the fence's release is awaited before the locks go and before `stopped`", async () => {
    const { registry, posted, events, started, channel } = await host(80);
    const stopping = started.stop("pagehide", true);
    await sleep(40);
    // Mid-release: both locks still held, nothing reported.
    expect(registry.held(ownerLockName("step"))).toBe(true);
    expect(registry.held(hostLockName("step"))).toBe(true);
    expect(posted.some((status) => status.t === "minnow:stopped")).toBe(false);
    await stopping;
    await sleep(1);
    expect(events).toEqual(["close fence", "fence closed", "stopped"]);
    expect(registry.held(ownerLockName("step"))).toBe(false);
    const stopped = posted.find((status) => status.t === "minnow:stopped");
    expect(stopped).toMatchObject({ reason: "pagehide" });
    expect((stopped as { stepDownMs: number }).stepDownMs).toBeGreaterThanOrEqual(70);
    channel.port1.close();
  });

  it("forced (a stolen fence): nothing waits", async () => {
    const { registry, posted, started, channel } = await host(80);
    void started.stop("lockLost", false);
    await sleep(1);
    expect(registry.held(ownerLockName("step"))).toBe(false);
    expect(posted.some((status) => status.t === "minnow:stopped")).toBe(true);
    channel.port1.close();
  });
});

// The public lease release contract is the dependency; no IndexedDB internals are used here.

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function controlledHost(
  release: () => ReturnType<OwnerEngineFactory<unknown>["closeFence"]>,
  path = new StandinWritePath(),
  closeSession?: () => Promise<void>,
) {
  const registry = new FakeLockRegistry();
  const channel = new MessageChannel();
  const events: string[] = [];
  const statuses: OwnerHostStatus[] = [];
  const engine = composeEngine({
    planner: withTestStatements(createLowerer()),
    executor: new StandinExecutor(),
    writePath: path,
    governor: new Governor(64 * 1024 * 1024),
    spill: new MemorySpillService(),
    reads: {} as ChunkReads,
  });
  const close = engine.close.bind(engine);
  vi.spyOn(engine, "close").mockImplementation(() => {
    events.push("engine close");
    return close();
  });
  const started = await startOwnerHost(
    {
      t: "minnow:host",
      database: "controlled",
      build: "test",
      clientPort: channel.port2,
      brokerPort: null,
      settings: {},
    },
    {
      openFence: () => ({}),
      closeFence: () => {
        events.push("release");
        return release();
      },
      ...(closeSession === undefined ? {} : { closeSession }),
      open: () => engine,
    },
    {
      locks: registry.context(),
      post: (status) => {
        statuses.push(status);
      },
    },
  );
  const client = new ProtocolClient();
  await client.connect(channel.port1);
  return { client, engine, started, events, statuses, registry, path };
}

describe("the complete step-down contract", () => {
  it("awaits the factory's storage session close before releasing the lease", async () => {
    const sessionClose = deferred<undefined>();
    const release = deferred<undefined>();
    const h = await controlledHost(
      () => release.promise,
      new StandinWritePath(),
      () => {
        h.events.push("store session close");
        return sessionClose.promise;
      },
    );
    const stopping = h.started.stop("closing", true);
    try {
      await sleep(10);
      expect([...h.events]).toEqual(["engine close", "store session close"]);
      sessionClose.resolve(undefined);
      await sleep(10);
      expect([...h.events]).toEqual(["engine close", "store session close", "release"]);
      expect(h.statuses.some((status) => status.t === "minnow:stopped")).toBe(false);
    } finally {
      sessionClose.resolve(undefined);
      release.resolve(undefined);
      await stopping;
      h.client.close();
    }
  });

  it("starts no late storage-session close after an engine-close timeout released the lease", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const engineClose = deferred<undefined>();
    const sessionClose = vi.fn(() => Promise.resolve());
    const h = await controlledHost(() => undefined, new StandinWritePath(), sessionClose);
    const close = vi.spyOn(h.engine, "close").mockImplementation(() => engineClose.promise);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const stopping = h.started.stop("closing", true);
      await vi.advanceTimersByTimeAsync(500);
      expect([...h.events]).toEqual(["release"]);
      expect(h.statuses.at(-1)).toMatchObject({ reloadSuggested: true });
      await stopping;
      engineClose.resolve(undefined);
      vi.useRealTimers();
      await sleep(10);
      expect(sessionClose).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
      engineClose.resolve(undefined);
      h.client.close();
      close.mockRestore();
      await h.engine.close();
      log.mockRestore();
    }
  });

  it("drains work from a client that disconnected before shutdown", async () => {
    const h = await controlledHost(() => undefined);
    const entered = deferred<undefined>();
    const work = deferred<unknown>();
    vi.spyOn(h.engine, "admin").mockImplementation(() => {
      entered.resolve(undefined);
      return work.promise;
    });
    h.client.send({ t: "admin", op: "storageStats", args: [] }).promise.catch(() => undefined);
    try {
      await entered.promise;
      h.client.close();
      await vi.waitFor(() => expect(h.started.server.stats.connections).toBe(0));
      const stopping = h.started.stop("closing", true);
      await sleep(10);
      expect([...h.events]).toEqual([]);
      work.resolve(null);
      await stopping;
      expect([...h.events]).toEqual(["engine close", "release"]);
    } finally {
      work.resolve(null);
      h.client.close();
      await h.started.stop("closing", true);
    }
  });

  it("bounds the owner drain at 500 ms and starts no late close after releasing the lease", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const h = await controlledHost(() => undefined);
    const entered = deferred<undefined>();
    const work = deferred<unknown>();
    vi.spyOn(h.engine, "admin").mockImplementation(() => {
      entered.resolve(undefined);
      return work.promise;
    });
    const pending = h.client
      .send({ t: "admin", op: "storageStats", args: [] })
      .promise.catch((error: unknown) => error);
    await entered.promise;
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const stopping = h.started.stop("closing", true);
      await vi.advanceTimersByTimeAsync(499);
      expect([...h.events]).toEqual([]);
      await vi.advanceTimersByTimeAsync(1);
      expect([...h.events]).toEqual(["release"]);
      expect(h.statuses.at(-1)).toMatchObject({ reloadSuggested: true });
      await stopping;
      work.resolve(null);
      vi.useRealTimers();
      await sleep(10);
      expect([...h.events]).toEqual(["release"]);
      expect(await pending).toMatchObject({ name: "SessionLostError", effect: "unknown" });
      expect(log).toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
      work.resolve(null);
      h.client.close();
      await h.engine.close();
      log.mockRestore();
    }
  });

  it("refuses lifecycle flushes while release is pending and after it finishes", async () => {
    const release = deferred<undefined>();
    const entered = deferred<undefined>();
    const h = await controlledHost(() => {
      entered.resolve(undefined);
      return release.promise;
    });
    const flush = vi.spyOn(h.engine, "flushPending");
    const stopping = h.started.stop("closing", true);
    try {
      await entered.promise;
      h.started.lifecycle("hidden", { mobile: false });
      expect(flush).not.toHaveBeenCalled();
      release.resolve(undefined);
      await stopping;
      h.started.lifecycle("hidden", { mobile: false });
      expect(flush).not.toHaveBeenCalled();
    } finally {
      release.resolve(undefined);
      await stopping;
      h.client.close();
    }
  });

  it("drains work before closing the engine, and admits no later storage work", async () => {
    const release = deferred<undefined>();
    const h = await controlledHost(() => release.promise);
    const work = deferred<unknown>();
    const entered = deferred<undefined>();
    const admin = vi.spyOn(h.engine, "admin").mockImplementation(() => {
      h.events.push("work");
      entered.resolve(undefined);
      return work.promise;
    });
    const waiting = h.client
      .send({ t: "admin", op: "storageStats", args: [] })
      .promise.catch(() => undefined);
    await entered.promise;
    const stopping = h.started.stop("closing", true);
    h.client.send({ t: "admin", op: "storageStats", args: [] }).promise.catch(() => undefined);
    const newcomer = raw(h.started.server);
    expect(newcomer.replies.at(-1)).toMatchObject({ t: "refused", reason: "steppingDown" });
    await sleep(10);
    expect(h.events).toEqual(["work"]);
    work.resolve(null);
    await sleep(10);
    expect(h.events).toEqual(["work", "engine close", "release"]);
    expect(admin).toHaveBeenCalledTimes(1);
    expect(h.statuses.some((s) => s.t === "minnow:stopped")).toBe(false);
    release.resolve(undefined);
    await stopping;
    h.client.close();
    await waiting;
  });

  it("closes a busy session before it closes the engine", async () => {
    const h = await controlledHost(() => undefined);
    const entered = deferred<undefined>();
    const work = deferred<undefined>();
    const open = h.engine.openSession.bind(h.engine);
    vi.spyOn(h.engine, "openSession").mockImplementation((...args) => {
      const session = open(...args);
      vi.spyOn(session, "begin").mockImplementation(() => {
        entered.resolve(undefined);
        return work.promise;
      });
      const close = session.close.bind(session);
      vi.spyOn(session, "close").mockImplementation(() => {
        h.events.push("session close");
        close();
      });
      return session;
    });
    const active = h.client.begin(7, true).catch(() => undefined);
    await entered.promise;
    const stopping = h.started.stop("closing", true);
    work.resolve(undefined);
    await stopping;
    expect(h.events).toEqual(["session close", "engine close", "release"]);
    h.client.close();
    await active;
  });

  it("reports an unsettled release to pending strict commits as unknown, then suggests reload", async () => {
    const failure = new ReleaseUnsettledError({
      drained: 0,
      aborted: 1,
      unsettled: 1,
      transactions: [],
      waitedMs: 1500,
    });
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const path = new StandinWritePath({ autoFlush: false });
    const h = await controlledHost(() => {
      throw failure;
    }, path);
    await flushUntil(path, h.client.query("CREATE TABLE pending (id INT PRIMARY KEY)"));
    await h.client.begin(1);
    await h.client.query("INSERT INTO pending VALUES (1)", [], { session: 1 });
    const diagnostic = deferred<undefined>();
    h.client.onOwnerError = () => diagnostic.resolve(undefined);
    const pending = h.client.commit(1).catch((error: unknown) => error);
    await vi.waitFor(() => expect(path.appliedLsn).toBeGreaterThan(path.durableLsn));
    await h.started.stop("closing", true);
    expect(h.statuses.at(-1)).toMatchObject({ t: "minnow:stopped", reloadSuggested: true });
    await diagnostic.promise;
    expect(h.client.pendingCount).toBe(0);
    expect(h.client.lostSessions.has(1)).toBe(true);
    const error = await pending;
    expect(error).toBeInstanceOf(SessionLostError);
    expect(error).toMatchObject({ effect: "unknown" });
    expect(log.mock.calls.flat().map(String).join(" ")).toMatch(/reload/iu);
    await vi.waitFor(() => expect(h.registry.held(ownerLockName("controlled"))).toBe(false));
    await path.flush();
    await h.engine.close();
    h.client.close();
    log.mockRestore();
  });

  it("keeps client ports open until the release outcome can be reported", async () => {
    const entered = deferred<undefined>();
    const release = deferred<undefined>();
    const h = await controlledHost(() => {
      entered.resolve(undefined);
      return release.promise;
    });
    const peer = raw(h.started.server);
    const close = vi.fn();
    Object.assign(peer.port, { close });
    const stopping = h.started.stop("closing", true);
    await entered.promise;
    expect(close).not.toHaveBeenCalled();
    release.resolve(undefined);
    await stopping;
    expect(close).toHaveBeenCalledTimes(1);
    expect(peer.replies.at(-1)).toMatchObject({ t: "stepDown", reason: "closing" });
    h.client.close();
  });

  it("pagehide uses the same awaited release as an explicit close", async () => {
    const release = deferred<undefined>();
    const h = await controlledHost(() => release.promise);
    h.started.lifecycle("pagehide", { mobile: false });
    await sleep(10);
    expect(h.statuses.some((s) => s.t === "minnow:stopped")).toBe(false);
    expect(h.registry.held(ownerLockName("controlled"))).toBe(true);
    release.resolve(undefined);
    await h.started.stop("pagehide", true);
    expect(h.statuses.at(-1)).toMatchObject({ t: "minnow:stopped" });
    h.client.close();
  });
});

const RELEASE_SEEDS = seedsFor("next-owner-release", [0x57e9]);
it.each(RELEASE_SEEDS)(
  "generated release outcomes and shutdown reasons (seed %i)",
  async (seed) => {
    const reach = createReach("next-owner-release", OWNER_RELEASE_STATES, { minimum: 1 });
    const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            reason: fc.constantFrom(
              "freeze",
              "pagehide",
              "hidden",
              "superseded",
              "closing",
              "lockLost",
            ),
            outcome: fc.constantFrom("void", "report", "throw", "unsettled report"),
            asynchronous: fc.boolean(),
            storageSession: fc.boolean(),
            workState: fc.constantFrom("idle", "active", "disconnected"),
            drained: fc.integer({ min: 0, max: 9 }),
            aborted: fc.integer({ min: 0, max: 9 }),
          }),
          async ({
            reason,
            outcome,
            asynchronous,
            storageSession,
            workState,
            drained,
            aborted,
          }) => {
            const entered = new Set<string>();
            const releaseEntered = deferred<undefined>();
            const release = deferred<undefined>();
            const storageClose = deferred<undefined>();
            let storageClosing = false;
            let releasing = false;
            const failure = new ReleaseUnsettledError({
              drained,
              aborted,
              unsettled: 1,
              transactions: [],
              waitedMs: 1500,
            });
            const complete = () => {
              if (outcome === "throw") throw failure;
              if (outcome === "unsettled report") return { drained, aborted, unsettled: 1 };
              return outcome === "report" ? { drained, aborted } : undefined;
            };
            const h = await controlledHost(
              () => {
                releasing = true;
                releaseEntered.resolve(undefined);
                return asynchronous ? release.promise.then(complete) : complete();
              },
              new StandinWritePath(),
              storageSession
                ? () => {
                    storageClosing = true;
                    return storageClose.promise;
                  }
                : undefined,
            );
            const diagnostic = deferred<{ error: Error; context: string }>();
            h.client.onOwnerError = (event) => diagnostic.resolve(event);
            const work = deferred<unknown>();
            try {
              if (workState !== "idle") {
                const began = deferred<undefined>();
                vi.spyOn(h.engine, "admin").mockImplementation(() => {
                  began.resolve(undefined);
                  return work.promise;
                });
                h.client
                  .send({ t: "admin", op: "storageStats", args: [] })
                  .promise.catch(() => undefined);
                await began.promise;
                if (workState === "disconnected") {
                  h.client.close();
                  await vi.waitFor(() => expect(h.started.server.stats.connections).toBe(0));
                }
              }
              const logCount = log.mock.calls.length;
              const stopping = h.started.stop(reason, true);
              if (workState !== "idle") {
                await sleep(5);
                expect([...h.events]).toEqual([]);
                work.resolve(null);
                entered.add(
                  workState === "active" ? "active work drains" : "disconnected work drains",
                );
              }
              if (storageSession) {
                await sleep(5);
                expect(storageClosing).toBe(true);
                expect(releasing).toBe(false);
                storageClose.resolve(undefined);
                entered.add("storage session close awaited");
              }
              await releaseEntered.promise;
              if (asynchronous) {
                expect(h.statuses.some((s) => s.t === "minnow:stopped")).toBe(false);
                expect(h.registry.held(ownerLockName("controlled"))).toBe(true);
                entered.add("release pending");
              }
              release.resolve(undefined);
              await stopping;
              const status = h.statuses.at(-1);
              expect(status).toMatchObject({ t: "minnow:stopped", reason });
              await vi.waitFor(() =>
                expect(h.registry.held(ownerLockName("controlled"))).toBe(false),
              );
              if (outcome === "throw" || outcome === "unsettled report") {
                expect(status).toMatchObject({ reloadSuggested: true });
                expect(log.mock.calls.length).toBeGreaterThan(logCount);
                if (workState !== "disconnected") {
                  const report = await diagnostic.promise;
                  expect(report.error).toBeInstanceOf(SessionLostError);
                  expect(report.error).toMatchObject({ effect: "unknown" });
                  expect(report.context).toMatch(/reload/iu);
                }
                entered.add(
                  outcome === "unsettled report"
                    ? "unsettled report"
                    : asynchronous
                      ? "async unsettled"
                      : "sync unsettled",
                );
              } else {
                expect(status).not.toHaveProperty("reloadSuggested");
                entered.add(outcome === "report" ? "clean report" : "clean void");
              }
              entered.add(reason);
              reach.recordHistory(entered);
            } finally {
              work.resolve(null);
              storageClose.resolve(undefined);
              release.resolve(undefined);
              h.client.close();
            }
          },
        ),
        { seed, numRuns: 100 },
      );
      reach.assertReached();
    } finally {
      log.mockRestore();
    }
  },
  60_000,
);
