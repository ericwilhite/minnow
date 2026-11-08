import { chaosPoster } from "./next-storage-post.js";
/**
 * The kill campaign's page controller (next-storage-run.ts) under a scheduling double (review
 * B1-p2g findings 1–3): a Worker and MessageChannel whose campaign messages can be delivered late
 * (in order, as a port delivers), lost, or posted after the kill decision, with the worker's
 * claimed-message counter in shared memory as in the browser. The controller is the exported one.
 *
 * - A failure or a strict acknowledgement posted before the kill is received through the
 *   observation barrier however late it arrives; a message posted before the kill that never
 *   arrives fails the campaign loudly; claims after the counter read are not waited for.
 * - A step that posted `began` with no platform request open lands in its window, not inside it.
 * - The failure-path regression kills once the counter shows the injected failure posted.
 */
import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../src/testing/seeds.js";
import {
  createReach,
  STORAGE_PAGE_BOUNDARY_REACH,
  STORAGE_PHASE_CONTROLLER_REACH,
} from "../src/testing/reach.js";
import type { ChaosMessage, ChaosState } from "./next-storage-chaos.js";
import {
  chaosBegin,
  chaosKill,
  chaosKillAfter,
  chaosKillAtFault,
  chaosPageBoundary,
  chaosWaitForPageIdle,
  setChaosBarrierTimeout,
} from "./next-storage-run.js";

const phaseReach = createReach("request phase controller", STORAGE_PHASE_CONTROLLER_REACH);

const sleep = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** One end of a channel: delivers in posting order, each message after its delay (or never). */
class PortDouble {
  onmessage: ((event: { data: unknown }) => void) | null = null;
  peer: PortDouble | null = null;
  closed = false;
  #queue: Promise<void> = Promise.resolve();
  /** The delivery delay of a message posted on this end; Infinity: lost (and all after it). */
  delayOf: (data: unknown) => number = () => 0;
  afterEnqueue: () => void = () => undefined;
  beforeDelivery: (data: unknown) => Promise<void> = () => Promise.resolve();

  postMessage(data: unknown): void {
    const peer = this.peer;
    const delay = this.delayOf(data);
    this.#queue = this.#queue.then(async () => {
      if (delay === Infinity) await new Promise<never>(() => undefined);
      await this.beforeDelivery(data);
      await sleep(delay);
      if (peer !== null && !peer.closed) peer.onmessage?.({ data });
    });
    this.afterEnqueue();
  }

  close(): void {
    this.closed = true;
  }
}

class ChannelDouble {
  readonly port1 = new PortDouble();
  readonly port2 = new PortDouble();
  constructor() {
    this.port1.peer = this.port2;
    this.port2.peer = this.port1;
  }
}

const RECOVERED: ChaosState = { round: 0, identity: "0:811c9dc5", generation: 1, flushed: 0 };

/** The worker side a scenario scripts: numbered posts and the shared counter, as the worker's. */
interface Scripted {
  post(message: ChaosMessage): void;
  /** Called with each go the page gives. */
  onGo: (sequence: number) => void;
  /** Called when the page terminates the worker (still able to post: termination is async). */
  onTerminate: () => void;
  readonly port: PortDouble;
  readonly gate: Int32Array;
}

let scenario: (worker: Scripted) => void = () => undefined;

class WorkerDouble {
  readonly #listeners = new Map<string, (event: { data: unknown }) => void>();
  #scripted: Scripted | null = null;
  #dead = false;

  constructor() {
    queueMicrotask(() => this.#listeners.get("message")?.({ data: "next-storage-ready" }));
  }

  addEventListener(type: string, listener: (event: { data: unknown }) => void): void {
    this.#listeners.set(type, listener);
  }

  postMessage(message: {
    chaosPort?: PortDouble;
    counter?: SharedArrayBuffer;
    killGate?: SharedArrayBuffer;
    request?: { op: string };
  }): void {
    if (message.chaosPort !== undefined) {
      const port = message.chaosPort;
      const counter = new Int32Array(message.counter ?? new SharedArrayBuffer(4));
      const post = chaosPoster(port, counter);
      const scripted: Scripted = {
        gate: new Int32Array(message.killGate ?? new SharedArrayBuffer(16)),
        port,
        post: (chaos) => {
          if (this.#dead) return;
          try {
            post(chaos);
          } catch (error) {
            if (error !== PAUSED) throw error;
          }
        },
        onGo: () => undefined,
        onTerminate: () => undefined,
      };
      port.onmessage = (event) => {
        const go = (event.data as { go?: number }).go;
        if (go !== undefined) scripted.onGo(go);
      };
      this.#scripted = scripted;
      return;
    }
    if (message.request?.op === "chaos" && this.#scripted !== null) {
      const scripted = this.#scripted;
      scripted.post({
        chaos: "recovered",
        recovery: {
          found: { round: 0, generation: 1, logLength: 0 },
          state: RECOVERED,
          blobs: 0,
          verifiedLogBytes: 0,
          openMs: 0,
          stolenLock: false,
        },
      });
      scenario(scripted);
    }
  }

  terminate(): void {
    this.#scripted?.onTerminate();
    this.#dead = true;
  }
}

const saved = { Worker: globalThis.Worker, MessageChannel: globalThis.MessageChannel };
beforeEach(() => {
  Object.assign(globalThis, { Worker: WorkerDouble, MessageChannel: ChannelDouble });
  setChaosBarrierTimeout(10_000);
});
afterEach(() => {
  Object.assign(globalThis, saved);
});

const PAUSED = new Error("worker paused after postMessage, before its next instruction");

const ACKED: ChaosState = { round: 1, identity: "1:acked", generation: 1, flushed: 0 };

describe("the observation barrier (finding 1)", () => {
  it("a failure posted before the kill is reported, delivered 200 ms later", async () => {
    scenario = (worker) => {
      worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      worker.onGo = () => {
        worker.post({ chaos: "began", sequence: 1 });
        worker.port.delayOf = (data) => ((data as ChaosMessage).chaos === "failure" ? 200 : 0);
        worker.post({ chaos: "failure", message: "ChaosCorruption: posted before the kill" });
      };
    };
    await chaosBegin("indexeddb", "barrier-failure", 1, null, null, "concurrent");
    await expect(chaosKillAfter(20)).rejects.toThrow("posted before the kill");
  });

  it("a strict acknowledgement posted before the kill is required, delivered 200 ms later", async () => {
    scenario = (worker) => {
      worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      worker.onGo = () => {
        worker.post({ chaos: "began", sequence: 1 });
        worker.port.delayOf = (data) => ((data as ChaosMessage).chaos === "ack" ? 200 : 0);
        worker.post({ chaos: "ack", state: ACKED });
      };
    };
    await chaosBegin("indexeddb", "barrier-ack", 1, null, null, "concurrent");
    const landing = await chaosKillAfter(20);
    expect(landing.expect.state).toEqual(ACKED);
    expect(landing.observation.received).toBeGreaterThanOrEqual(landing.observation.posted);
  });

  it("requires a strict ack when interrupted between postMessage and the next instruction", async () => {
    let postedAck!: () => void;
    const ack = new Promise<void>((resolve) => {
      postedAck = resolve;
    });
    scenario = (worker) => {
      worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      worker.onGo = () => {
        worker.post({ chaos: "began", sequence: 1 });
        worker.port.delayOf = () => 200;
        worker.port.afterEnqueue = () => {
          postedAck();
          throw PAUSED;
        };
        worker.post({ chaos: "ack", state: ACKED });
      };
    };
    await chaosBegin("indexeddb", "post-counter-gap", 1, null, null, "concurrent");
    await ack;
    const landing = await chaosKillAfter(0);
    expect(landing.expect.state).toEqual(ACKED);
  });

  it("a message posted before the kill that never arrives fails the campaign loudly", async () => {
    setChaosBarrierTimeout(200);
    scenario = (worker) => {
      worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      worker.onGo = () => {
        worker.port.delayOf = () => Infinity;
        worker.post({ chaos: "ack", state: ACKED });
      };
    };
    await chaosBegin("indexeddb", "barrier-lost", 1, null, null, "concurrent");
    await expect(chaosKillAfter(20)).rejects.toThrow("observation barrier");
  });

  it("what the worker posts after the counter was read is not waited for (the tail)", async () => {
    scenario = (worker) => {
      worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      worker.onGo = () => {
        worker.post({ chaos: "began", sequence: 1 });
      };
      // Termination is asynchronous: the worker posts once more, after the page read the
      // counter, and that message is delivered only much later.
      worker.onTerminate = () => {
        worker.port.delayOf = () => 5_000;
        worker.post({ chaos: "ack", state: ACKED });
      };
    };
    await chaosBegin("indexeddb", "barrier-tail", 1, null, null, "concurrent");
    const started = performance.now();
    const landing = await chaosKillAfter(20);
    expect(performance.now() - started).toBeLessThan(1_000);
    // Unobserved: the step stays in flight, so recovery may find it done or not.
    expect(landing.expect.state).toEqual(RECOVERED);
    expect(landing.inFlight).toEqual(["publish"]);
    expect(landing.observation.received).toBe(landing.observation.posted);
  });
});

describe("inside storage needs an open platform request (finding 2)", () => {
  const killedAt = async (requests: boolean) => {
    scenario = (worker) => {
      // After the test armed its kill (serial mode answers intents until then).
      setTimeout(() => {
        worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      }, 50);
      worker.onGo = () => {
        worker.post({ chaos: "began", sequence: 1 });
        if (requests)
          worker.post({
            chaos: "request",
            id: 1,
            phase: "begin",
            target: "data+data-index+roots",
            op: "readwrite/strict",
          });
      };
    };
    await chaosBegin("indexeddb", "landing", 1, null);
    return chaosKill({ target: "publish", occurrence: 1, delayMs: 20, mode: "terminate" });
  };

  it("began and no request open: the kill lands in the step's window", async () => {
    const landing = await killedAt(false);
    expect(landing?.landed).toBe("window-publish");
    expect(landing?.requests).toEqual([]);
  });

  it("began and a transaction open: the kill lands inside the step", async () => {
    const landing = await killedAt(true);
    expect(landing?.landed).toBe("in-publish");
    expect(landing?.requests).toEqual(["data+data-index+roots:readwrite/strict"]);
  });

  it("concurrent: windows and open requests are reported apart", async () => {
    let releaseSecondGo!: () => void;
    const secondGo = new Promise<void>((resolve) => {
      releaseSecondGo = resolve;
    });
    let markersPosted!: () => void;
    const markers = new Promise<void>((resolve) => {
      markersPosted = resolve;
    });
    scenario = (worker) => {
      // Deliberately hold the second lane: wall time cannot establish that it began.
      const pagePort = worker.port.peer;
      if (pagePort === null) throw new Error("no page port");
      pagePort.beforeDelivery = (data) =>
        (data as { go?: number }).go === 2 ? secondGo : Promise.resolve();
      worker.post({ chaos: "intent", intent: { step: "write", sequence: 1 } });
      worker.post({ chaos: "intent", intent: { step: "spill", sequence: 2 } });
      worker.onGo = (sequence) => {
        worker.post({ chaos: "began", sequence });
        if (sequence === 2) {
          worker.post({
            chaos: "request",
            id: 7,
            phase: "begin",
            target: "spill+spill-index",
            op: "readwrite/relaxed",
          });
          markersPosted();
        }
      };
    };
    await chaosBegin("indexeddb", "windows", 1, null, null, "concurrent");
    try {
      releaseSecondGo();
      await markers;
      const landing = await chaosKillAfter(0);
      expect(landing.inOperation).toEqual(["spill", "write"]);
      expect(landing.requests).toEqual(["spill+spill-index:readwrite/relaxed"]);
    } finally {
      releaseSecondGo();
    }
  });
});

describe("the failure-path regression kills once the failure is posted (finding 3)", () => {
  for (const mode of ["terminate", "page"] as const)
    it(`${mode}: the injected failure, posted before the kill and delivered after, is reported`, async () => {
      scenario = (worker) => {
        // After the test armed its kill.
        setTimeout(() => {
          worker.post({ chaos: "intent", intent: { step: "spill", sequence: 1 } });
        }, 50);
        worker.onGo = () => {
          worker.post({ chaos: "began", sequence: 1 });
          worker.post({ chaos: "injecting" });
          worker.port.delayOf = (data) => ((data as ChaosMessage).chaos === "failure" ? 200 : 0);
          worker.post({ chaos: "failure", message: "ChaosCorruption: injected" });
        };
      };
      await chaosBegin("indexeddb", `fault-${mode}`, 1, null, "spill-mismatch");
      const outcome = await chaosKillAtFault(mode);
      expect(outcome.failure).toContain("injected");
      expect(outcome.receivedAtDecision).toBeLessThan(outcome.failureMessage);
      expect(outcome.observation.posted).toBeGreaterThanOrEqual(outcome.failureMessage);
    });
});

// A native transaction can complete before any page timer runs. The worker obeys the shared
// request gate instead: the prefix is fully posted before PARKED, and the request stays open.
for (const seed of seedsFor("storage-request-phase-kills", [1701, 1702, 1703, 1704]))
  for (const target of ["publish", "truncate", "log-flush"] as const)
    it(`parks inside the actual short ${target} request (${String(seed)})`, async () => {
      phaseReach.beginHistory();
      let parked = false;
      let announce: () => void = () => undefined;
      scenario = (worker) => {
        announce = () => worker.post({ chaos: "intent", intent: { step: target, sequence: 1 } });
        worker.onGo = () => {
          worker.post({ chaos: "began", sequence: 1 });
          worker.post({
            chaos: "request",
            id: seed,
            phase: "begin",
            target: target === "publish" ? "roots" : "log+log-index",
            op: "readwrite/strict",
          });
          if (Atomics.load(worker.gate, 0) === 1) {
            parked = true;
            Atomics.store(worker.gate, 0, 2);
            return;
          }
          worker.post({
            chaos: "request",
            id: seed,
            phase: "end",
            target: "log",
            op: "readwrite/strict",
          });
          worker.post({ chaos: "settled", sequence: 1 });
          worker.post({ chaos: "done", sequence: 1 });
        };
      };
      await chaosBegin("indexeddb", `phase-${target}-${String(seed)}`, seed, null);
      const plan = {
        target,
        occurrence: 1,
        delayMs: seed % 8,
        mode: "terminate" as const,
        atRequest: true,
      };
      const killing = chaosKill(plan);
      announce();
      const landing = await killing;
      expect(landing?.landed).toBe(`in-${target}`);
      expect(parked, "worker must park after a real request begins").toBe(true);
      expect(landing?.requests).toHaveLength(1);
      phaseReach.reach("short-request-phase-parked");
      expect(landing?.observation.received).toBe(landing?.observation.posted);
    });

for (const seed of seedsFor("storage-noop-request-gates", [1701, 1702, 1703, 1704]))
  it(`advances past a no-request occurrence without granting reach (${String(seed)})`, async () => {
    phaseReach.beginHistory();
    let announce: () => void = () => undefined;
    scenario = (worker) => {
      announce = () => worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      worker.onGo = (sequence) => {
        worker.post({ chaos: "began", sequence });
        if (sequence === 1) {
          worker.post({ chaos: "settled", sequence });
          worker.post({ chaos: "done", sequence });
          worker.post({ chaos: "intent", intent: { step: "publish", sequence: 2 } });
        } else {
          worker.post({
            chaos: "request",
            id: seed,
            phase: "begin",
            target: "roots",
            op: "readwrite/strict",
          });
          expect(Atomics.load(worker.gate, 1)).toBe(2);
          Atomics.store(worker.gate, 0, 2);
        }
      };
    };
    await chaosBegin("indexeddb", `no-request-${String(seed)}`, seed, null);
    const killing = chaosKill({
      target: "publish",
      occurrence: 1,
      delayMs: 0,
      mode: "terminate",
      atRequest: true,
    });
    announce();
    const landing = await killing;
    expect(landing?.landed).toBe("in-publish");
    expect(landing?.expect.inFlight.map((v) => v.sequence)).toEqual([2]);
    phaseReach.reach("no-request-occurrence-advanced");
  });

for (const seed of seedsFor("storage-parked-prefix-drain", [1701, 1702, 1703, 1704]))
  it(`drains a parked worker's complete prefix before teardown (${String(seed)})`, async () => {
    phaseReach.beginHistory();
    let announce: () => void = () => undefined;
    let delivered = 0;
    scenario = (worker) => {
      const page = worker.port.peer;
      if (page === null) throw Error("missing page port");
      const receive = page.onmessage;
      page.onmessage = (event) => {
        delivered++;
        receive?.(event);
      };
      announce = () => worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
      worker.onGo = () => {
        worker.port.delayOf = () => 10 + (seed % 5);
        worker.post({ chaos: "began", sequence: 1 });
        worker.post({ chaos: "ack", state: ACKED });
        worker.post({
          chaos: "request",
          id: seed,
          phase: "begin",
          target: "roots",
          op: "readwrite/strict",
        });
        Atomics.store(worker.gate, 0, 2);
      };
      // The recovery message was already queued before this callback installed the observer.
      worker.onTerminate = () =>
        expect(delivered, "every posted message is delivered before teardown").toBe(5);
    };
    await chaosBegin("indexeddb", `parked-prefix-${String(seed)}`, seed, null);
    const killing = chaosKill({
      target: "publish",
      occurrence: 1,
      delayMs: 0,
      mode: "terminate",
      atRequest: true,
    });
    announce();
    const landing = await killing;
    expect(landing?.expect.state).toEqual(ACKED);
    phaseReach.reach("parked-prefix-drained-before-teardown");
    expect(landing?.observation.received).toBe(landing?.observation.posted);
  });

afterAll(() => phaseReach.assertReached());

const pageReach = createReach("page-boundary acknowledgements", STORAGE_PAGE_BOUNDARY_REACH);
afterAll(() => pageReach.assertReached());

// Acknowledgements may arrive between recovery and arming, as well as after arming.
for (const beforeArm of [0, 1] as const)
  for (const seed of seedsFor("storage-page-boundary-acks", [1701, 1702, 1703, 1704]))
    it(`includes all page-boundary acknowledgements (${String(beforeArm)} before arming, seed ${String(seed)})`, async () => {
      pageReach.beginHistory();
      let announce: () => void = () => undefined;
      let acknowledged = RECOVERED;
      let gos = 0;
      let blocked!: () => void;
      const blockedAck = new Promise<void>((resolve) => {
        blocked = resolve;
      });
      let resume!: () => void;
      const resumeAck = new Promise<void>((resolve) => {
        resume = resolve;
      });
      scenario = (worker) => {
        const random = mulberry32(seed);
        worker.port.delayOf = () => Math.floor(random() * 4);
        const page = worker.port.peer;
        if (page === null) throw Error("missing page port");
        const receive = page.onmessage;
        page.onmessage = (event) => {
          receive?.(event);
          const message = event.data as ChaosMessage;
          if (message.chaos === "ack") acknowledged = message.state;
        };
        worker.port.beforeDelivery = async (data) => {
          const message = data as ChaosMessage;
          if (message.chaos === "ack" && message.state.round === beforeArm + 1) {
            blocked();
            await resumeAck;
          }
        };
        announce = () => {
          if (beforeArm > 0) worker.post({ chaos: "ack", state: ACKED });
          worker.post({ chaos: "intent", intent: { step: "publish", sequence: 1 } });
        };
        worker.onGo = () => {
          gos++;
          worker.post({ chaos: "began", sequence: 1 });
          worker.post({ chaos: "ack", state: { ...ACKED, round: beforeArm + 1 } });
          worker.post({ chaos: "settled", sequence: 1 });
          worker.post({ chaos: "done", sequence: 1 });
          worker.post({ chaos: "intent", intent: { step: "write", sequence: 2 } });
        };
      };
      const recovered = await chaosBegin("indexeddb", `page-boundary-${String(seed)}`, seed, null);
      const arming = chaosKill({ target: "publish", occurrence: 1, delayMs: 0, mode: "page" });
      announce();
      await arming;
      await blockedAck;
      let idle = false;
      const waiting = chaosWaitForPageIdle().then((state) => {
        idle = true;
        return state;
      });
      await Promise.resolve();
      await Promise.resolve();
      expect(idle, "the active publish has not acknowledged yet").toBe(false);
      pageReach.reach("waits for the armed acknowledgement");
      resume();
      const lastAck = await waiting;
      const boundary = await chaosPageBoundary();
      expect(boundary.landed).toBe("between");
      expect(boundary.expect.inFlight).toEqual([]);
      expect(boundary.expect.state.round).toBeGreaterThanOrEqual(recovered.state.round + 1);
      expect(boundary.expect.state).toEqual(acknowledged);
      expect(lastAck).toEqual(acknowledged);
      expect(gos, "the held next intent must not run").toBe(1);
      pageReach.reach("next intent held");
      pageReach.reach(beforeArm === 0 ? "one root since recovery" : "two roots since recovery");
    });
