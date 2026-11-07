import { afterAll, expect, it, vi } from "vitest";
import { createReach, STORAGE_REQUEST_GATE_REACH } from "../src/testing/reach.js";
import { seedsFor } from "../src/testing/seeds.js";
import { armRequestGate, ChaosRequestGate, gatedChaosPoster } from "./next-storage-gate.js";
import type { ChaosMessage } from "./next-storage-chaos.js";

const reach = createReach("native request kill gate", STORAGE_REQUEST_GATE_REACH);
for (const seed of seedsFor("storage-request-gates", [1701, 1702, 1703, 1704])) {
  for (const asynchronous of [false, true])
    it(`parks only after the request's complete posted prefix (${String(seed)}, async=${String(asynchronous)})`, async () => {
      reach.beginHistory();
      const control = new Int32Array(new SharedArrayBuffer(16));
      const counter = new Int32Array(new SharedArrayBuffer(4));
      const posted: Array<ChaosMessage & { n: number }> = [];
      let bodyIssued = false,
        parks = 0;
      const gate = new ChaosRequestGate(control, asynchronous, () => {
        parks++;
        expect(posted.at(-1)).toMatchObject({ chaos: "request", phase: "begin", id: seed });
        expect(Atomics.load(counter, 0)).toBe(posted.length);
        expect(posted.at(-1)?.n).toBe(posted.length);
        if (asynchronous) expect(bodyIssued).toBe(true);
        reach.reach("prefix-fully-posted");
        if (asynchronous) reach.reach("request-after-body-issue");
      });
      const post = gatedChaosPoster(
        { postMessage: (message: ChaosMessage & { n: number }) => posted.push(message) },
        counter,
        gate,
      );
      post({ chaos: "began", sequence: 1 });
      armRequestGate(control, 1);
      await Promise.resolve();
      expect(parks).toBe(0);
      reach.reach("no-credit-for-window");
      post({ chaos: "request", id: seed, phase: "begin", target: "roots", op: "readwrite/strict" });
      bodyIssued = true;
      await Promise.resolve();
      expect(parks).toBe(1);
      expect(Atomics.load(control, 0)).toBe(2);
    });
  it(`waits for the actual queued batch and refuses ended/mismatched requests (${String(seed)})`, async () => {
    reach.beginHistory();
    const control = new Int32Array(new SharedArrayBuffer(16));
    let parks = 0;
    const gate = new ChaosRequestGate(control, true, () => {
      parks++;
    });
    const post = gatedChaosPoster({ postMessage: () => undefined }, null, gate);
    post({ chaos: "began", sequence: 1 });
    armRequestGate(control, 1);
    post({ chaos: "request", id: 1, phase: "begin", target: "data", op: "readwrite/relaxed" });
    post({ chaos: "request", id: 1, phase: "end", target: "data", op: "readwrite/relaxed" });
    await Promise.resolve();
    expect(parks).toBe(0);
    reach.reach("no-credit-for-ended-request");
    let returned = false;
    const queued = gate.queuedWrite().then(() => {
      returned = true;
    });
    await Promise.resolve();
    expect(returned).toBe(false);
    post({
      chaos: "request",
      id: 2,
      phase: "begin",
      target: "data+data-index",
      op: "readwrite/relaxed",
    });
    await queued;
    expect(parks).toBe(1);
    reach.reach("queued-write-native-batch");
    armRequestGate(control, 0, "spill", 2);
    post({ chaos: "request", id: 3, phase: "begin", target: "spill", op: "readwrite/relaxed" });
    await Promise.resolve();
    expect(parks).toBe(1);
    post({ chaos: "began", sequence: 2 });
    post({ chaos: "request", id: 4, phase: "begin", target: "roots", op: "readwrite/strict" });
    await Promise.resolve();
    expect(parks).toBe(1);
    post({
      chaos: "request",
      id: 5,
      phase: "begin",
      target: "spill+spill-index",
      op: "readwrite/relaxed",
    });
    await Promise.resolve();
    expect(parks).toBe(2);
    reach.reach("concurrent-kind-and-two-windows");
  });
}

for (const seed of seedsFor("storage-park-termination", [1701, 1702, 1703, 1704]))
  it(`returns to a VM safepoint without resuming storage after a park wake (${String(seed)})`, () => {
    reach.beginHistory();
    const control = new Int32Array(new SharedArrayBuffer(16));
    const gate = new ChaosRequestGate(control, false);
    const posted: ChaosMessage[] = [];
    const post = gatedChaosPoster(
      { postMessage: (message: ChaosMessage) => posted.push(message) },
      null,
      gate,
    );
    const terminated = new Error("VM worker termination");
    let wakes = 0,
      returned = false;
    const wait = vi.spyOn(Atomics, "wait").mockImplementation((_array, _index, _value, timeout) => {
      expect(timeout, "bounded wait lets the VM observe termination").toBe(100);
      expect(Atomics.load(control, 0)).toBe(2);
      if (++wakes === 2 + (seed % 3)) throw terminated;
      return "timed-out";
    });
    try {
      post({ chaos: "began", sequence: seed });
      armRequestGate(control, seed);
      expect(() => {
        post({
          chaos: "request",
          id: seed,
          phase: "begin",
          target: "roots",
          op: "readwrite/strict",
        });
        returned = true;
      }).toThrow(terminated);
      expect(returned).toBe(false);
      expect(posted).toHaveLength(2);
      expect(wakes).toBe(2 + (seed % 3));
      reach.reach("bounded-park-wake-never-resumes-storage");
    } finally {
      wait.mockRestore();
    }
  });

afterAll(() => reach.assertReached());
