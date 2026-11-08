/** Isolated R6 experiment: identical CPU work, different task yields; no SQL or owner protocol. */
import { yieldTask } from "../../dist/next/task.js";

export interface SchedulingVariant {
  readonly name: string;
  readonly quantumMs: number;
  readonly scheduler: "fixture" | "shared";
}

export interface SchedulingWork {
  readonly isolated: boolean;
  readonly units: number;
  readonly checksum: number;
  readonly busyMs: number;
  readonly wallMs: number;
  readonly yields: number;
}

type Input = { readonly port: MessagePort } | { readonly variant: SchedulingVariant };
const scope = self as unknown as {
  onmessage: ((event: MessageEvent<Input>) => void) | null;
  postMessage(message: unknown): void;
};
const WORK_UNITS = 1_000_000;

/** The latency fixture's original yield, scoped to one worker's lifetime. */
function fixtureYield(): Promise<void> {
  return new Promise((resolve) => {
    const channel = new MessageChannel();
    channel.port1.onmessage = () => {
      channel.port1.close();
      resolve();
    };
    channel.port2.postMessage(null);
  });
}

async function work(variant: SchedulingVariant): Promise<SchedulingWork> {
  const start = performance.now();
  let units = 0;
  let checksum = 12345;
  let busyMs = 0;
  let yields = 0;
  const pause = variant.scheduler === "fixture" ? fixtureYield : yieldTask;
  while (units < WORK_UNITS) {
    const sliceStart = performance.now();
    do {
      // Fixed instruction count and observable checksum: shorter slices cannot reduce the work.
      for (let i = 0; i < 1024; i++) checksum = (Math.imul(checksum ^ i, 1664525) + 1013904223) | 0;
      units++;
    } while (units < WORK_UNITS && performance.now() - sliceStart < variant.quantumMs);
    busyMs += performance.now() - sliceStart;
    await pause();
    yields++;
  }
  return {
    isolated: crossOriginIsolated,
    units,
    checksum,
    busyMs,
    wallMs: performance.now() - start,
    yields,
  };
}

scope.onmessage = (event) => {
  const message = event.data;
  if ("port" in message) {
    const port = message.port;
    port.onmessage = (event: MessageEvent<unknown>) => {
      port.postMessage(event.data);
    };
    scope.postMessage({ ready: true });
  } else {
    scope.postMessage({ started: true });
    void work(message.variant).then((result) => {
      scope.postMessage({ done: result });
    });
  }
};
