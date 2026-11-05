/**
 * The browser lane's page API for the next core's owner (browser-tests/next-owner.spec.ts): opens
 * a protocol client with its connector (election, broker, lifecycle) and runs statements, so the
 * spec can drive several real tabs.
 */
import { ProtocolClient, type ClientResult } from "../../dist/next/owner/client.js";
import {
  OwnerConnector,
  relayPageLifecycle,
  workerOwner,
} from "../../dist/next/owner/connector.js";
import type { LifecycleEvent } from "../../dist/next/owner/election.js";
import type { WireValue } from "../../dist/next/owner/protocol.js";

interface Opened {
  readonly name: string;
  readonly client: ProtocolClient;
  readonly connector: OwnerConnector;
  readonly state: { superseded: boolean };
}

let opened: Opened | null = null;
const sent = new Map<string, Promise<ClientResult>>();

function current(): Opened {
  if (opened === null) throw new Error("open() first");
  return opened;
}

export interface OpenOptions {
  readonly protocol?: number;
  readonly settings?: Readonly<Record<string, unknown>>;
  readonly mobile?: boolean;
}

/** This page's Web Lock calls (the connector's), for the latency lane. */
let pageLockCalls = 0;
const countedLocks = {
  request: (
    name: string,
    options: LockOptions,
    callback: LockGrantedCallback<unknown>,
  ): Promise<unknown> => {
    pageLockCalls++;
    return navigator.locks.request(name, options, callback);
  },
  query: () => navigator.locks.query(),
};

/** Web Lock calls so far: this page's, and the owner worker's (asked over a channel). */
export async function lockCalls(): Promise<{
  readonly page: number;
  readonly owner: number | null;
}> {
  const channel = new BroadcastChannel("minnow-next-test:stats");
  const owner = await new Promise<number | null>((resolve) => {
    const timer = setTimeout(() => {
      resolve(null);
    }, 500);
    channel.onmessage = (event: MessageEvent) => {
      const message = event.data as { t?: string; count?: number };
      if (message.t !== "lockCalls") return;
      clearTimeout(timer);
      resolve(message.count ?? null);
    };
    channel.postMessage({ t: "lockCalls?" });
  });
  channel.close();
  return { page: pageLockCalls, owner };
}

export async function open(name: string, options: OpenOptions = {}): Promise<{ hosting: boolean }> {
  const mobile = options.mobile ?? false;
  const state = { superseded: false };
  const client = new ProtocolClient(
    options.protocol === undefined ? {} : { protocol: options.protocol },
  );
  const connector = new OwnerConnector({
    database: name,
    client,
    locks: countedLocks,
    openBroker: () =>
      new SharedWorker(new URL("./broker-worker.ts", import.meta.url), {
        type: "module",
        name: "minnow-next-broker",
      }).port,
    startOwner: workerOwner(
      () => new Worker(new URL("./owner-worker.ts", import.meta.url), { type: "module" }),
      { mobile },
    ),
    context: () => ({
      kind: "page",
      visible: document.visibilityState === "visible",
      platform: { mobile },
      superseded: state.superseded,
    }),
    settings: options.settings ?? {},
    retryMs: 200,
    onSuperseded: () => {
      state.superseded = true;
    },
  });
  relayPageLifecycle(connector, window);
  opened = { name, client, connector, state };
  await connector.start();
  return { hosting: connector.hosting };
}

const plain = (result: ClientResult) => ({
  rows: result.rows(),
  summary: result.summary,
});

const errorOf = (error: unknown) => ({
  error: error instanceof Error ? error.name : "Error",
  message: error instanceof Error ? error.message : String(error),
});

export async function query(sql: string, params: WireValue[] = [], session = 0) {
  try {
    return plain(await current().client.query(sql, params, { session }));
  } catch (error) {
    return errorOf(error);
  }
}

export async function begin(session: number) {
  return plain(await current().client.begin(session));
}

/** Sends without waiting; `settle(key)` later. */
export function send(key: string, sql: string, params: WireValue[] = [], session = 0): void {
  const promise =
    sql === "COMMIT" && session !== 0
      ? current().client.commit(session)
      : current().client.query(sql, params, { session });
  promise.catch(() => undefined);
  sent.set(key, promise);
}

export async function settle(key: string) {
  const promise = sent.get(key);
  if (promise === undefined) throw new Error(`nothing sent as ${key}`);
  try {
    return plain(await promise);
  } catch (error) {
    return errorOf(error);
  }
}

export function lifecycle(event: LifecycleEvent): void {
  current().connector.lifecycle(event);
}

export function status() {
  const { connector, state } = current();
  return { status: connector.status, hosting: connector.hosting, superseded: state.superseded };
}

/** Posts a test knob to the owner worker of `name` (owner-worker.ts). */
export function knob(name: string, message: Readonly<Record<string, unknown>>): void {
  const channel = new BroadcastChannel(`minnow-next-test:${name}`);
  channel.postMessage(message);
  channel.close();
}

/** Deletes the lane's durable stand-in state for `name` (OPFS file and IndexedDB database). */
export async function wipe(name: string): Promise<void> {
  const file = `minnow-next-standin-${name}`;
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- WebKit's pinned Linux build has no navigator.storage
  if (typeof navigator.storage?.getDirectory === "function")
    await (await navigator.storage.getDirectory()).removeEntry(file).catch(() => undefined);
  await new Promise<void>((resolve) => {
    const request = indexedDB.deleteDatabase(file);
    request.onsuccess =
      request.onerror =
      request.onblocked =
        () => {
          resolve();
        };
  });
}

/**
 * The platform baseline: bare round trips from this tab to the broker SharedWorker (another
 * process), timed like `pointReads`. Returns µs quantiles.
 */
export async function brokerRoundTrips(
  count: number,
  warmup = 200,
  /** Carry a point-read-sized frame (four small typed arrays): transferred, or cloned. */
  payload: "none" | "transfer" | "clone" = "none",
) {
  const port = new SharedWorker(new URL("./broker-worker.ts", import.meta.url), {
    type: "module",
    name: "minnow-next-broker",
  }).port;
  const times: number[] = [];
  let nonce = 0;
  for (let i = 0; i < warmup + count; i++) {
    const start = performance.now();
    await new Promise<void>((resolve) => {
      port.onmessage = () => {
        resolve();
      };
      if (payload === "none") port.postMessage({ t: "ping", v: 1, nonce: ++nonce });
      else {
        const arrays = [
          Int32Array.of(1),
          Int32Array.of(2),
          new TextEncoder().encode("name 1"),
          Int32Array.of(0, 6),
        ];
        port.postMessage(
          { t: "ping", v: 1, nonce: ++nonce, arrays },
          payload === "transfer" ? arrays.map((array) => array.buffer) : [],
        );
      }
    });
    if (i >= warmup) times.push((performance.now() - start) * 1000);
  }
  port.close();
  times.sort((a, b) => a - b);
  return { count: times.length, p50: quantile(times, 0.5), p95: quantile(times, 0.95) };
}

/** Whether this page has fine timers (cross-origin isolated), and the clock's observed step. */
export function timerResolution(): { readonly isolated: boolean; readonly stepUs: number } {
  let step = Infinity;
  let last = performance.now();
  for (let i = 0; i < 100_000 && step > 0.001; i++) {
    const now = performance.now();
    if (now > last) {
      step = Math.min(step, now - last);
      last = now;
    }
  }
  return { isolated: globalThis.crossOriginIsolated, stepUs: step * 1000 };
}

function quantile(sorted: readonly number[], q: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? Number.NaN;
}

/**
 * Warm prepared point reads from this tab: `count` reads after `warmup`, timed from the call to
 * its resolution (the R6 measure). Returns µs quantiles.
 */
export async function pointReads(count: number, warmup = 200, keys = 100) {
  const { client } = current();
  const prepared = await client.prepare("SELECT * FROM t WHERE id = $1");
  const first = await client.execute(prepared.id, [1]);
  const version = first.summary.catalogVersion;
  const times: number[] = [];
  for (let i = 0; i < warmup + count; i++) {
    const start = performance.now();
    await client.execute(prepared.id, [1 + (i % keys)], 0, version);
    if (i >= warmup) times.push((performance.now() - start) * 1000);
  }
  times.sort((a, b) => a - b);
  return {
    count: times.length,
    p50: quantile(times, 0.5),
    p95: quantile(times, 0.95),
    p99: quantile(times, 0.99),
    max: times.at(-1) ?? Number.NaN,
  };
}
