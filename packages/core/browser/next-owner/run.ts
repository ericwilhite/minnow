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
import { STEP_DOWN_BOUND_MS, type LifecycleEvent } from "../../dist/next/owner/election.js";
import { frameTransferList } from "../../dist/next/exec/frame-buffers.js";
import type { WireValue } from "../../dist/next/owner/protocol.js";

interface Opened {
  readonly name: string;
  readonly client: ProtocolClient;
  readonly connector: OwnerConnector;
  readonly state: {
    superseded: boolean;
    stepDowns: Array<{ stepDownMs: number; reloadSuggested: boolean }>;
  };
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
  const state = {
    superseded: false,
    stepDowns: [] as Array<{ stepDownMs: number; reloadSuggested: boolean }>,
  };
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
      () => {
        const worker = new Worker(new URL("./owner-worker.ts", import.meta.url), {
          type: "module",
        });
        worker.addEventListener("message", (event: MessageEvent) => {
          const status = event.data as {
            t: string;
            stepDownMs?: number;
            reloadSuggested?: boolean;
          };
          if (status.t === "minnow:stopped")
            state.stepDowns.push({
              stepDownMs: status.stepDownMs ?? -1,
              reloadSuggested: status.reloadSuggested ?? false,
            });
        });
        return worker;
      },
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
  ...(typeof error === "object" && error !== null && "effect" in error
    ? { effect: error.effect }
    : {}),
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

/** Prepares `sql` on the current connection; returns its id. */
export async function prepare(sql: string): Promise<number> {
  return (await current().client.prepare(sql)).id;
}

/** Sends prepared statement `id` without waiting; `settle(key)` later. */
export function sendPrepared(key: string, id: number, params: WireValue[] = [], session = 0): void {
  const promise = current().client.execute(id, params, session);
  promise.catch(() => undefined);
  sent.set(key, promise);
}

/**
 * Starts a connector that must host `name`, closes it once its owner worker was spawned (before
 * it can report ready), and reports how `start()` settled within 2 s: the error's name, or
 * `hung` (review 2, finding 4, over the real `workerOwner`).
 */
export async function closeBeforeReady(name: string): Promise<string> {
  let spawned: () => void = () => undefined;
  const spawn = new Promise<void>((resolve) => {
    spawned = resolve;
  });
  const start = workerOwner(
    () => {
      spawned();
      return new Worker(new URL("./owner-worker.ts", import.meta.url), { type: "module" });
    },
    { mobile: false },
  );
  const connector = new OwnerConnector({
    database: name,
    client: new ProtocolClient(),
    locks: countedLocks,
    openBroker: null,
    startOwner: start,
    context: () => ({
      kind: "page",
      visible: true,
      platform: { mobile: false },
      superseded: false,
    }),
  });
  const settled = connector.start().then(
    () => "started",
    (error: unknown) => (error instanceof Error ? error.name : "Error"),
  );
  await spawn;
  connector.close();
  const outcome = await Promise.race([
    settled,
    new Promise<string>((resolve) => setTimeout(() => resolve("hung"), 2_000)),
  ]);
  // Startup close also awaits graceful release. Allow its public bound and asynchronous lock cleanup.
  const hostHeld = async () =>
    (await navigator.locks.query()).held?.some((lock) => lock.name === `minnow:${name}:host`) ===
    true;
  for (let tries = 0; tries < (STEP_DOWN_BOUND_MS + 1_000) / 10 && (await hostHeld()); tries++)
    await new Promise((resolve) => setTimeout(resolve, 10));
  const pending = (await navigator.locks.query()).held?.map((lock) => lock.name) ?? [];
  return `${outcome}${(await hostHeld()) ? ` (host lock still held: ${pending.join(", ")})` : ""}`;
}

/**
 * Gaps (ms) between successive messages the client heard from its owner over `ms`, sampled from
 * `lastHeardAt` every 10 ms: with a long request in flight, the owner's `alive` cadence.
 */
export async function heardGaps(ms: number): Promise<number[]> {
  const client = current().client;
  const seen: number[] = [];
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const heard = client.lastHeardAt;
    if (seen.at(-1) !== heard) seen.push(heard);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return seen.slice(1).map((at, i) => at - (seen[i] ?? at));
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

/**
 * R22.1's measurement: round trips to the broker carrying `bytes` of payload in four buffers,
 * transferred and cloned, p50 µs per size. The smallest size where transferring wins is the
 * engine's crossover; `TRANSFER_MIN_BYTES` should sit near it.
 */
export async function transferCrossover(sizes = [256, 1024, 4096, 16_384, 65_536], count = 600) {
  const port = new SharedWorker(new URL("./broker-worker.ts", import.meta.url), {
    type: "module",
    name: "minnow-next-broker",
  }).port;
  const p50 = async (bytes: number, transfer: boolean) => {
    const times: number[] = [];
    for (let i = 0; i < count + 100; i++) {
      const arrays = [0, 1, 2, 3].map(() => new Uint8Array(Math.max(1, bytes / 4)));
      const start = performance.now();
      await new Promise<void>((resolve) => {
        port.onmessage = () => {
          resolve();
        };
        port.postMessage(
          { t: "ping", v: 1, nonce: i, arrays },
          transfer ? arrays.map((array) => array.buffer) : [],
        );
      });
      if (i >= 100) times.push((performance.now() - start) * 1000);
    }
    times.sort((a, b) => a - b);
    return quantile(times, 0.5);
  };
  const rows: Array<{ bytes: number; transfer: number; clone: number }> = [];
  for (const bytes of sizes)
    rows.push({ bytes, transfer: await p50(bytes, true), clone: await p50(bytes, false) });
  port.close();
  return rows;
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

/** Keep this page alive to observe the release acknowledgement, but retire its connector. */
export async function stepDownAndWait(event: "freeze" | "pagehide") {
  const { connector, state } = current();
  const previous = state.stepDowns.length;
  connector.lifecycle(event);
  connector.close();
  const deadline = Date.now() + 5_000;
  while (state.stepDowns.length === previous && Date.now() < deadline)
    await new Promise((resolve) => setTimeout(resolve, 10));
  const report = state.stepDowns[previous];
  if (report === undefined)
    throw new Error("The worker was terminated without a release acknowledgement");
  return report;
}

/** Fixture-only barrier: the worker confirms execution or an applied, unacknowledged commit. */
export async function waitForOwnerPhase(
  name: string,
  phase: "held" | "executing" | "commitPending",
) {
  const channel = new BroadcastChannel(`minnow-next-test:${name}`);
  const deadline = Date.now() + 5_000;
  try {
    while (Date.now() < deadline) {
      const nonce = crypto.randomUUID();
      const reached = await new Promise<boolean>((resolve) => {
        const timer = setTimeout(() => resolve(false), 1_000);
        channel.onmessage = (event: MessageEvent) => {
          const reply = event.data as {
            t: string;
            nonce?: string;
            held?: boolean;
            executing?: boolean;
            commitPending?: boolean;
          };
          if (reply.t !== "phase" || reply.nonce !== nonce) return;
          clearTimeout(timer);
          resolve(reply[phase] === true);
        };
        channel.postMessage({ t: "phase?", nonce });
      });
      if (reached) return;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error(`Owner did not reach ${phase}`);
  } finally {
    channel.close();
  }
}

/** Real worker/MessagePort export smoke: credit and cancellation never detach held views. */
export async function exportReleaseCheck() {
  const { client } = current();
  const ordinary = await client.query("SELECT * FROM t");
  const stream = client.exportStream("SELECT * FROM t", [], { windowBytes: 1 });
  const first = await stream.frames.next();
  if (first.done) throw new Error("missing worker export frame");
  const handle = first.value;
  const buffers = frameTransferList(handle.payload);
  const views = buffers.map((buffer) => new Uint8Array(buffer));
  const expected = views.map((view) => Array.from(view));
  const columns = (await stream.columns).length;
  stream.close();
  await stream.done;
  const validAfterClose = views.every(
    (view, i) => JSON.stringify(Array.from(view)) === JSON.stringify(expected[i]),
  );
  handle.release();
  handle.release();
  const detached = views.every((view) => view.length === 0);
  // Force subsequent dedicated output to use returned size classes, then check old views again.
  const again = client.exportStream("SELECT * FROM t");
  let rows = 0;
  for await (const frame of again.frames) {
    rows += frame.payload.rowCount;
    frame.release();
  }
  await again.done;
  return {
    columns,
    validAfterClose,
    detached,
    stillDetached: views.every((view) => view.length === 0),
    rows,
    ordinaryRows: ordinary.rows(),
  };
}
