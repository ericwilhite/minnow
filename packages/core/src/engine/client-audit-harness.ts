/**
 * Test harness for the worker RPC layer. It builds an in-process worker boundary whose
 * listeners are keyed by event type (so the host's global-error listeners are never fed message
 * frames), can sever delivery to simulate a terminated worker, can inject raw frames and
 * transport events, and wraps a block store with per-method faults.
 */
import type { BlockStore } from "../storage/types.js";
import { MemoryBlockStore } from "../storage/index.js";
import type { ClientTransport } from "./client.js";
import type { RpcScope } from "./worker-server.js";

type Listener = (event: MessageEvent<unknown>) => void;

export interface Boundary {
  clientSide: ClientTransport;
  workerSide: RpcScope;
  /** Frames posted by the client, in order (after clone). */
  sentByClient: unknown[];
  /** Frames posted by the worker, in order (after clone). */
  sentByWorker: unknown[];
  /** Stop delivering frames in either direction: the worker is "terminated". */
  sever(): void;
  /** Inject a raw frame into the client as if the worker had posted it. */
  injectToClient(frame: unknown): void;
  /** Inject a raw frame into the worker as if the client had posted it. */
  injectToWorker(frame: unknown): void;
  emitWorkerGlobal(type: "error" | "unhandledrejection" | "messageerror", event: object): void;
  emitTransport(type: "error" | "messageerror", event?: object): void;
  /** Resolves after every frame queued so far has been delivered. */
  flush(): Promise<void>;
}

export function createBoundary(): Boundary {
  const clientListeners = new Map<string, Listener[]>();
  const workerListeners = new Map<string, Listener[]>();
  let chain = Promise.resolve();
  let severed = false;
  const sentByClient: unknown[] = [];
  const sentByWorker: unknown[] = [];
  const deliver = (
    target: Map<string, Listener[]>,
    message: unknown,
    transfer?: ArrayBuffer[],
  ): void => {
    const data = structuredClone(message, transfer === undefined ? undefined : { transfer });
    if (severed) return;
    chain = chain.then(() => {
      if (severed) return;
      for (const listener of target.get("message") ?? []) listener({ data } as MessageEvent);
    });
  };
  const add = (map: Map<string, Listener[]>, type: string, listener: Listener): void => {
    const list = map.get(type) ?? [];
    list.push(listener);
    map.set(type, list);
  };
  const remove = (map: Map<string, Listener[]>, type: string, listener: Listener): void => {
    const list = map.get(type) ?? [];
    const index = list.indexOf(listener);
    if (index >= 0) list.splice(index, 1);
  };
  const clientSide = {
    postMessage: (message: unknown, options?: { transfer?: ArrayBuffer[] }) => {
      sentByClient.push(structuredClone(message));
      deliver(workerListeners, message, options?.transfer);
    },
    addEventListener: (type: string, listener: Listener) => {
      add(clientListeners, type, listener);
    },
    removeEventListener: (type: string, listener: Listener) => {
      remove(clientListeners, type, listener);
    },
    terminate: () => {
      severed = true;
    },
  } as unknown as ClientTransport;
  const workerSide = {
    postMessage: (message: unknown, options?: { transfer?: ArrayBuffer[] }) => {
      sentByWorker.push(structuredClone(message));
      deliver(clientListeners, message, options?.transfer);
    },
    addEventListener: (type: string, listener: Listener) => {
      add(workerListeners, type, listener);
    },
  } as unknown as RpcScope;
  return {
    sentByClient,
    sentByWorker,
    clientSide,
    workerSide,
    sever: () => {
      severed = true;
    },
    injectToClient: (frame) => {
      for (const listener of clientListeners.get("message") ?? []) {
        listener({ data: frame } as MessageEvent);
      }
    },
    injectToWorker: (frame) => {
      for (const listener of workerListeners.get("message") ?? []) {
        listener({ data: frame } as MessageEvent);
      }
    },
    emitWorkerGlobal: (type, event) => {
      for (const listener of workerListeners.get(type) ?? []) listener(event as MessageEvent);
    },
    emitTransport: (type, event) => {
      for (const listener of clientListeners.get(type) ?? []) listener(event as MessageEvent);
    },
    flush: async () => {
      await chain;
      await new Promise((resolve) => setTimeout(resolve, 5));
    },
  };
}

export function settled(ms = 20): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export type Fault = (
  method: string,
  args: unknown[],
  run: () => Promise<unknown>,
) => Promise<unknown>;

/**
 * A MemoryBlockStore behind a Proxy: every async method routes through `fault`, which can run
 * the real operation and then throw, throw without running, delay, or count. Methods listed in
 * `hide` are absent from the store, which steers the engine onto its fallback path (hiding
 * `writeTransaction`, for instance, forces the two-step create/stage/commit protocol).
 */
export function faultyStore(
  fault: Fault,
  options: { hide?: string[] } = {},
): { store: BlockStore; calls: Array<{ method: string; args: unknown[] }> } {
  const inner = new MemoryBlockStore();
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const hidden = new Set(options.hide ?? []);
  const store = new Proxy(inner as unknown as Record<string, unknown>, {
    get(target, property) {
      if (typeof property === "string" && hidden.has(property)) return undefined;
      const value = target[property as string];
      if (typeof value !== "function" || typeof property !== "string") return value;
      const method = property;
      return (...args: unknown[]): unknown => {
        const run = (): unknown => (value as (...a: unknown[]) => unknown).apply(inner, args);
        // Only intercept promise-returning store operations; sync helpers pass through.
        const isAsync =
          (value as { constructor?: { name?: string } }).constructor?.name === "AsyncFunction";
        if (!isAsync) return run();
        calls.push({ method, args });
        return fault(method, args, run as () => Promise<unknown>);
      };
    },
    has(target, property) {
      if (typeof property === "string" && hidden.has(property)) return false;
      return Reflect.has(target, property);
    },
  }) as unknown as BlockStore;
  return { store, calls };
}
