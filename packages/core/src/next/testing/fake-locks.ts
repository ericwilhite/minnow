/**
 * An in-memory Web Locks implementation for Node tests of election, liveness and the broker,
 * following the platform semantics the storage probe measured (table 3): exclusive locks granted
 * FIFO, `ifAvailable` refused while held, `steal` rejecting the holder's request with
 * `AbortError`, abortable waits, `query()`. Each `context()` stands for one tab or worker; killing
 * it releases its locks and drops its waits, as closing a tab does.
 */
import type { WebLockManager } from "../owner/web-locks.js";

interface Request {
  readonly context: FakeLockContext;
  readonly grant: () => void;
  readonly reject: (error: Error) => void;
}

interface Held {
  readonly context: FakeLockContext;
  readonly release: () => void;
  readonly steal: (error: Error) => void;
}

const abortError = (message: string): Error => {
  const error = new Error(message);
  error.name = "AbortError";
  return error;
};

export class FakeLockRegistry {
  readonly #held = new Map<string, Held>();
  readonly #queues = new Map<string, Request[]>();

  context(): FakeLockContext {
    return new FakeLockContext(this);
  }

  held(name: string): boolean {
    return this.#held.has(name);
  }

  query(): { held: Array<{ name: string; clientId: string }>; pending: Array<{ name: string }> } {
    return {
      held: [...this.#held].map(([name, held]) => ({ name, clientId: held.context.id })),
      pending: [...this.#queues].flatMap(([name, queue]) => queue.map(() => ({ name }))),
    };
  }

  request(
    context: FakeLockContext,
    name: string,
    options: {
      readonly steal?: boolean;
      readonly signal?: AbortSignal;
      readonly ifAvailable?: boolean;
    },
    callback: (lock: unknown) => Promise<void> | void,
  ): Promise<unknown> {
    if (context.dead) return Promise.reject(abortError("context is gone"));
    if (options.signal?.aborted === true) return Promise.reject(abortError("aborted"));
    return new Promise((resolve, reject) => {
      const run = () => {
        let released = false;
        let stolen: ((error: Error) => void) | null = null;
        const release = () => {
          if (released) return;
          released = true;
          if (this.#held.get(name)?.context === context) this.#held.delete(name);
          this.#next(name);
        };
        this.#held.set(name, {
          context,
          release,
          steal: (error) => {
            stolen?.(error);
            released = true;
          },
        });
        context.track(name);
        const result = (async () => callback({ name }))();
        stolen = (error) => {
          reject(error);
        };
        result.then(
          (value) => {
            if (!released) {
              release();
              resolve(value);
            }
          },
          (error: unknown) => {
            if (!released) {
              release();
              reject(error instanceof Error ? error : new Error(String(error)));
            }
          },
        );
      };
      const holder = this.#held.get(name);
      if (holder === undefined) {
        run();
        return;
      }
      if (options.steal === true) {
        this.#held.delete(name);
        holder.steal(abortError("The lock was stolen"));
        run();
        return;
      }
      if (options.ifAvailable === true) {
        void Promise.resolve(callback(null)).then(resolve, reject);
        return;
      }
      const queue = this.#queues.get(name) ?? [];
      const request: Request = { context, grant: run, reject };
      queue.push(request);
      this.#queues.set(name, queue);
      options.signal?.addEventListener(
        "abort",
        () => {
          const index = queue.indexOf(request);
          if (index >= 0) {
            queue.splice(index, 1);
            reject(abortError("aborted"));
          }
        },
        { once: true },
      );
    });
  }

  #next(name: string): void {
    const queue = this.#queues.get(name);
    const request = queue?.shift();
    if (queue?.length === 0) this.#queues.delete(name);
    request?.grant();
  }

  /** Releases every lock of `context` and drops its waits (a closed tab). */
  kill(context: FakeLockContext): void {
    for (const held of [...this.#held.values()]) if (held.context === context) held.release();
    for (const [name, queue] of this.#queues) {
      const kept = queue.filter((request) => request.context !== context);
      for (const request of queue)
        if (request.context === context) request.reject(abortError("context is gone"));
      if (kept.length === 0) this.#queues.delete(name);
      else this.#queues.set(name, kept);
    }
  }
}

let nextContextId = 0;

export class FakeLockContext implements WebLockManager {
  dead = false;
  /** Like the platform's `clientId` in `query()`. */
  readonly id = `context-${String(++nextContextId)}`;
  readonly #registry: FakeLockRegistry;
  readonly #names = new Set<string>();

  constructor(registry: FakeLockRegistry) {
    this.#registry = registry;
  }

  track(name: string): void {
    this.#names.add(name);
  }

  request(
    name: string,
    options: {
      readonly steal?: boolean;
      readonly signal?: AbortSignal;
      readonly ifAvailable?: boolean;
    },
    callback: (lock: unknown) => Promise<void> | void,
  ): Promise<unknown> {
    return this.#registry.request(this, name, options, callback);
  }

  query(): Promise<{ held: Array<{ name: string; clientId: string }> }> {
    return Promise.resolve(this.#registry.query());
  }

  kill(): void {
    this.dead = true;
    this.#registry.kill(this);
  }
}
