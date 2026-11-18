import { SnapshotHandleError } from "../snapshot-errors.js";
export { SnapshotHandleError };
/** Connection-scoped, bounded portable archive transport. Native storage is supplied by the
 * owner composition, never by the page. Handles cannot cross reconnects.
 */
import type { MemoryGovernor, Grant } from "../memory/governor.js";
import type { SnapshotSink } from "../format/snapshot.js";
import { SnapshotScopeError } from "../snapshot-errors.js";
import { SNAPSHOT_TRANSFER_BYTES, type SnapshotOp } from "./snapshot-api.js";
import type { WireValue } from "./protocol.js";

export interface OwnerSnapshotOperations {
  readonly governor: MemoryGovernor;
  /** Recovery failed: the server must step down rather than serve a stale path. */
  fatal?: boolean;
  fatalError?: unknown;
  /** Complete work minimum, admitted atomically with transport (zero for a pure sink).
   * The supplied work grant belongs to SnapshotConnection; export never admits again. */
  readonly exportMinimumBytes: number;
  export(sink: SnapshotSink, signal: AbortSignal, work: Grant): Promise<void>;
  /** Spool this one-shot producer, then use the replayable core loader with replace:true.
   * Settle/exclude ordinary writers and reopen ordinary recovery before resolving/rejecting.
   */
  import(source: AsyncIterable<Uint8Array>, signal: AbortSignal): Promise<void>;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  // A producer can fail before its next pull; the consumer still observes the same promise.
  void promise.catch(() => undefined);
  return { promise, resolve, reject };
}
function abortError() {
  return new DOMException("Snapshot transport closed", "AbortError");
}
function checkedId(args: readonly WireValue[]): string {
  const id = args[0];
  if (typeof id !== "string" || id.length === 0)
    throw new SnapshotHandleError("Invalid snapshot handle");
  return id;
}
const OPS: ReadonlySet<string> = new Set([
  "snapshotExportOpen",
  "snapshotExportRead",
  "snapshotExportClose",
  "snapshotImportOpen",
  "snapshotImportWrite",
  "snapshotImportFinish",
  "snapshotImportAbort",
]);
export const isSnapshotOp = (op: string): op is SnapshotOp => OPS.has(op);

interface Flow {
  readonly id: string;
  readonly kind: "export" | "import";
  readonly abort: AbortController;
  readonly grant: Grant;
  readonly done: Promise<void>;
  failure?: { readonly reason: unknown };
  busy?: boolean;
  read?: () => Promise<Uint8Array | null>;
  write?: (bytes: Uint8Array) => Promise<void>;
  finish?: () => void;
}

export class SnapshotConnection {
  #flow: Flow | undefined;
  #closed = false;
  #opening: AbortController | undefined;
  #openingDone: Promise<string> | undefined;
  readonly #lifetime = new AbortController();
  constructor(readonly operations: OwnerSnapshotOperations) {}

  get active(): boolean {
    return this.#flow !== undefined || this.#opening !== undefined;
  }

  async command(op: SnapshotOp, args: readonly WireValue[], signal: AbortSignal): Promise<unknown> {
    signal.throwIfAborted();
    if (this.#closed) throw new SnapshotHandleError("Snapshot connection has ended");
    if (op === "snapshotExportOpen" || op === "snapshotImportOpen") {
      if (this.#flow !== undefined || this.#opening !== undefined)
        throw new SnapshotHandleError("Close this connection's active snapshot first");
      const opening = new AbortController();
      this.#opening = opening;
      try {
        const started = this.#open(
          op === "snapshotExportOpen" ? "export" : "import",
          AbortSignal.any([signal, opening.signal]),
        );
        this.#openingDone = started;
        return await started;
      } finally {
        if (this.#opening === opening) {
          this.#opening = undefined;
          this.#openingDone = undefined;
        }
      }
    }
    const id = checkedId(args),
      flow = this.#flow;
    if (op === "snapshotExportClose" || op === "snapshotImportAbort") {
      if (flow?.id === id) await this.#end(flow);
      return null;
    }
    if (flow?.id !== id)
      throw new SnapshotHandleError("Snapshot handle was lost or belongs to another connection");
    if (flow.failure !== undefined) throw flow.failure.reason;
    if (flow.busy === true) throw new SnapshotHandleError("A snapshot step is already pending");
    flow.busy = true;
    const abort = () => flow.abort.abort(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    try {
      if (op === "snapshotExportRead") {
        if (flow.read === undefined) throw new SnapshotHandleError("Not an export handle");
        return await flow.read();
      }
      if (op === "snapshotImportWrite") {
        const chunk = args[1];
        if (
          flow.write === undefined ||
          !(chunk instanceof Uint8Array) ||
          chunk.length === 0 ||
          chunk.buffer.byteLength > SNAPSHOT_TRANSFER_BYTES
        )
          throw new SnapshotHandleError("Invalid bounded snapshot chunk");
        await flow.write(chunk);
        return null;
      }
      if (flow.finish === undefined) throw new SnapshotHandleError("Not an import handle");
      flow.finish();
      await flow.done;
      if (this.#flow === flow) this.#flow = undefined;
      return null;
    } finally {
      flow.busy = false;
      signal.removeEventListener("abort", abort);
    }
  }

  async #open(kind: Flow["kind"], signal: AbortSignal): Promise<string> {
    // Fund the incoming clone or dedicated outgoing buffer BEFORE allowing any chunk message.
    const transport = {
      label: "snapshot transport",
      minimumBytes: SNAPSHOT_TRANSFER_BYTES + 1024,
      spillable: false,
    };
    const minimum = kind === "export" ? this.operations.exportMinimumBytes : undefined;
    const [grant, work] =
      minimum === undefined
        ? [await this.operations.governor.admit({ ...transport, signal })]
        : await this.operations.governor.admitGroup({
            label: "portable snapshot export",
            includesProgress: true,
            grants: [
              transport,
              { label: "portable export", minimumBytes: minimum, spillable: true },
            ],
            signal,
          });
    if (grant === undefined) throw new Error("Missing snapshot transport admission");
    const abort = new AbortController(),
      id = crypto.randomUUID();
    const workSignal = AbortSignal.any([abort.signal, signal, this.#lifetime.signal]);
    if (!grant.allocate(SNAPSHOT_TRANSFER_BYTES + 1024)) {
      grant.close();
      work?.close();
      throw new SnapshotScopeError("Snapshot transport lost its admission");
    }
    let flow: Flow;
    if (kind === "export") {
      if (work === undefined) {
        grant.close();
        throw new SnapshotScopeError("Missing complete snapshot export admission");
      }
      let available = deferred<Uint8Array | null>(),
        consumed = deferred<undefined>();
      let handedOut = false;
      const aborted = () => {
        available.reject(workSignal.reason);
        consumed.reject(workSignal.reason);
      };
      workSignal.addEventListener("abort", aborted, { once: true });
      const done = Promise.resolve()
        .then(() =>
          this.operations.export(
            {
              write: async (bytes) => {
                for (let at = 0; at < bytes.length; at += SNAPSHOT_TRANSFER_BYTES) {
                  workSignal.throwIfAborted();
                  // The native sink borrows its input. Only this dedicated copy is transferred.
                  available.resolve(bytes.slice(at, at + SNAPSHOT_TRANSFER_BYTES));
                  await consumed.promise;
                }
              },
            },
            workSignal,
            work,
          ),
        )
        .then(() => {
          available.resolve(null);
        });
      flow = {
        id,
        kind,
        abort,
        grant,
        done,
        async read() {
          if (handedOut) {
            handedOut = false;
            const previous = consumed;
            consumed = deferred<undefined>();
            available = deferred<Uint8Array | null>();
            previous.resolve(undefined);
          }
          const chunk = await available.promise;
          handedOut = chunk !== null;
          return chunk;
        },
      };
      void done.catch((reason: unknown) => {
        flow.failure = { reason };
        available.reject(reason);
        consumed.reject(reason);
      });
      void done
        .finally(() => {
          workSignal.removeEventListener("abort", aborted);
          grant.close();
          work.close();
        })
        .catch(() => undefined);
    } else {
      let waiting = deferred<IteratorResult<Uint8Array>>(),
        consumed = deferred<undefined>();
      const ready = deferred<undefined>();
      let ended = false,
        payloadHeld = true;
      const inputEnded = () => {
        if (!payloadHeld) return;
        payloadHeld = false;
        grant.free(SNAPSHOT_TRANSFER_BYTES);
        grant.release(SNAPSHOT_TRANSFER_BYTES);
      };
      const aborted = () => {
        waiting.reject(workSignal.reason);
        consumed.reject(workSignal.reason);
      };
      workSignal.addEventListener("abort", aborted, { once: true });
      const iterator: AsyncIterator<Uint8Array> = {
        next: () => {
          ready.resolve(undefined);
          consumed.resolve(undefined);
          if (ended) {
            inputEnded();
            return Promise.resolve({ done: true, value: undefined });
          }
          return waiting.promise.then((result) => {
            waiting = deferred<IteratorResult<Uint8Array>>();
            if (result.done) inputEnded();
            return result;
          });
        },
        return: () => {
          ended = true;
          consumed.resolve(undefined);
          return Promise.resolve({ done: true, value: undefined });
        },
      };
      const done = Promise.resolve().then(() =>
        this.operations.import({ [Symbol.asyncIterator]: () => iterator }, workSignal),
      );
      void done.catch((reason: unknown) => {
        flow.failure = { reason };
        ready.reject(reason);
        waiting.reject(reason);
        consumed.reject(reason);
      });
      void done
        .finally(() => {
          workSignal.removeEventListener("abort", aborted);
          grant.close();
        })
        .catch(() => undefined);
      flow = {
        id,
        kind,
        abort,
        grant,
        done,
        async write(bytes) {
          if (ended) throw new SnapshotHandleError("Snapshot input already ended");
          consumed = deferred<undefined>();
          waiting.resolve({ done: false, value: bytes });
          await consumed.promise;
        },
        finish() {
          ended = true;
          waiting.resolve({ done: true, value: undefined });
        },
      };
      // The native spool admits before asking for a chunk. Do not allow input before then.
      await Promise.race([ready.promise, done]);
    }
    if (this.#closed || signal.aborted) {
      abort.abort(signal.reason ?? abortError());
      await flow.done.catch(() => undefined);
      throw signal.reason ?? abortError();
    }
    this.#flow = flow;
    return id;
  }

  async #end(flow: Flow): Promise<void> {
    flow.abort.abort(abortError());
    await flow.done.catch(() => undefined);
    if (this.#flow === flow) this.#flow = undefined;
  }
  async close(): Promise<void> {
    this.#closed = true;
    this.#lifetime.abort(abortError());
    this.#opening?.abort(abortError());
    await this.#openingDone?.catch(() => undefined);
    if (this.#flow !== undefined) await this.#end(this.#flow);
  }
}
