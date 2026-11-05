/**
 * The conformance cases every `StorageBackend` must pass (simulated here with both fences; OPFS
 * and IndexedDB in the storage track, in Node and in real browsers). Plain assertions, no test
 * framework, so browser suites can run them. Conformance alone is not durability evidence: the
 * storage kit (kit.ts) runs these cases together with a physical crash sweep, and a backend is
 * accepted only by the kit.
 */
import {
  DATA_ALIGN,
  OwnerUnresponsiveError,
  OwnershipLostError,
  StoreClosedError,
  StoreCorruptionError,
  type StorageBackend,
  type StoreSession,
} from "./contract.js";

export interface ConformanceCase {
  readonly name: string;
  run(createBackend: () => Promise<StorageBackend>): Promise<void>;
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(`Store conformance: ${message}`);
}

const bytes = (...values: number[]) => new Uint8Array(values);

function equal(left: Uint8Array | null | undefined, right: Uint8Array | null): boolean {
  if (left === null || left === undefined || right === null) return (left ?? null) === right;
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

async function rejects(
  action: () => unknown,
  type: new (...args: never[]) => Error,
  message: string,
): Promise<void> {
  try {
    await action();
  } catch (error) {
    assert(error instanceof type, `${message}: threw ${String(error)}`);
    return;
  }
  throw new Error(`Store conformance: ${message}: did not throw`);
}

async function owner(backend: StorageBackend, timeoutMs = 2_000) {
  const lease = await backend.acquire({ timeoutMs });
  return { lease, session: await backend.open(lease) };
}

/** Everything a session can observe, to prove a fenced owner changed nothing. */
async function view(session: StoreSession) {
  const data = await session.dataLength();
  const [content] = await session.readData([{ offset: 0, length: data }]);
  return {
    root: await session.readRoot(),
    data: content ?? new Uint8Array(0),
    log: await session.readLog(0, session.logLength()),
    headers: await session.readLogHeaders(),
  };
}

function sameView(a: Awaited<ReturnType<typeof view>>, b: Awaited<ReturnType<typeof view>>) {
  return (
    equal(a.root, b.root) &&
    equal(a.data, b.data) &&
    equal(a.log, b.log) &&
    equal(a.headers[0], b.headers[0]) &&
    equal(a.headers[1], b.headers[1])
  );
}

/** Every mutating call, for fencing checks. */
function mutations(session: StoreSession): Array<[string, () => Promise<unknown>]> {
  return [
    ["publish", () => session.publish(bytes(66))],
    ["writeData", () => session.writeData(0, bytes(66))],
    ["releaseData", () => session.releaseData([{ offset: 0, length: DATA_ALIGN }])],
    ["appendLog", () => session.appendLog(bytes(66))],
    ["writeLogHeader", () => session.writeLogHeader(0, bytes(66))],
    ["flushLog", () => session.flushLog()],
    ["truncateLog", () => session.truncateLog(0)],
    ["resetLog", () => session.resetLog()],
  ];
}

export const storeConformanceCases: readonly ConformanceCase[] = [
  {
    name: "a new store is empty",
    async run(createBackend) {
      const { lease, session } = await owner(await createBackend());
      assert(lease.epoch >= 1 && session.epoch === lease.epoch, "epoch");
      assert((await session.readRoot()) === null, "no root");
      assert((await session.dataLength()) === 0, "no data");
      assert(session.logLength() === 0, "empty log");
      await lease.release();
    },
  },
  {
    name: "data extents are written, read by coalesced ranges, published and survive reacquiring",
    async run(createBackend) {
      const backend = await createBackend();
      const first = await owner(backend);
      await first.session.writeData(0, bytes(1, 2, 3));
      await first.session.writeData(DATA_ALIGN, bytes(4, 5));
      await rejects(() => first.session.writeData(7, bytes(1)), RangeError, "an unaligned extent");
      await first.session.publish(bytes(9, 9));
      const [a, b, c, empty] = await first.session.readData([
        { offset: DATA_ALIGN, length: 2 },
        { offset: 1, length: 2 },
        { offset: 0, length: 3 },
        { offset: DATA_ALIGN + 2, length: 0 },
      ]);
      assert(equal(a, bytes(4, 5)) && equal(b, bytes(2, 3)) && equal(c, bytes(1, 2, 3)), "ranges");
      assert(empty?.length === 0, "empty range at the end");
      await rejects(
        () => first.session.readData([{ offset: DATA_ALIGN, length: 3 }]),
        StoreCorruptionError,
        "a read past the data length",
      );
      await first.lease.release();
      const second = await owner(backend);
      assert(second.lease.epoch > first.lease.epoch, "epochs increase");
      assert(equal(await second.session.readRoot(), bytes(9, 9)), "root survives");
      const [again] = await second.session.readData([{ offset: 1, length: 2 }]);
      assert(equal(again, bytes(2, 3)), "data survives");
      await second.session.releaseData([{ offset: DATA_ALIGN, length: DATA_ALIGN }]);
      assert((await second.session.dataLength()) <= DATA_ALIGN + 2, "a free tail may be returned");
      await second.session.publish(bytes(7));
      await second.session.publish(bytes(8));
      assert(equal(await second.session.readRoot(), bytes(8)), "newest root");
      assert(equal(await second.session.readPreviousRoot(), bytes(7)), "previous root survives");
      await second.lease.release();
    },
  },
  {
    name: "readDataInto fills a caller's buffer exactly and refuses a read past the data length",
    async run(createBackend) {
      const { lease, session } = await owner(await createBackend());
      await session.writeData(0, bytes(1, 2, 3, 4));
      const target = new Uint8Array(3);
      await session.readDataInto(1, target);
      assert(equal(target, bytes(2, 3, 4)), "bytes read into the buffer");
      await rejects(
        () => session.readDataInto(2, new Uint8Array(3)),
        StoreCorruptionError,
        "a read past the data length",
      );
      await lease.release();
    },
  },
  {
    name: "the scratch file, when there is one, is fenced with the store",
    async run(createBackend) {
      const backend = await createBackend();
      const first = await owner(backend);
      const scratch = backend.scratch(first.lease);
      if (scratch === null) {
        await first.lease.release();
        return;
      }
      scratch.write(bytes(7), { at: 0 });
      const second = await backend.acquire({ steal: true, timeoutMs: 2_000 });
      await rejects(
        () => scratch.write(bytes(8), { at: 0 }),
        Error,
        "the lost owner's scratch handle is closed",
      );
      assert(backend.scratch(second) !== null, "the new owner has the scratch file");
      await second.release();
      await first.lease.release();
    },
  },
  {
    name: "the log appends, flushes, keeps header slots, truncates and resets",
    async run(createBackend) {
      const backend = await createBackend();
      const first = await owner(backend);
      const { session } = first;
      assert((await session.appendLog(bytes(1, 2, 3))) === 0, "first append at 0");
      assert((await session.appendLog(bytes(4))) === 3, "appends are contiguous");
      await session.writeLogHeader(1, bytes(7, 7));
      await session.flushLog();
      assert(equal(await session.readLog(1, 3), bytes(2, 3, 4)), "read across appends");
      await rejects(() => session.readLog(2, 3), StoreCorruptionError, "a read past the log end");
      await first.lease.release();
      const second = await owner(backend);
      const again = second.session;
      assert(again.logLength() === 4, "flushed log survives");
      const headers = await again.readLogHeaders();
      assert(equal(headers[1]?.subarray(0, 2), bytes(7, 7)), "header slot survives");
      await again.truncateLog(2);
      assert((await again.appendLog(bytes(9))) === 2, "appends follow a truncation");
      await again.flushLog();
      assert(equal(await again.readLog(0, 3), bytes(1, 2, 9)), "rewritten tail");
      await again.resetLog();
      assert(again.logLength() === 0, "reset body");
      const reset = await again.readLogHeaders();
      assert(
        reset.every((slot) => slot === null || slot.every((byte) => byte === 0)),
        "reset slots",
      );
      await second.lease.release();
    },
  },
  {
    name: "a second owner waits while the first holds, and a fenced first owner changes nothing",
    async run(createBackend) {
      const backend = await createBackend();
      const first = await owner(backend);
      await first.session.writeData(0, bytes(1));
      await first.session.publish(bytes(1));
      await first.session.appendLog(bytes(5, 5));
      await first.session.flushLog();
      // Opened while the first still holds its session: it must wait, then give up.
      await rejects(() => backend.acquire({ timeoutMs: 40 }), OwnerUnresponsiveError, "waiting");
      await first.session.appendLog(bytes(6));
      assert(first.session.logLength() === 3, "the first owner still works");
      // Stale-owner resumption: a second owner takes over; the first resumes and tries everything.
      const second = await backend.acquire({ steal: true, timeoutMs: 2_000 });
      const session = await backend.open(second);
      assert(second.epoch > first.lease.epoch, "the new owner has a newer epoch");
      assert(first.lease.signal.aborted, "the old lease knows it was lost");
      const before = await view(session);
      for (const [name, action] of mutations(first.session))
        await rejects(action, OwnershipLostError, `fenced ${name}`);
      assert(sameView(before, await view(session)), "a fenced owner's calls had no effect");
      assert(session.logLength() >= 2, "the flushed log is the new owner's to recover");
      await second.release();
      await first.lease.release();
    },
  },
  {
    name: "a closed session refuses calls",
    async run(createBackend) {
      const { lease, session } = await owner(await createBackend());
      await session.close();
      await rejects(() => session.readRoot(), StoreClosedError, "readRoot after close");
      await rejects(() => session.appendLog(bytes(1)), StoreClosedError, "appendLog after close");
      await lease.release();
    },
  },
];
