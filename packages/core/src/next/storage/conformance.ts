/**
 * The conformance cases every `DurableStore` implementation must pass (memory here; OPFS and
 * IndexedDB in the storage track, in Node and in real browsers). Plain assertions, no test
 * framework, so browser suites can run them too. Crash behaviour is not tested here: it needs a
 * platform-level fault model (fault-store.ts for the engine's side; each store's own tests for
 * how it meets guarantees 1–6).
 */
import {
  StaleEpochError,
  StoreClosedError,
  StoreCorruptionError,
  type DurableStore,
  type StoreSession,
} from "./contract.js";

export interface ConformanceCase {
  readonly name: string;
  run(createStore: () => Promise<DurableStore>): Promise<void>;
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(`Store conformance: ${message}`);
}

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

function equal(left: Uint8Array | null, right: Uint8Array | null): boolean {
  if (left === null || right === null) return left === right;
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

async function writePack(session: StoreSession, id: number, ...chunks: Uint8Array[]) {
  const writer = session.createPack(id);
  for (const chunk of chunks) await writer.append(chunk);
  return writer.seal();
}

export const storeConformanceCases: readonly ConformanceCase[] = [
  {
    name: "a new store is empty",
    async run(createStore) {
      const session = await (await createStore()).open();
      assert(session.epoch >= 1, "epoch starts at 1");
      assert((await session.readRoot()) === null, "no root");
      assert((await session.listPacks()).length === 0, "no packs");
      assert(session.logLength() === 0, "empty log");
      const [first, second] = await session.readLogHeaders();
      assert(first === null && second === null, "no headers");
      session.close();
    },
  },
  {
    name: "packs are written, published, read by range and survive reopening",
    async run(createStore) {
      const store = await createStore();
      const session = await store.open();
      const writer = session.createPack(7);
      assert((await writer.append(bytes(1, 2, 3))) === 0, "first append at 0");
      assert((await writer.append(bytes(4, 5))) === 3, "second append after the first");
      assert(writer.length === 5, "writer length");
      const sealed = await writer.seal();
      assert(sealed.id === 7 && sealed.length === 5, "sealed pack");
      await session.publish(bytes(9, 9), [sealed]);
      const [whole, middle, empty] = await session.read([
        { pack: 7, offset: 0, length: 5 },
        { pack: 7, offset: 2, length: 2 },
        { pack: 7, offset: 5, length: 0 },
      ]);
      assert(equal(whole ?? null, bytes(1, 2, 3, 4, 5)), "whole range");
      assert(equal(middle ?? null, bytes(3, 4)), "inner range across appends");
      assert(empty?.length === 0, "empty range at the end");
      await rejects(
        () => session.read([{ pack: 7, offset: 4, length: 2 }]),
        StoreCorruptionError,
        "a read past a pack's end",
      );
      await rejects(
        () => session.read([{ pack: 8, offset: 0, length: 1 }]),
        StoreCorruptionError,
        "a read of a missing pack",
      );
      await rejects(() => session.createPack(7), Error, "reusing a pack id");
      session.close();
      const reopened = await store.open();
      assert(equal(await reopened.readRoot(), bytes(9, 9)), "root survives reopening");
      const [again] = await reopened.read([{ pack: 7, offset: 1, length: 3 }]);
      assert(equal(again ?? null, bytes(2, 3, 4)), "pack survives reopening");
      reopened.close();
    },
  },
  {
    name: "publish replaces the root; reclaim is idempotent; listing is ordered",
    async run(createStore) {
      const session = await (await createStore()).open();
      const a = await writePack(session, 3, bytes(1));
      await session.publish(bytes(1), [a]);
      const b = await writePack(session, 1, bytes(2));
      const c = await writePack(session, 2, bytes(3));
      await session.publish(bytes(2), [b, c]);
      assert(equal(await session.readRoot(), bytes(2)), "newest root");
      assert((await session.listPacks()).join() === "1,2,3", "ascending ids");
      await session.reclaim([3, 3, 99]);
      await session.reclaim([3]);
      assert((await session.listPacks()).join() === "1,2", "reclaimed");
      session.close();
    },
  },
  {
    name: "the log appends, flushes, keeps header slots, truncates and resets",
    async run(createStore) {
      const store = await createStore();
      const session = await store.open();
      assert(session.appendLog(bytes(1, 2, 3)) === 0, "first append at 0");
      assert(session.appendLog(bytes(4)) === 3, "appends are contiguous");
      session.writeLogHeader(1, bytes(7, 7));
      await session.flushLog();
      assert(session.logLength() === 4, "length");
      assert(equal(await session.readLog(1, 3), bytes(2, 3, 4)), "read across appends");
      await rejects(() => session.readLog(2, 3), StoreCorruptionError, "a read past the log end");
      session.close();
      const reopened = await store.open();
      assert(reopened.logLength() === 4, "flushed log survives reopening");
      const [first, second] = await reopened.readLogHeaders();
      assert(first === null && equal(second, bytes(7, 7)), "header slot survives reopening");
      await reopened.truncateLog(2);
      assert(reopened.logLength() === 2, "truncated");
      assert(reopened.appendLog(bytes(9)) === 2, "appends follow a truncation");
      await reopened.flushLog();
      assert(equal(await reopened.readLog(0, 3), bytes(1, 2, 9)), "rewritten tail");
      await reopened.resetLog();
      assert(reopened.logLength() === 0, "reset body");
      const headers = await reopened.readLogHeaders();
      assert(headers[0] === null && headers[1] === null, "reset headers");
      reopened.close();
    },
  },
  {
    name: "a newer session fences every mutation of an older one",
    async run(createStore) {
      const store = await createStore();
      const old = await store.open();
      const writer = old.createPack(1);
      const current = await store.open();
      assert(current.epoch > old.epoch, "epochs increase");
      const fenced: Array<[string, () => unknown]> = [
        ["publish", () => old.publish(bytes(1), [])],
        ["createPack", () => old.createPack(2)],
        ["pack append", () => writer.append(bytes(1))],
        ["reclaim", () => old.reclaim([1])],
        ["appendLog", () => old.appendLog(bytes(1))],
        ["writeLogHeader", () => old.writeLogHeader(0, bytes(1))],
        ["flushLog", () => old.flushLog()],
        ["truncateLog", () => old.truncateLog(0)],
        ["resetLog", () => old.resetLog()],
      ];
      for (const [name, action] of fenced) await rejects(action, StaleEpochError, `fenced ${name}`);
      assert(current.appendLog(bytes(5)) === 0, "the fenced session changed nothing");
      await current.flushLog();
      current.close();
      old.close();
    },
  },
  {
    name: "a closed session refuses every call",
    async run(createStore) {
      const session = await (await createStore()).open();
      session.close();
      await rejects(() => session.readRoot(), StoreClosedError, "readRoot after close");
      await rejects(() => session.appendLog(bytes(1)), StoreClosedError, "appendLog after close");
    },
  },
];
