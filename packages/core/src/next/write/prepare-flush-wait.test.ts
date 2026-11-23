/**
 * A write step waiting for a running flush before it grows its metadata ends with its
 * statement's cancellation.
 *
 * `#prepareMinimum` waits for the running flushes when the step's metadata would have to wait to
 * grow while one has its writer open (provisional writer work settles before any metadata growth
 * waits: governor rules 4/5/7, native-write-pressure.test.ts). That wait holds the statement's
 * memory outside the governor, so it must end when the statement is cancelled (Astra's review of
 * #56). A flush waiting for its own credit is still funding and is not waited for.
 *
 * Law: a statement's waits end with its cancellation (`StatementOptions.signal`: "cancels waits
 * inside the statement"); PostgreSQL statement atomicity: a cancelled statement writes nothing.
 */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
} from "./test-support.js";
import type { WritePathImpl } from "./engine.js";

const MiB = 1 << 20;
const PENDING = Symbol("pending");
const within = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);

it("cancels a write step waiting for a running flush before its metadata grows", async () => {
  let hold = false,
    held = 0;
  let resume: (() => void) | null = null;
  // Memtables of two rows: the third committed row freezes the first two and starts their flush.
  const h = new Harness("strict", "memory", 2, undefined, {
    beforeFlushPublish: () => {
      if (!hold) return Promise.resolve();
      held++;
      return new Promise<void>((resolve) => {
        resume = resolve;
      });
    },
  });
  const governor = new Governor(32 * MiB);
  Object.defineProperty(h, "governor", { value: governor });
  const path: WritePathImpl = await h.open();
  const undo: Array<() => void> = [];
  try {
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", INT)]),
          })
        ).id ?? 0;
    });
    hold = true;
    for (let id = 0; id < 3; id++)
      await autocommit(path, (s) => s.table(t).insert(batch(ints([id]), ints([id])), null));
    for (let i = 0; i < 200 && held === 0; i++)
      await new Promise((resolve) => setTimeout(resolve, 5));
    expect(held, "a flush runs, held before it publishes").toBe(1);
    const tx = await path.begin();
    const cancel = new AbortController();
    const statement = tx.statement({ rules, signal: cancel.signal });
    try {
      // No room for the step's metadata at once: the writer's metadata slack and all ordinary
      // free memory are taken (real allocations).
      const metadata = path.writerHoldings().grants[1];
      if (metadata === undefined) throw new Error("metadata account");
      const idle = metadata.held - metadata.allocated - metadata.loaned;
      if (idle > 0 && metadata.allocate(idle)) undo.push(() => metadata.free(idle));
      const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
      const hog = governor.tryAdmit({ label: "pressure", minimumBytes: free, spillable: false });
      if (hog === null) throw new Error("pressure grant");
      undo.push(() => hog.close());
      const write = Promise.resolve(
        statement.table(t).insert(batch(ints([10]), ints([10])), null),
      ).then(
        () => "written" as const,
        (error: unknown) => error,
      );
      expect(await within(write, 500), "it waits for the running flush").toBe(PENDING);
      cancel.abort(new Error("cancelled by the test"));
      const outcome = await within(write, 1_000);
      expect(outcome, "cancellation ends the wait").not.toBe(PENDING);
      expect(outcome).not.toBe("written");
    } finally {
      for (const step of undo.splice(0)) step();
      statement.rollback();
      tx.rollback();
      hold = false;
      (resume as (() => void) | null)?.();
    }
    await path.quiesce();
    await autocommit(path, (s) => s.table(t).insert(batch(ints([20]), ints([20])), null));
    const view = path.pin();
    try {
      expect(await rowsOf(view, t)).toEqual([
        [0, 0],
        [1, 1],
        [2, 2],
        [20, 20],
      ]);
    } finally {
      view.release();
    }
    expect(path.failure).toBeNull();
  } finally {
    for (const step of undo) step();
    hold = false;
    (resume as (() => void) | null)?.();
    await h.close();
  }
}, 60_000);

it("stops waiting for a flush that queues its funding again, deciding the step (Astra)", async () => {
  // Astra's review of #56, round 2: the flush the step waited for was open when it started
  // waiting, then met pressure at its publication, unwound its writer and queued 4,123 bytes of
  // funding for memory the statement holds, while the step still waited for it: a cycle only
  // cancellation broke. The step now wakes when the writer queues growth and prepares again,
  // its growth carrying the statement's holdings: served, or refused typed.
  let hold = false,
    held = 0;
  let resume: (() => void) | null = null;
  const h = new Harness("strict", "memory", 2, undefined, {
    beforeFlushPublish: () => {
      if (!hold) return Promise.resolve();
      held++;
      return new Promise<void>((resolve) => {
        resume = resolve;
      });
    },
  });
  const governor = new Governor(32 * MiB);
  Object.defineProperty(h, "governor", { value: governor });
  const path: WritePathImpl = await h.open();
  const undo: Array<() => void> = [];
  try {
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", INT)]),
          })
        ).id ?? 0;
    });
    hold = true;
    for (let id = 0; id < 3; id++)
      await autocommit(path, (s) => s.table(t).insert(batch(ints([id]), ints([id])), null));
    for (let i = 0; i < 200 && held === 0; i++)
      await new Promise((resolve) => setTimeout(resolve, 5));
    expect(held, "a flush runs, held before it publishes").toBe(1);
    const tx = await path.begin();
    const statement = tx.statement({ rules });
    try {
      // Every idle writer byte and all ordinary free memory taken; the statement holds the
      // latter (its query's grant, as an executing query would register it).
      for (const grant of path.writerHoldings().grants) {
        const idle = grant.held - grant.allocated - grant.loaned;
        if (idle > 0 && grant.allocate(idle)) undo.push(() => grant.free(idle));
      }
      const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
      const hog = governor.tryAdmit({
        label: "statement query",
        minimumBytes: free,
        spillable: false,
      });
      if (hog === null) throw new Error("statement grant");
      undo.push(() => hog.close());
      statement.waitHoldings = () => ({ grants: [hog], lent: path.writerHoldings() });
      const write = Promise.resolve(
        statement.table(t).insert(batch(ints([10]), ints([10])), null),
      ).then(
        () => "written" as const,
        (error: unknown) => error,
      );
      expect(await within(write, 500), "it waits for the running flush").toBe(PENDING);
      hold = false;
      (resume as (() => void) | null)?.();
      // Decided, never left waiting: written, or refused typed (it cannot be served beside
      // what the statement holds while the flush's funding is queued ahead of it).
      const outcome = await within(write, 2_000);
      expect(outcome, "decided after the flush queues its funding").not.toBe(PENDING);
      if (outcome !== "written")
        expect(outcome).toMatchObject({ name: "MemoryAdmissionError", sqlState: "53200" });
    } finally {
      for (const step of undo.splice(0)) step();
      statement.rollback();
      tx.rollback();
      hold = false;
      (resume as (() => void) | null)?.();
    }
    await path.quiesce();
    await autocommit(path, (s) => s.table(t).insert(batch(ints([20]), ints([20])), null));
    expect(path.failure).toBeNull();
  } finally {
    for (const step of undo) step();
    hold = false;
    (resume as (() => void) | null)?.();
    await h.close();
  }
}, 60_000);
