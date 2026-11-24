/**
 * A merge waiting in the governor for its credit, beside a statement that holds the writer turn
 * and the memory the merge waits for, never leaves the statement's requests waiting forever.
 *
 * The memtable now funds rows from the writer's idle commit and merge credit (statement-sizing
 * PR 2), so a merge that starts during a statement may have to grow its credit again. Its growth
 * is holder growth, served before every admission: the statement's next request waited behind a
 * merge that could only be served once the statement ended (a `GovernorStallError` here). While
 * it waits for its credit the merge is not yet running (`#merging` is set once it holds it), so
 * the writer's holdings already count.
 *
 * Law (GOVERNOR-WAIT-POLICY point 5, next/memory/README.md): a request that could never be served
 * beside what its statement keeps while it waits is refused with 53200 before it waits. Growth
 * the writer queues ahead of the statement that cannot fit beside the statement's holdings blocks
 * every request of it (`servableNow` runs the holder queue ahead of the request).
 */
import { afterEach, expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { WriteCredits } from "../write/credits.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
  TEXT,
  texts,
} from "../write/test-support.js";
import type { WritePathImpl } from "../write/engine.js";

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

afterEach(() => {
  Governor.stallAfterMs = null;
  WriteCredits.observeComplete = null;
});

/** One history; returns the growth the merge waited in the governor for. */
async function history(second: "free" | "reclaim", pin = false): Promise<readonly string[]> {
  Governor.stallAfterMs = 4_000;
  // Merges run only when asked (`runMaintenance`): this one starts once the statement holds memory.
  const h = new Harness("strict", "memory", 2, undefined, { merges: { mode: "manual", fanIn: 2 } });
  const governor = new Governor(32 * MiB);
  Object.defineProperty(h, "governor", { value: governor });
  let merging = false,
    waits: string[] = [];
  WriteCredits.observeComplete = (step) => {
    if (merging) waits.push(step.label);
  };
  const path: WritePathImpl = await h.open();
  const held: Grant[] = [];
  let view: ReturnType<WritePathImpl["pin"]> | null = null;
  try {
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
          })
        ).id ?? 0;
    });
    let id = 0;
    // A reader older than the flushes below: the memtables they publish stay charged to the
    // writer's rows for it (held until it closes).
    view = pin ? path.pin() : null;
    // Two level-0 runs: a merge is due.
    for (let k = 0; k < 6; k++)
      await autocommit(path, (s) =>
        s.table(t).insert(batch(ints([id++]), texts(["w".repeat(1000)])), null),
      );
    await path.quiesce();
    const tx = await path.begin();
    const statement = tx.statement({ rules });
    try {
      const lent = statement.writerHoldings();
      if (lent === null) throw new Error("a writable statement holds the writer turn");
      const holdings = () => ({ grants: [...held], lent });
      const take = async (label: string, bytes: number) => {
        const grants = await within(
          governor.admitGroup({
            label,
            includesProgress: true,
            grants: [{ label, minimumBytes: bytes, spillable: false }],
            holdings,
          }),
          3_000,
        );
        if (grants === PENDING) throw new Error(`${label} waited`);
        held.push(...grants);
      };
      // The statement's query takes all but 1 MiB of what it can be served.
      await take("statement query", governor.servableNow(holdings(), true) - MiB);
      // Its rows then find no free memory: the memtable takes the writer's commit and merge credit.
      for (let k = 0; k < 3 && path.writerHoldings().grants[3]?.held !== 0; k++)
        await statement.table(t).insert(batch(ints([id++]), texts(["s".repeat(3 * MiB)])), null);
      expect(path.writerHoldings().grants[3]?.held, "the merge credit funds rows").toBe(0);
      // And all but 2 MiB of the rest: less than the merge needs, so it waits; but with the
      // writer's rows (which stay) it would be more, were they not counted.
      await take(
        "statement query 2",
        second === "free"
          ? governor.capBytes - governor.usedBytes - 2 * MiB
          : // Asking beyond free memory makes the writer return its idle row credit first.
            governor.servableNow(holdings(), true) - 2 * MiB,
      );
      merging = true;
      waits = [];
      const merge = path.runMaintenance();
      // It waits in the governor: for the metadata it opens its inputs with, or for its credit.
      for (let i = 0; i < 300 && waits.length === 0; i++)
        await new Promise((resolve) => setTimeout(resolve, 10));
      expect(waits, "the merge waits in the governor").not.toEqual([]);
      expect(await within(merge, 200), "the merge waits for its credit").toBe(PENDING);
      if (view !== null) {
        // #71 review P1: what the reader pins lies inside the writer's grants, which the writer
        // keeps whole while its growth is queued. The statement counts the pin and what the
        // writer keeps beyond it (`LentHoldings.retained(pinned)`): together, each byte once.
        const pinned = view.blockedBytes();
        expect(pinned, "the reader keeps flushed rows from release").toBeGreaterThan(0);
        const whole = lent.retained(0);
        expect(pinned, "inside what the writer keeps").toBeLessThanOrEqual(whole);
        expect(lent.retained(pinned) + pinned).toBe(whole);
      }
      // Decided before any wait: the merge, served first, can never be served beside the
      // statement's memory, and nor can anything the statement asks.
      const request = governor
        .admitGroup({
          label: "statement request",
          includesProgress: true,
          grants: [{ label: "statement request", minimumBytes: MiB, spillable: false }],
          holdings,
        })
        .then(
          (grants) => {
            for (const grant of grants) grant.close();
            return "served" as const;
          },
          (error: unknown) => error,
        );
      expect(await within(request, 1_000)).toMatchObject({
        name: "MemoryAdmissionError",
        sqlState: "53200",
      });
      // The statement's query ends: the merge is served and completes.
      for (const grant of held.splice(0)) grant.close();
      expect(await within(merge, 5_000), "the merge completes").not.toBe(PENDING);
    } finally {
      for (const grant of held.splice(0)) grant.close();
      statement.rollback();
      tx.rollback();
    }
    expect(path.failure).toBeNull();
    return waits;
  } finally {
    view?.release();
    await h.close();
  }
}

it("refuses a statement's request typed while the writer's growth waits for memory the statement holds", async () => {
  // With no idle row credit left the merge waits for the metadata it opens its inputs with
  // (ordinary growth: no progress reserve); with some, it waits for its credit. The law holds on
  // both.
  expect(await history("reclaim")).toContain("complete grow write path metadata");
  expect((await history("free")).length).toBeGreaterThan(0);
}, 120_000);

it("counts what the statement's reader pins once beside the writer's queued growth", async () => {
  expect((await history("free", true)).length).toBeGreaterThan(0);
}, 120_000);
