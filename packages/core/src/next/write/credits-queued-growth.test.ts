/**
 * The writer's own growth queued in the governor (`WriteCredits.growTo`), as a statement holding
 * the writer turn is bounded beside it (`servableNow`, `NativeGrantGroupOptions.returns` and
 * `onLeave`). Real concurrent `growTo` calls, observed through the drain.
 *
 * Law (memory/README.md, GOVERNOR-WAIT-POLICY point 5; lead ruling 2026-11-24 on #71's review):
 * each queued job returns the credit its own completion returns, and an account several jobs name
 * is credited once. A job withdrawn leaves the owner's record before the drain its withdrawal
 * starts, as a served one does in the drain that serves it.
 */
import { expect, it } from "vitest";
import { Governor, type Grant, type WaitingHoldings } from "../memory/governor.js";
import { WriteCredits } from "./credits.js";

const MiB = 1 << 20;
const CAP = 20 * MiB;

const turn = () =>
  new Promise<"pending">((resolve) =>
    setImmediate(() => {
      resolve("pending");
    }),
  );

/** `kept`: a holding the writer lends that no growth returns (as its lifetime holdings). */
async function setting(kept = 0) {
  const governor = new Governor(CAP);
  const credits = await WriteCredits.open(governor, {
    rows: 5 * MiB,
    metadata: 0,
    commits: 0,
    merge: 0,
  });
  expect(credits.rows.allocate(5 * MiB)).toBe(true);
  const statement = await governor.admit({
    label: "statement",
    minimumBytes: 8 * MiB,
    spillable: false,
  });
  const extra = await governor.admit({
    label: "writer kept",
    minimumBytes: kept,
    spillable: false,
  });
  const writerHeld = () => credits.grants().reduce((sum, grant) => sum + grant.held, 0);
  // Ordinary admissions fit cap − reserve: hold up to it, so each 3 MiB growth waits for this.
  const finite = await governor.admit({
    label: "independent holder",
    minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
    spillable: false,
  });
  return { governor, credits, statement, finite, writerHeld, extra };
}

const grow = (credits: WriteCredits, account: Grant, bytes: number, signal: AbortSignal) =>
  credits.growTo(account, account.held + bytes, signal, true).then(
    () => "served",
    (error: unknown) => (error as Error).message,
  );

it("credits the writer's accounts once across its concurrent queued growth", async () => {
  const { governor, credits, statement, finite, writerHeld, extra } = await setting(2 * MiB);
  const stop = new AbortController();
  const metadata = grow(credits, credits.metadata, 3 * MiB, stop.signal);
  const merge = grow(credits, credits.merge, 3 * MiB, stop.signal);
  expect(await Promise.race([metadata, turn()])).toBe("pending");
  expect(credits.queuedGrowth.progress, "both queued").toBe(6 * MiB);
  // The writer also lends a 2 MiB holding no growth returns.
  const holdings: WaitingHoldings = {
    grants: [statement],
    lent: {
      grants: [...credits.grants(), extra],
      retained: () => writerHeld() + extra.held,
    },
  };
  // Both jobs fit beside the statement and its writer (8 + 5 + 2 + 3, then + 3 after the first
  // returns the accounts): the statement keeps its 8 and the writer's 2. Counted per job, the
  // accounts' credit would be returned twice and the bound would rise by it.
  expect(governor.servableNow(holdings, true)).toBe(CAP - 10 * MiB);
  stop.abort(new Error("done"));
  expect([await metadata, await merge]).toEqual(["done", "done"]);
  extra.close();
  finite.close();
  statement.close();
  credits.close();
  expect(governor.usedBytes).toBe(0);
});

it("drops a withdrawn growth's record before the drain its withdrawal starts", async () => {
  const { governor, credits, statement, finite, writerHeld, extra } = await setting();
  const first = new AbortController();
  const second = new AbortController();
  const metadata = grow(credits, credits.metadata, 3 * MiB, first.signal);
  const merge = grow(credits, credits.merge, 3 * MiB, second.signal);
  expect(await Promise.race([merge, turn()])).toBe("pending");
  // A statement step waiting beside the writer: its bound is checked in every drain, and its
  // holdings callback reads the owner's record there (as the writer's own `retained` does).
  const seen: number[] = [];
  const holdings = (): WaitingHoldings => ({
    grants: [statement],
    lent: {
      grants: credits.grants(),
      retained: () => {
        seen.push(credits.queuedGrowth.progress);
        return writerHeld();
      },
    },
  });
  const step = governor
    .admitGroup({
      label: "statement step",
      grants: [{ label: "statement step", minimumBytes: MiB, spillable: false }],
      includesProgress: true,
      holdings,
    })
    .then(
      (grants) => {
        for (const grant of grants) grant.close();
        return "served";
      },
      (error: unknown) => (error as { sqlState?: string }).sqlState ?? (error as Error).message,
    );
  expect(await Promise.race([step, turn()])).toBe("pending");
  seen.length = 0;
  second.abort(new Error("merge cancelled"));
  // Synchronously, in the drain the cancel ran: only the first growth's record is left.
  expect(seen.length, "the cancel's drain checked the bound").toBeGreaterThan(0);
  expect(new Set(seen)).toEqual(new Set([3 * MiB]));
  expect(credits.queuedGrowth.progress).toBe(3 * MiB);
  expect(await merge).toBe("merge cancelled");
  first.abort(new Error("metadata cancelled"));
  expect(credits.queuedGrowth.progress).toBe(0);
  expect(await metadata).toBe("metadata cancelled");
  // With the writer's growth gone, nothing queued holds the step back: 1 MiB fits now.
  expect(await step).toBe("served");
  extra.close();
  finite.close();
  statement.close();
  credits.close();
  expect(governor.usedBytes).toBe(0);
});
