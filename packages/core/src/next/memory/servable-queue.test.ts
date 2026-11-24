/**
 * GOVERNOR-WAIT-POLICY point 5 over the governor's real holder queue (`servableNow`): every
 * request here is queued for real, and each outcome is observed from the drain.
 *
 * Law (memory/README.md, lead rulings 2026-11-24 on #71's review): a request is refused 53200
 * only if it could never be served beside what its statement keeps; otherwise it waits, and every
 * finite holder (anything the statement does not keep) is taken to release. The holder growth
 * queued ahead of it is run as the drain runs it: FIFO, by the drain's own room rule; growth that
 * fits is served and its work returns the lent credit it names, each grant once; growth that
 * never fits blocks every request behind it. The bound and the drain cannot disagree.
 *
 * Generated histories: servable-queue-generated.test.ts.
 */
import { afterEach, expect, it } from "vitest";
import { Governor, type Grant, type WaitingHoldings } from "./governor.js";
import { Owner, take, turn } from "./servable-queue.test-support.js";

const MiB = 1 << 20;
const CAP = 20 * MiB;

afterEach(() => {
  Governor.stallAfterMs = null;
});

/** A request's outcome: "served" (its grants closed at once), else the error's SQLSTATE or message. */
const outcome = (pending: Promise<readonly Grant[]>) =>
  pending.then(
    (grants) => {
      for (const grant of grants) grant.close();
      return "served";
    },
    (error: unknown) => (error as { sqlState?: string }).sqlState ?? (error as Error).message,
  );

const settled = <T>(pending: Promise<T>) => Promise.race([pending, turn()]);

const step = (
  governor: Governor,
  bytes: number,
  holdings: () => WaitingHoldings,
  progress = true,
  signal?: AbortSignal,
) =>
  outcome(
    governor.admitGroup({
      label: "statement step",
      grants: [{ label: "statement step", minimumBytes: bytes, spillable: false }],
      ...(progress ? { includesProgress: true } : {}),
      holdings,
      ...(signal === undefined ? {} : { signal }),
    }),
  );

it("waits beside a finite independent holder, and is served once it releases", async () => {
  // Reviewer P1: the statement holds 8, the writer 5 and asks 6 more, another holder 4. Judged on
  // what is free now (3), the writer's 6 never fits and the statement was refused 53200; with the
  // other holder released (it is finite) the writer's growth fits (13 + 6 ≤ 20).
  const governor = new Governor(CAP);
  const owner = new Owner(governor);
  const statement = await take(governor, "statement", 8 * MiB);
  const writer = await owner.account("writer accounts", 5 * MiB);
  const finite = await take(governor, "independent holder", 4 * MiB);
  const growth = owner.job("flush growth", 6 * MiB, {
    progress: true,
    returns: [writer],
  });
  const holdings = () => ({ grants: [statement], lent: owner.lent });
  // The writer's growth returns its 5 once served: the statement may get up to 20 − 8.
  expect(governor.servableNow(holdings(), true)).toBe(CAP - 8 * MiB);
  const request = step(governor, MiB, holdings);
  expect(await settled(request), "waits, never 53200").toBe("pending");
  finite.close();
  expect(await growth).toBe("served");
  expect(await request).toBe("served");
  statement.close();
  owner.close();
  expect(governor.usedBytes).toBe(0);
});

it("refuses at once behind a holder-queue head that never fits: the drain is FIFO", async () => {
  // Reviewer P1: 7 free; B asks 9 at the head, A behind it asks 7 and would return 2. Run in any
  // order (A, then B) the bound was 12 MiB, but the drain never serves A past B: B waits for the
  // statement's own 8 (a knot), so the statement could never be served.
  const governor = new Governor(CAP);
  const owner = new Owner(governor);
  const statement = await take(governor, "statement", 8 * MiB);
  const bBase = await owner.account("B base", 3 * MiB);
  const aBase = await owner.account("A base", 2 * MiB);
  const granted: string[] = [];
  governor.observeGrants((event) => granted.push(event.label));
  const stop = new AbortController();
  const b = owner.job("B growth", 9 * MiB, {
    progress: true,
    returns: [bBase],
    signal: stop.signal,
  });
  const a = owner.job("A growth", 7 * MiB, {
    progress: true,
    returns: [aBase],
    signal: stop.signal,
  });
  const holdings = () => ({ grants: [statement], lent: owner.lent });
  expect(governor.servableNow(holdings(), true)).toBeLessThanOrEqual(0);
  expect(await settled(step(governor, MiB, holdings)), "refused at once").toBe("53200");
  expect(granted, "the drain served neither").toEqual([]);
  stop.abort(new Error("cancelled"));
  expect([await b, await a]).toEqual(["cancelled", "cancelled"]);
  statement.close();
  owner.close();
  expect(governor.usedBytes).toBe(0);
});

it("waits behind holder growth the drain serves in order, and is served after it", async () => {
  // The same holdings with A ahead of B: A (7 ≤ 7) is served and returns 2, then B (9 ≤ 9).
  const governor = new Governor(CAP);
  const owner = new Owner(governor);
  const statement = await take(governor, "statement", 8 * MiB);
  const bBase = await owner.account("B base", 3 * MiB);
  const aBase = await owner.account("A base", 2 * MiB);
  const finite = await take(governor, "independent holder", MiB);
  const granted: string[] = [];
  governor.observeGrants((event) => granted.push(event.label));
  const a = owner.job("A growth", 7 * MiB, { progress: true, returns: [aBase] });
  const b = owner.job("B growth", 9 * MiB, { progress: true, returns: [bBase] });
  const holdings = () => ({ grants: [statement], lent: owner.lent });
  expect(governor.servableNow(holdings(), true)).toBe(CAP - 8 * MiB);
  const request = step(governor, CAP - 8 * MiB, holdings);
  expect(await settled(request)).toBe("pending");
  finite.close();
  expect([await a, await b, await request]).toEqual(["served", "served", "served"]);
  expect(granted).toEqual(["A growth", "B growth", "statement step"]);
  statement.close();
  owner.close();
  expect(governor.usedBytes).toBe(0);
});

it("credits a lent grant two queued jobs name once", async () => {
  // Reviewer P1: two jobs of one owner both name its 5 MiB account. Counted per job, the bound
  // grew by 5 MiB more than the owner could ever return (17 MiB, though the statement alone holds
  // 8 of 20). The owner also lends 3 MiB no job returns: that stays kept.
  const governor = new Governor(CAP);
  const owner = new Owner(governor);
  const statement = await take(governor, "statement", 8 * MiB);
  const shared = await owner.account("shared writer account", 5 * MiB);
  await owner.account("writer floor", 3 * MiB);
  // 17.5 of 20 held: neither job fits now (each asks 3), both do once this holder releases.
  const finite = await take(governor, "independent holder", 1.5 * MiB);
  const first = owner.job("first growth", 3 * MiB, { progress: true, returns: [shared] });
  const second = owner.job("second growth", 3 * MiB, { progress: true, returns: [shared] });
  const holdings = () => ({ grants: [statement], lent: owner.lent });
  const bound = CAP - 8 * MiB - 3 * MiB;
  expect(governor.servableNow(holdings(), true)).toBe(bound);
  expect(await settled(step(governor, bound + 1, holdings)), "above the bound").toBe("53200");
  const request = step(governor, bound, holdings);
  expect(await settled(request)).toBe("pending");
  finite.close();
  expect([await first, await second, await request]).toEqual(["served", "served", "served"]);
  statement.close();
  owner.close();
  expect(governor.usedBytes).toBe(0);
});

it("drops a withdrawn job's owner record before the drain its withdrawal starts", async () => {
  // Reviewer P2: a cancelled job's record outlived it through the drain its cancel ran, so the
  // owner still reported everything it holds (as while its work is queued): 53200. Here the owner
  // keeps all 5 while a job of its is queued and 1 otherwise (its reclaimer returns the rest).
  const governor = new Governor(CAP);
  const chain = governor.openHolderChain("writer", "writer");
  const records = new Set<string>(["B"]);
  const statement = await take(governor, "statement", 8 * MiB);
  const owner = await take(governor, "writer accounts", 5 * MiB, true);
  owner.onReclaimRequest(() => {
    if (records.size === 0 && owner.held > MiB) owner.release(owner.held - MiB);
  });
  const finite = await take(governor, "independent holder", 4.5 * MiB);
  const stop = new AbortController();
  const b = governor
    .admitGroup({
      label: "B growth",
      grants: [{ label: "B growth", minimumBytes: 3 * MiB, spillable: false }],
      holdsCredit: chain,
      includesProgress: true,
      returns: () => [owner],
      signal: stop.signal,
      onLeave: () => records.delete("B"),
    })
    .then(
      () => "served",
      (error: unknown) => (error as Error).message,
    );
  const seen: number[] = [];
  const holdings = () => ({
    grants: [statement],
    lent: {
      grants: [owner],
      retained: () => {
        seen.push(records.size);
        return records.size > 0 ? owner.held : MiB;
      },
    },
  });
  // B queued: it fits beside the statement (13 + 3) and returns the owner's 5: 20 − 8 = 12.
  expect(await settled(step(governor, 13 * MiB, holdings)), "above the bound").toBe("53200");
  const request = step(governor, 10 * MiB, holdings);
  expect(await settled(request), "within it").toBe("pending");
  seen.length = 0;
  stop.abort(new Error("B cancelled"));
  // B gone, the owner keeps 1: 20 − 9 = 11. Had its record stayed through this drain, the owner
  // would keep all 5 and nothing returns them: 7 < 10, refused.
  expect(seen.length, "the cancel's drain checked the bound").toBeGreaterThan(0);
  expect(seen.every((size) => size === 0)).toBe(true);
  expect(await b).toBe("B cancelled");
  expect(await settled(request)).toBe("pending");
  finite.close();
  expect(await request).toBe("served");
  statement.close();
  owner.close();
  chain.close();
  expect(governor.usedBytes).toBe(0);
});

it("bounds an ordinary request beside a held loan by the drain's own room rule", async () => {
  // Reviewer P2: the statement holds 8 and a loan of the whole reserve; the writer holds 5 and asks
  // cap − reserve − 13 of ordinary growth. The drain serves that growth (loans do not count against
  // the ordinary limit); the bound charged the loan to both limits and found nothing servable.
  const governor = new Governor(CAP);
  const reserve = governor.reserveBytes;
  const owner = new Owner(governor);
  const writer = await owner.account("writer accounts", 5 * MiB);
  const statement = await take(governor, "statement", 8 * MiB);
  const loan = await statement.borrow(reserve);
  const ask = CAP - reserve - 13 * MiB;
  expect(governor.canAdmitNow(ask, false), "the drain would serve it now").toBe(true);
  const finite = await take(governor, "independent holder", MiB);
  const growth = owner.job("ordinary growth", ask, { returns: [writer] });
  const holdings = () => ({ grants: [statement], lent: owner.lent });
  // After the growth returns the writer's 5: physical cap − (8 + reserve); ordinary
  // (cap − reserve) − 8, the loan not counted. Both are 12 MiB − reserve.
  const bound = 12 * MiB - reserve;
  expect(governor.servableNow(holdings(), false)).toBe(bound);
  expect(await settled(step(governor, bound + 1, holdings, false))).toBe("53200");
  const request = step(governor, bound, holdings, false);
  expect(await settled(request)).toBe("pending");
  finite.close();
  expect([await growth, await request]).toEqual(["served", "served"]);
  loan.repay();
  statement.close();
  owner.close();
  expect(governor.usedBytes).toBe(0);
});

it("refuses only the request whose leave hook throws, with that hook's error", async () => {
  // Reviewer P3: the hook's error escaped through an unrelated grant's close(), untyped, and left
  // the removed request pending.
  const governor = new Governor(CAP);
  const blocker = await take(governor, "blocker", 16 * MiB);
  const fault = Object.assign(new Error("hook fault"), { sqlState: "55000" });
  const faulty = outcome(
    governor.admitGroup({
      label: "faulty",
      grants: [{ label: "faulty", minimumBytes: 5 * MiB, spillable: false }],
      onLeave: () => {
        throw fault;
      },
    }),
  );
  const behind = outcome(
    governor.admitGroup({
      label: "behind",
      grants: [{ label: "behind", minimumBytes: 5 * MiB, spillable: false }],
    }),
  );
  expect(() => {
    blocker.close();
  }).not.toThrow();
  expect([await faulty, await behind]).toEqual(["55000", "served"]);
  expect(governor.usedBytes).toBe(0);
});

it("looks at each queued holder once per bound (one pass)", async () => {
  const governor = new Governor(CAP);
  const chain = governor.openHolderChain("writer", "writer");
  const blocker = await take(governor, "blocker", 16 * MiB);
  const owner = await take(governor, "owner", MiB);
  const stop = new AbortController();
  let calls = 0;
  const jobs: Array<Promise<unknown>> = [];
  for (let k = 0; k < 32; k++)
    jobs.push(
      governor
        .admitGroup({
          label: `job ${String(k)}`,
          grants: [{ label: `job ${String(k)}`, minimumBytes: 5 * MiB, spillable: false }],
          holdsCredit: chain,
          includesProgress: true,
          signal: stop.signal,
          returns: () => {
            calls += 1;
            return [owner];
          },
        })
        .catch(() => undefined),
    );
  governor.servableNow({ grants: [], lent: { grants: [owner], retained: () => owner.held } }, true);
  expect(calls).toBe(32);
  stop.abort(new Error("done"));
  await Promise.all(jobs);
  for (const grant of [blocker, owner]) grant.close();
  chain.close();
  expect(governor.usedBytes).toBe(0);
});
