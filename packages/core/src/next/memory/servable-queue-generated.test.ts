/**
 * Generated histories for GOVERNOR-WAIT-POLICY point 5 over the governor's real holder queue
 * (servable-queue.test.ts holds the pinned law tests).
 *
 * Each history: a statement holding a grant (and sometimes a loan), an owner lending it 1–3
 * accounts, 0–2 independent finite holders, and 1–3 jobs of the owner queued as holder growth
 * (each of either capacity class, each naming some of the owner's accounts it returns, the same
 * account possibly by several), then the statement's step (either class). One job may be cancelled
 * before the step's decision, or inside a drain (from another job's leave hook, as it is served).
 * The finite holders then release one at a time.
 *
 * Law (memory/README.md, lead rulings 2026-11-24): the step is refused 53200 if and only if it
 * could never be served beside what its statement keeps, judged when it is decided and again when
 * the queue changes; otherwise it is served once the finite holders release, never left waiting.
 * "Could never be served" is the shadow model below, written from the policy, not from the
 * governor: every finite holder released; the owner's queued growth served FIFO by the room rule
 * (every request fits the cap; one that is not a private-progress group also fits cap − reserve,
 * where loans do not count); each served job returns the open accounts it names (each once); a
 * job that never fits blocks the step behind it.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { createReach, SERVABLE_QUEUE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant, type Loan } from "./governor.js";
import { Owner, failure, take, turn } from "./servable-queue.test-support.js";

const UNIT = 1 << 19; // half a MiB
/** The cap and its reserve (`reserveFor`: an eighth), in units. */
const C = 40,
  R = 5;
const CAP = C * UNIT;

afterEach(() => {
  Governor.stallAfterMs = null;
});

interface Job {
  readonly ask: number;
  readonly progress: boolean;
  /** Indexes of the owner's accounts it returns. */
  readonly returns: readonly number[];
}

interface History {
  readonly statement: number;
  readonly loan: number;
  readonly accounts: readonly number[];
  readonly finite: readonly number[];
  readonly jobs: readonly Job[];
  readonly step: { readonly bytes: number; readonly progress: boolean };
  readonly cancel:
    | { readonly kind: "none" }
    | { readonly kind: "before"; readonly job: number }
    | { readonly kind: "during"; readonly job: number; readonly trigger: number };
}

const anyHistory: fc.Arbitrary<History> = fc
  .record({
    statement: fc.integer({ min: 0, max: 16 }),
    loan: fc.oneof(fc.constant(0), fc.integer({ min: 1, max: 5 })),
    accounts: fc.array(fc.integer({ min: 1, max: 8 }), { minLength: 1, maxLength: 3 }),
    finite: fc.array(fc.integer({ min: 1, max: 8 }), { maxLength: 2 }),
    jobs: fc.array(
      fc.record({
        ask: fc.integer({ min: 1, max: 20 }),
        progress: fc.boolean(),
        returns: fc.array(fc.integer({ min: 0, max: 2 }), { maxLength: 3 }),
      }),
      { minLength: 1, maxLength: 3 },
    ),
    step: fc.record({ bytes: fc.integer({ min: 1, max: 28 }), progress: fc.boolean() }),
    cancel: fc.oneof(
      { weight: 2, arbitrary: fc.constant({ kind: "none" as const }) },
      { weight: 1, arbitrary: fc.record({ kind: fc.constant("before" as const), job: fc.nat(2) }) },
      {
        weight: 2,
        arbitrary: fc.record({
          kind: fc.constant("during" as const),
          job: fc.nat(2),
          trigger: fc.nat(2),
        }),
      },
    ),
  })
  .map((drawn) => ({
    ...drawn,
    loan: drawn.statement === 0 ? 0 : drawn.loan,
    // A cancel from a drain withdraws a job queued behind the one being served (FIFO: never one
    // served before it; a served growth cancelled before it runs is servable-queue.test.ts's).
    cancel:
      drawn.cancel.kind !== "during"
        ? drawn.cancel
        : drawn.cancel.job === drawn.cancel.trigger
          ? { kind: "none" as const }
          : {
              kind: "during" as const,
              job: Math.max(drawn.cancel.job, drawn.cancel.trigger),
              trigger: Math.min(drawn.cancel.job, drawn.cancel.trigger),
            },
    jobs: drawn.jobs.map((job) => ({
      ...job,
      returns: [...new Set(job.returns.filter((index) => index < drawn.accounts.length))],
    })),
  }));

/**
 * Histories shaped toward a cancel that matters: a job queued behind the trigger is the only one
 * returning an account the step needs, behind a finite holder large enough to queue both.
 */
const cancelShaped: fc.Arbitrary<History> = fc
  .record({
    statement: fc.integer({ min: 0, max: 6 }),
    account: fc.integer({ min: 4, max: 8 }),
    finite: fc.integer({ min: 10, max: 14 }),
    trigger: fc.record({ ask: fc.integer({ min: 8, max: 16 }), progress: fc.boolean() }),
    victim: fc.record({ ask: fc.integer({ min: 1, max: 10 }), progress: fc.boolean() }),
    progress: fc.boolean(),
    offset: fc.nat(7),
  })
  .map(({ statement, account, finite, trigger, victim, progress, offset }) => {
    // The step lands between its bound without the victim's return and its bound with it.
    const kept = statement + account;
    const without = progress ? C - kept : Math.min(C - kept, C - R - kept);
    return {
      statement,
      loan: 0,
      accounts: [account],
      finite: [finite],
      jobs: [
        { ...trigger, returns: [] },
        { ...victim, returns: [0] },
      ],
      step: { bytes: Math.max(1, without + 1 + (offset % account)), progress },
      cancel: { kind: "during" as const, job: 1, trigger: 0 },
    };
  });

const history: fc.Arbitrary<History> = fc.oneof(
  { weight: 4, arbitrary: anyHistory },
  { weight: 1, arbitrary: cancelShaped },
);

// ---- the shadow model (in units) ----

interface Room {
  used: number;
  readonly loaned: number;
}
const fits = (room: Room, ask: number, whole: boolean) =>
  room.used + ask <= C && (whole || room.used - room.loaned + ask <= C - R);
const most = (room: Room, whole: boolean) =>
  whole ? C - room.used : Math.min(C - room.used, C - R - (room.used - room.loaned));

interface Predicted {
  /** "served at once", "refused at once", "refused after a cancel" or "served after release". */
  readonly outcome: string;
  /** Jobs queued when the step is decided, in order. */
  readonly queued: readonly number[];
}

function predict(h: History): Predicted {
  const open = new Set(h.accounts.map((_, index) => index));
  const room: Room = {
    used:
      h.statement +
      h.loan +
      h.accounts.reduce((a, b) => a + b, 0) +
      h.finite.reduce((a, b) => a + b, 0),
    loaned: h.loan,
  };
  const queue: number[] = [];
  const running: number[] = [];
  const cancelled = new Set<number>();
  const cancelOf = (job: number) => {
    if (cancelled.has(job) || h.jobs[job] === undefined) return;
    cancelled.add(job);
    const at = queue.indexOf(job);
    if (at >= 0) queue.splice(at, 1);
  };
  const serve = (job: number) => {
    const spec = h.jobs[job];
    if (spec === undefined) return;
    room.used += spec.ask;
    running.push(job);
    const cancel = h.cancel;
    if (cancel.kind === "during" && cancel.trigger === job && cancel.job !== job) {
      // Its leave hook cancels the other job, unless that one was served already.
      if (!running.includes(cancel.job)) cancelOf(cancel.job);
    }
  };
  const drain = () => {
    for (let head = queue[0]; head !== undefined; head = queue[0]) {
      const spec = h.jobs[head];
      if (spec === undefined || !fits(room, spec.ask, spec.progress)) return;
      queue.shift();
      serve(head);
    }
  };
  // Enqueued in order, synchronously: one served at once runs until a later turn.
  h.jobs.forEach((spec, job) => {
    if (cancelled.has(job)) return;
    if (queue.length === 0 && fits(room, spec.ask, spec.progress)) serve(job);
    else queue.push(job);
  });
  const settle = () => {
    // Running jobs finish (their growth and the open accounts they name return), and each return
    // drains the queue; to the fixpoint.
    while (running.length > 0) {
      const job = running.shift();
      const spec = job === undefined ? undefined : h.jobs[job];
      if (spec === undefined) continue;
      room.used -= spec.ask;
      for (const index of spec.returns) if (open.delete(index)) room.used -= h.accounts[index] ?? 0;
      drain();
    }
  };
  settle();
  if (h.cancel.kind === "before") {
    cancelOf(h.cancel.job);
    drain();
    settle();
  }
  const queued = [...queue];
  if (queue.length === 0 && fits(room, h.step.bytes, h.step.progress))
    return { outcome: "served at once", queued };
  // The bound: finite holders released, the statement's and the open accounts' credit kept.
  const bound = (jobs: readonly number[]) => {
    const kept: Room = {
      used:
        h.statement + h.loan + [...open].reduce((sum, index) => sum + (h.accounts[index] ?? 0), 0),
      loaned: h.loan,
    };
    const credited = new Set<number>();
    for (const job of jobs) {
      const spec = h.jobs[job];
      if (spec === undefined) continue;
      if (!fits(kept, spec.ask, spec.progress)) return Math.min(0, most(kept, h.step.progress));
      for (const index of spec.returns)
        if (open.has(index) && !credited.has(index)) {
          credited.add(index);
          kept.used -= h.accounts[index] ?? 0;
        }
    }
    return most(kept, h.step.progress);
  };
  if (h.step.bytes > bound(queue)) return { outcome: "refused at once", queued };
  const cancel = h.cancel;
  if (cancel.kind === "during") {
    const trigger = queue.indexOf(cancel.trigger);
    const victim = queue.indexOf(cancel.job);
    // Served in turn, the trigger withdraws a job still queued behind it.
    if (trigger >= 0 && victim > trigger) {
      const rest = queue.filter((job) => job !== cancel.job);
      if (h.step.bytes > bound(rest)) return { outcome: "refused after a cancel", queued };
    }
  }
  return { outcome: "served after release", queued };
}

// ---- the real run ----

async function run(
  h: History,
  reach: ReturnType<typeof createReach<(typeof SERVABLE_QUEUE_REACH)[number]>>,
) {
  const predicted = predict(h);
  const governor = new Governor(CAP);
  const owner = new Owner(governor);
  const statement = await take(governor, "statement", h.statement * UNIT);
  let loan: Loan | null = null;
  if (h.loan > 0) loan = await statement.borrow(h.loan * UNIT);
  const accounts: Grant[] = [];
  for (const [index, bytes] of h.accounts.entries())
    accounts.push(await owner.account(`account ${String(index)}`, bytes * UNIT));
  const finite: Grant[] = [];
  for (const [index, bytes] of h.finite.entries())
    finite.push(await take(governor, `finite ${String(index)}`, bytes * UNIT));
  const stops = h.jobs.map(() => new AbortController());
  const cancelReason = () => new Error("cancelled");
  const jobs = h.jobs.map((spec, job) => {
    const cancel = h.cancel;
    return owner.job(`job ${String(job)}`, spec.ask * UNIT, {
      progress: spec.progress,
      returns: spec.returns.map((index) => accounts[index]).filter((grant) => grant !== undefined),
      signal: (stops[job] ?? new AbortController()).signal,
      ...(cancel.kind === "during" && cancel.trigger === job && cancel.job !== job
        ? {
            onLeave: () => {
              stops[cancel.job]?.abort(cancelReason());
            },
          }
        : {}),
    });
  });
  await turn();
  await turn();
  if (h.cancel.kind === "before") {
    stops[h.cancel.job]?.abort(cancelReason());
    await turn();
  }
  const step = governor
    .admitGroup({
      label: "statement step",
      grants: [{ label: "statement step", minimumBytes: h.step.bytes * UNIT, spillable: false }],
      ...(h.step.progress ? { includesProgress: true } : {}),
      holdings: () => ({ grants: [statement], lent: owner.lent }),
    })
    .then((grants) => {
      for (const grant of grants) grant.close();
      return "served";
    }, failure);
  const decided = await Promise.race([step, turn()]);
  for (const grant of finite) {
    grant.close();
    await turn();
  }
  const final = await Promise.race([
    step,
    new Promise<string>((resolve) =>
      setTimeout(() => {
        resolve("still waiting");
      }, 4_000),
    ),
  ]);
  const actual =
    final === "served"
      ? decided === "served"
        ? "served at once"
        : "served after release"
      : final === "53200"
        ? decided === "53200"
          ? "refused at once"
          : "refused after a cancel"
        : final;
  // Cleanup: the statement ends, so every job fits; any left waiting is withdrawn.
  loan?.repay();
  statement.close();
  await turn();
  for (const stop of stops) stop.abort(cancelReason());
  await Promise.all(jobs);
  owner.close();
  // Reach: from the model's inputs and the observed outcome.
  const queued = predicted.queued.map((job) => h.jobs[job]).filter((job) => job !== undefined);
  if (queued.length >= 2) reach.reach("simultaneous writer growth");
  if (
    queued.some((job) => job.progress !== h.step.progress) ||
    new Set(queued.map((job) => job.progress)).size > 1
  )
    reach.reach("mixed capacity classes");
  if (h.loan > 0 && queued.length > 0) reach.reach("loan held beside queued growth");
  const shared = queued.flatMap((job) => job.returns);
  if (new Set(shared).size < shared.length) reach.reach("shared account named twice");
  if (h.cancel.kind === "before") reach.reach("cancel before the decision");
  if (h.cancel.kind === "during" && predicted.queued.includes(h.cancel.trigger))
    reach.reach("cancel during a drain");
  for (const state of SERVABLE_QUEUE_REACH) if (state === actual) reach.reach(state);
  return { predicted: predicted.outcome, actual, used: governor.usedBytes };
}

for (const seed of seedsFor("next-servable-queue", [0x71a1, 0x71a2]))
  it(`decides each step as the wait policy does over the real holder queue (${String(seed)})`, async () => {
    const reach = createReach("servable queue", SERVABLE_QUEUE_REACH);
    Governor.stallAfterMs = 2_000;
    await fc.assert(
      fc.asyncProperty(history, async (h) => {
        const feasible =
          h.statement +
            h.accounts.reduce((a, b) => a + b, 0) +
            h.finite.reduce((a, b) => a + b, 0) <=
            C - R &&
          h.statement +
            h.loan +
            h.accounts.reduce((a, b) => a + b, 0) +
            h.finite.reduce((a, b) => a + b, 0) <=
            C;
        fc.pre(feasible);
        reach.beginHistory();
        const result = await run(h, reach);
        expect(result.actual, JSON.stringify(h)).toBe(result.predicted);
        expect(result.used, "every byte returned").toBe(0);
      }),
      { seed, numRuns: 1000 },
    );
    reach.assertReached();
  }, 300_000);
