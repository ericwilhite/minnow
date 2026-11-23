/**
 * The retry guard (memory/retry-guard.ts) counts only rounds without progress, and a spin it
 * reports is never retried or recovered from.
 *
 * Astra's review of #60:
 * - `MetadataBudget.retry` counted every pressure retry, also those funded for more than the one
 *   before: 66 one-byte allocations, unwinding between attempts and funding each larger request,
 *   threw `RetrySpinError` on attempt 65 despite continuous progress. A retry now starts the
 *   count again when it is funded for more (`checkRetries` with the progress-reset count).
 * - The merge loop's generic recovery (write/engine.ts `#runMerges`) took a `RetrySpinError` for
 *   a failed merge: after two it left the inputs unmergeable and maintenance ended with
 *   `path.failure === null`. A spin now fails the write path.
 *
 * Law: GOVERNOR-WAIT-POLICY (next/memory/README.md): a wait that cannot end is reported, not
 * hidden; the guard's contract (retry-guard.ts): only rounds without progress count, and its
 * error is an invariant failure no handler recovers from.
 */
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { WriteCredits } from "./credits.js";
import { NativeWriteSteps } from "./native-step.js";
import { RETRY_GUARD_LIMIT, RetrySpinError } from "../memory/retry-guard.js";
import { MetadataBudget, MetadataPressure } from "./budget.js";
import type { WritePathImpl } from "./engine.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

const MiB = 1 << 20;

/**
 * A metadata budget over a grant of `start` bytes that grows only when `grow` funds it: exactly
 * what is asked when `funds`, nothing otherwise. A stand-in grant (the budget's own logic is under
 * test): a governor grant would grow on allocation itself.
 */
function budget(start: number, funds: boolean) {
  const grant = {
    label: "metadata",
    held: start,
    allocated: 0,
    loaned: 0,
    allocate(bytes: number) {
      if (this.allocated + bytes > this.held) return false;
      this.allocated += bytes;
      return true;
    },
    free(bytes: number) {
      this.allocated -= bytes;
    },
    tryGrow: () => false,
  };
  const metadata = new MetadataBudget(grant as unknown as Grant, () => 0, 16 * MiB);
  const grow = (wanted: number) => {
    if (funds && wanted > grant.held) grant.held = wanted;
    return Promise.resolve();
  };
  return { grant, metadata, grow };
}

/** Allocates `total` single bytes, unwinding them all when one is refused (an attempt's
 * provisional state), and returns how many attempts ran. */
function onebyte(metadata: MetadataBudget, total: number) {
  let attempts = 0;
  const work = () => {
    attempts++;
    let taken = 0;
    try {
      while (taken < total) {
        metadata.allocate(1);
        taken++;
      }
    } catch (error) {
      metadata.free(taken);
      throw error;
    }
    metadata.free(taken);
    return attempts;
  };
  return work;
}

it("lets a metadata retry that is funded for more each time run past the limit", async () => {
  expect(RETRY_GUARD_LIMIT, "the guard is on in this suite").toBe(64);
  // One byte held: the work needs 66, and each refusal is funded one byte more (Astra's probe).
  const { metadata, grow } = budget(1, true);
  const work = onebyte(metadata, 66);
  const attempts: unknown = await Promise.resolve(metadata.retry(work, () => undefined, grow)).then(
    (count) => count,
    (error: unknown) => error,
  );
  expect(attempts, "66 attempts, each one further than the last").toBe(66);
});

it("still reports a metadata retry that asks for no more each time", async () => {
  // Funded for nothing: every retry asks for the same bytes again.
  const { metadata, grow } = budget(1, false);
  const work = onebyte(metadata, 2);
  const outcome: unknown = await Promise.resolve(metadata.retry(work, () => undefined, grow)).then(
    () => "finished",
    (error: unknown) => error,
  );
  expect(outcome).toBeInstanceOf(RetrySpinError);
  expect((outcome as RetrySpinError).label).toBe("metadata metadata");
  expect(MetadataPressure, "the pressure it retried").toBeDefined();
});

it("lets a metadata retry that waits on timers run past the limit without progress", async () => {
  // Funded nothing for 70 retries, each waiting on a timer, then funded: a slow wait, not a spin.
  const { grant, metadata } = budget(1, false);
  let waits = 0;
  const grow = async (wanted: number) => {
    await new Promise((resolve) => setTimeout(resolve, 0));
    if (++waits >= 70 && wanted > grant.held) grant.held = wanted;
  };
  const work = onebyte(metadata, 2);
  const attempts: unknown = await Promise.resolve(metadata.retry(work, () => undefined, grow)).then(
    (count) => count,
    (error: unknown) => error,
  );
  expect(attempts, "71 attempts: 70 timer waits funded nothing, then one was").toBe(71);
});

/** Native write steps over a scope whose stabilization releases 64 bytes of protected memory
 * and reports the change on each of `rounds` rounds, yielding through a timer or a microtask. */
async function nativeStabilization(rounds: number, yieldTo: "timer" | "microtask") {
  const governor = new Governor(4 << 20);
  const credits = await WriteCredits.open(governor, {
    rows: 64 * 1024,
    metadata: 64 * 1024,
    commits: 64 * 1024,
    merge: 64 * 1024,
  });
  let protectedBytes = 64 * rounds;
  let left = rounds;
  // eslint-disable-next-line prefer-const -- assigned after the scope that reads it
  let steps: NativeWriteSteps;
  const scope = {
    governor,
    holderChain: credits.holderChain,
    charge: { allocate: () => true, free: () => undefined },
    check: () => undefined,
    protectedBytes: () => protectedBytes,
    stabilize: async () => {
      if (yieldTo === "timer") await new Promise((resolve) => setTimeout(resolve, 0));
      else await Promise.resolve();
      if (left > 0) {
        left--;
        protectedBytes -= 64;
        steps.changed();
      }
    },
  };
  steps = new NativeWriteSteps(scope);
  const owner = {};
  try {
    const boundary = await steps.prepare(owner, 0, new AbortController().signal);
    boundary.release();
    return "finished";
  } catch (error) {
    return error;
  } finally {
    credits.close();
  }
}

it("lets native stabilization that releases memory through timers run 66 rounds (Astra)", async () => {
  // Astra's re-check of #60: a finite scope that yields through a timer, cuts protected memory by
  // 64 bytes and reports the change on each of 66 rounds threw on round 65.
  expect(await nativeStabilization(66, "timer")).toBe("finished");
});

it("still reports native stabilization that changes in microtasks without end", async () => {
  expect(await nativeStabilization(1_000, "microtask")).toBeInstanceOf(RetrySpinError);
});

async function mergingPath(): Promise<{ harness: Harness; path: WritePathImpl; t: number }> {
  const harness = new Harness("strict", "memory", 4, undefined, {
    merges: { fanIn: 2, stallRuns: 2 },
  });
  const path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  // Every read a merge makes reports a spin (as a guarded loop inside merge funding would).
  const read = harness.space.read.bind(harness.space);
  harness.space.read = (refs, options) => {
    const label = (options.budget as { label?: string }).label ?? "";
    if (label.startsWith("merge"))
      return Promise.reject(new RetrySpinError("injected merge funding", 65));
    return read(refs, options);
  };
  return { harness, path, t };
}

it("fails the write path when a merge's retry loop spins, never recovering from it", async () => {
  const { harness, path, t } = await mergingPath();
  try {
    for (let from = 0; from < 12; from += 4)
      await autocommit(path, (s) =>
        s.table(t).insert(batch(ints(Array.from({ length: 4 }, (_, k) => from + k))), null),
      );
    await path.quiesce().catch(() => undefined);
    expect(path.failure, "the spin surfaces as the write path's failure").toBeInstanceOf(
      RetrySpinError,
    );
    expect(path.maintenance.mergeFailures, "not counted as a failed merge").toBe(0);
  } finally {
    await harness.close().catch(() => undefined);
  }
}, 120_000);
