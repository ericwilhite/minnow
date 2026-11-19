/** Governor rules2/4/7: return dormant estimates without stealing a funded native step. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITE_IDLE_FORECAST_REACH,
  WRITE_IDLE_NOTIFICATION_REACH,
  WRITE_DONATED_METADATA_REACH,
} from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { WriteCredits, WRITE_ROW_STEP_BYTES } from "./credits.js";
import { yieldTask } from "../task.js";

for (const seed of seedsFor("next-write-idle-forecast", [47227, 47228]))
  it(`retains actual buffers and prepared merge/row steps during idle reclaim (seed ${String(seed)})`, async () => {
    const reach = createReach("native idle forecasts", WRITE_IDLE_FORECAST_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 16 }), async (units) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20);
        const minimum = { rows: 1 << 20, metadata: 1 << 20, commits: 1 << 20, merge: 1 << 20 };
        const credits = await WriteCredits.open(governor, minimum);
        credits.bindWorkingMinimum(() => ({
          metadata: credits.hasMergeStep ? credits.metadata.held : 0,
        }));
        const bytes = units * 1024;
        const accounts = [credits.rows, credits.metadata, credits.commits];
        const buffers = new Map<Grant, Uint8Array>();
        let active: Grant | undefined;
        let prepared: ReturnType<WriteCredits["protectRows"]> | undefined;
        try {
          for (const account of accounts) {
            expect(account.allocate(bytes)).toBe(true);
            buffers.set(account, new Uint8Array(bytes).fill(31));
          }
          await credits.prepareMetadata(1024);
          credits.reclaimMetadata();
          prepared = credits.protectRows(bytes);
          reach.reach("live row metadata and WAL buffers");
          const admit = governor.admitGroup.bind(governor);
          governor.admitGroup = async (options) => {
            const grants = await admit(options);
            if (options.label === "funded merge handoff") {
              await yieldTask();
              credits.reclaimIdleForecasts();
              prepared?.check();
              expect(credits.merge.held, "funded merge minimum survives its account handoff").toBe(
                minimum.merge,
              );
              expect(credits.rows.held, "prepared row floor survives reclaim").toBe(
                2 * bytes + WRITE_ROW_STEP_BYTES,
              );
              expect(credits.metadata.held, "funded metadata survives account handoff").toBe(
                bytes + 1024,
              );
              reach.reach("funded metadata retained across await");
              expect(credits.commits.held).toBe(bytes);
              for (const buffer of buffers.values()) expect(buffer.at(-1)).toBe(31);
              reach.reach("funded merge retained across await");
              reach.reach("prepared row retained across reclaim");
              reach.reach("all other dormant credit returned");
            }
            return grants;
          };
          active = await credits.acquireMerge("funded merge handoff", minimum.merge);
          expect(active.allocate(bytes)).toBe(true);
          buffers.set(active, new Uint8Array(bytes).fill(31));
          reach.reach("merge consumes its prepaid buffers");
          credits.reclaimIdleForecasts();
          for (const buffer of buffers.values()) expect(buffer.at(-1)).toBe(31);
        } finally {
          prepared?.release();
          for (const [account, buffer] of buffers) {
            expect(buffer.at(-1)).toBe(31);
            buffers.delete(account);
            account.free(bytes);
          }
          if (active) credits.releaseMerge(active);
          credits.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-idle-notification", [47229, 47230]))
  it(`wakes a queued reader after the last native step settles (seed ${String(seed)})`, async () => {
    const reach = createReach("idle phase notification", WRITE_IDLE_NOTIFICATION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 16 }), async (units) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20);
        const credits = await WriteCredits.open(governor, {
          rows: 1 << 20,
          metadata: 1 << 20,
          commits: 1 << 20,
          merge: 1 << 20,
        });
        const state = { busy: true, complete: false };
        const stop = new AbortController();
        const buffers = new Map<Grant, Uint8Array>();
        const bytes = units * 1024;
        let reader: Grant | undefined;
        let pending: Promise<readonly Grant[]> | undefined;
        try {
          for (const account of [credits.rows, credits.metadata, credits.commits]) {
            expect(account.allocate(bytes)).toBe(true);
            buffers.set(account, new Uint8Array(bytes).fill(19));
          }
          credits.rows.onReclaimRequest(() => (state.busy ? 0 : credits.reclaimIdleForecasts()));
          // A previously asked async metadata holder need not be asked twice. The
          // synchronous row callback can retire all of this idle owner's forecasts.
          credits.metadata.onReclaimRequest(() =>
            Promise.resolve(state.busy ? 0 : credits.reclaimIdleForecasts()),
          );
          pending = governor.admitGroup({
            label: "reader waiting for phase settlement",
            grants: [{ label: "complete reader", minimumBytes: 6 << 20, spillable: false }],
            includesProgress: true,
            signal: stop.signal,
          });
          void pending.then(
            (grants) => {
              reader = grants[0];
              state.complete = true;
            },
            () => undefined,
          );
          await yieldTask();
          expect(state.complete).toBe(false);
          reach.reach("reader queued during active step");
          for (const buffer of buffers.values()) expect(buffer.at(-1)).toBe(19);
          reach.reach("real buffers retained");
          state.busy = false;
          credits.notifyReclaimable();
          for (let turn = 0; turn < 16 && !state.complete; turn++) await yieldTask();
          const stalled = !state.complete;
          if (stalled) credits.reclaimIdleForecasts(); // cleanup only; intended RED is bounded progress.
          await pending;
          expect(stalled, "phase settlement must wake the already queued fitting reader").toBe(
            false,
          );
          expect(reader?.allocate(4096)).toBe(true);
          if (!reader) throw new Error("Missing admitted reader");
          buffers.set(reader, new Uint8Array(4096).fill(19));
          reach.reach("settlement wakes existing admission");
        } finally {
          stop.abort(new Error("notification history ended"));
          await Promise.allSettled(pending ? [pending] : []);
          for (const [account, buffer] of buffers) {
            const size = buffer.byteLength;
            expect(buffer.at(-1)).toBe(19);
            buffers.delete(account);
            account.free(size);
          }
          reader?.close();
          credits.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-donated-metadata", [47231, 47232]))
  it(`does not resurrect donated metadata credit for a later phase (seed ${String(seed)})`, async () => {
    const reach = createReach("donated metadata forecast", WRITE_DONATED_METADATA_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 16 }), async (units) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20);
        const minimum = { rows: 1 << 20, metadata: 1 << 20, commits: 1 << 20, merge: 1 << 20 };
        const credits = await WriteCredits.open(governor, minimum);
        const kept = units * 1024;
        try {
          await credits.prepareMetadata(minimum.metadata);
          reach.reach("metadata forecast funded");
          const total =
            credits.rows.held + credits.metadata.held + credits.commits.held + credits.merge.held;
          await credits.growTo(credits.rows, total - kept);
          expect(credits.metadata.held).toBe(kept);
          reach.reach("metadata credit donated to row step");
          await credits.prepareMerge(minimum.merge);
          expect(
            credits.metadata.held,
            "a later phase cannot restore an unfunded old forecast",
          ).toBe(kept);
          reach.reach("merge retains only still funded metadata");
          await credits.prepareMetadata(kept + 4096);
          expect(credits.metadata.held).toBeGreaterThanOrEqual(kept + 4096);
          reach.reach("real later metadata step readmitted");
        } finally {
          credits.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });
