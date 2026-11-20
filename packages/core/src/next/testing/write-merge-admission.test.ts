/** Governor rules2/4/7: cold run admission precedes private merge ownership. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_MERGE_ADMISSION_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { MetadataPressure } from "../write/budget.js";
import { WriteCredits } from "../write/credits.js";
import { RunReader } from "../write/runs.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
  watchGrants,
} from "../write/test-support.js";

for (const seed of seedsFor("next-write-merge-admission", [47207, 47208]))
  it(`unwinds cold metadata pressure before retaining a merge (seed ${String(seed)})`, async () => {
    const reach = createReach("merge admission", WRITE_MERGE_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 4 }), async (parts) => {
        reach.beginHistory();
        const h = new Harness("strict", "memory", 2, undefined, {
          merges: { mode: "manual", fanIn: 2 },
        });
        Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
        let metadata: Grant | undefined;
        const accounts: Grant[] = [],
          donors: Array<{ grant: Grant; buffer?: Uint8Array; bytes: number }> = [];
        const caches: Array<Parameters<Governor["registerCache"]>[0]> = [];
        const register = h.governor.registerCache.bind(h.governor);
        h.governor.registerCache = (cache) => {
          caches.push(cache);
          return register(cache);
        };
        watchGrants(h.governor, (grant) => {
          if (grant.label === "write path metadata") metadata = grant;
          if (
            [
              "write path",
              "write path metadata",
              "write commit buffers",
              "write merge credit",
            ].includes(grant.label)
          )
            accounts.push(grant);
        });
        let armed = false,
          injected = false,
          retainedAtAdmission = 0,
          metadataLost = 0,
          admissionSteps = 0;
        let activeMerge: Grant | undefined,
          reader: Grant | undefined,
          bytes: Uint8Array | undefined;
        let pending: Promise<void> | undefined;
        let closed = false;
        const releaseDonors = () => {
          for (const held of donors.splice(0)) {
            expect(held.buffer?.at(-1)).toBe(13);
            delete held.buffer;
            held.grant.free(held.bytes);
          }
        };
        const releaseReader = () => {
          if (bytes) expect(bytes.at(-1)).toBe(17);
          bytes = undefined;
          reader?.free(reader.allocated);
          reader?.close();
          reader = undefined;
        };
        const open = RunReader.open.bind(RunReader);
        const opening = vi.spyOn(RunReader, "open").mockImplementation((...args) => {
          if (armed && !injected) {
            injected = true;
            if (!metadata || !reader) throw new Error("Missing cold-admission accounts");
            for (const cache of caches) cache.evict(cache.evictableBytes);
            const reclaimable = accounts.reduce((n, g) => n + g.allocated, 0);
            for (const grant of accounts) {
              const idle = grant.held - grant.allocated - grant.loaned;
              if (idle > 0) {
                expect(grant.allocate(idle)).toBe(true);
                donors.push({ grant, bytes: idle, buffer: new Uint8Array(idle).fill(13) });
              }
            }
            reach.reach("other accounts own real buffers");
            const free = h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes;
            expect(reader.allocate(free)).toBe(true);
            bytes = new Uint8Array(free).fill(17);
            reach.reach("cold read under competing credit");
            throw new MetadataPressure(reclaimable + 256, metadata.held + reclaimable + 256);
          }
          return open(...args);
        });
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to each native owner below.
        const acquire = WriteCredits.prototype.acquireMerge;
        const acquired = vi
          .spyOn(WriteCredits.prototype, "acquireMerge")
          .mockImplementation(async function (this: WriteCredits, ...args) {
            activeMerge = await acquire.apply(this, args);
            try {
              // A query can ask for idle credit in the handoff before beginWrite.
              // The real engine's binder must protect the funded metadata window
              // as well as the private merge grant, even before a writer exists.
              if (armed && typeof this.reclaimIdleForecasts === "function") {
                const before = this.metadata.held;
                this.reclaimIdleForecasts();
                if (this.metadata.held !== before) {
                  metadataLost++;
                  // Observer cleanup only: restore credit just returned so a
                  // rejected mutant can finish rather than mask its assertion.
                  if (!this.metadata.tryGrow(before - this.metadata.held))
                    throw new Error(
                      "funded native metadata lost in merge handoff: cleanup could not restore credit",
                    );
                }
                reach.reach("metadata owned through native merge handoff");
              }
              return activeMerge;
            } catch (error) {
              // The observer owns this handoff until it returns: unwind it even
              // when a mutant is rejected, preserving the causal assertion.
              release.call(this, activeMerge);
              activeMerge = undefined;
              throw error;
            }
          });
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to each native owner below.
        const release = WriteCredits.prototype.releaseMerge;
        const released = vi
          .spyOn(WriteCredits.prototype, "releaseMerge")
          .mockImplementation(function (this: WriteCredits, grant) {
            release.call(this, grant);
            if (activeMerge === grant) activeMerge = undefined;
          });
        // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to each native owner below.
        const grow = WriteCredits.prototype.growTo;
        const growth = vi.spyOn(WriteCredits.prototype, "growTo").mockImplementation(function (
          this: WriteCredits,
          ...args
        ) {
          if (armed && bytes !== undefined) {
            reach.reach("metadata retry reached");
          }
          return grow.apply(this, args).finally(releaseDonors);
        });
        const admit = h.governor.admitGroup.bind(h.governor);
        const admitted = vi.spyOn(h.governor, "admitGroup").mockImplementation((options) => {
          if (armed && ++admissionSteps > 128)
            throw new Error("Cold metadata retried without bounded progress");
          const pending = admit(options);
          if (!armed || bytes === undefined || !options.label.startsWith("complete grow "))
            return pending;
          return (async () => {
            const state = { settled: false };
            void pending.then(
              () => {
                state.settled = true;
              },
              () => {
                state.settled = true;
              },
            );
            for (let turn = 0; turn < 8; turn++) await Promise.resolve();
            expect(state.settled, "cold retry must really wait for external credit").toBe(false);
            if ((activeMerge?.held ?? 0) > 0) retainedAtAdmission++;
            reach.reach("real global metadata wait");
            releaseReader();
            return await pending;
          })();
        });
        const restore = () => {
          armed = false;
          opening.mockRestore();
          acquired.mockRestore();
          released.mockRestore();
          growth.mockRestore();
          admitted.mockRestore();
        };
        const close = async () => {
          armed = false;
          releaseReader();
          releaseDonors();
          if (pending) await Promise.allSettled([pending]);
          restore();
          if (!closed) {
            closed = true;
            await h.close();
          }
        };
        onTestFinished(close);
        try {
          let path = await h.open();
          await autocommit(path, (s) =>
            s.changeCatalog({
              kind: "createTable",
              table: tableInput("merge_credit", [column("id", INT, false)], { primaryKey: [0] }),
            }),
          );
          for (let part = 0; part <= parts; part++) {
            await autocommit(path, (s) =>
              s.table(1).insert(batch(ints([part * 2, part * 2 + 1])), null),
            );
            // Keep successive frozen memtables in distinct durable runs.
            await path.quiesce();
          }
          accounts.length = caches.length = 0;
          await h.reopen();
          path = h.path;
          reader = await h.governor.admit({
            label: "merge admission competitor",
            minimumBytes: 0,
            spillable: true,
          });
          armed = true;
          let complete = false;
          pending = path.runMaintenance().then(() => {
            complete = true;
          });
          void pending.catch(() => undefined);
          const settled = () => complete;
          for (let turn = 0; turn < 64 && !settled(); turn++) await yieldTask();
          expect(complete, "merge must make bounded progress after competitor settlement").toBe(
            true,
          );
          await pending;
          armed = false;
          expect(injected).toBe(true);
          expect(metadataLost, "funded native metadata lost in merge handoff").toBe(0);
          expect(
            retainedAtAdmission,
            "private merge credit retained across metadata admission",
          ).toBe(0);
          reach.reach("no private merge during admission");
          expect(path.maintenance.merges).toBeGreaterThan(0);
          expect(path.maintenance.mergeFailures).toBe(0);
          const pin = path.pin();
          try {
            expect(await rowsOf(pin, 1)).toEqual(
              Array.from({ length: (parts + 1) * 2 }, (_, id) => [id]),
            );
            reach.reach("exact merged rows");
          } finally {
            pin.release();
          }
        } finally {
          await close();
        }
        expect(h.governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 8, endOnFailure: true },
    );
    reach.assertReached();
  });
