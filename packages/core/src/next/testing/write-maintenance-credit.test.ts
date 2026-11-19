/** Queued maintenance owns no provisional writer and cannot pin future workspace. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITE_MAINTENANCE_CREDIT_REACH,
  WRITE_QUEUED_READER_REACH,
} from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "../write/credits.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "../write/test-support.js";

for (const seed of seedsFor("next-write-maintenance-credit", [47217, 47218]))
  it(`grows while maintenance waits for the current transaction (seed ${String(seed)})`, async () => {
    const reach = createReach("queued native maintenance credit", WRITE_MAINTENANCE_CREDIT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (units) => {
        reach.beginHistory();
        const h = new Harness("strict", "memory", 128, undefined, { merges: { mode: "off" } });
        Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
        let credits: WriteCredits | undefined,
          holder: Grant | undefined,
          bytes: Uint8Array | undefined;
        let maintenance: Promise<void> | undefined, growth: Promise<void> | undefined;
        let tx: Awaited<ReturnType<Harness["path"]["begin"]>> | undefined;
        let armed = false,
          closed = false,
          observed = false;
        const open = WriteCredits.open.bind(WriteCredits);
        const opened = vi.spyOn(WriteCredits, "open").mockImplementation(async (...args) => {
          credits = await open(...args);
          return credits;
        });
        const release = () => {
          if (bytes) expect(bytes.at(-1)).toBe(37);
          bytes = undefined;
          holder?.free(holder.allocated);
          holder?.close();
          holder = undefined;
        };
        const close = async () => {
          armed = false;
          release();
          tx?.rollback();
          tx = undefined;
          await Promise.allSettled([
            ...(growth ? [growth] : []),
            ...(maintenance ? [maintenance] : []),
          ]);
          opened.mockRestore();
          if (!closed) {
            closed = true;
            await h.close();
          }
        };
        onTestFinished(close);
        try {
          const path = await h.open();
          await autocommit(path, (s) =>
            s.changeCatalog({
              kind: "createTable",
              table: tableInput("queued", [column("id", INT)], { primaryKey: null }),
            }),
          );
          await path.quiesce();
          expect(credits?.metadata.held, "completed WAL returns its metadata window").toBe(
            credits?.metadata.allocated,
          );
          reach.reach("completed WAL window returned");
          tx = await path.begin();
          maintenance = path.recycleLog();
          void maintenance.catch(() => undefined);
          await yieldTask();
          expect(path.maintenanceState.recycling).toBe(true);
          reach.reach("maintenance queued behind transaction");
          if (!credits) throw new Error("Missing native credits");
          // This is an admitted future step, not work owned by queued maintenance.
          await credits.prepareMetadata(2 << 20);
          holder = await h.governor.admit({
            label: "queued-maintenance reader",
            minimumBytes: 0,
            spillable: true,
          });
          const spare = h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes;
          expect(holder.allocate(spare)).toBe(true);
          bytes = new Uint8Array(spare).fill(37);
          const transfer = h.governor.transferCredit.bind(h.governor);
          h.governor.transferCredit = (source, target, bytes) => {
            const moved = transfer(source, target, bytes);
            if (
              armed &&
              moved &&
              bytes > 0 &&
              source === credits?.metadata &&
              target === credits.rows
            ) {
              observed = true;
              reach.reach("idle metadata moves before row admission");
            }
            return moved;
          };
          armed = true;
          let completed = false,
            error: unknown = null;
          const wanted = credits.rows.held + credits.commits.held + units * (128 << 10);
          growth = credits.growTo(credits.rows, wanted).then(
            () => {
              completed = true;
            },
            (cause: unknown) => {
              error = cause;
              completed = true;
            },
          );
          const settled = () => completed;
          for (let turn = 0; turn < 32 && !settled(); turn++) await yieldTask();
          expect(completed, "writer progress must not depend on its own maintenance slot").toBe(
            true,
          );
          expect(error).toBeNull();
          expect(observed).toBe(true);
          reach.reach("writer progresses before slot release");
          armed = false;
          release();
          tx.rollback();
          tx = undefined;
          await maintenance;
          reach.reach("maintenance completes after rollback");
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

for (const seed of seedsFor("next-write-queued-reader", [47237, 47238]))
  it(`reclaims idle forecasts while recycling waits for a transaction (seed ${String(seed)})`, async () => {
    const reach = createReach("reader behind queued recycling", WRITE_QUEUED_READER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 48, 64),
        fc.integer({ min: 1, max: 3 }),
        fc.integer({ min: 1, max: 4 }),
        async (cap, headroom, count) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 128, undefined, { merges: { mode: "off" } });
          Object.defineProperty(h, "governor", { value: new Governor(cap * (1 << 20)) });
          const stop = new AbortController();
          let credits: WriteCredits | undefined,
            tx: Awaited<ReturnType<Harness["path"]["begin"]>> | undefined,
            maintenance: Promise<void> | undefined,
            pending: Promise<void> | undefined,
            reader: Grant | undefined,
            output: Uint8Array | undefined,
            closing: Promise<void> | undefined;
          const state: { settled: boolean; error: unknown } = { settled: false, error: null };
          const open = WriteCredits.open.bind(WriteCredits);
          const captured = vi
            .spyOn(WriteCredits, "open")
            .mockImplementation(async (...args) => (credits = await open(...args)));
          const release = () => {
            if (output) expect(output.at(-1)).toBe(53);
            output = undefined;
            reader?.free(reader.allocated);
            reader?.close();
            reader = undefined;
          };
          const close = () =>
            (closing ??= (async () => {
              stop.abort(new Error("queued reader cleanup"));
              if (pending) await pending;
              release();
              tx?.rollback();
              tx = undefined;
              if (maintenance) await maintenance;
              captured.mockRestore();
              await h.close();
            })());
          onTestFinished(close);
          try {
            const path = await h.open();
            await autocommit(path, (s) =>
              s.changeCatalog({
                kind: "createTable",
                table: tableInput("queued_reader", [column("id", INT)], { primaryKey: null }),
              }),
            );
            const values = Array.from({ length: count }, (_, i) => i);
            await autocommit(path, (s) => s.table(1).insert(batch(ints(values)), null));
            await path.quiesce();
            tx = await path.begin();
            maintenance = path.recycleLog();
            void maintenance.catch(() => undefined);
            await yieldTask();
            expect(path.maintenanceState.recycling).toBe(true);
            reach.reach("recycling waits behind idle transaction");
            if (!credits) throw new Error("Missing native credits");
            await credits.prepareMetadata(headroom * (1 << 20));
            expect(credits.metadata.held - credits.metadata.allocated).toBeGreaterThanOrEqual(
              headroom * (1 << 20),
            );
            reach.reach("future workspace has no live allocation");
            pending = h.governor
              .admitGroup({
                label: "reader behind queued recycling",
                includesProgress: true,
                grants: [
                  {
                    label: "complete reader",
                    minimumBytes: (cap - 6) * (1 << 20),
                    spillable: false,
                  },
                ],
                signal: stop.signal,
              })
              .then(
                (grants) => {
                  reader = grants[0];
                  state.settled = true;
                },
                (error: unknown) => {
                  state.error = error;
                  state.settled = true;
                },
              );
            for (let turn = 0; turn < 16 && !state.settled; turn++) await yieldTask();
            const blocked = !state.settled;
            // Cleanup aid only: preserve the bounded progress assertion on the parent/mutant.
            if (blocked) credits.reclaimIdleForecasts();
            await pending;
            expect(blocked, "queued maintenance cannot pin dormant writer credit").toBe(false);
            expect(state.error).toBeNull();
            expect(path.maintenanceState.recycling).toBe(true);
            reach.reach("fitting reader progresses before transaction releases slot");
            expect(reader?.allocate(65536)).toBe(true);
            output = new Uint8Array(65536).fill(53);
            release();
            tx.rollback();
            tx = undefined;
            await maintenance;
            reach.reach("recycling completes after reader and transaction");
            await autocommit(path, (s) => s.table(1).insert(batch(ints([count])), null));
            const pin = path.pin();
            try {
              expect(await rowsOf(pin, 1)).toEqual([...values, count].map((value) => [value]));
            } finally {
              pin.release();
            }
            reach.reach("exact rows and continued writes");
            reach.reach(cap === 32 ? "32 MiB" : cap === 48 ? "48 MiB" : "64 MiB");
          } finally {
            await close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        // Three fixed cap boundaries plus eight random histories per seed.
        numRuns: 11,
        examples: [
          [32, 1, 1],
          [48, 2, 2],
          [64, 3, 4],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
