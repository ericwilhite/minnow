/** A suspended WAL encoder owns its complete prepaid frame window through acknowledgement. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_COMMIT_CONTINUATION_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "../write/credits.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  TEXT,
  texts,
  tableInput,
  rowsOf,
} from "../write/test-support.js";

for (const seed of seedsFor("next-write-credit-continuation", [47215, 47216]))
  it(`preserves active commit frames while another phase grows (seed ${String(seed)})`, async () => {
    const reach = createReach("native commit continuation", WRITE_COMMIT_CONTINUATION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 8 }), async (rows) => {
        reach.beginHistory();
        let credits: WriteCredits | undefined,
          reader: Grant | undefined,
          retained: Uint8Array | undefined;
        let armed = false,
          injected = false,
          frame = false,
          closed = false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let growth: Promise<void> | undefined, pending: Promise<unknown> | undefined;
        const release = () => {
          if (retained) expect(retained.at(-1)).toBe(71);
          retained = undefined;
          reader?.free(reader.allocated);
          reader?.close();
          reader = undefined;
        };
        const open = WriteCredits.open.bind(WriteCredits);
        const opened = vi.spyOn(WriteCredits, "open").mockImplementation(async (...args) => {
          credits = await open(...args);
          return credits;
        });
        const h = new Harness("strict", "memory", 65536, undefined, {
          merges: { mode: "off" },
          onCommitBuffer(buffer, live) {
            if (!armed || !live) return;
            if (!injected && buffer.length === 4096) {
              injected = true;
              growth = Promise.resolve().then(async () => {
                if (!armed || !credits || !reader) return;
                const bytes = h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes;
                expect(reader.allocate(bytes)).toBe(true);
                retained = new Uint8Array(bytes).fill(71);
                reach.reach("reader retained during prefix");
                await credits.growTo(credits.metadata, credits.metadata.held + 8 * 1024 * 1024);
                reach.reach("growth during active WAL step");
              });
              void growth.catch(() => undefined);
            } else if (injected && buffer.length >= 65536) {
              frame = true;
              timer ??= setTimeout(release, 0);
              reach.reach("later frame produced with prepaid credit");
            }
          },
        });
        Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
        const close = async () => {
          armed = false;
          if (timer !== undefined) clearTimeout(timer);
          release();
          await Promise.allSettled([...(growth ? [growth] : []), ...(pending ? [pending] : [])]);
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
              table: tableInput("continuation", [column("v", TEXT)], { primaryKey: null }),
            }),
          );
          await path.quiesce();
          reader = await h.governor.admit({
            label: "WAL continuation reader",
            minimumBytes: 0,
            spillable: true,
          });
          armed = true;
          const value = "x".repeat(256 * 1024);
          let completed = false,
            error: unknown = null;
          pending = autocommit(path, (s) =>
            s.table(1).insert(batch(texts(Array.from({ length: rows }, () => value))), null),
          ).then(
            () => {
              completed = true;
            },
            (cause: unknown) => {
              error = cause;
              completed = true;
            },
          );
          const settled = () => completed;
          for (let turn = 0; turn < 64 && !settled(); turn++) await yieldTask();
          expect(completed, "WAL continuation must progress in bounded task turns").toBe(true);
          expect(error).toBeNull();
          expect(injected).toBe(true);
          expect(frame).toBe(true);
          release();
          await growth;
          await pending;
          // The artificial next metadata phase has completed without retaining output.
          credits?.retireMetadataHeadroom(0);
          armed = false;
          await path.quiesce();
          expect(credits?.metadata.held, "finished metadata step returns its forecast").toBe(
            credits?.metadata.allocated,
          );
          reach.reach("completed metadata window retired");
          await h.reopen();
          const pin = h.path.pin();
          try {
            const actual = await rowsOf(pin, 1);
            expect(actual.length).toBe(rows);
            expect(actual.every((row) => row[0] === value)).toBe(true);
          } finally {
            pin.release();
          }
          reach.reach("exact strict commit and reopen");
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
