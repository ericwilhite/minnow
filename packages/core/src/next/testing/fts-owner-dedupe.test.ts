/** Real owner COMMIT ledger precedes FTS preparation, including in-flight retries.
 * Fixture rows use the native writer while the SQL modify executor is owned by B5b. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_OWNER_DEDUPE_REACH } from "../../testing/reach.js";
import { Harness, rowsOf, batch, texts, rules } from "../write/test-support.js";
import { setupFts, ftsParts } from "./fts-transaction.test-support.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { createLowerer } from "../lower/create.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { chunkReads } from "../exec/scan.js";
import { raw } from "./owner-harness.js";
import type { OwnerMessage } from "../owner/protocol.js";
for (const seed of seedsFor("next-fts-owner-dedupe", [0xb855, 0xb856]))
  it(`admits retries before FTS preparation through the real owner ${String(seed)}`, async (context) => {
    const reach = createReach("FTS owner dedupe", FTS_OWNER_DEDUPE_REACH);
    let stopped = false,
      retire: (() => Promise<void>) | undefined;
    context.onTestFinished(() => {
      stopped = true;
      void retire?.().catch(() => undefined);
    });
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 5 }), async (repeats) => {
        reach.beginHistory();
        const h = new Harness();
        let preparations = 0;
        h.onSpace = (space) => {
          const begin = space.beginWrite.bind(space);
          space.beginWrite = (label) => {
            if (label === "FTS preparation") preparations++;
            return begin(label);
          };
        };
        let active: ReturnType<typeof connection> | undefined;
        const close = async () => {
          const c = active;
          active = undefined;
          if (c) await c.close();
        };
        retire = close;
        function connection(epoch: number) {
          const engine = composeEngine({
            planner: createLowerer(),
            writePath: h.path,
            governor: h.governor,
            spill: new MemorySpillService(),
            reads: chunkReads,
          });
          const server = new OwnerServer(engine, {
              ownerId: `fts-${String(epoch)}`,
              epoch,
              build: "test",
            }),
            port = raw(server);
          return {
            port,
            async close() {
              const stop = server.quiesce("closing");
              await stop.drained;
              stop.finish();
              await engine.close();
            },
          };
        }
        async function replies(n: number, seq = 2): Promise<OwnerMessage[]> {
          for (let turn = 0; turn < 10_000; turn++) {
            if (stopped) throw new Error("Retired FTS dedupe history");
            const out =
              active?.port.replies.filter(
                (m) => (m.t === "done" || m.t === "error") && m.seq === seq,
              ) ?? [];
            if (out.length >= n) return out;
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
          }
          throw new Error("No native FTS terminal reply");
        }
        try {
          const path = await setupFts(h);
          // B5b's SQL modify executor is not on this base. Seed accepted native rows into
          // the exact transaction BEGIN receives; COMMIT, WAL, ledger and recovery are real.
          const begin = path.begin.bind(path);
          let seeded: Awaited<ReturnType<typeof begin>> | null = await begin();
          const statement = seeded.statement({ rules });
          await statement.table(1).insert(batch(texts(["alpha alpha beta"])), null);
          await statement.end();
          path.begin = (options) => {
            if (seeded) {
              const tx = seeded;
              seeded = null;
              return Promise.resolve(tx);
            }
            return begin(options);
          };
          active = connection(1);
          active.port.deliver(active.port.statement(1, "BEGIN", [], { resendable: false }));
          expect(await replies(1, 1)).toMatchObject([{ t: "done" }]);
          const request = active.port.statement(2, "COMMIT", [], { resendable: false });
          for (let i = 0; i < repeats; i++) active.port.deliver(request);
          const first = await replies(repeats);
          expect(
            first.every((m) => m.t === "done"),
            JSON.stringify(first),
          ).toBe(true);
          expect(preparations, "duplicate must not enter FTS preparation").toBe(1);
          expect(first.filter((m) => m.t === "done" && m.summary.replayed)).toHaveLength(
            repeats - 1,
          );
          reach.reach("in flight");
          active.port.deliver(request);
          const terminal = await replies(repeats + 1);
          expect(terminal.at(-1)).toMatchObject({ t: "done", summary: { replayed: true } });
          expect(preparations).toBe(1);
          reach.reach("terminal retry");
          // A retry of the first COMMIT must not commit a later transaction's native rows.
          seeded = await begin();
          const later = seeded.statement({ rules });
          await later.table(1).insert(batch(texts(["uncommitted beta"])), null);
          await later.end();
          active.port.deliver(active.port.statement(3, "BEGIN", [], { resendable: false }));
          expect(await replies(1, 3)).toMatchObject([{ t: "done" }]);
          active.port.deliver(request);
          expect((await replies(repeats + 2)).at(-1)).toMatchObject({
            t: "done",
            summary: { replayed: true },
          });
          expect(preparations).toBe(1);
          active.port.deliver(active.port.statement(4, "ROLLBACK", [], { resendable: false }));
          expect(await replies(1, 4)).toMatchObject([{ t: "done" }]);
          reach.reach("later transaction untouched");
          const expected = (await ftsParts(h)).entries;
          expect(expected).toHaveLength(1);
          await close();
          await h.reopen();
          active = connection(2);
          active.port.deliver({ ...request, retry: true });
          expect(await replies(1)).toMatchObject([{ t: "done", summary: { replayed: true } }]);
          expect(preparations).toBe(1);
          reach.reach("replayed ledger");
          const pin = h.path.pin();
          try {
            expect(await rowsOf(pin, 1)).toEqual([["alpha alpha beta"]]);
          } finally {
            pin.release();
          }
          expect((await ftsParts(h)).entries).toEqual(expected);
          reach.reach("rows once");
          reach.reach("parts once");
          await h.path.recycleLog();
          await close();
          await h.reopen();
          active = connection(3);
          active.port.deliver({ ...request, retry: true });
          expect(await replies(1)).toMatchObject([{ t: "done", summary: { replayed: true } }]);
          expect(preparations).toBe(1);
          reach.reach("checkpointed ledger");
        } finally {
          await close();
          await h.close();
          retire = undefined;
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);
