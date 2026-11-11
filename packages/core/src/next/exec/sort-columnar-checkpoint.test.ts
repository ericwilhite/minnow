/** A checkpoint owns forward-replay borrows until sealing; reclaim/close must drain them. */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, COLUMNAR_CHECKPOINT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ColumnarSortOperator } from "./sort-columnar.js";
import { SortOperator, sortMinimumBytes, type SortSpec } from "./sort.js";
import type { RecordCheckpoint } from "./record-checkpoint.js";
import { INT, CollectSink, batchesOf } from "./join-fixtures.js";
const reach = createReach("columnar checkpoint replay", COLUMNAR_CHECKPOINT_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-columnar-checkpoint-replay", [0xb7d1, 0xb7d2]))
  for (const cancel of [false, true])
    it(`fences reclaim and cancellation during forward replay (${String(seed)},${String(cancel)})`, async () => {
      reach.beginHistory();
      const types = [INT, INT],
        governor = new Governor(64 << 20),
        spill = new MemorySpillService();
      const grant = await governor.admit({
        label: "checkpoint replay",
        minimumBytes: sortMinimumBytes(types),
        spillable: true,
      });
      const spec: SortSpec = {
        types,
        keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
        limit: null,
        offset: 0,
        withTies: false,
      };
      let entered!: () => void, release!: () => void;
      const borrowed = new Promise<void>((done) => {
          entered = done;
        }),
        gate = new Promise<void>((done) => {
          release = done;
        });
      let fallbackReclaimed = false;
      const op = new ColumnarSortOperator(
        spec,
        { grant, spill, queryId: seed },
        new CollectSink(),
        sortMinimumBytes(types),
        (context) => {
          const fallback = new SortOperator(spec, context, new CollectSink());
          return new Proxy(fallback, {
            get(target, property) {
              if (property === "push")
                return (...args: Parameters<SortOperator["push"]>) => {
                  target.push(...args);
                  entered();
                  return "blocked";
                };
              if (property === "ready")
                return async () => {
                  await gate;
                  await target.ready();
                };
              if (property === "reclaim")
                return () => {
                  fallbackReclaimed = true;
                  return target.reclaim();
                };
              const value: unknown = Reflect.get(target, property, target);
              return typeof value === "function" ? (value.bind(target) as unknown) : value;
            },
          });
        },
      );
      for (const batch of batchesOf(
        types,
        Array.from({ length: 4096 + (seed % 64) }, (_, i) => [i % 3, i]),
      ))
        if (op.push(batch) === "blocked") await op.ready();
      const work = op.checkpoint();
      void work.catch(() => undefined);
      let saved: RecordCheckpoint | undefined, reclaiming: Promise<void> | undefined;
      const reason = new Error("cancel replay");
      try {
        await borrowed;
        let reclaimed = false;
        reclaiming = Promise.resolve(op.reclaim()).then(() => {
          reclaimed = true;
        });
        await new Promise<void>((done) => setImmediate(done));
        expect(fallbackReclaimed, "checkpoint replay must fence the fallback reclaimer").toBe(
          false,
        );
        expect(reclaimed).toBe(false);
        reach.reach("reclaim waits for replay");
        if (cancel) {
          op.abort(reason);
          let closed = false;
          void op.closed.then(() => {
            closed = true;
          });
          await Promise.resolve();
          expect(closed).toBe(false);
          expect(governor.usedBytes).toBeGreaterThan(0);
          reach.reach("cancel retains replay credit");
        }
        release();
        if (cancel) await expect(work).rejects.toBe(reason);
        else {
          saved = await work;
          expect(saved.tape.rows).toBe(4096 + (seed % 64));
          reach.reach("sealed full prefix");
        }
        await reclaiming;
      } finally {
        release();
        saved ??= await work.catch(() => undefined);
        await reclaiming?.catch(() => undefined);
        op.abort(reason);
        await op.closed;
        await saved?.close();
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        reach.reach("zero resources");
      }
    });
