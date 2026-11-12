import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STAR_EDGE_REACH } from "../../testing/reach.js";
import type { Batch } from "../vector/vector.js";
import { starColumns } from "./star-pipeline.js";

it.each(seedsFor("next-star-edge", [0xb76d, 0xb76e]))(
  "borrows recoded output and retires old input ownership (seed %i)",
  async (seed) => {
    const reach = createReach("star representation edge", STAR_EDGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("ready", "blocked", "cancel", "map-error", "push-error"),
        async (mode) => {
          reach.beginHistory();
          const batch: Batch = { length: 1, selection: null, columns: [] };
          let released = 0,
            pushes = 0;
          let wake: () => void = () => undefined,
            fail: (reason: unknown) => void = () => undefined;
          const waiting = new Promise<void>((resolve, reject) => {
            wake = resolve;
            fail = reject;
          });
          void waiting.catch(() => undefined);
          const error = new Error("star edge fault");
          const edge = starColumns(
            (input) => {
              expect(input).toBe(batch);
              if (mode === "map-error") throw error;
              return { ...input };
            },
            {
              push(output, transfer) {
                expect(output).not.toBe(batch);
                expect(transfer, "recoded arrays cannot detach old input buffers").toBeUndefined();
                pushes++;
                if (mode === "push-error") throw error;
                return mode === "ready" ? "ready" : "blocked";
              },
              ready: () => waiting,
              finish: () => undefined,
              abort: () => fail(error),
            },
          );
          const transfer = { detachableBuffers: new Set<ArrayBuffer>(), release: () => released++ };
          if (mode.endsWith("error")) {
            expect(() => edge.push(batch, transfer)).toThrow(error);
            expect(pushes).toBe(mode === "map-error" ? 0 : 1);
          } else {
            expect(edge.push(batch, transfer)).toBe(mode === "ready" ? "ready" : "blocked");
            if (mode !== "ready") {
              expect(released).toBe(0);
              const ready = edge.ready();
              if (mode === "cancel") {
                edge.abort(error);
                await expect(ready).rejects.toBe(error);
              } else {
                wake();
                await ready;
              }
            }
          }
          edge.abort(error);
          expect(released).toBe(1);
          reach.reach(mode);
        },
      ),
      {
        seed,
        numRuns: 15,
        examples: [["ready"], ["blocked"], ["cancel"], ["map-error"], ["push-error"]],
      },
    );
    reach.assertReached();
  },
);
