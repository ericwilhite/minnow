/** An already-cancelled wait still observes the producer it was handed. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PIPELINE_ABORT_WAIT_REACH } from "../../testing/reach.js";
import { abortable } from "./pipeline.js";

for (const seed of seedsFor("next-pipeline-abort-wait", [0xb7ee, 0xb7ef]))
  it(`observes both sides of a cancelled wait seed ${String(seed)}`, async () => {
    const reach = createReach("pipeline cancelled wait", PIPELINE_ABORT_WAIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (preAborted, rejects, pending) => {
          reach.beginHistory();
          const stop = new AbortController(),
            cancel = new Error("cancelled wait"),
            producerError = new Error("producer failed");
          let resolve!: (value: number) => void, reject!: (reason: unknown) => void;
          const producer = new Promise<number>((yes, no) => {
            resolve = yes;
            reject = no;
          });
          if (!pending) {
            if (rejects) reject(producerError);
            else resolve(7);
          }
          if (preAborted) stop.abort(cancel);
          const waiting = abortable(producer, stop.signal);
          // Register the consumer before settlement, exactly as Driver.#resume does.
          const observed = waiting.then(
            (value) => ({ value }),
            (reason: unknown) => ({ reason }),
          );
          if (!preAborted) stop.abort(cancel);
          if (pending) {
            if (rejects) reject(producerError);
            else resolve(7);
          }
          expect(await observed).toEqual({ reason: cancel });
          // A genuine turn lets any unobserved producer rejection reach Vitest. No custom
          // unhandledRejection listener/suppression, wall-clock expiry or timeout tolerance.
          await new Promise<void>((done) => setImmediate(done));
          reach.reach(preAborted ? "already aborted" : "abort during wait");
          reach.reach(rejects ? "producer rejects" : "producer resolves");
          reach.reach(pending ? "pending producer" : "settled producer");
          reach.reach("original cancellation reason");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [true, true, false],
          [true, true, true],
          [false, true, true],
          [true, false, false],
        ],
      },
    );
    reach.assertReached();
  });
