/** Series borrows its arguments through downstream readiness and cancellation. */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { OutputArena } from "./kernels/vectors.js";
import { INT, batchesOf } from "./join-fixtures.js";
import { SeriesOperator } from "./series.js";
import type { Operator } from "./pipeline.js";
it.each(["blocked abort", "synchronous abort", "consumer failure"])(
  "keeps the input transfer until %s settles",
  async (phase) => {
    const governor = new Governor(4 << 20),
      grant = await governor.admit({
        label: "series borrow",
        minimumBytes: 1 << 20,
        spillable: true,
      });
    const arena = new OutputArena(grant, 32),
      reason = new Error(phase);
    let release!: () => void,
      entered!: () => void,
      released = 0;
    const gate = new Promise<void>((resolve) => {
        release = resolve;
      }),
      begun = new Promise<void>((resolve) => {
        entered = resolve;
      });
    const sink: Operator = {
      push: () => {
        entered();
        if (phase === "consumer failure") throw reason;
        if (phase === "synchronous abort") op.abort(reason);
        return "blocked";
      },
      ready: () => gate,
      finish: () => undefined,
      abort: () => undefined,
    };
    const op = new SeriesOperator("integer", [INT, INT, INT], arena, sink);
    const input = batchesOf([INT, INT, INT], [[1, 200, 1]])[0];
    if (!input) throw new Error("Missing arguments");
    op.push(input, {
      release: () => {
        released++;
      },
    });
    const outcome = op.ready().then(
      () => null,
      (error: unknown) => error,
    );
    try {
      await begun;
      if (phase !== "consumer failure") {
        if (phase === "blocked abort") op.abort(reason);
        let closed = false;
        const closing = op.closed.then(() => {
          closed = true;
        });
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(closed).toBe(false);
        expect(released).toBe(0);
        release();
        await closing;
      }
      expect(await outcome).toBe(reason);
      expect(released).toBe(1);
    } finally {
      release();
      op.abort(reason);
      await outcome;
      await op.closed;
      arena.close();
      grant.close();
    }
    expect(governor.usedBytes).toBe(0);
  },
);
