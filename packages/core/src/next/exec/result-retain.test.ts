/** Law: governor rule 1 and cache takeFrom ownership; adoption cannot free/reacquire credit. */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { ReservedGrant } from "./admission.js";
import { DeferredGrant } from "./deferred-grant.js";
import { ResultChannel } from "./result.js";
import { ResultSink } from "./result-sink.js";
import { I32, numbers } from "./kernels/test-support.js";

it.each([false, true])(
  "moves a result to retained cache once, including deferred native credit: %s",
  async (native) => {
    const governor = new Governor(8 * 1024 * 1024);
    const fixed = await governor.admit({ label: "fixed", minimumBytes: 4096, spillable: true });
    const extra = await governor.admit({ label: "extra", minimumBytes: 0, spillable: true });
    const deferred = new DeferredGrant("native result");
    if (native)
      deferred.install(
        new ReservedGrant(fixed, extra, fixed.held, null, false, undefined, true),
        () => undefined,
      );
    const source = native ? deferred : fixed;
    const cache = governor.registerCache({ evictableBytes: 0, evict: () => 0 });
    const channel = new ResultChannel([{ name: "n", type: I32, nullable: false }]);
    const sink = new ResultSink([I32], source, channel, 0);
    sink.push({ columns: [numbers(I32, [7])], length: 1, selection: null });
    await sink.finish();
    const frame = await channel.next();
    if (frame?.adopt === undefined) throw new Error("Missing native frame capability");
    const allocated = source.allocated;
    expect(frame.adopt(() => false)).toBeNull();
    expect(source.allocated).toBe(allocated);
    const adopted = frame.adopt((grant, bytes) => cache.takeFrom(grant, bytes));
    expect(adopted).toBeGreaterThan(0);
    expect(frame.adopt(() => true)).toBeNull();
    frame.release();
    frame.release();
    expect(await channel.next()).toBeNull();
    await channel.delivered;
    source.close();
    extra.close();
    expect(governor.usedBytes).toBe(adopted);
    const column = frame.payload.columns[0];
    expect(column?.form === "flat" && column.data.layout === "i32" && column.data.values[0]).toBe(
      7,
    );
    cache.release(adopted ?? 0);
    cache.unregister();
    expect(governor.usedBytes).toBe(0);
  },
);
