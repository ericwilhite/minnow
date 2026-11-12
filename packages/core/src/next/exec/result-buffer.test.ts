/** One continuously charged backing allocation changes cleanup owner at a successful offer. */
import { expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import { ResultBuffers, takeResultBuffers } from "./result-buffer.js";
import { ResultChannel } from "./result.js";
import { ResultSink } from "./result-sink.js";
import { TEXT } from "./kernels/test-support.js";
import type { Batch } from "../vector/vector.js";
function batch(owned: ResultBuffers, size: number): Batch {
  const bytes = owned.bytes(size);
  bytes.fill(65);
  const offsets = new Int32Array(owned.bytes(8).buffer);
  offsets[1] = size;
  return {
    length: 1,
    selection: null,
    columns: [
      {
        form: "flat",
        type: TEXT,
        length: 1,
        validity: null,
        data: { layout: "varlen", bytes, offsets },
      },
    ],
  };
}
it.each(["credit", "cancel", "offer failure"] as const)(
  "owns exact result buffers once through %s",
  async (mode) => {
    const governor = new Governor(8 << 20),
      size = 65536,
      charged = size + 8;
    const grant = await governor.admit({
      label: "result",
      minimumBytes: charged + 4096,
      spillable: true,
    });
    const channel = new ResultChannel([{ name: "j", type: TEXT, nullable: false }], {
      windowBytes: 1024,
    });
    const sink = new ResultSink([TEXT], grant, channel, 0);
    const allocate = vi.spyOn(grant, "allocate");
    const owned = await sink.allocateOwnedResult(charged),
      input = batch(owned, size),
      column = input.columns[0];
    if (column?.form !== "flat" || column.data.layout !== "varlen")
      throw new Error("Expected text");
    const backing = column.data.bytes.buffer;
    const reason = new Error("injected offer failure");
    const offer =
      mode === "offer failure"
        ? vi.spyOn(channel, "offer").mockImplementation(() => {
            throw reason;
          })
        : null;
    let work: Promise<void> | undefined;
    try {
      if (mode === "offer failure") expect(() => sink.push(input, owned.transfer)).toThrow(reason);
      else {
        const pushed = sink.push(input, owned.transfer);
        work = (async () => {
          if (pushed === "blocked") await sink.ready();
          await sink.finish();
        })();
        void work.catch(() => undefined);
        const frame = await channel.next();
        if (!frame) throw new Error("Expected frame");
        expect(frame.transfer).toContain(backing);
        expect(grant.allocated).toBeGreaterThanOrEqual(charged);
        expect(allocate.mock.calls.filter(([n]) => n === charged)).toHaveLength(1);
        if (mode === "cancel") {
          sink.discard(reason);
          channel.cancel(reason);
          await expect(work).rejects.toThrow();
        } else {
          const copy = structuredClone(frame.payload, { transfer: [...frame.transfer] });
          expect(copy.rowCount).toBe(1);
          frame.release();
          await work;
          await channel.delivered;
        }
      }
    } finally {
      offer?.mockRestore();
      owned.close();
      sink.discard(reason);
      channel.cancel(reason);
      if (work) await Promise.allSettled([work]);
      allocate.mockRestore();
      expect(grant.allocated).toBe(0);
      grant.close();
      expect(governor.usedBytes).toBe(0);
    }
  },
);
it("copies a foreign result allocation instead of adopting its grant", async () => {
  const governor = new Governor(8 << 20),
    size = 65536;
  const foreign = await governor.admit({
      label: "foreign",
      minimumBytes: size + 8,
      spillable: true,
    }),
    grant = await governor.admit({ label: "result", minimumBytes: 2 * size, spillable: true });
  const channel = new ResultChannel([{ name: "j", type: TEXT, nullable: false }], {
      windowBytes: 1024,
    }),
    sink = new ResultSink([TEXT], grant, channel, 0);
  const owned = new ResultBuffers(foreign, size + 8),
    input = batch(owned, size),
    column = input.columns[0];
  if (column?.form !== "flat" || column.data.layout !== "varlen") throw new Error("Expected text");
  const source = column.data.bytes.buffer;
  const work = (async () => {
    if (sink.push(input, owned.transfer) === "blocked") await sink.ready();
    await sink.finish();
  })();
  void work.catch(() => undefined);
  try {
    const frame = await channel.next();
    if (!frame) throw new Error("Expected frame");
    expect(frame.transfer).not.toContain(source);
    expect(foreign.allocated).toBe(0);
    frame.release();
    await work;
    await channel.delivered;
  } finally {
    owned.close();
    sink.discard(new Error("cleanup"));
    channel.cancel();
    await Promise.allSettled([work]);
    foreign.close();
    grant.close();
    expect(governor.usedBytes).toBe(0);
  }
});

it.each([false, true])(
  "waits only for existing consumer credit, cancellably=%s",
  async (cancel) => {
    const size = 65536,
      governor = new Governor(8 << 20),
      grant = await governor.admit({ label: "result", minimumBytes: size + 4096, spillable: true });
    const channel = new ResultChannel([{ name: "j", type: TEXT, nullable: false }], {
        windowBytes: 1024,
      }),
      sink = new ResultSink([TEXT], grant, channel, 0);
    const first = await sink.allocateOwnedResult(size + 8),
      input = batch(first, size);
    const admit = vi.spyOn(governor, "admit"),
      group = vi.spyOn(governor, "admitGroup"),
      borrow = vi.spyOn(grant, "borrow");
    let second: ResultBuffers | undefined;
    const pushed = sink.push(input, first.transfer),
      producer = pushed === "blocked" ? sink.ready() : Promise.resolve();
    void producer.catch(() => undefined);
    const frame = await channel.next();
    if (!frame) throw new Error("Expected frame");
    let settled = false;
    const waiting = sink.allocateOwnedResult(size + 8).then(
      (value) => {
        settled = true;
        second = value;
        return value;
      },
      (error: unknown) => {
        settled = true;
        throw error;
      },
    );
    void waiting.catch(() => undefined);
    const reason = new Error("consumer credit cancelled");
    try {
      // No memory admission or side loan may satisfy this consumer-credit wait.
      await Promise.resolve();
      await Promise.resolve();
      expect(settled).toBe(false);
      if (cancel) {
        sink.discard(reason);
        channel.cancel(reason);
        await expect(waiting).rejects.toThrow(reason);
        await expect(producer).rejects.toThrow();
      } else {
        frame.release();
        await waiting;
        second?.close();
        await producer;
        await sink.finish();
        await channel.delivered;
      }
      expect(admit).not.toHaveBeenCalled();
      expect(group).not.toHaveBeenCalled();
      expect(borrow).not.toHaveBeenCalled();
    } finally {
      second?.close();
      first.close();
      sink.discard(reason);
      channel.cancel(reason);
      await Promise.allSettled([waiting, producer]);
      admit.mockRestore();
      group.mockRestore();
      borrow.mockRestore();
      expect(grant.allocated).toBe(0);
      grant.close();
      expect(governor.usedBytes).toBe(0);
    }
  },
);

it("rejects a foreign grant before moving any result charge", async () => {
  const governor = new Governor(8 << 20),
    foreign = await governor.admit({ label: "foreign", minimumBytes: 4096, spillable: true }),
    result = await governor.admit({ label: "result", minimumBytes: 4096, spillable: true }),
    owned = new ResultBuffers(foreign, 1024);
  try {
    const buffer = owned.bytes(1024).buffer;
    expect(takeResultBuffers(owned.transfer, result, new Set([buffer]))).toBeNull();
    expect(foreign.allocated).toBe(1024);
    expect(result.allocated).toBe(0);
  } finally {
    owned.close();
    foreign.close();
    result.close();
  }
  expect(governor.usedBytes).toBe(0);
});
