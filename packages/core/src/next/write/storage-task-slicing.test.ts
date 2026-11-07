/** R23/R25: cached dependencies do not replace task yields or cancellation checks. */
import { expect, it, vi } from "vitest";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { ExtentListKeeper, listedExtents } from "../storage/extent-list.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import type { ExtentSpace, OpenSpaceOptions } from "../storage/extents.js";
import { FreeSpaceMap } from "../storage/free-space.js";
import { TaskClock, sortInSlices } from "../task.js";

const added = () =>
  Array.from({ length: 100_000 }, (_, index) => ({ offset: 2 ** 30 + index * 4096, length: 1000 }));
const budget = () => countingBudget(Infinity);

it("services a queued timer during a large preparation with immediately resolved writes", async () => {
  const space = new MemoryExtentSpace();
  const keeper = new ExtentListKeeper([], { reader: space, budget: budget() });
  let serviced = false;
  const timer = setTimeout(() => {
    serviced = true;
  }, 0);
  try {
    await keeper.prepare(1, { added: added(), released: [], writer: space.beginWrite("large") });
    expect(serviced).toBe(true);
  } finally {
    clearTimeout(timer);
    keeper.close();
  }
});

it("cancels a large preparation after a task turn and returns its reservations", async () => {
  const space = new MemoryExtentSpace();
  const controller = new AbortController();
  const charged = budget();
  const keeper = new ExtentListKeeper([], {
    reader: space,
    budget: charged,
    signal: controller.signal,
  });
  const reason = new Error("cancel preparation");
  const timer = setTimeout(() => controller.abort(reason), 0);
  try {
    await expect(
      keeper.prepare(1, { added: added(), released: [], writer: space.beginWrite("cancel") }),
    ).rejects.toBe(reason);
    expect(charged.used).toBe(0);
    expect(keeper.list).toHaveLength(0);
  } finally {
    clearTimeout(timer);
    keeper.close();
  }
});

it("refuses an already cancelled preparation before writing", async () => {
  const controller = new AbortController();
  const reason = new Error("cancelled before prepare");
  controller.abort(reason);
  const space = new MemoryExtentSpace();
  const writer = space.beginWrite("already cancelled");
  const keeper = new ExtentListKeeper([], {
    reader: space,
    budget: budget(),
    signal: controller.signal,
  });
  try {
    await expect(keeper.prepare(1, { added: added(), released: [], writer })).rejects.toBe(reason);
    expect(writer.extents).toHaveLength(0);
  } finally {
    keeper.close();
  }
});

it("services a timer while draining cached list pages", async () => {
  const space = new MemoryExtentSpace();
  const keeper = new ExtentListKeeper([], { reader: space, budget: budget() });
  const prepared = await keeper.prepare(1, {
    added: added(),
    released: [],
    writer: space.beginWrite("seed"),
  });
  let serviced = false;
  const timer = setTimeout(() => {
    serviced = true;
  }, 0);
  try {
    let count = 0;
    for await (const extent of listedExtents(prepared.list, space, budget())) {
      void extent;
      count++;
    }
    expect(count).toBe(100_000 + prepared.list.length);
    expect(serviced).toBe(true);
  } finally {
    clearTimeout(timer);
    keeper.close();
  }
});

it("checks cancellation on resuming an immediately resolved list read", async () => {
  const space = new MemoryExtentSpace();
  const keeper = new ExtentListKeeper([], { reader: space, budget: budget() });
  const prepared = await keeper.prepare(1, {
    added: added().slice(0, 10),
    released: [],
    writer: space.beginWrite("seed"),
  });
  const controller = new AbortController();
  const reason = new Error("cancel cached read");
  const read = space.read.bind(space);
  space.read = async (refs, options) => {
    const lease = await read(refs, options);
    controller.abort(reason);
    return lease;
  };
  const charged = budget();
  const iterator = listedExtents(prepared.list, space, charged, { signal: controller.signal });
  try {
    await expect(iterator.next()).rejects.toBe(reason);
    expect(charged.used).toBe(0);
  } finally {
    await iterator.return(undefined);
    keeper.close();
  }
});

async function startup(
  signal?: AbortSignal,
  extra: Pick<OpenSpaceOptions, "clock" | "yieldTask"> = {},
  collected?: () => void,
): Promise<{ space: ExtentSpace; close(): Promise<void> }> {
  const backend = new MemoryBackend();
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  const count = 100_000;
  session.readRoot = async () => new Uint8Array([1]);
  session.readPreviousRoot = async () => new Uint8Array([2]);
  session.dataLength = async () => count * 8192;
  try {
    const space = await openExtentSpace(session, {
      reachable: function* (root) {
        for (let index = 0; index < count; index++)
          yield { offset: index * 8192 + (root[0] === 1 ? 0 : 4096), length: 1000 };
        if (root[0] === 2) collected?.();
      },
      ...extra,
      budget: budget(),
      registerCache: () => countingMemory(Infinity),
      ...(signal === undefined ? {} : { signal }),
    });
    return {
      space,
      async close() {
        await space.close();
        await session.close();
        await lease.release();
      },
    };
  } catch (error) {
    await session.close();
    await lease.release();
    throw error;
  }
}

it("slices startup collection, root union, sorting and free-map construction", async () => {
  let serviced = false;
  const timer = setTimeout(() => {
    serviced = true;
  }, 0);
  const opened = await startup();
  try {
    expect(serviced).toBe(true);
    expect(opened.space.stats.dataBytes).toBe(100_000 * 8192);
  } finally {
    clearTimeout(timer);
    await opened.close();
  }
});

it("cancels startup work after a task turn and preserves the original reason", async () => {
  const controller = new AbortController();
  const reason = new Error("cancel startup union");
  const timer = setTimeout(() => controller.abort(reason), 0);
  const outcome = await startup(controller.signal).then(
    (opened) => ({ opened, error: null }),
    (error: unknown) => ({ opened: null, error }),
  );
  try {
    expect(outcome.error).toBe(reason);
  } finally {
    clearTimeout(timer);
    await outcome.opened?.close();
  }
});

it("shares elapsed task time across preparations and resolved writes", async () => {
  let time = 0;
  let yields = 0;
  let serviced = false;
  const clock = new TaskClock(() => time);
  const space = new MemoryExtentSpace();
  const work = {
    reader: space,
    budget: budget(),
    clock,
    yieldTask: async () => {
      yields++;
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    },
  };
  const first = new ExtentListKeeper([], work);
  const second = new ExtentListKeeper([], work);
  const writer = (label: string) => {
    const out = space.beginWrite(label);
    const write = out.write.bind(out);
    out.write = (chunks) => {
      time += 3;
      return write(chunks);
    };
    return out;
  };
  const timer = setTimeout(() => {
    serviced = true;
  }, 0);
  try {
    const delta = [{ offset: 2 ** 30, length: 1000 }];
    await first.prepare(1, { added: delta, released: [], writer: writer("first") });
    expect(yields).toBe(0);
    await second.prepare(1, { added: delta, released: [], writer: writer("second") });
    expect(yields).toBe(1);
    expect(serviced).toBe(true);
  } finally {
    clearTimeout(timer);
    first.close();
    second.close();
  }
});

it("checks the union deadline before entering free-map construction", async () => {
  let active = false;
  let ticks = 0;
  let enteredMap = false;
  const clock = new TaskClock(() => (active ? ticks++ : 0));
  const controller = new AbortController();
  const reason = new Error("cancel root union");
  let timer: ReturnType<typeof setTimeout> | undefined;
  const original = FreeSpaceMap.fromAllocatedAsync.bind(FreeSpaceMap);
  const spy = vi.spyOn(FreeSpaceMap, "fromAllocatedAsync").mockImplementation((...args) => {
    enteredMap = true;
    return original(...args);
  });
  const outcome = await startup(
    controller.signal,
    { clock, yieldTask: () => new Promise<void>((resolve) => setTimeout(resolve, 0)) },
    () => {
      active = true;
      timer = setTimeout(() => controller.abort(reason), 0);
    },
  ).then(
    (opened) => ({ opened, error: null }),
    (error: unknown) => ({ opened: null, error }),
  );
  try {
    expect(outcome.error).toBe(reason);
    expect(enteredMap).toBe(false);
  } finally {
    clearTimeout(timer);
    spy.mockRestore();
    await outcome.opened?.close();
  }
});

it("cancels free-map input construction before consuming the whole iterable", async () => {
  let time = 0;
  let consumed = false;
  const clock = new TaskClock(() => time);
  const controller = new AbortController();
  const reason = new Error("cancel free map");
  function* input() {
    for (let index = 0; index < 100_000; index++)
      yield {
        offset: index * 8192,
        get length() {
          time += 0.001;
          return 1000;
        },
      };
    consumed = true;
  }
  const timer = setTimeout(() => controller.abort(reason), 0);
  try {
    await expect(
      FreeSpaceMap.fromAllocatedAsync(4096, 100_000 * 8192, input(), {
        clock,
        signal: controller.signal,
        yieldTask: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
      }),
    ).rejects.toBe(reason);
    expect(consumed).toBe(false);
  } finally {
    clearTimeout(timer);
  }
});

it("sorts in bounded tasks and preserves cancellation reasons", async () => {
  const values = Array.from({ length: 100_000 }, (_, index) => 100_000 - index);
  let serviced = false;
  const timer = setTimeout(() => {
    serviced = true;
  }, 0);
  try {
    await sortInSlices(values, (a, b) => a - b);
    expect(serviced).toBe(true);
    expect(values[0]).toBe(1);
    expect(values.at(-1)).toBe(100_000);
  } finally {
    clearTimeout(timer);
  }
  const controller = new AbortController();
  const reason = new RangeError("cancel sort");
  controller.abort(reason);
  await expect(sortInSlices(values, (a, b) => a - b, { signal: controller.signal })).rejects.toBe(
    reason,
  );
});

it("preserves free-map overlap refusal and duplicate handling", async () => {
  const map = await FreeSpaceMap.fromAllocatedAsync(4096, 32768, [
    { offset: 16384, length: 1 },
    { offset: 0, length: 1 },
    { offset: 16384, length: 1 },
  ]);
  expect(map.end).toBe(20480);
  expect(map.runs()).toEqual([{ offset: 4096, length: 12288 }]);
  await expect(
    FreeSpaceMap.fromAllocatedAsync(4096, 16384, [
      { offset: 0, length: 8192 },
      { offset: 4096, length: 1 },
    ]),
  ).rejects.toBeInstanceOf(RangeError);
});

for (const field of ["added", "released"] as const)
  it(`yields and cancels during ${field} input copying`, async () => {
    let time = 0;
    let copied = 0;
    const input = new Proxy(added(), {
      get(target, key, receiver) {
        if (typeof key === "string" && /^\d+$/u.test(key)) {
          copied++;
          time += 0.01;
        }
        return Reflect.get(target, key, receiver) as unknown;
      },
    });
    const space = new MemoryExtentSpace();
    const charged = countingBudget(8 << 20);
    const controller = new AbortController();
    const reason = new Error("cancel during input copy");
    const keeper = new ExtentListKeeper([], {
      reader: space,
      budget: charged,
      clock: new TaskClock(() => time),
      signal: controller.signal,
      yieldTask: async () => {
        expect(copied).toBeGreaterThan(0);
        expect(copied).toBeLessThanOrEqual(512);
        expect(charged.used).toBeGreaterThan(0);
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        controller.abort(reason);
      },
    });
    try {
      await expect(
        keeper.prepare(1, {
          added: field === "added" ? input : [],
          released: field === "released" ? input : [],
          writer: space.beginWrite("copy cancel"),
        }),
      ).rejects.toBe(reason);
      expect(charged.used).toBe(0);
    } finally {
      keeper.close();
    }
  });
