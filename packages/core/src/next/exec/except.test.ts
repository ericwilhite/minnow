import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ExceptOperator, exceptMinimumBytes, exceptKeyLayout } from "./except.js";
import { INT, TEXT, ArraySource, CollectSink, batchesOf, type Row } from "./join-fixtures.js";
import { drive } from "./pipeline.js";

it.each([false, true])("EXCEPT all=%s counts duplicates and NULLs", async (all) => {
  const types = [INT, TEXT];
  const governor = new Governor(32 << 20),
    spill = new MemorySpillService();
  const grant = await governor.admit({
    label: "except",
    minimumBytes: exceptMinimumBytes(types),
    spillable: true,
  });
  const sink = new CollectSink(2);
  const op = new ExceptOperator(types, all, { grant, spill, queryId: 1 }, sink);
  const left: Row[] = [
    [1, "a"],
    [1, "a"],
    [null, "x"],
    [null, "x"],
    [2, null],
    [3, "c"],
  ];
  const right: Row[] = [
    [1, "a"],
    [null, "x"],
    [2, null],
    [4, "d"],
  ];
  try {
    await drive(new ArraySource(batchesOf(types, left, { batchRows: 2 })), op.left);
    await drive(new ArraySource(batchesOf(types, right, { batchRows: 3 })), op.right);
    await op.closed;
    expect(sink.rows.sort()).toEqual(
      (all
        ? [
            [1, "a"],
            [null, "x"],
            [3, "c"],
          ]
        : [[3, "c"]]
      ).sort(),
    );
    expect(spill.bytesInUse).toBe(0);
  } finally {
    op.abort(new Error("test done"));
    await op.closed;
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("spills many distinct groups under its admitted cap and cleans every tape", async () => {
  const types = [INT, TEXT];
  const minimum = exceptMinimumBytes(types);
  const governor = new Governor(Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7)));
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "except", minimumBytes: minimum, spillable: true });
  const sink = new CollectSink(11);
  const op = new ExceptOperator(types, true, { grant, spill, queryId: 2 }, sink);
  const rows: Row[] = Array.from({ length: 6000 }, (_, i) => [
    i,
    `${String(i)}:${"x".repeat(256)}`,
  ]);
  try {
    await drive(new ArraySource(batchesOf(types, rows, { batchRows: 127 })), op.left);
    await drive(
      new ArraySource(batchesOf(types, rows.slice(0, 3000), { batchRows: 511 })),
      op.right,
    );
    await op.closed;
    expect(sink.rows.map((row) => row[0]).sort((a, b) => Number(a) - Number(b))).toEqual(
      Array.from({ length: 3000 }, (_, i) => i + 3000),
    );
    expect(op.profile.spilledBytes).toBeGreaterThan(0);
    expect(op.profile.spillPasses).toBeGreaterThan(0);
  } finally {
    op.abort(new Error("test done"));
    await op.closed;
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
}, 120_000);

it("handles repeated oversized keys without retaining one step's loan into the next", async () => {
  const types = [TEXT];
  const minimum = exceptMinimumBytes(types);
  const governor = new Governor(Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7)));
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "except", minimumBytes: minimum, spillable: true });
  const sink = new CollectSink(1);
  const op = new ExceptOperator(types, true, { grant, spill, queryId: 3 }, sink);
  const a = "a".repeat(700000),
    b = "b".repeat(900000);
  try {
    await drive(new ArraySource(batchesOf(types, [[a], [a], [b], [b]], { batchRows: 1 })), op.left);
    await drive(new ArraySource(batchesOf(types, [[a], [b]], { batchRows: 1 })), op.right);
    await op.closed;
    expect(sink.rows.map((row) => String(row[0]).length).sort()).toEqual([700000, 900000]);
  } finally {
    op.abort(new Error("test done"));
    await op.closed;
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
}, 120_000);

it("peels colliding oversized partitions with bounded state rather than refusing", async () => {
  const types = [TEXT],
    minimum = exceptMinimumBytes(types);
  const governor = new Governor(Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7)));
  const spill = new MemorySpillService();
  const grant = await governor.admit({
    label: "except collision",
    minimumBytes: minimum,
    spillable: true,
  });
  const sink = new CollectSink(7);
  const op = new ExceptOperator(types, true, { grant, spill, queryId: 4, hashMask: 0 }, sink);
  const left: Row[] = Array.from({ length: 40 }, (_, i) => [`${String(i)}:${"x".repeat(40000)}`]);
  try {
    await drive(new ArraySource(batchesOf(types, [...left, ...left], { batchRows: 3 })), op.left);
    await drive(new ArraySource(batchesOf(types, left.slice(0, 20), { batchRows: 5 })), op.right);
    await op.closed;
    expect(sink.rows.map((row) => String(row[0]).split(":")[0]).sort()).toEqual(
      [...left.slice(0, 20), ...left.slice(20), ...left.slice(20)]
        .map((row) => String(row[0]).split(":")[0])
        .sort(),
    );
    expect(op.profile.collisionPasses).toBeGreaterThan(0);
  } finally {
    op.abort(new Error("test done"));
    await op.closed;
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
}, 120_000);

it("waits for a cancelled append's borrow before cleanup and publishes no directory afterward", async () => {
  const types = [TEXT],
    minimum = exceptMinimumBytes(types);
  const governor = new Governor(Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7)));
  const memory = new MemorySpillService();
  let entered!: () => void, resume!: () => void;
  const begun = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const blocked = new Promise<void>((resolve) => {
    resume = resolve;
  });
  let calls = 0;
  const spill = {
    get bytesInUse() {
      return memory.bytesInUse;
    },
    sweep: () => memory.sweep(),
    create: async (...args: Parameters<MemorySpillService["create"]>) => {
      const file = await memory.create(...args);
      return {
        queryId: file.queryId,
        get length() {
          return file.length;
        },
        read: file.read.bind(file),
        readInto: file.readInto.bind(file),
        close: file.close.bind(file),
        append: async (page: Uint8Array) => {
          calls++;
          if (calls === 1) {
            entered();
            await blocked;
          }
          return file.append(page);
        },
      };
    },
  };
  const grant = await governor.admit({
    label: "except cancelled",
    minimumBytes: minimum,
    spillable: true,
  });
  const sink = new CollectSink();
  const op = new ExceptOperator(types, true, { grant, spill, queryId: 5 }, sink);
  const rows: Row[] = Array.from({ length: 4000 }, (_, i) => [`${String(i)}:${"x".repeat(512)}`]);
  const running = drive(new ArraySource(batchesOf(types, rows, { batchRows: 127 })), op.left);
  const reason = new Error("cancel blocked append");
  try {
    await begun;
    op.abort(reason);
    let closed = false;
    void op.closed.then(() => {
      closed = true;
    });
    await Promise.resolve();
    expect(closed).toBe(false);
    resume();
    await expect(running).rejects.toBe(reason);
    await op.closed;
    expect(calls).toBe(1);
    expect(sink.rows).toEqual([]);
  } finally {
    resume();
    op.abort(reason);
    await Promise.resolve(running).catch(() => undefined);
    await op.closed;
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
}, 120_000);

it("cancels output backpressure without leaving a retained representative", async () => {
  const types = [INT],
    minimum = exceptMinimumBytes(types),
    governor = new Governor(32 << 20),
    spill = new MemorySpillService();
  const grant = await governor.admit({
    label: "except output",
    minimumBytes: minimum,
    spillable: true,
  });
  let entered!: () => void, reject!: (reason: unknown) => void;
  const begun = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const blocked = new Promise<void>((_, no) => {
    reject = no;
  });
  const sink = {
    push: () => {
      entered();
      return "blocked" as const;
    },
    ready: () => blocked,
    finish: () => undefined,
    abort: (reason: unknown) => reject(reason),
  };
  const op = new ExceptOperator(types, true, { grant, spill, queryId: 6 }, sink);
  await drive(new ArraySource(batchesOf(types, [[1], [1], [1]])), op.left);
  const running = drive(new ArraySource(batchesOf(types, [])), op.right),
    reason = new Error("cancel output");
  try {
    await begun;
    op.abort(reason);
    await expect(running).rejects.toBe(reason);
    await op.closed;
  } finally {
    op.abort(reason);
    await Promise.resolve(running).catch(() => undefined);
    await op.closed;
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
});

it("keeps unsupported equality types as explicit IR capability refusals", () => {
  for (const types of [
    [],
    Array.from({ length: 32 }, () => INT),
    [{ kind: "array" as const, element: INT }],
    [{ kind: "json" as const, binary: true }],
    [{ kind: "locator" as const, tableId: 1 }],
    [{ kind: "text" as const, fixed: true as const, maxLength: 3 }],
  ])
    expect(() => exceptKeyLayout(types)).toThrow(
      expect.objectContaining({
        name: "UnsupportedPlanError",
        sqlState: "0A000",
        construct: "setOp",
      }),
    );
});
