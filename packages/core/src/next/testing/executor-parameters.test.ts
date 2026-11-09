/** Execution-owned parameter storage is charged before materialization, and dies with execution. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { bindParameter, prepareParameters } from "../exec/parameters.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";

import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import { createExecutor } from "../exec/create.js";
import type { Execution, ParameterValue, PreparedPlan } from "../exec/executor.js";
import type { ChunkReads } from "../exec/scan.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { decodeUtf8 } from "../vector/varlen.js";
import { ParameterBindError } from "../exec/errors.js";
import { MemoryAdmissionError } from "../memory/governor.js";
import { vi } from "vitest";
import { yieldTask } from "../exec/pipeline.js";

function preparedInput(type: PhysicalType, count = 1, zero = false): PreparedPlan {
  const output = [{ name: "v", type, nullable: true }];
  const input: Rel = {
    id: 1,
    op: "values",
    output,
    rows: Array.from({ length: count }, () => [{ op: "param", slot: 0, type }]),
  };
  const root: Rel = zero
    ? { id: 2, op: "limit", output, input, limit: { kind: "const", value: 0 }, offset: null }
    : input;
  const plan: PhysicalPlan = {
    root,
    columns: output,
    parameters: [type],
    placeholderCount: 1,
    clockSlot: null,
    initPlans: [],
    catalogVersion: 0,
    volatile: false,
  };
  return createExecutor().prepare(plan);
}
let nextQueryId = 0;
function start(
  prepared: PreparedPlan,
  values: readonly ParameterValue[],
  governor: Governor,
  windowBytes = 64,
  signal?: AbortSignal,
) {
  const spill = new MemorySpillService();
  const execution = prepared.execute({
    parameters: values,
    snapshot: { catalog: { version: 0 }, observedLsn: 0 } as unknown as Snapshot,
    statement: null,
    statementTime: 0n,
    reads: {} as ChunkReads,
    governor,
    spill,
    queryId: ++nextQueryId,
    sequences: null,
    sequenceSession: null,
    returning: "atExecuted",
    resultWindowBytes: windowBytes,
    resultFrameRows: 1,
    ...(signal === undefined ? {} : { signal }),
  });
  return { execution, spill };
}
async function drain(execution: Execution) {
  const values: string[] = [];
  for (;;) {
    const frame = await execution.results.next();
    if (frame === null) break;
    try {
      const vector = frame.payload.columns[0];
      if (vector?.form !== "flat" || vector.data.layout !== "varlen")
        throw new Error("expected text result");
      for (let row = 0; row < frame.payload.rowCount; row++)
        values.push(decodeUtf8(vector.data, row));
    } finally {
      frame.release();
    }
  }
  await execution.executed;
  await execution.delivered;
  return values;
}

it.each(["text", "bytes"] as const)(
  "refuses an over-cap %s parameter before creating its owned copy, even with LIMIT 0",
  async (kind) => {
    const governor = new Governor(8 << 20),
      prepared = preparedInput({ kind }, 1, true);
    const value = kind === "text" ? "x".repeat(9 << 20) : new Uint8Array(9 << 20);
    const encoder = vi.spyOn(TextEncoder.prototype, "encode");
    const { execution, spill } = start(prepared, [value], governor);
    try {
      await expect(execution.executed).rejects.toBeInstanceOf(MemoryAdmissionError);
      await expect(execution.delivered).rejects.toMatchObject({ code: "MINNOW_MEMORY_ADMISSION" });
      expect(encoder).not.toHaveBeenCalled();
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
      expect(() =>
        governor.tryAdmit({ label: "other", minimumBytes: 9 << 20, spillable: false }),
      ).toThrow(MemoryAdmissionError);
    } finally {
      encoder.mockRestore();
      execution.cancel();
      await Promise.allSettled([execution.executed, execution.delivered]);
    }
  },
);

it.each(seedsFor("next-executor-parameter-lifecycle", [0xc803, 0xc804]))(
  "releases charged input buffers on completion, cancellation and binding error (seed %i)",
  async (seed) => {
    const reach = createReach("parameter lifecycle", [
      "success",
      "cancel",
      "error",
      "zero",
      "unicode",
      "concurrent",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("success", "cancel", "error", "zero"),
        fc.integer({ min: 128, max: 2048 }),
        fc.boolean(),
        async (mode, size, unicode) => {
          reach.beginHistory();
          const governor = new Governor(8 << 20),
            prepared = preparedInput({ kind: "text" }, 12, mode === "zero");
          const a = (unicode ? "🦦é" : "a").repeat(size),
            b = "b".repeat(size);
          const left = start(prepared, [mode === "error" ? 123 : a], governor);
          const right = start(prepared, [b], governor);
          try {
            if (mode === "error") {
              await expect(left.execution.executed).rejects.toBeInstanceOf(ParameterBindError);
              await expect(left.execution.delivered).rejects.toBeInstanceOf(ParameterBindError);
            } else if (mode === "cancel") {
              const frame = await left.execution.results.next();
              expect(frame).not.toBeNull();
              expect(left.execution.state).toBe("running");
              left.execution.cancel("generated cancellation");
              frame?.release();
              await expect(left.execution.executed).rejects.toMatchObject({
                code: "MINNOW_QUERY_CANCELLED",
              });
              await expect(left.execution.delivered).rejects.toMatchObject({
                code: "MINNOW_QUERY_CANCELLED",
              });
            } else
              expect(await drain(left.execution)).toEqual(
                mode === "zero" ? [] : Array<string>(12).fill(a),
              );
            expect(await drain(right.execution)).toEqual(
              mode === "zero" ? [] : Array<string>(12).fill(b),
            );
            expect(governor.usedBytes).toBe(0);
            expect(left.spill.bytesInUse + right.spill.bytesInUse).toBe(0);
            reach.reach(mode);
            reach.reach("concurrent");
            if (unicode) reach.reach("unicode");
          } finally {
            left.execution.cancel();
            right.execution.cancel();
            await Promise.allSettled([
              left.execution.executed,
              left.execution.delivered,
              right.execution.executed,
              right.execution.delivered,
            ]);
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ["success", 128, true],
          ["cancel", 128, false],
          ["error", 128, false],
          ["zero", 128, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it("asks the grant before UTF-8/byte copies or numeric coefficient construction", () => {
  const encoder = vi.spyOn(TextEncoder.prototype, "encode");
  const copy = vi.spyOn(Uint8Array.prototype, "slice");
  const integer = vi.spyOn(globalThis, "BigInt");
  const refused = { allocate: () => false, free: () => undefined };
  try {
    expect(() => bindParameter({ kind: "text" }, "🦦".repeat(100), 0, refused)).toThrow(
      MemoryAdmissionError,
    );
    expect(() => bindParameter({ kind: "bytes" }, new Uint8Array(100), 0, refused)).toThrow(
      MemoryAdmissionError,
    );
    expect(() =>
      bindParameter(
        { kind: "decimal", precision: null, scale: null },
        "123456789012345678901234567890",
        0,
        refused,
      ),
    ).toThrow(MemoryAdmissionError);
    expect(encoder).not.toHaveBeenCalled();
    expect(copy).not.toHaveBeenCalled();
    expect(integer).not.toHaveBeenCalled();
  } finally {
    encoder.mockRestore();
    copy.mockRestore();
    integer.mockRestore();
  }
});

it("does not retain execution parameter bytes after executed while frames still await credit", async () => {
  const governor = new Governor(8 << 20),
    prepared = preparedInput({ kind: "text" });
  const admit = governor.tryAdmit.bind(governor);
  const inputs: Grant[] = [];
  vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
    const grant = admit(options);
    if (grant !== null && options.label.endsWith("fixed-buffers")) inputs.push(grant);
    return grant;
  });
  const { execution } = start(prepared, ["p".repeat(128)], governor, 1024);
  const frame = await execution.results.next();
  try {
    expect(frame).not.toBeNull();
    await execution.executed;
    expect(inputs).toHaveLength(1);
    // Whole-minimum admission now includes the result account. Its shared parent can
    // retain frame bytes, but none of the parameter allocation may survive executed.
    const binding = await prepareParameters(prepared.plan, ["p".repeat(128)]);
    expect(inputs[0]?.allocated).toBeLessThan(binding.bytes);
    expect(inputs[0]?.held).toBeLessThanOrEqual(1 << 20);
    expect(governor.usedBytes).toBeGreaterThan(0);
  } finally {
    frame?.release();
    execution.cancel();
    await Promise.allSettled([execution.executed, execution.delivered]);
  }
  expect(governor.usedBytes).toBe(0);
});

it("cancels a queued admission before encoding caller inputs", async () => {
  const governor = new Governor(8 << 20),
    prepared = preparedInput({ kind: "text" });
  const blocker = await governor.admit({
    label: "blocker",
    minimumBytes: governor.capBytes - governor.reserveBytes,
    spillable: false,
  });
  const encoder = vi.spyOn(TextEncoder.prototype, "encode");
  const { execution } = start(prepared, ["x".repeat(1024)], governor);
  try {
    expect(execution.state).toBe("running");
    execution.cancel("queued input");
    await expect(execution.executed).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED" });
    await expect(execution.delivered).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED" });
    expect(encoder).not.toHaveBeenCalled();
    expect(governor.usedBytes).toBe(blocker.held);
  } finally {
    encoder.mockRestore();
    blocker.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it.each(seedsFor("next-executor-parameter-cap", [0xc805, 0xc806]))(
  "admits or refuses parameter storage under a generated cap boundary (seed %i)",
  async (seed) => {
    const reach = createReach("parameter capacity", [
      "fits",
      "oversize",
      "text",
      "bytes",
      "utf8-over-cap",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("text", "bytes"),
        fc.constantFrom("fits", "oversize", "utf8"),
        fc.integer({ min: 0, max: 1024 }),
        async (kind, regime, delta) => {
          reach.beginHistory();
          const governor = new Governor(8 << 20),
            prepared = preparedInput(
              kind === "text" ? { kind: "text" } : { kind: "bytes" },
              1,
              true,
            );
          const count = (regime === "fits" ? 16384 : regime === "utf8" ? 4 << 20 : 9 << 20) + delta;
          const source =
            kind === "text" ? (regime === "utf8" ? "é" : "x").repeat(count) : new Uint8Array(count);
          const encodedBytes = typeof source === "string" && regime === "utf8" ? count * 2 : count;
          const refused = encodedBytes > governor.capBytes - governor.reserveBytes;
          const { execution, spill } = start(prepared, [source], governor);
          try {
            if (refused) {
              await expect(execution.executed).rejects.toBeInstanceOf(MemoryAdmissionError);
              await expect(execution.delivered).rejects.toBeInstanceOf(MemoryAdmissionError);
              reach.reach("oversize");
            } else {
              expect(await drain(execution)).toEqual([]);
              reach.reach("fits");
            }
            reach.reach(kind);
            if (kind === "text" && regime === "utf8") reach.reach("utf8-over-cap");
            expect(governor.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
          } finally {
            execution.cancel();
            await Promise.allSettled([execution.executed, execution.delivered]);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["text", "fits", 0],
          ["bytes", "oversize", 0],
          ["text", "utf8", 0],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it("queues concurrent large bindings atomically instead of pinning inputs ahead of operator admission", async () => {
  const source = preparedInput({ kind: "text" }, 2).plan;
  const prepared = createExecutor().prepare({
    ...source,
    parameters: Array<PhysicalType>(8).fill({ kind: "text" }),
    placeholderCount: 8,
  });
  const governor = new Governor(8 << 20);
  const admit = governor.tryAdmit.bind(governor);
  vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
    const grant = admit(options);
    if (grant !== null && options.label.endsWith("/result")) {
      const release = grant.release.bind(grant);
      // A bounded sentinel turns the old synchronous reclaim spin into an assertion failure,
      // rather than relying on a timeout that the spinning event loop cannot even service.
      grant.release = (bytes) => {
        if (bytes === 0) throw new Error("result reclaim made no progress");
        release(bytes);
      };
    }
    return grant;
  });
  const a = "a".repeat(512 << 10),
    b = "b".repeat(512 << 10);
  const left = start(prepared, Array<string>(8).fill(a), governor),
    right = start(prepared, Array<string>(8).fill(b), governor);
  try {
    // Sizing can admit either execution first. An unread first result may keep the
    // other admission queued, so both consumers must return their frame credit.
    let consumers = 0;
    const consume = async (execution: Execution) => {
      consumers++;
      await yieldTask();
      // Reject a serial-consumption mutant without waiting for a deadlocked test timeout.
      expect(consumers, "both concurrent result consumers must start").toBe(2);
      return drain(execution);
    };
    const [leftRows, rightRows] = await Promise.all([
      consume(left.execution),
      consume(right.execution),
    ]);
    expect(leftRows).toEqual([a, a]);
    expect(rightRows).toEqual([b, b]);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
    expect(governor.usedBytes).toBe(0);
  } finally {
    left.execution.cancel();
    right.execution.cancel();
    await Promise.allSettled([
      left.execution.executed,
      left.execution.delivered,
      right.execution.executed,
      right.execution.delivered,
    ]);
  }
}, 120000);

it("charges bounded wide-NUMERIC sizing probes as well as adopting immutable coefficients", async () => {
  const governor = new Governor(8 << 20);
  const grant = await governor.admit({
    label: "numeric-sizing",
    minimumBytes: 1 << 20,
    spillable: false,
  });
  const original = BigInt.asIntN.bind(BigInt);
  const probe = vi.spyOn(BigInt, "asIntN").mockImplementation((bits, value) => {
    expect(grant.allocated).toBeGreaterThan(0);
    return original(bits, value);
  });
  try {
    const value = (1n << 200n) + 17n;
    const vector = bindParameter(
      { kind: "decimal", precision: null, scale: null },
      value,
      0,
      grant,
    );
    expect(vector.data.layout).toBe("decimal");
    if (vector.data.layout === "decimal" && vector.data.coefficients.repr === "wide")
      expect(vector.data.coefficients.values[0]).toBe(value);
    else throw new Error("expected wide coefficient");
    expect(probe).toHaveBeenCalled();
    expect(grant.allocated).toBeGreaterThan(0);
    expect(grant.allocated).toBeLessThan(512 << 10); // The temporary sizing charge was returned.
  } finally {
    probe.mockRestore();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("preserves UTF-8 across copy chunks and copies mutable bytes in explicit charged binding", async () => {
  const governor = new Governor(8 << 20),
    grant = await governor.admit({
      label: "explicit-binding",
      minimumBytes: 1 << 20,
      spillable: false,
    });
  try {
    const prepared = preparedInput({ kind: "text" });
    for (const source of [
      "a".repeat(8191) + "🦦é",
      "a".repeat(8192) + "\ud800",
      "\udc00" + "a".repeat(8190) + "🦦",
    ]) {
      const bound = await prepared.bind([source], grant);
      const vector = bound.slots[0];
      if (vector?.data.layout !== "varlen") throw new Error("expected text");
      expect(vector.data.bytes).toEqual(new TextEncoder().encode(source));
    }
    const source = Uint8Array.of(1, 2, 3);
    const bytes = await preparedInput({ kind: "bytes" }).bind([source], grant);
    source.fill(0);
    expect(bytes.slots[0]?.data).toMatchObject({ layout: "varlen", bytes: Uint8Array.of(1, 2, 3) });
  } finally {
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("cancels while the first UTF-8 chunk is copied and frees the admitted partial binding", async () => {
  const governor = new Governor(8 << 20),
    prepared = preparedInput({ kind: "text" }, 1, true),
    abort = new AbortController();
  const encode = TextEncoder.prototype.encodeInto.bind(new TextEncoder());
  const copy = vi.spyOn(TextEncoder.prototype, "encodeInto").mockImplementation(function (
    this: TextEncoder,
    source,
    destination,
  ) {
    const result = encode.call(this, source, destination);
    abort.abort("during parameter copy");
    return result;
  });
  const { execution, spill } = start(prepared, ["x".repeat(256 << 10)], governor, 64, abort.signal);
  try {
    await expect(execution.executed).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED" });
    await expect(execution.delivered).rejects.toMatchObject({ code: "MINNOW_QUERY_CANCELLED" });
    expect(copy).toHaveBeenCalledTimes(1);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  } finally {
    copy.mockRestore();
    execution.cancel();
    await Promise.allSettled([execution.executed, execution.delivered]);
  }
});
