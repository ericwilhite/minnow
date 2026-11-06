/** Real reuse, not poison: a suspended inline part survives page release and other loads. */
import fc from "fast-check";
import { afterAll, describe, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";

vi.stubEnv("MINNOW_VERIFY_CACHED_BYTES", "0");
vi.resetModules();
const { MemoryBackend } = await import("../storage/memory-store.js");
const { countingMemory, openOver } = await import("../storage/space-test-support.js");
const { VERIFY_CACHED_BYTES } = await import("../storage/verify-cached.js");
const { ChunkReader } = await import("./reader.js");
const { defined, vectorOf } = await import("./test-support.js");
const { GroupWriter } = await import("./writer.js");

async function fixture(width = 80) {
  const { space, budget } = await openOver(new MemoryBackend(), {
    memory: countingMemory(Infinity),
  });
  const type = { kind: "bytes" } as const;
  const values = Array.from({ length: 200 }, (_, row) =>
    Uint8Array.from({ length: width }, (_, k) => (row * 17 + k) % 251),
  );
  const writer = new GroupWriter({
    columns: [{ columnId: 1, type }],
    writer: space.beginWrite("inline stream"),
    inlineBytes: 0,
    pageBytes: 1024,
    rawPageBytes: 1024,
  });
  await writer.append([vectorOf(type, values)], values.length);
  return { space, budget, values, chunk: defined((await writer.finish()).columns[0]) };
}

const reach = createReach("next-inline-stream-pool", [
  "release and load while suspended",
  "multiple replacement loads",
  "advance",
  "return",
  "close",
  "abort",
  "peeked page copied",
  "leased page retained",
]);
afterAll(() => {
  reach.assertReached();
  vi.unstubAllEnvs();
});

describe("inline stream parts over pooled pages", () => {
  it("preserves the reviewer's 80-byte part while page 1 reuses page 0's buffer", async () => {
    expect(VERIFY_CACHED_BYTES).toBe(false);
    const f = await fixture();
    const reader = await ChunkReader.open(f.space, f.chunk, { budget: f.budget, once: true });
    try {
      await reader.load([0]);
      const stream = reader.valueParts(0);
      const before = f.budget.used;
      const first = await stream.next();
      const additionalCharge = f.budget.used - before;
      expect(first.done).toBe(false);
      if (first.done) throw new Error("no inline part");
      reader.release([0]);
      await reader.load([1]);
      expect(first.value).toEqual(f.values[0]);
      expect(additionalCharge).toBe(0);
      expect(first.value.buffer.byteLength).toBe(65536);
      await stream.return(undefined);
    } finally {
      reader.close();
      await f.space.close();
    }
    expect(f.budget.used).toBe(0);
  });

  it("retains separate shares for two suspended streams on the same page", async () => {
    const f = await fixture();
    const reader = await ChunkReader.open(f.space, f.chunk, { budget: f.budget, once: true });
    try {
      await reader.load([0]);
      const a = reader.valueParts(0),
        b = reader.valueParts(1);
      const first = await a.next(),
        second = await b.next();
      reader.release([0]);
      await reader.load([1]);
      expect(first.value).toEqual(f.values[0]);
      expect(second.value).toEqual(f.values[1]);
      await a.return(undefined);
      reader.release([1]);
      await reader.load([2]);
      expect(second.value).toEqual(f.values[1]);
      reader.close();
      await expect(b.next()).rejects.toThrow(/closed/u);
    } finally {
      reader.close();
      await f.space.close();
    }
    expect(f.budget.used).toBe(0);
  });

  it("refuses an inline copy before allocating it and releases nothing twice", async () => {
    const f = await fixture();
    // A peek has no retainable lease: it must make a charged value-sized copy.
    const warmer = await ChunkReader.open(f.space, f.chunk, { budget: f.budget });
    await warmer.load([0]);
    warmer.close();
    let deny = false;
    const reader = await ChunkReader.open(f.space, f.chunk, {
      budget: {
        allocate: (bytes) => !deny && f.budget.allocate(bytes),
        free: (bytes) => f.budget.free(bytes),
      },
      once: true,
    });
    try {
      await reader.load([0]);
      const before = f.budget.used;
      deny = true;
      await expect(reader.valueParts(0).next()).rejects.toThrow(/budget|bytes/u);
      expect(f.budget.used).toBe(before);
    } finally {
      reader.close();
      await f.space.close();
    }
    expect(f.budget.used).toBe(0);
  });

  describe.each(seedsFor("next-inline-stream-pool", [0x1a11, 0x5eed]))("seed %i", (seed) => {
    it("keeps suspended parts intact through generated release/load interleavings", async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 16, max: 500 }),
          fc.array(fc.nat(1000), { minLength: 1, maxLength: 8 }),
          fc.constantFrom("advance", "return", "close", "abort"),
          fc.boolean(),
          async (width, replacements, ending, peek) => {
            reach.beginHistory();
            const f = await fixture(width);
            if (peek) {
              const warmer = await ChunkReader.open(f.space, f.chunk, { budget: f.budget });
              await warmer.load([0]);
              warmer.close();
            }
            const abort = new AbortController();
            const reader = await ChunkReader.open(f.space, f.chunk, {
              budget: f.budget,
              once: true,
              signal: abort.signal,
            });
            try {
              await reader.load([0]);
              const stream = reader.valueParts(0);
              const before = f.budget.used;
              const first = await stream.next();
              const additional = f.budget.used - before;
              expect(first.done).toBe(false);
              reader.release([0]);
              for (const n of replacements) {
                const page = 1 + (n % (reader.directory.pages.length - 1));
                await reader.load([page]);
                expect(first.value).toEqual(f.values[0]);
                reader.release([page]);
              }
              expect(additional).toBe(peek ? width : 0);
              reach.reach(peek ? "peeked page copied" : "leased page retained");
              reach.reach("release and load while suspended");
              if (replacements.length > 1) reach.reach("multiple replacement loads");
              reach.reach(ending);
              if (ending === "close") {
                reader.close();
                await expect(stream.next()).rejects.toThrow(/closed/u);
              } else if (ending === "abort") {
                abort.abort();
                await expect(stream.next()).rejects.toThrow();
              } else if (ending === "return") await stream.return(undefined);
              else expect((await stream.next()).done).toBe(true);
            } finally {
              reader.close();
              await f.space.close();
            }
            expect(f.budget.used).toBe(0);
          },
        ),
        { seed, numRuns: 60 },
      );
    }, 120_000);
  });
});
