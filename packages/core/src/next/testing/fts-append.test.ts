import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_APPEND_REACH } from "../../testing/reach.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { ftsSemantics } from "../fts-config.js";
import {
  writeFtsInsertDelta,
  FTS_APPEND_WORKSPACE_BYTES,
  FTS_APPEND_MINIMUM_BYTES,
  FTS_APPEND_RESULT_BYTES,
} from "../format/fts-append.js";
import { writeFtsNode, walkFtsTree } from "../format/fts-storage.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import type { FtsInsertDocument, FtsInsertPosting } from "../format/fts-append.js";
async function* stream<T>(values: Iterable<T>): AsyncGenerator<T> {
  yield* values;
}
function account(limit = FTS_APPEND_MINIMUM_BYTES) {
  return {
    held: 0,
    peak: 0,
    allocate(n: number) {
      if (this.held + n > limit) return false;
      this.held += n;
      this.peak = Math.max(this.peak, this.held);
      return true;
    },
    free(n: number) {
      this.held -= n;
      expect(this.held).toBeGreaterThanOrEqual(0);
    },
  };
}
for (const seed of seedsFor("next-fts-append", [0xb831, 0xb832]))
  it(`streams exact insert-delta pages seed ${String(seed)}`, async () => {
    const reach = createReach("FTS append", FTS_APPEND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 65 }),
        fc.integer({ min: 1, max: 4096 }),
        async (n, frequency) => {
          reach.beginHistory();
          const docs: FtsInsertDocument[] = Array.from({ length: n }, (_, i) => ({
            id: 2 ** 52 + i * 2 + 1,
            length: i % 3 === 0 ? 0 : frequency,
            renderable: i % 3 !== 0 || i % 2 === 0,
          }));
          const posts: FtsInsertPosting[] = docs
            .filter((d) => d.length)
            .map((d) => ({ term: "a".repeat(31) + "\ud801", id: d.id, frequency: d.length }));
          const space = new MemoryExtentSpace(),
            writer = space.beginWrite("FTS append"),
            b = account();
          try {
            const delta = await writeFtsInsertDelta(stream(docs), stream(posts), b, writer);
            expect(b.held).toBe(FTS_APPEND_RESULT_BYTES);
            expect(b.peak).toBeGreaterThan(FTS_APPEND_WORKSPACE_BYTES);
            expect(delta.documents.count).toBe(n);
            expect(delta.terms?.first ?? null).toBe(
              posts.length ? "a".repeat(31) + "\ud801" : null,
            );
            expect(delta.documents.tokens).toBe(BigInt(posts.length) * BigInt(frequency));
            const root = await writeFtsNode(
              "ftsRoot",
              {
                kind: "column",
                ftsId: 1,
                tableId: 1,
                columnId: 1,
                schemaVersion: 1,
                generation: 1,
                mutationEpoch: 0,
                ...ftsSemantics(),
                state: "ready",
                owner: null,
                leaseUntil: 0,
                baseLsn: 1,
                throughLsn: 1,
                nextRowId: (docs.at(-1)?.id ?? 0) + 1,
                documentCount: n,
                tokenCount: delta.documents.tokens,
                documents: delta.documents,
                terms: delta.terms,
                deltas: null,
              },
              b,
              writer,
            );
            delta.release();
            delta.release();
            expect(() => delta.documents).toThrow("Released FTS insert delta");
            const actualDocs: FtsInsertDocument[] = [],
              actualPosts: FtsInsertPosting[] = [];
            for await (const event of walkFtsTree(root, b, space)) {
              if (event.kind === "ftsDocuments" && event.value.kind === "leaf")
                for (let i = 0; i < event.value.ids.length; i++)
                  actualDocs.push({
                    id: event.value.ids[i] ?? -1,
                    length: event.value.lengths[i] ?? -1,
                    renderable: event.value.renderable[i] === 1,
                  });
              if (event.kind === "ftsPostings" && event.value.kind === "leaf")
                for (let i = 0; i < event.value.ids.length; i++)
                  actualPosts.push({
                    term: "a".repeat(31) + "\ud801",
                    id: event.value.ids[i] ?? -1,
                    frequency: event.value.frequencies[i] ?? -1,
                  });
            }
            expect(actualDocs).toEqual(docs);
            expect(actualPosts).toEqual(posts);
            expect(b.held).toBe(0);
            reach.reach("exact documents");
            reach.reach("exact postings");
            reach.reach("wide IDs");
            reach.reach("raw UTF16");
            if (docs.some((doc) => !doc.renderable)) reach.reach("NULL documents");
            if (docs.some((doc) => doc.renderable && doc.length === 0))
              reach.reach("empty renderable");
            reach.reach("charged workspace");
            reach.reach("drained");
            if (!n) reach.reach("empty");
          } finally {
            await writer.abort();
          }
        },
      ),
      { seed, numRuns: 80 },
    );
    reach.assertReached();
  });
it("crosses document/posting leaves and term-directory leaves within a fixed budget", async () => {
  const b = account(),
    space = new MemoryExtentSpace(),
    writer = space.beginWrite("large delta");
  try {
    const docs = Array.from({ length: 4100 }, (_, i) => ({
      id: i + 1,
      length: 2,
      renderable: true,
    }));
    const posts = [
      ...docs.map((d) => ({ term: "a", id: d.id, frequency: 1 })),
      ...docs.map((d) => ({ term: "b" + String(d.id).padStart(4, "0"), id: d.id, frequency: 1 })),
    ];
    const delta = await writeFtsInsertDelta(stream(docs), stream(posts), b, writer);
    expect(delta.documents.count).toBe(4100);
    expect(delta.terms?.count).toBe(4101n);
    expect(delta.terms?.tokens).toBe(8200n);
    expect(b.held).toBe(FTS_APPEND_RESULT_BYTES);
    delta.release();
    expect(b.held).toBe(0);
  } finally {
    await writer.abort();
  }
});
it("refuses before input consumption, drains cancellation and never owns publication", async () => {
  let touched = 0;
  const input = async function* () {
    touched++;
    yield { id: 1, length: 0, renderable: true };
  };
  const space = new MemoryExtentSpace(),
    writer = space.beginWrite("cancel delta"),
    denied = account(0);
  try {
    await expect(writeFtsInsertDelta(input(), stream([]), denied, writer)).rejects.toBeInstanceOf(
      ParseBudgetError,
    );
    expect(touched).toBe(0);
    const abort = new AbortController(),
      b = account();
    const cancelled = async function* () {
      yield { id: 1, length: 0, renderable: true };
      abort.abort(new Error("stop append"));
      yield { id: 2, length: 0, renderable: true };
    };
    await expect(
      writeFtsInsertDelta(cancelled(), stream([]), b, writer, abort.signal),
    ).rejects.toThrow("stop append");
    expect(b.held).toBe(0);
    expect(writer.state).toBe("open");
  } finally {
    await writer.abort();
  }
});

it("keeps workspace through a failed pending append and cancellation", async () => {
  const b = account(),
    space = new MemoryExtentSpace(),
    writer = space.beginWrite("gated delta");
  let enter!: () => void, resume!: () => void;
  const entered = new Promise<void>((resolve) => {
    enter = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    resume = resolve;
  });
  const io: import("../storage/extents.js").ExtentWriter = {
    get label() {
      return writer.label;
    },
    get state() {
      return writer.state;
    },
    get bytes() {
      return writer.bytes;
    },
    get extents() {
      return writer.extents;
    },
    open: (n) => writer.open(n),
    discard: (e) => writer.discard(e),
    abort: () => writer.abort(),
    async write() {
      enter();
      await gate;
      throw new Error("disk append failed");
    },
  };
  const abort = new AbortController();
  const pending = writeFtsInsertDelta(
    stream([{ id: 1, length: 0, renderable: true }]),
    stream([]),
    b,
    io,
    abort.signal,
  );
  let settled = false;
  void pending.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  const rejected = expect(pending).rejects.toThrow("disk append failed");
  try {
    await entered;
    abort.abort(new Error("cancel append"));
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(b.held).toBeGreaterThan(FTS_APPEND_WORKSPACE_BYTES);
  } finally {
    resume();
    await rejected;
    await writer.abort();
  }
  expect(b.held).toBe(0);
});
