/** Registered native FTS DAGs retain ownership through asynchronous IO and iterator close. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_STORAGE_REACH } from "../../testing/reach.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import type { ByteBudget, BlobRef, ExtentReader, ExtentWriter } from "../storage/extents.js";
import { ftsSemantics } from "../fts-config.js";
import { writeFtsNode, readFtsNode, walkFtsTree } from "../format/fts-storage.js";
import {
  ftsDocumentSummary,
  ftsPostingSummary,
  ftsTermSummary,
  ftsDeltaSummary,
  type FtsValues,
  type FtsFormatName,
} from "../format/fts-codec.js";
import type { FtsColumnRoot } from "../format/fts.js";
import { ManifestCorruptionError } from "../format/bytes.js";

function budget() {
  return {
    held: 0,
    peak: 0,
    allocate(n: number) {
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
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
function withWrite(writer: ExtentWriter, write: ExtentWriter["write"]): ExtentWriter {
  return {
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
    open: (capacity) => writer.open(capacity),
    write,
    discard: (extent) => writer.discard(extent),
    abort: () => writer.abort(),
  };
}
async function graph(writer: ExtentWriter, account: ByteBudget, frequency: number) {
  const refs: BlobRef[] = [];
  async function put<K extends FtsFormatName>(kind: K, value: FtsValues[K]) {
    const ref = await writeFtsNode(kind, value, account, writer);
    refs.push(ref);
    return ref;
  }
  async function batch(first: number, freq: number) {
    const docs: FtsValues["ftsDocuments"] = {
      kind: "leaf",
      ids: Float64Array.of(first, first + 1),
      lengths: Uint16Array.of(freq, 0),
      renderable: Uint8Array.of(1, 0),
    };
    const { depth: _d, ...docSum } = ftsDocumentSummary(docs);
    void _d;
    const docRef = { ref: await put("ftsDocuments", docs), ...docSum };
    const documents = {
      ref: await put("ftsDocuments", {
        kind: "branch",
        depth: 1,
        count: 2,
        tokens: BigInt(freq),
        children: [docRef],
      }),
      ...docSum,
    };
    const posts: FtsValues["ftsPostings"] = {
      kind: "leaf",
      ids: Float64Array.of(first),
      frequencies: Uint16Array.of(freq),
    };
    const { depth: _p, ...postSum } = ftsPostingSummary(posts);
    void _p;
    const posting = { ref: await put("ftsPostings", posts), ...postSum };
    const postings = {
      ref: await put("ftsPostings", {
        kind: "branch",
        depth: 1,
        count: 1,
        tokens: BigInt(freq),
        children: [posting],
      }),
      ...postSum,
    };
    const terms: FtsValues["ftsTerms"] = {
      kind: "leaf",
      entries: [{ term: "a".repeat(31) + "\ud801", postings }],
    };
    const { depth: _t, ...termSum } = ftsTermSummary(terms);
    void _t;
    const termRef = { ref: await put("ftsTerms", terms), ...termSum };
    return {
      documents,
      terms: {
        ref: await put("ftsTerms", {
          kind: "branch",
          depth: 1,
          count: 1n,
          tokens: BigInt(freq),
          children: [termRef],
        }),
        ...termSum,
      },
    };
  }
  const base = await batch(1, frequency),
    tail = await batch(3, frequency);
  const delta: FtsValues["ftsDelta"] = {
    kind: "leaf",
    ftsId: 1,
    generation: 1,
    mutationEpoch: 0,
    entries: [{ lsn: 2, ...tail }],
  };
  const { depth: _l, ...deltaSum } = ftsDeltaSummary(delta);
  void _l;
  const deltaRef = { ref: await put("ftsDelta", delta), ...deltaSum };
  const deltas = {
    ref: await put("ftsDelta", {
      kind: "branch",
      ftsId: 1,
      generation: 1,
      mutationEpoch: 0,
      depth: 1,
      count: 1,
      documents: 2,
      tokens: BigInt(frequency),
      children: [deltaRef],
    }),
    ...deltaSum,
  };
  const column: FtsColumnRoot = {
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
    throughLsn: 2,
    nextRowId: 5,
    documentCount: 4,
    tokenCount: BigInt(frequency) * 2n,
    ...base,
    deltas,
  };
  const columnRef = await put("ftsRoot", column);
  const directory = await put("ftsRoot", {
    kind: "directory",
    tableId: 1,
    mutationEpoch: 0,
    depth: 0,
    count: 1,
    children: [{ ref: columnRef, first: 1, last: 1, count: 1 }],
  });
  const root = await put("ftsRoot", {
    kind: "directory",
    tableId: 1,
    mutationEpoch: 0,
    depth: 1,
    count: 1,
    children: [{ ref: directory, first: 1, last: 1, count: 1 }],
  });
  return { root, refs, column, columnRef };
}
for (const seed of seedsFor("next-fts-storage", [0xb81d, 0xb81e]))
  it(`walks complete FTS trees and drains fault/cancel histories seed ${String(seed)}`, async () => {
    const reach = createReach("FTS storage", FTS_STORAGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4096 }),
        fc.integer({ min: 0, max: 7 }),
        async (freq, stop) => {
          reach.beginHistory();
          const account = budget(),
            space = new MemoryExtentSpace(),
            writer = space.beginWrite("fts");
          try {
            const built = await graph(writer, account, freq);
            const orphan = await writeFtsNode(
              "ftsDocuments",
              {
                kind: "leaf",
                ids: Float64Array.of(99),
                lengths: Uint16Array.of(0),
                renderable: Uint8Array.of(1),
              },
              account,
              writer,
            );
            expect(account.held).toBe(0);
            const seen: BlobRef[] = [];
            for await (const event of walkFtsTree(built.root, account, space)) {
              expect(account.held).toBeGreaterThan(0);
              seen.push(event.ref);
              if (event.kind === "ftsRoot")
                reach.reach(event.value.kind === "directory" ? "directory" : "root");
              else if (event.value.kind === "branch")
                reach.reach(
                  event.kind === "ftsTerms"
                    ? "terms branch"
                    : event.kind === "ftsPostings"
                      ? "postings branch"
                      : event.kind === "ftsDocuments"
                        ? "documents branch"
                        : "delta branch",
                );
            }
            expect(seen).toHaveLength(built.refs.length);
            expect(new Set(seen.map((r) => r.extent.offset))).toEqual(
              new Set(built.refs.map((r) => r.extent.offset)),
            );
            expect(seen).not.toContainEqual(orphan);
            reach.reach("exact graph");
            reach.reach("orphan excluded");
            // A valid parent page that lies about a child must fail the reference walk.
            const bad = await writeFtsNode(
              "ftsRoot",
              {
                ...built.column,
                terms: built.column.terms ? { ...built.column.terms, first: "0", last: "0" } : null,
              },
              account,
              writer,
            );
            await expect(async () => {
              for await (const event of walkFtsTree(bad, account, space)) {
                void event; // drain
              }
            }).rejects.toThrow(ManifestCorruptionError);
            reach.reach("parent bounds");
            const cancel = new AbortController();
            let count = 0;
            try {
              for await (const event of walkFtsTree(built.root, account, space, cancel.signal)) {
                void event;
                if (count++ === stop) cancel.abort(new Error("FTS stopped"));
              }
            } catch (error) {
              expect(error).toEqual(new Error("FTS stopped"));
            }
            expect(account.held).toBe(0);
            expect(space.leased).toBe(0);
            reach.reach("cancel");
            // The real writer borrows our encoded buffer until its promise settles.
            const gate = deferred(),
              entered = deferred(),
              abort = new AbortController();
            const guarded = withWrite(writer, async (parts) => {
              entered.resolve();
              await gate.promise;
              const bytes = parts[0];
              if (!bytes) throw new Error("missing FTS IO buffer");
              expect(account.held).toBeGreaterThan(bytes.length);
              return writer.write(parts);
            });
            const pending = writeFtsNode("ftsRoot", built.column, account, guarded, abort.signal);
            const rejected = expect(pending).rejects.toThrow("cancel write");
            try {
              await entered.promise;
              abort.abort(new Error("cancel write"));
              expect(account.held).toBeGreaterThan(0);
            } finally {
              gate.resolve();
              await rejected;
            }
            expect(account.held).toBe(0);
            reach.reach("write fence");
            const readGate = deferred(),
              readEntered = deferred(),
              readAbort = new AbortController();
            const delayed: ExtentReader = {
              peek: () => null,
              async read(refs, options) {
                const lease = await space.read(refs, options);
                readEntered.resolve();
                await readGate.promise;
                return lease;
              },
            };
            const read = readFtsNode("ftsRoot", built.root, account, delayed, readAbort.signal);
            const readRejected = expect(read).rejects.toThrow("cancel read");
            try {
              await readEntered.promise;
              readAbort.abort(new Error("cancel read"));
              expect(account.held).toBeGreaterThan(0);
            } finally {
              readGate.resolve();
              await readRejected;
            }
            expect(space.leased).toBe(0);
            expect(account.held).toBe(0);
            reach.reach("read fence");
            let early = 0;
            for await (const event of walkFtsTree(built.root, account, space)) {
              void event;
              if (early++ === stop) break;
            }
            expect(space.leased).toBe(0);
            expect(account.held).toBe(0);
            reach.reach("early close");
            const failedWriter = withWrite(writer, () =>
              Promise.reject(new Error("disk write failed")),
            );
            await expect(
              writeFtsNode("ftsRoot", built.column, account, failedWriter),
            ).rejects.toThrow("disk write failed");
            const failedReader: ExtentReader = {
              peek: () => null,
              read: () => Promise.reject(new Error("disk read failed")),
            };
            await expect(readFtsNode("ftsRoot", built.root, account, failedReader)).rejects.toThrow(
              "disk read failed",
            );
            expect(account.held).toBe(0);
            reach.reach("IO failure");
            reach.reach("drained");
          } finally {
            await writer.abort();
          }
        },
      ),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });
