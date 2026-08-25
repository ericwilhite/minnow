import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MAX_STORED_BLOCK_BYTE_LENGTH } from "../block-format/index.js";
import { MinnowDatabase } from "../engine/database.js";
import { UniqueConstraintError } from "../engine/errors.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { MemoryBlockStore } from "./memory.js";
import {
  decodeSnapshotFrameStream,
  MAX_SNAPSHOT_STREAM_CHUNK_BYTES,
  readSnapshotSummary,
} from "./snapshot.js";

class SnapshotBatchMemoryStore extends MemoryBlockStore {
  readonly batches: Array<{ blocks: number; metadataBytes: number; bytes: number }> = [];

  override async appendSnapshotImportFrames(
    input: Parameters<MemoryBlockStore["appendSnapshotImportFrames"]>[0],
  ) {
    this.batches.push({
      blocks: input.frames.filter((frame) => frame.kind === "block").length,
      metadataBytes: input.frames.reduce(
        (total, frame) => total + (frame.kind === "block" ? 0 : frame.payload.byteLength),
        0,
      ),
      bytes: input.frames.reduce((total, frame) => total + frame.payload.byteLength, 0),
    });
    return super.appendSnapshotImportFrames(input);
  }
}

async function seededDatabase(): Promise<{ database: MinnowDatabase; store: MemoryBlockStore }> {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.execute(
    "CREATE TABLE authors (handle VARCHAR(40) PRIMARY KEY, name VARCHAR(80) NOT NULL, reputation INTEGER)",
  );
  await database.execute(
    "CREATE TABLE posts (id INTEGER PRIMARY KEY, handle VARCHAR(40) NOT NULL, title VARCHAR(200) NOT NULL, score INTEGER)",
  );
  await database.execute(
    "INSERT INTO authors (handle, name, reputation) VALUES ('ada', 'Ada Lovelace', 100), ('grace', 'Grace Hopper', 250), ('alan', 'Alan Turing', 175)",
  );
  await database.execute(
    `INSERT INTO posts (id, handle, title, score) VALUES
       (1, 'ada', 'Notes on the analytical engine', 12),
       (2, 'grace', 'A compiler for english commands', 30),
       (3, 'alan', 'Computing machinery and intelligence', 44),
       (4, 'grace', 'Nanoseconds and other short wires', 9)`,
  );
  await database.execute("UPDATE posts SET score = score + 5 WHERE handle = 'grace'");
  await database.execute("DELETE FROM posts WHERE id = 4");
  await database.buildFtsIndex("authors", "name");
  return { database, store };
}

async function* bytesAsChunks(bytes: Uint8Array): AsyncGenerator<Uint8Array> {
  for (let offset = 0; offset < bytes.byteLength; offset += MAX_SNAPSHOT_STREAM_CHUNK_BYTES) {
    yield bytes.subarray(offset, offset + MAX_SNAPSHOT_STREAM_CHUNK_BYTES);
  }
}

const REPORT = `SELECT a.name, COUNT(p.id) AS posts, SUM(p.score) AS points
FROM authors a JOIN posts p ON p.handle = a.handle
GROUP BY a.name ORDER BY points DESC`;

describe("MinnowDatabase framed snapshots", () => {
  it("streams a multi-commit snapshot in bounded chunks and retries a lost final acknowledgement", async () => {
    const { database: source } = await seededDatabase();
    const expected = (await source.query(REPORT)).rows;
    const chunks: Uint8Array[] = [];
    for await (const chunk of source.exportSnapshotStream()) {
      expect(chunk.byteLength).toBeLessThanOrEqual(MAX_SNAPSHOT_STREAM_CHUNK_BYTES);
      chunks.push(chunk.slice());
    }
    expect(chunks.length).toBeGreaterThan(1);

    const restored = new MinnowDatabase(new MemoryBlockStore());
    const input = async function* (): AsyncGenerator<Uint8Array> {
      for (const chunk of chunks) yield chunk;
    };
    await restored.importSnapshotStream(input());
    await restored.importSnapshotStream(input());
    expect((await restored.query(REPORT)).rows).toEqual(expected);
  });

  it("reports the fixed header summary without materializing body metadata", async () => {
    const { database, store } = await seededDatabase();
    const bytes = await database.exportSnapshot();
    const summary = await readSnapshotSummary(bytes);
    expect(summary).toMatchObject({
      formatVersion: 1,
      version: await store.getCurrentManifestVersion(),
      tableCount: 2,
      byteLength: bytes.byteLength,
    });
    expect(summary.blockCount).toBeGreaterThan(0);
    expect(summary.payloadBytes).toBeGreaterThan(0);
  });

  it("stages many small frames in bounded adapter batches", async () => {
    const { database: source } = await seededDatabase();
    const bytes = await source.exportSnapshot();
    const store = new SnapshotBatchMemoryStore();
    await new MinnowDatabase(store).importSnapshot(bytes);
    expect(store.batches.some((batch) => batch.blocks > 1)).toBe(true);
    expect(Math.max(...store.batches.map((batch) => batch.blocks))).toBeLessThanOrEqual(64);
    expect(Math.max(...store.batches.map((batch) => batch.metadataBytes))).toBeLessThanOrEqual(
      16 * 1024 * 1024,
    );
    expect(Math.max(...store.batches.map((batch) => batch.bytes))).toBeLessThanOrEqual(
      MAX_STORED_BLOCK_BYTE_LENGTH + 8 * 1024,
    );
  });

  it("releases an abandoned export pin and removes explicitly cancelled import staging", async () => {
    const { database: source, store } = await seededDatabase();
    const iterator = source.exportSnapshotStream()[Symbol.asyncIterator]();
    expect((await iterator.next()).done).toBe(false);
    await iterator.return();
    expect((await store.listLeases()).filter((lease) => lease.kind === "backup")).toEqual([]);

    const bytes = await source.exportSnapshot();
    const headerLength = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
      12,
      true,
    );
    const headerEnd = 20 + headerLength;
    const controller = new AbortController();
    const cancelled = async function* (): AsyncGenerator<Uint8Array> {
      yield bytes.slice(0, headerEnd);
      controller.abort(new Error("cancel import"));
      yield bytes.slice(headerEnd);
    };
    const targetStore = new MemoryBlockStore();
    const target = new MinnowDatabase(targetStore);
    await expect(
      target.importSnapshotStream(cancelled(), { signal: controller.signal }),
    ).rejects.toThrow("cancel import");
    expect(await target.inspectInterruptedImport()).toBeNull();
    expect(await targetStore.getCurrentManifestVersion()).toBeNull();
  });

  it("validates a large direct source chunk by contents rather than transport partitioning", async () => {
    const oversized = async function* (): AsyncGenerator<Uint8Array> {
      yield new Uint8Array(MAX_SNAPSHOT_STREAM_CHUNK_BYTES + 1);
    };
    const consume = async (): Promise<void> => {
      for await (const entry of decodeSnapshotFrameStream(oversized())) void entry;
    };
    await expect(consume()).rejects.toThrow(/Not a Minnow snapshot/);
  });

  it("exports one file that restores into Memory and IndexedDB", async () => {
    const { database: source } = await seededDatabase();
    const before = await source.query(REPORT);
    const phases: string[] = [];
    const bytes = await source.exportSnapshot({
      onProgress: (progress) => phases.push(progress.phase),
    });
    expect(phases[0]).toBe("reading");
    expect(phases.at(-1)).toBe("done");
    expect(phases.filter((phase) => phase === "done")).toHaveLength(1);
    expect(phases).toContain("transfer");

    const memory = new MinnowDatabase(new MemoryBlockStore());
    await memory.importSnapshot(bytes);
    expect((await memory.query(REPORT)).rows).toEqual(before.rows);

    const indexedStore = await IndexedDbBlockStore.open({
      name: crypto.randomUUID(),
      indexedDB: new IDBFactory(),
    });
    const indexed = new MinnowDatabase(indexedStore);
    await indexed.importSnapshot(bytes);
    expect((await indexed.query(REPORT)).rows).toEqual(before.rows);
  });

  it("preserves UNIQUE membership and writes correctly after restore", async () => {
    const { database: source } = await seededDatabase();
    const restored = new MinnowDatabase(new MemoryBlockStore());
    await restored.importSnapshot(await source.exportSnapshot());
    await expect(
      restored.execute("INSERT INTO authors VALUES ('ada', 'Other Ada', 1)"),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    await restored.execute("INSERT INTO authors VALUES ('donald', 'Donald Knuth', 200)");
    expect((await restored.query("SELECT COUNT(*) AS n FROM authors")).rows).toEqual([{ n: 4 }]);
  });

  it("answers from the restored catalog rather than an earlier empty-cache result", async () => {
    const { database: source } = await seededDatabase();
    const restored = new MinnowDatabase(new MemoryBlockStore());
    await expect(restored.query("SELECT COUNT(*) AS n FROM authors")).rejects.toThrow();
    await restored.importSnapshot(await source.exportSnapshot());
    expect((await restored.query("SELECT COUNT(*) AS n FROM authors")).rows).toEqual([{ n: 3 }]);
  });

  it("rejects corruption, truncation, trailing bytes, and non-empty targets", async () => {
    const { database: source } = await seededDatabase();
    const bytes = await source.exportSnapshot();
    const damaged = bytes.slice();
    const damagedIndex = Math.floor(damaged.byteLength / 2);
    damaged[damagedIndex] = (damaged[damagedIndex] ?? 0) ^ 0xff;
    await expect(
      new MinnowDatabase(new MemoryBlockStore()).importSnapshot(damaged),
    ).rejects.toThrow();
    await expect(
      new MinnowDatabase(new MemoryBlockStore()).importSnapshot(bytes.subarray(0, -1)),
    ).rejects.toThrow(/truncated/);
    const trailing = new Uint8Array(bytes.byteLength + 1);
    trailing.set(bytes);
    await expect(
      new MinnowDatabase(new MemoryBlockStore()).importSnapshot(trailing),
    ).rejects.toThrow(/trailing/);
    await expect(source.importSnapshot(bytes)).rejects.toThrow(/already holds/);
  });

  it("reports unsupported stores at the capability boundary", async () => {
    const bare = Object.create(new MemoryBlockStore()) as Record<string, unknown>;
    bare.beginSnapshotFrameExport = undefined;
    bare.readSnapshotExportFrame = undefined;
    bare.closeSnapshotFrameExport = undefined;
    bare.beginSnapshotFrameImport = undefined;
    bare.renewSnapshotFrameImport = undefined;
    bare.appendSnapshotImportFrames = undefined;
    bare.finishSnapshotFrameImport = undefined;
    bare.cancelSnapshotFrameImport = undefined;
    const database = new MinnowDatabase(bare as unknown as MemoryBlockStore);
    await expect(database.exportSnapshot()).rejects.toThrow(/cannot stream snapshots/);
    await expect(database.importSnapshot(new Uint8Array(0))).rejects.toThrow(
      /cannot stream snapshots/,
    );
  });

  it("accepts arbitrary legal source chunk boundaries", async () => {
    const { database: source } = await seededDatabase();
    const bytes = await source.exportSnapshot();
    const target = new MinnowDatabase(new MemoryBlockStore());
    const oneByteChunks = async function* (): AsyncGenerator<Uint8Array> {
      for (const byte of bytes) yield Uint8Array.of(byte);
    };
    await target.importSnapshotStream(oneByteChunks());
    expect((await target.query(REPORT)).rows).toEqual((await source.query(REPORT)).rows);
    let entries = 0;
    for await (const entry of decodeSnapshotFrameStream(bytesAsChunks(bytes))) {
      void entry;
      entries += 1;
    }
    expect(entries).toBeGreaterThan(2);
  });
});
