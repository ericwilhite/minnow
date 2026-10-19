/**
 * Reads over damaged IndexedDB storage. A flipped byte inside a stored block — packed or
 * unpacked — must surface as `StorageCorruptionError` (classified `corruption`, never retried)
 * naming the block, not as the decoder's bare `Error`, and the integrity check must report the
 * same block. A `manifest/current` pointer that went backwards is invisible to readers, who
 * simply answer from the older version, so the integrity check must report the readable
 * manifest it left stranded above the pointer.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { classifyError } from "../engine/errors.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import { readRawStore, writeRawValue } from "./indexeddb-audit-helpers.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { StorageCorruptionError } from "./types.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

const OPTIONS = { autoCompact: false, autoCollect: false, rowsPerBlock: 8 } as const;
/** `SUM(b)` over the audited rows; the padding rows carry `b = 0`. */
const EXPECTED = { n: 1_550, s: 1225 };

interface Built {
  indexedDB: IDBFactory;
  name: string;
}

interface RawSegment {
  id: string;
  packedBlockId?: string;
  columnBlockIds: Record<string, string[]>;
}

/**
 * One commit well past the packing limit (incompressible text, so its stored bytes stay large)
 * and a few single-row commits the engine packs, then the connection is closed so nothing is
 * cached when the damaged database is reopened.
 */
async function build(): Promise<Built> {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const store = await IndexedDbBlockStore.open({ name, indexedDB });
  const db = new MinnowDatabase(store, OPTIONS);
  await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
  const pad = (): string => `${crypto.randomUUID()}${crypto.randomUUID()}${crypto.randomUUID()}`;
  await db.insertBatch("t", [
    ...Array.from({ length: 40 }, (_, index) => ({
      id: index,
      a: `row-${String(index)}`,
      b: index,
    })),
    ...Array.from({ length: 1_500 }, (_, index) => ({ id: 1_000 + index, a: `z${pad()}`, b: 0 })),
  ]);
  for (let id = 40; id < 50; id += 1) {
    await db.execute(
      `INSERT INTO t (id, a, b) VALUES (${String(id)}, 'row-${String(id)}', ${String(id)})`,
    );
  }
  expect(
    (await db.query("SELECT COUNT(*) AS n, SUM(b) AS s FROM t", { memoize: false })).rows,
  ).toEqual([EXPECTED]);
  await db.close();
  store.close();
  return { indexedDB, name };
}

async function segments(built: Built): Promise<RawSegment[]> {
  return (await readRawStore(built.indexedDB, built.name, "segments")).map(
    (entry) => entry.value as RawSegment,
  );
}

async function blockBytes(built: Built, blockId: string): Promise<Uint8Array> {
  const blocks = await readRawStore(built.indexedDB, built.name, "blocks");
  const bytes = blocks.find((entry) => entry.key === blockId)?.value;
  if (!(bytes instanceof Uint8Array)) throw new Error(`block ${blockId} is missing`);
  return bytes;
}

async function flipByte(built: Built, blockId: string, at: number): Promise<void> {
  const bytes = new Uint8Array(await blockBytes(built, blockId));
  bytes[at] = (bytes[at] ?? 0) ^ 0x5a;
  await writeRawValue(built.indexedDB, built.name, "blocks", blockId, bytes);
}

async function readDamaged(
  built: Built,
): Promise<{ failure: unknown; issues: readonly unknown[] }> {
  const store = await IndexedDbBlockStore.open({ name: built.name, indexedDB: built.indexedDB });
  const db = new MinnowDatabase(store, OPTIONS);
  try {
    const failure = await db
      .query("SELECT COUNT(*) AS n, SUM(b) AS s, MIN(a) AS lo FROM t", { memoize: false })
      .then(
        () => undefined,
        (error: unknown) => error,
      );
    const { issues } = await store.checkIntegrity();
    return { failure, issues };
  } finally {
    await db.close();
    store.close();
  }
}

/** The error names the block, or — for a packed block whose member failed to decode — a member of it. */
function expectBlockCorruption(failure: unknown, blockId: string): void {
  expect(failure).toBeInstanceOf(StorageCorruptionError);
  const error = failure as StorageCorruptionError;
  expect(error.backend).toBe("indexeddb");
  expect(
    error.location === `blocks/${blockId}` || error.location.startsWith(`blocks/${blockId}\u0000`),
    error.location,
  ).toBe(true);
  expect(error.message).toMatch(/checksum mismatch/);
  expect(error.cause).toBeInstanceOf(Error);
  expect(classifyError(error)).toMatchObject({ kind: "corruption", retry: "never" });
}

describe("reads over damaged IndexedDB storage", () => {
  it("a flipped byte in an unpacked block is StorageCorruptionError naming the block", async () => {
    const built = await build();
    const unpacked = (await segments(built)).find((segment) => segment.packedBlockId === undefined);
    const blockId = Object.values(unpacked?.columnBlockIds ?? {})[2]?.[0];
    if (blockId === undefined) throw new Error("the large commit was not stored unpacked");
    const bytes = await blockBytes(built, blockId);
    await flipByte(built, blockId, bytes.byteLength - 1);
    const { failure, issues } = await readDamaged(built);
    expectBlockCorruption(failure, blockId);
    expect(issues).toContainEqual(
      expect.objectContaining({ code: "invalid-block", location: `blocks/${blockId}` }),
    );
  });

  it("a flipped byte in a block's envelope is StorageCorruptionError too", async () => {
    const built = await build();
    const unpacked = (await segments(built)).find((segment) => segment.packedBlockId === undefined);
    const blockId = Object.values(unpacked?.columnBlockIds ?? {})[1]?.[0];
    if (blockId === undefined) throw new Error("the large commit was not stored unpacked");
    // The header and its metadata come first; the envelope checksum covers them.
    await flipByte(built, blockId, 12);
    const { failure } = await readDamaged(built);
    expect(failure).toBeInstanceOf(StorageCorruptionError);
    expect((failure as StorageCorruptionError).location).toMatch(/^blocks/);
    expect(classifyError(failure)).toMatchObject({ kind: "corruption" });
  });

  it("a flipped byte in a packed block is StorageCorruptionError naming the packed block", async () => {
    const built = await build();
    const packed = (await segments(built)).find((segment) => segment.packedBlockId !== undefined);
    const blockId = packed?.packedBlockId;
    if (blockId === undefined) throw new Error("no single-row commit was packed");
    const bytes = await blockBytes(built, blockId);
    await flipByte(built, blockId, Math.floor(bytes.byteLength * 0.7));
    const { failure, issues } = await readDamaged(built);
    expectBlockCorruption(failure, blockId);
    expect(issues).toContainEqual(
      expect.objectContaining({ code: "invalid-block", location: `blocks/${blockId}` }),
    );
  });

  it("a current pointer that went backwards is reported as an orphan manifest", async () => {
    const built = await build();
    const pointer = (await readRawStore(built.indexedDB, built.name, "catalog")).find(
      (entry) => entry.key === "manifest/current",
    );
    const latest = pointer?.value as number;
    expect(latest).toBeGreaterThan(0);
    await writeRawValue(built.indexedDB, built.name, "catalog", "manifest/current", latest - 1);
    const store = await IndexedDbBlockStore.open({ name: built.name, indexedDB: built.indexedDB });
    const db = new MinnowDatabase(store, OPTIONS);
    try {
      // Readers cannot tell: they answer from the older version the pointer now names.
      const rows = (await db.query("SELECT COUNT(*) AS n FROM t", { memoize: false })).rows;
      expect(rows).toEqual([{ n: EXPECTED.n - 1 }]);
      const { issues } = await store.checkIntegrity({ mode: "metadata" });
      expect(issues).toContainEqual(
        expect.objectContaining({
          code: "orphan-manifest",
          location: `manifests/${String(latest)}`,
        }),
      );
      // Restoring the pointer clears the report.
      await writeRawValue(built.indexedDB, built.name, "catalog", "manifest/current", latest);
      expect((await store.checkIntegrity({ mode: "metadata" })).issues).toEqual([]);
    } finally {
      await db.close();
      store.close();
    }
  });
});
