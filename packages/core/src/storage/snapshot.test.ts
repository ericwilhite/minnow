import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { UniqueConstraintError } from "../engine/errors.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { MemoryBlockStore } from "./memory.js";
import { decodeSnapshot, encodeSnapshot, readSnapshotSummary } from "./snapshot.js";

/**
 * A database with something of every kind a snapshot has to carry. `authors` stays append-only
 * so it can hold a full-text index (the engine only indexes append-only tables) and carries the
 * unique key a later write has to conflict against; `posts` is updated and deleted after it is
 * inserted, so the restored copy has to fold its segments back in the original commit order.
 */
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
  // Rewrite and remove rows after the fact: the restored copy must fold these later segments
  // over the inserts they patch, which is what makes commit ordering load-bearing.
  await database.execute("UPDATE posts SET score = score + 5 WHERE handle = 'grace'");
  await database.execute("DELETE FROM posts WHERE id = 4");
  await database.buildFtsIndex("authors", "name");
  return { database, store };
}

const REPORT = `SELECT a.name, COUNT(p.id) AS posts, SUM(p.score) AS points
FROM authors a JOIN posts p ON p.handle = a.handle
GROUP BY a.name ORDER BY points DESC`;

describe("database snapshots", () => {
  it("round-trips a database through the container into a fresh memory store", async () => {
    const { database: source, store: sourceStore } = await seededDatabase();
    const before = await source.query(REPORT);
    const search = await source.query(
      "SELECT handle FROM authors WHERE MATCH(name) AGAINST 'hopper' ORDER BY handle",
    );
    expect(search.rows).toEqual([{ handle: "grace" }]);

    const snapshot = await sourceStore.exportSnapshot();
    const bytes = await encodeSnapshot(snapshot);

    const summary = await readSnapshotSummary(bytes);
    expect(summary.formatVersion).toBe(1);
    expect(summary.tableCount).toBe(2);
    expect(summary.blockCount).toBeGreaterThan(0);
    expect(summary.byteLength).toBe(bytes.byteLength);

    const restoredStore = MemoryBlockStore.fromSnapshot(await decodeSnapshot(bytes));
    const restored = new MinnowDatabase(restoredStore);
    expect((await restored.query(REPORT)).rows).toEqual(before.rows);
    expect((await restored.query("SELECT id, score FROM posts ORDER BY id")).rows).toEqual(
      (await source.query("SELECT id, score FROM posts ORDER BY id")).rows,
    );
    // The full-text base covered the exported version, so it carries over ready-to-use. The
    // MATCH below would answer correctly either way — the scan re-verifies candidates — so the
    // recorded state is what proves the base came with it instead of being rebuilt.
    const authors = (await restoredStore.listTables()).find((table) => table.name === "authors");
    expect(Object.values(authors?.ftsColumns ?? {}).map((column) => column.state)).toEqual([
      "ready",
    ]);
    expect(
      (await restored.query("SELECT handle FROM authors WHERE MATCH(name) AGAINST 'hopper'")).rows,
    ).toEqual([{ handle: "grace" }]);
  });

  it("marks a full-text index that no longer covers the version for rebuild", async () => {
    const { database: source, store: sourceStore } = await seededDatabase();
    // A commit after the build leaves the base behind the current version. The snapshot must
    // not present a stale base as ready; it exports the column as invalid so a load rebuilds.
    await source.execute(
      "INSERT INTO authors (handle, name, reputation) VALUES ('radia', 'Radia Perlman', 190)",
    );
    const restored = MemoryBlockStore.fromSnapshot(
      await decodeSnapshot(await encodeSnapshot(await sourceStore.exportSnapshot())),
    );
    const authors = (await restored.listTables()).find((table) => table.name === "authors");
    expect(Object.values(authors?.ftsColumns ?? {}).map((column) => column.state)).toEqual([
      "invalid",
    ]);
    expect(
      (
        await new MinnowDatabase(restored).query(
          "SELECT handle FROM authors WHERE MATCH(name) AGAINST 'perlman'",
        )
      ).rows,
    ).toEqual([{ handle: "radia" }]);
  });

  it("keeps writing correctly after a restore", async () => {
    const { store: sourceStore } = await seededDatabase();
    const bytes = await encodeSnapshot(await sourceStore.exportSnapshot());
    const restored = new MinnowDatabase(MemoryBlockStore.fromSnapshot(await decodeSnapshot(bytes)));

    // Unique-key membership came across, so a duplicate still conflicts.
    await expect(
      restored.execute("INSERT INTO authors (handle, name) VALUES ('ada', 'Impostor')"),
    ).rejects.toThrow(UniqueConstraintError);

    await restored.execute(
      "INSERT INTO authors (handle, name, reputation) VALUES ('barbara', 'Barbara Liskov', 210)",
    );
    await restored.execute(
      "INSERT INTO posts (id, handle, title, score) VALUES (5, 'barbara', 'Programming with abstract data types', 51)",
    );
    expect((await restored.query("SELECT COUNT(*) AS n FROM authors")).rows).toEqual([{ n: 4 }]);
    expect((await restored.query("SELECT id FROM posts ORDER BY id")).rows).toEqual([
      { id: 1 },
      { id: 2 },
      { id: 3 },
      { id: 5 },
    ]);
    // Row IDs continue past the restored high-water mark rather than colliding with hidden ones.
    expect((await restored.query("SELECT SUM(score) AS points FROM posts")).rows).toEqual([
      { points: 12 + 35 + 44 + 51 },
    ]);
  });

  it("loads into IndexedDB and reads back identically", async () => {
    const { database: source, store: sourceStore } = await seededDatabase();
    const before = await source.query(REPORT);
    const bytes = await encodeSnapshot(await sourceStore.exportSnapshot());

    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    await store.importSnapshot(await decodeSnapshot(bytes));
    expect((await new MinnowDatabase(store).query(REPORT)).rows).toEqual(before.rows);
    store.close();

    // Reopening proves the load is durable rather than living in the loading instance's caches.
    const reopened = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(reopened);
    expect((await database.query(REPORT)).rows).toEqual(before.rows);
    await expect(
      database.execute("INSERT INTO authors (handle, name) VALUES ('grace', 'Impostor')"),
    ).rejects.toThrow(UniqueConstraintError);
    await database.execute(
      "INSERT INTO authors (handle, name, reputation) VALUES ('barbara', 'Barbara Liskov', 210)",
    );
    expect((await database.query("SELECT COUNT(*) AS n FROM authors")).rows).toEqual([{ n: 4 }]);
    expect(
      (await database.query("SELECT handle FROM authors WHERE MATCH(name) AGAINST 'lovelace'"))
        .rows,
    ).toEqual([{ handle: "ada" }]);
  });

  it("refuses to load into a store that already holds data", async () => {
    const { store: sourceStore } = await seededDatabase();
    const snapshot = await decodeSnapshot(await encodeSnapshot(await sourceStore.exportSnapshot()));
    const store = await IndexedDbBlockStore.open({
      name: crypto.randomUUID(),
      indexedDB: new IDBFactory(),
    });
    await store.importSnapshot(snapshot);
    await expect(store.importSnapshot(snapshot)).rejects.toThrow(/already holds/);
  });

  it("rejects a damaged container instead of loading half a database", async () => {
    const { store: sourceStore } = await seededDatabase();
    const bytes = await encodeSnapshot(await sourceStore.exportSnapshot());

    await expect(decodeSnapshot(bytes.subarray(0, bytes.byteLength - 1))).rejects.toThrow(
      /length mismatch/,
    );
    await expect(decodeSnapshot(bytes.subarray(0, 8))).rejects.toThrow(/Truncated snapshot header/);

    const wrongMagic = bytes.slice();
    wrongMagic[0] = 0x00;
    await expect(decodeSnapshot(wrongMagic)).rejects.toThrow(/Not a Minnow snapshot/);

    const wrongVersion = bytes.slice();
    new DataView(wrongVersion.buffer).setUint32(8, 99, true);
    await expect(decodeSnapshot(wrongVersion)).rejects.toThrow(/Unsupported snapshot version 99/);

    const damagedHeader = bytes.slice();
    // Flip a byte inside the stored header. The checksum is verified before the header is
    // decompressed, so this fails as a checksum mismatch rather than as a gzip error.
    damagedHeader[34] = (damagedHeader[34] ?? 0) ^ 0xff;
    await expect(decodeSnapshot(damagedHeader)).rejects.toThrow(/header checksum mismatch/);

    // A damaged block header fails here: `inspectBlock` verifies each block's envelope
    // checksum without decompressing it. A damaged payload is caught later, by the block's
    // own payload checksum, when the column is actually decoded.
    const damagedBlock = bytes.slice();
    const firstBlockAt = 28 + new DataView(bytes.buffer).getUint32(12, true);
    damagedBlock[firstBlockAt + 13] = (damagedBlock[firstBlockAt + 13] ?? 0) ^ 0xff;
    await expect(decodeSnapshot(damagedBlock)).rejects.toThrow(/checksum mismatch|encoding/);
  });

  it("refuses to snapshot a database with nothing committed", async () => {
    await expect(new MemoryBlockStore().exportSnapshot()).rejects.toThrow(/no committed version/);
  });
});
