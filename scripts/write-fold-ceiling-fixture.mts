/** Manual frozen-writer fixture generation; never run as test setup. */
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { gzipSync } from "node:zlib";
import { MinnowDatabase } from "@minnowdb/core-layout10";
import { OpfsBlockStore } from "@minnowdb/core-layout10/storage/opfs";
import { MemoryOpfs } from "../packages/core/src/testing/opfs-shim.js";

const out = process.argv[2];
if (out === undefined)
  throw new Error("Usage: vite-node scripts/write-fold-ceiling-fixture.mts output.json.gz");
const writer = JSON.parse(
  await readFile(
    new URL("../node_modules/@minnowdb/core-layout10/package.json", import.meta.url),
    "utf8",
  ),
) as { version: string };
assert.equal(writer.version, "0.15.0");
const shim = new MemoryOpfs();
const store = await OpfsBlockStore.open({ name: "schema-change-folds", root: shim.root });
const database = new MinnowDatabase(store, { autoCompact: false });
await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER, c0 INTEGER)");
await database.execute("INSERT INTO t VALUES (1, 1, 1), (2, 2, 2)");
await database.execute("UPDATE t SET c0 = 9 WHERE id = 1");
await database.execute("ALTER TABLE t ADD COLUMN a BOOLEAN");
await database.execute("ALTER TABLE t ADD COLUMN s TEXT NOT NULL DEFAULT 'west'");
await database.execute("ALTER TABLE t DROP COLUMN c0");
let nextId = 3;
let refused = false;
for (; nextId < 3 + 4096 + 16; nextId++) {
  try {
    await database.execute("INSERT INTO t VALUES (?, ?, ?, ?)", [
      nextId,
      nextId,
      nextId % 2 === 0,
      `r${String(nextId)}`,
    ]);
  } catch (error) {
    assert.equal((error as Error).name, "CompactionBacklogError");
    refused = true;
    break;
  }
}
assert.equal(refused, true);
assert.equal(nextId, 4097);
await assert.rejects(
  database.compactTable("t", { minimumLevel0Segments: 1 }),
  /Full-row compaction source is missing columns/,
);
await database.close();
// Snapshot committed native bytes after releasing the historical store's sync handles.
store._crashForTests();
const files: Record<string, string> = {};
async function snapshot(directory: FileSystemDirectoryHandle, prefix: string): Promise<void> {
  const entries = [];
  for await (const [name, entry] of directory.entries()) entries.push([name, entry] as const);
  entries.sort(([a], [b]) => a.localeCompare(b));
  for (const [name, entry] of entries) {
    const path = prefix + name;
    if (entry.kind === "directory") await snapshot(entry as FileSystemDirectoryHandle, path + "/");
    else
      files[path] = Buffer.from(
        await (await (entry as FileSystemFileHandle).getFile()).arrayBuffer(),
      ).toString("base64");
  }
}
await snapshot(shim.root, "");
const fixture = {
  writerPackageVersion: writer.version,
  layoutFormatVersion: 10,
  levelZeroSegments: 4096,
  nextId,
  files,
};
const bytes = gzipSync(JSON.stringify(fixture));
await writeFile(out, bytes);
console.log(
  JSON.stringify({ nextId, files: Object.keys(files).length, compressedBytes: bytes.length }),
);
