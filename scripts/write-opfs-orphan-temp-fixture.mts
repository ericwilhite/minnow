/**
 * Freezes an OPFS database as core 0.19.x left it after a query spilled, was abandoned, and its
 * expired spill owner was swept by the next spilling query: the swept owner's temp pages stay in
 * the leader's ledger and on disk with no owner record, a checkpoint captures them, and the
 * write-ahead log continues after it. 0.19.x refuses to reopen this database
 * ("Temp page has no owner"). `orphan-temp-pages.test.ts` opens it with the current build.
 *
 * Only a 0.19.x writer produces this state; a current build removes swept owners' pages, and the
 * script refuses to write a fixture that holds no orphan. To regenerate, run it from a 0.19.1
 * checkout: `npx vite-node scripts/write-opfs-orphan-temp-fixture.mts`.
 */
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { MinnowDatabase } from "../packages/core/src/engine/database.ts";
import { MemoryOpfs } from "../packages/core/src/testing/opfs-shim.ts";
import { OpfsBlockStore } from "../packages/core/src/storage/opfs/index.ts";
import { OPFS_LAYOUT_VERSION } from "../packages/core/src/storage/opfs/upgrades.ts";
import { decodeSyncCheckpoint } from "../packages/core/src/storage/toolkit/wire.ts";

const databaseName = "orphan-fixture";
const prefix = `minnowdb/${databaseName}`;
const shim = new MemoryOpfs();
const iso = (ms: number): string => new Date(ms).toISOString();
const t0 = Date.parse("2026-11-01T00:00:00.000Z");
let clock = t0;
const now = (): Date => new Date((clock += 1));

// The checkpoint lands on the sweeping create (entry 7); the rest stays in the log's tail.
const store = await OpfsBlockStore.open({
  name: databaseName,
  root: shim.root,
  checkpointEntries: 7,
});
const database = new MinnowDatabase(store, { now, compression: "raw" });
await database.createTable({
  name: "kept",
  columns: [
    { name: "id", type: "number" },
    { name: "note", type: "string" },
  ],
});
await database.insertBatch("kept", { columns: { id: [1, 2], note: ["first", "second"] } });

const page = (ownerId: string, runId: string, pageIndex: number, fill: number) => ({
  ownerId,
  runId,
  pageIndex,
  bytes: new Uint8Array(16).fill(fill),
});
// An abandoned spill: its owner was never removed and expired at t0 + 60 s.
await store.createTempOwner({
  ownerId: "query-abandoned",
  createdAt: iso(t0),
  expiresAt: iso(t0 + 60_000),
  revision: 0,
});
await store.putTempRunPages([
  page("query-abandoned", "run-0", 0, 1),
  page("query-abandoned", "run-0", 1, 2),
]);
// The next spilling query, after that expiry: its create sweeps the abandoned owner's record.
await store.createTempOwner({
  ownerId: "query-live",
  createdAt: iso(t0 + 120_000),
  expiresAt: iso(t0 + 180_000),
  revision: 0,
});
await store.putTempRunPage(page("query-live", "run-0", 0, 3));
await database.insertBatch("kept", { columns: { id: [3], note: ["third"] } });

if (
  (await store.getTempOwner("query-abandoned")) !== undefined ||
  (await store.getTempRunPage("query-abandoned", "run-0", 0)) === undefined
) {
  throw new Error("This writer swept the owner's pages with it: run the script from core 0.19.x");
}
store._crashForTests();

const files: Record<string, string> = {};
for (const path of [
  `${prefix}/format.json`,
  `${prefix}/wal`,
  `${prefix}/wal-acknowledgements`,
  `${prefix}/checkpoint-a`,
  `${prefix}/checkpoint-b`,
  `${prefix}/extents/000000`,
  `${prefix}/temp/query-abandoned/run-0/0`,
  `${prefix}/temp/query-abandoned/run-0/1`,
  `${prefix}/temp/query-live/run-0/0`,
]) {
  const bytes = shim.readFileBytes(path);
  if (bytes === undefined) throw new Error(`The writer left no file at ${path}`);
  files[path] = Buffer.from(bytes).toString("base64");
}

const checkpoints = ["checkpoint-a", "checkpoint-b"].flatMap((slot) => {
  const bytes = shim.readFileBytes(`${prefix}/${slot}`);
  return bytes === undefined || bytes.byteLength === 0
    ? []
    : [
        decodeSyncCheckpoint(bytes) as {
          tempPages: Array<{ ownerId: string }>;
          core: { tempOwners: Array<{ ownerId: string }> };
        },
      ];
});
if (
  !checkpoints.some(
    (checkpoint) =>
      checkpoint.tempPages.some(({ ownerId }) => ownerId === "query-abandoned") &&
      !checkpoint.core.tempOwners.some(({ ownerId }) => ownerId === "query-abandoned"),
  )
) {
  throw new Error("No checkpoint captured the orphan pages");
}

const { version: writerPackageVersion } = JSON.parse(
  await readFile(new URL("../packages/core/package.json", import.meta.url), "utf8"),
) as { version: string };
const fixture = {
  layoutFormatVersion: OPFS_LAYOUT_VERSION,
  writerPackageVersion,
  files,
};
const target = fileURLToPath(
  new URL("../packages/core/format-fixtures/opfs-orphan-temp-pages.json", import.meta.url),
);
await writeFile(target, `${JSON.stringify(fixture, null, 2)}\n`);
console.log(`Wrote ${target}`);
