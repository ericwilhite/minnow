import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { OPFS_LAYOUT_VERSION } from "../dist/storage/opfs/upgrades.js";

type Kind = "indexeddb" | "opfs";
const EVENT_KEY = "supersede-run:event";

/**
 * A tab on the current build: a real worker, the default reload policy, and rows a buffered
 * writer accepted but has not written — its limits would hold them for ten minutes.
 */
export async function openOlderTab(kind: Kind, name: string): Promise<void> {
  const worker = new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  const client = new MinnowDatabaseClient(worker, {
    store: { kind, name },
    databaseOptions: { autoCompact: false, autoCollect: false },
    onSuperseded: (event) => sessionStorage.setItem(EVENT_KEY, JSON.stringify(event)),
  });
  await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
  const writer = client.bufferedWriter("items", { maxRows: 10_000, maxAgeMs: 600_000 });
  for (let id = 1; id <= 25; id += 1) await writer.add({ id });
  (globalThis as { supersedeRunClient?: MinnowDatabaseClient }).supersedeRunClient = client;
}

/**
 * What a newer build's tab does to the database. OPFS: it asks the older layout to step aside,
 * as its open does while it waits to upgrade. IndexedDB: it opens the next schema version, which
 * the browser holds until every older connection closes.
 */
export async function actAsNewerBuild(kind: Kind, name: string): Promise<{ upgraded: boolean }> {
  if (kind === "opfs") {
    const channel = new BroadcastChannel(`minnowdb-store:${name}`);
    channel.postMessage({ kind: "upgrade", layout: OPFS_LAYOUT_VERSION + 1 });
    channel.close();
    return { upgraded: true };
  }
  const current = await new Promise<number>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => {
      const { version } = request.result;
      request.result.close();
      resolve(version);
    };
    request.onerror = () => reject(request.error ?? new Error("open failed"));
  });
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, current + 1);
    request.onupgradeneeded = () => undefined;
    request.onsuccess = () => {
      request.result.close();
      resolve({ upgraded: true });
    };
    request.onerror = () => reject(request.error ?? new Error("upgrade failed"));
  });
}

/**
 * The reloaded page: what the client told the application before reloading, whether it recorded
 * the reload (its guard against a loop), and — for OPFS, whose layout the stand-in newer build
 * did not change — the rows the older tab's writer had accepted, read back by this build.
 */
export async function afterReload(
  kind: Kind,
  name: string,
): Promise<{ event: string | null; guarded: boolean; rows: number | null }> {
  const event = sessionStorage.getItem(EVENT_KEY);
  const guarded = sessionStorage.getItem(`minnowdb:superseded-reload:${name}`) !== null;
  if (kind !== "opfs") return { event, guarded, rows: null };
  const worker = new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  const client = new MinnowDatabaseClient(worker, { store: { kind, name } });
  try {
    const result = await client.query("SELECT COUNT(*) AS n FROM items");
    return { event, guarded, rows: Number(result.rows[0]?.n) };
  } finally {
    await client.close({ terminateWorker: true });
  }
}
