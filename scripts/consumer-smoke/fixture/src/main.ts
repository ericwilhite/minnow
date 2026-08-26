import { column, schema, table, type QueryResult } from "@minnowdb/core";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { deleteOpfsDatabase } from "@minnowdb/core/storage/opfs";
import { readSnapshotSummary } from "@minnowdb/core/storage/snapshots";
import { mountMinnowDevtools } from "@minnowdb/devtools";
import { streamCsv, streamNdjson } from "@minnowdb/export";
import { createKysely } from "@minnowdb/kysely";
import { useLiveQuery } from "@minnowdb/react";
import { createElement } from "react";
import { createRoot } from "react-dom/client";

const items = table("items", {
  id: column.integer().unique(),
  name: column.string(),
  score: column.number(),
});
const appSchema = schema([items]);

interface SmokeResult {
  migrationTables: string[];
  workerRows: Array<{ id: number; name: string; score: number }>;
  kyselyScore: number;
  liveRows: number;
  csv: string;
  ndjsonLines: number;
  snapshotTables: number;
  restoredRows: number;
  reopenedRows: number;
  reactText: string;
  devtoolsMounted: boolean;
}

declare global {
  interface Window {
    runConsumerSmoke(): Promise<SmokeResult>;
  }
}

function createClient(kind: "indexeddb" | "opfs", name: string): MinnowDatabaseClient {
  return new MinnowDatabaseClient(
    new Worker(new URL("./db-worker.ts", import.meta.url), { type: "module" }),
    { store: { kind, name } },
  );
}

async function waitFor<T>(read: () => T | undefined, label: string): Promise<T> {
  const deadline = performance.now() + 10_000;
  for (;;) {
    const value = read();
    if (value !== undefined) return value;
    if (performance.now() >= deadline) throw new Error(`Timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

async function deleteIndexedDb(name: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);
    request.addEventListener("success", () => resolve());
    request.addEventListener("error", () => reject(request.error ?? new Error("delete failed")));
    request.addEventListener("blocked", () =>
      reject(new Error(`IndexedDB deletion blocked: ${name}`)),
    );
  });
}

async function streamText(stream: ReadableStream<Uint8Array>): Promise<string> {
  return new Response(stream).text();
}

async function reactProbe(rowCount: number): Promise<string> {
  const node = document.createElement("div");
  document.body.append(node);
  const snapshot = { status: "ready" as const, rows: Array.from({ length: rowCount }) };
  const store = {
    subscribe: () => () => undefined,
    getSnapshot: () => snapshot,
    getServerSnapshot: () => snapshot,
  };
  function Probe() {
    const current = useLiveQuery(store);
    return createElement("span", null, `${String(current.rows.length)} rows`);
  }
  const root = createRoot(node);
  try {
    root.render(createElement(Probe));
    return await waitFor(
      () => (node.textContent === `${String(rowCount)} rows` ? node.textContent : undefined),
      "the React adapter",
    );
  } finally {
    root.unmount();
    node.remove();
  }
}

async function runConsumerSmoke(): Promise<SmokeResult> {
  const identity = crypto.randomUUID();
  const indexedDbName = `minnow-consumer-idb-${identity}`;
  const opfsName = `minnow-consumer-opfs-${identity}`;
  let indexedDbClient: MinnowDatabaseClient | undefined;
  let restoredClient: MinnowDatabaseClient | undefined;
  let reopenedClient: MinnowDatabaseClient | undefined;

  try {
    indexedDbClient = createClient("indexeddb", indexedDbName);
    await indexedDbClient.ready();
    const migration = await indexedDbClient.migrate(appSchema);
    await indexedDbClient.execute(
      "INSERT INTO items (id, name, score) VALUES ($1, $2, $3), ($4, $5, $6)",
      [1, "Ada", 10, 2, "Grace", 20],
    );

    const first = await indexedDbClient.query("SELECT id, name, score FROM items ORDER BY id");
    const kysely = createKysely({ driver: indexedDbClient, schema: appSchema });
    const kyselyRows = await kysely
      .selectFrom("items")
      .select(["id", "name", "score"])
      .orderBy("id")
      .execute();
    await kysely.destroy();

    const observations: QueryResult[] = [];
    let liveError: unknown;
    const live = indexedDbClient.liveQueries({ channelName: `${indexedDbName}-commits` });
    const subscription = await live.subscribe("SELECT id, name, score FROM items ORDER BY id", {
      onChange: (result) => observations.push(result),
      onError: (error) => {
        liveError = error;
      },
    });
    await waitFor(
      () => observations.find((result) => result.rows.length === 2),
      "initial live rows",
    );
    await indexedDbClient.execute("INSERT INTO items (id, name, score) VALUES ($1, $2, $3)", [
      3,
      "Linus",
      30,
    ]);
    await live.refresh();
    const liveResult = await waitFor(() => {
      if (liveError !== undefined) throw liveError;
      return observations.find((result) => result.rows.length === 3);
    }, "updated live rows");
    await subscription.close();
    await live.close();

    const csv = await streamText(
      streamCsv(indexedDbClient, "SELECT id, name, score FROM items ORDER BY id"),
    );
    const ndjson = await streamText(
      streamNdjson(indexedDbClient, "SELECT id, name, score FROM items ORDER BY id"),
    );

    const mounted = mountMinnowDevtools(indexedDbClient, { mode: "inline" });
    const devtoolsMounted = mounted.element.shadowRoot !== null;
    mounted.destroy();
    const reactText = await reactProbe(liveResult.rows.length);

    const snapshot = await indexedDbClient.exportSnapshot();
    const summary = await readSnapshotSummary(snapshot);
    await indexedDbClient.close({ terminateWorker: true });
    indexedDbClient = undefined;

    restoredClient = createClient("opfs", opfsName);
    await restoredClient.ready();
    await restoredClient.importSnapshot(snapshot);
    const restored = await restoredClient.query("SELECT id FROM items ORDER BY id");
    await restoredClient.close({ terminateWorker: true });
    restoredClient = undefined;

    reopenedClient = createClient("opfs", opfsName);
    await reopenedClient.ready();
    const reopened = await reopenedClient.query("SELECT id FROM items ORDER BY id");
    await reopenedClient.close({ terminateWorker: true });
    reopenedClient = undefined;

    return {
      migrationTables: migration.createdTables,
      workerRows: first.rows as Array<{ id: number; name: string; score: number }>,
      kyselyScore: kyselyRows.reduce((total, row) => total + row.score, 0),
      liveRows: liveResult.rows.length,
      csv,
      ndjsonLines: ndjson.trim().split("\n").length,
      snapshotTables: summary.tableCount,
      restoredRows: restored.rows.length,
      reopenedRows: reopened.rows.length,
      reactText,
      devtoolsMounted,
    };
  } finally {
    await reopenedClient?.close({ terminateWorker: true }).catch(() => undefined);
    await restoredClient?.close({ terminateWorker: true }).catch(() => undefined);
    await indexedDbClient?.close({ terminateWorker: true }).catch(() => undefined);
    await deleteOpfsDatabase({ name: opfsName });
    await deleteIndexedDb(indexedDbName);
  }
}

window.runConsumerSmoke = runConsumerSmoke;
const status = document.querySelector("#status");
if (status !== null) status.textContent = "ready";
