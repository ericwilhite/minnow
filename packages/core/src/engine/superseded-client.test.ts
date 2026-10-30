/**
 * What a page sees when a newer build in another tab takes its database over, through the real
 * worker host and client over an in-process boundary.
 *
 * The worker's store steps aside (it runs the previous OPFS layout here, as an older build's
 * would, so the newer build's open supersedes it), flushing what its buffered writers accepted
 * first, and tells the page. By default the page reloads once, after its calls in flight settle;
 * `whenSuperseded: "prompt"` only reports it. A page whose database a newer build already
 * upgraded reloads too, and a reload that keeps landing on the old build stops after once.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import { OPFS_LAYOUT_VERSION } from "../storage/opfs/upgrades.js";
import { StorageConnectionClosedError, StorageFormatVersionError } from "../storage/types.js";
import { MinnowDatabaseClient, type MinnowDatabaseClientOptions } from "./client.js";
import { createBoundary } from "./client-audit-harness.js";
import { MinnowDatabase } from "./database.js";
import {
  attachWorkerHost,
  singleStoreFactory,
  workerStoreHooks,
  type WorkerStoreFactory,
} from "./worker-server.js";

const OLDER = OPFS_LAYOUT_VERSION - 1;

/** A page: `location.reload`, session storage, and a document the client reports visibility to. */
function stubPage(): { reload: ReturnType<typeof vi.fn>; storage: Map<string, string> } {
  const reload = vi.fn();
  const storage = new Map<string, string>();
  vi.stubGlobal("location", { reload });
  vi.stubGlobal("document", { visibilityState: "visible", addEventListener: () => undefined });
  vi.stubGlobal("sessionStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => storage.set(key, value),
    removeItem: (key: string) => storage.delete(key),
  });
  return { reload, storage };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

/** The OPFS worker store on a shim, running `layout` as that build would. */
function opfsFactory(shim: MemoryOpfs, layout: number): WorkerStoreFactory {
  return singleStoreFactory("opfs", (descriptor, options) =>
    OpfsBlockStore.open({
      name: descriptor.name,
      root: shim.root,
      layoutVersion: layout,
      rpcTimeoutMs: 200,
      ...workerStoreHooks(options),
    }),
  );
}

function openClient(
  factory: WorkerStoreFactory,
  options: MinnowDatabaseClientOptions = {},
): MinnowDatabaseClient {
  const boundary = createBoundary();
  attachWorkerHost(boundary.workerSide, factory);
  return new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "opfs", name: "app" },
    ...options,
  });
}

/** The newer build's tab: opens the database at the current layout, superseding the rest. */
async function newerBuild(shim: MemoryOpfs): Promise<{ count: () => Promise<number> }> {
  const store = await OpfsBlockStore.open({
    name: "app",
    root: shim.root,
    dispatchBudgetMs: 2_000,
  });
  const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  return {
    count: async () =>
      Number((await database.query("SELECT COUNT(*) AS n FROM items")).rows[0]?.n ?? -1),
  };
}

const until = async (condition: () => boolean, what: string): Promise<void> => {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Timed out waiting for ${what}`);
};

describe("a page whose database a newer build takes over", () => {
  it("reloads once by default, after writing the rows its buffered writer accepted", async () => {
    const page = stubPage();
    const shim = new MemoryOpfs();
    const events: Array<{ reloading: boolean }> = [];
    const client = openClient(opfsFactory(shim, OLDER), {
      onSuperseded: (event) => events.push(event),
    });
    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    // Accepted, not yet written: the writer's own limits would hold them a minute longer.
    const writer = client.bufferedWriter("items", { maxRows: 1_000, maxAgeMs: 60_000 });
    for (let id = 1; id <= 25; id += 1) await writer.add({ id });

    const newer = await newerBuild(shim);
    await until(() => page.reload.mock.calls.length === 1, "the page to reload");
    expect(events).toEqual([{ reloading: true }]);
    expect(client.superseded).toBe(true);
    expect(await newer.count()).toBe(25);
    // Reloading again would be a loop: the page reloads once.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(page.reload).toHaveBeenCalledTimes(1);
  });

  it('only reports it with whenSuperseded: "prompt", and every call says why', async () => {
    const page = stubPage();
    const shim = new MemoryOpfs();
    const events: Array<{ reloading: boolean }> = [];
    const client = openClient(opfsFactory(shim, OLDER), {
      whenSuperseded: "prompt",
      onSuperseded: (event) => events.push(event),
    });
    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO items VALUES (1)");
    expect(client.superseded).toBe(false);

    const newer = await newerBuild(shim);
    await until(() => client.superseded, "the client to learn it");
    expect(events).toEqual([{ reloading: false }]);
    const failure = await client.query("SELECT * FROM items").catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(StorageConnectionClosedError);
    expect(failure).toMatchObject({ reason: "superseded" });
    expect(page.reload).not.toHaveBeenCalled();
    expect(await newer.count()).toBe(1);
  });

  it("reloads a page whose database a newer build already upgraded, once", async () => {
    const page = stubPage();
    const shim = new MemoryOpfs();
    // The newer build upgraded the database while this page, an old build, was closed.
    const newer = await OpfsBlockStore.open({ name: "app", root: shim.root });
    newer.close();
    const failures: unknown[] = [];
    const first = openClient(opfsFactory(shim, OLDER));
    await first.ready().catch((error: unknown) => failures.push(error));
    expect(failures[0]).toBeInstanceOf(StorageFormatVersionError);
    expect(failures[0]).toMatchObject({ relation: "newer" });
    await until(() => page.reload.mock.calls.length === 1, "the page to reload");
    // The reload served the old build again (a stale cache): no second reload.
    const second = openClient(opfsFactory(shim, OLDER));
    await second.ready().catch(() => undefined);
    expect(second.superseded).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(page.reload).toHaveBeenCalledTimes(1);
    // The newer build arrives: it opens, which clears the guard for any later supersession.
    const current = openClient(opfsFactory(shim, OPFS_LAYOUT_VERSION));
    await current.ready();
    expect(page.storage.size).toBe(0);
  });

  it("reloads nothing outside a page", async () => {
    const shim = new MemoryOpfs();
    const events: Array<{ reloading: boolean }> = [];
    const client = openClient(opfsFactory(shim, OLDER), {
      onSuperseded: (event) => events.push(event),
    });
    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    await newerBuild(shim);
    await until(() => client.superseded, "the client to learn it");
    expect(events).toEqual([{ reloading: false }]);
  });
});
