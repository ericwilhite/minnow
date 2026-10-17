/**
 * The format marker is judged by every opener, and written by two kinds of writer: the opener
 * that creates a database, and the leader that rewrites an older layout's marker. Each write is
 * several OPFS steps — create the file, take its handle, truncate, write, flush, close — so an
 * opener that looked in between saw an empty or locked marker. If the writer then finished and
 * went on to create the WAL before that opener scanned the directory, the opener refused a
 * healthy database as corrupt: "The OPFS format marker is torn, but the database directory
 * already contains storage artifacts (wal)". Worse, an opener that saw the empty marker before
 * the WAL existed wrote the marker again, truncating a published marker beside a live WAL —
 * a power loss in that window leaves exactly the torn-marker-beside-artifacts state that every
 * later open refuses.
 *
 * Each test holds one opener at a step of a marker write, lets the other run into that window,
 * and checks that judging and writing the marker are atomic from the other opener's side.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import type { TableRecord } from "../types.js";
import { OpfsBlockStore } from "./index.js";
import { OPFS_LAYOUT_VERSION } from "./upgrades.js";

interface Gates {
  /** After `getFileHandle` resolves, before the caller sees the handle. */
  afterGetFileHandle?: (path: string, create: boolean) => Promise<void> | void;
  /** When `getFile` refuses, as it does for a file another handle holds. */
  onGetFileError?: (path: string, error: unknown) => void;
  /** After `getFile` resolves, before the caller reads it. */
  afterGetFile?: (path: string, size: number) => Promise<void> | void;
  /** After `createSyncAccessHandle` resolves: the file is held, nothing written yet. */
  afterSyncHandle?: (path: string) => Promise<void> | void;
  /** Before a directory's names are listed. */
  beforeKeys?: (path: string) => Promise<void> | void;
}

/**
 * The shim's tree as one opener sees it, with a pause point at each step a marker write or a
 * marker judgement takes. Every opener gets its own view of the same files.
 */
function gatedDirectory(
  inner: FileSystemDirectoryHandle,
  gates: Gates,
  segments: readonly string[] = [],
): FileSystemDirectoryHandle {
  const pathOf = (name: string) => [...segments, name].join("/");
  const wrap = (entry: FileSystemHandle, name: string): FileSystemHandle =>
    entry.kind === "file"
      ? gatedFile(entry as FileSystemFileHandle, gates, pathOf(name))
      : gatedDirectory(entry as FileSystemDirectoryHandle, gates, [...segments, name]);
  const directory = {
    kind: "directory" as const,
    name: inner.name,
    async getDirectoryHandle(name: string, options?: { create?: boolean }) {
      return gatedDirectory(await inner.getDirectoryHandle(name, options), gates, [
        ...segments,
        name,
      ]);
    },
    async getFileHandle(name: string, options?: { create?: boolean }) {
      const handle = await inner.getFileHandle(name, options);
      await gates.afterGetFileHandle?.(pathOf(name), options?.create === true);
      return gatedFile(handle, gates, pathOf(name));
    },
    removeEntry(name: string, options?: { recursive?: boolean }) {
      return inner.removeEntry(name, options);
    },
    async *keys(): AsyncGenerator<string> {
      await gates.beforeKeys?.(segments.join("/"));
      for await (const name of inner.keys()) yield name;
    },
    async *entries(): AsyncGenerator<[string, FileSystemHandle]> {
      for await (const [name, entry] of inner.entries()) yield [name, wrap(entry, name)];
    },
    [Symbol.asyncIterator]() {
      return directory.entries();
    },
  };
  return directory as unknown as FileSystemDirectoryHandle;
}

function gatedFile(inner: FileSystemFileHandle, gates: Gates, path: string): FileSystemFileHandle {
  return {
    kind: "file",
    name: inner.name,
    async getFile() {
      let file: File;
      try {
        file = await inner.getFile();
      } catch (error) {
        gates.onGetFileError?.(path, error);
        throw error;
      }
      await gates.afterGetFile?.(path, file.size);
      return file;
    },
    async createSyncAccessHandle() {
      const handle = await inner.createSyncAccessHandle();
      await gates.afterSyncHandle?.(path);
      return handle;
    },
  } as unknown as FileSystemFileHandle;
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function until(condition: () => boolean | Promise<boolean>): Promise<void> {
  for (let attempt = 0; attempt < 2_000; attempt += 1) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("Condition never held");
}

/**
 * Whether some opener of this database is queued behind a Web Lock another holds. A paused
 * writer waits for the other opener either to run into its window or to be held off by it.
 */
async function lockQueued(name: string): Promise<boolean> {
  const snapshot = await navigator.locks.query();
  return (snapshot.pending ?? []).some((lock) => lock.name?.endsWith(`:${name}`) === true);
}

function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-10-17T00:00:00.000Z",
  };
}

const CURRENT_MARKER = JSON.stringify({ formatVersion: OPFS_LAYOUT_VERSION });
let databases = 0;
/** Web Locks are per process, and these tests park openers on them: no name is ever reused. */
function freshName(label: string): string {
  databases += 1;
  return `marker-race-${label}-${String(process.pid)}-${String(databases)}`;
}

function markerText(shim: MemoryOpfs, name: string): string | undefined {
  const bytes = shim.readFileBytes(`minnowdb/${name}/format.json`);
  return bytes === undefined ? undefined : new TextDecoder().decode(bytes);
}

// Node 22 has no Web Locks; the browser suite races real workers over the same paths.
describe.skipIf(
  (globalThis as { navigator?: { locks?: LockManager } }).navigator?.locks === undefined,
)("format marker races between openers", () => {
  it("an opener that reads a marker mid-creation waits for it instead of refusing the database", async () => {
    const shim = new MemoryOpfs();
    const name = freshName("create-read");
    const markerPath = `minnowdb/${name}/format.json`;
    let creatorPaused = false;
    let observerReadMarker = false;
    const creatorOpened = deferred();

    // The creator stops with format.json created and empty, before it takes the file's handle.
    const creatorRoot = gatedDirectory(shim.root, {
      async afterGetFileHandle(path, create) {
        if (!create || path !== markerPath || creatorPaused) return;
        creatorPaused = true;
        await until(async () => observerReadMarker || (await lockQueued(name)));
      },
    });
    // The other opener reads the marker inside that window, then lists the directory only
    // after the creator has finished opening — by which time the WAL exists.
    const observerRoot = gatedDirectory(shim.root, {
      afterGetFile(path) {
        if (path === markerPath) observerReadMarker = true;
      },
      async beforeKeys(path) {
        if (path === `minnowdb/${name}`) await creatorOpened.promise;
      },
    });

    const creating = OpfsBlockStore.open({ name, root: creatorRoot });
    await until(() => creatorPaused);
    const observing = OpfsBlockStore.open({ name, root: observerRoot });
    const creator = await creating;
    creatorOpened.resolve();
    const observer = await observing;
    try {
      expect(markerText(shim, name)).toBe(CURRENT_MARKER);
      await observer.addTable(table("from-observer"));
      expect((await creator.listTables()).map(({ name: table }) => table)).toEqual([
        "from-observer",
      ]);
    } finally {
      observer._crashForTests();
      creator._crashForTests();
    }
  });

  it("never rewrites a marker another opener already published beside a live WAL", async () => {
    const shim = new MemoryOpfs();
    const name = freshName("create-twice");
    const markerPath = `minnowdb/${name}/format.json`;
    const walPath = `minnowdb/${name}/wal`;
    const tornBesideWal: string[] = [];
    shim.setWriteFault((path, phase) => {
      // A truncate or write of the marker while a WAL exists is a window in which the
      // database, as stored, is a torn marker beside artifacts: refused after power loss.
      if (path === markerPath && phase === "write" && shim.readFileBytes(walPath) !== undefined) {
        tornBesideWal.push(phase);
      }
    });
    let firstPaused = false;
    let secondOpened = false;

    // The first opener stops with format.json created and empty, before it takes the handle;
    // the second runs into that window and, unless it is held off, opens the database fully.
    const firstRoot = gatedDirectory(shim.root, {
      async afterGetFileHandle(path, create) {
        if (!create || path !== markerPath || firstPaused) return;
        firstPaused = true;
        await until(async () => secondOpened || (await lockQueued(name)));
      },
    });
    const opening = OpfsBlockStore.open({ name, root: firstRoot });
    await until(() => firstPaused);
    const secondOpening = OpfsBlockStore.open({ name, root: shim.root }).then((store) => {
      secondOpened = true;
      return store;
    });
    const first = await opening;
    const second = await secondOpening;
    try {
      expect(tornBesideWal).toEqual([]);
      expect(markerText(shim, name)).toBe(CURRENT_MARKER);
      await first.addTable(table("from-first"));
      expect((await second.listTables()).map(({ name: table }) => table)).toEqual(["from-first"]);
    } finally {
      shim.setWriteFault(null);
      second._crashForTests();
      first._crashForTests();
    }
  });

  it("a creator that dies mid-write leaves the marker to the next opener's repair", async () => {
    const shim = new MemoryOpfs();
    const name = freshName("create-dies");
    const markerPath = `minnowdb/${name}/format.json`;
    const death = new DOMException("the tab died mid-write", "AbortError");
    shim.setWriteFault((path, phase) => {
      if (path === markerPath && phase === "write") throw death;
    });
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toBe(death);
    shim.setWriteFault(null);
    // The creator died holding the marker lock with format.json created and empty, before any
    // WAL. Its lock died with it: the next opener is not held off, and repairs the marker as
    // it would any torn first open's.
    expect(markerText(shim, name)).toBe("");
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    try {
      expect(markerText(shim, name)).toBe(CURRENT_MARKER);
      await store.addTable(table("after-repair"));
    } finally {
      store._crashForTests();
    }
  });

  it("fails rather than hangs behind a connection that stalls holding the marker", async () => {
    const shim = new MemoryOpfs();
    const name = freshName("stalled");
    let release!: () => void;
    let held!: () => void;
    const holding = new Promise<void>((resolve) => {
      held = resolve;
    });
    // A connection frozen part-way through writing the marker of a database it is creating.
    const stalled = navigator.locks.request(
      `minnowdb-opfs-marker:${name}`,
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
          held();
        }),
    );
    await holding;
    await expect(
      OpfsBlockStore.open({ name, root: shim.root, dispatchBudgetMs: 50 }),
    ).rejects.toThrow(/holding the OPFS format marker/);
    expect(markerText(shim, name)).toBeUndefined();
    release();
    await stalled;
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    try {
      expect(markerText(shim, name)).toBe(CURRENT_MARKER);
    } finally {
      store._crashForTests();
    }
  });

  it("an opener that reads a marker mid-upgrade waits for the rewrite instead of refusing", async () => {
    const shim = new MemoryOpfs();
    const name = freshName("upgrade-read");
    const markerPath = `minnowdb/${name}/format.json`;
    const seeded = await OpfsBlockStore.open({ name, root: shim.root });
    await seeded.addTable(table("kept"));
    seeded._crashForTests();
    // Layouts 7 to 11 store this layout's bytes; the upgrade rewrites only the marker, with a
    // witness written before and removed after.
    shim.writeFileBytes(markerPath, new TextEncoder().encode('{"formatVersion":11}'));

    let upgraderPaused = false;
    let readerFoundMarkerHeld = false;
    const upgraderOpened = deferred();
    // The upgrading leader stops holding the marker's handle, mid-rewrite.
    const upgraderRoot = gatedDirectory(shim.root, {
      async afterSyncHandle(path) {
        if (path !== markerPath || upgraderPaused) return;
        upgraderPaused = true;
        await until(async () => readerFoundMarkerHeld || (await lockQueued(name)));
      },
    });
    // The reader finds the marker held, then looks for the rewrite's witness only once the
    // rewrite has finished and removed it.
    const readerRoot = gatedDirectory(shim.root, {
      onGetFileError(path) {
        if (path === markerPath) readerFoundMarkerHeld = true;
      },
      async afterGetFileHandle(path) {
        if (readerFoundMarkerHeld && path.startsWith(`minnowdb/${name}/upgrade-`)) {
          await upgraderOpened.promise;
        }
      },
    });

    const upgrading = OpfsBlockStore.open({ name, root: upgraderRoot });
    await until(() => upgraderPaused);
    const reading = OpfsBlockStore.open({ name, root: readerRoot });
    const upgrader = await upgrading;
    upgraderOpened.resolve();
    const reader = await reading;
    try {
      expect(markerText(shim, name)).toBe(CURRENT_MARKER);
      expect((await reader.listTables()).map(({ name: table }) => table)).toEqual(["kept"]);
      await reader.addTable(table("after-upgrade"));
      expect((await upgrader.listTables()).map(({ name: table }) => table)).toEqual([
        "after-upgrade",
        "kept",
      ]);
    } finally {
      reader._crashForTests();
      upgrader._crashForTests();
    }
  });
});
