/** Built-package OPFS write benchmark worker; driven by scripts/write-browser-bench.mts. */
import {
  autocommit,
  batch,
  column,
  ints,
  texts,
  tableInput,
  rules,
  INT,
  TEXT,
} from "../dist/next/write/test-support.js";
import { WritePathImpl } from "../dist/next/write/engine.js";
import { Governor } from "../dist/next/memory/governor.js";
import { reachableExtents } from "../dist/next/format/reachable.js";
import { openExtentSpace } from "../dist/next/storage/space.js";
import { createOpfsBackend, webLocks, OPFS_FILE_NAMES } from "../dist/next/storage/opfs.js";
import type { Durability } from "../dist/next/access/write-path.js";

export interface WriteBenchInput {
  dir: string;
  durability: Durability;
  mode: "single" | "load";
  loadMode: "tx" | "commits";
  singles: number;
  rows: number;
}
const scope = self as unknown as DedicatedWorkerGlobalScope;
let input: WriteBenchInput;
let opened: Awaited<ReturnType<typeof open>>;
let parts: Array<ReturnType<typeof columns>> = [];
let tableId = 0;
const columns = (ids: number[]) =>
  batch(ints(ids), texts(ids.map((id) => `value ${String(id)}`)), ints(ids.map((id) => id % 1000)));
const batches = (count: number, size: number, start = 0) =>
  Array.from({ length: Math.ceil(count / size) }, (_, part) =>
    columns(
      Array.from(
        { length: Math.min(size, count - part * size) },
        (_, k) => start + part * size + k,
      ),
    ),
  );

async function open(config: WriteBenchInput) {
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle(config.dir, { create: true });
  const backend = createOpfsBackend({
    directory,
    name: config.dir,
    locks: webLocks(navigator.locks),
  });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  const governor = new Governor(256 * 1024 * 1024);
  const startup = await governor.admit({
    label: "startup",
    minimumBytes: 8 * 1024 * 1024,
    spillable: true,
  });
  const space = await openExtentSpace(session, {
    reachable: reachableExtents(),
    budget: startup,
    registerCache: (cache) => governor.registerCache(cache),
  });
  const path = await WritePathImpl.open({
    session,
    space,
    governor,
    options: { durability: config.durability },
  });
  return {
    path,
    async close() {
      await path.close();
      await space.close();
      startup.close();
      await session.close();
      await lease.release();
    },
  };
}

async function prepare(config: WriteBenchInput) {
  input = config;
  if ((globalThis as { __MINNOW_VERIFY_CACHED_BYTES__?: boolean }).__MINNOW_VERIFY_CACHED_BYTES__)
    throw new Error("Cached-byte test guard must be off in performance runs");
  opened = await open(config);
  const p = opened.path;
  await autocommit(p, async (statement) => {
    tableId =
      (
        await statement.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", TEXT), column("n", INT)]),
        })
      ).id ?? 0;
  });
  if (config.mode === "single") {
    for (const part of batches(100_000, 2048))
      await autocommit(p, (s) => s.table(tableId).insert(part, null));
  }
  await p.quiesce();
  parts = batches(
    config.mode === "single" ? config.singles : config.rows,
    config.mode === "single" ? 1 : 2048,
    config.mode === "single" ? 1_000_000 : 0,
  );
  return { ready: true };
}

async function run(profile: boolean) {
  const p = opened.path;
  if (profile) console.profile("write-bench");
  const start = performance.now();
  if (input.mode === "single") {
    for (const part of parts) {
      const tx = await p.begin();
      const s = tx.statement({ rules });
      await s.table(tableId).insert(part, null);
      await s.end();
      const receipt = await tx.commit(null);
      if (input.durability === "strict") await receipt.durable;
    }
    await p.flush();
  } else {
    const perCommit = input.loadMode === "tx" ? parts.length : 5;
    for (let from = 0; from < parts.length; from += perCommit) {
      const tx = await p.begin();
      const s = tx.statement({ rules });
      for (let k = from; k < Math.min(parts.length, from + perCommit); k++)
        await s.table(tableId).insert(parts[k] ?? columns([]), null);
      await s.end();
      await (
        await tx.commit(null)
      ).durable;
    }
    await p.quiesce();
  }
  const wallMs = performance.now() - start;
  if (profile) console.profileEnd("write-bench");
  return { wallMs, maintenance: p.maintenance, stats: p.stats() };
}

async function verify(config: WriteBenchInput) {
  const h = await open(config);
  try {
    const view = h.path.pin();
    try {
      const def = view.catalog.tables.find((t) => t.name === "t");
      if (def === undefined) throw new Error("Missing recovered table");
      if (def.primaryKey === null) throw new Error("Missing recovered primary key");
      const t = view.table(def.id, {
        budget: { allocate: () => true, free: () => undefined },
      });
      const expected = config.mode === "single" ? 100_000 + config.singles : config.rows;
      const cursor = view.index(def.id, def.primaryKey).range({ lower: null, upper: null }, "asc");
      let indexed = 0;
      try {
        const out = new Float64Array(2048);
        for (;;) {
          const count = await cursor.next(out);
          if (count === 0) break;
          indexed += count;
        }
      } finally {
        cursor.close();
      }
      if (t.liveRowCount !== expected || indexed !== expected)
        throw new Error(
          `Recovered rows ${String(t.liveRowCount)}, keys ${String(indexed)}, expected ${String(expected)}`,
        );
      const directory = await (
        await navigator.storage.getDirectory()
      ).getDirectoryHandle(config.dir);
      const fileBytes: Record<string, number> = {};
      for (const name of OPFS_FILE_NAMES)
        fileBytes[name] = (await (await directory.getFileHandle(name)).getFile()).size;
      return { recoveredRows: t.liveRowCount, recoveredKeys: indexed, fileBytes };
    } finally {
      view.release();
    }
  } finally {
    await h.close();
  }
}
scope.onmessage = (
  event: MessageEvent<{ id: number; op: string; input: WriteBenchInput; profile?: boolean }>,
) => {
  const request = event.data;
  void (async () => {
    switch (request.op) {
      case "prepare":
        return prepare(request.input);
      case "run":
        return run(request.profile === true);
      case "close":
        parts = [];
        await opened.close();
        return { closed: true };
      case "verify":
        return verify(request.input);
      case "remove":
        await (
          await navigator.storage.getDirectory()
        ).removeEntry(request.input.dir, { recursive: true });
        return { removed: true };
      default:
        throw new Error("Unknown benchmark request");
    }
  })().then(
    (value) => scope.postMessage({ id: request.id, value }),
    (error: unknown) =>
      scope.postMessage({
        id: request.id,
        error:
          error instanceof Error
            ? `${error.name}: ${error.message}\n${error.stack ?? ""}`
            : String(error),
      }),
  );
};
