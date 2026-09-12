/**
 * Shared scaffolding for the multi-tab coordination tests: a minimal table record, polling
 * helpers, a store opener with short RPC timeouts, outcome helpers that name an error's class,
 * and a shim patch that stretches `createSyncAccessHandle` into a macrotask gap.
 */
import type { TableRecord } from "../types.js";
import type { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore, type OpfsBlockStoreOptions } from "./store.js";

export function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function waitFor(
  condition: () => Promise<boolean> | boolean,
  what: string,
  attempts = 800,
): Promise<void> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (await condition()) return;
    await sleep(5);
  }
  throw new Error(`Timed out waiting for ${what}`);
}

export function opener(
  shim: MemoryOpfs,
  name: string,
  overrides: Partial<OpfsBlockStoreOptions> = {},
): () => Promise<OpfsBlockStore> {
  return () => OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 200, ...overrides });
}

/** "ok" or the error's class name, so a test can assert the exact outcome shape. */
export async function outcome(work: Promise<unknown>): Promise<string> {
  return work.then(
    () => "ok",
    (error: unknown) => (error instanceof Error ? error.name : String(error)),
  );
}

/** "ok" or `Name: message`, for assertions that a whole batch of writes succeeded. */
export async function outcomeMessage(work: Promise<unknown>): Promise<string> {
  return work.then(
    () => "ok",
    (error: unknown) =>
      error instanceof Error ? `${error.name}: ${error.message}` : String(error),
  );
}

/**
 * Patches the shim's file-handle prototype so opening a sync access handle on a matching file
 * waits a macrotask delay first. In a browser `createSyncAccessHandle` is real IPC, so a leader
 * that awaits it inside its operation queue is genuinely open to a message arriving in between;
 * the in-memory shim resolves in a microtask, which hides that window. Returns the restore.
 */
export async function delaySyncHandleOpens(
  shim: MemoryOpfs,
  matches: (name: string) => boolean,
  delayMs: number,
): Promise<() => void> {
  const probe = await shim.root.getFileHandle("__delay-probe__", { create: true });
  const prototype = Object.getPrototypeOf(probe) as {
    createSyncAccessHandle: (this: { name: string }) => Promise<unknown>;
  };
  const original = prototype.createSyncAccessHandle;
  prototype.createSyncAccessHandle = async function patched(this: { name: string }) {
    if (matches(this.name)) await sleep(delayMs);
    return original.call(this);
  };
  await shim.root.removeEntry("__delay-probe__");
  return () => {
    prototype.createSyncAccessHandle = original;
  };
}
