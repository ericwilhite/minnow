// Page side of index-takeover.spec.ts: one engine per tab, in its own dedicated worker.
import { rehydrateError, type SerializedError } from "@minnowdb/core/worker-protocol";

interface Reply {
  id?: number;
  result?: unknown;
  error?: SerializedError;
  parked?: true;
}

let worker: Worker | undefined;
let next = 0;
const waiting = new Map<
  number,
  { resolve: (value: unknown) => void; reject: (error: Error) => void }
>();
let parked: Promise<void> | undefined;

function call(message: Record<string, unknown>): Promise<unknown> {
  if (worker === undefined) throw new Error("Open the tab's database first");
  const id = next++;
  const reply = new Promise<unknown>((resolve, reject) => waiting.set(id, { resolve, reject }));
  worker.postMessage({ ...message, id });
  return reply;
}

/** Opens this tab's engine. With `park`, its index builds stop inside their first chunk write. */
export async function openTab(kind: "indexeddb" | "opfs", name: string, park: boolean) {
  worker = new Worker(new URL("./index-takeover-worker.ts", import.meta.url), { type: "module" });
  parked = new Promise<void>((resolve) => {
    worker?.addEventListener("message", (event: MessageEvent<Reply>) => {
      if (event.data.parked === true) resolve();
    });
  });
  worker.addEventListener("message", (event: MessageEvent<Reply>) => {
    const { id, result, error } = event.data;
    if (id === undefined) return;
    const pending = waiting.get(id);
    waiting.delete(id);
    if (error !== undefined) pending?.reject(rehydrateError(error, new Map()));
    else pending?.resolve(result);
  });
  await call({ op: "open", kind, name, park });
}

/** Runs a statement; resolves with its duration, or with the error it failed with. */
export async function execute(
  sql: string,
  params?: unknown[],
): Promise<{ ms?: number; error?: string }> {
  try {
    return (await call({ op: "execute", sql, ...(params === undefined ? {} : { params }) })) as {
      ms: number;
    };
  } catch (error) {
    return { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) };
  }
}

const background = new Map<string, Promise<{ ms?: number; error?: string }>>();

/** Starts a statement without waiting for it; `settled` reports on it later. */
export function start(label: string, sql: string): void {
  background.set(label, execute(sql));
}

export async function settled(
  label: string,
  withinMs: number,
): Promise<{ ms?: number; error?: string } | "pending"> {
  const pending = background.get(label);
  if (pending === undefined) throw new Error(`Nothing started as ${label}`);
  return Promise.race([
    pending,
    new Promise<"pending">((resolve) => setTimeout(() => resolve("pending"), withinMs)),
  ]);
}

/** Resolves once this tab's index build has parked mid-build. */
export async function untilParked(): Promise<void> {
  if (parked === undefined) throw new Error("Open the tab's database first");
  await parked;
}
