import { DatabaseStoreUnavailableError } from "./errors.js";

/**
 * The `{ kind: "auto" }` store: OPFS where this context can hold synchronous access handles,
 * IndexedDB where it cannot (Safari's private browsing, a page context, an older build).
 *
 * The choice is made once per database name and remembered in a small IndexedDB record, so a
 * database created on one store is never silently reopened, empty, on the other: a remembered
 * store that cannot open now is reported as `DatabaseStoreUnavailableError` instead. Delete the
 * record with `forgetStoreChoice` when the database itself is deleted.
 */

export type AutoStoreKind = "opfs" | "indexeddb";

const CHOICE_DATABASE = "minnowdb-store-choice";
const CHOICE_STORE = "choices";

/** Test seams: the OPFS probe and the IndexedDB factory that keeps the choices. */
export const autoStoreTestHooks: {
  opfsAvailable?: () => Promise<boolean>;
  indexedDB?: IDBFactory;
} = {};

/**
 * Whether OPFS is usable here: the directory handle resolves, and file handles expose
 * synchronous access — which only a dedicated worker has, and Safari's private browsing never.
 */
export async function opfsAvailable(): Promise<boolean> {
  if (autoStoreTestHooks.opfsAvailable !== undefined) return autoStoreTestHooks.opfsAvailable();
  const storage = (
    globalThis as { navigator?: { storage?: { getDirectory?: () => Promise<unknown> } } }
  ).navigator?.storage;
  if (typeof storage?.getDirectory !== "function") return false;
  const fileHandle = (globalThis as { FileSystemFileHandle?: { prototype?: object } })
    .FileSystemFileHandle;
  if (
    fileHandle?.prototype === undefined ||
    typeof (fileHandle.prototype as { createSyncAccessHandle?: unknown }).createSyncAccessHandle !==
      "function"
  ) {
    return false;
  }
  try {
    await storage.getDirectory();
    return true;
  } catch {
    return false;
  }
}

/**
 * Resolves `auto` for one database name: the remembered store, or the best one available.
 * A fresh choice is reserved with an insert-only write, so two workers deciding at the same
 * instant in contexts that disagree still end up on the one store the record names; the
 * caller reports through `settleAutoStoreChoice` whether that reservation produced a database.
 */
export async function resolveAutoStoreKind(
  name: string,
): Promise<{ kind: AutoStoreKind; reserved: boolean }> {
  const remembered = await readChoice(name);
  if (remembered === "indexeddb") return { kind: "indexeddb", reserved: false };
  if (remembered === "opfs") {
    if (await opfsAvailable()) return { kind: "opfs", reserved: false };
    throw new DatabaseStoreUnavailableError(
      "opfs",
      name,
      `Database "${name}" lives on the OPFS store, which this context cannot open; it is not ` +
        "reopened on IndexedDB, where it would be empty",
    );
  }
  const kind: AutoStoreKind = (await opfsAvailable()) ? "opfs" : "indexeddb";
  const reserved = await reserveChoice(name, kind);
  if (reserved) return { kind, reserved: true };
  // Another connection reserved the name first; its choice stands.
  return resolveAutoStoreKind(name);
}

/**
 * Opens the store `auto` resolves to. A reservation that produced no database is released
 * again, so a first open that failed does not pin the name to a store that never held data.
 */
export async function openAutoStore<Store>(
  name: string,
  open: (kind: AutoStoreKind) => Promise<Store>,
): Promise<{ store: Store; kind: AutoStoreKind }> {
  const { kind, reserved } = await resolveAutoStoreKind(name);
  try {
    return { store: await open(kind), kind };
  } catch (error) {
    if (reserved) await forgetStoreChoice(name).catch(() => undefined);
    throw error;
  }
}

/** Forgets which store `auto` chose for a database; call it when deleting the database. */
export async function forgetStoreChoice(name: string): Promise<void> {
  const database = await openChoices();
  if (database === undefined) return;
  try {
    await complete(
      database.transaction(CHOICE_STORE, "readwrite").objectStore(CHOICE_STORE).delete(name),
    );
  } finally {
    database.close();
  }
}

function choiceFactory(): IDBFactory | undefined {
  return (
    autoStoreTestHooks.indexedDB ??
    (globalThis as { indexedDB?: IDBFactory }).indexedDB ??
    undefined
  );
}

async function openChoices(): Promise<IDBDatabase | undefined> {
  const factory = choiceFactory();
  if (factory === undefined) return undefined;
  const request = factory.open(CHOICE_DATABASE, 1);
  request.addEventListener("upgradeneeded", () => {
    if (!request.result.objectStoreNames.contains(CHOICE_STORE)) {
      request.result.createObjectStore(CHOICE_STORE);
    }
  });
  try {
    return await complete(request);
  } catch {
    // A context that refuses IndexedDB entirely gets no memory of the choice, and probes.
    return undefined;
  }
}

async function readChoice(name: string): Promise<AutoStoreKind | undefined> {
  const database = await openChoices();
  if (database === undefined) return undefined;
  try {
    const value: unknown = await complete(
      database.transaction(CHOICE_STORE, "readonly").objectStore(CHOICE_STORE).get(name),
    );
    const kind =
      typeof value === "object" && value !== null ? (value as { kind?: unknown }).kind : undefined;
    return kind === "opfs" || kind === "indexeddb" ? kind : undefined;
  } finally {
    database.close();
  }
}

/** Records the choice unless one exists; false means another connection got there first. */
async function reserveChoice(name: string, kind: AutoStoreKind): Promise<boolean> {
  const database = await openChoices();
  if (database === undefined) return true;
  try {
    await complete(
      database.transaction(CHOICE_STORE, "readwrite").objectStore(CHOICE_STORE).add({ kind }, name),
    );
    return true;
  } catch (error) {
    if (error instanceof Error && error.name === "ConstraintError") return false;
    throw error;
  } finally {
    database.close();
  }
}

function complete<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => resolve(request.result));
    request.addEventListener("error", () =>
      reject(request.error ?? new Error("IndexedDB request failed")),
    );
  });
}
