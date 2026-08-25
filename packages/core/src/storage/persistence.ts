/** How an application wants the browser to treat origin-level eviction protection. */
export type OriginPersistencePolicy = "best-effort" | "request" | "required";

export type OriginPersistenceReason =
  "already-persisted" | "granted" | "denied" | "unsupported" | "unavailable";

export interface OriginPersistenceStatus {
  /** The policy that was evaluated. */
  policy: OriginPersistencePolicy;
  /** Whether the browser currently reports this origin as protected from automatic eviction. */
  persisted: boolean;
  /** Whether this call asked the browser to grant persistence. */
  requested: boolean;
  /** Why `persisted` has its current value. */
  reason: OriginPersistenceReason;
}

/**
 * The browser declined or could not provide eviction protection required by the application.
 *
 * This is separate from adapter durability. Strict IndexedDB/OPFS commits protect against torn
 * or reordered writes; only the browser can protect an entire origin from quota eviction.
 */
export class OriginPersistenceRequiredError extends Error {
  override readonly name = "OriginPersistenceRequiredError";
}

interface PersistenceStorageManager {
  persisted?: () => Promise<boolean>;
  persist?: () => Promise<boolean>;
}

function defaultStorageManager(): PersistenceStorageManager | undefined {
  if (typeof navigator === "undefined") return undefined;
  return navigator.storage;
}

/**
 * Checks or requests browser-managed protection from whole-origin eviction.
 *
 * Call this from a user gesture before opening a durable store when losing the entire origin is
 * unacceptable. `best-effort` only inspects existing protection, `request` asks when possible,
 * and `required` rejects unless protection is already present or is granted now.
 *
 * The optional storage manager exists for deterministic tests and non-browser wrappers.
 */
export async function ensureOriginPersistence(
  policy: OriginPersistencePolicy = "request",
  storageManager: PersistenceStorageManager | undefined = defaultStorageManager(),
): Promise<OriginPersistenceStatus> {
  const runtimePolicy: unknown = policy;
  if (
    runtimePolicy !== "best-effort" &&
    runtimePolicy !== "request" &&
    runtimePolicy !== "required"
  ) {
    throw new TypeError(`Unknown origin persistence policy: ${policy}`);
  }

  if (storageManager?.persisted === undefined) {
    if (policy === "required") {
      throw new OriginPersistenceRequiredError(
        "Persistent origin storage is required, but the StorageManager persistence API is unavailable",
      );
    }
    return { policy, persisted: false, requested: false, reason: "unsupported" };
  }

  let alreadyPersisted: boolean;
  try {
    alreadyPersisted = await storageManager.persisted();
  } catch (error) {
    if (policy === "required") {
      throw new OriginPersistenceRequiredError(
        "Persistent origin storage is required, but its status could not be read",
        { cause: error },
      );
    }
    return { policy, persisted: false, requested: false, reason: "unavailable" };
  }

  if (alreadyPersisted) {
    return { policy, persisted: true, requested: false, reason: "already-persisted" };
  }
  if (policy === "best-effort") {
    return { policy, persisted: false, requested: false, reason: "denied" };
  }
  if (storageManager.persist === undefined) {
    if (policy === "required") {
      throw new OriginPersistenceRequiredError(
        "Persistent origin storage is required, but this browser cannot request it",
      );
    }
    return { policy, persisted: false, requested: false, reason: "unsupported" };
  }

  let granted: boolean;
  try {
    granted = await storageManager.persist();
  } catch (error) {
    if (policy === "required") {
      throw new OriginPersistenceRequiredError(
        "Persistent origin storage is required, but the browser rejected the request",
        { cause: error },
      );
    }
    return { policy, persisted: false, requested: true, reason: "unavailable" };
  }
  if (!granted && policy === "required") {
    throw new OriginPersistenceRequiredError(
      "Persistent origin storage is required, but the browser denied the request",
    );
  }
  return {
    policy,
    persisted: granted,
    requested: true,
    reason: granted ? "granted" : "denied",
  };
}
