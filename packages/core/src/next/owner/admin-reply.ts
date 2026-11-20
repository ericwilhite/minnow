/** Own read-only metadata through MessagePort clone and receipt ACK, then relinquish it to the caller. */
import type { Grant, MemoryGovernor } from "../memory/governor.js";
export class OwnedAdminReply<T> {
  #value: T | null;
  #grant: Grant | null;
  constructor(value: T, grant: Grant) {
    this.#value = value;
    this.#grant = grant;
  }
  get value(): T {
    if (this.#grant === null) throw new TypeError("Admin reply is closed");
    return this.#value as T;
  }
  close(): void {
    const grant = this.#grant;
    this.#grant = null;
    this.#value = null;
    grant?.close();
  }
}
/** No source input, loan or result buffer may be retained while waiting. Version drift retries metadata only. */
export async function inspectReply<T>(
  governor: MemoryGovernor,
  version: () => number,
  estimate: () => number,
  build: () => T,
  signal?: AbortSignal,
): Promise<OwnedAdminReply<T>> {
  for (;;) {
    signal?.throwIfAborted();
    const before = version(),
      bytes = estimate();
    const grants = await governor.admitGroup({
      label: "owner inspection",
      grants: [{ label: "owner inspection reply", minimumBytes: bytes, spillable: false }],
      includesProgress: true,
      ...(signal === undefined ? {} : { signal }),
    });
    const grant = grants[0];
    if (grant === undefined) throw new Error("Missing inspection grant");
    try {
      signal?.throwIfAborted();
      if (version() !== before || estimate() > bytes) {
        grant.close();
        continue;
      }
      if (!grant.allocate(bytes)) throw new Error("Inspection recipe under-admitted");
      return new OwnedAdminReply(build(), grant);
    } catch (error) {
      grant.close();
      throw error;
    }
  }
}
