/** Private benchmark setup, never an application import path. The caller owns an empty,
 * disposable directory. Native ownership is established before copying a file-store image;
 * ownership records are target-local and are never copied from the cached source.
 */
import {
  createOpfsBackend,
  type OpfsDirectoryLike,
} from "../../../packages/core/src/next/storage/opfs.js";
import type { LockManagerLike } from "../../../packages/core/src/next/storage/fence.js";

export interface FixtureInstall {
  readonly directory: OpfsDirectoryLike;
  readonly name: string;
  readonly locks: LockManagerLike;
  readonly files: ReadonlyArray<{ readonly name: string; readonly bytes: number }>;
  readonly read: (name: string) => Promise<Response>;
}
export async function installFileStoreFixture(options: FixtureInstall): Promise<void> {
  const names = ["data", "log", "root-0", "root-1"] as const;
  for (const name of names) {
    const file = options.files.find((entry) => entry.name === name);
    if (file === undefined || !Number.isSafeInteger(file.bytes) || file.bytes < 0)
      throw new Error("Missing or invalid fixture file");
  }
  const keys = options.directory.keys?.();
  if (keys === undefined) throw new Error("Fixture setup requires an enumerable empty target");
  for await (const _ of keys) {
    void _;
    throw new Error("Fixture target must be empty");
  }
  // Native initialization belongs HERE, before the first imported payload. It must not be
  // synthesized from the source's epoch: independent targets need independent identities.
  const backend = createOpfsBackend({
    directory: options.directory,
    name: options.name,
    locks: options.locks,
  });
  const lease = await backend.acquire();
  await lease.release();
  for (const name of names) {
    const expected = options.files.find((file) => file.name === name);
    if (expected === undefined) throw new Error("Missing fixture file");
    const source = await options.read(name);
    if (!source.ok || source.body === null) throw new Error("Fixture fetch failed");
    const handle = await options.directory.getFileHandle(name, { create: true }),
      out = await handle.createSyncAccessHandle();
    const reader = source.body.getReader();
    let at = 0;
    try {
      out.truncate(0);
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        let offset = 0;
        while (offset < part.value.length) {
          const remaining = part.value.subarray(offset),
            written = out.write(remaining, { at });
          if (!Number.isSafeInteger(written) || written <= 0 || written > remaining.length)
            throw new Error("Short fixture write");
          at += written;
          offset += written;
        }
      }
      if (at !== expected.bytes) throw new Error("Fixture length mismatch");
      out.flush();
    } finally {
      reader.releaseLock();
      out.close();
    }
  }
}
