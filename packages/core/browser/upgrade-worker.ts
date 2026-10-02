import { MinnowDatabase } from "@minnowdb/core";
import { decodeBlock } from "@minnowdb/core/block-format";
import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { OpfsBlockStore as Layout6Store } from "@minnowdb/core-layout6/storage/opfs";
import { OpfsBlockStore as Layout7Store } from "@minnowdb/core-layout7/storage/opfs";
import type { NativeUpgradeResult } from "./upgrade-run.js";

self.onmessage = (event: MessageEvent<{ files: Record<string, string>; name: string }>) => {
  void run(event.data).then(
    (result) => self.postMessage({ result }),
    (error: unknown) =>
      self.postMessage({
        error: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
      }),
  );
};

async function run({
  files,
  name,
}: {
  files: Record<string, string>;
  name: string;
}): Promise<NativeUpgradeResult> {
  const root = await navigator.storage.getDirectory();
  for (const [path, base64] of Object.entries(files)) {
    const parts = path.split("/");
    parts[1] = name;
    const filename = parts.pop();
    if (filename === undefined) throw new Error("Invalid native fixture path");
    let directory = root;
    for (const part of parts)
      directory = await directory.getDirectoryHandle(part, { create: true });
    const handle = await (
      await directory.getFileHandle(filename, { create: true })
    ).createSyncAccessHandle();
    try {
      handle.write(Uint8Array.from(atob(base64), (char) => char.charCodeAt(0)));
      handle.flush();
    } finally {
      handle.close();
    }
  }
  let store = await OpfsBlockStore.open({ name });
  let database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    const tables = (await store.listTables()).map(({ name }) => name);
    const bytes = await store.getBlock("fixture-block");
    if (bytes === undefined) throw new Error("Old fixture block was lost");
    const values = (await decodeBlock(bytes)).column.values;
    const followerBlock = await store.getBlock("fixture-follower-block");
    const walOnlyPreserved =
      followerBlock?.length === bytes.length &&
      followerBlock.every((byte, index) => byte === bytes[index]);
    await database.execute("CREATE TABLE after_upgrade (id INTEGER PRIMARY KEY, value TEXT)");
    await database.execute("INSERT INTO after_upgrade VALUES (1, 'retained')");
    await database.close();
    store._crashForTests();
    store = await OpfsBlockStore.open({ name });
    database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    const rows = (await database.query("SELECT id, value FROM after_upgrade")).rows;
    const directory = await (await root.getDirectoryHandle("minnowdb")).getDirectoryHandle(name);
    const marker = JSON.parse(
      await (await (await directory.getFileHandle("format.json")).getFile()).text(),
    ) as { formatVersion: number };
    // Every released reader of an older layout refuses the upgraded database unchanged.
    const olderReaders = [
      { open: (options: { name: string }) => Layout6Store.open(options), supported: 6 },
      { open: (options: { name: string }) => Layout7Store.open(options), supported: 7 },
    ];
    let refusals = 0;
    for (const reader of olderReaders) {
      try {
        const old = await reader.open({ name });
        old.close();
      } catch (error) {
        if (!(error instanceof Error)) throw error;
        const properties = error as Error & {
          actualVersion?: unknown;
          supportedVersion?: unknown;
        };
        if (
          error.name !== "StorageFormatVersionError" ||
          properties.actualVersion !== 8 ||
          properties.supportedVersion !== reader.supported
        )
          throw error;
        refusals += 1;
      }
    }
    const olderReaderRefused = refusals === olderReaders.length;
    return {
      tables,
      blockValues: Array.from<unknown>(values),
      walOnlyPreserved,
      rows,
      format: marker.formatVersion,
      integrity: (await store.checkIntegrity({ mode: "full" })).ok,
      olderReaderRefused,
    };
  } finally {
    await database.close();
    store.close();
  }
}
