/**
 * Vitest global setup: one just-initialised PGlite data directory per run, for
 * ./pglite-snapshot.client.ts to start default-option instances from instead of running initdb in
 * each. Made here, from the installed package, every run; never committed. A snapshot set from
 * outside (`MINNOW_PGLITE_SNAPSHOT`, or `off`) is left as is. Every project runs this setup; the
 * first one to run makes the snapshot, and only it removes it.
 */
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../node_modules/@electric-sql/pglite/dist/index.js";

export default async function setup(): Promise<(() => void) | undefined> {
  if ((process.env.MINNOW_PGLITE_SNAPSHOT ?? "").trim() !== "") return undefined;
  const dir = mkdtempSync(join(tmpdir(), "minnow-pglite-"));
  const path = join(dir, "initdb.tar");
  const fresh = await PGlite.create();
  try {
    writeFileSync(path, new Uint8Array(await (await fresh.dumpDataDir("none")).arrayBuffer()));
  } finally {
    await fresh.close();
  }
  process.env.MINNOW_PGLITE_SNAPSHOT = path;
  return () => {
    rmSync(dir, { recursive: true, force: true });
  };
}
