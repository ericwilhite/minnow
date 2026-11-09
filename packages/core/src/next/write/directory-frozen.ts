import { readFile, mkdtemp, writeFile, rm } from "node:fs/promises";
import { gunzipSync } from "node:zlib";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { openDirectory } from "./directory-compat-api.js";
import type { DirectoryImage } from "./directory-fixture-files.js";
const fixtures = new URL("./fixtures/directory/", import.meta.url);
export async function readDirectoryFixture(name: string): Promise<DirectoryImage> {
  const manifest = JSON.parse(await readFile(new URL("fixtures.json", fixtures), "utf8")) as Array<{
    name: string;
    sha256: string;
  }>;
  const bytes = await readFile(new URL(`${name}.json.gz`, fixtures));
  if (
    createHash("sha256").update(bytes).digest("hex") !==
    manifest.find((f) => f.name === name)?.sha256
  )
    throw new Error("Frozen native fixture checksum changed");
  return JSON.parse(gunzipSync(bytes).toString()) as DirectoryImage;
}
export async function frozenDirectory() {
  const metadata = JSON.parse(await readFile(new URL("baseline.json", fixtures), "utf8")) as {
    bundleSha256: string;
  };
  const bytes = gunzipSync(await readFile(new URL("baseline.mjs.gz", fixtures)));
  if (createHash("sha256").update(bytes).digest("hex") !== metadata.bundleSha256)
    throw new Error("Frozen baseline checksum changed");
  const scratch = await mkdtemp(join(tmpdir(), "minnow-directory-baseline-"));
  try {
    const file = join(scratch, "baseline.mjs");
    await writeFile(file, bytes);
    const baseline = (await import(/* @vite-ignore */ pathToFileURL(file).href)) as {
      openDirectory: typeof openDirectory;
    };
    return {
      open: baseline.openDirectory,
      dispose: () => rm(scratch, { recursive: true, force: true }),
    };
  } catch (error) {
    await rm(scratch, { recursive: true, force: true });
    throw error;
  }
}
