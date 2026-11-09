/** Freeze the exact baseline implementation and native files. Run explicitly; tests never
 * regenerate these bytes. All scratch follows os.tmpdir(), including the archived source. */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, writeFile, rm, cp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gzipSync } from "node:zlib";
import { build, version } from "esbuild";
import { format, resolveConfig } from "prettier";
import { DirectoryFiles } from "../packages/core/src/next/write/directory-fixture-files.js";
import { SimLocks } from "../packages/core/src/next/storage/sim-disk.js";
import type { openDirectory } from "../packages/core/src/next/write/directory-compat-api.js";
const revision = "4133d25a284654517d1d7a0f723cb843e1cd6611";
const entry = "packages/core/src/next/write/directory-compat-api.ts";
const root = fileURLToPath(new URL("..", import.meta.url));
const output = resolve(root, "packages/core/src/next/write/fixtures/directory");
const style = await resolveConfig(join(output, "fixtures.json"));
const json = (value: unknown) => format(JSON.stringify(value), { ...style, parser: "json" });
const scratch = await mkdtemp(join(tmpdir(), "minnow-directory-freeze-"));
try {
  const archive = join(scratch, "source.tar");
  execFileSync("git", ["archive", "--output", archive, revision, "packages/core/src"], {
    cwd: root,
  });
  execFileSync("tar", ["-xf", archive, "-C", scratch]);
  await cp(join(root, entry), join(scratch, entry));
  const result = await build({
    entryPoints: [join(scratch, entry)],
    bundle: true,
    format: "esm",
    platform: "node",
    target: "es2022",
    minify: true,
    write: false,
    metafile: true,
    absWorkingDir: scratch,
  });
  if (Object.values(result.metafile.outputs).some((o) => o.imports.some((i) => i.external)))
    throw new Error("Baseline bundle must be self-contained");
  const bytes = result.outputFiles[0]?.contents;
  if (bytes === undefined) throw new Error("No baseline bundle");
  await mkdir(output, { recursive: true });
  await writeFile(join(output, "baseline.mjs.gz"), gzipSync(bytes, { level: 9 }));
  const inputs: Record<string, string> = {};
  for (const path of Object.keys(result.metafile.inputs).sort())
    inputs[path] = createHash("sha256")
      .update(await readFile(join(scratch, path)))
      .digest("hex");
  await writeFile(
    join(output, "baseline.json"),
    await json({
      revision,
      esbuild: version,
      entry,
      bundleSha256: createHash("sha256").update(bytes).digest("hex"),
      inputs,
    }),
  );
  const file = join(scratch, "baseline.mjs");
  await writeFile(file, bytes);
  const api = (await import(pathToFileURL(file).href)) as { openDirectory: typeof openDirectory };
  const manifest = [];
  for (const groups of [0, 4095, 4099]) {
    const disk = new DirectoryFiles(),
      locks = new SimLocks();
    const db = await api.openDirectory(disk, locks);
    await db.create();
    if (groups > 0) await db.insert(Array.from({ length: groups }, (_, k) => k));
    await db.settle();
    const pages = await db.pages();
    if (pages.reduce((n, p) => n + p.groupCount, 0) !== groups)
      throw new Error("Fixture group count mismatch");
    await db.space.audit();
    await db.close();
    const image = disk.image(),
      name = `packed-${groups}`;
    const packed = gzipSync(Buffer.from(JSON.stringify(image)), { level: 9 });
    await writeFile(join(output, `${name}.json.gz`), packed);
    manifest.push({
      name,
      groups,
      pageCounts: pages.map((p) => p.groupCount),
      shared: pages.length > 1 && pages[0]?.ref.extent.offset === pages[1]?.ref.extent.offset,
      sha256: createHash("sha256").update(packed).digest("hex"),
    });
    console.info(name, packed.length, manifest.at(-1));
  }
  await writeFile(join(output, "fixtures.json"), await json(manifest));
} finally {
  await rm(scratch, { recursive: true, force: true });
}
