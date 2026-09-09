/** Shared browser-download measurement for the report and regression gate. */
import { build } from "esbuild";
import { createRequire } from "node:module";
import { gzipSync, constants } from "node:zlib";
import { readFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(path.join(repoRoot, "package.json"));

export interface EngineSpec {
  id: string;
  name: string;
  /** The npm package a user installs. */
  packageName: string;
  /** Module specifier bundled as the entry point. */
  entry: string;
  /** Optional virtual application entry when a usable engine requires more than one subpath. */
  entrySource?: string;
  /**
   * Files the entry fetches at run time rather than importing, resolved against the package
   * directory. They never reach the bundle, but the browser downloads every one of them.
   */
  runtimeAssets: string[];
  note: string;
}

export const ENGINES: EngineSpec[] = [
  {
    id: "minnow",
    name: "Minnow",
    packageName: "@minnowdb/core",
    entry: "@minnowdb/core",
    entrySource:
      'export { MinnowDatabase } from "@minnowdb/core"; ' +
      'export { OpfsBlockStore } from "@minnowdb/core/storage/opfs";',
    runtimeAssets: [],
    note: "The main engine plus its larger durable adapter (OPFS). Plain JavaScript; nothing else to fetch.",
  },
  {
    id: "sqlite",
    name: "SQLite Wasm",
    packageName: "@sqlite.org/sqlite-wasm",
    entry: "@sqlite.org/sqlite-wasm",
    runtimeAssets: ["dist/sqlite3.wasm"],
    note: "The loader plus the SQLite Wasm build it instantiates.",
  },
  {
    id: "pglite",
    name: "PGlite",
    packageName: "@electric-sql/pglite",
    entry: "@electric-sql/pglite",
    runtimeAssets: ["dist/pglite.wasm", "dist/pglite.data"],
    note: "The loader, the PostgreSQL Wasm build, and the packed data directory it unpacks on first run.",
  },
];

/**
 * Not every package exports its own manifest, so fall back to walking up from the resolved
 * entry until a package.json claims the name.
 */
function packageDirectory(packageName: string): string {
  try {
    return path.dirname(require.resolve(`${packageName}/package.json`));
  } catch {
    let directory = path.dirname(require.resolve(packageName));
    for (;;) {
      const manifest = path.join(directory, "package.json");
      try {
        const parsed = JSON.parse(readFileSync(manifest, "utf8")) as { name?: string };
        if (parsed.name === packageName) return directory;
      } catch {
        // Keep walking: this level has no readable manifest.
      }
      const parent = path.dirname(directory);
      if (parent === directory) throw new Error(`Cannot locate package: ${packageName}`);
      directory = parent;
    }
  }
}

function packageVersion(packageName: string): string {
  const manifest = JSON.parse(
    readFileSync(path.join(packageDirectory(packageName), "package.json"), "utf8"),
  ) as { version?: string };
  return manifest.version ?? "unknown";
}

const gzipBytes = (bytes: Uint8Array): number =>
  gzipSync(bytes, { level: constants.Z_BEST_COMPRESSION }).byteLength;

/**
 * Bundles one entry the way an application would. Runtime assets stay external: esbuild would
 * otherwise try to inline or copy them, and they are counted separately at their shipped size.
 */
async function bundleBytes(spec: EngineSpec): Promise<Uint8Array> {
  const result = await build({
    ...(spec.entrySource === undefined
      ? { entryPoints: [spec.entry] }
      : {
          stdin: {
            contents: spec.entrySource,
            resolveDir: repoRoot,
            sourcefile: `${spec.id}-size-entry.js`,
          },
        }),
    absWorkingDir: repoRoot,
    bundle: true,
    write: false,
    minify: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    define: { "process.env.NODE_ENV": '"production"' },
    external: ["fs", "path", "node:*"],
    loader: { ".wasm": "empty", ".data": "empty" },
    logLevel: "silent",
  });
  const output = result.outputFiles?.[0];
  if (output === undefined) throw new Error(`esbuild produced no output for ${spec.entry}`);
  return output.contents;
}

interface MeasuredAsset {
  name: string;
  bytes: number;
  gzipBytes: number;
}

export async function measure(spec: EngineSpec): Promise<{
  id: string;
  name: string;
  package: string;
  version: string;
  note: string;
  assets: MeasuredAsset[];
  totalBytes: number;
  totalGzipBytes: number;
}> {
  const bundle = await bundleBytes(spec);
  const assets: MeasuredAsset[] = [
    {
      name: "JavaScript (bundled, minified)",
      bytes: bundle.byteLength,
      gzipBytes: gzipBytes(bundle),
    },
  ];
  const directory = packageDirectory(spec.packageName);
  for (const relative of spec.runtimeAssets) {
    const file = path.join(directory, relative);
    if (!statSync(file).isFile()) throw new Error(`Runtime asset is missing: ${file}`);
    const bytes = readFileSync(file);
    assets.push({
      name: path.basename(relative),
      bytes: bytes.byteLength,
      gzipBytes: gzipBytes(bytes),
    });
  }
  return {
    id: spec.id,
    name: spec.name,
    package: spec.packageName,
    version: packageVersion(spec.packageName),
    note: spec.note,
    assets,
    totalBytes: assets.reduce((total, asset) => total + asset.bytes, 0),
    totalGzipBytes: assets.reduce((total, asset) => total + asset.gzipBytes, 0),
  };
}
