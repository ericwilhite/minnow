/**
 * Copies the comparison engines' shipped browser builds into `public/vendor/`.
 *
 * SQLite Wasm and PGlite both resolve their own assets — the `.wasm` modules, the OPFS async
 * proxy worker, PGlite's packed data directory — through `new URL(…, import.meta.url)` and
 * dynamic imports the bundler cannot follow. Bundling them therefore either fails or produces a
 * build whose asset URLs point nowhere.
 *
 * Serving them as plain files sidesteps the bundler entirely: the browser resolves a URL
 * specifier natively, and every relative asset next to the entry resolves the way the package
 * expects. This is what Vite's `optimizeDeps.exclude` was buying, made explicit.
 *
 * Runs from `prebuild` and `predev`, so the files are always in step with node_modules.
 */
import { cp, mkdir, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(path.join(here, "../package.json"));
const vendor = path.join(here, "../public/vendor");

const PACKAGES = [
  { name: "@sqlite.org/sqlite-wasm", from: "dist", to: "sqlite" },
  { name: "@electric-sql/pglite", from: "dist", to: "pglite" },
];

await rm(vendor, { recursive: true, force: true });
await mkdir(vendor, { recursive: true });

/**
 * Not every package exports its own package.json, so the root is found by walking up from any
 * resolvable entry rather than by resolving that subpath.
 */
function packageRoot(name) {
  let dir = path.dirname(require.resolve(name));
  while (path.basename(dir) !== path.basename(name)) {
    const parent = path.dirname(dir);
    if (parent === dir) throw new Error(`Could not locate ${name}`);
    dir = parent;
  }
  return dir;
}

for (const entry of PACKAGES) {
  const root = packageRoot(entry.name);
  const source = path.join(root, entry.from);
  const target = path.join(vendor, entry.to);
  await cp(source, target, {
    recursive: true,
    // Source maps are a third of the payload and nothing reads them here.
    filter: (file) => !file.endsWith(".map"),
  });
  process.stdout.write(`vendored ${entry.name} -> public/vendor/${entry.to}\n`);
}
