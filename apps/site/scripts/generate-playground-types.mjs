/**
 * Collects the published type declarations into one file the TypeScript console loads.
 *
 * The console runs the real TypeScript language service in the browser, and a language service
 * is only as good as the `.d.ts` files it is given. Handing it a summary written by hand would
 * make the console a demonstration of a summary: it would autocomplete methods the package does
 * not have and stay silent about the ones it does. So the declarations shipped to npm are the
 * declarations the editor checks against, copied verbatim from `dist`.
 *
 * Two adjustments are made on the way through, both forced by the editor's resolver:
 *
 * - Relative specifiers lose their `.js` extension. TypeScript maps `./x.js` onto `./x.d.ts`
 *   only under the Node16 and Bundler resolvers, and the editor's bundled service offers
 *   Classic and Node. Extensionless specifiers resolve under Node, which is the one we use.
 * - Source-map comments are dropped. Nothing serves the `.d.ts.map` files, so every one of them
 *   would be a 404 on load.
 *
 * One file that is not a package declaration rides along: `lib/dataset/schema.ts`, the
 * playground's own schema, shipped as the editor's `./schema` module so the ambient `db` and
 * `database` can be typed by `typeof retailDefinition` — the value `migrate()` actually runs.
 *
 * Runs from `prebuild` and `predev`, after the packages are built, so the file is always in step
 * with `dist`.
 */
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.join(here, "../../..");
const output = path.join(here, "../public/playground-types.json");

/** The packages a snippet in the console may import from. */
const PACKAGES = ["@minnowdb/core", "@minnowdb/kysely", "kysely"];
/** Must match `components/playground/runtime-modules.ts`; a test ratchets the generated result. */
const RUNTIME_IMPORTS = new Set([
  "@minnowdb/core",
  "@minnowdb/core/client",
  "@minnowdb/kysely",
  "kysely",
]);

/** Where the editor is told each package lives; matches what a real `node_modules` would hold. */
const root = (name) => `file:///node_modules/${name}`;

async function* declarations(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) yield* declarations(full);
    else if (entry.name.endsWith(".d.ts") && !/\.(test|spec)\.d\.ts$/.test(entry.name)) yield full;
  }
}

/**
 * Rewrites the specifiers in one declaration file. Only `from "…"`, `import("…")`, and bare
 * `import "…"` are touched, so a `.js` inside prose in a doc comment survives.
 */
function rewrite(source) {
  return source
    .replace(/(\bfrom\s*|\bimport\s*\(\s*|\bimport\s+)(["'])(\.[^"']*?)\.js\2/g, "$1$2$3$2")
    .replace(/^\/\/# sourceMappingURL=.*$/gm, "")
    .trimEnd();
}

const files = {};
const paths = {};

for (const name of PACKAGES) {
  const packageDir = name.startsWith("@minnowdb/")
    ? path.join(repo, "packages", name.replace("@minnowdb/", ""))
    : path.join(repo, "node_modules", name);
  const manifest = JSON.parse(await readFile(path.join(packageDir, "package.json"), "utf8"));

  for await (const file of declarations(path.join(packageDir, "dist"))) {
    const relative = path.relative(packageDir, file).split(path.sep).join("/");
    files[`${root(name)}/${relative}`] = rewrite(await readFile(file, "utf8"));
  }

  // The export map is what a bundler resolves, so it is what the editor is told too — a subpath
  // the package does not publish must not resolve here either.
  for (const [subpath, target] of Object.entries(manifest.exports ?? {})) {
    const runtime =
      typeof target === "string"
        ? target
        : typeof target === "object" && target !== null
          ? target.default
          : undefined;
    const types =
      typeof target === "object" && target !== null && typeof target.types === "string"
        ? target.types
        : typeof runtime === "string" && runtime.endsWith(".js")
          ? runtime.replace(/\.js$/, ".d.ts")
          : undefined;
    if (typeof types !== "string") continue;
    const specifier = subpath === "." ? name : `${name}/${subpath.slice(2)}`;
    // Relative to the compiler's `baseUrl`, which the console sets to `file:///`.
    paths[specifier] = [`node_modules/${name}/${types.replace(/^\.\//, "")}`];
  }
}

// The schema module resolves relative to the ambient `file:///playground.d.ts`, so it sits at the
// root; its only import is `@minnowdb/core`, which the paths above already cover.
files["file:///schema.ts"] = rewrite(
  await readFile(path.join(here, "../lib/dataset/schema.ts"), "utf8"),
);

const missing = Object.entries(paths).filter(([, [target]]) => !(`file:///${target}` in files));
if (missing.length > 0) {
  throw new Error(
    `No declaration file for ${missing.map(([specifier]) => specifier).join(", ")}. ` +
      `Build the packages first: npm run build:packages`,
  );
}

await mkdir(path.dirname(output), { recursive: true });
await writeFile(
  output,
  JSON.stringify({ paths, files, runtimeModules: [...RUNTIME_IMPORTS].sort() }),
);

const bytes = Object.values(files).reduce((total, source) => total + source.length, 0);
console.log(
  `playground-types.json: ${String(Object.keys(files).length)} declarations, ` +
    `${String(Math.round(bytes / 1024))} KB`,
);
