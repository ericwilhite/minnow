/**
 * Audit the shipped module graph against a Git revision with unchanged dependencies/compiler.
 * Run after the package build: node scripts/library-size-audit.mts --base=f452eb82
 * Native dist must match re-emitted HEAD byte for byte before comparing frozen source families.
 * Metafiles, module deltas and bundles use os.tmpdir() by default, never public assets.
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { constants, gzipSync } from "node:zlib";
import { build, type BuildOptions, type Plugin } from "esbuild";
import ts from "typescript";

const root = resolve(import.meta.dirname, "..");
const args = process.argv.slice(2);
const baseArg = args.find((arg) => arg.startsWith("--base="))?.slice(7);
if (baseArg === undefined || args.some((arg) => !/^--(?:base|output)=/u.test(arg)))
  throw new TypeError("Usage: library-size-audit.mts --base=REV [--output=DIR]");
const output = resolve(
  args.find((arg) => arg.startsWith("--output="))?.slice(9) ??
    join(tmpdir(), "minnow-bundle-audit"),
);
const git = (...argv: string[]): string =>
  execFileSync("git", argv, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  });
const base = git("rev-parse", "--verify", "--end-of-options", `${baseArg}^{commit}`).trim();
const head = git("rev-parse", "HEAD").trim();
const changedBuildInputs = git(
  "diff",
  "--name-only",
  base,
  head,
  "--",
  "package-lock.json",
  "tsconfig.base.json",
  "packages/*/package.json",
).trim();
if (changedBuildInputs !== "")
  throw new TypeError(
    `Audit requires matching dependencies/compiler inputs: ${changedBuildInputs}`,
  );
mkdirSync(output, { recursive: true });

const entries: Record<string, BuildOptions> = {
  main: { entryPoints: ["@minnowdb/core"] },
  opfs: {
    stdin: {
      contents:
        'export { MinnowDatabase } from "@minnowdb/core"; ' +
        'export { OpfsBlockStore } from "@minnowdb/core/storage/opfs";',
      resolveDir: root,
    },
  },
  indexeddb: { entryPoints: ["@minnowdb/core/worker/indexeddb"] },
  worker: { stdin: { contents: 'import "@minnowdb/core/worker";', resolveDir: root } },
  client: { stdin: { contents: 'export * from "@minnowdb/core/client";', resolveDir: root } },
};

const sources = new Map<string, string>();
function frozenSource(revision: string): Plugin {
  return {
    name: "frozen-published-source",
    setup(bundle) {
      bundle.onLoad({ filter: /\/packages\/[^/]+\/dist\/.*\.js$/ }, ({ path }) => {
        const source = relative(root, path).replace("/dist/", "/src/").replace(/\.js$/u, ".ts");
        const key = `${revision}:${source}`;
        let contents = sources.get(key);
        if (contents === undefined) {
          contents = ts.transpileModule(git("show", key), {
            compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
          }).outputText;
          sources.set(key, contents);
        }
        return { contents, loader: "js", resolveDir: dirname(path) };
      });
    },
  };
}

interface Measurement {
  raw: number;
  gzip: number;
  sha256: string;
  modules: Record<string, number>;
  forbidden: string[];
}
const measurements: Record<string, Record<string, Measurement>> = {};
for (const [variant, revision] of [
  ["current", null],
  ["headSource", head],
  ["base", base],
] as const) {
  const measured: Record<string, Measurement> = {};
  measurements[variant] = measured;
  for (const [label, entry] of Object.entries(entries)) {
    const result = await build({
      ...entry,
      absWorkingDir: root,
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
      metafile: true,
      plugins: revision === null ? [] : [frozenSource(revision)],
    });
    const bytes = result.outputFiles[0]?.contents;
    if (bytes === undefined) throw new Error(`Missing bundle for ${variant}/${label}`);
    measured[label] = {
      raw: bytes.length,
      gzip: gzipSync(bytes, { level: constants.Z_BEST_COMPRESSION }).length,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      modules: Object.fromEntries(
        Object.values(result.metafile.outputs).flatMap((file) =>
          Object.entries(file.inputs)
            .filter(([, input]) => input.bytesInOutput > 0)
            .map(([path, input]) => [path, input.bytesInOutput]),
        ),
      ),
      // Include parsed-but-shaken inputs too: private or test code must not be reachable.
      forbidden: Object.keys(result.metafile.inputs).filter((path) =>
        /\/(?:src|dist)\/(?:next|testing|__tests__)\/|\.test\.[cm]?[jt]s$/u.test(path),
      ),
    };
    writeFileSync(join(output, `${variant}-${label}.meta.json`), JSON.stringify(result.metafile));
    writeFileSync(join(output, `${variant}-${label}.mjs`), bytes);
  }
}

const deltas = Object.fromEntries(
  Object.keys(entries).map((entry) => {
    const current = measurements.current?.[entry],
      control = measurements.headSource?.[entry],
      reference = measurements.base?.[entry];
    if (current === undefined || control === undefined || reference === undefined)
      throw new Error(`Missing measurement for ${entry}`);
    if (current.sha256 !== control.sha256)
      throw new Error(`Native dist differs from frozen HEAD source for ${entry}; build first`);
    if (current.forbidden.length + reference.forbidden.length !== 0)
      throw new Error(
        `Private/test code reached ${entry}: ${JSON.stringify({ current: current.forbidden, base: reference.forbidden })}`,
      );
    return [
      entry,
      {
        raw: current.raw - reference.raw,
        gzip: current.gzip - reference.gzip,
        added: Object.keys(current.modules).filter((path) => !(path in reference.modules)),
        removed: Object.keys(reference.modules).filter((path) => !(path in current.modules)),
        moduleBytes: Object.keys({ ...reference.modules, ...current.modules })
          .map((path) => ({
            path,
            base: reference.modules[path] ?? 0,
            current: current.modules[path] ?? 0,
            delta: (current.modules[path] ?? 0) - (reference.modules[path] ?? 0),
          }))
          .filter((module) => module.delta !== 0),
      },
    ];
  }),
);
writeFileSync(
  join(output, "audit.json"),
  JSON.stringify({ base, head, measurements, deltas }, null, 2),
);
console.log(JSON.stringify({ base, head, deltas, output }, null, 2));
