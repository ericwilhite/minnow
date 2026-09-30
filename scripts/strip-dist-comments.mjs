// @ts-check
/**
 * Strips comments from a package's shipped JavaScript before it is packed. The sources keep
 * their doc comments, and so do the declaration files (an editor reads those); only `dist/**.js`
 * is rewritten and whitespace is compacted, with zero effect on application bundles. Runs
 * as the package's `prepack` hook, so `npm pack` and `npm publish` ship the trimmed files
 * whichever command built them.
 *
 * Usage: node scripts/strip-dist-comments.mjs <dist directory>
 */
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { transformSync } from "esbuild";
import ts from "typescript";

/** @param {string} directory @returns {string[]} */
function javascriptFiles(directory) {
  /** @type {string[]} */
  const files = [];
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) files.push(...javascriptFiles(path));
    else if (entry.endsWith(".js")) files.push(path);
  }
  return files;
}

/**
 * Comments and whitespace are removed: no syntax lowering or identifier renaming, so the
 * module keeps its shape and stack traces keep their names. TypeScript drops every comment
 * (esbuild keeps the ones inside object literals and argument lists); esbuild then reprints the
 * result compactly, since TypeScript's output is wider than the original.
 * @param {string} source @returns {string}
 */
export function stripComments(source) {
  const uncommented = ts.transpileModule(source, {
    compilerOptions: {
      target: ts.ScriptTarget.ESNext,
      module: ts.ModuleKind.ESNext,
      removeComments: true,
    },
  }).outputText;
  return transformSync(uncommented, {
    format: "esm",
    target: "esnext",
    legalComments: "none",
    minifyWhitespace: true,
  }).code;
}

const directory = process.argv[2];
if (directory !== undefined && import.meta.url === `file://${process.argv[1] ?? ""}`) {
  let before = 0;
  let after = 0;
  for (const file of javascriptFiles(directory)) {
    const source = readFileSync(file, "utf8");
    const stripped = stripComments(source);
    before += Buffer.byteLength(source);
    after += Buffer.byteLength(stripped);
    if (stripped !== source) writeFileSync(file, stripped);
  }
  // stderr: `npm pack --json` owns stdout.
  console.error(
    `strip-dist-comments: ${directory} ${String(before)} -> ${String(after)} bytes of JavaScript`,
  );
}
