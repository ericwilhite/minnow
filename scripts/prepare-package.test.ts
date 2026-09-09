import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { compactEmbeddedStyles, preparePackage, unusedDeclarations } from "./prepare-package.mjs";

const roots: string[] = [];
function fixture(files: Record<string, string>): string {
  const root = mkdtempSync(join(tmpdir(), "minnow-package-preparation-"));
  roots.push(root);
  for (const [file, contents] of Object.entries(files)) {
    mkdirSync(dirname(join(root, file)), { recursive: true });
    writeFileSync(join(root, file), contents);
  }
  return root;
}
afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

const manifest = JSON.stringify({
  name: "package-preparation-fixture",
  version: "1.0.0",
  type: "module",
  files: ["dist"],
  exports: { ".": { types: "./dist/index.d.ts", default: "./dist/index.js" } },
});

describe("declaration publication", () => {
  it("follows public type edges and keeps local output intact while npm excludes unused types", () => {
    const root = fixture({
      "package.json": manifest,
      "dist/index.js": 'export { value } from "./internal.js";',
      "dist/internal.js": "export const value = 1;",
      "dist/index.d.ts": '/// <reference path="./ambient.d.ts" />\nexport * from "./public.js";',
      "dist/public.d.ts": 'export type Value = import("./value.js").Value;',
      "dist/value.d.ts":
        'import type { Value as Cycle } from "./public.js"; export interface Value { next?: Cycle }',
      "dist/ambient.d.ts": "interface FixtureAmbient { value: number }",
      "dist/internal.d.ts": "export declare const value = 1;",
    });
    expect(preparePackage(root)).toEqual(["dist/internal.d.ts"]);
    expect(readFileSync(join(root, "dist/internal.d.ts"), "utf8")).toContain("value = 1");
    const output = execFileSync(
      "npm",
      ["pack", "--ignore-scripts", "--dry-run", "--json", "--cache", join(root, "cache")],
      {
        cwd: root,
        encoding: "utf8",
      },
    );
    const [report] = JSON.parse(output) as Array<{ files: Array<{ path: string }> }>;
    const files = report?.files.map(({ path }) => path);
    expect(files).toEqual(
      expect.arrayContaining([
        "dist/index.d.ts",
        "dist/public.d.ts",
        "dist/value.d.ts",
        "dist/ambient.d.ts",
        "dist/internal.js",
      ]),
    );
    expect(files).not.toContain("dist/internal.d.ts");
    // A formerly internal type can become public on the next build: no stale exclusion survives.
    writeFileSync(join(root, "dist/index.d.ts"), 'export * from "./internal.js";');
    expect(preparePackage(root)).not.toContain("dist/internal.d.ts");
    expect(readFileSync(join(root, "dist/.npmignore"), "utf8")).not.toContain("/internal.d.ts");
  });

  it("includes alternate conditional type entries and refuses missing local dependencies", () => {
    const root = fixture({
      "package.json": JSON.stringify({
        exports: {
          ".": {
            import: { types: "./dist/index.d.mts" },
            require: { types: "./dist/index.d.cts" },
          },
        },
      }),
      "dist/index.d.mts": 'export type Value = import("./shared.js").Value;',
      "dist/index.d.cts": "export declare const value: number;",
      "dist/shared.d.ts": "export type Value = number;",
    });
    expect(unusedDeclarations(root)).toEqual([]);
    writeFileSync(join(root, "dist/shared.d.ts"), 'export * from "./missing.js";');
    expect(() => preparePackage(root)).toThrow("Unresolved declaration import ./missing.js");
  });
});

describe("embedded CSS publication", () => {
  it("preserves significant string, selector, calc, and custom-property whitespace", () => {
    const css =
      '.parent .child { content: "two  words"; --label: "a  b"; width: calc(100% - 2px); background: url("data:text/plain,a b"); }';
    const source = `export const styles = ${JSON.stringify(css)};\nexport const unrelated = "unchanged";\n`;
    const compact = compactEmbeddedStyles(source);
    expect(compact).toContain(".parent .child{");
    expect(compact).toContain("two  words");
    expect(compact).toContain("a  b");
    expect(compact).toContain("calc(100% - 2px)");
    expect(compact).toContain(String.raw`data:text/plain,a\\ b`);
    expect(compact).toContain('export const unrelated = "unchanged";');
    expect(compactEmbeddedStyles(compact)).toBe(compact);
  });

  it("refuses dynamic styles instead of evaluating package code or omitting rules", () => {
    expect(() => compactEmbeddedStyles("export const styles = buildStyles();")).toThrow(
      "static string",
    );
    expect(() => compactEmbeddedStyles("export const styles = `a { color: ${theme} }`;")).toThrow(
      "static string",
    );
  });
});
