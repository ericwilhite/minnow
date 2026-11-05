/**
 * The module rules of the new core (next/README.md, "Dependency rules"), enforced so a stray
 * import fails CI instead of quietly coupling tracks:
 *
 * 1. Nothing outside `next/` imports `next/` until cut-over.
 * 2. Inside `next/`, a module imports only the modules its layer allows (`ALLOWED`).
 * 3. Outside `next/`, non-test sources import the old code only as `import type`, except the
 *    modules that are bridges by design: `lower/` (calls the front end), `owner/` (the composition
 *    root) and `testing/` (the differential adapter).
 */
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(import.meta.dirname, "..");
const NEXT = import.meta.dirname;

/** Module → the next/ modules it may import ("." is the top-level types.ts). */
const ALLOWED: Readonly<Record<string, readonly string[]>> = {
  ".": [],
  hash: [],
  vector: ["."],
  storage: [".", "hash"],
  format: [".", "hash", "storage", "memory"],
  spill: ["hash", "storage"],
  memory: ["vector"],
  keys: [".", "hash", "vector", "memory"],
  ir: [".", "vector"],
  // Tracks (not written yet) — their layer, fixed now:
  exec: [".", "hash", "vector", "keys", "ir", "memory", "spill"],
  write: [".", "hash", "vector", "keys", "storage", "format", "memory"],
  lower: [".", "ir", "vector"],
  owner: [
    ".",
    "keys",
    "hash",
    "vector",
    "storage",
    "format",
    "spill",
    "memory",
    "ir",
    "exec",
    "write",
    "lower",
  ],
  testing: [
    ".",
    "keys",
    "hash",
    "vector",
    "storage",
    "format",
    "spill",
    "memory",
    "ir",
    "exec",
    "write",
    "lower",
    "owner",
  ],
};
const BRIDGES = new Set(["lower", "owner", "testing"]);

function sourceFiles(directory: string): string[] {
  const files: string[] = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...sourceFiles(path));
    else if (entry.name.endsWith(".ts")) files.push(path);
  }
  return files;
}

interface ImportEdge {
  readonly target: string;
  readonly typeOnly: boolean;
}

function importsOf(path: string): ImportEdge[] {
  const source = readFileSync(path, "utf8");
  return [...source.matchAll(/^(import|export)(\s+type)?[^;]*?["']([^"']+)["']/gmu)]
    .filter((match) => (match[3] ?? "").startsWith("."))
    .map((match) => ({
      target: resolve(dirname(path), match[3] ?? ""),
      typeOnly: match[2] !== undefined,
    }));
}

function moduleOf(path: string): string {
  const parts = relative(NEXT, path).split(sep);
  return parts.length === 1 ? "." : (parts[0] ?? ".");
}

const inNext = (path: string) => path.startsWith(NEXT + sep);

describe("next/ dependency rules", () => {
  it("nothing outside next/ imports it", () => {
    for (const file of sourceFiles(SRC).filter((path) => !inNext(path)))
      for (const edge of importsOf(file))
        expect(inNext(edge.target), `${relative(SRC, file)} imports next/`).toBe(false);
  });

  it("every next/ module imports only what its layer allows", () => {
    for (const file of sourceFiles(NEXT)) {
      const module = moduleOf(file);
      const allowed = ALLOWED[module];
      expect(allowed, `next/${module} has no declared layer`).toBeDefined();
      for (const edge of importsOf(file).filter((candidate) => inNext(candidate.target))) {
        const target = moduleOf(edge.target);
        if (target === module) continue;
        expect(allowed, `${relative(SRC, file)} imports next/${target}`).toContain(target);
      }
    }
  });

  it("non-bridge next/ sources use the old code only as types", () => {
    for (const file of sourceFiles(NEXT)) {
      if (BRIDGES.has(moduleOf(file)) || file.endsWith(".test.ts")) continue;
      for (const edge of importsOf(file).filter((candidate) => !inNext(candidate.target)))
        expect(edge.typeOnly, `${relative(SRC, file)} imports ${relative(SRC, edge.target)}`).toBe(
          true,
        );
    }
  });
});
