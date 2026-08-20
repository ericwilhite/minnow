/**
 * The dependency rules that keep the storage contract clean, enforced as a test so a stray
 * import fails CI instead of quietly re-coupling the layers:
 *
 * 1. **The contract owes nothing to anyone.** `types.ts` and `snapshot.ts` are the whole
 *    vocabulary between engine and store; they may reach the block-format utilities and each
 *    other, never an adapter or the toolkit.
 * 2. **The toolkit is built on the contract alone.** `storage/toolkit/` may not import from
 *    any adapter — an adapter uses the toolkit, never the reverse.
 * 3. **The engine sees only the contract surface.** Engine, plan, and transaction sources
 *    import storage only through its public barrels (`storage/index.js`, `storage/types.js`,
 *    `storage/snapshot.js`) — never an adapter's internals and never the toolkit. The one
 *    exception is the worker host's descriptor switch — the composition root — which
 *    dynamically imports exactly the three adapter entry modules, so a bundler can split them
 *    into their own chunks and an application ships only the store it opens.
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(import.meta.dirname, "..");

function sourceFiles(directory: string): string[] {
  const files: string[] = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...sourceFiles(path));
    else if (entry.name.endsWith(".ts") && !entry.name.endsWith(".test.ts")) files.push(path);
  }
  return files;
}

function importsOf(path: string): string[] {
  const source = readFileSync(path, "utf8");
  return [...source.matchAll(/(?:from|import)\s*\(?\s*"([^"]+)"/g)]
    .map((match) => match[1] ?? "")
    .filter((specifier) => specifier.startsWith("."));
}

describe("storage contract boundaries", () => {
  it("the contract imports nothing from adapters or the toolkit", () => {
    for (const file of ["types.ts", "snapshot.ts"].map((name) => join(SRC, "storage", name))) {
      for (const specifier of importsOf(file)) {
        expect(
          ["./types.js", "./snapshot.js", "../block-format/index.js"],
          `${file} imports ${specifier}`,
        ).toContain(specifier);
      }
    }
  });

  it("the toolkit imports only the contract, block-format, and itself", () => {
    for (const file of sourceFiles(join(SRC, "storage", "toolkit"))) {
      for (const specifier of importsOf(file)) {
        const allowed =
          specifier.startsWith("./") ||
          specifier === "../types.js" ||
          specifier === "../snapshot.js" ||
          specifier === "../../block-format/index.js";
        expect(allowed, `${file} imports ${specifier}`).toBe(true);
      }
    }
  });

  it("the engine reaches storage only through its public surface", () => {
    // The worker host is the composition root: the one module allowed to name the adapter
    // entry modules, and only those, so its dynamic imports can code-split them.
    const compositionRoot = join(SRC, "engine", "worker-host.ts");
    const adapterEntries = ["storage/indexeddb.js", "storage/memory.js", "storage/opfs/index.js"];
    const layers = ["engine", "plan", "transactions"].map((name) => join(SRC, name));
    for (const file of layers.flatMap(sourceFiles)) {
      for (const specifier of importsOf(file)) {
        if (!specifier.includes("/storage/")) continue;
        const surface = specifier.replace(/^(\.\.\/)+/, "");
        const allowed = ["storage/index.js", "storage/types.js", "storage/snapshot.js"];
        if (file === compositionRoot) allowed.push(...adapterEntries);
        expect(allowed, `${file} imports ${specifier}`).toContain(surface);
      }
    }
  });
});
