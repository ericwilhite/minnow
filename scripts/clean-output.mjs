/** Removes only generated workspace compiler output before a repository build. */
import { readdir, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packagesRoot = path.join(repoRoot, "packages");

// Discover package directories instead of duplicating the workspace list. This also removes
// ignored output left behind when a package is renamed or deleted.
for (const entry of await readdir(packagesRoot, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  await rm(path.join(packagesRoot, entry.name, "dist"), {
    recursive: true,
    force: true,
    maxRetries: 3,
  });
}
