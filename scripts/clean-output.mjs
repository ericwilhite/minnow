/** Removes only generated workspace compiler output before a repository build. */
import { rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packageNames = ["core", "devtools", "export", "kysely", "react"];

for (const name of packageNames) {
  await rm(path.join(repoRoot, "packages", name, "dist"), {
    recursive: true,
    force: true,
    maxRetries: 3,
  });
}
