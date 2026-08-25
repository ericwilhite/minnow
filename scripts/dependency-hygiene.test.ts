import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const repoRoot = join(import.meta.dirname, "..");

function json(file: string): Record<string, unknown> {
  return JSON.parse(readFileSync(join(repoRoot, file), "utf8")) as Record<string, unknown>;
}

describe("dependency hygiene", () => {
  it("keeps Monaco's workspace sanitizer override installed", () => {
    const root = json("package.json") as {
      devDependencies: Record<string, string>;
      overrides: Record<string, Record<string, string>>;
    };
    const site = json("apps/site/package.json") as {
      dependencies: Record<string, string>;
    };
    const lock = json("package-lock.json") as {
      packages: Record<string, { version?: string }>;
    };

    // npm 11 applies a root override to workspace dependencies only when the parent dependency
    // is also present at the root. Keep the duplicate Monaco declaration until upstream no
    // longer pins a vulnerable DOMPurify release.
    expect(root.devDependencies["monaco-editor"]).toBe(site.dependencies["monaco-editor"]);
    const patchedVersion = root.overrides["monaco-editor"]?.dompurify;
    expect(patchedVersion).toBe("3.4.14");
    expect(lock.packages["node_modules/dompurify"]?.version).toBe(patchedVersion);
  });
});
