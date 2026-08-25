import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const repoRoot = join(import.meta.dirname, "..");
const publicPackages = ["core", "devtools", "export", "kysely", "react"] as const;

async function filesUnder(directory: string, prefix = ""): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const relative = join(prefix, entry.name);
    if (entry.isDirectory())
      files.push(...(await filesUnder(join(directory, entry.name), relative)));
    else files.push(relative);
  }
  return files;
}

describe("published package shape", () => {
  it.each(publicPackages)("ships the declared MIT license in @minnowdb/%s", async (name) => {
    await expect(readFile(join(repoRoot, "packages", name, "LICENSE"), "utf8")).resolves.toMatch(
      /^MIT License\n/u,
    );
  });

  it.each(publicPackages)("keeps development-only output out of @minnowdb/%s", async (name) => {
    const manifest = JSON.parse(
      await readFile(join(repoRoot, "packages", name, "package.json"), "utf8"),
    ) as { files?: string[] };

    // Published packages contain dist, not src. TypeScript's maps point back into that absent
    // source tree and therefore add archive/install bytes without enabling source debugging or
    // declaration navigation. If a package starts shipping src, revisit this assertion together.
    expect(manifest.files).toContain("!dist/**/*.map");
    expect(manifest.files).toContain("!dist/**/*.test.*");
    expect(manifest.files).toContain("!dist/**/*.spec.*");
    expect(manifest.files).toContain("!dist/*.tsbuildinfo");
  });

  it("cleans stale compiler output before every repository build", async () => {
    const manifest = JSON.parse(await readFile(join(repoRoot, "package.json"), "utf8")) as {
      scripts?: Record<string, string>;
    };
    expect(manifest.scripts?.clean).toBe("node scripts/clean-output.mjs");
    expect(manifest.scripts?.build).toMatch(/^npm run clean && /u);
  });

  it("has no emitted core JavaScript without a current source file", async () => {
    const outputRoot = join(repoRoot, "packages", "core", "dist");
    const sourceRoot = join(repoRoot, "packages", "core", "src");
    const outputs = (await filesUnder(outputRoot)).filter((file) => file.endsWith(".js"));
    for (const output of outputs) {
      const source = join(sourceRoot, output.replace(/\.js$/u, ".ts"));
      await expect(readFile(source, "utf8"), output).resolves.toBeTypeOf("string");
    }
  });

  it("does not emit map references to source files the packages do not ship", async () => {
    const config = JSON.parse(await readFile(join(repoRoot, "tsconfig.base.json"), "utf8")) as {
      compilerOptions?: { sourceMap?: boolean; declarationMap?: boolean };
    };
    expect(config.compilerOptions?.sourceMap).toBe(false);
    expect(config.compilerOptions?.declarationMap).toBe(false);
  });

  it("keeps the core test fixture helpers out of the tarball", async () => {
    const manifest = JSON.parse(
      await readFile(join(repoRoot, "packages", "core", "package.json"), "utf8"),
    ) as { files?: string[] };
    expect(manifest.files).toContain("!dist/storage/fixture-shape.*");
    expect(manifest.files).toContain("!dist/engine/storage-test-helpers.*");
  });

  it("boots browser tests through published package subpaths", async () => {
    const workerEntry = await readFile(
      join(repoRoot, "packages", "core", "browser", "published-worker.ts"),
      "utf8",
    );
    expect(workerEntry).toContain('import "@minnowdb/core/worker"');
    for (const file of ["run.ts", "worker-run.ts", "opfs-run.ts"]) {
      const source = await readFile(join(repoRoot, "packages", "core", "browser", file), "utf8");
      expect(source, file).not.toContain('from "../src/');
    }
  });

  it("uses a relative application worker wrapper around the published worker entry", async () => {
    const wrapper = await readFile(
      join(repoRoot, "apps", "site", "components", "playground", "minnow-worker.ts"),
      "utf8",
    );
    const consoleSource = await readFile(
      join(repoRoot, "apps", "site", "components", "playground", "console.tsx"),
      "utf8",
    );
    expect(wrapper).toContain('import "@minnowdb/core/worker"');
    expect(consoleSource).toContain('new URL("./minnow-worker.ts", import.meta.url)');
    expect(consoleSource).not.toContain('new URL("@minnowdb/core/worker"');
  });
});
