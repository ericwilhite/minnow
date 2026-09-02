import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const repoRoot = join(import.meta.dirname, "..");
const packagesRoot = join(repoRoot, "packages");
const publicPackages = readdirSync(packagesRoot, { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name)
  .filter((name) => existsSync(join(packagesRoot, name, "package.json")))
  .filter((name) => {
    const manifest = JSON.parse(readFileSync(join(packagesRoot, name, "package.json"), "utf8")) as {
      private?: boolean;
    };
    return manifest.private !== true;
  })
  .sort();

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
    expect(manifest.files).toContain("!dist/testing/seeds.*");
    expect(manifest.files).toContain("!dist/testing/oracle.*");
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

describe("published core tarball", () => {
  it("ships its JavaScript without comments and stays under the packed-size budget", async () => {
    // The prepack hook strips comments from dist/**.js (never from the declarations), which is
    // a fifth of the tarball. A dry-run pack runs the hook the way a publish does, so this
    // proves the wiring rather than the script alone.
    const { stripComments } = (await import("./strip-dist-comments.mjs")) as {
      stripComments: (source: string) => string;
    };
    const stripped = stripComments(
      "/** doc */\nexport function f(a) {\n  // note\n  return a + 1; /* trailing */\n}\n",
    );
    expect(stripped).not.toMatch(/\/\*|\/\//u);
    expect(stripped).toMatch(/export/u);
    expect(stripped).toContain("return a + 1;");
    const coreRoot = join(repoRoot, "packages", "core");
    const manifest = JSON.parse(await readFile(join(coreRoot, "package.json"), "utf8")) as {
      scripts?: Record<string, string>;
    };
    expect(manifest.scripts?.prepack).toBe("node ../../scripts/strip-dist-comments.mjs dist");
    const output = execFileSync("npm", ["pack", "--dry-run", "--json"], {
      cwd: coreRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
    const [report] = JSON.parse(output.slice(output.indexOf("["))) as Array<{
      size: number;
      unpackedSize: number;
    }>;
    // Measured after stripping: 799 KB packed / 3.98 MB unpacked (1,003 KB / 4.89 MB before).
    expect(report?.size, "packed bytes").toBeLessThanOrEqual(850_000);
    expect(report?.unpackedSize, "unpacked bytes").toBeLessThanOrEqual(4_200_000);
    const emitted = await readFile(join(coreRoot, "dist", "engine", "optimizer.js"), "utf8");
    expect(emitted).not.toContain("/**");
    expect(emitted).not.toMatch(/^\s*\/\//mu);
  }, 60_000);
});
