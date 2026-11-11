import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readdir, readFile, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
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
    expect(manifest.files).toContain("!dist/testing/semantic-corpus.*");
    expect(manifest.files).toContain("!dist/testing/maintenance-diagnostics.*");
    expect(manifest.files).toContain("!dist/engine/client-audit-harness.*");
    expect(manifest.files).toContain("!dist/storage/indexeddb-audit-helpers.*");
    expect(manifest.files).toContain("!dist/storage/opfs/power-loss-model.*");
    expect(manifest.files).toContain("!dist/storage/opfs/leader-test-helpers.*");
  });

  it("keeps the unreleased rebuild core (src/next) out of the tarball until cut-over", async () => {
    const manifest = JSON.parse(
      await readFile(join(repoRoot, "packages", "core", "package.json"), "utf8"),
    ) as { files?: string[]; exports?: Record<string, unknown> };
    expect(manifest.files).toContain("!dist/next/**");
    expect(Object.keys(manifest.exports ?? {}).filter((path) => path.includes("next"))).toEqual([]);
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
    // The prepack hook strips comments and compacts whitespace in dist/**.js, retaining names
    // and declaration documentation. A dry-run pack runs the hook the way a publish does, so this
    // proves the wiring rather than the script alone.
    const { stripComments } = (await import("./strip-dist-comments.mjs")) as {
      stripComments: (source: string) => string;
    };
    const stripped = stripComments(
      "/** doc */\nexport function f(a) {\n  // note\n  return a + 1; /* trailing */\n}\n",
    );
    expect(stripped).not.toMatch(/\/\*|\/\//u);
    expect(stripped).toMatch(/export/u);
    expect(stripped).toContain("return a+1");
    expect(stripped).toContain("function f(a)");
    expect(stripped).not.toContain("\n  ");
    const module = (await import(`data:text/javascript,${encodeURIComponent(stripped)}`)) as {
      f: (value: number) => number;
    };
    expect(module.f(41)).toBe(42);
    const coreRoot = join(repoRoot, "packages", "core");
    const manifest = JSON.parse(await readFile(join(coreRoot, "package.json"), "utf8")) as {
      scripts?: Record<string, string>;
    };
    expect(manifest.scripts?.prepack).toBe(
      "node ../../scripts/prepare-package.mjs --strip-comments",
    );
    // npm's prepack hook rewrites JavaScript. Pack an isolated copy so concurrent bundle and
    // declaration tests always read the original build, never partially rewritten files.
    const originalOptimizer = await readFile(
      join(coreRoot, "dist", "engine", "optimizer.js"),
      "utf8",
    );
    const isolatedRoot = await mkdtemp(join(tmpdir(), "minnow-package-shape-"));
    const isolatedCore = join(isolatedRoot, "packages", "core");
    try {
      await cp(coreRoot, isolatedCore, { recursive: true });
      await mkdir(join(isolatedRoot, "scripts"));
      for (const script of ["prepare-package.mjs", "strip-dist-comments.mjs"]) {
        await cp(join(repoRoot, "scripts", script), join(isolatedRoot, "scripts", script));
      }
      await symlink(join(repoRoot, "node_modules"), join(isolatedRoot, "node_modules"), "dir");
      const output = execFileSync("npm", ["pack", "--dry-run", "--json"], {
        cwd: isolatedCore,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      });
      const [report] = JSON.parse(output.slice(output.indexOf("["))) as Array<{
        size: number;
        unpackedSize: number;
        files: Array<{ path: string }>;
      }>;
      expect(report?.files.map(({ path }) => path)).not.toContain("dist/engine/query-cache.d.ts");
      expect(report?.files.map(({ path }) => path)).not.toContain(
        "dist/testing/semantic-corpus.js",
      );
      expect(report?.files.map(({ path }) => path)).not.toContain(
        "dist/testing/maintenance-diagnostics.js",
      );
      expect(report?.files.map(({ path }) => path)).toContain("dist/engine/query.d.ts");
      expect(report?.files.filter(({ path }) => path.startsWith("dist/next/"))).toEqual([]);
      // prepare-package.mjs strips only the JavaScript it expects to ship; every shipped file
      // must come out stripped, so a too-wide skip rule fails here.
      for (const { path } of report?.files ?? []) {
        if (!path.endsWith(".js")) continue;
        const shipped = await readFile(join(isolatedCore, path), "utf8");
        expect(shipped, path).not.toMatch(/^\s*(?:\/\*\*|\/\/)/mu);
      }
      // Existing publication budgets also cover declaration pruning. Raised from 870,000 and
      // 4,300,000 with 0.10.1's coordination and recovery fixes, then to 920,000 and 4,500,000
      // when the interaction-plan simulator joined the published testing entry, then to 955,000
      // with the 0.16.0 audit (951,556 measured), then lowered to 954,000 when shared engine
      // helpers replaced duplicated copies (953,067 measured), and raised to 960,000 for PostgreSQL
      // datetime and interval text (959,373 measured), 961,000 with full-text fold coverage
      // (960,174 measured), 962,000 with UNIQUE builds under writes (961,154 measured), 973,000
      // with the single-row write paths and delta merges (972,128 measured), 978,000 with the
      // storage commit-path costs (977,096 measured), 980,000 with the engine's batch-write
      // paths (979,032 measured), 982,000 with TIMESTAMP columns without time zone (981,457
      // measured), 995,000 with packed segments (994,119 measured), and 1,001,000 with the
      // correctness audit (1,000,105 measured), then 1,005,000 for offline identity APIs
      // (about 1,004,000 measured), 1,007,000 for ordered live aggregate output (1,006,355
      // measured), 1,011,000 when older tabs step aside for a newer build (1,010,358
      // measured), and 1,012,000 when a swept spill owner takes its pages and the conformance kit
      // checks it (1,011,923 measured), then 1,014,000 with typed integer execution
      // (1,013,733 measured), then 1,015,000 when the parser learned to record declared numeric
      // widths for the rebuild core's lowering (1,014,321 measured), then 1,016,500 with the
      // PostgreSQL name-resolution and DISTINCT ON fixes (1,015,868 measured), then 1,017,750 with
      // the transaction-mode grammar and READ ONLY enforcement (1,017,120 measured), then
      // 1,018,250 with PostgreSQL's name, argument and comment errors (1,017,725 measured), then
      // 1,019,500 for the 0.20.0 SQL fixes (1,019,104 measured);
      // ratchets, not targets.
      // Integrated release/lowering front end: 1,021,845 packed bytes; cap raised to 1,022,500.
      // Frame compatibility metadata adds 402 packed bytes (SQL matrix + PostgreSQL profile),
      // taking cd21ba42's 1,022,296 bytes to 1,022,698. Counterfactual packs replacing only
      // those JSON files recover the baseline; no next/ or test files ship. Keep 302 bytes
      // of headroom without changing the unpacked budget or worker/client bundle caps.
      // Timestamp source/precision admission: 1,023,032 packed / 4,434,467 unpacked
      // (+200 / +734 versus JSON_TABLE metadata alone). The packed cap is exceeded by
      // 32 bytes: raise it once by 250 bytes; leave the unpacked cap and isolation checks.
      // Native-only interval/series provenance: 1,023,531 packed / 4,436,274 unpacked
      // (+499 / +1,807 versus eef0ee76). No next/ or test files ship. Raise packed once
      // by 500; keep the unpacked cap and every package-isolation assertion.
      expect(report?.size, "packed bytes").toBeLessThanOrEqual(1_023_750);
      expect(report?.unpackedSize, "unpacked bytes").toBeLessThanOrEqual(4_500_000);
      const emitted = await readFile(join(isolatedCore, "dist", "engine", "optimizer.js"), "utf8");
      expect(emitted).not.toContain("/**");
      expect(emitted).not.toMatch(/^\s*\/\//mu);
      expect(await readFile(join(coreRoot, "dist", "engine", "optimizer.js"), "utf8")).toBe(
        originalOptimizer,
      );
    } finally {
      await rm(isolatedRoot, { recursive: true, force: true });
    }
  }, 60_000);
});
