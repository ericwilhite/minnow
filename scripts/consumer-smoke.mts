/**
 * Installs the package tarballs into a fresh project outside the workspace, then typechecks,
 * bundles, and runs that project. Workspace links cannot make this test pass: every @minnowdb
 * dependency is a file: reference to the archive npm pack actually produced.
 */
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { chromium } from "@playwright/test";

const repoRoot = path.resolve(import.meta.dirname, "..");
const fixtureRoot = path.join(import.meta.dirname, "consumer-smoke", "fixture");
const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";
const binary = (root: string, name: string): string =>
  path.join(root, "node_modules", ".bin", process.platform === "win32" ? `${name}.cmd` : name);

interface PackageManifest {
  name: string;
  version: string;
  private?: boolean;
}

interface PackedPackage extends PackageManifest {
  archive: string;
}

function run(command: string, args: string[], cwd: string): string {
  const result = spawnSync(command, args, { cwd, encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed:\n${result.stdout.trim()}\n${result.stderr.trim()}`,
    );
  }
  return result.stdout.trim();
}

function publicPackages(): PackageManifest[] {
  return readdirSync(path.join(repoRoot, "packages"), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(repoRoot, "packages", entry.name, "package.json"))
    .filter((file) => existsSync(file))
    .map((file) => JSON.parse(readFileSync(file, "utf8")) as PackageManifest)
    .filter((manifest) => manifest.private !== true)
    .sort((a, b) => a.name.localeCompare(b.name));
}

function packPackages(archiveRoot: string, cacheRoot: string): PackedPackage[] {
  return publicPackages().map((manifest) => {
    const output = run(
      npmCommand,
      [
        "pack",
        "--workspace",
        manifest.name,
        "--pack-destination",
        archiveRoot,
        "--cache",
        cacheRoot,
        "--json",
      ],
      repoRoot,
    );
    const reports = JSON.parse(output) as Array<{ filename?: string }>;
    const filename = reports[0]?.filename;
    if (filename === undefined) throw new Error(`npm pack did not name ${manifest.name}'s archive`);
    const archive = path.join(archiveRoot, path.basename(filename));
    if (!existsSync(archive)) throw new Error(`npm pack did not create ${archive}`);
    return { ...manifest, archive };
  });
}

function installedVersion(name: string): string {
  const manifest = JSON.parse(
    readFileSync(path.join(repoRoot, "node_modules", ...name.split("/"), "package.json"), "utf8"),
  ) as { version?: string };
  if (manifest.version === undefined) throw new Error(`${name} has no installed version`);
  return manifest.version;
}

function writeConsumerManifest(consumerRoot: string, packages: PackedPackage[]): void {
  const dependencies = Object.fromEntries(
    packages.map((entry) => [entry.name, pathToFileURL(entry.archive).href]),
  );
  Object.assign(dependencies, {
    kysely: installedVersion("kysely"),
    react: installedVersion("react"),
    "react-dom": installedVersion("react-dom"),
  });
  const devDependencies = Object.fromEntries(
    ["@types/react", "@types/react-dom", "typescript", "vite"].map((name) => [
      name,
      installedVersion(name),
    ]),
  );
  writeFileSync(
    path.join(consumerRoot, "package.json"),
    `${JSON.stringify(
      {
        name: "minnow-packed-consumer-smoke",
        private: true,
        type: "module",
        dependencies,
        devDependencies,
      },
      null,
      2,
    )}\n`,
  );
}

function verifyInstalledPackages(consumerRoot: string, packages: PackedPackage[]): void {
  for (const entry of packages) {
    const packageRoot = path.join(consumerRoot, "node_modules", ...entry.name.split("/"));
    const installed = JSON.parse(readFileSync(path.join(packageRoot, "package.json"), "utf8")) as {
      version?: string;
    };
    assert.equal(installed.version, entry.version, `${entry.name} installed the wrong version`);
    assert.equal(existsSync(path.join(packageRoot, "src")), false, `${entry.name} shipped src`);
    assert.equal(existsSync(path.join(packageRoot, "dist")), true, `${entry.name} omitted dist`);
  }
}

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Could not reserve a port");
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return address.port;
}

async function waitForServer(url: string, preview: ChildProcessWithoutNullStreams): Promise<void> {
  const deadline = Date.now() + 15_000;
  for (;;) {
    if (preview.exitCode !== null)
      throw new Error(`Vite preview exited with ${String(preview.exitCode)}`);
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch {
      // The preview process has not bound its socket yet.
    }
    if (Date.now() >= deadline) throw new Error("Timed out waiting for the consumer preview");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

async function stop(preview: ChildProcessWithoutNullStreams): Promise<void> {
  if (preview.exitCode !== null) return;
  preview.kill("SIGTERM");
  await Promise.race([
    new Promise<void>((resolve) => preview.once("exit", () => resolve())),
    new Promise<void>((resolve) => setTimeout(resolve, 5_000)),
  ]);
  if (preview.exitCode === null) preview.kill("SIGKILL");
}

async function browserSmoke(consumerRoot: string): Promise<unknown> {
  const port = await freePort();
  const url = `http://127.0.0.1:${String(port)}`;
  const preview = spawn(
    binary(consumerRoot, "vite"),
    ["preview", "--host", "127.0.0.1", "--port", String(port), "--strictPort"],
    { cwd: consumerRoot, stdio: "pipe" },
  );
  let previewOutput = "";
  preview.stdout.on("data", (chunk: Buffer) => {
    previewOutput += chunk.toString();
  });
  preview.stderr.on("data", (chunk: Buffer) => {
    previewOutput += chunk.toString();
  });
  try {
    await waitForServer(url, preview);
    const browser = await chromium.launch();
    try {
      const page = await browser.newPage();
      page.setDefaultTimeout(120_000);
      const browserErrors: string[] = [];
      page.on("console", (message) => {
        if (message.type() === "error") browserErrors.push(message.text());
      });
      page.on("pageerror", (error) => browserErrors.push(error.message));
      await page.goto(url);
      await page.locator("#status").waitFor({ state: "visible" });
      assert.equal(await page.locator("#status").textContent(), "ready");
      // The fixture's main.ts declares this on Window, but the fixture is excluded from the
      // scripts tsconfig (it compiles against the packed tarballs), so the type lives here.
      const result = await page.evaluate(() =>
        (window as unknown as { runConsumerSmoke: () => Promise<unknown> }).runConsumerSmoke(),
      );
      assert.deepEqual(browserErrors, []);
      return result;
    } finally {
      await browser.close();
    }
  } catch (error) {
    throw new Error(`Packed consumer browser smoke failed:\n${previewOutput.trim()}`, {
      cause: error,
    });
  } finally {
    await stop(preview);
  }
}

const temporaryRoot = mkdtempSync(path.join(tmpdir(), "minnow-consumer-smoke-"));
try {
  const archiveRoot = path.join(temporaryRoot, "archives");
  const cacheRoot = path.join(temporaryRoot, "npm-cache");
  const consumerRoot = path.join(temporaryRoot, "consumer");
  mkdirSync(archiveRoot);
  cpSync(fixtureRoot, consumerRoot, { recursive: true });
  const packages = packPackages(archiveRoot, cacheRoot);
  writeConsumerManifest(consumerRoot, packages);
  console.log(`Packed ${packages.map((entry) => `${entry.name}@${entry.version}`).join(", ")}.`);

  run(
    npmCommand,
    [
      "install",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      "--package-lock=false",
      "--cache",
      cacheRoot,
    ],
    consumerRoot,
  );
  verifyInstalledPackages(consumerRoot, packages);
  console.log("Installed every tarball outside the workspace.");

  run(binary(consumerRoot, "tsc"), ["--project", "tsconfig.json"], consumerRoot);
  run(binary(consumerRoot, "tsc"), ["--project", "tsconfig.nodenext.json"], consumerRoot);
  console.log("Resolved every public entry point with Bundler and NodeNext TypeScript consumers.");

  run(binary(consumerRoot, "vite"), ["build"], consumerRoot);
  console.log("Built the consumer application and its package worker with Vite.");

  const result = await browserSmoke(consumerRoot);
  assert.deepEqual(result, {
    migrationTables: ["items"],
    workerRows: [
      { id: 1, name: "Ada", score: 10 },
      { id: 2, name: "Grace", score: 20 },
    ],
    kyselyScore: 30,
    liveRows: 3,
    csv: "id,name,score\r\n1,Ada,10\r\n2,Grace,20\r\n3,Linus,30\r\n",
    ndjsonLines: 3,
    snapshotTables: 1,
    restoredRows: 3,
    reopenedRows: 3,
    reactText: "3 rows",
    devtoolsMounted: true,
  });
  console.log("Consumer browser smoke passed:", result);
} finally {
  rmSync(temporaryRoot, { recursive: true, force: true });
}
