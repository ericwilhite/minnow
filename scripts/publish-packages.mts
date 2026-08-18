/**
 * Publishes any package whose version is not on the registry yet.
 *
 * The version in the manifest is the release. There is nothing else to remember: bump a package
 * with `npm run version:set`, push it, and once CI is green this publishes exactly the packages
 * whose version npm does not already have, then tags each one. Re-running publishes nothing,
 * because the registry is the record of what has shipped — so a rerun after a failure is safe,
 * and a push that changes no version is a no-op.
 *
 * In CI there is no token: npm is configured to trust the release workflow and hands it a
 * short-lived credential when it asks, which is also what signs the provenance attestation.
 * Provenance is only attached when the repository being built is the one the manifests name,
 * because npm rejects a statement that points anywhere else.
 *
 * Run locally it publishes with whatever `npm whoami` says, which is how a package is created in
 * the first place — a trusted publisher can only be configured for a package that exists.
 */
import { spawnSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dryRun = process.argv.includes("--dry-run");
const inCI = process.env.GITHUB_ACTIONS === "true";

interface Manifest {
  name?: string;
  version?: string;
  private?: boolean;
  repository?: { url?: string } | string;
}

interface Publishable {
  name: string;
  version: string;
}

function run(command: string, args: string[]): { status: number; stdout: string; stderr: string } {
  const result = spawnSync(command, args, { cwd: repoRoot, encoding: "utf8" });
  return { status: result.status ?? 1, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

/** Every publishable package in the workspace, read from the manifests rather than a list here. */
function publishable(): Publishable[] {
  return readdirSync(path.join(repoRoot, "packages"), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(repoRoot, "packages", entry.name, "package.json"))
    .map((file) => JSON.parse(readFileSync(file, "utf8")) as Manifest)
    .filter(
      (manifest): manifest is Manifest & Publishable =>
        manifest.private !== true &&
        typeof manifest.name === "string" &&
        typeof manifest.version === "string",
    )
    .map((manifest) => ({ name: manifest.name, version: manifest.version }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Whether the registry already has this exact version. A missing package and a missing version
 * both answer no; anything else — a network failure, an expired token — throws, because treating
 * "I could not tell" as "already published" would skip a release in silence.
 */
function alreadyPublished(entry: Publishable): boolean {
  const result = run("npm", ["view", `${entry.name}@${entry.version}`, "version"]);
  if (result.status === 0 && result.stdout.trim().length > 0) return true;
  if (/E404|404 Not Found|No match found/.test(result.stderr)) return false;
  throw new Error(`npm view ${entry.name}@${entry.version} failed:\n${result.stderr.trim()}`);
}

/**
 * What a package would actually ship. Tests and build metadata are compiled into `dist` beside
 * the code, so an unguarded `files` field publishes them — 40% of the engine's tarball, once.
 */
function packedFiles(name: string): string[] {
  const result = run("npm", ["pack", "--workspace", name, "--dry-run", "--json"]);
  if (result.status !== 0) throw new Error(`npm pack ${name} failed:\n${result.stderr.trim()}`);
  const [report] = JSON.parse(result.stdout) as Array<{ files: Array<{ path: string }> }>;
  return (report?.files ?? []).map((file) => file.path);
}

function refuseStrayFiles(entry: Publishable): void {
  const stray = packedFiles(entry.name).filter((file) =>
    /\.(test|spec)\.|\.tsbuildinfo$/.test(file),
  );
  if (stray.length > 0) {
    throw new Error(
      `${entry.name} would ship ${String(stray.length)} file(s) that are not part of the ` +
        `package, starting with ${stray[0] ?? ""}. Narrow "files" in its manifest.`,
    );
  }
}

/** True when the manifests name the repository this is running in, which is what npm verifies. */
function provenanceAvailable(): boolean {
  if (!inCI) return false;
  const manifest = JSON.parse(
    readFileSync(path.join(repoRoot, "packages/core/package.json"), "utf8"),
  ) as Manifest;
  const declared =
    typeof manifest.repository === "string"
      ? manifest.repository
      : (manifest.repository?.url ?? "");
  const building = process.env.GITHUB_REPOSITORY ?? "";
  return building.length > 0 && declared.includes(building);
}

const provenance = provenanceAvailable();
const pending = publishable().filter((entry) => !alreadyPublished(entry));

if (pending.length === 0) {
  console.log("Every package's version is already on npm. Nothing to publish.");
  process.exit(0);
}

console.log(
  `Publishing ${pending.map((entry) => `${entry.name}@${entry.version}`).join(", ")}` +
    `${provenance ? " with provenance" : ""}.`,
);

for (const entry of pending) {
  refuseStrayFiles(entry);

  const args = ["publish", "--workspace", entry.name, "--access", "public"];
  if (provenance) args.push("--provenance");
  if (dryRun) args.push("--dry-run");

  const published = spawnSync("npm", args, { cwd: repoRoot, stdio: "inherit" });
  if (published.status !== 0) {
    console.error(`Publishing ${entry.name}@${entry.version} failed.`);
    process.exit(published.status ?? 1);
  }

  // The tag records what was published, after the fact, so a failed publish leaves no tag behind
  // claiming otherwise.
  const tag = `${entry.name}@${entry.version}`;
  if (dryRun || !inCI) {
    console.log(`Would tag ${tag}.`);
    continue;
  }
  const tagged = run("git", ["tag", tag]);
  if (tagged.status !== 0) console.error(`Could not tag ${tag}: ${tagged.stderr.trim()}`);
  else {
    const pushed = run("git", ["push", "origin", tag]);
    if (pushed.status !== 0) console.error(`Could not push ${tag}: ${pushed.stderr.trim()}`);
  }
}

console.log(`Published ${String(pending.length)} package${pending.length === 1 ? "" : "s"}.`);
