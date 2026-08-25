/**
 * The workspace's versions.
 *
 * Every published package shares a major version, and moves independently inside it. A major is
 * the compatibility line: `@minnowdb/kysely@2.x` works with later `@minnowdb/core@2.x` releases
 * at or above its declared dependency floor. The adapter is built on the engine's published
 * primitives, and a change that breaks that cross-package contract is by definition a major one.
 * Below that line each package releases at its own pace, so a fix to the devtools console does not
 * have to drag the engine's version along with it.
 *
 * That invariant is what this script writes and checks. Internal dependencies use a floor at the
 * sibling release this package was built against and a ceiling at the next shared major, so npm
 * refuses an incompatible engine version on its own.
 *
 *   npm run version:check                      verify the workspace agrees with itself
 *   npm run version:set -- minor @minnowdb/core  move one package inside the shared major
 *   npm run version:set -- 0.1.1 @minnowdb/kysely
 *   npm run version:set -- major               move every package to the next major, together
 *
 * The private packages — the repository root and the docs site — mirror `@minnowdb/core`, which
 * is the version the site documents.
 *
 * None of this describes the data the engine writes. The on-disk encoding carries its own
 * `BLOCK_FORMAT_VERSION`, which moves on its own schedule and is what the fixtures in
 * packages/core/format-fixtures/ are keyed to.
 */
import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const versionsFile = path.join(repoRoot, "apps/site/public/versions.json");
const SEMVER = /^(\d+)\.(\d+)\.(\d+)(?:-[0-9A-Za-z.-]+)?$/;
const DEPENDENCY_FIELDS = ["dependencies", "devDependencies", "peerDependencies"] as const;
/** The package the docs describe, and the one the private manifests follow. */
const ENGINE = "@minnowdb/core";

interface Manifest {
  name?: string;
  version?: string;
  private?: boolean;
  workspaces?: string[];
  [field: string]: unknown;
}

interface WorkspacePackage {
  /** Path relative to the repository root, as it reads in an error message. */
  relativePath: string;
  file: string;
  manifest: Manifest;
}

interface Version {
  major: number;
  minor: number;
  patch: number;
}

function parseVersion(version: string | undefined): Version | undefined {
  const parsed = SEMVER.exec(version ?? "");
  if (parsed === null) return undefined;
  return { major: Number(parsed[1]), minor: Number(parsed[2]), patch: Number(parsed[3]) };
}

/** Ordered comparison, so a dependency floor can be checked against what is actually installed. */
function atMost(floor: Version, version: Version): boolean {
  if (floor.major !== version.major) return floor.major < version.major;
  if (floor.minor !== version.minor) return floor.minor < version.minor;
  return floor.patch <= version.patch;
}

/** The range a package depends on a sibling by: this version or later, inside the shared major. */
function dependencyRange(siblingVersion: string, major: number): string {
  return `>=${siblingVersion} <${String(major + 1)}.0.0`;
}

/** Which major compatibility line a documentation build belongs to. */
function docsLabel(version: string): string {
  const parsed = parseVersion(version);
  if (parsed === undefined) return version;
  return `${String(parsed.major)}.x`;
}

/** Where a frozen major line is served after the next major replaces it at the site root. */
function docsArchivePath(version: string): string {
  const parsed = parseVersion(version);
  return parsed === undefined ? `/${version}/` : `/v${String(parsed.major)}/`;
}

function readManifest(file: string): Manifest {
  return JSON.parse(readFileSync(file, "utf8")) as Manifest;
}

function packageAt(relativePath: string): WorkspacePackage {
  const file = path.join(repoRoot, relativePath);
  return { relativePath, file, manifest: readManifest(file) };
}

const root = packageAt("package.json");

/** Every package the root manifest claims, plus the root itself, in a stable order. */
function workspacePackages(): WorkspacePackage[] {
  const directories = (root.manifest.workspaces ?? []).flatMap((pattern) => {
    // The manifest uses `packages/*` and `apps/*`; anything fancier is not worth a glob library.
    if (!pattern.endsWith("/*")) return [pattern];
    const parent = pattern.slice(0, -2);
    return readdirSync(path.join(repoRoot, parent), { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => `${parent}/${entry.name}`)
      .sort();
  });

  // A directory without a manifest is not a workspace, whatever the glob matched.
  return [
    root,
    ...directories
      .map((directory) => `${directory}/package.json`)
      .filter((relativePath) => existsSync(path.join(repoRoot, relativePath)))
      .map(packageAt),
  ];
}

/** The packages that are actually published, which is what a user can install a mismatch of. */
function isPublished(entry: WorkspacePackage): boolean {
  return entry.manifest.private !== true && entry !== root;
}

function internalDependencies(manifest: Manifest): Array<[string, string]> {
  return DEPENDENCY_FIELDS.flatMap((field) => {
    const dependencies = manifest[field] as Record<string, string> | undefined;
    return Object.entries(dependencies ?? {}).filter(([name]) => name.startsWith("@minnowdb/"));
  });
}

interface DocsVersion {
  version: string;
  label: string;
  path: string;
}

interface DocsVersions {
  current: DocsVersion;
  archived: DocsVersion[];
}

function readDocsVersions(): DocsVersions {
  return JSON.parse(readFileSync(versionsFile, "utf8")) as DocsVersions;
}

function check(): number {
  const packages = workspacePackages();
  const published = packages.filter(isPublished);
  const problems: string[] = [];
  const versions = new Map<string, string>();

  for (const entry of published) {
    const version = entry.manifest.version;
    if (parseVersion(version) === undefined) {
      problems.push(`${entry.relativePath}: version "${version ?? ""}" is not a semantic version`);
      continue;
    }
    versions.set(entry.manifest.name ?? entry.relativePath, version ?? "");
  }

  const majors = new Set([...versions.values()].map((version) => parseVersion(version)?.major));
  if (majors.size > 1) {
    problems.push(
      `the published packages are on different majors, and a major is the compatibility line: ${[
        ...versions,
      ]
        .map(([name, version]) => `${name}@${version}`)
        .join(", ")}`,
    );
  }
  const major = [...majors][0] ?? 0;

  for (const entry of packages) {
    for (const [name, range] of internalDependencies(entry.manifest)) {
      const siblingVersion = versions.get(name);
      if (siblingVersion === undefined) continue;
      const expectedCeiling = `<${String(major + 1)}.0.0`;
      const floorText = /^>=(\S+)/.exec(range)?.[1];
      const floor = parseVersion(floorText);
      if (floor === undefined || !range.endsWith(expectedCeiling)) {
        problems.push(
          `${entry.relativePath}: depends on ${name}@${range}, and the compatibility range is ` +
            `"${dependencyRange(siblingVersion, major)}" or an older floor below the same ceiling`,
        );
      } else if (!atMost(floor, parseVersion(siblingVersion) ?? floor)) {
        problems.push(
          `${entry.relativePath}: requires ${name}@${range}, and ${name} is on ${siblingVersion}`,
        );
      }
    }
  }

  // The site publishes this file, and the picker in the docs sidebar reads it. A version left
  // behind here is a reader told they are on a release that no longer exists.
  const engineVersion = versions.get(ENGINE);
  const docsVersions = readDocsVersions();
  const documented = docsVersions.current;
  if (engineVersion !== undefined && documented.version !== engineVersion) {
    problems.push(
      `apps/site/public/versions.json: the current docs claim ${documented.version}, and ${ENGINE} is on ${engineVersion}`,
    );
  }
  if (engineVersion !== undefined && documented.label !== docsLabel(engineVersion)) {
    problems.push(
      `apps/site/public/versions.json: the current docs label is "${documented.label}", and major ${String(major)} must be listed as "${docsLabel(engineVersion)}"`,
    );
  }
  if (documented.path !== "/") {
    problems.push(
      `apps/site/public/versions.json: the current docs must be served at "/", not "${documented.path}"`,
    );
  }

  const documentedMajors = new Set<number>();
  if (engineVersion !== undefined)
    documentedMajors.add(parseVersion(engineVersion)?.major ?? major);
  for (const archived of docsVersions.archived) {
    const archivedVersion = parseVersion(archived.version);
    if (archivedVersion === undefined) {
      problems.push(
        `apps/site/public/versions.json: archived docs version "${archived.version}" is not semantic`,
      );
      continue;
    }
    if (documentedMajors.has(archivedVersion.major)) {
      problems.push(
        `apps/site/public/versions.json: major ${String(archivedVersion.major)} is listed more than once`,
      );
    }
    documentedMajors.add(archivedVersion.major);
    if (archived.label !== docsLabel(archived.version)) {
      problems.push(
        `apps/site/public/versions.json: ${archived.version} must be labeled "${docsLabel(archived.version)}"`,
      );
    }
    if (archived.path !== docsArchivePath(archived.version)) {
      problems.push(
        `apps/site/public/versions.json: ${archived.version} must be served at "${docsArchivePath(archived.version)}"`,
      );
    }
  }

  if (problems.length > 0) {
    console.error("The workspace's versions disagree:\n");
    for (const problem of problems) console.error(`  ${problem}`);
    console.error("\nRun `npm run version:set` to write them, or fix the manifests by hand.");
    return 1;
  }

  console.log(
    `Major ${String(major)}: ${[...versions].map(([name, version]) => `${name}@${version}`).join(", ")}.`,
  );
  return 0;
}

/** Resolve `major`/`minor`/`patch` against a current version, or take a literal one. */
function resolveVersion(argument: string, current: string): string {
  if (SEMVER.test(argument)) return argument;
  const parsed = parseVersion(current);
  if (parsed === undefined) throw new Error(`the version "${current}" is not a semantic version`);

  switch (argument) {
    case "major":
      return `${String(parsed.major + 1)}.0.0`;
    case "minor":
      return `${String(parsed.major)}.${String(parsed.minor + 1)}.0`;
    case "patch":
      return `${String(parsed.major)}.${String(parsed.minor)}.${String(parsed.patch + 1)}`;
    default:
      throw new Error(`"${argument}" is neither a semantic version nor major, minor, or patch`);
  }
}

/**
 * A textual edit rather than a rewrite of the parsed object: package.json files are read by
 * people and diffed in review, and JSON.stringify would reorder nothing but reformat plenty.
 */
function writeManifest(
  entry: WorkspacePackage,
  version: string | undefined,
  major: number,
  updateDependencies: boolean,
): void {
  let text = readFileSync(entry.file, "utf8");
  if (version !== undefined) {
    text = text.replace(
      /^(\s*"version":\s*")[^"]*(",?)$/m,
      (_match, prefix: string, suffix: string) => `${prefix}${version}${suffix}`,
    );
  }
  if (updateDependencies) {
    for (const [name] of internalDependencies(entry.manifest)) {
      const sibling = siblingVersions.get(name);
      if (sibling === undefined) continue;
      const dependency = new RegExp(`^(\\s*"${name.replace("/", "\\/")}":\\s*")[^"]*(",?)$`, "m");
      text = text.replace(
        dependency,
        (_match, prefix: string, suffix: string) =>
          `${prefix}${dependencyRange(sibling, major)}${suffix}`,
      );
    }
  }
  writeFileSync(entry.file, text);
}

const siblingVersions = new Map<string, string>();

function set(argument: string, packageName: string | undefined): number {
  const packages = workspacePackages();
  const published = packages.filter(isPublished);
  const currentMajor = parseVersion(published[0]?.manifest.version)?.major ?? 0;

  const target =
    packageName === undefined
      ? undefined
      : published.find((entry) => entry.manifest.name === packageName);
  if (packageName !== undefined && target === undefined) {
    console.error(
      `"${packageName}" is not a published package. They are: ${published.map((entry) => entry.manifest.name).join(", ")}.`,
    );
    return 2;
  }

  // A major is the compatibility line, so it never moves for one package on its own.
  const wholeWorkspace =
    argument === "major" ||
    (SEMVER.test(argument) && parseVersion(argument)?.major !== currentMajor);

  if (wholeWorkspace && target !== undefined) {
    console.error(
      `${argument} moves the major, which every package shares. Run it without a package name.`,
    );
    return 2;
  }
  if (!wholeWorkspace && target === undefined) {
    console.error(
      "Inside a major each package moves on its own, so this needs a package name:\n" +
        `  npm run version:set -- ${argument} ${ENGINE}`,
    );
    return 2;
  }

  const moved = new Map<string, string>();
  let major = currentMajor;

  if (wholeWorkspace) {
    const next = resolveVersion(argument, published[0]?.manifest.version ?? "0.0.0");
    major = parseVersion(next)?.major ?? currentMajor;
    for (const entry of packages) {
      moved.set(entry.relativePath, next);
    }
  } else {
    const next = resolveVersion(argument, target?.manifest.version ?? "0.0.0");
    if (parseVersion(next)?.major !== currentMajor) {
      console.error(`${next} leaves major ${String(currentMajor)}, which every package shares.`);
      return 2;
    }
    moved.set(target?.relativePath ?? "", next);
    // The private manifests mirror the engine, since that is the version the site documents.
    if (target?.manifest.name === ENGINE) {
      for (const entry of packages.filter((candidate) => !isPublished(candidate))) {
        moved.set(entry.relativePath, next);
      }
    }
  }

  // A released package's floor records the siblings that release was built against. Do not
  // silently change an unchanged published manifest: the publisher keys on version and would not
  // publish those new bytes. Moved packages and private workspace manifests follow the versions
  // selected by this command.
  siblingVersions.clear();
  for (const entry of published) {
    siblingVersions.set(
      entry.manifest.name ?? "",
      moved.get(entry.relativePath) ?? entry.manifest.version ?? "",
    );
  }

  for (const entry of packages) {
    const next = moved.get(entry.relativePath);
    writeManifest(entry, next, major, next !== undefined || !isPublished(entry));
  }

  const engineVersion = siblingVersions.get(ENGINE) ?? "";
  const list = readDocsVersions();
  const previous = list.current;
  list.current = { version: engineVersion, label: docsLabel(engineVersion), path: "/" };
  writeFileSync(versionsFile, `${JSON.stringify(list, null, 2)}\n`);

  // npm ci fails outright when the lockfile disagrees with a manifest, so the lockfile is part of
  // the version bump rather than something to remember afterwards.
  const install = spawnSync("npm", ["install", "--package-lock-only"], {
    cwd: repoRoot,
    stdio: "inherit",
  });
  if (install.status !== 0) {
    console.error("The manifests were written, but refreshing package-lock.json failed.");
    return install.status ?? 1;
  }

  for (const entry of published) {
    const next = moved.get(entry.relativePath);
    if (next !== undefined) console.log(`${entry.manifest.name ?? ""} -> ${next}`);
  }
  if (docsLabel(engineVersion) !== previous.label) {
    const archivePath = docsArchivePath(previous.version).replace(/\/$/, "");
    console.log(
      `The docs at / now document ${docsLabel(engineVersion)}. To keep ${previous.label} readable, ` +
        `build @minnowdb/core@${previous.version} with SITE_BASE_PATH=${archivePath}, copy its ` +
        `static output into apps/site/public${archivePath}, and add it to the \`archived\` list ` +
        "in apps/site/public/versions.json before publishing the new major.",
    );
  }
  console.log(
    "Commit the result. After publication succeeds, the release workflow tags each published package.",
  );
  return 0;
}

const [mode, argument, packageName] = process.argv.slice(2);
if (mode === "check") {
  process.exit(check());
} else if (mode === "set" && argument !== undefined) {
  process.exit(set(argument, packageName));
} else {
  console.error(
    "Usage: version.mts check | set major | set <minor|patch|version> <@minnowdb/package>",
  );
  process.exit(2);
}
