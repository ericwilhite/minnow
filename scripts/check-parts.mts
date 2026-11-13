/**
 * Proves a split run ran everything exactly once: the union of what CI's parts actually ran
 * equals what one unsplit run would run, with nothing twice.
 *
 *   vite-node scripts/check-parts.mts vitest <dir of vitest JSON reports> [vitest args...]
 *   vite-node scripts/check-parts.mts playwright <browser> <dir of Playwright JSON reports>
 *
 * The parts choose their files by measured duration (scripts/test-parts.mjs); every part must
 * compute the same assignment, so anything that differs between them (an environment variable
 * set on one matrix entry, a file list that differs) silently skips or repeats work while every
 * part passes. This reads the reports the parts uploaded, which list what each one ran, and
 * compares them with the runner's own listing of the unsplit run. Run it after the parts.
 */
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));

function reports(dir: string): string[] {
  if (!statSync(dir).isDirectory()) return [dir];
  return readdirSync(dir).flatMap((name) => reports(join(dir, name)));
}

/** Repository-relative with forward slashes, whatever machine wrote the path. */
function repositoryPath(path: string): string {
  const posix = path.split("\\").join("/");
  for (const marker of ["/packages/", "/scripts/", "/apps/"]) {
    const at = posix.indexOf(marker);
    if (at >= 0) return posix.slice(at + 1);
  }
  return relative(root, path).split("\\").join("/");
}

function compare(what: string, expected: readonly string[], ran: readonly string[]): string[] {
  const problems: string[] = [];
  const counts = new Map<string, number>();
  for (const item of ran) counts.set(item, (counts.get(item) ?? 0) + 1);
  const wanted = new Set(expected);
  for (const item of wanted) if (!counts.has(item)) problems.push(`${what} never ran: ${item}`);
  for (const [item, count] of counts) {
    if (!wanted.has(item)) problems.push(`${what} ran but is not in the unsplit run: ${item}`);
    if (count > 1) problems.push(`${what} ran ${String(count)} times: ${item}`);
  }
  return problems;
}

function checkVitest(dir: string, args: readonly string[]): string[] {
  const listing = JSON.parse(
    execFileSync("npx", ["vitest", "list", "--filesOnly", "--json", ...args], {
      cwd: root,
      encoding: "utf8",
      maxBuffer: 64 << 20,
    }),
  ) as Array<{ file: string; projectName?: string }>;
  const expected = listing.map((entry) => repositoryPath(entry.file));
  const ran = reports(dir)
    .filter((path) => path.endsWith(".json"))
    .flatMap(
      (path) =>
        (JSON.parse(readFileSync(path, "utf8")) as { testResults: Array<{ name: string }> })
          .testResults,
    )
    .map((result) => repositoryPath(result.name));
  console.log(`vitest: ${String(expected.length)} files listed, ${String(ran.length)} run`);
  return compare("test file", expected, ran);
}

interface Suite {
  title?: string;
  suites?: Suite[];
  specs?: Array<{
    title: string;
    file: string;
    line: number;
    tests: Array<{ projectName: string }>;
  }>;
}

function playwrightTests(
  report: { config: { rootDir: string }; suites: Suite[] },
  browser: string,
) {
  const out: string[] = [];
  const walk = (suite: Suite, path: readonly string[]) => {
    const here = suite.title === undefined ? path : [...path, suite.title];
    for (const spec of suite.specs ?? [])
      for (const test of spec.tests)
        if (test.projectName === browser)
          out.push(
            `${repositoryPath(join(report.config.rootDir, spec.file))}:${String(spec.line)} ${[...here.slice(1), spec.title].join(" > ")}`,
          );
    for (const child of suite.suites ?? []) walk(child, here);
  };
  for (const suite of report.suites) walk(suite, []);
  return out;
}

function checkPlaywright(browser: string, dir: string): string[] {
  const env = { ...process.env };
  delete env.MINNOW_BROWSER_PART;
  const listing = JSON.parse(
    execFileSync(
      "npx",
      [
        "playwright",
        "test",
        "--config",
        "playwright.library.config.ts",
        "--project",
        browser,
        "--list",
        "--reporter=json",
      ],
      { cwd: root, encoding: "utf8", env, maxBuffer: 64 << 20 },
    ),
  ) as { config: { rootDir: string }; suites: Suite[] };
  const expected = playwrightTests(listing, browser);
  const ran = reports(dir)
    .filter((path) => path.endsWith(".json"))
    .flatMap((path) =>
      playwrightTests(
        JSON.parse(readFileSync(path, "utf8")) as { config: { rootDir: string }; suites: Suite[] },
        browser,
      ),
    );
  console.log(
    `playwright ${browser}: ${String(expected.length)} tests listed, ${String(ran.length)} run`,
  );
  return compare(`${browser} test`, expected, ran);
}

const [mode, ...rest] = process.argv.slice(2);
let problems: string[];
if (mode === "vitest" && rest[0] !== undefined) problems = checkVitest(rest[0], rest.slice(1));
else if (mode === "playwright" && rest[0] !== undefined && rest[1] !== undefined)
  problems = checkPlaywright(rest[0], rest[1]);
else {
  console.error(
    "usage: check-parts.mts vitest <reports> [vitest args] | playwright <browser> <reports>",
  );
  process.exit(2);
}
for (const problem of problems) console.error(problem);
if (problems.length > 0) process.exit(1);
console.log("every part's work ran exactly once");
