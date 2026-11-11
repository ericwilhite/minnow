/** Pin an executor control to a repository revision, sharing only owner contracts.
 * Build core first. Scratch follows os.tmpdir(); set TMPDIR in the shell when needed.
 * node scripts/build-next-executor-control.mts --revision <sha> --output <file.mjs>
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { build } from "esbuild";
const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const at = args.indexOf(name);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
const revision = execFileSync(
  "git",
  ["rev-parse", "--verify", "--end-of-options", `${option("--revision", "origin/main")}^{commit}`],
  { encoding: "utf8" },
).trim();
if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error("Expected a full commit SHA");
const output = resolve(
  option("--output", join(tmpdir(), `minnow-executor-control-${revision}.mjs`)),
);
const scratch = await mkdtemp(join(tmpdir(), "minnow-executor-control-"));
const shared = [
  "next/storage/extents.js",
  "next/memory/governor.js",
  "next/errors.js",
  "next/exec/errors.js",
  "next/task.js",
];
try {
  const archive = execFileSync(
    "git",
    ["archive", revision, "packages/core/src", "packages/core/package.json"],
    { maxBuffer: 64 << 20 },
  );
  execFileSync("tar", ["-xf", "-", "-C", scratch], { input: archive });
  await mkdir(dirname(output), { recursive: true });
  const source = join(scratch, "packages/core/src") + sep;
  const contracts = Object.fromEntries(
    await Promise.all(
      shared.map(async (name) => {
        const path = resolve("packages/core/dist", name);
        return [
          name,
          {
            path,
            sha256: createHash("sha256")
              .update(await readFile(path))
              .digest("hex"),
          },
        ];
      }),
    ),
  );
  const entry = join(scratch, "control.mts");
  await writeFile(
    entry,
    [
      `export { createExecutor } from ${JSON.stringify(join(source, "next/exec/create.ts"))};`,
      `export { AggregateOperator } from ${JSON.stringify(join(source, "next/exec/agg/aggregate.ts"))};`,
    ].join("\n") + "\n",
  );
  await build({
    absWorkingDir: scratch,
    entryPoints: [entry],
    outfile: output,
    bundle: true,
    format: "esm",
    platform: "node",
    packages: "external",
    plugins: [
      {
        name: "shared-owner-contracts",
        setup(builder) {
          builder.onResolve({ filter: /\.js$/ }, (input) => {
            const path = resolve(input.resolveDir, input.path);
            if (!path.startsWith(source)) return null;
            const contract = contracts[path.slice(source.length).split(sep).join("/")];
            return contract ? { path: contract.path, external: true } : null;
          });
        },
      },
    ],
  });
  const manifest = {
    revision,
    sourceTree: execFileSync("git", ["rev-parse", `${revision}:packages/core/src`], {
      encoding: "utf8",
    }).trim(),
    output,
    sha256: createHash("sha256")
      .update(await readFile(output))
      .digest("hex"),
    sharedContracts: contracts,
    limits:
      "Executor from the pinned revision over the benchmark's shared storage/memory owner; not a second whole-engine instance.",
    node: process.version,
  };
  await writeFile(`${output}.json`, JSON.stringify(manifest, null, 2) + "\n");
  console.log(JSON.stringify(manifest));
} finally {
  await rm(scratch, { recursive: true, force: true });
}
