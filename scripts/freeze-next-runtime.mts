/** Prepare complete, separate core runtimes before the quiet window; never times queries. */
import {
  mkdir,
  readFile,
  readdir,
  realpath,
  symlink,
  writeFile,
  access,
  rm,
} from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { join, relative, resolve } from "node:path";

const value = (name: string) => {
  const at = process.argv.indexOf(name);
  const result = at < 0 ? undefined : process.argv[at + 1];
  if (!result) throw new Error(`Required: ${name}`);
  return resolve(result);
};
const root = value("--repository"),
  destination = value("--output"),
  dependencies = value("--dependencies");
const at = process.argv.indexOf("--revision"),
  revision = process.argv[at + 1];
if (at < 0 || !revision || !/^[0-9a-f]{40}$/.test(revision))
  throw new Error("A full --revision SHA is required");
if (
  await access(destination).then(
    () => true,
    () => false,
  )
)
  throw new Error("Refusing an existing frozen runtime");
const run = (command: string, args: string[], cwd = root) => {
  const result = spawnSync(command, args, { cwd, encoding: "utf8" });
  if (result.status !== 0) throw new Error(`${command}: ${result.stdout}\n${result.stderr}`);
  return result.stdout.trim();
};
const tree = run("git", ["rev-parse", `${revision}^{tree}`]);
await mkdir(destination, { recursive: true });
const archive = join(destination, "source.tar");
run("git", ["archive", "--format=tar", `--output=${archive}`, revision]);
run("tar", ["-xf", archive, "-C", destination]);
const modules = join(destination, "node_modules");
async function linkModules(from: string) {
  await mkdir(join(modules, "@minnowdb"), { recursive: true });
  for (const entry of await readdir(from, { withFileTypes: true })) {
    if (entry.name === "@minnowdb") {
      for (const name of await readdir(join(from, entry.name)))
        if (name !== "core")
          await symlink(
            await realpath(join(from, entry.name, name)),
            join(modules, entry.name, name),
          );
    } else await symlink(await realpath(join(from, entry.name)), join(modules, entry.name));
  }
  await symlink("../../packages/core", join(modules, "@minnowdb/core"));
}
// Compile every project input with the development dependencies, then replace
// those links with the separately frozen runtime-only dependency set.
await linkModules(join(root, "node_modules"));
const compiler = join(root, "node_modules/typescript/bin/tsc");
run(process.execPath, [compiler, "-b", "packages/core", "--pretty", "false"], destination);
await rm(modules, { recursive: true });
await linkModules(dependencies);
const dist = join(destination, "packages/core/dist");
const files: Record<string, string> = {};
async function pin(file: string): Promise<void> {
  files[relative(destination, file)] = createHash("sha256")
    .update(await readFile(file))
    .digest("hex");
}
async function record(directory: string): Promise<void> {
  for (const entry of await readdir(directory, { withFileTypes: true }))
    if (entry.isDirectory()) await record(join(directory, entry.name));
    else await pin(join(directory, entry.name));
}
await record(dist);
await pin(archive);
await pin(join(destination, "packages/core/package.json"));
await writeFile(
  join(destination, "manifest.json"),
  JSON.stringify(
    {
      revision,
      tree,
      node: process.version,
      files,
      build: {
        kind: "complete core TypeScript project build",
        compiler,
        target: "ES2022",
        typecheckClaim: true,
      },
      dependencies: await realpath(dependencies),
    },
    null,
    2,
  ) + "\n",
);
console.log(
  JSON.stringify({
    destination,
    revision,
    tree,
    files: Object.keys(files).length,
    measuredRequests: 0,
  }),
);
