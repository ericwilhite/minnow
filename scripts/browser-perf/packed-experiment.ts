/** Rejected packed-frame diagnostic. Rebuild its exact source snapshot from a frozen archive
 * and patch under ~/.cache. No checkout, production import or real index is changed.
 * Only tests and an explicitly opted-in benchmark may load this snapshot.
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Plugin as EsbuildPlugin } from "esbuild";
import type { Plugin as VitePlugin } from "vite";

const fixture = fileURLToPath(new URL("./experiments/packed-frame-copy.patch", import.meta.url));
export const PACKED_EXPERIMENT_BASE = "095f5f1c";
export function packedExperiment() {
  const root = resolve("."),
    next = join(root, "packages/core/src/next") + "/";
  const scratch = mkdtempSync(join(homedir(), ".cache", "minnow-packed-snapshot-"));
  const metadata = JSON.parse(
    readFileSync(new URL("./experiments/packed-frame-copy.json", import.meta.url), "utf8"),
  ) as {
    base: string;
    state: string;
    patchSha256: string;
    baseArchiveSha256: string;
    files: Record<string, string>;
  };
  const archive = fileURLToPath(new URL("./experiments/packed-frame-base.tar.gz", import.meta.url));
  if (
    metadata.state !== "rejected" ||
    createHash("sha256").update(readFileSync(fixture)).digest("hex") !== metadata.patchSha256 ||
    createHash("sha256").update(readFileSync(archive)).digest("hex") !== metadata.baseArchiveSha256
  )
    throw new Error("Packed diagnostic fixture changed");
  execFileSync("tar", ["-xzf", archive, "-C", scratch]);
  execFileSync("git", ["apply", "--unsafe-paths", fixture], { cwd: scratch });
  let closed = false;
  const close = () => {
    if (!closed) {
      closed = true;
      rmSync(scratch, { recursive: true, force: true });
      process.off("exit", close);
    }
  };
  process.once("exit", close);
  const sources = new Map<string, string>();
  const source = (file: string) => {
    const path = relative(root, file).replaceAll("\\", "/");
    if (path.startsWith("..")) throw new Error("Packed snapshot escaped the repo");
    let text = sources.get(path);
    if (text === undefined) {
      text = readFileSync(join(scratch, path), "utf8");
      const hash = metadata.files[path];
      if (hash && createHash("sha256").update(text).digest("hex") !== hash)
        throw new Error("Packed snapshot source hash mismatch");
      sources.set(path, text);
    }
    return text;
  };
  const belongs = (path: string) =>
    path.startsWith(next) ||
    Object.hasOwn(metadata.files, relative(root, path).replaceAll("\\", "/"));
  const candidatePath = (id: string, importer?: string) => {
    if (!id.startsWith(".") && !id.startsWith("/")) return null;
    let path = resolve(
      importer ? dirname(importer.split("?")[0] ?? importer) : root,
      id.split("?")[0] ?? id,
    );
    if (path.endsWith(".js")) path = path.slice(0, -3) + ".ts";
    return belongs(path) ? path : null;
  };
  const esbuild: EsbuildPlugin = {
    name: "rejected-packed-frame-snapshot",
    setup(build) {
      build.onResolve({ filter: /^[./]/ }, async (args) => {
        if ((args.pluginData as { skipPacked?: boolean } | undefined)?.skipPacked) return;
        const path = candidatePath(args.path, args.importer || undefined);
        if (path) return { path, namespace: "rejected-packed" };
        if (args.namespace === "rejected-packed")
          return build.resolve(args.path, {
            resolveDir: dirname(args.importer),
            kind: args.kind,
            pluginData: { skipPacked: true },
          });
      });
      build.onLoad({ filter: /.*/, namespace: "rejected-packed" }, (args) => ({
        contents: source(args.path),
        loader: "ts",
        resolveDir: dirname(args.path),
      }));
    },
  };
  const vite: VitePlugin = {
    name: "rejected-packed-frame-snapshot",
    enforce: "pre",
    resolveId(id, importer) {
      if (id.includes("executorBaseline") || importer?.includes("executorBaseline")) return;
      const path = candidatePath(id, importer);
      if (path && !existsSync(path)) return path + "?rejectedPacked";
    },
    load(id) {
      if (id.includes("executorBaseline")) return;
      const path = id.split("?")[0];
      if (path && belongs(path)) return source(path);
    },
    closeBundle: close,
  };
  return { esbuild, vite, close };
}
