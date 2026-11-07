/** Bench-only immutable executor snapshot. Every relative core dependency belongs to the same
 * Git revision; both families load before timing. Never used without explicit comparison opt-in.
 */
import { execFileSync } from "node:child_process";
import { resolve, relative } from "node:path";
import type { Plugin as VitePlugin } from "vite";
import type { Plugin as EsbuildPlugin } from "esbuild";

export function executorSnapshot(revision: string) {
  const root = resolve("packages/core/src/next");
  const sources = new Map<string, string>();
  const source = (file: string) => {
    const key = relative(root, file).replaceAll("\\", "/");
    if (key.startsWith("..")) throw new Error("Executor snapshot escaped its core root");
    let text = sources.get(key);
    if (text === undefined) {
      text = execFileSync("git", ["show", `${revision}:packages/core/src/next/${key}`], {
        encoding: "utf8",
      });
      sources.set(key, text);
    }
    return text;
  };
  const esbuild: EsbuildPlugin = {
    name: "executor-snapshot",
    setup(build) {
      build.onLoad({ filter: /\/packages\/core\/src\/next\/.*\.ts$/ }, ({ path }) => ({
        contents: source(path),
        loader: "ts",
      }));
    },
  };
  const vite: VitePlugin = {
    name: "executor-snapshot",
    enforce: "pre",
    load(id) {
      const [file, query] = id.split("?");
      if (!file || !new URLSearchParams(query).has("executorBaseline")) return;
      let text = source(file);
      // Relative imports remain in the frozen family. No source fallback is allowed.
      text = text.replace(
        /(from\s*|import\s*)(["'])(\.[^"']+)\2/gu,
        (whole: string, prefix: string, quote: string, path: string) => {
          if (!path.endsWith(".js")) return whole;
          return `${prefix}${quote}${path.slice(0, -3)}.ts?executorBaseline${quote}`;
        },
      );
      if (file.endsWith("/exec/create.ts"))
        text += `\nexport const EXECUTOR_BASELINE_REVISION = ${JSON.stringify(revision)};\nexport {chunkReads as EXECUTOR_BASELINE_READS} from "./scan.ts?executorBaseline";\n`;
      return text;
    },
  };
  return { esbuild, vite };
}
