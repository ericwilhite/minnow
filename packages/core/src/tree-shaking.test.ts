/**
 * Pins the packaging promise that an application ships only the store it uses. Two mechanisms
 * carry it, and either can regress silently: the package's `sideEffects` declaration (a stray
 * top-level side effect anywhere under a barrel makes bundlers keep everything), and the
 * worker host's dynamic adapter imports (a static import fuses every adapter into the worker
 * chunk again).
 *
 * Bundles the built package the way an application's bundler would — so like the playground
 * declaration suite, it reads `dist/` and expects it to be current (`tsc -b`, which every
 * checked pipeline runs before vitest).
 *
 * The probes are string literals that survive bundling: the OPFS store's BroadcastChannel
 * prefix and the IndexedDB store's key-partition prefix, each found nowhere else in the
 * package.
 */
import { join } from "node:path";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

const OPFS_MARKER = "minnowdb-store:";
const IDB_MARKER = "unique-key-base-part";

const repoRoot = join(import.meta.dirname, "..", "..", "..");

async function bundle(contents: string): Promise<string> {
  const result = await build({
    stdin: { contents, resolveDir: repoRoot },
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    logLevel: "silent",
  });
  const output = result.outputFiles[0];
  if (output === undefined) throw new Error("esbuild produced no output");
  return output.text;
}

describe("adapters tree-shake out of application bundles", () => {
  it("the engine alone carries no adapter", async () => {
    const output = await bundle(
      `import { MinnowDatabase } from "@minnowdb/core"; console.log(MinnowDatabase);`,
    );
    expect(output).not.toContain(OPFS_MARKER);
    expect(output).not.toContain(IDB_MARKER);
  });

  it("importing the IndexedDB store does not pull the OPFS store", async () => {
    const output = await bundle(
      `import { IndexedDbBlockStore } from "@minnowdb/core/storage"; console.log(IndexedDbBlockStore);`,
    );
    expect(output).toContain(IDB_MARKER);
    expect(output).not.toContain(OPFS_MARKER);
  });

  it("importing the OPFS store does not pull the IndexedDB store", async () => {
    const output = await bundle(
      `import { OpfsBlockStore } from "@minnowdb/core/storage"; console.log(OpfsBlockStore);`,
    );
    expect(output).toContain(OPFS_MARKER);
    expect(output).not.toContain(IDB_MARKER);
  });

  it("the worker entry splits every adapter into its own lazy chunk", async () => {
    const result = await build({
      entryPoints: ["@minnowdb/core/worker"],
      absWorkingDir: repoRoot,
      bundle: true,
      write: false,
      splitting: true,
      format: "esm",
      platform: "browser",
      target: "es2022",
      outdir: "tree-shaking-virtual-out",
      metafile: true,
      logLevel: "silent",
    });
    // The metafile marks dynamic-import targets as entry points too; the worker's own chunk is
    // the one whose entryPoint is the worker module.
    const outputs = result.metafile.outputs;
    const workerPath = Object.keys(outputs).find((path) =>
      outputs[path]?.entryPoint?.endsWith("dist/engine/worker.js"),
    );
    expect(workerPath).toBeDefined();
    // What the worker loads at startup is the transitive closure of its static import edges;
    // a fused adapter would appear there whether inlined or split into an eager chunk.
    const eager = new Set<string>();
    const walk = (path: string): void => {
      if (eager.has(path)) return;
      eager.add(path);
      for (const edge of outputs[path]?.imports ?? []) {
        if (edge.kind === "import-statement") walk(edge.path);
      }
    };
    walk(workerPath ?? "");
    const textOf = new Map(
      result.outputFiles.map((file) => {
        const relative = Object.keys(outputs).find((path) =>
          file.path.endsWith(path.replace(/^.*\//, "")),
        );
        return [relative ?? file.path, file.text] as const;
      }),
    );
    for (const path of eager) {
      expect(textOf.get(path), `an adapter loads eagerly with the worker (${path})`).not.toContain(
        OPFS_MARKER,
      );
      expect(textOf.get(path), `an adapter loads eagerly with the worker (${path})`).not.toContain(
        IDB_MARKER,
      );
    }
    // And every adapter still arrives — as its own lazily loaded chunk.
    const lazyText = [...textOf.entries()]
      .filter(([path]) => !eager.has(path))
      .map(([, text]) => text)
      .join("\n");
    expect(lazyText).toContain(OPFS_MARKER);
    expect(lazyText).toContain(IDB_MARKER);
  });
});
