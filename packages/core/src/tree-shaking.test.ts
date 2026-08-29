/**
 * Pins the package download budget and the promise that an application ships only the store it
 * uses. Both can regress silently through a barrel export, a stray side effect, or a static worker
 * adapter import.
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
import { constants, gzipSync } from "node:zlib";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

const OPFS_MARKER = "minnowdb-store:";
const IDB_MARKER = "unique-key-base-part";
const CLIENT_MARKER = "Database client is closed";
const KEYED_LIVE_MARKER = "Live query window maxRows must be a positive whole number";
const TYPED_TABLE_MARKER = "Upsert requires a unique key:";
const DATABASE_MARKER = "A database cannot queue more than";
// Result-domain metadata, DATE, guarded upserts, typed catalog errors, bounded cancellation,
// exact-NUMERIC dictionary predicates, maintenance hints, set-at-a-time correlated SQL, nested
// JSON provenance, stored generated columns, the keyed point-read fast path, declared-scale
// NUMERIC rendering, the -> and ->> JSON operators, exact numeric constants (PostgreSQL's
// NUMERIC typing of decimal and big-integer literals, with compile-time exact folding), and
// PostgreSQL quotient-scale selection intentionally expand the complete engine surface.
// Measured: 736.4 KiB raw / 210.6 KiB gzip. Pin both with less than 1% headroom.
const COMPLETE_ENTRY_RAW_BUDGET = 739 * 1024;
const COMPLETE_ENTRY_GZIP_BUDGET = 213 * 1024;
// Measured with the larger durable adapter: 1069.9 KiB raw / 294.1 KiB gzip.
const ENGINE_WITH_OPFS_RAW_BUDGET = 1072 * 1024;
const ENGINE_WITH_OPFS_GZIP_BUDGET = 296 * 1024;

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

describe("core packaging", () => {
  it("keeps the complete main entry below its download budget", async () => {
    const result = await build({
      entryPoints: ["@minnowdb/core"],
      absWorkingDir: repoRoot,
      bundle: true,
      write: false,
      minify: true,
      format: "esm",
      platform: "browser",
      target: "es2022",
      define: { "process.env.NODE_ENV": '"production"' },
      logLevel: "silent",
    });
    const output = result.outputFiles[0];
    if (output === undefined) throw new Error("esbuild produced no main-entry output");
    const gzipBytes = gzipSync(output.contents, {
      level: constants.Z_BEST_COMPRESSION,
    }).byteLength;

    expect(output.contents.byteLength, "complete main entry raw bytes").toBeLessThanOrEqual(
      COMPLETE_ENTRY_RAW_BUDGET,
    );
    expect(gzipBytes, "complete main entry gzip bytes").toBeLessThanOrEqual(
      COMPLETE_ENTRY_GZIP_BUDGET,
    );
  });

  it("keeps a usable engine with the larger durable adapter below its download budget", async () => {
    const result = await build({
      stdin: {
        contents:
          'export { MinnowDatabase } from "@minnowdb/core"; ' +
          'export { OpfsBlockStore } from "@minnowdb/core/storage/opfs";',
        resolveDir: repoRoot,
      },
      bundle: true,
      write: false,
      minify: true,
      format: "esm",
      platform: "browser",
      target: "es2022",
      define: { "process.env.NODE_ENV": '"production"' },
      logLevel: "silent",
    });
    const output = result.outputFiles[0];
    if (output === undefined) throw new Error("esbuild produced no durable-engine output");
    const gzipBytes = gzipSync(output.contents, {
      level: constants.Z_BEST_COMPRESSION,
    }).byteLength;

    expect(output.contents.byteLength, "engine plus OPFS raw bytes").toBeLessThanOrEqual(
      ENGINE_WITH_OPFS_RAW_BUDGET,
    );
    expect(gzipBytes, "engine plus OPFS gzip bytes").toBeLessThanOrEqual(
      ENGINE_WITH_OPFS_GZIP_BUDGET,
    );
  });

  it("the engine alone carries no adapter", async () => {
    const output = await bundle(
      `import { MinnowDatabase } from "@minnowdb/core"; console.log(MinnowDatabase);`,
    );
    expect(output).not.toContain(OPFS_MARKER);
    expect(output).not.toContain(IDB_MARKER);
  });

  it("keeps optional client and typed-live APIs out of the complete main surface", async () => {
    const output = await bundle(`import * as core from "@minnowdb/core"; console.log(core);`);
    expect(output).not.toContain(CLIENT_MARKER);
    expect(output).not.toContain(KEYED_LIVE_MARKER);

    const live = await bundle(
      `import { KeyedLiveQuery } from "@minnowdb/core/live"; console.log(KeyedLiveQuery);`,
    );
    expect(live).toContain(KEYED_LIVE_MARKER);
  });

  it("keeps the optional typed-table renderer on the schema entry", async () => {
    const main = await bundle(`import * as core from "@minnowdb/core"; console.log(core);`);
    expect(main).not.toContain(TYPED_TABLE_MARKER);

    const schema = await bundle(
      `import { typedTable } from "@minnowdb/core/schema"; console.log(typedTable);`,
    );
    expect(schema).toContain(TYPED_TABLE_MARKER);
  });

  it("the standalone query tools carry no engine or adapter", async () => {
    const output = await bundle(
      `import { compileStatement } from "@minnowdb/core/query"; console.log(compileStatement);`,
    );
    expect(output).not.toContain(CLIENT_MARKER);
    expect(output).not.toContain(OPFS_MARKER);
    expect(output).not.toContain(IDB_MARKER);
  });

  it("the standalone schema DSL carries no database engine or adapter", async () => {
    const output = await bundle(
      `import { column, table } from "@minnowdb/core/schema"; console.log(column, table);`,
    );
    expect(output).not.toContain(DATABASE_MARKER);
    expect(output).not.toContain(CLIENT_MARKER);
    expect(output).not.toContain(OPFS_MARKER);
    expect(output).not.toContain(IDB_MARKER);
  });

  it("retains the worker's side-effect attachment through a bare package import", async () => {
    const output = await bundle('import "@minnowdb/core/worker";');

    // This protocol refusal is emitted by the attached host before initialization. An entry-point
    // build would survive even if package.json accidentally stopped marking worker.js as a side
    // effect; a consumer's bare import would not.
    expect(output).toContain("Database is not initialized: send init first");
  });

  it("an IndexedDB-only subpath does not pull the OPFS store", async () => {
    const output = await bundle(
      `import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb"; console.log(IndexedDbBlockStore);`,
    );
    expect(output).toContain(IDB_MARKER);
    expect(output).not.toContain(OPFS_MARKER);
  });

  it("an OPFS-only subpath does not pull the IndexedDB store", async () => {
    const output = await bundle(
      `import { OpfsBlockStore } from "@minnowdb/core/storage/opfs"; console.log(OpfsBlockStore);`,
    );
    expect(output).toContain(OPFS_MARKER);
    expect(output).not.toContain(IDB_MARKER);
  });

  it("keeps named imports from the complete storage barrel tree-shakable", async () => {
    const indexedDb = await bundle(
      `import { IndexedDbBlockStore } from "@minnowdb/core/storage"; console.log(IndexedDbBlockStore);`,
    );
    expect(indexedDb).toContain(IDB_MARKER);
    expect(indexedDb).not.toContain(OPFS_MARKER);

    const opfs = await bundle(
      `import { OpfsBlockStore } from "@minnowdb/core/storage"; console.log(OpfsBlockStore);`,
    );
    expect(opfs).toContain(OPFS_MARKER);
    expect(opfs).not.toContain(IDB_MARKER);
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
