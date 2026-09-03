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
// PostgreSQL quotient-scale selection, schema-bound wildcard lowering, execution-time DML
// scalar materialization, PostgreSQL GROUP BY / set-operation tail resolution, untyped-literal
// coercion, the table-driven PostgreSQL scalar function surface (TO_CHAR templates, regular
// expressions, MD5, FORMAT, AGE), mutation aliases with subquery assignments, and SERIAL /
// IDENTITY DDL, correlation probes that carry the outer predicates and mirror key ranges,
// uncorrelated IN subqueries planned as joins, dictionary-decided CASE aggregate branches, and
// exact NUMERIC ROUND/TRUNC/ABS/FLOOR/CEIL/MOD/SIGN with PostgreSQL display-scale inference
// intentionally expand the complete engine surface.
// Measured: 809.3 KiB raw / 233.1 KiB gzip. Pin both with less than 1% headroom.
const COMPLETE_ENTRY_RAW_BUDGET = 836 * 1024;
const COMPLETE_ENTRY_GZIP_BUDGET = 242 * 1024;
// Measured with the larger durable adapter: 1143.0 KiB raw / 316.5 KiB gzip.
const ENGINE_WITH_OPFS_RAW_BUDGET = 1170 * 1024;
const ENGINE_WITH_OPFS_GZIP_BUDGET = 326 * 1024;
// The IndexedDB-only worker entry: the whole engine, the host, and one adapter, bundled without
// code splitting the way Vite's default iife worker format does. The generic entry inlined the
// same way measured 1465.6 KiB raw / 397.8 KiB gzip. Measured: 1158.8 KiB raw / 322.2 KiB gzip.
const INDEXEDDB_WORKER_RAW_BUDGET = 1196 * 1024;
const INDEXEDDB_WORKER_GZIP_BUDGET = 332 * 1024;

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

/** A production build the way an application ships it: minified, without code splitting. */
async function measure(
  label: string,
  source: { entryPoint: string } | { contents: string },
): Promise<{ rawBytes: number; gzipBytes: number }> {
  const result = await build({
    ...("entryPoint" in source
      ? { entryPoints: [source.entryPoint], absWorkingDir: repoRoot }
      : { stdin: { contents: source.contents, resolveDir: repoRoot } }),
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
  if (output === undefined) throw new Error(`esbuild produced no ${label} output`);
  return {
    rawBytes: output.contents.byteLength,
    gzipBytes: gzipSync(output.contents, { level: constants.Z_BEST_COMPRESSION }).byteLength,
  };
}

describe("core packaging", () => {
  it("keeps the complete main entry below its download budget", async () => {
    const { rawBytes, gzipBytes } = await measure("main-entry", { entryPoint: "@minnowdb/core" });

    expect(rawBytes, "complete main entry raw bytes").toBeLessThanOrEqual(
      COMPLETE_ENTRY_RAW_BUDGET,
    );
    expect(gzipBytes, "complete main entry gzip bytes").toBeLessThanOrEqual(
      COMPLETE_ENTRY_GZIP_BUDGET,
    );
  });

  it("keeps a usable engine with the larger durable adapter below its download budget", async () => {
    const { rawBytes, gzipBytes } = await measure("durable-engine", {
      contents:
        'export { MinnowDatabase } from "@minnowdb/core"; ' +
        'export { OpfsBlockStore } from "@minnowdb/core/storage/opfs";',
    });

    expect(rawBytes, "engine plus OPFS raw bytes").toBeLessThanOrEqual(ENGINE_WITH_OPFS_RAW_BUDGET);
    expect(gzipBytes, "engine plus OPFS gzip bytes").toBeLessThanOrEqual(
      ENGINE_WITH_OPFS_GZIP_BUDGET,
    );
  });

  it("keeps the IndexedDB-only worker entry below its download budget", async () => {
    const { rawBytes, gzipBytes } = await measure("IndexedDB worker", {
      entryPoint: "@minnowdb/core/worker/indexeddb",
    });

    expect(rawBytes, "IndexedDB-only worker raw bytes").toBeLessThanOrEqual(
      INDEXEDDB_WORKER_RAW_BUDGET,
    );
    expect(gzipBytes, "IndexedDB-only worker gzip bytes").toBeLessThanOrEqual(
      INDEXEDDB_WORKER_GZIP_BUDGET,
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

  it("retains each per-store worker's side-effect attachment through a bare import", async () => {
    for (const kind of ["indexeddb", "opfs", "memory"]) {
      const output = await bundle(`import "@minnowdb/core/worker/${kind}";`);
      expect(output, `@minnowdb/core/worker/${kind}`).toContain(
        "Database is not initialized: send init first",
      );
    }
  });

  it("each per-store worker entry bundles exactly its own adapter", async () => {
    // Bundled without splitting, as Vite's default iife worker format does: the dynamic imports
    // in the generic entry's store factory are inlined, so it carries every adapter …
    const generic = await bundle('import "@minnowdb/core/worker";');
    expect(generic).toContain(IDB_MARKER);
    expect(generic).toContain(OPFS_MARKER);

    // … and the per-store entries are the way to carry one.
    const indexedDb = await bundle('import "@minnowdb/core/worker/indexeddb";');
    expect(indexedDb).toContain(IDB_MARKER);
    expect(indexedDb).not.toContain(OPFS_MARKER);

    const opfs = await bundle('import "@minnowdb/core/worker/opfs";');
    expect(opfs).toContain(OPFS_MARKER);
    expect(opfs).not.toContain(IDB_MARKER);

    const memory = await bundle('import "@minnowdb/core/worker/memory";');
    expect(memory).not.toContain(IDB_MARKER);
    expect(memory).not.toContain(OPFS_MARKER);
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
