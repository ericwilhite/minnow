/**
 * Pins the package download budget and the promise that an application ships only what it uses:
 * the store it opens, and on a page that hosts the engine in a worker, none of the engine. Each can
 * regress silently through a barrel export, a re-export routed through a heavy module, a stray
 * side effect, or a static adapter import.
 *
 * Bundles the built package the way an application's bundler would — so like the playground
 * declaration suite, it reads `dist/` and expects it to be current (`tsc -b`, which every
 * checked pipeline runs before vitest).
 *
 * The probes are string literals that survive bundling: the OPFS store's BroadcastChannel
 * prefix and the IndexedDB store's key-partition prefix, each found nowhere else in the
 * package, and the SQL tokenizer's own refusal for an unterminated dollar-quoted string.
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
const SQL_COMPILER_MARKER = "Unterminated dollar-quoted string";
/** The parser, planner, optimizer, function tables, executors, and engine. */
const SQL_ENGINE_MODULES =
  /\/engine\/(query|optimizer|sql-functions|sql-semantics|sql-regex|vector|database|schema-expressions)\.js$/u;

// Each budget is the current measurement plus a little headroom; a change that moves one updates
// the measurement beside it, and the history lives in git.

// Everything the main entry exports, bundled: the in-thread engine. 1101.3 KiB raw / 329.3 KiB gzip.
const COMPLETE_ENTRY_RAW_BUDGET = 1102 * 1024;
const COMPLETE_ENTRY_GZIP_BUDGET = 330 * 1024;
// The engine with the larger durable adapter, OPFS. 1493.9 KiB raw / 430.7 KiB gzip.
const ENGINE_WITH_OPFS_RAW_BUDGET = 1495 * 1024;
const ENGINE_WITH_OPFS_GZIP_BUDGET = 431 * 1024;
// The IndexedDB-only worker entry: the whole engine, the host, and one adapter, bundled without
// code splitting the way Vite's default iife worker format does. 1491.4 KiB raw / 428.3 KiB gzip.
const INDEXEDDB_WORKER_RAW_BUDGET = 1492 * 1024;
const INDEXEDDB_WORKER_GZIP_BUDGET = 429 * 1024;
// What a page that hosts the engine in a worker ships on the main thread: the client, the schema
// DSL and error classes, the Kysely dialect and live queries, and the React hooks, with `kysely`
// and `react` left to the application. 101.1 KiB raw / 30.1 KiB gzip.
const MAIN_THREAD_RAW_BUDGET = 103 * 1024;
const MAIN_THREAD_GZIP_BUDGET = 31 * 1024;
// The schema DSL and the error classes, taken from the main entry. 32.2 KiB raw / 10.2 KiB gzip.
const SCHEMA_DSL_RAW_BUDGET = 33 * 1024;
const SCHEMA_DSL_GZIP_BUDGET = 11 * 1024;
// The standalone live entry. 8.7 KiB gzip.
const LIVE_ENTRY_GZIP_BUDGET = 9 * 1024;

/** A worker-hosted application's main-thread imports: declare, catch, and query. */
const MAIN_THREAD_APPLICATION = `
  import { MinnowDatabaseClient } from "@minnowdb/core/client";
  import {
    column, schema, table, DatabaseReadBacklogError, LiveQueryLimitError,
    MaintenanceBacklogError, SqlCompileError, UniqueConstraintError, UnknownTableError,
  } from "@minnowdb/core";
  import {
    MinnowDialect, createKyselyLiveQueries, jsonArrayFrom, jsonBuildObject, jsonObjectFrom,
  } from "@minnowdb/kysely";
  import { useLiveQuery, useSuspenseLiveQuery } from "@minnowdb/react";
  console.log(
    MinnowDatabaseClient, column, schema, table, DatabaseReadBacklogError, LiveQueryLimitError,
    MaintenanceBacklogError, SqlCompileError, UniqueConstraintError, UnknownTableError,
    MinnowDialect, createKyselyLiveQueries, jsonArrayFrom, jsonBuildObject, jsonObjectFrom,
    useLiveQuery, useSuspenseLiveQuery,
  );
`;
const SCHEMA_DSL_AND_ERRORS = `
  export {
    column, schema, table, view, DatabaseReadBacklogError, LiveQueryLimitError,
    MaintenanceBacklogError, SqlCompileError, UniqueConstraintError, UnknownTableError,
  } from "@minnowdb/core";
`;

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

/**
 * A production build the way an application ships it: minified, without code splitting. Peer
 * dependencies named in `external` stay the application's own.
 */
async function measure(
  label: string,
  source: { entryPoint: string } | { contents: string },
  external: readonly string[] = [],
): Promise<{ rawBytes: number; gzipBytes: number; text: string; inputs: readonly string[] }> {
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
    external: [...external],
    metafile: true,
    logLevel: "silent",
  });
  const output = result.outputFiles[0];
  if (output === undefined) throw new Error(`esbuild produced no ${label} output`);
  // Inputs that contribute bytes; a module whose every statement was shaken away is not shipped.
  const inputs = Object.values(result.metafile.outputs).flatMap((meta) =>
    Object.entries(meta.inputs).flatMap(([path, input]) => (input.bytesInOutput > 0 ? [path] : [])),
  );
  return {
    rawBytes: output.contents.byteLength,
    gzipBytes: gzipSync(output.contents, { level: constants.Z_BEST_COMPRESSION }).byteLength,
    text: output.text,
    inputs,
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

  it("keeps a worker-hosted application's main thread free of the SQL engine", async () => {
    const { rawBytes, gzipBytes, text, inputs } = await measure(
      "main thread",
      { contents: MAIN_THREAD_APPLICATION },
      ["kysely", "react"],
    );

    expect(inputs.filter((path) => SQL_ENGINE_MODULES.test(path))).toEqual([]);
    expect(text).not.toContain(SQL_COMPILER_MARKER);
    expect(text).not.toContain(DATABASE_MARKER);
    expect(rawBytes, "main-thread raw bytes").toBeLessThanOrEqual(MAIN_THREAD_RAW_BUDGET);
    expect(gzipBytes, "main-thread gzip bytes").toBeLessThanOrEqual(MAIN_THREAD_GZIP_BUDGET);
  });

  it("the schema DSL and error classes carry no SQL compiler, from either entry", async () => {
    const main = await measure("schema DSL", { contents: SCHEMA_DSL_AND_ERRORS });
    expect(main.inputs.filter((path) => SQL_ENGINE_MODULES.test(path))).toEqual([]);
    expect(main.text).not.toContain(SQL_COMPILER_MARKER);
    expect(main.rawBytes, "schema DSL raw bytes").toBeLessThanOrEqual(SCHEMA_DSL_RAW_BUDGET);
    expect(main.gzipBytes, "schema DSL gzip bytes").toBeLessThanOrEqual(SCHEMA_DSL_GZIP_BUDGET);

    const subpath = await measure("schema subpath", {
      contents: 'export { column, schema, table, view } from "@minnowdb/core/schema";',
    });
    expect(subpath.inputs.filter((path) => SQL_ENGINE_MODULES.test(path))).toEqual([]);
    expect(subpath.text).not.toContain(SQL_COMPILER_MARKER);

    // The probe itself: the engine does carry the tokenizer's refusal.
    const engine = await bundle(
      `import { MinnowDatabase } from "@minnowdb/core"; console.log(MinnowDatabase);`,
    );
    expect(engine).toContain(SQL_COMPILER_MARKER);
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

  it("keeps SQL evaluators out of the complete standalone live entry", async () => {
    const result = await build({
      entryPoints: ["@minnowdb/core/live"],
      absWorkingDir: repoRoot,
      bundle: true,
      write: false,
      minify: true,
      format: "esm",
      platform: "browser",
      target: "es2022",
      metafile: true,
    });
    const inputs = Object.keys(result.metafile.inputs);
    expect(
      inputs.some((file) => /engine\/(query|sql-functions|vector|database)\.js$/u.test(file)),
    ).toBe(false);
    const output = result.outputFiles[0];
    expect(output).toBeDefined();
    if (output === undefined) throw new Error("Missing live bundle");
    expect(
      gzipSync(output.contents, { level: constants.Z_BEST_COMPRESSION }).byteLength,
    ).toBeLessThanOrEqual(LIVE_ENTRY_GZIP_BUDGET);
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
