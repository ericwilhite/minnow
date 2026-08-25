import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { beforeAll, describe, expect, it } from "vitest";
import * as core from "@minnowdb/core";
import { MinnowDatabase } from "@minnowdb/core";
import * as coreClient from "@minnowdb/core/client";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { createKysely, search } from "@minnowdb/kysely";
import { sql } from "kysely";
import { retailBatches, retailDefinition, retailSchema } from "@/lib/dataset/retail";
import { playgroundDeclarations } from "./declarations";
import { runSnippet, unsupportedRuntimeImports } from "./run";
import { PLAYGROUND_RUNTIME_MODULES, type PlaygroundRuntimeModule } from "./runtime-modules";
import { snippets } from "./snippets";

/**
 * Both halves of the console's promise, checked without a browser.
 *
 * The editor's type checking and the editor's Run button are the same TypeScript compiler and the
 * same rewriter this file uses, given the same declarations and the same options. So a snippet
 * that typechecks and runs here is one that typechecks and runs there — and a snippet offered on
 * the home page with a red squiggle under it, or one that throws when a reader clicks it, fails
 * this suite first.
 */

const here = path.dirname(fileURLToPath(import.meta.url));
const bundle = JSON.parse(
  readFileSync(path.join(here, "../../public/playground-types.json"), "utf8"),
) as { paths: Record<string, string[]>; files: Record<string, string>; runtimeModules: string[] };

/**
 * The console's compiler options. `Bundler` resolution is deliberately absent: the editor's
 * bundled service offers only Classic and Node, which is why the generator strips the `.js` from
 * every relative specifier. Checking under Node here is what proves that stripping was enough.
 */
const options: ts.CompilerOptions = {
  target: ts.ScriptTarget.ESNext,
  module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Node10,
  // A snippet awaits at the top level without importing anything, so it has to be treated as a
  // module even when it looks like a script.
  moduleDetection: ts.ModuleDetectionKind.Force,
  lib: ["lib.es2022.d.ts", "lib.dom.d.ts", "lib.dom.iterable.d.ts"],
  strict: true,
  exactOptionalPropertyTypes: true,
  noEmit: true,
  baseUrl: "/",
  paths: bundle.paths,
};

/** The declaration files, addressed the way a compiler on this machine addresses a path. */
const declarations = new Map(
  Object.entries(bundle.files).map(([uri, source]) => [uri.replace("file://", ""), source]),
);
declarations.set("/playground.d.ts", playgroundDeclarations(retailSchema));

function diagnose(code: string): string[] {
  const entry = "/snippet.ts";
  const sources = new Map(declarations);
  sources.set(entry, code);

  const libDir = path.dirname(ts.sys.getExecutingFilePath());
  const read = (name: string): string | undefined =>
    sources.get(name) ??
    (name.includes("lib.") && name.endsWith(".d.ts")
      ? ts.sys.readFile(path.join(libDir, path.basename(name)))
      : undefined);

  const host: ts.CompilerHost = {
    fileExists: (name) => read(name) !== undefined,
    readFile: read,
    getSourceFile: (name, languageVersion) => {
      const source = read(name);
      return source === undefined
        ? undefined
        : ts.createSourceFile(name, source, languageVersion, true);
    },
    getDefaultLibFileName: (compilerOptions) =>
      path.join(libDir, ts.getDefaultLibFileName(compilerOptions)),
    writeFile: () => undefined,
    getCurrentDirectory: () => "/",
    getCanonicalFileName: (name) => name,
    useCaseSensitiveFileNames: () => true,
    getNewLine: () => "\n",
  };

  const program = ts.createProgram([entry, "/playground.d.ts"], options, host);
  return [...program.getSemanticDiagnostics(), ...program.getSyntacticDiagnostics()].map(
    (diagnostic) => {
      const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, " ");
      const at = diagnostic.file?.getLineAndCharacterOfPosition(diagnostic.start ?? 0);
      return at === undefined ? message : `line ${String(at.line + 1)}: ${message}`;
    },
  );
}

describe("the console's declarations", () => {
  it("resolve every package a snippet may import", () => {
    const probe = [
      'import { createKysely, search } from "@minnowdb/kysely";',
      'import { sql } from "kysely";',
      'import { MinnowDatabase, column, schema, table } from "@minnowdb/core";',
      'import { MinnowDatabaseClient } from "@minnowdb/core/client";',
      "void [createKysely, search, sql, MinnowDatabase, column, schema, table, MinnowDatabaseClient];",
    ].join("\n");
    expect(diagnose(probe)).toEqual([]);
  });

  it("records exactly the modules the Run button can execute", () => {
    expect(bundle.runtimeModules).toEqual([...PLAYGROUND_RUNTIME_MODULES].sort());
  });

  it("rejects value imports that have declarations but no runtime module", () => {
    const compilerOptions = { target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext };
    const valueImport = ts.transpileModule(
      'import { MemoryBlockStore } from "@minnowdb/core/storage/memory"; void MemoryBlockStore;',
      { compilerOptions },
    ).outputText;
    const typeImport = ts.transpileModule(
      'import type { MemoryBlockStore } from "@minnowdb/core/storage/memory"; void 0;',
      { compilerOptions },
    ).outputText;
    expect(unsupportedRuntimeImports(valueImport, PLAYGROUND_RUNTIME_MODULES)).toEqual([
      "@minnowdb/core/storage/memory",
    ]);
    expect(unsupportedRuntimeImports(typeImport, PLAYGROUND_RUNTIME_MODULES)).toEqual([]);
    expect(
      unsupportedRuntimeImports(
        'const module = await import("@minnowdb/core/storage/opfs"); void module;',
        PLAYGROUND_RUNTIME_MODULES,
      ),
    ).toEqual(["@minnowdb/core/storage/opfs"]);
  });

  it("type the two names the console hands a snippet", () => {
    const probe = [
      'const row = await db.selectFrom("stores").select(["city"]).executeTakeFirstOrThrow();',
      "const city: string = row.city;",
      "void city;",
      "void database.listTables();",
    ].join("\n");
    expect(diagnose(probe)).toEqual([]);
  });

  it("reject a column the playground's schema does not have", () => {
    expect(diagnose('db.selectFrom("orders").select(["nmae"]);').join(" ")).toMatch(/nmae/);
    expect(diagnose('db.selectFrom("ordrs").selectAll();').join(" ")).toMatch(/ordrs/);
  });

  it("infers the complete aggregate sample without output generics", () => {
    const aggregate = snippets.find(({ id }) => id === "revenue-by-month");
    if (aggregate === undefined) throw new Error("Missing revenue aggregate snippet");
    expect(
      diagnose(
        `${aggregate.code}\nconst inferred: { month: Date; orders: number; revenue: number } = rows[0]!;\nvoid inferred;`,
      ),
    ).toEqual([]);
  });

  it("keeps explicit row generics at the arbitrary raw-SQL boundary only", () => {
    for (const snippet of snippets) {
      if (snippet.id !== "raw-sql") expect(snippet.code).not.toMatch(/sql\s*</u);
    }
    expect(snippets.find(({ id }) => id === "raw-sql")?.code).toMatch(/sql\s*</u);
  });
});

describe("every snippet the console offers", () => {
  it.each(snippets.map((snippet) => [snippet.id, snippet] as const))(
    "%s typechecks",
    (_id, snippet) => {
      expect(diagnose(snippet.code)).toEqual([]);
    },
  );
});

describe("running every snippet", () => {
  let scope: Parameters<typeof runSnippet>[1];

  beforeAll(async () => {
    const engine = new MinnowDatabase(new MemoryBlockStore());
    await engine.migrate(retailDefinition);
    for (const batch of retailBatches({ scale: 0.05 })) {
      await engine.insertBatch(batch.table, batch.rows);
    }
    const modules = {
      kysely: { sql },
      "@minnowdb/kysely": { createKysely, search },
      "@minnowdb/core": core,
      "@minnowdb/core/client": coreClient,
    } satisfies Record<PlaygroundRuntimeModule, unknown>;
    expect(Object.keys(modules).sort()).toEqual([...PLAYGROUND_RUNTIME_MODULES].sort());
    scope = {
      modules,
      globals: {
        db: createKysely({ driver: engine, schema: retailDefinition }),
        database: engine,
      },
    };
  });

  it.each(snippets.map((snippet) => [snippet.id, snippet] as const))(
    "%s answers against the playground dataset",
    async (_id, snippet) => {
      const compiled = ts.transpileModule(snippet.code, {
        compilerOptions: { target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext },
      }).outputText;

      const result = await runSnippet(compiled, scope);
      expect(result.failure).toBeUndefined();
      expect(result.output.length).toBeGreaterThan(0);
    },
  );
});
