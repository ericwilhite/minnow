import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { beforeAll, describe, expect, it } from "vitest";
import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { createKysely } from "@minnowdb/kysely";
import { sql } from "kysely";
import { retailBatches, retailDefinition, retailSchema } from "@/lib/dataset/retail";
import { playgroundDeclarations } from "./declarations";
import { runSnippet } from "./run";
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
) as { paths: Record<string, string[]>; files: Record<string, string> };

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
      'import { createKysely } from "@minnowdb/kysely";',
      'import { sql } from "kysely";',
      'import { MinnowDatabase, column, schema, table } from "@minnowdb/core";',
      'import { MemoryBlockStore } from "@minnowdb/core/storage";',
      'import { MinnowDatabaseClient } from "@minnowdb/core/client";',
      "void [createKysely, sql, MinnowDatabase, column, schema, table, MemoryBlockStore, MinnowDatabaseClient];",
    ].join("\n");
    expect(diagnose(probe)).toEqual([]);
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
    scope = {
      modules: { kysely: { sql }, "@minnowdb/kysely": { createKysely }, "@minnowdb/core": {} },
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
