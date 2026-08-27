/**
 * Snapshots the declaration syntax reachable from every published TypeScript entry point.
 *
 * A package build proves that declarations are valid; this gate proves that public names,
 * signatures, type/value reachability, and entry-point policy did not change accidentally.
 * Update the snapshot only after classifying compatibility impact and documenting it.
 */
import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import ts from "typescript";

const repoRoot = path.resolve(import.meta.dirname, "..");
const snapshotFile = path.join(import.meta.dirname, "api-contract.snapshot.json");
const policyFile = path.join(import.meta.dirname, "api-contract.policy.json");
const mode = process.argv[2] ?? "check";

const API_AUDIENCES = ["application", "extension", "metadata", "runtime", "testing"] as const;
type ApiAudience = (typeof API_AUDIENCES)[number];
type ExportKind = "type" | "value" | "type-and-value";

interface PackageManifest {
  name: string;
  private?: boolean;
  exports?: Record<string, string | { types?: string; default?: string }>;
}

interface PublishedEntry {
  packageName: string;
  subpath: string;
  target: string | { types?: string; default?: string };
  declarationFile?: string;
}

interface ApiPolicyEntry {
  subpath: string;
  audience: ApiAudience;
  description: string;
}

interface ApiPolicyPackage {
  name: string;
  entries: ApiPolicyEntry[];
}

interface ApiPolicy {
  schemaVersion: 1;
  description: string;
  packages: ApiPolicyPackage[];
}

interface SnapshotExport {
  name: string;
  kind: ExportKind;
  declarations: string[];
}

interface SnapshotEntry {
  subpath: string;
  audience: ApiAudience;
  exports: SnapshotExport[];
}

interface SnapshotPackage {
  name: string;
  entries: SnapshotEntry[];
}

interface ApiSnapshot {
  schemaVersion: 2;
  packages: SnapshotPackage[];
}

const entryKey = (packageName: string, subpath: string): string => `${packageName}:${subpath}`;

function publishedEntries(): PublishedEntry[] {
  const entries: PublishedEntry[] = [];
  const packagesRoot = path.join(repoRoot, "packages");
  for (const directory of readdirSync(packagesRoot, { withFileTypes: true })) {
    if (!directory.isDirectory()) continue;
    const packageRoot = path.join(packagesRoot, directory.name);
    const manifestFile = path.join(packageRoot, "package.json");
    if (!existsSync(manifestFile)) continue;
    const manifest = JSON.parse(readFileSync(manifestFile, "utf8")) as PackageManifest;
    if (manifest.private === true) continue;
    for (const [subpath, target] of Object.entries(manifest.exports ?? {})) {
      const types = typeof target === "string" ? undefined : target.types;
      entries.push({
        packageName: manifest.name,
        subpath,
        target,
        ...(types === undefined ? {} : { declarationFile: path.resolve(packageRoot, types) }),
      });
    }
  }
  return entries.sort(
    (left, right) =>
      left.packageName.localeCompare(right.packageName) ||
      left.subpath.localeCompare(right.subpath),
  );
}

function apiPolicy(entries: readonly PublishedEntry[]): Map<string, ApiPolicyEntry> {
  const raw = JSON.parse(readFileSync(policyFile, "utf8")) as Partial<ApiPolicy>;
  if (raw.schemaVersion !== 1 || !Array.isArray(raw.packages)) {
    throw new Error("Unsupported or malformed API contract policy");
  }
  if (typeof raw.description !== "string" || raw.description.trim().length === 0) {
    throw new Error("API contract policy needs a description");
  }

  const policy = new Map<string, ApiPolicyEntry>();
  for (const packagePolicy of raw.packages) {
    if (
      packagePolicy === null ||
      typeof packagePolicy !== "object" ||
      typeof packagePolicy.name !== "string" ||
      !Array.isArray(packagePolicy.entries)
    ) {
      throw new Error("API contract policy contains a malformed package");
    }
    for (const candidate of packagePolicy.entries) {
      if (
        candidate === null ||
        typeof candidate !== "object" ||
        typeof candidate.subpath !== "string" ||
        !API_AUDIENCES.includes(candidate.audience as ApiAudience) ||
        typeof candidate.description !== "string" ||
        candidate.description.trim().length === 0
      ) {
        throw new Error(`API contract policy contains a malformed entry for ${packagePolicy.name}`);
      }
      const key = entryKey(packagePolicy.name, candidate.subpath);
      if (policy.has(key)) throw new Error(`API contract policy duplicates ${key}`);
      policy.set(key, candidate as ApiPolicyEntry);
    }
  }

  const actual = new Set(entries.map(({ packageName, subpath }) => entryKey(packageName, subpath)));
  const missing = [...actual].filter((key) => !policy.has(key));
  const stale = [...policy.keys()].filter((key) => !actual.has(key));
  if (missing.length > 0 || stale.length > 0) {
    throw new Error(
      [
        ...(missing.length === 0
          ? []
          : [
              `Unclassified published entry points:\n${missing.map((key) => `  ${key}`).join("\n")}`,
            ]),
        ...(stale.length === 0
          ? []
          : [
              `API policy entries with no package export:\n${stale.map((key) => `  ${key}`).join("\n")}`,
            ]),
      ].join("\n"),
    );
  }
  return policy;
}

function resolvedSymbol(checker: ts.TypeChecker, symbol: ts.Symbol): ts.Symbol {
  let current = symbol;
  const seen = new Set<ts.Symbol>();
  while ((current.flags & ts.SymbolFlags.Alias) !== 0 && !seen.has(current)) {
    seen.add(current);
    current = checker.getAliasedSymbol(current);
  }
  return current;
}

const TYPE_MODE = 1;
const VALUE_MODE = 2;

function inherentMode(checker: ts.TypeChecker, symbol: ts.Symbol): number {
  const flags = resolvedSymbol(checker, symbol).flags;
  let value = 0;
  if ((flags & ts.SymbolFlags.Type) !== 0) value |= TYPE_MODE;
  if ((flags & ts.SymbolFlags.Value) !== 0) value |= VALUE_MODE;
  if ((flags & ts.SymbolFlags.Namespace) !== 0) value |= TYPE_MODE | VALUE_MODE;
  return value === 0 ? TYPE_MODE : value;
}

function exportKind(exportMode: number): ExportKind {
  if ((exportMode & TYPE_MODE) !== 0 && (exportMode & VALUE_MODE) !== 0) {
    return "type-and-value";
  }
  return (exportMode & VALUE_MODE) !== 0 ? "value" : "type";
}

function hasExportModifier(node: ts.Node): boolean {
  return ts.canHaveModifiers(node)
    ? (ts.getModifiers(node)?.some(({ kind }) => kind === ts.SyntaxKind.ExportKeyword) ?? false)
    : false;
}

function bindingNames(name: ts.BindingName): ts.Identifier[] {
  if (ts.isIdentifier(name)) return [name];
  return name.elements.flatMap((element) =>
    ts.isOmittedExpression(element) ? [] : bindingNames(element.name),
  );
}

function moduleSymbol(
  checker: ts.TypeChecker,
  declaration: ts.ExportDeclaration,
): ts.Symbol | undefined {
  if (declaration.moduleSpecifier === undefined) return undefined;
  const symbol = checker.getSymbolAtLocation(declaration.moduleSpecifier);
  return symbol === undefined ? undefined : resolvedSymbol(checker, symbol);
}

/**
 * TypeScript's module symbol flattens `export type *`, losing whether a value declaration is
 * reachable only as a type. Reconstruct that one piece of public syntax through the declaration
 * graph so the snapshot distinguishes `import type` from a runtime import.
 */
function exportModes(
  checker: ts.TypeChecker,
  source: ts.SourceFile,
  cache: Map<ts.SourceFile, Map<string, number>>,
  active: Set<ts.SourceFile>,
): Map<string, number> {
  const cached = cache.get(source);
  if (cached !== undefined) return cached;
  if (active.has(source)) return new Map();
  active.add(source);

  const modes = new Map<string, number>();
  const add = (name: string, exportMode: number): void => {
    modes.set(name, (modes.get(name) ?? 0) | exportMode);
  };

  for (const statement of source.statements) {
    if (ts.isExportDeclaration(statement)) {
      const target = moduleSymbol(checker, statement);
      const targetSource = target?.declarations?.find(ts.isSourceFile);
      const targetModes =
        targetSource === undefined
          ? new Map<string, number>()
          : exportModes(checker, targetSource, cache, active);
      const targetExports =
        target === undefined
          ? new Map<string, ts.Symbol>()
          : new Map(checker.getExportsOfModule(target).map((symbol) => [symbol.getName(), symbol]));

      if (statement.exportClause === undefined) {
        for (const [name, symbol] of targetExports) {
          if (name === "default") continue;
          const targetMode = targetModes.get(name) ?? inherentMode(checker, symbol);
          add(name, statement.isTypeOnly ? TYPE_MODE : targetMode);
        }
      } else if (ts.isNamedExports(statement.exportClause)) {
        for (const element of statement.exportClause.elements) {
          const importedName = (element.propertyName ?? element.name).text;
          const targetSymbol = targetExports.get(importedName);
          const targetMode =
            targetModes.get(importedName) ??
            (targetSymbol === undefined ? TYPE_MODE : inherentMode(checker, targetSymbol));
          add(
            element.name.text,
            statement.isTypeOnly || element.isTypeOnly ? TYPE_MODE : targetMode,
          );
        }
      } else {
        add(
          statement.exportClause.name.text,
          statement.isTypeOnly ? TYPE_MODE : TYPE_MODE | VALUE_MODE,
        );
      }
      continue;
    }

    if (!hasExportModifier(statement)) continue;
    if (ts.isVariableStatement(statement)) {
      for (const declaration of statement.declarationList.declarations) {
        for (const name of bindingNames(declaration.name)) {
          const symbol = checker.getSymbolAtLocation(name);
          if (symbol !== undefined) add(name.text, inherentMode(checker, symbol));
        }
      }
      continue;
    }
    if (
      (ts.isClassDeclaration(statement) ||
        ts.isEnumDeclaration(statement) ||
        ts.isFunctionDeclaration(statement) ||
        ts.isInterfaceDeclaration(statement) ||
        ts.isModuleDeclaration(statement) ||
        ts.isTypeAliasDeclaration(statement)) &&
      statement.name !== undefined
    ) {
      const symbol = checker.getSymbolAtLocation(statement.name);
      if (symbol !== undefined) add(statement.name.text, inherentMode(checker, symbol));
    }
  }

  active.delete(source);
  cache.set(source, modes);
  return modes;
}

function generateSnapshot(
  entries: readonly PublishedEntry[],
  policy: ReadonlyMap<string, ApiPolicyEntry>,
): ApiSnapshot {
  const typedEntries = entries.filter(
    (entry): entry is PublishedEntry & { declarationFile: string } =>
      entry.declarationFile !== undefined,
  );
  const missing = typedEntries.filter(({ declarationFile }) => !existsSync(declarationFile));
  if (missing.length > 0) {
    throw new Error(
      `Build declarations before checking the API contract:\n${missing
        .map(
          ({ packageName, subpath }) =>
            `  ${packageName}${subpath === "." ? "" : subpath.slice(1)}`,
        )
        .join("\n")}`,
    );
  }

  const program = ts.createProgram({
    rootNames: typedEntries.map(({ declarationFile }) => declarationFile),
    options: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
      moduleResolution: ts.ModuleResolutionKind.Bundler,
      resolveJsonModule: true,
      skipLibCheck: true,
      strict: true,
    },
  });
  const checker = program.getTypeChecker();
  const printer = ts.createPrinter({ removeComments: true, newLine: ts.NewLineKind.LineFeed });
  const modeCache = new Map<ts.SourceFile, Map<string, number>>();
  const packages = new Map<string, SnapshotEntry[]>();

  for (const entry of typedEntries) {
    const source = program.getSourceFile(entry.declarationFile);
    const sourceSymbol = source === undefined ? undefined : checker.getSymbolAtLocation(source);
    if (source === undefined || sourceSymbol === undefined) {
      throw new Error(`Could not inspect declarations for ${entry.packageName} ${entry.subpath}`);
    }
    const modes = exportModes(checker, source, modeCache, new Set());
    const exported = checker
      .getExportsOfModule(sourceSymbol)
      .map((publicSymbol) => {
        const symbol = resolvedSymbol(checker, publicSymbol);
        const declarations = (symbol.getDeclarations() ?? [])
          .filter((declaration) => declaration.getSourceFile().isDeclarationFile)
          .map((declaration) =>
            printer.printNode(ts.EmitHint.Unspecified, declaration, declaration.getSourceFile()),
          )
          .sort();
        return {
          name: publicSymbol.getName(),
          kind: exportKind(
            modes.get(publicSymbol.getName()) ?? inherentMode(checker, publicSymbol),
          ),
          declarations,
        };
      })
      .sort((left, right) => left.name.localeCompare(right.name));
    const packageEntries = packages.get(entry.packageName) ?? [];
    const entryPolicy = policy.get(entryKey(entry.packageName, entry.subpath));
    if (entryPolicy === undefined) throw new Error("API policy validation did not cover an entry");
    packageEntries.push({
      subpath: entry.subpath,
      audience: entryPolicy.audience,
      exports: exported,
    });
    packages.set(entry.packageName, packageEntries);
  }

  return {
    schemaVersion: 2,
    packages: [...packages].map(([name, packageEntries]) => ({ name, entries: packageEntries })),
  };
}

function printReport(
  snapshot: ApiSnapshot,
  entries: readonly PublishedEntry[],
  policy: ReadonlyMap<string, ApiPolicyEntry>,
): void {
  const audiences = new Map<ApiAudience, number>();
  for (const entry of entries) {
    const audience = policy.get(entryKey(entry.packageName, entry.subpath))?.audience;
    if (audience !== undefined) audiences.set(audience, (audiences.get(audience) ?? 0) + 1);
  }
  const declarations = snapshot.packages.flatMap(({ entries: packageEntries }) => packageEntries);
  const exported = declarations.flatMap(({ exports }) => exports);
  const unique = new Set(
    snapshot.packages.flatMap(({ name, entries: packageEntries }) =>
      packageEntries.flatMap(({ exports }) =>
        exports.map(({ name: exportName }) => `${name}:${exportName}`),
      ),
    ),
  );
  console.log(
    `API policy covers ${String(entries.length)} entry points: ${API_AUDIENCES.map(
      (audience) => `${String(audiences.get(audience) ?? 0)} ${audience}`,
    ).join(", ")}.`,
  );
  console.log(
    `Declaration snapshot covers ${String(declarations.length)} typed entry points, ` +
      `${String(exported.length)} exports (${String(unique.size)} unique per package).`,
  );
  if (mode !== "report") return;
  for (const packageSnapshot of snapshot.packages) {
    console.log(`\n${packageSnapshot.name}`);
    for (const entry of packageSnapshot.entries) {
      const kinds: Record<ExportKind, number> = { type: 0, value: 0, "type-and-value": 0 };
      for (const item of entry.exports) kinds[item.kind] += 1;
      console.log(
        `  ${entry.subpath.padEnd(24)} ${entry.audience.padEnd(11)} ` +
          `${String(entry.exports.length).padStart(4)} exports ` +
          `(${String(kinds.value)} value, ${String(kinds.type)} type, ` +
          `${String(kinds["type-and-value"])} both)`,
      );
    }
  }
}

const entries = publishedEntries();
const policy = apiPolicy(entries);
const snapshot = generateSnapshot(entries, policy);
const generated = `${JSON.stringify(snapshot, null, 2)}\n`;
printReport(snapshot, entries, policy);

if (mode === "write") {
  writeFileSync(snapshotFile, generated);
  console.log(`Wrote ${path.relative(repoRoot, snapshotFile)}.`);
} else if (mode === "check") {
  if (!existsSync(snapshotFile) || readFileSync(snapshotFile, "utf8") !== generated) {
    console.error(
      "Published declarations changed. Review compatibility and changelog impact, then run `npm run api:update`.",
    );
    process.exitCode = 1;
  } else {
    console.log("Published declaration contract matches the checked snapshot.");
  }
} else if (mode !== "report") {
  throw new Error(`Unknown API contract mode: ${mode}`);
}
