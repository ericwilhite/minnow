/** Rebuild-only DDL provenance, outside the published CompiledStatement union. */
const drops = new WeakMap<object, "type" | "sequence">();
/** PostgreSQL's underlying built-in pg_type names, rather than SQL parser aliases. */
const builtIns = new Set(
  "int2 int4 int8 float4 float8 numeric bool text varchar bpchar timestamp timestamptz date time interval json jsonb uuid bytea".split(
    " ",
  ),
);
export function builtInCatalogType(name: string): boolean {
  return builtIns.has(name);
}
export function rememberCatalogDrop(statement: object, kind: "type" | "sequence"): void {
  drops.set(statement, kind);
}
export function catalogDrop(statement: object): "type" | "sequence" | undefined {
  return drops.get(statement);
}

let catalogCapture = 0;
const declarations = new WeakMap<object, readonly object[]>();
/** Only definition binding retains pruned CTE sources. Ordinary query execution never sees them. */
export function captureCatalogDeclarations<T>(compile: () => T): T {
  catalogCapture++;
  try {
    return compile();
  } finally {
    catalogCapture--;
  }
}
export function rememberCatalogDeclarations(query: object, create: () => readonly object[]): void {
  if (catalogCapture === 0) return;
  const sources = create();
  if (sources.length) declarations.set(query, sources);
}
export function catalogDeclarations(query: object): readonly object[] | undefined {
  return declarations.get(query);
}
export function copyCatalogDeclarations(source: object, target: object): void {
  const sources = declarations.get(source);
  if (sources) declarations.set(target, sources);
}

/** Native-only identity mode, carried by the default object through column copies. */
const alwaysIdentities = new WeakSet();
export function rememberAlwaysIdentity(value: object): void {
  alwaysIdentities.add(value);
}
export function alwaysIdentity(value: object | undefined): boolean {
  return value !== undefined && alwaysIdentities.has(value);
}

/** Exact native clauses; the published compiled DTO keeps its shipped three-action shape. */
export type ReferentialSourceAction =
  "restrict" | "cascade" | "set null" | "no action" | "set default";
export interface ForeignKeyActionSource {
  readonly onDelete: ReferentialSourceAction;
  readonly onUpdate: ReferentialSourceAction | "follow";
}
const foreignKeys = new WeakMap<object, ForeignKeyActionSource>();
export function rememberForeignKeyActions(key: object, actions: ForeignKeyActionSource): void {
  foreignKeys.set(key, actions);
}
export function foreignKeyActions(key: object): ForeignKeyActionSource | undefined {
  return foreignKeys.get(key);
}
