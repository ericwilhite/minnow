/** Internal, read-only owner descriptions. These are DTOs, never native catalog records or IR. */
export interface CatalogColumnDescription {
  readonly id: number;
  readonly name: string;
  readonly sqlType: string;
  readonly typeId: number | null;
  readonly autoIncrement: boolean;
  readonly nullable: boolean;
  readonly defaultSql: string | null;
  readonly generatedSql: string | null;
  readonly identity: { readonly sequenceId: number; readonly always: boolean } | null;
  readonly enumValues: readonly string[] | null;
}
export interface CatalogIndexDescription {
  readonly id: number;
  readonly name: string;
  readonly columnIds: readonly number[];
  readonly directions: ReadonlyArray<"asc" | "desc">;
  readonly unique: boolean;
  readonly kind: "ordered" | "equality";
  readonly role: "primary" | "unique" | "secondary" | "foreignKey";
  readonly state: "ready" | "invalid";
}
export interface CatalogTableDescription {
  readonly id: number;
  readonly name: string;
  readonly schemaVersion: number;
  readonly managed: boolean;
  readonly primaryKey: number | null;
  readonly columns: readonly CatalogColumnDescription[];
  readonly indexes: readonly CatalogIndexDescription[];
  readonly checks: ReadonlyArray<{
    readonly id: number;
    readonly name: string;
    readonly sql: string;
    readonly columnIds: readonly number[];
  }>;
  readonly foreignKeys: ReadonlyArray<{
    readonly id: number;
    readonly name: string;
    readonly columnIds: readonly number[];
    readonly parentTableId: number;
    readonly parentIndexId: number;
    readonly parentName: string | null;
    readonly supportIndexId: number | null;
    readonly onDelete: string;
    readonly onUpdate: string;
    readonly enforced: boolean;
  }>;
}
export interface CatalogDescription {
  readonly version: 1;
  readonly catalogVersion: number;
  readonly tables: readonly CatalogTableDescription[];
  readonly views: ReadonlyArray<{
    readonly id: number;
    readonly name: string;
    readonly sql: string;
    readonly managed: boolean;
    readonly columns: ReadonlyArray<{
      readonly name: string;
      readonly sqlType: string;
      readonly nullable: boolean;
    }>;
    readonly dependencies: ReadonlyArray<{
      readonly kind: "table" | "view" | "type" | "sequence";
      readonly objectId: number;
      readonly columnIds: readonly number[];
    }>;
  }>;
  readonly types: ReadonlyArray<{
    readonly id: number;
    readonly name: string;
    readonly labels: readonly string[];
  }>;
  /** Definition only: session currval and mutable storage state aren't catalog facts. */
  readonly sequences: ReadonlyArray<{
    readonly id: number;
    readonly name: string;
    readonly start: string;
    readonly increment: string;
    readonly min: string;
    readonly max: string;
    readonly cycle: boolean;
    readonly ownedBy: { readonly tableId: number; readonly columnId: number } | null;
  }>;
  /** Logical definitions only; no native roots, leases or guessed build/firing state. */
  readonly fts: ReadonlyArray<{
    readonly id: number;
    readonly tableId: number;
    readonly columnId: number;
    readonly tokenizerVersion: number;
    readonly rendererVersion: number;
    readonly scorerVersion: number;
    readonly configVersion: number;
  }>;
  readonly triggers: ReadonlyArray<{
    readonly id: number;
    readonly name: string;
    readonly tableId: number;
    readonly timing: "before" | "after";
    readonly event: "insert" | "update" | "delete";
    readonly statements: ReadonlyArray<{
      readonly sql: string;
      readonly bindings: ReadonlyArray<{
        readonly source: "old" | "new";
        readonly columnId: number;
        readonly name: string;
      }>;
    }>;
  }>;
}
/** Diagnostic lowered plan. Runtime spill/routes and timings require execution and aren't claimed. */
export interface ExplainDescription {
  readonly version: 1;
  readonly catalogVersion: number;
  readonly command: string;
  readonly kind: string;
  readonly text: string;
  readonly parameters: readonly string[];
  readonly columns: ReadonlyArray<{
    readonly name: string;
    readonly sqlType: string;
    readonly nullable: boolean;
  }>;
}
