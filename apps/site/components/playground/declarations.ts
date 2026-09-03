/**
 * What the editor is told about this page.
 *
 * The packages' own `.d.ts` files describe `Kysely<DB>` and `MinnowDatabaseClient<TSchema>`;
 * they cannot name *this* schema, because the playground's declaration is a value in this
 * repository rather than a published type. So the type generator ships `lib/dataset/schema.ts`
 * to the editor as a module of its own, `./schema`, and the ambient file below derives everything
 * from it: the Kysely `DB` table by table (so each table's one-line description survives as
 * hover text) and the schema-typed client. One declaration, which is what stops the editor
 * promising a column the database does not have.
 */
import { PLAYGROUND_DATABASE } from "@/lib/dataset/load";
import type { TableSpec } from "@/lib/dataset/retail";

/**
 * The ambient file the console adds to the editor: the row types, and the two names a snippet is
 * handed. It declares no imports or exports of its own, which is what keeps these global — the
 * `sql` tag is deliberately not among them, so a snippet that wants it imports it the way an
 * application would.
 */
export function playgroundDeclarations(tables: readonly TableSpec[]): string {
  const rows = tables.map(
    (table) => `  /** ${table.description} */\n  ${table.name}: RetailTables["${table.name}"];`,
  );

  return `/** The declaration the database was migrated from; a snippet may import it from "./schema" too. */
type RetailSchema = typeof import("./schema").retailDefinition;
type RetailTables = import("@minnowdb/kysely").InferKyselyDatabase<RetailSchema>;

/**
 * The playground's database, as Kysely sees it. Every table below was created from
 * the same declaration, so a column that autocompletes here exists in storage.
 */
interface DB {
${rows.join("\n")}
}

/** Kysely over the database this page built. Start with \`db.selectFrom("orders")\`. */
declare const db: import("kysely").Kysely<DB>;

/**
 * The engine underneath, typed by the same declaration: batch writes, keyed updates, reads and
 * write scopes are all checked by table name. Try \`database.readTable("stores", { columns: ["city"] })\`.
 */
declare const database: import("@minnowdb/core/client").MinnowDatabaseClient<RetailSchema>;
`;
}

/**
 * How those two names were made, shown to the reader beside the editor.
 *
 * It is the code this page actually runs, minus the progress reporting and the load receipt —
 * a reader who copies it into an application gets the same objects. Kept next to the
 * declarations so the two describe one setup rather than two.
 */
export const SETUP_SNIPPET = `import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { createKysely } from "@minnowdb/kysely";
import { retailDefinition } from "./schema";

// db-worker.ts contains: import "@minnowdb/core/worker";
const worker = new Worker(new URL("./db-worker.ts", import.meta.url), {
  type: "module",
});
const database = new MinnowDatabaseClient(worker, {
  store: { kind: "indexeddb", name: "${PLAYGROUND_DATABASE}" },
  schema: retailDefinition, // types every batch method by table name
});
await database.ready();
await database.migrate();

const db = createKysely({ driver: database, schema: retailDefinition });`;
