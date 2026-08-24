/**
 * What the editor is told about this page.
 *
 * The packages' own `.d.ts` files describe `Kysely<DB>`; they cannot describe *this* `DB`,
 * because the playground's schema is a value in this repository rather than a published type. So
 * the ambient declaration below is printed from the same `retailSchema` the loader migrates —
 * one derivation, which is what stops the editor promising a column the database does not have.
 */
import { PLAYGROUND_DATABASE } from "@/lib/dataset/load";
import type { TableSpec } from "@/lib/dataset/retail";

const TYPES = {
  boolean: "boolean",
  number: "number",
  string: "string",
  datetime: "Date",
} as const;

/**
 * The ambient file the console adds to the editor: the row types, and the two names a snippet is
 * handed. It declares no imports or exports of its own, which is what keeps these global — the
 * `sql` tag is deliberately not among them, so a snippet that wants it imports it the way an
 * application would.
 */
export function playgroundDeclarations(tables: readonly TableSpec[]): string {
  const rows = tables.map((table) => {
    const columns = table.columns.map((column) => {
      const value = `${TYPES[column.type]}${column.nullable === true ? " | null" : ""}`;
      const insert = `${value}${column.nullable === true ? " | undefined" : ""}`;
      const update = column.name === table.uniqueKey ? "never" : value;
      return `    ${column.name}: MinnowColumnType<${value}, ${insert}, ${update}, ${value}>;`;
    });
    return `  /** ${table.description} */\n  ${table.name}: {\n${columns.join("\n")}\n  };`;
  });

  return `type MinnowColumnType<S, I, U, O> = import("@minnowdb/kysely").MinnowColumnType<S, I, U, O>;

/**
 * The playground's database, as Kysely sees it. Every table below was created from
 * the same declaration, so a column that autocompletes here exists in storage.
 */
interface DB {
${rows.join("\n")}
}

/** Kysely over the database this page built. Start with \`db.selectFrom("orders")\`. */
declare const db: import("kysely").Kysely<DB>;

/** The engine underneath, for the batch APIs and everything the builder does not cover. */
declare const database: import("@minnowdb/core/client").MinnowDatabaseClient;
`;
}

/**
 * How those two names were made, shown to the reader beside the editor.
 *
 * It is the code this page actually runs, minus the progress reporting — a reader who copies it
 * into an application gets the same objects. Kept next to the declarations so the two describe
 * one setup rather than two.
 */
export const SETUP_SNIPPET = `import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { createKysely } from "@minnowdb/kysely";
import { retailDefinition } from "./schema";

const worker = new Worker(new URL("@minnowdb/core/worker", import.meta.url), {
  type: "module",
});
const database = new MinnowDatabaseClient(worker, {
  store: { kind: "indexeddb", name: "${PLAYGROUND_DATABASE}" },
});
await database.ready();
await database.migrate(retailDefinition);

const db = createKysely({ driver: database, schema: retailDefinition });`;
