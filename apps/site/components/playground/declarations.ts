/**
 * What the editor is told about this page.
 *
 * The packages' own `.d.ts` files describe `Minnow<DB>`; they cannot describe *this* `DB`,
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
    const columns = table.columns.map(
      (col) => `    ${col.name}: ${TYPES[col.type]}${col.nullable === true ? " | null" : ""};`,
    );
    // `FromRow` reads the row once and produces the three shapes the builders need — what a
    // row selects as, what an insert accepts, and what an update changes. Without it every
    // column resolves to `never` and the builder accepts nothing.
    return `  /** ${table.description} */\n  ${table.name}: FromRow<{\n${columns.join("\n")}\n  }>;`;
  });

  return `type FromRow<TRow> = import("@minnowdb/client").FromRow<TRow>;

/**
 * The playground's database, as the typed client sees it. Every table below was created from
 * the same declaration, so a column that autocompletes here exists in storage.
 */
interface DB {
${rows.join("\n")}
}

/** The typed client over the database this page built. Start with \`db.selectFrom("orders")\`. */
declare const db: import("@minnowdb/client").Minnow<DB>;

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
import { createMinnow, type InferDatabase } from "@minnowdb/client";
import { retailDefinition } from "./schema";

const worker = new Worker(new URL("@minnowdb/core/worker", import.meta.url), {
  type: "module",
});
const database = new MinnowDatabaseClient(worker, {
  store: { kind: "indexeddb", name: "${PLAYGROUND_DATABASE}" },
});
await database.ready();
await database.migrate(retailDefinition);

interface DB extends InferDatabase<typeof retailDefinition> {}
const db = createMinnow<DB>(database, { schema: retailDefinition });`;
