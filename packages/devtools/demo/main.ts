/**
 * Manual and browser-test fixture for the devtools panel: a small in-memory database, a host page
 * that must stay interactive while the panel is open, and the panel itself mounted over it.
 *
 * Serve the repository root with vite and open /packages/devtools/demo/.
 */
import { column, createMinnow, MinnowDatabase, schema, table } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { mountMinnowDevtools } from "@minnowdb/devtools";

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  city: column.string().nullable(),
});
const orders = table("orders", {
  order_id: column.number().unique(),
  person: column.string().references("people", "name"),
  total: column.number(),
});
const demoSchema = schema([people, orders]);

const database = new MinnowDatabase(new MemoryBlockStore());
await database.migrate(demoSchema);

const db = createMinnow(database, { schema: demoSchema });
await db
  .insertInto("people")
  .values([
    { name: "Ada", score: 10, city: "London" },
    { name: "Grace", score: 20, city: "DC" },
    { name: "Katherine", score: 30, city: null },
  ])
  .execute();
await db
  .insertInto("orders")
  .values([
    { order_id: 1, person: "Ada", total: 12.5 },
    { order_id: 2, person: "Ada", total: 7.5 },
    { order_id: 3, person: "Grace", total: 40 },
  ])
  .execute();

// Attaching the facade proves the driver accessor: the panel unwraps it to the database itself.
const devtools = mountMinnowDevtools(db, {
  corner: "bottom-right",
  defaultOpen: true,
  initialQuery: "SELECT name, score, city FROM people ORDER BY score DESC",
});

const log = document.querySelector("#log");
let clicks = 0;
document.querySelector("#ping")?.addEventListener("click", () => {
  clicks += 1;
  if (log !== null) log.textContent = `host page clicks: ${String(clicks)}`;
});

// Exposed so browser tests can drive the panel without reaching into the shadow root.
Object.assign(window, { devtools, database, db });
