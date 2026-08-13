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

/**
 * A wide, keyless table and a big keyed one, so the explorer's two hard cases — no cursor to page
 * with, and far more rows than fit in the DOM — are both reachable from the demo.
 */
const wideColumns = Array.from({ length: 40 }, (_, index) => ({
  name: `metric_${String(index).padStart(2, "0")}`,
  type: "number" as const,
}));
await database.createTable({
  name: "readings",
  columns: [{ name: "sensor", type: "string" }, ...wideColumns],
});

const bulk = Number(new URLSearchParams(location.search).get("rows") ?? "20000");
await database.createTable({
  name: "events",
  uniqueKey: "event_id",
  columns: [
    { name: "event_id", type: "number" },
    { name: "kind", type: "string" },
    { name: "amount", type: "number" },
    { name: "note", type: "string", nullable: true },
    { name: "at", type: "datetime" },
  ],
});
// A table whose key and timestamp the engine fills, so the insert form's optional columns are
// reachable from the demo.
await database.createTable({
  name: "notes",
  uniqueKey: "note_id",
  columns: [
    { name: "note_id", type: "number", defaultValue: { kind: "autoincrement" } },
    { name: "body", type: "string" },
    { name: "written_at", type: "datetime", defaultValue: { kind: "now" } },
  ],
});
await database.insert("notes", { body: "the first note" });

const kinds = ["created", "paid", "shipped", "refunded", "cancelled"];
const writer = database.bufferedWriter("events", { maxRows: 5000 });
for (let index = 0; index < bulk; index += 1) {
  await writer.add({
    event_id: index,
    kind: kinds[index % kinds.length] ?? "created",
    amount: Math.round((index % 997) * 13.5 * 100) / 100,
    note: index % 11 === 0 ? null : `note ${String(index % 250)}`,
    at: new Date(Date.UTC(2026, 0, 1 + (index % 28))),
  });
}
await writer.close();

for (let index = 0; index < 200; index += 1) {
  const row: Record<string, number | string> = { sensor: `sensor ${String(index % 12)}` };
  for (const column of wideColumns) row[column.name] = Math.round(Math.sin(index) * 1000) / 10;
  await database.insert("readings", row);
}

// Attaching the facade proves the driver accessor: the panel unwraps it to the database itself.
const devtools = mountMinnowDevtools(db, {
  corner: "bottom-right",
  defaultOpen: true,
  initialQuery: "SELECT name, score, city FROM people ORDER BY score DESC",
});

// The panel reads `theme` off its host element, so both palettes are reachable without changing
// the operating system's setting.
const themeSelect = document.querySelector<HTMLSelectElement>("#theme");
themeSelect?.addEventListener("change", () => {
  if (themeSelect.value === "") devtools.element.removeAttribute("theme");
  else devtools.element.setAttribute("theme", themeSelect.value);
});

const log = document.querySelector("#log");
let clicks = 0;
document.querySelector("#ping")?.addEventListener("click", () => {
  clicks += 1;
  if (log !== null) log.textContent = `host page clicks: ${String(clicks)}`;
});

// Exposed so browser tests can drive the panel without reaching into the shadow root.
Object.assign(window, { devtools, database, db });
