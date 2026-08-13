/**
 * The database behind the docs playground: small enough to read whole, wide enough that joins,
 * grouping, filters, and full-text search all have something to say. It runs entirely in the
 * page against an in-memory store, so nothing is persisted and a reload starts over.
 */
import { MinnowDatabase, column, createMinnow, schema, table } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";

const customers = table("customers", {
  customer_id: column.number().unique(),
  name: column.string(),
  segment: column.string(),
  city: column.string().nullable(),
  joined: column.datetime(),
});

const orders = table("orders", {
  order_id: column.number().unique(),
  customer_id: column.number().references("customers", "customer_id"),
  status: column.string(),
  total: column.number(),
  note: column.string().nullable(),
  placed_at: column.datetime(),
});

export const playgroundSchema = schema([customers, orders]);

const names = [
  ["Ada Lovelace", "enterprise", "London"],
  ["Grace Hopper", "enterprise", "Arlington"],
  ["Katherine Johnson", "mid-market", null],
  ["Alan Turing", "smb", "Manchester"],
  ["Barbara Liskov", "mid-market", "Boston"],
  ["Edsger Dijkstra", "smb", "Eindhoven"],
  ["Radia Perlman", "enterprise", "Boston"],
  ["Tony Hoare", "smb", null],
] as const;

const statuses = ["paid", "paid", "pending", "refunded", "cancelled"] as const;
const notes = [null, "gift wrap", "net 30", null, "damaged in transit", null] as const;

/** Builds and seeds the playground database. Deterministic, so the docs can quote its output. */
export async function createPlaygroundDatabase(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.migrate(playgroundSchema);
  const db = createMinnow(database, { schema: playgroundSchema });

  await db
    .insertInto("customers")
    .values(
      names.map(([name, segment, city], index) => ({
        customer_id: index + 1,
        name,
        segment,
        city,
        joined: new Date(Date.UTC(2025, index % 12, 1 + (index % 27))),
      })),
    )
    .execute();

  await db
    .insertInto("orders")
    .values(
      Array.from({ length: 60 }, (_, index) => ({
        order_id: index + 1,
        customer_id: (index % names.length) + 1,
        status: statuses[index % statuses.length] ?? "paid",
        total: Math.round((25 + ((index * 37) % 900)) * 100) / 100,
        note: notes[index % notes.length] ?? null,
        placed_at: new Date(Date.UTC(2026, 0, 1 + (index % 28), 9, index % 60)),
      })),
    )
    .execute();

  return database;
}

/** What the console starts with — a query that shows a join, an aggregate, and an ordering. */
export const playgroundQuery = `SELECT c.segment, COUNT(*) AS orders, ROUND(SUM(o.total), 2) AS revenue
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE o.status = 'paid'
GROUP BY c.segment
ORDER BY revenue DESC`;
