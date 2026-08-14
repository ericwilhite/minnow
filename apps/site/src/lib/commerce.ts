/**
 * The commerce dataset behind the floating-devtools demo: five related tables generated
 * deterministically in the page and loaded into a worker-hosted database in columnar batches.
 * The same seed always builds the same rows, so numbers quoted in the docs stay true.
 */
import type { MinnowDatabaseClient } from "@minnowdb/core/client";

/** Deterministic PRNG, so every visitor gets the identical dataset. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const REGION_NAMES = [
  ["North America", "USD"],
  ["Latin America", "BRL"],
  ["Western Europe", "EUR"],
  ["Northern Europe", "SEK"],
  ["Middle East", "AED"],
  ["East Asia", "JPY"],
  ["South Asia", "INR"],
  ["Oceania", "AUD"],
] as const;

const FIRST_NAMES = [
  "Ada",
  "Grace",
  "Katherine",
  "Alan",
  "Barbara",
  "Edsger",
  "Radia",
  "Tony",
  "Margaret",
  "Donald",
  "Frances",
  "John",
  "Joan",
  "Dennis",
  "Adele",
  "Ken",
] as const;

const LAST_NAMES = [
  "Lovelace",
  "Hopper",
  "Johnson",
  "Turing",
  "Liskov",
  "Dijkstra",
  "Perlman",
  "Hoare",
  "Hamilton",
  "Knuth",
  "Allen",
  "Backus",
  "Clarke",
  "Ritchie",
  "Goldberg",
  "Thompson",
] as const;

const SEGMENTS = ["enterprise", "mid-market", "smb", "consumer"] as const;
const CATEGORIES = ["audio", "compute", "display", "input", "storage", "networking"] as const;
const ADJECTIVES = ["quiet", "rapid", "solid", "compact", "modular", "wireless"] as const;
const ORDER_STATUSES = [
  "paid",
  "paid",
  "paid",
  "pending",
  "shipped",
  "shipped",
  "refunded",
  "cancelled",
] as const;

export const COMMERCE_COUNTS = {
  regions: REGION_NAMES.length,
  customers: 5_000,
  products: 800,
  orders: 40_000,
  order_items: 120_000,
} as const;

export const COMMERCE_TOTAL_ROWS = Object.values(COMMERCE_COUNTS).reduce(
  (sum, rows) => sum + rows,
  0,
);

/** What the demo's console opens with — a four-table join answered from the worker. */
export const commerceQuery = `SELECT r.name AS region, p.category, COUNT(*) AS items, ROUND(SUM(i.quantity * i.unit_price), 2) AS revenue
FROM order_items i
JOIN orders o ON o.order_id = i.order_id
JOIN customers c ON c.customer_id = o.customer_id
JOIN regions r ON r.region_id = c.region_id
JOIN products p ON p.product_id = i.product_id
WHERE o.status = 'paid'
GROUP BY r.name, p.category
ORDER BY revenue DESC
LIMIT 12`;

interface LoadProgress {
  table: string;
  loadedRows: number;
  totalRows: number;
}

/**
 * Creates the five tables and loads them in columnar chunks, reporting progress per chunk.
 * Chunked so the worker commits steadily instead of holding one giant batch.
 */
export async function loadCommerceDataset(
  client: MinnowDatabaseClient,
  onProgress?: (progress: LoadProgress) => void,
): Promise<void> {
  const random = mulberry32(20260813);
  const pick = <T>(values: readonly T[]): T => {
    const value = values[Math.floor(random() * values.length)];
    if (value === undefined) throw new Error("Empty pick list");
    return value;
  };
  let loadedRows = 0;
  const report = (table: string): void => {
    onProgress?.({ table, loadedRows, totalRows: COMMERCE_TOTAL_ROWS });
  };

  const insertChunked = async (
    table: string,
    rowCount: number,
    columns: Record<string, (index: number) => string | number | boolean | Date | null>,
  ): Promise<void> => {
    const names = Object.keys(columns);
    const CHUNK = 20_000;
    for (let start = 0; start < rowCount; start += CHUNK) {
      const length = Math.min(CHUNK, rowCount - start);
      const batch: Record<string, Array<string | number | boolean | Date | null>> = {};
      for (const name of names) {
        const generate = columns[name];
        const values = new Array<string | number | boolean | Date | null>(length);
        for (let offset = 0; offset < length; offset += 1) {
          values[offset] = generate(start + offset);
        }
        batch[name] = values;
      }
      await client.insertBatch(table, { columns: batch });
      loadedRows += length;
      report(table);
    }
  };

  await client.createTable({
    name: "regions",
    uniqueKey: "region_id",
    columns: [
      { name: "region_id", type: "number" },
      { name: "name", type: "string" },
      { name: "currency", type: "string" },
    ],
  });
  await insertChunked("regions", COMMERCE_COUNTS.regions, {
    region_id: (index) => index + 1,
    name: (index) => REGION_NAMES[index]?.[0] ?? "Unknown",
    currency: (index) => REGION_NAMES[index]?.[1] ?? "USD",
  });

  await client.createTable({
    name: "customers",
    uniqueKey: "customer_id",
    columns: [
      { name: "customer_id", type: "number" },
      { name: "region_id", type: "number" },
      { name: "name", type: "string" },
      { name: "segment", type: "string" },
      { name: "signed_up", type: "datetime" },
      { name: "active", type: "boolean" },
    ],
  });
  await insertChunked("customers", COMMERCE_COUNTS.customers, {
    customer_id: (index) => index + 1,
    region_id: () => 1 + Math.floor(random() * COMMERCE_COUNTS.regions),
    name: (index) => `${pick(FIRST_NAMES)} ${pick(LAST_NAMES)} ${String(index + 1)}`,
    segment: () => pick(SEGMENTS),
    signed_up: () =>
      new Date(Date.UTC(2024, Math.floor(random() * 24), 1 + Math.floor(random() * 27))),
    active: () => random() < 0.9,
  });

  await client.createTable({
    name: "products",
    uniqueKey: "product_id",
    columns: [
      { name: "product_id", type: "number" },
      { name: "sku", type: "string" },
      { name: "category", type: "string" },
      { name: "price", type: "number" },
      { name: "stock", type: "number" },
    ],
  });
  await insertChunked("products", COMMERCE_COUNTS.products, {
    product_id: (index) => index + 1,
    sku: (index) => `${pick(ADJECTIVES)}-${pick(CATEGORIES)}-${String(index + 1).padStart(4, "0")}`,
    category: () => pick(CATEGORIES),
    price: () => Math.round((5 + random() * 995) * 100) / 100,
    stock: () => Math.floor(random() * 500),
  });

  await client.createTable({
    name: "orders",
    uniqueKey: "order_id",
    columns: [
      { name: "order_id", type: "number" },
      { name: "customer_id", type: "number" },
      { name: "status", type: "string" },
      { name: "placed_at", type: "datetime" },
      { name: "total", type: "number" },
    ],
  });
  await insertChunked("orders", COMMERCE_COUNTS.orders, {
    order_id: (index) => index + 1,
    customer_id: () => 1 + Math.floor(random() * COMMERCE_COUNTS.customers),
    status: () => pick(ORDER_STATUSES),
    placed_at: () =>
      new Date(
        Date.UTC(
          2025,
          Math.floor(random() * 20),
          1 + Math.floor(random() * 27),
          Math.floor(random() * 24),
          Math.floor(random() * 60),
        ),
      ),
    total: () => Math.round(random() * 2_000 * 100) / 100,
  });

  await client.createTable({
    name: "order_items",
    uniqueKey: "item_id",
    columns: [
      { name: "item_id", type: "number" },
      { name: "order_id", type: "number" },
      { name: "product_id", type: "number" },
      { name: "quantity", type: "number" },
      { name: "unit_price", type: "number" },
    ],
  });
  await insertChunked("order_items", COMMERCE_COUNTS.order_items, {
    item_id: (index) => index + 1,
    order_id: () => 1 + Math.floor(random() * COMMERCE_COUNTS.orders),
    product_id: () => 1 + Math.floor(random() * COMMERCE_COUNTS.products),
    quantity: () => 1 + Math.floor(random() * 8),
    unit_price: () => Math.round((5 + random() * 995) * 100) / 100,
  });
}
