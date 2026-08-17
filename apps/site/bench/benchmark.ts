import type { ColumnValues, Compression, LogicalType } from "@minnowdb/core/block-format";

export const scenarioIds = ["telemetry", "commerce", "compression"] as const;
export const benchmarkFocuses = ["roundtrip", "write", "read"] as const;
export const durabilityModes = ["relaxed", "strict"] as const;

export type ScenarioId = (typeof scenarioIds)[number];
export type BenchmarkFocus = (typeof benchmarkFocuses)[number];
export type DurabilityMode = (typeof durabilityModes)[number];

type ColumnDefinitionByType = {
  [Type in LogicalType]: {
    name: string;
    type: Type;
    estimatedBytesPerRow: number;
    valueAt(index: number, rowCount: number, scale: number): ColumnValues[Type] | null;
  };
};

export type ColumnDefinition = ColumnDefinitionByType[LogicalType];

export interface EntityDefinition {
  name: string;
  rows(scale: number): number;
  columns: ColumnDefinition[];
  primaryKey?: string;
  relationship?: string;
}

export interface ScenarioDefinition {
  id: ScenarioId;
  name: string;
  description: string;
  entities: EntityDefinition[];
}

export type GeneratedBatchColumns = Record<string, Array<boolean | number | string | Date | null>>;

export function generateEntityBatch(
  entity: EntityDefinition,
  startRow: number,
  rowCount: number,
  entityRows: number,
  scale: number,
): GeneratedBatchColumns {
  return Object.fromEntries(
    entity.columns.map((column) => [
      column.name,
      Array.from({ length: rowCount }, (_, index) =>
        column.valueAt(startRow + index, entityRows, scale),
      ),
    ]),
  );
}

export interface BenchmarkConfig {
  scenario: ScenarioId;
  focus: BenchmarkFocus;
  scale: number;
  rows: number;
  compression: Compression;
  targetBlockBytes: number;
  durability: DurabilityMode;
}

export interface BenchmarkProgress {
  phase:
    | "preparing"
    | "writing"
    | "committing"
    | "reading"
    | "transactions"
    | "library"
    | "queries"
    | "complete";
  completed: number;
  total: number;
  message: string;
}

export interface BlockMeasurement {
  id: string;
  entity: string;
  column: string;
  type: LogicalType;
  rows: number;
  encodedBytes: number;
  storedBytes: number;
  compressionRatio: number;
  checksum: number;
  verified: boolean;
}

export interface EntityMeasurement {
  name: string;
  rows: number;
  columns: number;
  blocks: number;
  encodedBytes: number;
  storedBytes: number;
}

export interface TransactionCheckMeasurement {
  id:
    | "data-commit"
    | "stable-snapshot"
    | "persistent-states"
    | "competing-writers"
    | "rebase"
    | "stale-recovery"
    | "leases"
    | "lost-response";
  label: string;
  durationMs: number;
  passed: boolean;
  detail: string;
}

export interface TransactionBenchmarkMeasurement {
  committedVersion: number;
  journaledBlocks: number;
  persistedStatus: "committed" | "missing" | "unexpected";
  probeDurationMs: number;
  checks: TransactionCheckMeasurement[];
  passed: boolean;
}

export interface LibraryTableMeasurement {
  name: string;
  rows: number;
  segments: number;
  blocks: number;
  storedBytes: number;
}

export interface LibraryCheckMeasurement {
  id:
    | "table-inventory"
    | "segment-inventory"
    | "concurrent-inserts"
    | "row-id-ranges"
    | "unique-schema"
    | "upsert-counts"
    | "unique-errors"
    | "snapshot-read"
    | "concurrent-upserts"
    | "update-patches"
    | "projected-read"
    | "delete-keys"
    | "append-compaction"
    | "write-metrics";
  label: string;
  durationMs: number;
  passed: boolean;
  detail: string;
}

export interface LibraryWriteBenchmarkMeasurement {
  rowsPerBlock: number;
  maxBatchRows: number;
  finalVersion: number;
  probeDurationMs: number;
  timingsMs: {
    createTables: number;
    generateBatches: number;
    insertBatches: number;
    upsertBatch: number;
    updateBatch: number;
    deleteBatch: number;
    projectedRead: number;
    verifyInventory: number;
    total: number;
  };
  upsert: {
    sampleRows: number;
    insertedRows: number;
    updatedRows: number;
    finalRows: number;
    baseVersion: number;
    version: number;
  };
  mutations: {
    updatedRows: number;
    deletedRows: number;
    projectedRows: number;
    updateVersion: number;
    deleteVersion: number | null;
    updateStoredBytes: number;
    deleteStoredBytes: number;
  };
  totals: {
    tables: number;
    rows: number;
    segments: number;
    blocks: number;
    storedBytes: number;
  };
  tables: LibraryTableMeasurement[];
  checks: LibraryCheckMeasurement[];
  passed: boolean;
}

/**
 * The library executing the query's own SQL. `supported` is decided by actually
 * compiling the SQL against the engine during the run, never by a checked-in
 * assumption, so this measurement cannot drift from the shipped SQL surface.
 */
export interface ReferenceQueryEngineMeasurement {
  supported: boolean;
  /** Compile or execution error reported by the engine when `supported` is false. */
  error?: string;
  /** Optimized plan from MinnowDatabase.explain(), absent when the SQL does not compile. */
  plan?: string;
  prepareMs: number;
  medianMs: number;
  p95Ms: number;
  iterations: number;
  resultRows: number;
  checksum: number;
  /** The engine result matches the independent JavaScript oracle tuple for tuple. */
  verified: boolean;
}

/** A hand-written JavaScript implementation kept as a baseline to compare the engine against. */
export interface ReferenceQueryJavascriptMeasurement {
  sampleCount: number;
  iterations: number;
  executionsPerSample: number;
  medianMs: number;
  p95Ms: number;
  oracleMs: number;
  totalMs: number;
  measurementMs: number;
  resultRows: number;
  checksum: number;
  oracleChecksum: number;
  verified: boolean;
}

export interface ReferenceQueryMeasurement {
  id: string;
  name: string;
  complexity: "simple" | "moderate" | "complex";
  /** The exact SQL submitted to the engine. Never an abbreviation of a longer statement. */
  sql: string;
  tables: string[];
  expectedRows: number;
  /** Set when the query is knowingly outside the current SQL surface, explaining what is missing. */
  surfaceGap?: string;
  engine: ReferenceQueryEngineMeasurement;
  javascript: ReferenceQueryJavascriptMeasurement;
  verified: boolean;
}

/**
 * Memory around one benchmark run, measured in the worker that owns the database. Whole-agent
 * bytes count WebAssembly and everything else the agent holds; the JavaScript heap counts only
 * the JS garbage collector's share. Whole-agent numbers are null on browsers that refuse the
 * measurement, and `agentDetail` then says why.
 */
export interface MemoryMeasurement {
  agentStatus: "measured" | "unsupported" | "not-isolated" | "refused";
  agentDetail: string;
  agentBytesBefore: number | null;
  agentBytesAfter: number | null;
  agentDeltaBytes: number | null;
  agentByType: Record<string, number> | null;
  heapBytesBefore: number | null;
  heapBytesAfter: number | null;
  heapDeltaBytes: number | null;
}

export interface ReferenceIntegrityCheckMeasurement {
  id: "row-counts-and-keys" | "foreign-keys" | "value-domains" | "transaction-balances";
  label: string;
  detail: string;
  durationMs: number;
  passed: boolean;
}

export interface ReferenceQueryBenchmarkMeasurement {
  orderRows: number;
  totalRows: number;
  setupMs: number;
  loadMs: number;
  indexMs: number;
  totalMs: number;
  /** Sum of the engine's median execution across every query it can compile. */
  engineTotalMs: number;
  engineSupportedQueries: number;
  sampleCount: number;
  tables: Array<{ name: string; rows: number; loadMs: number; relationship: string }>;
  integrityChecks: ReferenceIntegrityCheckMeasurement[];
  queries: ReferenceQueryMeasurement[];
  passed: boolean;
  note: string;
}

export interface AdHocQueryMetrics {
  prepareMs: number;
  executeMedianMs: number;
  executeP95Ms: number;
  totalMs: number;
  iterations: number;
  sourceRows: number;
  datasetRows: number;
  storedBytes: number;
}

export interface AdHocQueryResult {
  runId: string;
  sql: string;
  /** Optimized logical plan and physical strategy notes from MinnowDatabase.explain(). */
  plan: string;
  tables: string[];
  columns: string[];
  rowCount: number;
  previewRows: Array<Record<string, unknown>>;
  truncated: boolean;
  metrics: AdHocQueryMetrics;
}

export interface PersistedDatasetStatus {
  kind: "persisted-dataset-status";
  available: boolean;
  datasetCount: number;
  runId?: string;
  databaseName?: string;
  createdAt?: string;
  scale?: number;
  totalRows?: number;
  storedBytes?: number;
}

export interface BenchmarkResult {
  runId: string;
  recordedAt: string;
  config: BenchmarkConfig;
  environment: {
    userAgent: string;
    platform: string;
    hardwareConcurrency: number;
  };
  storage: {
    usageBefore?: number;
    usageAfter?: number;
    quota?: number;
    measuredDelta?: number;
    note: string;
  };
  memory: MemoryMeasurement;
  totals: {
    rows: number;
    entities: number;
    columns: number;
    blocks: number;
    encodedBytes: number;
    storedBytes: number;
    compressionRatio: number;
  };
  timingsMs: {
    transactionBegin: number;
    generate: number;
    encode: number;
    write: number;
    manifestCommit: number;
    read: number;
    decode: number;
    verify: number;
    aggregate: number;
    total: number;
  };
  kernel: {
    name: "numeric sum verification";
    numericValues: number;
    sum: number;
  };
  transactions: TransactionBenchmarkMeasurement;
  library: LibraryWriteBenchmarkMeasurement;
  referenceQueries: ReferenceQueryBenchmarkMeasurement;
  entities: EntityMeasurement[];
  blocks: BlockMeasurement[];
  verified: boolean;
}

const statuses = ["ok", "warning", "offline", "maintenance"] as const;
const regions = ["north", "south", "east", "west"] as const;
const segments = ["starter", "growth", "enterprise"] as const;
const textShapes = ["alpha", "alpha alpha", "beta", "gamma delta epsilon"] as const;

export const scenarios: ScenarioDefinition[] = [
  {
    id: "telemetry",
    name: "Devices & readings",
    description: "A list of devices and 100,000 time-stamped readings from them.",
    entities: [
      {
        name: "devices",
        rows: (total) => Math.max(10, Math.ceil(total / 1_000)),
        columns: [
          numberColumn("device_id", (index) => index + 1),
          stringColumn("region", (index) => regions[index % regions.length] ?? "north", 10),
          booleanColumn("enabled", (index) => index % 17 !== 0),
          datetimeColumn("installed_at", (index) => epoch(2023, index * 86_400_000)),
        ],
      },
      {
        name: "readings",
        rows: (total) => total,
        columns: [
          datetimeColumn("recorded_at", (index) => epoch(2026, index * 1_000)),
          numberColumn(
            "device_id",
            (index, rows) => (index % Math.max(10, Math.ceil(rows / 1_000))) + 1,
          ),
          numberColumn("temperature", (index) => 18 + deterministic(index) * 14),
          booleanColumn("healthy", (index) => index % 97 !== 0),
          stringColumn(
            "status",
            (index) => (index % 41 === 0 ? null : (statuses[index % statuses.length] ?? "ok")),
            14,
          ),
        ],
      },
    ],
  },
  {
    id: "commerce",
    name: "Relational commerce",
    description:
      "50 connected commerce tables ranging from compact dimensions to multi-million-row transactional ledgers.",
    entities: relationalCommerceEntities(),
  },
  {
    id: "compression",
    name: "Repeating data patterns",
    description: "Numbers and text with patterns that are easy or hard to make smaller.",
    entities: [
      {
        name: "samples",
        rows: (total) => total,
        columns: [
          numberColumn("monotonic", (index) => index),
          numberColumn("repeated", (index) => index % 8),
          numberColumn("pseudo_random", (index) => deterministic(index + 311) * 1_000_000),
          booleanColumn("sparse_flag", (index) => index % 1_000 === 0),
          stringColumn(
            "text_shape",
            (index) => textShapes[index % textShapes.length] ?? "alpha",
            24,
          ),
        ],
      },
    ],
  },
];

export const relationalBaseRows = {
  countries: 8,
  regions: 16,
  currencies: 4,
  sales_channels: 6,
  customer_segments: 6,
  tax_jurisdictions: 24,
  tax_rates: 48,
  warehouses: 8,
  stores: 24,
  employees: 120,
  suppliers: 40,
  brands: 30,
  categories: 20,
  products: 200,
  product_suppliers: 400,
  price_lists: 12,
  product_prices: 600,
  purchase_orders: 240,
  purchase_order_items: 1_200,
  receipts: 900,
  receipt_items: 2_700,
  customers: 200,
  customer_addresses: 300,
  customer_payment_methods: 250,
  loyalty_accounts: 180,
  loyalty_transactions: 1_200,
  promotions: 30,
  carts: 600,
  cart_items: 2_400,
  orders: 1_000,
  order_items: 3_000,
  order_events: 25_000,
  order_discounts: 250,
  order_taxes: 3_000,
  payments: 1_000,
  payment_transactions: 2_000,
  payment_attempts: 3_000,
  fraud_reviews: 120,
  shipments: 800,
  shipment_items: 2_400,
  shipment_events: 3_200,
  delivery_attempts: 1_000,
  returns: 100,
  return_items: 100,
  refunds: 80,
  support_tickets: 300,
  support_messages: 1_500,
  inventory_movements: 4_000,
  inventory_snapshots: 10_000,
  audit_events: 22_000,
} as const;

export type RelationalTableName = keyof typeof relationalBaseRows;

export function relationalRowCount(name: RelationalTableName, scale: number): number {
  return Math.max(1, Math.ceil(relationalBaseRows[name] * scale));
}

export function relationalTotalRows(scale: number): number {
  return Object.keys(relationalBaseRows).reduce(
    (total, name) => total + relationalRowCount(name as RelationalTableName, scale),
    0,
  );
}

function relationalCommerceEntities(): EntityDefinition[] {
  const row =
    (name: RelationalTableName) =>
    (scale: number): number =>
      relationalRowCount(name, scale);
  const foreign = (name: RelationalTableName, index: number, scale: number): number =>
    (index % relationalRowCount(name, scale)) + 1;
  const entity = (
    name: RelationalTableName,
    primaryKey: string,
    relationship: string,
    columns: ColumnDefinition[],
  ): EntityDefinition => ({ name, rows: row(name), primaryKey, relationship, columns });
  return [
    entity("countries", "country_id", "countries 1 → many regions", [
      numberColumn("country_id", (index) => index + 1),
      stringColumn("code", (index) => `C${String(index + 1).padStart(2, "0")}`, 6),
      stringColumn("name", (index) => `Country ${String(index + 1)}`, 18),
    ]),
    entity("regions", "region_id", "regions many → 1 countries", [
      numberColumn("region_id", (index) => index + 1),
      numberColumn("country_id", (index, _rows, scale) => foreign("countries", index, scale)),
      stringColumn("name", (index) => `Region ${String(index + 1)}`, 18),
    ]),
    entity("currencies", "currency_id", "currencies 1 → many orders/payments", [
      numberColumn("currency_id", (index) => index + 1),
      stringColumn("code", (index) => ["USD", "EUR", "GBP", "CAD"][index % 4] ?? "USD", 7),
      numberColumn("usd_rate", (index) => 0.75 + (index % 8) * 0.1),
    ]),
    entity("sales_channels", "channel_id", "sales channels 1 → many carts/price lists", [
      numberColumn("channel_id", (index) => index + 1),
      stringColumn(
        "name",
        (index) =>
          ["web", "mobile", "store", "marketplace", "partner", "phone"][index % 6] ?? "web",
        14,
      ),
      booleanColumn("active", (index) => index % 11 !== 0),
    ]),
    entity("customer_segments", "segment_id", "segments 1 → many loyalty accounts", [
      numberColumn("segment_id", (index) => index + 1),
      stringColumn(
        "name",
        (index) =>
          ["new", "starter", "growth", "premium", "enterprise", "at_risk"][index % 6] ?? "new",
        14,
      ),
      numberColumn("points_multiplier", (index) => 1 + (index % 4) * 0.25),
    ]),
    entity("tax_jurisdictions", "jurisdiction_id", "jurisdictions many → 1 regions", [
      numberColumn("jurisdiction_id", (index) => index + 1),
      numberColumn("region_id", (index, _rows, scale) => foreign("regions", index, scale)),
      stringColumn("name", (index) => `Tax zone ${String(index + 1)}`, 20),
    ]),
    entity("tax_rates", "tax_rate_id", "tax rates many → 1 jurisdictions", [
      numberColumn("tax_rate_id", (index) => index + 1),
      numberColumn("jurisdiction_id", (index, _rows, scale) =>
        foreign("tax_jurisdictions", index, scale),
      ),
      stringColumn("tax_type", (index) => ["sales", "state", "local"][index % 3] ?? "sales", 10),
      numberColumn("rate", (index) => 0.02 + (index % 9) * 0.0075),
    ]),
    entity("warehouses", "warehouse_id", "warehouses many → 1 regions", [
      numberColumn("warehouse_id", (index) => index + 1),
      numberColumn("region_id", (index, _rows, scale) => foreign("regions", index, scale)),
      stringColumn("name", (index) => `Warehouse ${String(index + 1)}`, 20),
    ]),
    entity("stores", "store_id", "stores many → 1 regions/warehouses", [
      numberColumn("store_id", (index) => index + 1),
      numberColumn("region_id", (index, _rows, scale) => foreign("regions", index, scale)),
      numberColumn("warehouse_id", (index, _rows, scale) => foreign("warehouses", index, scale)),
      stringColumn("name", (index) => `Store ${String(index + 1)}`, 18),
      booleanColumn("active", (index) => index % 17 !== 0),
    ]),
    entity("employees", "employee_id", "employees many → 1 stores", [
      numberColumn("employee_id", (index) => index + 1),
      numberColumn("store_id", (index, _rows, scale) => foreign("stores", index, scale)),
      stringColumn(
        "role",
        (index) =>
          ["associate", "manager", "support", "operations", "auditor"][index % 5] ?? "associate",
        14,
      ),
      booleanColumn("active", (index) => index % 23 !== 0),
    ]),
    entity("suppliers", "supplier_id", "suppliers many → 1 regions", [
      numberColumn("supplier_id", (index) => index + 1),
      numberColumn("region_id", (index, _rows, scale) => foreign("regions", index, scale)),
      stringColumn("name", (index) => `Supplier ${String(index + 1)}`, 20),
      booleanColumn("active", (index) => index % 11 !== 0),
    ]),
    entity("brands", "brand_id", "brands 1 → many products", [
      numberColumn("brand_id", (index) => index + 1),
      stringColumn("name", (index) => `Brand ${String(index + 1)}`, 16),
    ]),
    entity("categories", "category_id", "categories 1 → many products", [
      numberColumn("category_id", (index) => index + 1),
      stringColumn("name", (index) => `Category ${String(index + 1)}`, 18),
    ]),
    entity("products", "product_id", "products many → 1 brands/categories", [
      numberColumn("product_id", (index) => index + 1),
      numberColumn("brand_id", (index, _rows, scale) => foreign("brands", index, scale)),
      numberColumn("category_id", (index, _rows, scale) => foreign("categories", index, scale)),
      stringColumn(
        "category",
        (index) =>
          ["Hardware", "Software", "Office", "Services", "Accessories"][index % 5] ?? "Hardware",
        16,
      ),
      stringColumn(
        "brand",
        (index, _rows, scale) => `Brand ${String(foreign("brands", index, scale))}`,
        16,
      ),
      stringColumn("sku", (index) => `SKU-${String(index + 1).padStart(6, "0")}`, 14),
      numberColumn("unit_cost", (index) => 5 + (index % 80) * 1.25),
    ]),
    entity("product_suppliers", "product_supplier_id", "catalog bridge: products ↔ suppliers", [
      numberColumn("product_supplier_id", (index) => index + 1),
      numberColumn("product_id", (index, _rows, scale) => foreign("products", index, scale)),
      numberColumn("supplier_id", (index, _rows, scale) => foreign("suppliers", index * 7, scale)),
      numberColumn("supplier_cost", (index) => 4 + (index % 90) * 1.1),
    ]),
    entity("price_lists", "price_list_id", "price lists many → 1 currencies/channels", [
      numberColumn("price_list_id", (index) => index + 1),
      numberColumn("currency_id", (index, _rows, scale) => foreign("currencies", index, scale)),
      numberColumn("channel_id", (index, _rows, scale) => foreign("sales_channels", index, scale)),
      stringColumn("name", (index) => `Price list ${String(index + 1)}`, 20),
      booleanColumn("active", (index) => index % 13 !== 0),
    ]),
    entity("product_prices", "product_price_id", "prices many → 1 products/price lists", [
      numberColumn("product_price_id", (index) => index + 1),
      numberColumn("product_id", (index, _rows, scale) => foreign("products", index, scale)),
      numberColumn("price_list_id", (index, _rows, scale) => foreign("price_lists", index, scale)),
      numberColumn("amount", (index) => 10 + (index % 240) * 1.35),
      datetimeColumn("effective_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
    ]),
    entity("purchase_orders", "purchase_order_id", "purchase orders → suppliers/warehouses", [
      numberColumn("purchase_order_id", (index) => index + 1),
      numberColumn("supplier_id", (index, _rows, scale) => foreign("suppliers", index, scale)),
      numberColumn("warehouse_id", (index, _rows, scale) => foreign("warehouses", index, scale)),
      numberColumn("currency_id", (index, _rows, scale) => foreign("currencies", index, scale)),
      stringColumn(
        "status",
        (index) => ["draft", "submitted", "received", "closed"][index % 4] ?? "draft",
        12,
      ),
      datetimeColumn("ordered_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
    ]),
    entity(
      "purchase_order_items",
      "purchase_order_item_id",
      "purchase lines → purchase orders/products",
      [
        numberColumn("purchase_order_item_id", (index) => index + 1),
        numberColumn("purchase_order_id", (index, _rows, scale) =>
          foreign("purchase_orders", Math.floor(index / 5), scale),
        ),
        numberColumn("product_id", (index, _rows, scale) => foreign("products", index, scale)),
        numberColumn("quantity", (index) => 10 + (index % 90)),
        numberColumn("unit_cost", (index) => 4 + (index % 120) * 0.95),
      ],
    ),
    entity("receipts", "receipt_id", "receipts → purchase orders/warehouses", [
      numberColumn("receipt_id", (index) => index + 1),
      numberColumn("purchase_order_id", (index, _rows, scale) =>
        foreign("purchase_orders", index, scale),
      ),
      numberColumn("warehouse_id", (index, _rows, scale) => foreign("warehouses", index, scale)),
      datetimeColumn("received_at", (index) => epoch(2025, ((index % 365) + 3) * 86_400_000)),
      stringColumn("status", (index) => (index % 9 === 0 ? "partial" : "received"), 10),
    ]),
    entity("receipt_items", "receipt_item_id", "receipt lines → receipts/purchase lines", [
      numberColumn("receipt_item_id", (index) => index + 1),
      numberColumn("receipt_id", (index, _rows, scale) =>
        foreign("receipts", Math.floor(index / 3), scale),
      ),
      numberColumn("purchase_order_item_id", (index, _rows, scale) =>
        foreign("purchase_order_items", index, scale),
      ),
      numberColumn("quantity_received", (index) => 1 + (index % 40)),
      booleanColumn("damaged", (index) => index % 97 === 0),
    ]),
    entity("customers", "customer_id", "customers many → 1 regions; 1 → many orders", [
      numberColumn("customer_id", (index) => index + 1),
      numberColumn("region_id", (index, _rows, scale) => foreign("regions", index, scale)),
      stringColumn("segment", (index) => segments[index % segments.length] ?? "starter", 12),
      booleanColumn("active", (index) => index % 13 !== 0),
      datetimeColumn(
        "joined_at",
        (index) => new Date(Date.UTC(2024, index % 12, (index % 27) + 1)),
      ),
    ]),
    entity("customer_addresses", "address_id", "addresses many → 1 customers/regions", [
      numberColumn("address_id", (index) => index + 1),
      numberColumn("customer_id", (index, _rows, scale) => foreign("customers", index, scale)),
      numberColumn("region_id", (index, _rows, scale) => foreign("regions", index * 3, scale)),
      numberColumn("jurisdiction_id", (index, _rows, scale) =>
        foreign("tax_jurisdictions", index, scale),
      ),
      stringColumn("kind", (index) => (index % 3 === 0 ? "billing" : "shipping"), 10),
    ]),
    entity("customer_payment_methods", "payment_method_id", "methods many → 1 customers", [
      numberColumn("payment_method_id", (index) => index + 1),
      numberColumn("customer_id", (index, _rows, scale) => foreign("customers", index, scale)),
      stringColumn("method", (index) => ["card", "bank", "wallet"][index % 3] ?? "card", 10),
      booleanColumn("active", (index) => index % 17 !== 0),
    ]),
    entity("loyalty_accounts", "loyalty_account_id", "loyalty accounts → customers/segments", [
      numberColumn("loyalty_account_id", (index) => index + 1),
      numberColumn("customer_id", (index, _rows, scale) => foreign("customers", index, scale)),
      numberColumn("segment_id", (index, _rows, scale) =>
        foreign("customer_segments", index, scale),
      ),
      numberColumn("points_balance", (index) => (index * 37) % 25_000),
      datetimeColumn("enrolled_at", (index) => epoch(2024, (index % 365) * 86_400_000)),
    ]),
    entity("loyalty_transactions", "loyalty_transaction_id", "loyalty ledger → accounts/orders", [
      numberColumn("loyalty_transaction_id", (index) => index + 1),
      numberColumn("loyalty_account_id", (index, _rows, scale) =>
        foreign("loyalty_accounts", index, scale),
      ),
      numberColumn("order_id", (index, _rows, scale) => foreign("orders", index, scale)),
      stringColumn("kind", (index) => (index % 5 === 0 ? "redeem" : "earn"), 8),
      numberColumn("points_delta", (index) => (index % 5 === 0 ? -50 : 20 + (index % 80))),
      datetimeColumn("occurred_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
    ]),
    entity("promotions", "promotion_id", "promotions 1 → many order discounts", [
      numberColumn("promotion_id", (index) => index + 1),
      stringColumn("code", (index) => `PROMO-${String(index + 1)}`, 14),
      numberColumn("discount_pct", (index) => 0.05 + (index % 4) * 0.025),
      booleanColumn("active", (index) => index % 7 !== 0),
    ]),
    entity("carts", "cart_id", "carts → customers/channels/currencies", [
      numberColumn("cart_id", (index) => index + 1),
      numberColumn("customer_id", (index, _rows, scale) => foreign("customers", index, scale)),
      numberColumn("channel_id", (index, _rows, scale) => foreign("sales_channels", index, scale)),
      numberColumn("currency_id", (index, _rows, scale) => foreign("currencies", index, scale)),
      stringColumn("status", (index) => (index % 4 === 0 ? "converted" : "open"), 10),
      datetimeColumn("updated_at", (index) => epoch(2025, (index % 90) * 86_400_000)),
    ]),
    entity("cart_items", "cart_item_id", "cart items → carts/products", [
      numberColumn("cart_item_id", (index) => index + 1),
      numberColumn("cart_id", (index, _rows, scale) =>
        foreign("carts", Math.floor(index / 4), scale),
      ),
      numberColumn("product_id", (index, _rows, scale) => foreign("products", index, scale)),
      numberColumn("quantity", (index) => 1 + (index % 5)),
      numberColumn("unit_price", (index) => 10 + (index % 200) * 1.2),
    ]),
    entity("orders", "order_id", "orders many → 1 customers/addresses/currencies", [
      numberColumn("order_id", (index) => index + 1),
      numberColumn("customer_id", (index, _rows, scale) => foreign("customers", index, scale)),
      numberColumn("shipping_address_id", (index, _rows, scale) =>
        foreign("customer_addresses", index, scale),
      ),
      numberColumn("currency_id", (index, _rows, scale) => foreign("currencies", index, scale)),
      datetimeColumn("placed_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
      stringColumn(
        "status",
        (index) => ["created", "paid", "shipped", "returned"][index % 4] ?? "created",
        12,
      ),
      numberColumn("total", (index) => Math.round((25 + (index % 400) * 1.75) * 100) / 100),
    ]),
    entity("order_items", "item_id", "items many → 1 orders/products", [
      numberColumn("item_id", (index) => index + 1),
      numberColumn("order_id", (index, _rows, scale) =>
        foreign("orders", Math.floor(index / 3), scale),
      ),
      numberColumn("product_id", (index, _rows, scale) => foreign("products", index, scale)),
      numberColumn("quantity", (index) => (index % 4) + 1),
      numberColumn("unit_price", (index) => 12 + (index % 180) * 1.4),
      numberColumn("discount_pct", (index) => [0, 0.05, 0.1, 0.15][index % 4] ?? 0),
    ]),
    entity("order_events", "order_event_id", "high-volume order event ledger → orders/employees", [
      numberColumn("order_event_id", (index) => index + 1),
      numberColumn("order_id", (index, _rows, scale) =>
        foreign("orders", Math.floor(index / 25), scale),
      ),
      numberColumn("employee_id", (index, _rows, scale) => foreign("employees", index, scale)),
      stringColumn(
        "event_type",
        (index) =>
          ["created", "priced", "authorized", "allocated", "packed", "shipped", "delivered"][
            index % 7
          ] ?? "created",
        14,
      ),
      stringColumn("source", (index) => (index % 3 === 0 ? "system" : "user"), 8),
      datetimeColumn("occurred_at", (index) => epoch(2025, (index % 31_536_000) * 1_000)),
    ]),
    entity("order_discounts", "order_discount_id", "discounts many → 1 orders/promotions", [
      numberColumn("order_discount_id", (index) => index + 1),
      numberColumn("order_id", (index, _rows, scale) => foreign("orders", index * 4, scale)),
      numberColumn("promotion_id", (index, _rows, scale) => foreign("promotions", index, scale)),
      numberColumn("amount", (index) => 2 + (index % 25) * 0.75),
    ]),
    entity("order_taxes", "order_tax_id", "tax lines many → 1 orders/items/tax rates", [
      numberColumn("order_tax_id", (index) => index + 1),
      numberColumn("order_id", (index, _rows, scale) =>
        foreign("orders", Math.floor(index / 3), scale),
      ),
      numberColumn("item_id", (index, _rows, scale) => foreign("order_items", index, scale)),
      numberColumn("tax_rate_id", (index, _rows, scale) => foreign("tax_rates", index, scale)),
      numberColumn("tax_amount", (index) => 0.5 + (index % 45) * 0.19),
    ]),
    entity("payments", "payment_id", "payments many → 1 orders/payment methods", [
      numberColumn("payment_id", (index) => index + 1),
      numberColumn("order_id", (index, _rows, scale) => foreign("orders", index, scale)),
      numberColumn("payment_method_id", (index, _rows, scale) =>
        foreign("customer_payment_methods", index, scale),
      ),
      numberColumn("currency_id", (index, _rows, scale) => foreign("currencies", index, scale)),
      stringColumn("method", (index) => ["card", "bank", "wallet"][index % 3] ?? "card", 10),
      stringColumn("status", (index) => (index % 7 === 0 ? "failed" : "captured"), 10),
      numberColumn("captured_amount", (index) =>
        index % 7 === 0 ? 0 : Math.round((25 + (index % 400) * 1.75) * 100) / 100,
      ),
    ]),
    entity("payment_transactions", "transaction_id", "transactions many → 1 payments", [
      numberColumn("transaction_id", (index) => index + 1),
      numberColumn("payment_id", (index, _rows, scale) =>
        foreign("payments", Math.floor(index / 2), scale),
      ),
      stringColumn("kind", (index) => (index % 2 === 0 ? "authorize" : "capture"), 12),
      stringColumn("status", (index) => (index % 19 === 0 ? "declined" : "succeeded"), 11),
      numberColumn("amount", (index) => 10 + (index % 300) * 1.25),
      datetimeColumn("processed_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
    ]),
    entity("payment_attempts", "payment_attempt_id", "payment attempts many → 1 payments", [
      numberColumn("payment_attempt_id", (index) => index + 1),
      numberColumn("payment_id", (index, _rows, scale) =>
        foreign("payments", Math.floor(index / 3), scale),
      ),
      stringColumn("processor", (index) => ["stripe", "adyen", "bank"][index % 3] ?? "stripe", 10),
      stringColumn("outcome", (index) => (index % 11 === 0 ? "declined" : "approved"), 10),
      numberColumn("latency_ms", (index) => 40 + (index % 900)),
      datetimeColumn("attempted_at", (index) => epoch(2025, (index % 31_536_000) * 1_000)),
    ]),
    entity("fraud_reviews", "fraud_review_id", "fraud reviews → payments/employees", [
      numberColumn("fraud_review_id", (index) => index + 1),
      numberColumn("payment_id", (index, _rows, scale) => foreign("payments", index * 9, scale)),
      numberColumn("employee_id", (index, _rows, scale) => foreign("employees", index, scale)),
      stringColumn("decision", (index) => (index % 8 === 0 ? "reject" : "approve"), 10),
      numberColumn("risk_score", (index) => (index * 17) % 100),
      datetimeColumn("reviewed_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
    ]),
    entity("shipments", "shipment_id", "shipments many → 1 orders/warehouses", [
      numberColumn("shipment_id", (index) => index + 1),
      numberColumn("order_id", (index, _rows, scale) => foreign("orders", index, scale)),
      numberColumn("warehouse_id", (index, _rows, scale) => foreign("warehouses", index, scale)),
      stringColumn(
        "carrier",
        (index) => ["postal", "ground", "express"][index % 3] ?? "postal",
        10,
      ),
      stringColumn(
        "status",
        (index) => ["packed", "shipped", "delivered"][index % 3] ?? "packed",
        11,
      ),
      datetimeColumn("shipped_at", (index) => epoch(2025, ((index % 365) + 2) * 86_400_000)),
    ]),
    entity("shipment_items", "shipment_item_id", "shipment items bridge shipments ↔ order items", [
      numberColumn("shipment_item_id", (index) => index + 1),
      numberColumn("shipment_id", (index, _rows, scale) =>
        foreign("shipments", Math.floor(index / 3), scale),
      ),
      numberColumn("item_id", (index, _rows, scale) => foreign("order_items", index, scale)),
      numberColumn("quantity", (index) => (index % 4) + 1),
    ]),
    entity("shipment_events", "shipment_event_id", "shipment event ledger → shipments", [
      numberColumn("shipment_event_id", (index) => index + 1),
      numberColumn("shipment_id", (index, _rows, scale) =>
        foreign("shipments", Math.floor(index / 4), scale),
      ),
      stringColumn(
        "event_type",
        (index) => ["labelled", "picked_up", "in_transit", "delivered"][index % 4] ?? "labelled",
        12,
      ),
      stringColumn("location", (index) => `Hub ${String((index % 40) + 1)}`, 12),
      datetimeColumn("occurred_at", (index) => epoch(2025, (index % 31_536_000) * 1_000)),
    ]),
    entity("delivery_attempts", "delivery_attempt_id", "delivery attempts many → 1 shipments", [
      numberColumn("delivery_attempt_id", (index) => index + 1),
      numberColumn("shipment_id", (index, _rows, scale) => foreign("shipments", index, scale)),
      numberColumn("attempt_number", (index) => 1 + (index % 3)),
      stringColumn("outcome", (index) => (index % 7 === 0 ? "missed" : "delivered"), 10),
      datetimeColumn("attempted_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
    ]),
    entity("returns", "return_id", "returns many → 1 orders/items", [
      numberColumn("return_id", (index) => index + 1),
      numberColumn("order_id", (index, _rows, scale) => foreign("orders", index * 10, scale)),
      numberColumn("item_id", (index, _rows, scale) => foreign("order_items", index * 30, scale)),
      stringColumn(
        "reason",
        (index) => ["damaged", "wrong_item", "changed_mind"][index % 3] ?? "damaged",
        14,
      ),
      datetimeColumn("returned_at", (index) => epoch(2025, ((index % 300) + 30) * 86_400_000)),
    ]),
    entity("return_items", "return_item_id", "return items many → 1 returns/order items", [
      numberColumn("return_item_id", (index) => index + 1),
      numberColumn("return_id", (index, _rows, scale) => foreign("returns", index, scale)),
      numberColumn("item_id", (index, _rows, scale) => foreign("order_items", index * 30, scale)),
      numberColumn("quantity", (index) => (index % 2) + 1),
    ]),
    entity("refunds", "refund_id", "refunds many → 1 returns/payments", [
      numberColumn("refund_id", (index) => index + 1),
      numberColumn("return_id", (index, _rows, scale) => foreign("returns", index, scale)),
      numberColumn("payment_id", (index, _rows, scale) => foreign("payments", index * 10, scale)),
      numberColumn("amount", (index) => 8 + (index % 75) * 1.2),
      stringColumn("status", (index) => (index % 9 === 0 ? "pending" : "settled"), 10),
    ]),
    entity("support_tickets", "ticket_id", "support tickets → customers/orders", [
      numberColumn("ticket_id", (index) => index + 1),
      numberColumn("customer_id", (index, _rows, scale) => foreign("customers", index, scale)),
      numberColumn("order_id", (index, _rows, scale) => foreign("orders", index * 3, scale)),
      stringColumn("priority", (index) => ["low", "normal", "high"][index % 3] ?? "normal", 8),
      stringColumn("status", (index) => (index % 5 === 0 ? "closed" : "open"), 8),
      datetimeColumn("opened_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
    ]),
    entity("support_messages", "message_id", "support messages → tickets/employees", [
      numberColumn("message_id", (index) => index + 1),
      numberColumn("ticket_id", (index, _rows, scale) =>
        foreign("support_tickets", Math.floor(index / 5), scale),
      ),
      numberColumn("employee_id", (index, _rows, scale) => foreign("employees", index, scale)),
      stringColumn("author_type", (index) => (index % 3 === 0 ? "customer" : "agent"), 10),
      stringColumn("body", (index) => `Deterministic support message ${String(index + 1)}`, 48),
      datetimeColumn("sent_at", (index) => epoch(2025, (index % 31_536_000) * 1_000)),
    ]),
    entity(
      "inventory_movements",
      "movement_id",
      "inventory ledger → products/warehouses/suppliers",
      [
        numberColumn("movement_id", (index) => index + 1),
        numberColumn("product_id", (index, _rows, scale) => foreign("products", index, scale)),
        numberColumn("warehouse_id", (index, _rows, scale) => foreign("warehouses", index, scale)),
        numberColumn("supplier_id", (index, _rows, scale) => foreign("suppliers", index, scale)),
        stringColumn(
          "kind",
          (index) => ["receipt", "sale", "return", "adjustment"][index % 4] ?? "receipt",
          12,
        ),
        numberColumn("quantity_delta", (index) => [10, -3, 1, -1][index % 4] ?? 1),
        datetimeColumn("occurred_at", (index) => epoch(2025, (index % 365) * 86_400_000)),
      ],
    ),
    entity("inventory_snapshots", "snapshot_id", "inventory snapshots → products/warehouses", [
      numberColumn("snapshot_id", (index) => index + 1),
      numberColumn("product_id", (index, _rows, scale) => foreign("products", index, scale)),
      numberColumn("warehouse_id", (index, _rows, scale) => foreign("warehouses", index, scale)),
      numberColumn("quantity_on_hand", (index) => 20 + (index % 500)),
      numberColumn("quantity_reserved", (index) => index % 40),
      datetimeColumn("captured_at", (index) => epoch(2025, (index % 8_760) * 3_600_000)),
    ]),
    entity("audit_events", "audit_event_id", "high-volume audit ledger → employees/orders", [
      numberColumn("audit_event_id", (index) => index + 1),
      numberColumn("employee_id", (index, _rows, scale) => foreign("employees", index, scale)),
      numberColumn("order_id", (index, _rows, scale) => foreign("orders", index, scale)),
      stringColumn(
        "action",
        (index) => ["read", "create", "update", "approve", "export"][index % 5] ?? "read",
        10,
      ),
      stringColumn("entity_type", (index) => (index % 2 === 0 ? "order" : "payment"), 10),
      datetimeColumn("occurred_at", (index) => epoch(2025, (index % 31_536_000) * 1_000)),
    ]),
  ];
}

export function getScenario(id: ScenarioId): ScenarioDefinition {
  const scenario = scenarios.find((candidate) => candidate.id === id);
  if (scenario === undefined) throw new Error(`Unknown benchmark scenario: ${id}`);
  return scenario;
}

function numberColumn(
  name: string,
  valueAt: (index: number, rows: number, scale: number) => number | null,
): ColumnDefinition {
  return { name, type: "number", estimatedBytesPerRow: 8.125, valueAt };
}

function stringColumn(
  name: string,
  valueAt: (index: number, rows: number, scale: number) => string | null,
  estimatedBytesPerRow: number,
): ColumnDefinition {
  return { name, type: "string", estimatedBytesPerRow, valueAt };
}

function booleanColumn(
  name: string,
  valueAt: (index: number, rows: number, scale: number) => boolean | null,
): ColumnDefinition {
  return { name, type: "boolean", estimatedBytesPerRow: 0.25, valueAt };
}

function datetimeColumn(
  name: string,
  valueAt: (index: number, rows: number, scale: number) => Date | null,
): ColumnDefinition {
  return { name, type: "datetime", estimatedBytesPerRow: 8.125, valueAt };
}

function epoch(year: number, offset: number): Date {
  return new Date(Date.UTC(year, 0, 1) + offset);
}

function deterministic(index: number): number {
  let value = (index + 1) | 0;
  value = Math.imul(value ^ (value >>> 16), 0x45d9f3b);
  value = Math.imul(value ^ (value >>> 16), 0x45d9f3b);
  return ((value ^ (value >>> 16)) >>> 0) / 0x1_0000_0000;
}
