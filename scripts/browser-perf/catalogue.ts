/**
 * What the browser performance lane measures: its datasets, its query shapes, and its cells.
 *
 * Shared by the Node driver (`scripts/browser-perf.mts`) and the in-page harness
 * (`scripts/browser-perf/page`), so both sides agree on every id, row count, and SQL text.
 *
 * Bump GENERATOR_VERSION whenever a generator, a schema, or a load path changes what an engine
 * holds after a load. Cached databases are keyed by it, so a bump forces a reload instead of
 * measuring a stale copy.
 */
export const GENERATOR_VERSION = 1;

export type EngineId = "minnow" | "duckdb" | "sqlite";
export const ENGINE_IDS: readonly EngineId[] = ["minnow", "duckdb", "sqlite"];

export interface WideDataset {
  readonly id: string;
  readonly kind: "W";
  readonly columns: 200 | 500;
  readonly rows: number;
  /** Opt-in only: `--scale=large`. */
  readonly large: boolean;
}

export interface StarDataset {
  readonly id: string;
  readonly kind: "S";
  /** `apps/site/bench` commerce scale: 333 is ~1M line items, 1,667 is ~5M. */
  readonly scale: number;
  readonly large: boolean;
}

export type AnalyticsDataset = WideDataset | StarDataset;

export const ANALYTICS_DATASETS: readonly AnalyticsDataset[] = [
  { id: "W1M", kind: "W", columns: 200, rows: 1_000_000, large: false },
  { id: "W500", kind: "W", columns: 500, rows: 250_000, large: false },
  { id: "S1", kind: "S", scale: 333, large: false },
  { id: "W5M", kind: "W", columns: 200, rows: 5_000_000, large: true },
  { id: "S5", kind: "S", scale: 1_667, large: true },
  // Tiny copies of the same shapes, for checking the lane's mechanics in seconds. Never in a
  // default or large run; ask for them by name with `--scale=W50K,S01`.
  { id: "W50K", kind: "W", columns: 200, rows: 50_000, large: false },
  { id: "S01", kind: "S", scale: 10, large: false },
];
export const SMOKE_DATASETS: readonly string[] = ["W50K", "S01"];

/** SQLite is optional on the analytics suite, and only where it finishes in reasonable time. */
export const SQLITE_ANALYTICS_DATASETS: readonly string[] = ["W1M", "S1", "W50K", "S01"];

export function analyticsDataset(id: string): AnalyticsDataset {
  const dataset = ANALYTICS_DATASETS.find((candidate) => candidate.id === id);
  if (dataset === undefined) {
    throw new Error(
      `Unknown dataset ${id}; expected one of ${ANALYTICS_DATASETS.map((d) => d.id).join(", ")}`,
    );
  }
  return dataset;
}

/** Rows in the dataset's principal table, for `${R90}`-style substitutions and reopen checks. */
export function principalRows(dataset: AnalyticsDataset): number {
  return dataset.kind === "W" ? dataset.rows : 0;
}

export interface AnalyticsQuery {
  readonly id: string;
  readonly family: "scan" | "agg" | "join" | "win" | "sort" | "spill" | "star";
  readonly sql: string;
  /** SQLite dialect override. */
  readonly sqlite?: string;
  /** The ORDER BY is total, so results are compared order-sensitively. */
  readonly ordered?: boolean;
  /** `export` streams every row through a cursor and checks (count, sum of id, non-null x00). */
  readonly kind?: "export";
  /** `count`: compare only the result's shape (row count × column count). */
  readonly check?: "count";
}

const C30 =
  "id, ts, day, d00, d01, d02, d03, d04, d05, h00, h01, h05, i00, i01, i02, i03, i04, f00, f01, f02, f03, n00, n01, n02, b00, b01, x00, x01, x02, x03";
const G20 =
  "COUNT(*) AS n, SUM(i10) AS s_i10, SUM(i11) AS s_i11, SUM(i12) AS s_i12, SUM(i13) AS s_i13, SUM(i15) AS s_i15, AVG(f10) AS a_f10, AVG(f11) AS a_f11, AVG(f12) AS a_f12, AVG(f13) AS a_f13, AVG(f14) AS a_f14, MIN(f15) AS mn_f15, MAX(f16) AS mx_f16, MIN(i16) AS mn_i16, MAX(i17) AS mx_i17, SUM(f17) AS s_f17, SUM(f18) AS s_f18, AVG(i18) AS a_i18, COUNT(x00) AS c_x00, MIN(d10) AS mn_d10, MAX(ts) AS mx_ts";

/** The wide-table shapes measured in measurements.md, in the same order and with the same SQL. */
export const WIDE_QUERIES: readonly AnalyticsQuery[] = [
  {
    id: "scan_first_page",
    family: "scan",
    check: "count",
    sql: "SELECT * FROM events_wide LIMIT 100",
  },
  {
    id: "scan_first_page_ordered",
    family: "scan",
    ordered: true,
    sql: "SELECT * FROM events_wide ORDER BY id LIMIT 100",
  },
  {
    id: "scan_proj3_agg",
    family: "scan",
    sql: "SELECT COUNT(*) AS n, SUM(f03) AS s, AVG(i05) AS a FROM events_wide WHERE i07 < 500000",
  },
  {
    id: "scan_selective_30col",
    family: "scan",
    ordered: true,
    sql: `SELECT ${C30} FROM events_wide WHERE i09 = 7 ORDER BY id`,
  },
  { id: "scan_export_all", family: "scan", kind: "export", sql: "SELECT * FROM events_wide" },
  {
    id: "agg_3dims_20aggs",
    family: "agg",
    ordered: true,
    sql: `SELECT d00, d01, d02, ${G20} FROM events_wide GROUP BY d00, d01, d02 ORDER BY d00, d01, d02`,
  },
  {
    id: "agg_highcard_text",
    family: "agg",
    ordered: true,
    sql: "SELECT h00, COUNT(*) AS n, SUM(f00) AS s FROM events_wide GROUP BY h00 ORDER BY s DESC, h00 LIMIT 100",
  },
  {
    id: "agg_highcard_int",
    family: "agg",
    ordered: true,
    sql: "SELECT i02, COUNT(*) AS n, SUM(f01) AS s FROM events_wide GROUP BY i02 ORDER BY n DESC, i02 LIMIT 100",
  },
  {
    id: "agg_count_distinct",
    family: "agg",
    sql: "SELECT COUNT(DISTINCT h01) AS u_email, COUNT(DISTINCT h05) AS u_acct FROM events_wide",
  },
  {
    id: "agg_count_distinct_grouped",
    family: "agg",
    ordered: true,
    sql: "SELECT d01, COUNT(DISTINCT h05) AS u FROM events_wide GROUP BY d01 ORDER BY d01",
  },
  {
    id: "agg_numeric_sum",
    family: "agg",
    sql: "SELECT SUM(n00) AS s0, SUM(n01) AS s1, SUM(n02) AS s2, SUM(n03) AS s3, AVG(n04) AS a4 FROM events_wide",
  },
  {
    id: "agg_numeric_grouped",
    family: "agg",
    ordered: true,
    sql: "SELECT d02, SUM(n05) AS s5, SUM(n06) AS s6, MIN(n07) AS m7, MAX(n08) AS x8 FROM events_wide GROUP BY d02 ORDER BY d02",
  },
  {
    id: "join_star_3dims",
    family: "join",
    ordered: true,
    sql: "SELECT a.tier AS tier_a, b.tier AS tier_b, c.label AS label_c, COUNT(*) AS n, SUM(e.f00) AS s FROM events_wide e JOIN dim_d03 a ON a.k = e.d03 JOIN dim_d04 b ON b.k = e.d04 JOIN dim_d07 c ON c.k = e.d07 WHERE a.active AND b.weight > 1 AND c.tier = 't1' GROUP BY a.tier, b.tier, c.label ORDER BY s DESC, tier_a, tier_b, label_c LIMIT 50",
  },
  {
    id: "join_fact_fact_int",
    family: "join",
    sql: "SELECT COUNT(*) AS n, SUM(b.f01) AS s FROM events_wide a JOIN events_wide b ON b.id = a.i20 WHERE a.i05 < 5",
  },
  {
    id: "join_fact_fact_text",
    family: "join",
    sql: "SELECT COUNT(*) AS n, SUM(b.f02) AS s FROM events_wide a JOIN events_wide b ON b.h00 = a.h07",
  },
  {
    id: "join_semi_exists",
    family: "join",
    sql: "SELECT COUNT(*) AS n FROM events_wide a WHERE EXISTS (SELECT 1 FROM events_wide b WHERE b.id = a.i20 AND b.b00)",
  },
  {
    id: "join_anti_not_exists",
    family: "join",
    sql: "SELECT COUNT(*) AS n FROM events_wide a WHERE NOT EXISTS (SELECT 1 FROM events_wide b WHERE b.id = a.i20 AND b.i05 = 0)",
  },
  {
    id: "win_topk_row_number",
    family: "win",
    ordered: true,
    sql: "SELECT d04, id, f05, rn FROM (SELECT d04, id, f05, ROW_NUMBER() OVER (PARTITION BY d04 ORDER BY f05 DESC, id) AS rn FROM events_wide) t WHERE rn <= 3 ORDER BY d04, rn",
  },
  {
    id: "win_rank",
    family: "win",
    ordered: true,
    sql: "SELECT d07, id, i06, rk FROM (SELECT d07, id, i06, RANK() OVER (PARTITION BY d07 ORDER BY i06 DESC) AS rk FROM events_wide) t WHERE rk <= 2 ORDER BY d07, rk, id",
  },
  {
    id: "win_running_sum",
    family: "win",
    sql: "SELECT COUNT(*) AS n, SUM(rs) AS total, MAX(rs) AS mx FROM (SELECT SUM(f06) OVER (PARTITION BY d00 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM events_wide) t",
  },
  {
    id: "win_lag_lead",
    family: "win",
    sql: "SELECT COUNT(*) AS n, SUM(nx - pv) AS d FROM (SELECT LAG(i08) OVER (PARTITION BY d05 ORDER BY id) AS pv, LEAD(i08) OVER (PARTITION BY d05 ORDER BY id) AS nx FROM events_wide) t",
  },
  {
    id: "win_moving_avg",
    family: "win",
    sql: "SELECT SUM(ma) AS s, MAX(ma) AS mx FROM (SELECT AVG(f07) OVER (PARTITION BY d03 ORDER BY id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma FROM events_wide) t",
  },
  {
    id: "win_median_pct",
    family: "win",
    ordered: true,
    sql: "SELECT d02, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f08) AS med, PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY i08) AS p90 FROM events_wide GROUP BY d02 ORDER BY d02",
  },
  {
    id: "sort_full_numeric",
    family: "sort",
    ordered: true,
    sql: "SELECT id, f09, d06 FROM events_wide ORDER BY f09, id",
  },
  {
    id: "sort_full_text",
    family: "sort",
    ordered: true,
    sql: "SELECT id, h01 FROM events_wide ORDER BY h01, id",
  },
  {
    id: "sort_deep_offset",
    family: "sort",
    ordered: true,
    sql: "SELECT id, d00, f09 FROM events_wide ORDER BY f09, id LIMIT 100 OFFSET ${R90}",
  },
  {
    id: "sort_deep_offset_pk",
    family: "sort",
    ordered: true,
    sql: "SELECT id, d00, f09 FROM events_wide ORDER BY id LIMIT 100 OFFSET ${R90}",
  },
  {
    id: "sort_topn",
    family: "sort",
    ordered: true,
    sql: "SELECT id, f09, h00 FROM events_wide ORDER BY f09 DESC, id LIMIT 100",
  },
  {
    id: "sort_topn_wide",
    family: "sort",
    ordered: true,
    sql: "SELECT * FROM events_wide ORDER BY f09 DESC, id LIMIT 100",
  },
  // Spill shapes run only under an explicit memory cap (see SPILL_BUDGETS).
  {
    id: "spill_group_1m",
    family: "spill",
    ordered: true,
    sql: "SELECT h00, COUNT(*) AS n, SUM(f00) AS s FROM events_wide GROUP BY h00 ORDER BY s DESC, h00 LIMIT 100",
  },
  {
    id: "spill_sort_wide_text",
    family: "spill",
    ordered: true,
    sql: "SELECT id, d00, h01, h02, f09 FROM events_wide ORDER BY h02, id LIMIT 10 OFFSET ${R99}",
  },
  {
    id: "spill_hash_join_text",
    family: "spill",
    sql: "SELECT COUNT(*) AS n, SUM(b.f02) AS s FROM events_wide a JOIN events_wide b ON b.h00 = a.h07",
  },
];

/** The star-schema shapes: the site bench's q2–q15 plus four join shapes. */
export const STAR_QUERIES: readonly AnalyticsQuery[] = [
  {
    id: "S_q2",
    family: "star",
    ordered: true,
    sql: "SELECT order_id, customer_id, total FROM orders WHERE status = 'paid' AND placed_at >= DATE '2025-04-01' AND placed_at < DATE '2025-07-01' ORDER BY total DESC, order_id LIMIT 25",
    sqlite:
      "SELECT order_id, customer_id, total FROM orders WHERE status = 'paid' AND placed_at >= '2025-04-01' AND placed_at < '2025-07-01' ORDER BY total DESC, order_id LIMIT 25",
  },
  {
    id: "S_q3",
    family: "star",
    ordered: true,
    sql: "SELECT status, COUNT(*) AS orders, SUM(total) AS revenue, AVG(total) AS avg_order FROM orders GROUP BY status ORDER BY revenue DESC",
  },
  {
    id: "S_q4",
    family: "star",
    ordered: true,
    sql: "SELECT c.customer_id, c.segment, SUM(p.captured_amount) AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id JOIN payments p ON p.order_id = o.order_id WHERE p.status = 'captured' GROUP BY c.customer_id, c.segment ORDER BY revenue DESC, c.customer_id LIMIT 20",
  },
  {
    id: "S_q5",
    family: "star",
    ordered: true,
    sql: "SELECT p.category, SUM(i.quantity * i.unit_price * (1 - i.discount_pct)) AS net_revenue FROM order_items i JOIN products p ON p.product_id = i.product_id GROUP BY p.category ORDER BY net_revenue DESC",
  },
  {
    id: "S_q6",
    family: "star",
    sql: "WITH returned_orders AS (SELECT DISTINCT i.order_id AS order_id FROM order_items i JOIN returns r ON r.item_id = i.item_id) SELECT o.customer_id, COUNT(*) AS orders FROM orders o WHERE o.order_id NOT IN (SELECT order_id FROM returned_orders) GROUP BY o.customer_id HAVING COUNT(*) >= 2",
  },
  {
    id: "S_q7",
    family: "star",
    ordered: true,
    sql: "WITH product_revenue AS (SELECT p.category AS category, i.product_id AS product_id, SUM(i.quantity * i.unit_price) AS revenue FROM order_items i JOIN products p ON p.product_id = i.product_id GROUP BY p.category, i.product_id), ranked AS (SELECT category, product_id, revenue, DENSE_RANK() OVER (PARTITION BY category ORDER BY ROUND(revenue, 4) DESC, product_id) AS rank FROM product_revenue) SELECT category, product_id, revenue, rank FROM ranked WHERE rank <= 3 ORDER BY category, rank",
  },
  {
    id: "S_q8",
    family: "star",
    ordered: true,
    sql: "WITH cohorts AS (SELECT customer_id AS customer_id, DATE_TRUNC('month', joined_at) AS cohort_month FROM customers), returned_orders AS (SELECT DISTINCT i.order_id AS order_id FROM order_items i JOIN returns r ON r.item_id = i.item_id), captured AS (SELECT k.cohort_month AS cohort_month, o.customer_id AS customer_id, SUM(p.captured_amount) AS revenue, COUNT(*) AS orders FROM payments p JOIN orders o ON o.order_id = p.order_id JOIN cohorts k ON k.customer_id = o.customer_id WHERE p.status = 'captured' GROUP BY k.cohort_month, o.customer_id) SELECT cohort_month, COUNT(*) AS customers, SUM(revenue) AS revenue FROM captured GROUP BY cohort_month ORDER BY cohort_month",
    sqlite:
      "WITH cohorts AS (SELECT customer_id AS customer_id, strftime('%Y-%m-01T00:00:00.000Z', joined_at) AS cohort_month FROM customers), returned_orders AS (SELECT DISTINCT i.order_id AS order_id FROM order_items i JOIN returns r ON r.item_id = i.item_id), captured AS (SELECT k.cohort_month AS cohort_month, o.customer_id AS customer_id, SUM(p.captured_amount) AS revenue, COUNT(*) AS orders FROM payments p JOIN orders o ON o.order_id = p.order_id JOIN cohorts k ON k.customer_id = o.customer_id WHERE p.status = 'captured' GROUP BY k.cohort_month, o.customer_id) SELECT cohort_month, COUNT(*) AS customers, SUM(revenue) AS revenue FROM captured GROUP BY cohort_month ORDER BY cohort_month",
  },
  {
    id: "S_q9",
    family: "star",
    ordered: true,
    sql: "WITH per_customer AS (SELECT r.name AS region, c.segment AS segment, c.customer_id AS customer_id, SUM(p.captured_amount) AS revenue FROM regions r JOIN customers c ON c.region_id = r.region_id JOIN orders o ON o.customer_id = c.customer_id JOIN payments p ON p.order_id = o.order_id WHERE p.status = 'captured' GROUP BY r.name, c.segment, c.customer_id) SELECT region, segment, COUNT(*) AS customers, SUM(revenue) AS revenue FROM per_customer GROUP BY region, segment ORDER BY region, segment",
  },
  {
    id: "S_q10",
    family: "star",
    ordered: true,
    sql: "SELECT p.category, COUNT(*) AS items, COUNT(r.item_id) AS returns, COUNT(r.item_id) * 1.0 / COUNT(*) AS return_rate FROM products p JOIN order_items i ON i.product_id = p.product_id LEFT JOIN (SELECT DISTINCT item_id AS item_id FROM returns) r ON r.item_id = i.item_id GROUP BY p.category ORDER BY p.category",
  },
  {
    id: "S_q11",
    family: "star",
    ordered: true,
    sql: "SELECT j.name, SUM(t.tax_amount) AS tax_collected FROM order_taxes t JOIN tax_rates r ON r.tax_rate_id = t.tax_rate_id JOIN tax_jurisdictions j ON j.jurisdiction_id = r.jurisdiction_id GROUP BY j.name ORDER BY tax_collected DESC, j.name",
  },
  {
    id: "S_q14",
    family: "star",
    ordered: true,
    sql: "SELECT kind, status, COUNT(*) AS transactions, SUM(amount) AS amount FROM payment_transactions GROUP BY kind, status ORDER BY kind, status",
  },
  {
    id: "S_q15",
    family: "star",
    ordered: true,
    sql: "WITH discounts AS (SELECT order_id AS order_id, SUM(amount) AS amount FROM order_discounts GROUP BY order_id), taxes AS (SELECT order_id AS order_id, SUM(tax_amount) AS amount FROM order_taxes GROUP BY order_id) SELECT o.status, SUM(COALESCE(d.amount, 0)) AS discounts, SUM(COALESCE(t.amount, 0)) AS taxes FROM orders o LEFT JOIN discounts d ON d.order_id = o.order_id LEFT JOIN taxes t ON t.order_id = o.order_id GROUP BY o.status ORDER BY o.status",
  },
  {
    id: "S_fact_fact",
    family: "star",
    ordered: true,
    sql: "SELECT o.status, COUNT(*) AS n, SUM(i.quantity * i.unit_price) AS revenue FROM orders o JOIN order_items i ON i.order_id = o.order_id GROUP BY o.status ORDER BY o.status",
  },
  {
    id: "S_semi",
    family: "star",
    sql: "SELECT COUNT(*) AS n FROM orders o WHERE EXISTS (SELECT 1 FROM order_items i JOIN returns r ON r.item_id = i.item_id WHERE i.order_id = o.order_id)",
  },
  {
    id: "S_anti",
    family: "star",
    sql: "SELECT COUNT(*) AS n FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.order_id AND p.status = 'captured')",
  },
  {
    id: "S_m2m",
    family: "star",
    ordered: true,
    sql: "SELECT ps.supplier_id, COUNT(*) AS n, SUM(i.quantity) AS units FROM order_items i JOIN product_suppliers ps ON ps.product_id = i.product_id GROUP BY ps.supplier_id ORDER BY ps.supplier_id",
  },
];

/** The star-schema tables the queries above read. q12, q13 and q16's tables are not loaded. */
export const STAR_TABLES: readonly string[] = [
  "regions",
  "customers",
  "products",
  "product_suppliers",
  "orders",
  "order_items",
  "order_discounts",
  "order_taxes",
  "tax_rates",
  "tax_jurisdictions",
  "payments",
  "payment_transactions",
  "returns",
];

/** Memory caps the spill shapes run under: Minnow's execution budget, DuckDB's memory_limit. */
export const SPILL_BUDGETS: ReadonlyArray<{ readonly label: string; readonly bytes: number }> = [
  { label: "64MiB", bytes: 64 * 1024 * 1024 },
  { label: "256MiB", bytes: 256 * 1024 * 1024 },
];
/** The spill shapes measure 1M-row work; on the smaller tables they would not spill. */
export const SPILL_DATASETS: readonly string[] = ["W1M", "W5M", "W50K"];

export function sqlFor(query: AnalyticsQuery, engine: EngineId, rows: number): string {
  const text = (engine === "sqlite" ? query.sqlite : undefined) ?? query.sql;
  return text
    .replaceAll("${R90}", String(Math.floor(rows * 0.9)))
    .replaceAll("${R99}", String(Math.floor(rows * 0.99)));
}

/** One analytics cell: one query on one dataset, at default options or under one memory cap. */
export interface AnalyticsCell {
  /** `W1M/scan_proj3_agg`, `W1M/spill_group_1m@64MiB`. */
  readonly id: string;
  readonly dataset: AnalyticsDataset;
  readonly query: AnalyticsQuery;
  readonly budget?: { readonly label: string; readonly bytes: number };
}

export function analyticsCells(dataset: AnalyticsDataset): AnalyticsCell[] {
  const cells: AnalyticsCell[] = [];
  const queries = dataset.kind === "W" ? WIDE_QUERIES : STAR_QUERIES;
  for (const query of queries) {
    if (query.family !== "spill") {
      cells.push({ id: `${dataset.id}/${query.id}`, dataset, query });
      continue;
    }
    if (!SPILL_DATASETS.includes(dataset.id)) continue;
    for (const budget of SPILL_BUDGETS) {
      cells.push({ id: `${dataset.id}/${query.id}@${budget.label}`, dataset, query, budget });
    }
  }
  return cells;
}

/** The point suite's cells. Every one compares Minnow with SQLite-Wasm on the OPFS SAH pool. */
export const POINT_CELLS = [
  "load_keyed_1m_strict",
  "insert_single_strict",
  "insert_concurrent_strict_8",
  "insert_concurrent_strict_32",
  "insert_concurrent_strict_64",
  "insert_prepared_relaxed",
  "pk_read_warm_int",
  "pk_read_warm_int_literal",
  "pk_read_warm_uuid",
  "pk_read_cold_1m",
  "pk_read_cross_tab",
  "pk_read_compacted",
  "pk_read_after_2k_writes",
] as const;
export type PointCellName = (typeof POINT_CELLS)[number];

export const POINT_ROWS = 1_000_000;
export const POINT_UUID_ROWS = 250_000;
