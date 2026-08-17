/**
 * The relational read suite: definitions, hand-written JavaScript
 * baselines, independent oracles, and the cross-engine runner. The dataset is
 * deterministic, so oracles run over tables regenerated in memory rather than rows read
 * back from any engine — the same inputs every engine received.
 */
import type { DatabaseRow } from "@minnowdb/core";
import { generateEntityBatch, getScenario, type EntityDefinition } from "../benchmark";
import { loadDriver, requireMaterialization, type EngineSession } from "../engines/session";
import type {
  EngineId,
  ReferenceEngineMeasurement,
  ReferenceQueryReport,
  ReferenceSuitePayload,
  ReferenceSuiteResult,
  WorkloadKind,
} from "../protocol";
import { canonicalTuples, referenceChecksum, tuplesMatch } from "./canonical";
import { getDataset } from "./registry";
import {
  assertNotCancelled,
  progress,
  measureRepeated,
  validateDatasetSuitePayload,
} from "./support";

export interface ReferenceQueryDefinition {
  id: string;
  name: string;
  complexity: "simple" | "moderate" | "complex";
  /**
   * OLTP reads reach a few rows through a key or an ordered range and are latency-bound. OLAP
   * reads scan, join, or aggregate and are throughput-bound. Note an unanchored LIKE is OLAP:
   * classification follows the work done, not how the SQL reads.
   */
  workload: WorkloadKind;
  /**
   * Submitted to MinnowDatabase verbatim. Queries outside the current SQL surface still
   * carry their complete intended statement and record the engine's refusal, so the
   * report shows the real gap instead of a placeholder.
   */
  sql: string;
  /**
   * Dialect overrides for engines whose SQL differs (sqlite has no DATE literal or
   * DATE_TRUNC). Support is still decided by executing, never by this table.
   */
  engineSql?: Partial<Record<EngineId, string>>;
  /** Output column names in select-list order. The engine result is compared as this tuple. */
  columns: string[];
  /** Maps one JavaScript reference row onto the same tuple shape as `columns`. */
  project: (row: Record<string, unknown>) => unknown[];
  /** Names what the SQL surface is missing when the statement is known not to compile. */
  surfaceGap?: string;
  tables: string[];
  expectedRows: number;
  /** Hand-written JavaScript over materialized rows, kept as a baseline beside the engine. */
  baseline: (context: ReferenceQueryContext) => unknown[];
  /** A second, independent JavaScript implementation used only to verify results. */
  oracle: (tables: ReadonlyMap<string, DatabaseRow[]>) => unknown[];
}

export interface ReferenceQueryContext {
  tables: ReadonlyMap<string, DatabaseRow[]>;
  regionsById: ReadonlyMap<number, DatabaseRow>;
  customersById: ReadonlyMap<number, DatabaseRow>;
  productsById: ReadonlyMap<number, DatabaseRow>;
  ordersById: ReadonlyMap<number, DatabaseRow>;
  returnedItemIds: ReadonlySet<number>;
  returnedOrderIds: ReadonlySet<number>;
}

const SAMPLE_COUNT = 7;

export function validateReferencePayload(value: unknown): ReferenceSuitePayload {
  return validateDatasetSuitePayload(value);
}

/** Materializes one full table as row objects from the deterministic generator. */
export function generateTableRows(entity: EntityDefinition, scale: number): DatabaseRow[] {
  const rowCount = entity.rows(scale);
  const columns = generateEntityBatch(entity, 0, rowCount, rowCount, scale);
  return Array.from({ length: rowCount }, (_, index) =>
    Object.fromEntries(
      entity.columns.map((column) => [column.name, columns[column.name]?.[index] ?? null]),
    ),
  );
}

export async function runReferenceSuite(
  requestId: string,
  payload: ReferenceSuitePayload,
): Promise<ReferenceSuiteResult> {
  const record = await getDataset(payload.datasetId);
  const entities = getScenario("commerce").entities;
  const entityByName = new Map(entities.map((entity) => [entity.name, entity]));
  const orderRows = entityByName.get("orders")?.rows(record.scale) ?? 0;
  const definitions = referenceQueryDefinitions(orderRows);
  const sessions = new Map<EngineId, EngineSession | Error>();
  for (const engine of payload.engines) {
    try {
      requireMaterialization(record, engine);
      const driver = await loadDriver(engine);
      sessions.set(engine, await driver.openSession(record));
    } catch (error) {
      sessions.set(engine, error instanceof Error ? error : new Error(String(error)));
    }
  }
  const totalSteps = definitions.length * (1 + payload.engines.length);
  let completed = 0;
  const queries: ReferenceQueryReport[] = [];
  try {
    for (const definition of definitions) {
      assertNotCancelled(requestId);
      progress(requestId, {
        phase: "oracle",
        completed,
        total: totalSteps,
        message: `Computing oracle · ${definition.name}`,
      });
      const tables = new Map(
        definition.tables.map((name) => {
          const entity = entityByName.get(name);
          if (entity === undefined) throw new Error(`Unknown table: ${name}`);
          return [name, generateTableRows(entity, record.scale)] as const;
        }),
      );
      const oracleResult = definition.oracle(tables);
      const oracleTuples = canonicalTuples(oracleResult, definition.project);
      tables.clear();
      completed += 1;
      const engineMeasurements: ReferenceEngineMeasurement[] = [];
      for (const engine of payload.engines) {
        assertNotCancelled(requestId);
        progress(requestId, {
          phase: "queries",
          completed,
          total: totalSteps,
          message: `${definition.id.toUpperCase()} · ${engine}`,
        });
        const session = sessions.get(engine);
        engineMeasurements.push(
          session === undefined || session instanceof Error
            ? {
                engine,
                supported: false,
                error: session instanceof Error ? session.message : "session unavailable",
                prepareMs: 0,
                medianMs: 0,
                p95Ms: 0,
                resultRows: 0,
                checksum: 0,
                verified: false,
              }
            : await measureOnSession(session, definition, engine, oracleTuples),
        );
        completed += 1;
      }
      queries.push({
        id: definition.id,
        name: definition.name,
        complexity: definition.complexity,
        workload: definition.workload,
        sql: definition.sql,
        tables: definition.tables,
        expectedRows: definition.expectedRows,
        oracleRows: oracleResult.length,
        ...(definition.surfaceGap === undefined ? {} : { surfaceGap: definition.surfaceGap }),
        engines: engineMeasurements,
      });
    }
  } finally {
    for (const session of sessions.values()) {
      if (!(session instanceof Error)) await session.close().catch(() => undefined);
    }
  }
  const totalMsByEngine: Partial<Record<EngineId, number>> = {};
  const supportedByEngine: Partial<Record<EngineId, number>> = {};
  for (const engine of payload.engines) {
    const measurements = queries.flatMap((query) =>
      query.engines.filter((measurement) => measurement.engine === engine),
    );
    totalMsByEngine[engine] = measurements.reduce(
      (total, measurement) => total + measurement.medianMs,
      0,
    );
    supportedByEngine[engine] = measurements.filter((measurement) => measurement.supported).length;
  }
  progress(requestId, {
    phase: "complete",
    completed: totalSteps,
    total: totalSteps,
    message: "Reference suite complete",
  });
  return {
    datasetId: record.id,
    scale: record.scale,
    sampleCount: SAMPLE_COUNT,
    engines: payload.engines,
    queries,
    totalMsByEngine,
    supportedByEngine,
    passed: queries.every((query) =>
      query.engines.every((measurement) => !measurement.supported || measurement.verified),
    ),
  };
}

async function measureOnSession(
  session: EngineSession,
  definition: ReferenceQueryDefinition,
  engine: EngineId,
  oracleTuples: readonly unknown[][],
): Promise<ReferenceEngineMeasurement> {
  const sql = definition.engineSql?.[engine] ?? definition.sql;
  try {
    const prepareStarted = performance.now();
    const prepared = await session.prepare(sql);
    const prepareMs = performance.now() - prepareStarted;
    try {
      let rows = await prepared.execute();
      const { medianMs, p95Ms, batchSize } = await measureRepeated(async () => {
        rows = await prepared.execute();
      }, SAMPLE_COUNT);
      // Engines with a result cache also report what a repeat costs an application that keeps
      // the default on. It is a different quantity from medianMs, so it gets its own field
      // instead of replacing it — and it is microseconds, so it needs the batching most.
      let cachedMedianMs: number | undefined;
      let cachedBatchSize: number | undefined;
      if (prepared.executeCached !== undefined) {
        // Bound to the statement: the harness only ever calls it back through the object.
        const runCached = (): Promise<unknown> => prepared.executeCached?.() ?? Promise.resolve([]);
        await runCached();
        const cached = await measureRepeated(runCached, SAMPLE_COUNT);
        cachedMedianMs = cached.medianMs;
        cachedBatchSize = cached.batchSize;
      }
      const tuples = canonicalTuples(rows, (row) =>
        definition.columns.map((column) => row[column]),
      );
      return {
        engine,
        supported: true,
        prepareMs,
        medianMs,
        p95Ms,
        batchSize,
        ...(cachedMedianMs === undefined ? {} : { cachedMedianMs }),
        ...(cachedBatchSize === undefined ? {} : { cachedBatchSize }),
        ...(prepared.peakMemoryBytes === undefined
          ? {}
          : { peakMemoryBytes: prepared.peakMemoryBytes }),
        resultRows: rows.length,
        checksum: referenceChecksum(tuples),
        verified: tuplesMatch(tuples, oracleTuples),
      };
    } finally {
      prepared.close();
    }
  } catch (error) {
    return {
      engine,
      supported: false,
      error: error instanceof Error ? error.message : String(error),
      prepareMs: 0,
      medianMs: 0,
      p95Ms: 0,
      resultRows: 0,
      checksum: 0,
      verified: false,
    };
  }
}
function numericKeySet(
  tables: ReadonlyMap<string, DatabaseRow[]>,
  table: string,
  column: string,
): Set<number> {
  return new Set(rows(tables, table).map((row) => numberField(row, column)));
}

export function buildReferenceQueryContext(
  tables: ReadonlyMap<string, DatabaseRow[]>,
): ReferenceQueryContext {
  const regionsById = indexRows(tables, "regions", "region_id");
  const customersById = indexRows(tables, "customers", "customer_id");
  const productsById = indexRows(tables, "products", "product_id");
  const ordersById = indexRows(tables, "orders", "order_id");
  const returnedItemIds = numericKeySet(tables, "returns", "item_id");
  const returnedOrderIds = new Set<number>();
  for (const item of rows(tables, "order_items")) {
    if (returnedItemIds.has(numberField(item, "item_id"))) {
      returnedOrderIds.add(numberField(item, "order_id"));
    }
  }
  return {
    tables,
    regionsById,
    customersById,
    productsById,
    ordersById,
    returnedItemIds,
    returnedOrderIds,
  };
}

function indexRows(
  tables: ReadonlyMap<string, DatabaseRow[]>,
  table: string,
  key: string,
): Map<number, DatabaseRow> {
  return new Map(rows(tables, table).map((row) => [numberField(row, key), row]));
}

/** Low, dense product keys so the probe resolves the same five rows at every scale. */
const PRODUCT_KEY_SET = [1, 2, 3, 5, 8];

export function referenceQueryDefinitions(orderRows: number): ReferenceQueryDefinition[] {
  const pointId = Math.max(1, Math.floor(orderRows / 2));
  // Selective probes derive their keys from the row count so they hit real rows at every
  // scale. Customers are a quarter of orders in this scenario and products a fiftieth, so
  // both stay in range without the definitions knowing the generator's exact ratios.
  const customerPointId = Math.max(1, Math.floor(orderRows / 8));
  // Every order gets exactly three line items, and orders are dense from 1, so these probes
  // resolve to the same row counts at every scale — which `expectedRows` has to be.
  const itemsOrderId = Math.max(1, Math.floor(orderRows / 3));
  const rangeFirstId = Math.max(1, Math.floor(orderRows / 4));
  const dateRangeRows = Math.min(
    25,
    Array.from({ length: orderRows }, (_, index) => index).filter(
      (index) => index % 4 === 1 && index % 365 >= 90 && index % 365 < 181,
    ).length,
  );
  return [
    {
      id: "q1",
      workload: "oltp",
      name: "Order point lookup",
      complexity: "simple",
      tables: ["orders"],
      expectedRows: 1,
      sql: `SELECT * FROM orders WHERE order_id = ${String(pointId)}`,
      columns: [
        "order_id",
        "customer_id",
        "shipping_address_id",
        "currency_id",
        "placed_at",
        "status",
        "total",
      ],
      project: (row) => [
        row.order_id,
        row.customer_id,
        row.shipping_address_id,
        row.currency_id,
        row.placed_at,
        row.status,
        row.total,
      ],
      baseline: (context) => {
        const row = context.ordersById.get(pointId);
        return row === undefined ? [] : [row];
      },
      oracle: (tables) =>
        rows(tables, "orders").filter((row) => numberField(row, "order_id") === pointId),
    },
    {
      id: "s1",
      workload: "oltp",
      name: "Customer by key",
      complexity: "simple",
      tables: ["customers"],
      expectedRows: 1,
      sql: `SELECT customer_id, region_id, segment FROM customers WHERE customer_id = ${String(customerPointId)}`,
      columns: ["customer_id", "region_id", "segment"],
      project: (row) => [row.customer_id, row.region_id, row.segment],
      baseline: (context) => {
        const row = context.customersById.get(customerPointId);
        return row === undefined ? [] : [row];
      },
      oracle: (tables) =>
        rows(tables, "customers").filter(
          (row) => numberField(row, "customer_id") === customerPointId,
        ),
    },
    {
      id: "s2",
      workload: "oltp",
      name: "Line items for one order",
      complexity: "simple",
      tables: ["order_items"],
      // The generator gives every order exactly three line items at every scale.
      expectedRows: 3,
      sql: `SELECT item_id, product_id, quantity FROM order_items WHERE order_id = ${String(itemsOrderId)} ORDER BY item_id`,
      columns: ["item_id", "product_id", "quantity"],
      project: (row) => [row.item_id, row.product_id, row.quantity],
      baseline: (context) =>
        rows(context.tables, "order_items")
          .filter((row) => numberField(row, "order_id") === itemsOrderId)
          .sort((left, right) => numberField(left, "item_id") - numberField(right, "item_id")),
      oracle: (tables) =>
        rows(tables, "order_items")
          .filter((row) => numberField(row, "order_id") === itemsOrderId)
          .sort((left, right) => numberField(left, "item_id") - numberField(right, "item_id")),
    },
    {
      id: "s3",
      workload: "oltp",
      name: "Small key set",
      complexity: "simple",
      tables: ["products"],
      expectedRows: PRODUCT_KEY_SET.length,
      sql: `SELECT product_id, category, unit_cost FROM products WHERE product_id IN (${PRODUCT_KEY_SET.map(String).join(", ")}) ORDER BY product_id`,
      columns: ["product_id", "category", "unit_cost"],
      project: (row) => [row.product_id, row.category, row.unit_cost],
      baseline: (context) =>
        PRODUCT_KEY_SET.map((key) => context.productsById.get(key)).filter(
          (row) => row !== undefined,
        ),
      oracle: (tables) =>
        rows(tables, "products")
          .filter((row) => PRODUCT_KEY_SET.includes(numberField(row, "product_id")))
          .sort(
            (left, right) => numberField(left, "product_id") - numberField(right, "product_id"),
          ),
    },
    {
      id: "s4",
      workload: "oltp",
      name: "Keyed range window",
      complexity: "simple",
      tables: ["orders"],
      expectedRows: 10,
      sql: `SELECT order_id, status, total FROM orders WHERE order_id >= ${String(rangeFirstId)} AND order_id < ${String(rangeFirstId + 100)} ORDER BY order_id LIMIT 10`,
      columns: ["order_id", "status", "total"],
      project: (row) => [row.order_id, row.status, row.total],
      baseline: (context) =>
        rows(context.tables, "orders")
          .filter((row) => {
            const id = numberField(row, "order_id");
            return id >= rangeFirstId && id < rangeFirstId + 100;
          })
          .sort((left, right) => numberField(left, "order_id") - numberField(right, "order_id"))
          .slice(0, 10),
      oracle: (tables) =>
        rows(tables, "orders")
          .filter((row) => {
            const id = numberField(row, "order_id");
            return id >= rangeFirstId && id < rangeFirstId + 100;
          })
          .sort((left, right) => numberField(left, "order_id") - numberField(right, "order_id"))
          .slice(0, 10),
    },
    {
      id: "s5",
      workload: "oltp",
      name: "Order exists check",
      complexity: "simple",
      tables: ["orders"],
      expectedRows: 1,
      sql: `SELECT COUNT(*) AS found FROM orders WHERE order_id = ${String(pointId)}`,
      columns: ["found"],
      project: (row) => [row.found],
      baseline: (context) => [{ found: context.ordersById.has(pointId) ? 1 : 0 }],
      oracle: (tables) => [
        {
          found: rows(tables, "orders").filter((row) => numberField(row, "order_id") === pointId)
            .length,
        },
      ],
    },
    {
      id: "q2",
      workload: "oltp",
      name: "Paid orders in a date range",
      complexity: "simple",
      tables: ["orders"],
      expectedRows: dateRangeRows,
      // order_id breaks total ties so the LIMIT selects the same 25 rows in every implementation.
      sql: "SELECT order_id, customer_id, total FROM orders WHERE status = 'paid' AND placed_at >= DATE '2025-04-01' AND placed_at < DATE '2025-07-01' ORDER BY total DESC, order_id LIMIT 25",
      // sqlite has no DATE literal; its datetimes are stored as ISO text, so plain
      // string bounds compare correctly.
      engineSql: {
        sqlite:
          "SELECT order_id, customer_id, total FROM orders WHERE status = 'paid' AND placed_at >= '2025-04-01' AND placed_at < '2025-07-01' ORDER BY total DESC, order_id LIMIT 25",
      },
      columns: ["order_id", "customer_id", "total"],
      project: (row) => [row.order_id, row.customer_id, row.total],
      baseline: (context) => paidOrdersInRange(rows(context.tables, "orders")),
      oracle: (tables) => paidOrdersInRange(rows(tables, "orders")),
    },
    {
      id: "q3",
      workload: "olap",
      name: "Revenue by order status",
      complexity: "moderate",
      tables: ["orders"],
      expectedRows: 4,
      sql: "SELECT status, COUNT(*) AS orders, SUM(total) AS revenue, AVG(total) AS avg_order FROM orders GROUP BY status ORDER BY revenue DESC",
      columns: ["status", "orders", "revenue", "avg_order"],
      project: (row) => [row.key, row.count, row.total, row.average],
      baseline: (context) => groupRevenue(rows(context.tables, "orders"), "status", "total"),
      oracle: groupRevenueOracle,
    },
    {
      id: "q4",
      workload: "olap",
      name: "Top customers by captured revenue",
      complexity: "moderate",
      tables: ["customers", "orders", "payments"],
      expectedRows: 20,
      sql: "SELECT c.customer_id, c.segment, SUM(p.captured_amount) AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id JOIN payments p ON p.order_id = o.order_id WHERE p.status = 'captured' GROUP BY c.customer_id, c.segment ORDER BY revenue DESC, c.customer_id LIMIT 20",
      columns: ["customer_id", "segment", "revenue"],
      project: (row) => [row.customerId, row.segment, row.total],
      baseline: topCustomersIndexed,
      oracle: topCustomers,
    },
    {
      id: "q5",
      workload: "olap",
      name: "Category revenue after discounts",
      complexity: "moderate",
      tables: ["order_items", "products"],
      expectedRows: 5,
      sql: "SELECT p.category, SUM(i.quantity * i.unit_price * (1 - i.discount_pct)) AS net_revenue FROM order_items i JOIN products p ON p.product_id = i.product_id GROUP BY p.category ORDER BY net_revenue DESC",
      columns: ["category", "net_revenue"],
      project: (row) => [row.category, row.revenue],
      baseline: categoryRevenueIndexed,
      oracle: categoryRevenue,
    },
    {
      id: "q6",
      workload: "olap",
      name: "Repeat customers without returns",
      complexity: "complex",
      tables: ["orders", "order_items", "returns"],
      expectedRows: expectedRepeatCustomerRows(orderRows),
      // COUNT(DISTINCT ...) and NOT EXISTS are outside the surface, so the returned orders
      // become a DISTINCT CTE and the anti-join becomes NOT IN over that CTE.
      sql: "WITH returned_orders AS (SELECT DISTINCT i.order_id AS order_id FROM order_items i JOIN returns r ON r.item_id = i.item_id) SELECT o.customer_id, COUNT(*) AS orders FROM orders o WHERE o.order_id NOT IN (SELECT order_id FROM returned_orders) GROUP BY o.customer_id HAVING COUNT(*) >= 2",
      columns: ["customer_id", "orders"],
      project: (row) => [row.customerId, row.count],
      baseline: repeatCustomersWithoutReturnsIndexed,
      oracle: repeatCustomersWithoutReturns,
    },
    {
      id: "q7",
      workload: "olap",
      name: "Top products within each category",
      complexity: "complex",
      tables: ["order_items", "products"],
      expectedRows: 15,
      // Ranking on ROUND(revenue, 4) makes mathematically-tied revenues true ties in every
      // engine — summation-order float noise would otherwise pick different winners — and
      // product_id then breaks those ties so DENSE_RANK stays unique and rank <= 3 keeps
      // exactly three rows per category. QUALIFY is not in the surface; the window feeds a
      // CTE instead.
      sql: "WITH product_revenue AS (SELECT p.category AS category, i.product_id AS product_id, SUM(i.quantity * i.unit_price) AS revenue FROM order_items i JOIN products p ON p.product_id = i.product_id GROUP BY p.category, i.product_id), ranked AS (SELECT category, product_id, revenue, DENSE_RANK() OVER (PARTITION BY category ORDER BY ROUND(revenue, 4) DESC, product_id) AS rank FROM product_revenue) SELECT category, product_id, revenue, rank FROM ranked WHERE rank <= 3 ORDER BY category, rank",
      // Postgres has no ROUND(double precision, int); the window key casts through numeric.
      engineSql: {
        pglite:
          "WITH product_revenue AS (SELECT p.category AS category, i.product_id AS product_id, SUM(i.quantity * i.unit_price) AS revenue FROM order_items i JOIN products p ON p.product_id = i.product_id GROUP BY p.category, i.product_id), ranked AS (SELECT category, product_id, revenue, DENSE_RANK() OVER (PARTITION BY category ORDER BY ROUND(revenue::numeric, 4) DESC, product_id) AS rank FROM product_revenue) SELECT category, product_id, revenue, rank FROM ranked WHERE rank <= 3 ORDER BY category, rank",
      },
      columns: ["category", "product_id", "revenue", "rank"],
      project: (row) => [row.category, row.productId, row.revenue, row.rank],
      baseline: rankedProductsIndexed,
      oracle: rankedProducts,
    },
    {
      id: "q8",
      workload: "olap",
      name: "Monthly cohort revenue and return rate",
      complexity: "complex",
      tables: ["customers", "orders", "order_items", "returns", "payments"],
      expectedRows: 12,
      // MinnowDatabase and PGlite both run DATE_TRUNC natively; the truncated month
      // canonicalizes to the same ISO instant on every path. sqlite has no DATE_TRUNC,
      // so its variant formats the month start as the equivalent ISO text directly.
      sql: "WITH cohorts AS (SELECT customer_id AS customer_id, DATE_TRUNC('month', joined_at) AS cohort_month FROM customers), returned_orders AS (SELECT DISTINCT i.order_id AS order_id FROM order_items i JOIN returns r ON r.item_id = i.item_id), captured AS (SELECT k.cohort_month AS cohort_month, o.customer_id AS customer_id, SUM(p.captured_amount) AS revenue, COUNT(*) AS orders FROM payments p JOIN orders o ON o.order_id = p.order_id JOIN cohorts k ON k.customer_id = o.customer_id WHERE p.status = 'captured' GROUP BY k.cohort_month, o.customer_id) SELECT cohort_month, COUNT(*) AS customers, SUM(revenue) AS revenue FROM captured GROUP BY cohort_month ORDER BY cohort_month",
      engineSql: {
        sqlite:
          "WITH cohorts AS (SELECT customer_id AS customer_id, strftime('%Y-%m-01T00:00:00.000Z', joined_at) AS cohort_month FROM customers), returned_orders AS (SELECT DISTINCT i.order_id AS order_id FROM order_items i JOIN returns r ON r.item_id = i.item_id), captured AS (SELECT k.cohort_month AS cohort_month, o.customer_id AS customer_id, SUM(p.captured_amount) AS revenue, COUNT(*) AS orders FROM payments p JOIN orders o ON o.order_id = p.order_id JOIN cohorts k ON k.customer_id = o.customer_id WHERE p.status = 'captured' GROUP BY k.cohort_month, o.customer_id) SELECT cohort_month, COUNT(*) AS customers, SUM(revenue) AS revenue FROM captured GROUP BY cohort_month ORDER BY cohort_month",
      },
      columns: ["cohort_month", "customers", "revenue"],
      project: (row) => [row.cohort, row.customers, row.revenue],
      baseline: cohortAnalysisIndexed,
      oracle: cohortAnalysis,
    },
    {
      id: "q9",
      workload: "olap",
      name: "Region and segment revenue matrix",
      complexity: "complex",
      tables: ["regions", "customers", "orders", "payments"],
      expectedRows: expectedRegionSegmentRows(orderRows),
      // Grouping per customer first, then per region and segment, counts distinct customers
      // and sums revenue in one statement without COUNT(DISTINCT ...).
      sql: "WITH per_customer AS (SELECT r.name AS region, c.segment AS segment, c.customer_id AS customer_id, SUM(p.captured_amount) AS revenue FROM regions r JOIN customers c ON c.region_id = r.region_id JOIN orders o ON o.customer_id = c.customer_id JOIN payments p ON p.order_id = o.order_id WHERE p.status = 'captured' GROUP BY r.name, c.segment, c.customer_id) SELECT region, segment, COUNT(*) AS customers, SUM(revenue) AS revenue FROM per_customer GROUP BY region, segment ORDER BY region, segment",
      columns: ["region", "segment", "customers", "revenue"],
      project: (row) => [row.region, row.segment, row.customers, row.revenue],
      baseline: regionSegmentRevenueIndexed,
      oracle: regionSegmentRevenue,
    },
    {
      id: "q10",
      workload: "olap",
      name: "Return rate by product category",
      complexity: "complex",
      tables: ["products", "order_items", "returns"],
      expectedRows: 5,
      // CASE is outside the surface. COUNT over the outer-joined key counts only matched rows,
      // and the DISTINCT derived table keeps a twice-returned item from inflating the line count.
      sql: "SELECT p.category, COUNT(*) AS items, COUNT(r.item_id) AS returns, COUNT(r.item_id) * 1.0 / COUNT(*) AS return_rate FROM products p JOIN order_items i ON i.product_id = p.product_id LEFT JOIN (SELECT DISTINCT item_id AS item_id FROM returns) r ON r.item_id = i.item_id GROUP BY p.category ORDER BY p.category",
      columns: ["category", "items", "returns", "return_rate"],
      project: (row) => [row.category, row.items, row.returns, row.returnRate],
      baseline: categoryReturnRateIndexed,
      oracle: categoryReturnRate,
    },
    {
      id: "q11",
      workload: "olap",
      name: "Tax collected by jurisdiction",
      complexity: "complex",
      tables: ["order_taxes", "tax_rates", "tax_jurisdictions"],
      expectedRows: Math.max(1, Math.ceil((orderRows * 24) / 1_000)),
      sql: "SELECT j.name, SUM(t.tax_amount) AS tax_collected FROM order_taxes t JOIN tax_rates r ON r.tax_rate_id = t.tax_rate_id JOIN tax_jurisdictions j ON j.jurisdiction_id = r.jurisdiction_id GROUP BY j.name ORDER BY tax_collected DESC",
      columns: ["name", "tax_collected"],
      project: (row) => [row.name, row.tax],
      baseline: taxByJurisdictionIndexed,
      oracle: taxByJurisdiction,
    },
    {
      id: "q12",
      workload: "olap",
      name: "Fulfillment volume by warehouse",
      complexity: "moderate",
      tables: ["shipments", "shipment_items", "warehouses"],
      expectedRows: Math.max(1, Math.ceil((orderRows * 8) / 1_000)),
      // Grouping per shipment first turns COUNT(DISTINCT shipment_id) into a plain COUNT(*).
      sql: "WITH per_shipment AS (SELECT w.name AS name, s.shipment_id AS shipment_id, SUM(si.quantity) AS units FROM warehouses w JOIN shipments s ON s.warehouse_id = w.warehouse_id JOIN shipment_items si ON si.shipment_id = s.shipment_id GROUP BY w.name, s.shipment_id) SELECT name, COUNT(*) AS shipments, SUM(units) AS units FROM per_shipment GROUP BY name ORDER BY units DESC",
      columns: ["name", "shipments", "units"],
      project: (row) => [row.name, row.shipments, row.units],
      baseline: fulfillmentByWarehouseIndexed,
      oracle: fulfillmentByWarehouse,
    },
    {
      id: "q13",
      workload: "olap",
      name: "Supplier inventory ledger",
      complexity: "moderate",
      tables: ["inventory_movements", "suppliers"],
      expectedRows: Math.max(1, Math.ceil((orderRows * 40) / 1_000)),
      sql: "SELECT s.name, COUNT(*) AS movements, SUM(m.quantity_delta) AS net_units FROM suppliers s JOIN inventory_movements m ON m.supplier_id = s.supplier_id GROUP BY s.name ORDER BY movements DESC",
      columns: ["name", "movements", "net_units"],
      project: (row) => [row.name, row.movements, row.netUnits],
      baseline: supplierInventoryIndexed,
      oracle: supplierInventory,
    },
    {
      id: "q14",
      workload: "olap",
      name: "Payment transaction funnel",
      complexity: "simple",
      tables: ["payment_transactions"],
      expectedRows: 4,
      sql: "SELECT kind, status, COUNT(*) AS transactions, SUM(amount) AS amount FROM payment_transactions GROUP BY kind, status ORDER BY kind, status",
      columns: ["kind", "status", "transactions", "amount"],
      project: (row) => [row.kind, row.status, row.count, row.amount],
      baseline: paymentFunnelIndexed,
      oracle: paymentFunnel,
    },
    {
      id: "q15",
      workload: "olap",
      name: "Discount and tax burden by order status",
      complexity: "complex",
      tables: ["orders", "order_discounts", "order_taxes"],
      expectedRows: 4,
      sql: "WITH discounts AS (SELECT order_id AS order_id, SUM(amount) AS amount FROM order_discounts GROUP BY order_id), taxes AS (SELECT order_id AS order_id, SUM(tax_amount) AS amount FROM order_taxes GROUP BY order_id) SELECT o.status, SUM(COALESCE(d.amount, 0)) AS discounts, SUM(COALESCE(t.amount, 0)) AS taxes FROM orders o LEFT JOIN discounts d ON d.order_id = o.order_id LEFT JOIN taxes t ON t.order_id = o.order_id GROUP BY o.status ORDER BY o.status",
      columns: ["status", "discounts", "taxes"],
      project: (row) => [row.status, row.discounts, row.taxes],
      baseline: orderAdjustmentsIndexed,
      oracle: orderAdjustments,
    },
    {
      id: "q16",
      workload: "olap",
      name: "Support message text search",
      complexity: "simple",
      tables: ["support_messages"],
      expectedRows: expectedMessageSearchRows(orderRows),
      // Search-as-you-type: a contains-LIKE over the highest-cardinality text column,
      // newest matches first. message_id breaks (impossible today) timestamp ties.
      sql: "SELECT message_id, ticket_id, author_type, sent_at FROM support_messages WHERE body LIKE '%message 12%' ORDER BY sent_at DESC, message_id LIMIT 25",
      columns: ["message_id", "ticket_id", "author_type", "sent_at"],
      project: (row) => [row.message_id, row.ticket_id, row.author_type, row.sent_at],
      baseline: (context) => messageSearch(rows(context.tables, "support_messages")),
      oracle: (tables) => messageSearch(rows(tables, "support_messages")),
    },
  ];
}

/** Shared by q16's baseline and oracle: contains-match, newest first, capped at 25. */
function messageSearch(messageRows: DatabaseRow[]): DatabaseRow[] {
  return messageRows
    .filter((row) => stringField(row, "body").includes("message 12"))
    .sort(
      (left, right) =>
        dateField(right, "sent_at").getTime() - dateField(left, "sent_at").getTime() ||
        numberField(left, "message_id") - numberField(right, "message_id"),
    )
    .slice(0, 25);
}

/** support_messages holds 1.5 rows per order; bodies end in their 1-based message id. */
function expectedMessageSearchRows(orderRows: number): number {
  const messageRows = Math.max(1, Math.ceil(orderRows * 1.5));
  let matches = 0;
  for (let id = 1; id <= messageRows; id += 1) {
    if (String(id).startsWith("12")) matches += 1;
  }
  return Math.min(25, matches);
}
/**
 * Shared by q2's baseline and oracle so both apply the same tie-break the SQL uses; the
 * LIMIT would otherwise pick a different 25 rows whenever two orders share a total.
 */
function paidOrdersInRange(orderRows: DatabaseRow[]): DatabaseRow[] {
  return orderRows
    .filter((row) => {
      const placed = dateField(row, "placed_at").getTime();
      return (
        stringField(row, "status") === "paid" &&
        placed >= Date.UTC(2025, 3, 1) &&
        placed < Date.UTC(2025, 6, 1)
      );
    })
    .sort(
      (left, right) =>
        numberField(right, "total") - numberField(left, "total") ||
        numberField(left, "order_id") - numberField(right, "order_id"),
    )
    .slice(0, 25);
}

function expectedRegionSegmentRows(orderRows: number): number {
  const customerRows = Math.max(1, Math.ceil(orderRows / 5));
  const regionRows = Math.max(1, Math.ceil((orderRows * 16) / 1_000));
  const groups = new Set<string>();
  for (let index = 0; index < orderRows; index += 1) {
    if (index % 7 === 0) continue;
    const customerIndex = index % customerRows;
    groups.add(`${String(customerIndex % regionRows)}:${String(customerIndex % 3)}`);
  }
  return groups.size;
}

function expectedRepeatCustomerRows(orderRows: number): number {
  const customerRows = Math.max(20, Math.ceil(orderRows / 5));
  const returnRows = Math.max(1, Math.floor(orderRows / 10));
  const returnedOrders = new Set(Array.from({ length: returnRows }, (_, index) => index * 10 + 1));
  const counts = new Map<number, number>();
  for (let orderId = 1; orderId <= orderRows; orderId += 1) {
    if (returnedOrders.has(orderId)) continue;
    const customerId = ((orderId - 1) % customerRows) + 1;
    counts.set(customerId, (counts.get(customerId) ?? 0) + 1);
  }
  return [...counts.values()].filter((count) => count >= 2).length;
}

export function rows(tables: ReadonlyMap<string, DatabaseRow[]>, name: string): DatabaseRow[] {
  return tables.get(name) ?? [];
}

export function numberField(row: DatabaseRow, name: string): number {
  const value = row[name];
  if (typeof value !== "number") throw new Error(`Expected numeric field: ${name}`);
  return value;
}

export function stringField(row: DatabaseRow, name: string): string {
  const value = row[name];
  if (typeof value !== "string") throw new Error(`Expected string field: ${name}`);
  return value;
}

export function dateField(row: DatabaseRow, name: string): Date {
  const value = row[name];
  if (!(value instanceof Date)) throw new Error(`Expected datetime field: ${name}`);
  return value;
}
function groupRevenue(input: DatabaseRow[], group: string, amount: string): unknown[] {
  const groups = new Map<string, { count: number; total: number }>();
  for (const row of input) {
    const key = stringField(row, group);
    const current = groups.get(key) ?? { count: 0, total: 0 };
    current.count += 1;
    current.total += numberField(row, amount);
    groups.set(key, current);
  }
  return [...groups]
    .map(([key, value]) => ({
      key,
      count: value.count,
      total: value.total,
      average: value.total / value.count,
    }))
    .sort((left, right) => right.total - left.total);
}

function groupRevenueOracle(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const statuses: Record<string, { count: number; total: number }> = {};
  for (const order of rows(tables, "orders")) {
    const status = stringField(order, "status");
    const current = statuses[status] ?? { count: 0, total: 0 };
    statuses[status] = {
      count: current.count + 1,
      total: current.total + numberField(order, "total"),
    };
  }
  return Object.entries(statuses)
    .map(([key, value]) => ({
      key,
      count: value.count,
      total: value.total,
      average: value.total / value.count,
    }))
    .sort((left, right) => right.total - left.total);
}

function topCustomers(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const customers = new Map(
    rows(tables, "customers").map((row) => [numberField(row, "customer_id"), row]),
  );
  const ordersById = new Map(
    rows(tables, "orders").map((row) => [numberField(row, "order_id"), row]),
  );
  const revenue = new Map<number, number>();
  for (const payment of rows(tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    revenue.set(
      customerId,
      (revenue.get(customerId) ?? 0) + numberField(payment, "captured_amount"),
    );
  }
  return [...revenue]
    .map(([customerId, total]) => ({
      customerId,
      segment: stringField(customers.get(customerId) ?? {}, "segment"),
      total,
    }))
    .sort((left, right) => right.total - left.total || left.customerId - right.customerId)
    .slice(0, 20);
}

function categoryRevenue(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const products = new Map(
    rows(tables, "products").map((row) => [numberField(row, "product_id"), row]),
  );
  const totals = new Map<string, number>();
  for (const item of rows(tables, "order_items")) {
    const product = products.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const net =
      numberField(item, "quantity") *
      numberField(item, "unit_price") *
      (1 - numberField(item, "discount_pct"));
    totals.set(category, (totals.get(category) ?? 0) + net);
  }
  return [...totals]
    .map(([category, revenue]) => ({ category, revenue }))
    .sort((left, right) => right.revenue - left.revenue);
}

function repeatCustomersWithoutReturns(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const returnedItems = new Set(rows(tables, "returns").map((row) => numberField(row, "item_id")));
  const returnedOrders = new Set<number>();
  for (const item of rows(tables, "order_items")) {
    if (returnedItems.has(numberField(item, "item_id")))
      returnedOrders.add(numberField(item, "order_id"));
  }
  const counts = new Map<number, number>();
  for (const order of rows(tables, "orders")) {
    if (returnedOrders.has(numberField(order, "order_id"))) continue;
    const customerId = numberField(order, "customer_id");
    counts.set(customerId, (counts.get(customerId) ?? 0) + 1);
  }
  return [...counts]
    .filter(([, count]) => count >= 2)
    .map(([customerId, count]) => ({ customerId, count }));
}

/** Mirrors q7's ROUND(revenue, 4) window key so float-noise near-ties rank identically. */
function roundedRevenue(revenue: number): number {
  return Math.round(revenue * 10_000) / 10_000;
}

function rankedProducts(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const products = new Map(
    rows(tables, "products").map((row) => [numberField(row, "product_id"), row]),
  );
  const totals = new Map<number, number>();
  for (const item of rows(tables, "order_items")) {
    const productId = numberField(item, "product_id");
    totals.set(
      productId,
      (totals.get(productId) ?? 0) +
        numberField(item, "quantity") * numberField(item, "unit_price"),
    );
  }
  const byCategory = new Map<string, Array<{ productId: number; revenue: number }>>();
  for (const [productId, revenue] of totals) {
    const product = products.get(productId);
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const list = byCategory.get(category) ?? [];
    list.push({ productId, revenue });
    byCategory.set(category, list);
  }
  return [...byCategory].flatMap(([category, values]) =>
    values
      .sort(
        (left, right) =>
          roundedRevenue(right.revenue) - roundedRevenue(left.revenue) ||
          left.productId - right.productId,
      )
      .slice(0, 3)
      .map((value, index) => ({ category, ...value, rank: index + 1 })),
  );
}

function cohortAnalysis(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const customers = new Map(
    rows(tables, "customers").map((row) => [numberField(row, "customer_id"), row]),
  );
  const orders = new Map(rows(tables, "orders").map((row) => [numberField(row, "order_id"), row]));
  const returnedItems = new Set(rows(tables, "returns").map((row) => numberField(row, "item_id")));
  const returnedOrders = new Set<number>();
  for (const item of rows(tables, "order_items"))
    if (returnedItems.has(numberField(item, "item_id")))
      returnedOrders.add(numberField(item, "order_id"));
  const cohorts = new Map<
    string,
    { customers: Set<number>; revenue: number; orders: number; returned: number }
  >();
  for (const payment of rows(tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const orderId = numberField(payment, "order_id");
    const order = orders.get(orderId);
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = customers.get(customerId);
    if (customer === undefined) continue;
    const joined = dateField(customer, "joined_at");
    const cohort = new Date(
      Date.UTC(joined.getUTCFullYear(), joined.getUTCMonth(), 1),
    ).toISOString();
    const value = cohorts.get(cohort) ?? {
      customers: new Set<number>(),
      revenue: 0,
      orders: 0,
      returned: 0,
    };
    value.customers.add(customerId);
    value.revenue += numberField(payment, "captured_amount");
    value.orders += 1;
    if (returnedOrders.has(orderId)) value.returned += 1;
    cohorts.set(cohort, value);
  }
  return [...cohorts]
    .map(([cohort, value]) => ({
      cohort,
      customers: value.customers.size,
      revenue: value.revenue,
      returnRate: value.returned / value.orders,
    }))
    .sort((left, right) => left.cohort.localeCompare(right.cohort));
}

function topCustomersIndexed(context: ReferenceQueryContext): unknown[] {
  const revenue = new Map<number, number>();
  for (const payment of rows(context.tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = context.ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    revenue.set(
      customerId,
      (revenue.get(customerId) ?? 0) + numberField(payment, "captured_amount"),
    );
  }
  return [...revenue]
    .map(([customerId, total]) => ({
      customerId,
      segment: stringField(context.customersById.get(customerId) ?? {}, "segment"),
      total,
    }))
    .sort((left, right) => right.total - left.total || left.customerId - right.customerId)
    .slice(0, 20);
}

function categoryRevenueIndexed(context: ReferenceQueryContext): unknown[] {
  const totals = new Map<string, number>();
  for (const item of rows(context.tables, "order_items")) {
    const product = context.productsById.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const net =
      numberField(item, "quantity") *
      numberField(item, "unit_price") *
      (1 - numberField(item, "discount_pct"));
    totals.set(category, (totals.get(category) ?? 0) + net);
  }
  return [...totals]
    .map(([category, revenue]) => ({ category, revenue }))
    .sort((left, right) => right.revenue - left.revenue);
}

function repeatCustomersWithoutReturnsIndexed(context: ReferenceQueryContext): unknown[] {
  const counts = new Map<number, number>();
  for (const order of rows(context.tables, "orders")) {
    if (context.returnedOrderIds.has(numberField(order, "order_id"))) continue;
    const customerId = numberField(order, "customer_id");
    counts.set(customerId, (counts.get(customerId) ?? 0) + 1);
  }
  return [...counts]
    .filter(([, count]) => count >= 2)
    .map(([customerId, count]) => ({ customerId, count }));
}

function rankedProductsIndexed(context: ReferenceQueryContext): unknown[] {
  const totals = new Map<number, number>();
  for (const item of rows(context.tables, "order_items")) {
    const productId = numberField(item, "product_id");
    totals.set(
      productId,
      (totals.get(productId) ?? 0) +
        numberField(item, "quantity") * numberField(item, "unit_price"),
    );
  }
  const byCategory = new Map<string, Array<{ productId: number; revenue: number }>>();
  for (const [productId, revenue] of totals) {
    const product = context.productsById.get(productId);
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const list = byCategory.get(category) ?? [];
    list.push({ productId, revenue });
    byCategory.set(category, list);
  }
  return [...byCategory].flatMap(([category, values]) =>
    values
      .sort(
        (left, right) =>
          roundedRevenue(right.revenue) - roundedRevenue(left.revenue) ||
          left.productId - right.productId,
      )
      .slice(0, 3)
      .map((value, index) => ({ category, ...value, rank: index + 1 })),
  );
}

function cohortAnalysisIndexed(context: ReferenceQueryContext): unknown[] {
  const cohorts = new Map<
    string,
    { customers: Set<number>; revenue: number; orders: number; returned: number }
  >();
  for (const payment of rows(context.tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const orderId = numberField(payment, "order_id");
    const order = context.ordersById.get(orderId);
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = context.customersById.get(customerId);
    if (customer === undefined) continue;
    const joined = dateField(customer, "joined_at");
    const cohort = new Date(
      Date.UTC(joined.getUTCFullYear(), joined.getUTCMonth(), 1),
    ).toISOString();
    const value = cohorts.get(cohort) ?? {
      customers: new Set<number>(),
      revenue: 0,
      orders: 0,
      returned: 0,
    };
    value.customers.add(customerId);
    value.revenue += numberField(payment, "captured_amount");
    value.orders += 1;
    if (context.returnedOrderIds.has(orderId)) value.returned += 1;
    cohorts.set(cohort, value);
  }
  return [...cohorts]
    .map(([cohort, value]) => ({
      cohort,
      customers: value.customers.size,
      revenue: value.revenue,
      returnRate: value.returned / value.orders,
    }))
    .sort((left, right) => left.cohort.localeCompare(right.cohort));
}

function regionSegmentRevenueIndexed(context: ReferenceQueryContext): unknown[] {
  return collectRegionSegmentRevenue(
    rows(context.tables, "payments"),
    context.ordersById,
    context.customersById,
    context.regionsById,
  );
}

function regionSegmentRevenue(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const ordersById = indexRows(tables, "orders", "order_id");
  const customersById = indexRows(tables, "customers", "customer_id");
  const regionsById = indexRows(tables, "regions", "region_id");
  const groups: Record<
    string,
    { region: string; segment: string; customers: Set<number>; revenue: number }
  > = {};
  for (const payment of rows(tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = customersById.get(customerId);
    if (customer === undefined) continue;
    const region = regionsById.get(numberField(customer, "region_id"));
    if (region === undefined) continue;
    const regionName = stringField(region, "name");
    const segment = stringField(customer, "segment");
    const key = `${regionName}\u0000${segment}`;
    const current = groups[key] ?? {
      region: regionName,
      segment,
      customers: new Set<number>(),
      revenue: 0,
    };
    current.customers.add(customerId);
    current.revenue += numberField(payment, "captured_amount");
    groups[key] = current;
  }
  return Object.values(groups)
    .map((value) => ({
      region: value.region,
      segment: value.segment,
      customers: value.customers.size,
      revenue: value.revenue,
    }))
    .sort(
      (left, right) =>
        left.region.localeCompare(right.region) || left.segment.localeCompare(right.segment),
    );
}

function collectRegionSegmentRevenue(
  payments: DatabaseRow[],
  ordersById: ReadonlyMap<number, DatabaseRow>,
  customersById: ReadonlyMap<number, DatabaseRow>,
  regionsById: ReadonlyMap<number, DatabaseRow>,
): unknown[] {
  const groups = new Map<
    string,
    { region: string; segment: string; customers: Set<number>; revenue: number }
  >();
  for (const payment of payments) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = customersById.get(customerId);
    if (customer === undefined) continue;
    const region = regionsById.get(numberField(customer, "region_id"));
    if (region === undefined) continue;
    const regionName = stringField(region, "name");
    const segment = stringField(customer, "segment");
    const key = `${regionName}\u0000${segment}`;
    const value = groups.get(key) ?? {
      region: regionName,
      segment,
      customers: new Set<number>(),
      revenue: 0,
    };
    value.customers.add(customerId);
    value.revenue += numberField(payment, "captured_amount");
    groups.set(key, value);
  }
  return [...groups.values()]
    .map((value) => ({
      region: value.region,
      segment: value.segment,
      customers: value.customers.size,
      revenue: value.revenue,
    }))
    .sort(
      (left, right) =>
        left.region.localeCompare(right.region) || left.segment.localeCompare(right.segment),
    );
}

function categoryReturnRateIndexed(context: ReferenceQueryContext): unknown[] {
  return collectCategoryReturnRate(
    rows(context.tables, "order_items"),
    context.productsById,
    context.returnedItemIds,
  );
}

function categoryReturnRate(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const productsById = indexRows(tables, "products", "product_id");
  const returnedItemIds = numericKeySet(tables, "returns", "item_id");
  const groups: Record<string, { items: number; returns: number }> = {};
  for (const item of rows(tables, "order_items")) {
    const product = productsById.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const current = groups[category] ?? { items: 0, returns: 0 };
    current.items += 1;
    if (returnedItemIds.has(numberField(item, "item_id"))) current.returns += 1;
    groups[category] = current;
  }
  return Object.entries(groups)
    .map(([category, value]) => ({
      category,
      items: value.items,
      returns: value.returns,
      returnRate: value.returns / value.items,
    }))
    .sort((left, right) => left.category.localeCompare(right.category));
}

function collectCategoryReturnRate(
  items: DatabaseRow[],
  productsById: ReadonlyMap<number, DatabaseRow>,
  returnedItemIds: ReadonlySet<number>,
): unknown[] {
  const groups = new Map<string, { items: number; returns: number }>();
  for (const item of items) {
    const product = productsById.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const value = groups.get(category) ?? { items: 0, returns: 0 };
    value.items += 1;
    if (returnedItemIds.has(numberField(item, "item_id"))) value.returns += 1;
    groups.set(category, value);
  }
  return [...groups]
    .map(([category, value]) => ({
      category,
      items: value.items,
      returns: value.returns,
      returnRate: value.returns / value.items,
    }))
    .sort((left, right) => left.category.localeCompare(right.category));
}

function taxByJurisdictionIndexed(context: ReferenceQueryContext): unknown[] {
  const rates = indexRows(context.tables, "tax_rates", "tax_rate_id");
  const jurisdictions = indexRows(context.tables, "tax_jurisdictions", "jurisdiction_id");
  const totals = new Map<string, number>();
  for (const tax of rows(context.tables, "order_taxes")) {
    const rate = rates.get(numberField(tax, "tax_rate_id"));
    const jurisdiction =
      rate === undefined ? undefined : jurisdictions.get(numberField(rate, "jurisdiction_id"));
    if (jurisdiction === undefined) continue;
    const name = stringField(jurisdiction, "name");
    totals.set(name, (totals.get(name) ?? 0) + numberField(tax, "tax_amount"));
  }
  return [...totals]
    .map(([name, tax]) => ({ name, tax }))
    .sort((left, right) => right.tax - left.tax || left.name.localeCompare(right.name));
}

function taxByJurisdiction(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const rateToJurisdiction: Record<string, number> = {};
  for (const rate of rows(tables, "tax_rates")) {
    rateToJurisdiction[String(numberField(rate, "tax_rate_id"))] = numberField(
      rate,
      "jurisdiction_id",
    );
  }
  const names = Object.fromEntries(
    rows(tables, "tax_jurisdictions").map((row) => [
      String(numberField(row, "jurisdiction_id")),
      stringField(row, "name"),
    ]),
  );
  const totals: Record<string, number> = {};
  for (const tax of rows(tables, "order_taxes")) {
    const jurisdictionId = rateToJurisdiction[String(numberField(tax, "tax_rate_id"))];
    const name = jurisdictionId === undefined ? undefined : names[String(jurisdictionId)];
    if (name === undefined) continue;
    totals[name] = (totals[name] ?? 0) + numberField(tax, "tax_amount");
  }
  return Object.entries(totals)
    .map(([name, tax]) => ({ name, tax }))
    .sort((left, right) => right.tax - left.tax || left.name.localeCompare(right.name));
}

function fulfillmentByWarehouseIndexed(context: ReferenceQueryContext): unknown[] {
  const shipments = indexRows(context.tables, "shipments", "shipment_id");
  const warehouses = indexRows(context.tables, "warehouses", "warehouse_id");
  const groups = new Map<string, { shipments: Set<number>; units: number }>();
  for (const item of rows(context.tables, "shipment_items")) {
    const shipment = shipments.get(numberField(item, "shipment_id"));
    const warehouse =
      shipment === undefined ? undefined : warehouses.get(numberField(shipment, "warehouse_id"));
    if (warehouse === undefined) continue;
    const name = stringField(warehouse, "name");
    const value = groups.get(name) ?? { shipments: new Set<number>(), units: 0 };
    value.shipments.add(numberField(item, "shipment_id"));
    value.units += numberField(item, "quantity");
    groups.set(name, value);
  }
  return [...groups]
    .map(([name, value]) => ({ name, shipments: value.shipments.size, units: value.units }))
    .sort((left, right) => right.units - left.units || left.name.localeCompare(right.name));
}

function fulfillmentByWarehouse(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const warehouseNames = Object.fromEntries(
    rows(tables, "warehouses").map((row) => [
      String(numberField(row, "warehouse_id")),
      stringField(row, "name"),
    ]),
  );
  const shipmentWarehouses = Object.fromEntries(
    rows(tables, "shipments").map((row) => [
      String(numberField(row, "shipment_id")),
      numberField(row, "warehouse_id"),
    ]),
  );
  const groups: Record<string, { shipments: Set<number>; units: number }> = {};
  for (const item of rows(tables, "shipment_items")) {
    const shipmentId = numberField(item, "shipment_id");
    const warehouseId = shipmentWarehouses[String(shipmentId)];
    const name = warehouseId === undefined ? undefined : warehouseNames[String(warehouseId)];
    if (name === undefined) continue;
    const value = groups[name] ?? { shipments: new Set<number>(), units: 0 };
    value.shipments.add(shipmentId);
    value.units += numberField(item, "quantity");
    groups[name] = value;
  }
  return Object.entries(groups)
    .map(([name, value]) => ({ name, shipments: value.shipments.size, units: value.units }))
    .sort((left, right) => right.units - left.units || left.name.localeCompare(right.name));
}

function supplierInventoryIndexed(context: ReferenceQueryContext): unknown[] {
  const suppliers = indexRows(context.tables, "suppliers", "supplier_id");
  const groups = new Map<string, { movements: number; netUnits: number }>();
  for (const movement of rows(context.tables, "inventory_movements")) {
    const supplier = suppliers.get(numberField(movement, "supplier_id"));
    if (supplier === undefined) continue;
    const name = stringField(supplier, "name");
    const value = groups.get(name) ?? { movements: 0, netUnits: 0 };
    value.movements += 1;
    value.netUnits += numberField(movement, "quantity_delta");
    groups.set(name, value);
  }
  return [...groups]
    .map(([name, value]) => ({ name, ...value }))
    .sort((left, right) => right.movements - left.movements || left.name.localeCompare(right.name));
}

function supplierInventory(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const names = Object.fromEntries(
    rows(tables, "suppliers").map((row) => [
      String(numberField(row, "supplier_id")),
      stringField(row, "name"),
    ]),
  );
  const groups: Record<string, { movements: number; netUnits: number }> = {};
  for (const movement of rows(tables, "inventory_movements")) {
    const name = names[String(numberField(movement, "supplier_id"))];
    if (name === undefined) continue;
    const value = groups[name] ?? { movements: 0, netUnits: 0 };
    value.movements += 1;
    value.netUnits += numberField(movement, "quantity_delta");
    groups[name] = value;
  }
  return Object.entries(groups)
    .map(([name, value]) => ({ name, ...value }))
    .sort((left, right) => right.movements - left.movements || left.name.localeCompare(right.name));
}

function paymentFunnelIndexed(context: ReferenceQueryContext): unknown[] {
  const groups = new Map<string, { kind: string; status: string; count: number; amount: number }>();
  for (const transaction of rows(context.tables, "payment_transactions")) {
    const kind = stringField(transaction, "kind");
    const status = stringField(transaction, "status");
    const key = `${kind}\u0000${status}`;
    const value = groups.get(key) ?? { kind, status, count: 0, amount: 0 };
    value.count += 1;
    value.amount += numberField(transaction, "amount");
    groups.set(key, value);
  }
  return [...groups.values()].sort(
    (left, right) => left.kind.localeCompare(right.kind) || left.status.localeCompare(right.status),
  );
}

function paymentFunnel(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const groups: Record<string, { kind: string; status: string; count: number; amount: number }> =
    {};
  for (const transaction of rows(tables, "payment_transactions")) {
    const kind = stringField(transaction, "kind");
    const status = stringField(transaction, "status");
    const key = `${kind}\u0000${status}`;
    const value = groups[key] ?? { kind, status, count: 0, amount: 0 };
    value.count += 1;
    value.amount += numberField(transaction, "amount");
    groups[key] = value;
  }
  return Object.values(groups).sort(
    (left, right) => left.kind.localeCompare(right.kind) || left.status.localeCompare(right.status),
  );
}

function orderAdjustmentsIndexed(context: ReferenceQueryContext): unknown[] {
  return collectOrderAdjustments(
    rows(context.tables, "orders"),
    rows(context.tables, "order_discounts"),
    rows(context.tables, "order_taxes"),
    true,
  );
}

function orderAdjustments(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  return collectOrderAdjustments(
    rows(tables, "orders"),
    rows(tables, "order_discounts"),
    rows(tables, "order_taxes"),
    false,
  );
}

function collectOrderAdjustments(
  orderRows: DatabaseRow[],
  discounts: DatabaseRow[],
  taxes: DatabaseRow[],
  useMaps: boolean,
): unknown[] {
  const discountByOrder = new Map<number, number>();
  const taxByOrder = new Map<number, number>();
  for (const discount of discounts) {
    const orderId = numberField(discount, "order_id");
    discountByOrder.set(
      orderId,
      (discountByOrder.get(orderId) ?? 0) + numberField(discount, "amount"),
    );
  }
  for (const tax of taxes) {
    const orderId = numberField(tax, "order_id");
    taxByOrder.set(orderId, (taxByOrder.get(orderId) ?? 0) + numberField(tax, "tax_amount"));
  }
  const groups = new Map<string, { discounts: number; taxes: number }>();
  for (const order of useMaps ? orderRows : [...orderRows].reverse()) {
    const status = stringField(order, "status");
    const orderId = numberField(order, "order_id");
    const value = groups.get(status) ?? { discounts: 0, taxes: 0 };
    value.discounts += discountByOrder.get(orderId) ?? 0;
    value.taxes += taxByOrder.get(orderId) ?? 0;
    groups.set(status, value);
  }
  return [...groups]
    .map(([status, value]) => ({ status, ...value }))
    .sort((left, right) => left.status.localeCompare(right.status));
}
