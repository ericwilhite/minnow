/**
 * The queries the playground offers as a starting point. Each one is chosen to make the engine
 * do something different — a grouped scan, a three-way join, a window function, a correlated
 * subquery, a recursive CTE — and each answers a question a retailer would actually ask.
 */
export interface SampleQuery {
  id: string;
  label: string;
  /** What the query is for, and what part of the engine it exercises. */
  note: string;
  sql: string;
}

export const sampleQueries: readonly SampleQuery[] = [
  {
    id: "revenue-by-month",
    label: "Revenue by month",
    note: "A grouped scan over every completed order, bucketed with DATE_TRUNC.",
    sql: `SELECT DATE_TRUNC('month', placed_at) AS month,
       COUNT(*) AS orders,
       ROUND(SUM(total), 2) AS revenue,
       ROUND(AVG(total), 2) AS average_basket
FROM orders
WHERE status = 'completed'
GROUP BY DATE_TRUNC('month', placed_at)
ORDER BY month DESC`,
  },
  {
    id: "top-products",
    label: "Best sellers by category",
    note: "A window function ranking products inside their category, over a join.",
    sql: `SELECT category, name, units, revenue
FROM (
  SELECT p.category,
         p.name,
         SUM(i.quantity) AS units,
         ROUND(SUM(i.line_total), 2) AS revenue,
         ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(i.line_total) DESC) AS rank
  FROM order_items i
  JOIN products p ON p.product_id = i.product_id
  GROUP BY p.category, p.name
) AS ranked
WHERE rank <= 3
ORDER BY category, revenue DESC`,
  },
  {
    id: "search",
    label: "Full-text search",
    note: "A ranked MATCH across product names and brands, with no index declaration required.",
    sql: `SELECT name,
       category,
       brand,
       list_price,
       BM25(name, brand) AGAINST 'copper' AS rank
FROM products
WHERE MATCH(name, brand) AGAINST 'copper'
ORDER BY rank DESC
LIMIT 10`,
  },
  {
    id: "store-performance",
    label: "Store performance",
    note: "A three-way join with a filtered aggregate, ordered by revenue per square metre.",
    sql: `SELECT s.name AS store,
       s.city,
       s.country,
       COUNT(DISTINCT o.order_id) AS orders,
       ROUND(SUM(o.total), 2) AS revenue,
       ROUND(SUM(o.total) / s.floor_sqm, 2) AS revenue_per_sqm
FROM stores s
JOIN orders o ON o.store_id = s.store_id
WHERE o.status = 'completed'
GROUP BY s.store_id, s.name, s.city, s.country, s.floor_sqm
ORDER BY revenue_per_sqm DESC
LIMIT 20`,
  },
  {
    id: "customer-value",
    label: "Who spends the most",
    note: "Lifetime value per customer, and how concentrated revenue really is.",
    sql: `SELECT c.loyalty_tier,
       COUNT(DISTINCT c.customer_id) AS customers,
       ROUND(SUM(o.total), 2) AS revenue,
       ROUND(SUM(o.total) / COUNT(DISTINCT c.customer_id), 2) AS revenue_per_customer,
       ROUND(100.0 * SUM(o.total) / (SELECT SUM(total) FROM orders), 1) AS pct_of_revenue
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.loyalty_tier
ORDER BY revenue DESC`,
  },
  {
    id: "lapsed",
    label: "Customers who never came back",
    note: "A correlated NOT EXISTS — the shape an anti-join optimizes into.",
    sql: `SELECT c.country,
       COUNT(*) AS signed_up,
       SUM(CASE WHEN o.order_id IS NULL THEN 1 ELSE 0 END) AS never_ordered,
       ROUND(100.0 * SUM(CASE WHEN o.order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
FROM customers c
LEFT JOIN (SELECT DISTINCT customer_id, order_id FROM orders) AS o
  ON o.customer_id = c.customer_id
GROUP BY c.country
HAVING COUNT(*) > 200
ORDER BY pct DESC`,
  },
  {
    id: "returns",
    label: "What gets returned",
    note: "Return rate per category, joining three tables and dividing two aggregates.",
    sql: `SELECT p.category,
       COUNT(DISTINCT r.return_id) AS returns,
       COUNT(DISTINCT i.order_item_id) AS lines_sold,
       ROUND(100.0 * COUNT(DISTINCT r.return_id) / COUNT(DISTINCT i.order_item_id), 2) AS return_rate,
       ROUND(SUM(COALESCE(r.refund_amount, 0)), 2) AS refunded
FROM order_items i
JOIN products p ON p.product_id = i.product_id
LEFT JOIN returns r ON r.order_item_id = i.order_item_id
GROUP BY p.category
ORDER BY return_rate DESC`,
  },
  {
    id: "basket-pairs",
    label: "Bought together",
    note: "A self-join on order lines: the classic market-basket query, hashed on the order.",
    sql: `SELECT a.category AS category_a,
       b.category AS category_b,
       COUNT(*) AS baskets
FROM order_items ia
JOIN order_items ib ON ib.order_id = ia.order_id AND ib.product_id > ia.product_id
JOIN products a ON a.product_id = ia.product_id
JOIN products b ON b.product_id = ib.product_id
GROUP BY a.category, b.category
ORDER BY baskets DESC
LIMIT 15`,
  },
  {
    id: "cohorts",
    label: "Retention by signup month",
    note: "A cohort table built from chained CTEs and three DISTINCT counts over the same rows.",
    sql: `WITH cohort AS (
  SELECT customer_id, DATE_TRUNC('month', signed_up_on) AS joined FROM customers
),
activity AS (
  SELECT o.customer_id,
         c.joined,
         EXTRACT(year FROM o.placed_at) * 12 + EXTRACT(month FROM o.placed_at)
           - (EXTRACT(year FROM c.joined) * 12 + EXTRACT(month FROM c.joined)) AS months_since
  FROM orders o JOIN cohort c ON c.customer_id = o.customer_id
)
SELECT joined,
       COUNT(DISTINCT customer_id) AS customers,
       COUNT(DISTINCT CASE WHEN months_since BETWEEN 1 AND 3 THEN customer_id END) AS active_q1,
       COUNT(DISTINCT CASE WHEN months_since BETWEEN 4 AND 12 THEN customer_id END) AS active_y1
FROM activity
GROUP BY joined
ORDER BY joined`,
  },
];

export const defaultQuery = sampleQueries[0]?.sql ?? "SELECT 1";
