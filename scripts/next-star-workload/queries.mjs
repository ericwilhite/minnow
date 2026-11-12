import { W_QUERIES as base, sqlFor } from "../next-executor-workload/queries.mjs";
export { sqlFor };
export const W_QUERIES = [
  ...base,
  {
    id: "join_star_float_analytics",
    fam: "join",
    ordered: true,
    sql: "SELECT a.tier AS tier_a, b.tier AS tier_b, c.label AS label_c, COUNT(*) AS n, AVG(e.f00) AS s, MIN(e.f00) AS lo, MAX(e.f00) AS hi FROM events_wide e JOIN dim_d03 a ON a.k = e.d03 JOIN dim_d04 b ON b.k = e.d04 JOIN dim_d07 c ON c.k = e.d07 WHERE a.active AND b.weight > 1 AND c.tier = 't1' GROUP BY a.tier, b.tier, c.label ORDER BY s DESC, tier_a, tier_b, label_c LIMIT 50",
  },
  {
    id: "join_star_attribute_analytics",
    fam: "join",
    ordered: true,
    sql: "SELECT a.tier AS tier_a,a.label AS label_a,COUNT(*) AS n,SUM(e.f00) AS s FROM events_wide e JOIN dim_d03 a ON a.k=e.d03 WHERE (a.active OR a.tier IS NULL) AND NOT (a.weight<2) AND (a.tier IS NOT NULL OR a.weight>2) GROUP BY a.tier,a.label ORDER BY s DESC,tier_a,label_a LIMIT 50",
  },
];
