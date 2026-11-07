/** Shared PostgreSQL graph corpus; excluded from the published package with other test data. */
export const GRAPH_LOCKS = ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"] as const;
export const GRAPH_DDL = [
  "CREATE TABLE t (id integer PRIMARY KEY)",
  "INSERT INTO t VALUES (1), (2)",
  ...GRAPH_LOCKS.flatMap((lock, i) => [
    `CREATE VIEW graph_v${String(i)} AS SELECT * FROM t FOR ${lock}`,
    `CREATE VIEW graph_n${String(i)} AS SELECT * FROM graph_v${String(i)}`,
    `CREATE VIEW graph_g${String(i)} AS WITH q AS (SELECT * FROM graph_n${String(i)}), unused AS (SELECT * FROM q) SELECT 1 AS id`,
  ]),
];
export function rowLockGraphs(): Array<{
  sql: string;
  lock: (typeof GRAPH_LOCKS)[number] | undefined;
  states: string[];
}> {
  const cases: ReturnType<typeof rowLockGraphs> = [];
  const graphs: number[][][] = [];
  // Every acyclic graph with two or three ordered declarations, plus a four-node chain/diamond.
  for (const size of [2, 3]) {
    const edges: Array<[number, number]> = [];
    for (let to = 1; to < size; to++) for (let from = 0; from < to; from++) edges.push([to, from]);
    for (let mask = 0; mask < 2 ** edges.length; mask++) {
      const graph: number[][] = Array.from({ length: size }, () => []);
      edges.forEach(([to, from], bit) => {
        if ((mask & (1 << bit)) !== 0) graph[to]?.push(from);
      });
      graphs.push(graph);
    }
  }
  graphs.push([[], [0], [1], [2]], [[], [0], [0], [1, 2]]);
  for (const [i, lock] of GRAPH_LOCKS.entries())
    for (const source of ["direct", "view", "nested view", "view graph"])
      for (const hint of ["", "MATERIALIZED ", "NOT MATERIALIZED "]) {
        const body =
          source === "direct"
            ? `SELECT * FROM t FOR ${lock}`
            : `SELECT * FROM graph_${source === "view" ? "v" : source === "view graph" ? "g" : "n"}${String(i)}`;
        for (const graph of graphs) {
          const referenced = graph.some((edges) => edges.includes(0));
          const definitions = graph
            .map((dependencies, n) => {
              const select =
                n === 0
                  ? body
                  : dependencies.length === 0
                    ? "SELECT 1 AS id"
                    : `SELECT a${String(dependencies[0])}.id FROM ${dependencies.map((dep) => `c${String(dep)} a${String(dep)}`).join(", ")}`;
              return `c${String(n)} AS ${hint}(${select})`;
            })
            .join(", ");
          for (const tail of [
            "SELECT 1 AS id",
            `SELECT * FROM c${String(graph.length - 1)}`,
            "SELECT * FROM t FOR KEY SHARE",
            "SELECT * FROM t WHERE id = 1",
            "SELECT * FROM t WHERE id = 1 FOR KEY SHARE",
          ]) {
            const states = [referenced ? "referenced by unused" : "unreferenced", source];
            if (hint === "MATERIALIZED ") states.push("materialized");
            if (hint === "NOT MATERIALIZED ") states.push("not materialized");
            if (graph.length > 2 && graph[2]?.includes(1)) states.push("chain");
            if (graph.some((deps) => deps.length > 1)) states.push("multiple references");
            if (tail.endsWith("FOR KEY SHARE")) states.push("strongest final clause");
            if (tail.includes("WHERE id")) states.push("point read");
            cases.push({
              sql: `WITH ${definitions} ${tail}`,
              lock: referenced ? lock : tail.endsWith("FOR KEY SHARE") ? "KEY SHARE" : undefined,
              states,
            });
          }
        }
        const inner = `WITH c AS ${hint}(${body}), d AS ${hint}(SELECT * FROM c) SELECT 1 AS id`;
        for (const [sql, used] of [
          [`WITH discarded AS (${inner}) SELECT 1 AS id`, false],
          [`WITH consumed AS (${inner}) SELECT * FROM consumed`, true],
          [`SELECT * FROM (${inner}) s`, true],
          [
            `WITH c AS ${hint}(${body}), d AS ${hint}(SELECT * FROM c), e AS ${hint}(SELECT * FROM c) SELECT 1 AS id`,
            true,
          ],
        ] as const)
          cases.push({
            sql,
            lock: used ? lock : undefined,
            states: [used ? "nested scope counted" : "nested scope discarded", source],
          });
      }
  for (const [i, lock] of GRAPH_LOCKS.entries())
    for (const view of [
      "direct",
      `graph_v${String(i)}`,
      `graph_n${String(i)}`,
      `graph_g${String(i)}`,
    ])
      for (const hint of ["", "MATERIALIZED ", "NOT MATERIALIZED "]) {
        const scalar =
          view === "direct"
            ? `(SELECT id FROM t LIMIT 1 FOR ${lock})`
            : `(SELECT id FROM ${view} LIMIT 1)`;
        for (const [select, counted] of [
          [`SELECT ${scalar} AS id`, true],
          [`SELECT CASE WHEN true THEN ${scalar} ELSE 1 END AS id`, true],
          [`SELECT CASE WHEN false THEN ${scalar} ELSE 1 END AS id`, false],
          [`SELECT CASE WHEN 1 = 0 THEN ${scalar} ELSE 1 END AS id`, false],
          [`SELECT CASE WHEN id = 0 THEN ${scalar} ELSE 1 END AS id FROM t`, true],
          [`SELECT COALESCE(1, ${scalar}) AS id`, false],
          [`SELECT COALESCE(NULL, ${scalar}) AS id`, true],
        ] as const)
          cases.push({
            sql: `WITH c AS ${hint}(${select}), d AS (SELECT * FROM c) SELECT 1 AS id`,
            lock: counted ? lock : undefined,
            states: [counted ? "scalar view" : "dead expression"],
          });
      }
  return cases;
}
