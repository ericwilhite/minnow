import { describe, expect, it } from "vitest";
import type { DatabaseRow } from "./database.js";
import { QueryMemoryContext } from "./memory.js";
import { compileQuery, createPreparedColumnarQuery, executeRowQuery } from "./query.js";
import { createColumnarTable } from "./vector.js";

const patterns = ["ascending", "descending", "shuffled", "all-null"] as const;

describe.each(["number", "datetime"] as const)("bounded ordering state for %s keys", (type) => {
  it.each(patterns)(
    "matches full row sorting over %s batches, filters and repeated executions",
    (pattern) => {
      const rows: DatabaseRow[] = Array.from({ length: 5000 }, (_, id) => {
        const value =
          pattern === "ascending"
            ? Math.floor(id / 3)
            : pattern === "descending"
              ? Math.floor((4999 - id) / 3)
              : ((id * 73) % 31) - 15;
        return {
          id,
          score:
            pattern === "all-null" || (pattern === "shuffled" && id % 9 === 0)
              ? null
              : type === "datetime"
                ? new Date(value * 86400000)
                : value === 0 && id % 2 === 0
                  ? -0
                  : value,
          enabled: id % 3 !== 0,
        };
      });
      const input = createColumnarTable(
        "data",
        new Map([
          ["id", { type: "number" as const, values: rows.map((row) => row.id ?? null) }],
          ["score", { type, values: rows.map((row) => row.score ?? null) }],
          ["enabled", { type: "boolean" as const, values: rows.map((row) => row.enabled ?? null) }],
        ]),
      );
      for (const direction of ["ASC", "DESC"]) {
        for (const filter of ["", "WHERE enabled=TRUE"]) {
          for (const tail of ["LIMIT 17 OFFSET 7", "LIMIT 0", "LIMIT 2000"]) {
            const sql = `SELECT id,score FROM data ${filter} ORDER BY score ${direction},id ${direction} ${tail}`;
            const expected = executeRowQuery(
              compileQuery(sql, { optimize: false }),
              new Map([["data", rows]]),
            );
            const memory = new QueryMemoryContext();
            const prepared = createPreparedColumnarQuery(
              compileQuery(sql),
              new Map([["data", input]]),
              memory,
            );
            try {
              for (let repeat = 0; repeat < 3; repeat++)
                expect(prepared.execute()).toEqual(expected);
            } finally {
              prepared.close();
            }
          }
        }
      }
    },
  );
});
