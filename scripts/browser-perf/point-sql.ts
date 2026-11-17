/** SQL shared by the executed point workload and its baseline identity. */
export const POINT_TABLES = {
  pt_int: {
    minnow:
      "id BIGINT PRIMARY KEY, a INTEGER NOT NULL, b DOUBLE PRECISION NOT NULL, c TEXT NOT NULL",
    sqlite: "id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b REAL NOT NULL, c TEXT NOT NULL",
  },
  pt_uuid: {
    minnow: "id TEXT PRIMARY KEY, a INTEGER NOT NULL, b DOUBLE PRECISION NOT NULL, c TEXT NOT NULL",
    sqlite: "id TEXT PRIMARY KEY, a INTEGER NOT NULL, b REAL NOT NULL, c TEXT NOT NULL",
  },
} as const;
export type PointTable = keyof typeof POINT_TABLES;

export const INSERT_SQL = (table: PointTable): string =>
  `INSERT INTO ${table} (id, a, b, c) VALUES (?, ?, ?, ?)`;
export const READ_SQL = (table: PointTable): string =>
  `SELECT id, a, b, c FROM ${table} WHERE id = ?`;
export const LITERAL_READ_SQL = (table: PointTable, literal: string): string =>
  `SELECT id, a, b, c FROM ${table} WHERE id = ${literal}`;
export const UPDATE_SQL = (table: PointTable): string =>
  `UPDATE ${table} SET a = a + 1, c = ? WHERE id = ?`;
