/** PostgreSQL owns result values. Independent buffer constructors own the
 * allocation witness; fixed seeds cover batch, OFFSET and coefficient boundaries. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXECUTOR_DEMAND_BOUNDARIES_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { createExecutor } from "../exec/create.js";
import type { Rel } from "../ir/ir.js";
import type { FlatVector } from "../vector/vector.js";
import { ints, column, tableInput } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { allocationAudit } from "./allocation-audit.js";

const kinds = ["BYTEA", "JSON", "JSONB"] as const;
const counts = [2047, 2048, 2049, 4097, 8193] as const;
const coefficientWidths = [1, 19, 38] as const;
const paths = ["native", "ties", "distinct", "sql-distinct"] as const;
const utf8 = new TextEncoder();
function varlen(type: FlatVector["type"], values: readonly Uint8Array[]): FlatVector {
  const offsets = new Int32Array(values.length + 1);
  for (let i = 0; i < values.length; i++)
    offsets[i + 1] = (offsets[i] ?? 0) + (values[i]?.length ?? 0);
  const bytes = new Uint8Array(offsets.at(-1) ?? 0);
  for (let i = 0; i < values.length; i++) bytes.set(values[i] ?? [], offsets[i] ?? 0);
  return {
    form: "flat",
    type,
    length: values.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
}
it.each(seedsFor("next-demand-allocation-boundaries", [0xb9e71, 0xb9e72]))(
  "complete admission covers multi-batch variable buffers under pinned pressure seed %i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      owner = core.harness.governor;
    const reach = createReach(
      "executor demand allocation boundaries",
      EXECUTOR_DEMAND_BOUNDARIES_REACH,
    );
    const evidence: Array<Record<string, unknown>> = [];
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...counts),
          fc.constantFrom(...kinds),
          fc.constantFrom(...paths),
          fc.constantFrom(0, 8192, 65535),
          fc.constantFrom(...coefficientWidths),
          async (count, kind, path, width, coefficientDigits) => {
            reach.beginHistory();
            await core.execute("DROP TABLE IF EXISTS demand_boundary");
            await pg.exec("DROP TABLE IF EXISTS demand_boundary");
            const ddl = `CREATE TABLE demand_boundary(id INT PRIMARY KEY,k INT,n NUMERIC,payload ${kind})`;
            // Native BYTEA is supported by the catalog/executor, while its SQL DDL
            // spelling is still refused by the front end (types.ts BytesType).
            if (kind === "BYTEA")
              await core.change({
                kind: "createTable",
                table: tableInput("demand_boundary", [
                  column("id", { kind: "int32", bits: 32 }, false),
                  column("k", { kind: "int32", bits: 32 }),
                  column("n", { kind: "decimal", precision: null, scale: null }),
                  column("payload", { kind: "bytes" }),
                ]),
              });
            else await core.execute(ddl);
            await pg.exec(ddl);
            const table = core.harness.path.catalog.tableByName("demand_boundary");
            if (!table) throw Error("boundary table");
            const numeric = table.columns[2]?.type,
              payload = table.columns[3]?.type;
            if (!numeric || !payload) throw Error("boundary types");
            const rows = Array.from({ length: count }, (_, i) => {
              const id = count - i,
                k = Math.floor(id / 3),
                scale = k % 4,
                coefficient =
                  BigInt("7".repeat(Math.min(coefficientDigits, [1, 19, 35, 38][k % 4] ?? 1))) +
                  BigInt(k);
              const digits = coefficient.toString().padStart(scale + 1, "0");
              const n = scale ? `${digits.slice(0, -scale)}.${digits.slice(-scale)}` : digits;
              const value = k === Math.floor(count / 3) ? "λ".repeat(width) : `x${String(k % 7)}`;
              const data =
                kind === "BYTEA"
                  ? Uint8Array.from([0, 255, ...utf8.encode(value)])
                  : utf8.encode(JSON.stringify({ s: value }));
              return { id, k, n, coefficient, scale, data };
            });
            const pgRows = rows.map((row) => ({
              id: row.id,
              k: row.k,
              n: row.n,
              payload:
                kind === "BYTEA"
                  ? Buffer.from(row.data).toString("hex")
                  : new TextDecoder().decode(row.data),
            }));
            const inserted = await pg.query<{ id: number; canonical: string }>(
              `INSERT INTO demand_boundary
            SELECT id,k,n,${kind === "BYTEA" ? "decode(payload,'hex')" : `payload::${kind}`}
            FROM json_to_recordset($1::json) x(id int,k int,n numeric,payload text)
            RETURNING id,payload::text AS canonical`,
              [JSON.stringify(pgRows)],
            );
            const canonical = new Map(inserted.rows.map((row) => [row.id, row.canonical]));
            for (let at = 0; at < count; at += 2048) {
              const part = rows.slice(at, at + 2048);
              const amount: FlatVector = {
                form: "flat",
                type: numeric,
                length: part.length,
                validity: null,
                data: {
                  layout: "decimal",
                  scale: null,
                  scales: Uint16Array.from(part, (r) => r.scale),
                  specials: null,
                  coefficients: { repr: "wide", values: part.map((r) => r.coefficient) },
                },
              };
              const data = part.map((row) =>
                kind === "BYTEA" ? row.data : utf8.encode(canonical.get(row.id) ?? ""),
              );
              await core.insertVectors(table, {
                length: part.length,
                selection: null,
                columns: [
                  ints(part.map((r) => r.id)),
                  ints(part.map((r) => r.k)),
                  amount,
                  varlen(payload, data),
                ],
              });
            }
            const governor = new Governor(64 << 20),
              active = new Set<Grant>();
            let minimum = 0;
            const admit = governor.tryAdmit.bind(governor);
            const admitSpy = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
              const grant = admit(options);
              if (grant && options.label !== "demand competitor") active.add(grant);
              return grant;
            });
            const grouped = governor.admitGroup.bind(governor);
            const groupSpy = vi
              .spyOn(governor, "admitGroup")
              .mockImplementation(async (options) => {
                if (options.label.endsWith("/fixed-buffers"))
                  minimum = options.grants.reduce((s, g) => s + g.minimumBytes, 0);
                const grants = await grouped(options);
                for (const grant of grants) active.add(grant);
                return grants;
              });
            const competitor: { grant: Grant | null } = { grant: null };
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Original is rebound below.
            const reserve = QueryRuntime.prototype.reserve;
            const pin = vi
              .spyOn(QueryRuntime.prototype, "reserve")
              .mockImplementation(async function (this: QueryRuntime, ...args) {
                await reserve.apply(this, args);
                if (this.request.governor === governor && !competitor.grant) {
                  competitor.grant = governor.tryAdmit({
                    label: "demand competitor",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  if (!competitor.grant) throw Error("pressure pin");
                  expect(
                    competitor.grant.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                }
              });
            const generic = createExecutor({ keyOrder: false, columnarSort: false });
            let sawDistinct = false,
              sawTies = false,
              sawSqlDistinct = false;
            const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              let id = 0;
              const visit = (node: Rel): void => {
                id = Math.max(id, node.id);
                if (node.op === "distinct") sawDistinct = true;
                if (node.op === "aggregate" && node.aggregates.length === 0) sawSqlDistinct = true;
                if (node.op === "sort" && node.withTies) sawTies = true;
                if ("input" in node) visit(node.input);
                if ("left" in node && "right" in node) {
                  visit(node.left);
                  visit(node.right);
                }
              };
              visit(plan.root);
              // SQL DISTINCT lowers to GROUP BY in engine/query.ts. Exercise its
              // equivalent native DISTINCT IR separately, without changing PG SQL.
              const distinct = (node: Rel): Rel => {
                if (node.op === "aggregate" && node.aggregates.length === 0) {
                  const projected: Rel = {
                    op: "project",
                    id: ++id,
                    input: node.input,
                    output: node.output,
                    exprs: node.groupBy.map((index) => ({
                      op: "column",
                      index,
                      type: node.input.output[index]?.type ?? { kind: "int32", bits: 32 },
                    })),
                  };
                  return { op: "distinct", id: node.id, input: projected, output: node.output };
                }
                if ("input" in node) return { ...node, input: distinct(node.input) };
                return node;
              };
              const chosen = path === "distinct" ? { ...plan, root: distinct(plan.root) } : plan;
              visit(chosen.root);
              return path === "native" ? createExecutor().prepare(chosen) : generic.prepare(chosen);
            });
            Object.assign(core.harness, { governor });
            const audit = allocationAudit(() => {
              let held = 0;
              for (const g of active) held += g.held;
              return Math.max(minimum, held);
            });
            const isDistinct = path === "distinct" || path === "sql-distinct";
            const projection = isDistinct
              ? kind === "JSON"
                ? "k,n,payload::text AS payload"
                : "k,n,payload"
              : "*";
            const sql =
              path === "ties"
                ? "SELECT k,n,payload FROM (SELECT k,n,payload FROM demand_boundary ORDER BY k OFFSET 2047 ROWS FETCH FIRST 2049 ROWS WITH TIES) q ORDER BY k"
                : `SELECT ${isDistinct ? "DISTINCT " : ""}${projection} FROM demand_boundary ORDER BY ${isDistinct ? "k" : "id"} LIMIT $1 OFFSET $2`;
            const params = path === "ties" ? [] : [2049, 2047];
            const normalize = (values: Array<Record<string, unknown>>) =>
              values.map((row) => ({
                ...row,
                payload:
                  kind === "BYTEA"
                    ? Array.from(row.payload as Uint8Array)
                    : typeof row.payload === "string"
                      ? (JSON.parse(row.payload) as unknown)
                      : row.payload,
              }));
            try {
              const actual = await core.execute(sql, params);
              if (!("batches" in actual)) throw Error("boundary result");
              const expected = (await pg.query<Record<string, unknown>>(sql, params)).rows;
              expect(normalize(oracleRows(actual))).toEqual(normalize(expected));
              if (expected.length) expect(audit.observedBytes).toBeGreaterThan(0);
              if (count === 8193 && isDistinct) {
                expect(expected.length).toBeGreaterThan(0);
                reach.reach("DISTINCT returns rows beyond offset");
              }
              expect(audit.completed.every((step) => step.actual <= step.credit)).toBe(true);
              if (path === "ties") expect(sawTies).toBe(true);
              if (path === "distinct") expect(sawDistinct).toBe(true);
              if (path === "sql-distinct") expect(sawSqlDistinct).toBe(true);
              expect(competitor.grant).not.toBeNull();
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              evidence.push({
                count,
                kind,
                path,
                width,
                coefficientDigits,
                minimumBytes: minimum,
                observedBackingBytes: audit.observedBytes,
                allocationFactories: audit.summary(),
                capBytes: governor.capBytes,
              });
              reach.reach(kind);
              reach.reach(path);
              reach.reach(
                count === 2047
                  ? "2047"
                  : count === 2048
                    ? "2048"
                    : count === 2049
                      ? "2049"
                      : count === 4097
                        ? "4097"
                        : "8193",
              );
              if (width >= 65535) reach.reach("wide payload");
              reach.reach("variable NUMERIC coefficients");
              reach.reach(
                coefficientDigits === 1
                  ? "safe coefficient source"
                  : coefficientDigits === 19
                    ? "int64 coefficient source"
                    : "int128 coefficient source",
              );
              reach.reach("LIMIT above batch");
              reach.reach("OFFSET crosses batch");
              reach.reach("independent backing credit");
              reach.reach("pinned pressure");
              reach.reach("PGlite exact rows");
            } finally {
              audit.close();
              prepare.mockRestore();
              pin.mockRestore();
              admitSpy.mockRestore();
              groupSpy.mockRestore();
              competitor.grant?.close();
              Object.assign(core.harness, { governor: owner });
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero final credit");
          },
        ),
        {
          seed,
          numRuns: 30,
          examples: counts.flatMap((count, i) =>
            kinds.map(
              (kind, j) =>
                [
                  count,
                  kind,
                  paths[(i + j) % 4],
                  i >= 3 ? 65535 : 8192,
                  coefficientWidths[(i + j) % 3] ?? 1,
                ] as [
                  (typeof counts)[number],
                  (typeof kinds)[number],
                  (typeof paths)[number],
                  number,
                  (typeof coefficientWidths)[number],
                ],
            ),
          ),
        },
      );
      reach.assertReached();
      if (process.env.MINNOW_B9_DEMAND_EVIDENCE)
        process.stdout.write(
          `B9_BOUNDARY_EVIDENCE ${JSON.stringify({ seed, histories: evidence })}\n`,
        );
    } finally {
      Object.assign(core.harness, { governor: owner });
      await core.close();
      await pg.close();
    }
  },
  900000,
);
