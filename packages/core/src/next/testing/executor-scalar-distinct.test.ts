/** COUNT(DISTINCT) ignores NULL and uses SQL equality, independent of membership storage. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { createExecutor } from "../exec/create.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { inputBatch } from "./executor-input.js";
import { BATCH_ROWS, type FlatVector } from "../vector/vector.js";

it.each(seedsFor("next-executor-scalar-distinct", [0xe355, 0xe356]))(
  "matches PostgreSQL/current with byte membership and generic controls (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite(),
      current = await currentEngine("memory");
    const reach = createReach("SQL scalar byte distinct", [
      "memory",
      "stored",
      "mixed",
      "empty",
      "filter",
      "null",
      "collision",
      "uuid",
      "generic",
      "control",
      "lower-memory",
    ]);
    await pg.waitReady;
    try {
      const rows = Array.from({ length: 5121 }, (_, i) => ({
        t:
          i % 29 === 28
            ? null
            : i % 701 === 0
              ? "collision-10520"
              : i % 701 === 1
                ? "collision-192355"
                : i % 701 === 2
                  ? ""
                  : `${String(i % 701)}-α-O'Brien-${"z".repeat((i % 701) % 71)}`,
        u:
          i % 31 === 30
            ? null
            : `00000000-0000-0000-0000-${(i % 701).toString(16).padStart(12, "0")}`,
        n: i % 17 === 16 ? null : `${String(i % 257)}.${i % 2 ? "50" : "5"}`,
        v: i % 19 === 18 ? null : i % 257,
      }));
      // The generic parameter binder intentionally refuses UUID. Supply its native vector to
      // the ordinary statement writer, with all input buffers charged outside query timing.
      const insert = async (name: string, values: typeof rows) => {
        const def = core.harness.path.catalog.tableByName(name);
        if (!def) throw new Error("fixture table");
        const grant = await core.harness.governor.admit({
          label: "fixture/input",
          minimumBytes: 1 << 20,
          spillable: true,
        });
        try {
          await core.write(async (statement) => {
            const writer = statement.table(def.id);
            for (let from = 0; from < values.length; from += BATCH_ROWS) {
              const part = values.slice(from, from + BATCH_ROWS),
                n = part.length,
                bytesHeld = 16 * n + 4 * ((n + 31) >>> 5);
              const input = inputBatch(
                { ...def, columns: def.columns.filter((c) => c.name !== "u") },
                { t: part.map((r) => r.t), n: part.map((r) => r.n), v: part.map((r) => r.v) },
                grant,
              );
              if (!grant.allocate(bytesHeld)) {
                input.close();
                throw new Error("fixture UUID grant");
              }
              try {
                const bytes = new Uint8Array(16 * n),
                  validity = new Uint32Array((n + 31) >>> 5);
                part.forEach((r, i) => {
                  if (r.u === null) return;
                  validity[i >>> 5] = (validity[i >>> 5] ?? 0) | (1 << (i & 31));
                  const hex = r.u.replaceAll("-", "");
                  for (let b = 0; b < 16; b++)
                    bytes[i * 16 + b] = parseInt(hex.slice(b * 2, b * 2 + 2), 16);
                });
                const u: FlatVector = {
                  form: "flat",
                  type: { kind: "uuid" },
                  length: n,
                  validity,
                  data: { layout: "fixed16", bytes },
                };
                const columns = [...input.batch.columns];
                columns.splice(1, 0, u);
                expect(await writer.insert({ ...input.batch, columns }, null)).toBe(n);
              } finally {
                grant.free(bytesHeld);
                input.close();
              }
            }
          });
        } finally {
          grant.close();
        }
      };
      for (const name of ["m", "s", "x", "e"]) {
        const sql = `CREATE TABLE ${name}(t TEXT,u UUID,n NUMERIC(12,2),v DOUBLE PRECISION)`;
        await core.execute(sql);
        await pg.exec(sql);
        await current.run({ kind: "sql", sql });
        if (name === "e") continue;
        await insert(name, name === "x" ? rows.slice(0, 4096) : rows);
        await current.run({ kind: "insertBatch", table: name, rows });
        for (let from = 0; from < rows.length; from += 128) {
          const part = rows.slice(from, from + 128);
          await pg.query(
            `INSERT INTO ${name} VALUES ${part.map((_, k) => `($${String(k * 4 + 1)},$${String(k * 4 + 2)},$${String(k * 4 + 3)},$${String(k * 4 + 4)})`).join(",")}`,
            part.flatMap((r) => [r.t, r.u, r.n, r.v]),
          );
        }
      }
      // Flush s/x, then add m and x's suffix to produce genuine memory/stored/mixed reads.
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      // m was flushed too; use a fresh memory table with the same contents.
      await core.execute("CREATE TABLE mem(t TEXT,u UUID,n NUMERIC(12,2),v DOUBLE PRECISION)");
      await insert("mem", rows);
      await insert("x", rows.slice(4096));
      for (const [name, state] of [
        ["mem", "memory"],
        ["s", "stored"],
        ["x", "mixed"],
      ] as const) {
        const def = core.harness.path.catalog.tableByName(name);
        if (!def) throw new Error("table");
        const pin = core.harness.path.pin(),
          grant = await core.harness.governor.admit({
            label: "fixture/state",
            minimumBytes: 1 << 20,
            spillable: true,
          });
        try {
          const kinds = new Set(
            pin
              .table(def.id, { budget: grant })
              .segments.filter((s) => s.rowCount > 0)
              .map((s) => s.kind),
          );
          expect(kinds.has("memory")).toBe(state !== "stored");
          expect(kinds.has("group")).toBe(state !== "memory");
        } finally {
          pin.release();
          grant.close();
        }
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (state, filtered, generic) => {
            reach.beginHistory();
            const table = ["m", "s", "x", "e"][state];
            if (!table) throw new Error("fixture");
            const select = `COUNT(DISTINCT t) AS nt,COUNT(DISTINCT u) AS nu${filtered ? ",COUNT(DISTINCT t) FILTER (WHERE v>100) AS nf" : ""}${generic ? ",COUNT(DISTINCT n) AS nn,COUNT(DISTINCT v) AS nv,SUM(DISTINCT v) AS sv" : ""}`;
            const sql = `SELECT ${select} FROM ${table}`,
              nextSql = `SELECT ${select} FROM ${state === 0 ? "mem" : table}`;
            const expected = (await pg.query<Record<string, unknown>>(sql)).rows,
              old = await current.run({ kind: "sql", sql });
            expect(old.kind).toBe("rows");
            expect(old.rows).toEqual(expected);
            const result = await core.execute(nextSql);
            if (!("batches" in result)) throw new Error("SELECT");
            expect(oracleRows(result)).toEqual(expected);
            const candidate = core.lastProfile?.operators.find(
              (o) => o.op === "aggregate",
            )?.peakHeldBytes;
            const saved = core.executor.prepare.bind(core.executor);
            try {
              const control = createExecutor({ scalarByteDistinct: false });
              core.executor.prepare = control.prepare.bind(control);
              const result = await core.execute(nextSql);
              if (!("batches" in result)) throw new Error("control SELECT");
              expect(oracleRows(result)).toEqual(expected);
              const before = core.lastProfile?.operators.find(
                (o) => o.op === "aggregate",
              )?.peakHeldBytes;
              if (state !== 3) {
                expect(candidate).toBeDefined();
                expect(before).toBeDefined();
                expect(
                  candidate ?? Infinity,
                  "scalar byte membership must avoid value-id/output interning",
                ).toBeLessThan(before ?? 0);
                reach.reach("lower-memory");
                reach.reach("null");
                reach.reach("collision");
              }
            } finally {
              core.executor.prepare = saved;
            }
            reach.reach((["memory", "stored", "mixed", "empty"] as const)[state] ?? "empty");
            if (filtered) reach.reach("filter");
            if (generic) reach.reach("generic");
            reach.reach("uuid");
            reach.reach("control");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [0, true, true],
            [1, false, true],
            [2, true, false],
            [3, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await current.close();
      await pg.close();
      await core.close();
      expect(core.harness.governor.usedBytes).toBe(0);
      expect(core.spill.bytesInUse).toBe(0);
    }
  },
  120000,
);
