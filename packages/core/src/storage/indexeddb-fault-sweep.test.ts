/**
 * Fault sweep over the IndexedDB store through the engine. For every kind of write, the k-th
 * IndexedDB write request (put/add/delete, any store, any transaction) is made to fail — by
 * aborting its transaction, by a QuotaExceededError, or by a "crash" (abort, then every
 * in-memory layer thrown away and the database reopened cold). After each injected failure:
 * the logical state equals the shadow model as it was before the operation, integrity is clean,
 * and the same operation then succeeds without injection and matches the shadow afterwards.
 *
 * `k` runs to `MINNOW_FAULT_SWEEP_DEPTH` per kind (default 8 — about half a minute for all three modes); set
 * it to 48 for the full audit sweep, which passes every write request of every kind.
 */
import { describe, expect, it, vi } from "vitest";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import { openEngine, reopenEngine, type OpenedEngine } from "./indexeddb-audit-helpers.js";
import { UniqueKeyConflictError } from "./types.js";

vi.setConfig({ testTimeout: heavyTestTimeout(900_000) });

const DEPTH = Number(process.env.MINNOW_FAULT_SWEEP_DEPTH ?? 8);

interface Row {
  id: number;
  name: string;
  qty: number;
}
type Shadow = Map<number, Row>;
type Mode = "abort" | "quota" | "crash";

interface OpKind {
  name: string;
  /** Runs variant n and returns the shadow after success; leaves `shadow` alone on failure. */
  run(ctx: OpenedEngine, shadow: Shadow, n: number): Promise<Shadow>;
  addsColumn?: boolean;
  dropsColumn?: boolean;
}

async function readRows(ctx: OpenedEngine): Promise<Row[]> {
  const result = await ctx.db.query("SELECT id, name, qty FROM items ORDER BY id", {
    memoize: false,
  });
  return result.rows.map(({ id, name, qty }) => {
    if (typeof id !== "number" || typeof name !== "string" || typeof qty !== "number") {
      throw new Error("Unexpected item row shape");
    }
    return { id, name, qty };
  });
}

function shadowRows(shadow: Shadow): Row[] {
  return [...shadow.values()].sort((left, right) => left.id - right.id);
}

async function columnsOf(ctx: OpenedEngine): Promise<string[]> {
  const result = await ctx.db.query("SELECT * FROM items LIMIT 1", { memoize: false });
  return [...result.columns].sort();
}

function pickExisting(shadow: Shadow, n: number): number | undefined {
  const ids = [...shadow.keys()].sort((left, right) => left - right);
  return ids.length === 0 ? undefined : ids[n % ids.length];
}

let nextId = 1;
function freshIds(count: number): number[] {
  const ids = Array.from({ length: count }, (_, index) => nextId + index);
  nextId += count;
  return ids;
}

const kinds: OpKind[] = [
  {
    name: "insertBatch",
    async run(ctx, shadow) {
      const rows = freshIds(10).map((id) => ({ id, name: `b${String(id)}`, qty: id % 7 }));
      await ctx.db.insertBatch("items", rows);
      const next = new Map(shadow);
      for (const row of rows) next.set(row.id, row);
      return next;
    },
  },
  {
    name: "insertSql",
    async run(ctx, shadow) {
      const [id] = freshIds(1);
      if (id === undefined) throw new Error("no id");
      await ctx.db.execute(
        `INSERT INTO items (id, name, qty) VALUES (${String(id)}, 's${String(id)}', 3)`,
      );
      return new Map(shadow).set(id, { id, name: `s${String(id)}`, qty: 3 });
    },
  },
  {
    name: "updateSql",
    async run(ctx, shadow, n) {
      const id = pickExisting(shadow, n);
      if (id === undefined) return shadow;
      await ctx.db.execute(`UPDATE items SET qty = qty + 1 WHERE id = ${String(id)}`);
      const row = shadow.get(id);
      if (row === undefined) throw new Error("shadow row missing");
      return new Map(shadow).set(id, { ...row, qty: row.qty + 1 });
    },
  },
  {
    name: "deleteSql",
    async run(ctx, shadow, n) {
      const id = pickExisting(shadow, n * 3 + 1);
      if (id === undefined) return shadow;
      await ctx.db.execute(`DELETE FROM items WHERE id = ${String(id)}`);
      const next = new Map(shadow);
      next.delete(id);
      return next;
    },
  },
  {
    name: "upsertSql",
    async run(ctx, shadow, n) {
      const id = pickExisting(shadow, n * 5 + 2);
      if (id === undefined) return shadow;
      const row = shadow.get(id);
      if (row === undefined) throw new Error("shadow row missing");
      await ctx.db.execute(
        `INSERT INTO items (id, name, qty) VALUES (${String(id)}, '${row.name}', 77) ON CONFLICT(id) DO UPDATE SET qty = excluded.qty`,
      );
      return new Map(shadow).set(id, { ...row, qty: 77 });
    },
  },
  {
    name: "rekeyUpdate",
    async run(ctx, shadow, n) {
      const id = pickExisting(shadow, n * 7 + 3);
      if (id === undefined) return shadow;
      const [newId] = freshIds(1);
      if (newId === undefined) throw new Error("no id");
      await ctx.db.execute(`UPDATE items SET id = ${String(newId)} WHERE id = ${String(id)}`);
      const next = new Map(shadow);
      const row = next.get(id);
      if (row === undefined) throw new Error("shadow row missing");
      next.delete(id);
      return next.set(newId, { ...row, id: newId });
    },
  },
  {
    name: "statementScope",
    async run(ctx, shadow, n) {
      const [a, b] = freshIds(2);
      const victim = pickExisting(shadow, n * 11 + 5);
      if (a === undefined || b === undefined) throw new Error("no id");
      let began = false;
      try {
        await ctx.db.execute("BEGIN");
        began = true;
        await ctx.db.execute(
          `INSERT INTO items (id, name, qty) VALUES (${String(a)}, 'x${String(a)}', 1)`,
        );
        await ctx.db.execute(
          `INSERT INTO items (id, name, qty) VALUES (${String(b)}, 'x${String(b)}', 2)`,
        );
        if (victim !== undefined) {
          await ctx.db.execute(`UPDATE items SET qty = 50 WHERE id = ${String(victim)}`);
        }
        await ctx.db.execute("COMMIT");
      } catch (error) {
        if (began) await ctx.db.execute("ROLLBACK").catch(() => undefined);
        throw error;
      }
      const next = new Map(shadow);
      next.set(a, { id: a, name: `x${String(a)}`, qty: 1 });
      next.set(b, { id: b, name: `x${String(b)}`, qty: 2 });
      if (victim !== undefined) {
        const row = next.get(victim);
        if (row === undefined) throw new Error("shadow row missing");
        next.set(victim, { ...row, qty: 50 });
      }
      return next;
    },
  },
  {
    name: "addColumn",
    addsColumn: true,
    async run(ctx, shadow, n) {
      await ctx.db.execute(`ALTER TABLE items ADD COLUMN extra${String(n)} INTEGER DEFAULT 0`);
      return shadow;
    },
  },
  {
    name: "dropColumn",
    dropsColumn: true,
    async run(ctx, shadow) {
      const extra = (await columnsOf(ctx)).filter((column) => column.startsWith("extra")).at(-1);
      if (extra === undefined) return shadow;
      await ctx.db.execute(`ALTER TABLE items DROP COLUMN ${extra}`);
      return shadow;
    },
  },
  {
    name: "compact",
    async run(ctx, shadow) {
      await ctx.db.compactTable("items", { minimumLevel0Segments: 1 });
      return shadow;
    },
  },
  {
    name: "collect",
    async run(ctx, shadow) {
      await ctx.db.collectGarbage();
      return shadow;
    },
  },
  {
    name: "duplicateInsert",
    async run(ctx, shadow, n) {
      const id = pickExisting(shadow, n);
      if (id === undefined) return shadow;
      const refused = await ctx.db
        .execute(`INSERT INTO items (id, name, qty) VALUES (${String(id)}, 'dup', 1)`)
        .then(
          () => undefined,
          (error: unknown) => error,
        );
      if (refused === undefined) throw new Error("duplicate primary key was admitted");
      if (!(refused instanceof Error)) {
        throw new Error("duplicate primary key rejected with a non-error", { cause: refused });
      }
      if (
        !(refused instanceof UniqueKeyConflictError) &&
        !/unique|duplicate/i.test(refused.message)
      ) {
        throw refused;
      }
      return shadow;
    },
  },
];

async function verifyState(ctx: OpenedEngine, shadow: Shadow, label: string): Promise<string[]> {
  expect(await readRows(ctx), `${label}: rows`).toEqual(shadowRows(shadow));
  expect((await ctx.store.checkIntegrity()).issues, `${label}: integrity`).toEqual([]);
  return columnsOf(ctx);
}

async function raceHang<T>(
  promise: Promise<T>,
): Promise<{ hung: boolean; value?: T; error?: unknown }> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const settled = promise.then(
    (value) => ({ hung: false, value }),
    (error: unknown) => ({ hung: false, error }),
  );
  const hang = new Promise<{ hung: boolean }>((resolve) => {
    timer = setTimeout(() => resolve({ hung: true }), 30_000);
  });
  try {
    return await Promise.race([settled, hang]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function sweep(mode: Mode): Promise<void> {
  let ctx = await openEngine();
  await ctx.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");
  await ctx.db.execute("CREATE UNIQUE INDEX items_name ON items(name)");
  let shadow: Shadow = new Map();
  const seed = kinds[0];
  if (seed === undefined) throw new Error("no kinds");
  shadow = await seed.run(ctx, shadow, 0);
  shadow = await seed.run(ctx, shadow, 1);
  let columns = await columnsOf(ctx);
  const failures: string[] = [];
  let variant = 0;
  for (const kind of kinds) {
    for (let k = 0; k < DEPTH; k += 1) {
      variant += 1;
      const n = variant;
      const injection = { fired: false };
      let writes = 0;
      ctx.instrumented.setHook((info) => {
        if (info.method !== "put" && info.method !== "add" && info.method !== "delete") {
          return undefined;
        }
        if (injection.fired) return undefined;
        if (writes === k) {
          injection.fired = true;
          return mode === "quota" ? "throw-quota" : "abort";
        }
        writes += 1;
        return undefined;
      });
      const before = shadow;
      const columnsBefore = columns;
      const outcome = await raceHang(kind.run(ctx, shadow, n));
      ctx.instrumented.setHook(undefined);
      const label = `${mode}/${kind.name}/k=${String(k)}`;
      const expectedColumns = (): string[] =>
        kind.addsColumn
          ? [...columnsBefore, `extra${String(n)}`].sort()
          : kind.dropsColumn
            ? columnsBefore.filter(
                (column) => column !== columnsBefore.filter((c) => c.startsWith("extra")).at(-1),
              )
            : columnsBefore;
      if (outcome.hung) {
        failures.push(`${label}: hung for 30s after the injected failure`);
        ctx = await reopenEngine(ctx);
        shadow = new Map((await readRows(ctx)).map((row) => [row.id, row]));
        columns = await columnsOf(ctx);
        continue;
      }
      if (!injection.fired) {
        // Fewer than k+1 writes: this kind's sweep is complete.
        if ("error" in outcome) {
          failures.push(`${label}: failed without injection: ${String(outcome.error)}`);
        } else {
          shadow = outcome.value ?? shadow;
          columns = expectedColumns();
        }
        break;
      }
      if (mode === "crash") ctx = await reopenEngine(ctx);
      if (!("error" in outcome)) {
        // Survived the injected failure through an internal retry: the state is the "after".
        shadow = outcome.value ?? shadow;
        columns = expectedColumns();
        try {
          expect(await verifyState(ctx, shadow, `${label} tolerated`)).toEqual(columns);
        } catch (error) {
          failures.push(`${label}: tolerated the failure but the state is wrong: ${String(error)}`);
        }
        continue;
      }
      try {
        expect(await verifyState(ctx, before, `${label} after failure`)).toEqual(columnsBefore);
      } catch (error) {
        failures.push(
          `${label}: state after failure differs from before: ${String(error)}\n  op: ${String(outcome.error)}`,
        );
        shadow = new Map((await readRows(ctx)).map((row) => [row.id, row]));
        columns = await columnsOf(ctx);
        continue;
      }
      try {
        shadow = await kind.run(ctx, before, n);
        columns = expectedColumns();
        expect(await verifyState(ctx, shadow, `${label} after retry`)).toEqual(columns);
      } catch (error) {
        failures.push(
          `${label}: retry failed: ${String(error)}\n  first: ${String(outcome.error)}`,
        );
        shadow = new Map((await readRows(ctx)).map((row) => [row.id, row]));
        columns = await columnsOf(ctx);
      }
    }
  }
  ctx = await reopenEngine(ctx);
  expect(await verifyState(ctx, shadow, `${mode}/cold reopen`)).toEqual(columns);
  await ctx.db.compactTable("items", { minimumLevel0Segments: 1 });
  await ctx.db.collectGarbage();
  expect(await verifyState(ctx, shadow, `${mode}/after maintenance`)).toEqual(columns);
  await ctx.db.close();
  ctx.store.close();
  expect(failures, failures.join("\n")).toEqual([]);
}

describe("IndexedDB fault sweep through the engine", () => {
  it("abort at every write request", async () => {
    await sweep("abort");
  });
  it("QuotaExceededError at every write request", async () => {
    await sweep("quota");
  });
  it("crash (abort, then cold reopen) at every write request", async () => {
    await sweep("crash");
  });
});
