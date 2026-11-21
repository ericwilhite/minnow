/**
 * INSERT ... VALUES whose cells compute variable-size values (text concatenation, casts to text,
 * JSON constructors and casts) against PostgreSQL (PGlite), the authority for the stored rows.
 * The cells run on the VALUES row host (exec/values-row-host.ts); these tests also pin its memory
 * law under a small cap: charged before allocation, holder growth only at the `next()` boundary,
 * 53200 for a row above the usable cap, cancellable waits, and no extra admissions for many
 * small rows.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor, type GrantEvent } from "../memory/governor.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, VALUES_ROW_HOST_REACH } from "../../testing/reach.js";
import { RowBorrowInvariantError, ValuesRowHost } from "../exec/values-row-host.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

type State = (typeof VALUES_ROW_HOST_REACH)[number];
const MiB = 1 << 20;
/** 32 MiB cap (the write owner's floor needs about 18 MiB): a 4 MiB progress reserve, 28 MiB
 * usable by one admission. */
const CAP = 32 * MiB;
const USABLE = CAP - CAP / 8;

async function smallCore(capBytes = CAP): Promise<StorageCore> {
  const core = new StorageCore(32);
  Object.defineProperty(core.harness, "governor", { value: new Governor(capBytes) });
  return core.open();
}
const governorOf = (core: StorageCore): Governor => core.harness.governor;

/** Value widths, in characters: inside the row slack, just past it, and multi-MiB. */
const WIDTHS = [0, 7, 3000, 70_000, 1_500_000, 4_000_000] as const;
/** One computed cell: its SQL with `$n` placeholders, the parameters it binds and its reach. */
interface Cell {
  readonly text: string;
  readonly json: string;
  readonly params: readonly string[];
  readonly reach: readonly State[];
}
const text = (width: number, which: number): string => ("abcdefgh"[which % 8] ?? "a").repeat(width);
const cell = fc
  .record({
    width: fc.constantFrom(...WIDTHS),
    textKind: fc.constantFrom("literal", "concat", "cast", "param concat", "null concat"),
    jsonKind: fc.constantFrom("null", "jsonb literal", "build object", "to_jsonb"),
    which: fc.nat(7),
  })
  .map(({ width, textKind, jsonKind, which }): ((at: number) => Cell) => (at) => {
    const params: string[] = [];
    const param = (value: string) => {
      params.push(value);
      return `$${String(at + params.length)}`;
    };
    const reach: State[] = [];
    let textSql: string;
    switch (textKind) {
      case "literal":
        textSql = `'${text(Math.min(width, 3000), which)}'`;
        break;
      case "concat":
        textSql = `'${text(Math.min(width, 3000), which)}' || '-' || 'z'`;
        reach.push("concatenation");
        break;
      case "cast":
        textSql = `${String(width + which)}::text`;
        reach.push("cast to text");
        break;
      case "param concat":
        textSql = `${param(text(width, which))} || '!'`;
        reach.push("concatenation", "parameter");
        break;
      case "null concat":
        textSql = `NULL || 'x'`;
        reach.push("concatenation", "null propagated");
        break;
    }
    let jsonSql: string;
    switch (jsonKind) {
      case "null":
        jsonSql = "NULL";
        break;
      case "jsonb literal":
        jsonSql = `'{"k": [1, "${text(Math.min(width, 2000), which)}"]}'::jsonb`;
        reach.push("jsonb literal");
        break;
      case "build object":
        jsonSql = `jsonb_build_object('k', ${param(text(width, which + 1))} || 'x', 'n', ${String(which)})`;
        reach.push("json constructor", "parameter");
        break;
      case "to_jsonb":
        // Typed: PostgreSQL cannot resolve to_jsonb(anyelement) from an untyped parameter
        // (42804), which next/ accepts as text (reported separately).
        jsonSql = `to_jsonb(${param(text(width, which + 2))}::text)`;
        reach.push("json constructor", "parameter");
        break;
    }
    if (width >= 70_000 && params.length > 0) reach.push("row past the slack");
    if (width >= 1_500_000 && params.length > 0) reach.push("multi-MiB row");
    return { text: textSql, json: jsonSql, params, reach };
  });

for (const seed of seedsFor("next-values-row-host", [0x7a11, 0x7a12]))
  it(`INSERT ... VALUES computes cells like PostgreSQL under a small cap (seed ${String(seed)})`, async () => {
    const reach = createReach("values row host", VALUES_ROW_HOST_REACH);
    const core = await smallCore(),
      pg = new PGlite();
    const grown: string[] = [];
    const stop = governorOf(core).observeGrants((event: GrantEvent) => {
      if (event.label.includes("/values#") && event.label.endsWith("/row")) grown.push(event.label);
    });
    let id = 0,
      refusedNearCap = 0;
    try {
      for (const ddl of ["CREATE TABLE t(id INT PRIMARY KEY, v TEXT, j JSONB)"]) {
        await pg.exec(ddl);
        await core.execute(ddl);
      }
      await fc.assert(
        fc.asyncProperty(fc.array(cell, { minLength: 1, maxLength: 4 }), async (rows) => {
          reach.beginHistory();
          grown.length = 0;
          // Each run starts from an empty table on both sides, so one failure cannot leave
          // the two out of step for the runs fast-check shrinks after it.
          await pg.query("DELETE FROM t");
          await core.execute("DELETE FROM t").catch((error: unknown) => {
            throw new Error("next/ DELETE FROM t failed", { cause: error });
          });
          id = 0;
          const params: string[] = [];
          const tuples = rows.map((make) => {
            const c = make(params.length);
            params.push(...c.params);
            for (const state of c.reach) reach.reach(state);
            return `(${String(++id)}, ${c.text}, ${c.json})`;
          });
          reach.reach(rows.length > 1 ? "several rows" : "one row");
          const sql = `INSERT INTO t VALUES ${tuples.join(", ")}`;
          const total = params.reduce((sum, value) => sum + value.length, 0);
          const before = await core
            .execute("SELECT count(*) AS n FROM t")
            .catch((error: unknown) => {
              throw new Error("next/ count before failed", { cause: error });
            });
          await pg.query(sql, params).catch((error: unknown) => {
            throw new Error(`PostgreSQL refused ${sql.slice(0, 300)}`, { cause: error });
          });
          const result = await core.execute(sql, params).then(
            () => null,
            (error: unknown) => error,
          );
          if (result !== null) {
            // Only a statement whose values approach the cap may be refused, and only typed.
            expect(
              total,
              `${sql.slice(0, 200)} refused: ${(result as Error).message}`,
            ).toBeGreaterThan(USABLE / 8);
            expect(result, sql.slice(0, 200)).toMatchObject({ sqlState: "53200" });
            refusedNearCap++;
            await pg.query(`DELETE FROM t WHERE id > ${String(id - rows.length)}`);
            const after = await core.execute("SELECT count(*) AS n FROM t");
            if (!("batches" in before) || !("batches" in after)) throw new Error("rows");
            // Nothing of the refused statement was written.
            expect(oracleRows(after)).toEqual(oracleRows(before));
            return;
          }
          if (grown.length > 0) reach.reach("row grant admitted");
          const check = "SELECT id, v, j::text AS j FROM t ORDER BY id";
          const mine = await core.execute(check).catch((error: unknown) => {
            throw new Error(`next/ check failed after ${sql.slice(0, 200)}`, { cause: error });
          });
          if (!("batches" in mine)) throw new Error("rows");
          expect(
            oracleRows(mine).map((row) => row.id),
            sql.slice(0, 400),
          ).toEqual((await pg.query<{ id: number }>(check)).rows.map((row) => row.id));
          expect(oracleRows(mine)).toEqual((await pg.query(check)).rows);
        }),
        { seed, numRuns: 25 },
      );
      reach.assertReached();
      console.info(
        `values row host seed ${String(seed)}: ${String(refusedNearCap)} typed refusals`,
      );
    } finally {
      stop();
      await core.close();
      await pg.close();
    }
  }, 300_000);

/** GOVERNOR-WAIT-POLICY point 5: a row that only its own statement could make room for. */
it("refuses a row that could never be served beside its own statement, promptly, writing nothing", async () => {
  // A wait would hang; the stall guard turns it into a named failure instead.
  const stall = Governor.stallAfterMs;
  Governor.stallAfterMs = 1500;
  const core = await smallCore();
  const events: GrantEvent[] = [];
  const stop = governorOf(core).observeGrants((event) => events.push(event));
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'kept')");
    events.length = 0;
    // A 2 MiB parameter concatenated four times: the row needs about 10 MiB, below the usable
    // cap (28 MiB), but this statement and its writer hold about 19 MiB they cannot release
    // until the row is served.
    const started = performance.now();
    const refusal: unknown = await core
      .execute("INSERT INTO t VALUES (2, 'a' || 'b'), (3, $1 || $1 || $1 || $1)", [
        "w".repeat(2 * MiB),
      ])
      .then(
        () => undefined,
        (error: unknown) => error,
      );
    expect(refusal).toMatchObject({ sqlState: "53200" });
    expect((refusal as Error).message).toContain("values#");
    expect(performance.now() - started).toBeLessThan(1000);
    // Refused before any wait: no row grant was asked for.
    expect(events.filter((event) => event.label.endsWith("/row"))).toEqual([]);
    const rows = await core.execute("SELECT id, v FROM t ORDER BY id");
    if (!("batches" in rows)) throw new Error("rows");
    expect(oracleRows(rows)).toEqual([{ id: 1, v: "kept" }]);
    // The engine is still usable.
    await core.execute("INSERT INTO t VALUES (4, 'x' || 'y')");
    const after = await core.execute("SELECT v FROM t WHERE id = 4");
    if (!("batches" in after)) throw new Error("rows");
    expect(oracleRows(after)).toEqual([{ v: "xy" }]);
  } finally {
    stop();
    Governor.stallAfterMs = stall;
    await core.close();
  }
});

it("waits for, and is served after, another statement that holds the room it needs", async () => {
  const stall = Governor.stallAfterMs;
  Governor.stallAfterMs = 5000;
  // 64 MiB: room for the row beside this statement's own holdings, once reclaim has run.
  const core = await smallCore(64 * MiB);
  const governor = governorOf(core);
  let competitor: ReturnType<typeof governor.tryAdmit> = null;
  let waited = false;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    const grow = governor.grow.bind(governor);
    // The competitor arrives when the row first grows the VALUES operator's grant past its
    // slack: it takes everything free but 1 MiB, so the row has to wait, then finishes 300 ms
    // later.
    governor.grow = (grant, bytes) => {
      if (competitor === null && grant.label.includes("/values#") && bytes >= 256 * 1024) {
        competitor = governor.tryAdmit({
          label: "competitor",
          minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes - MiB,
          spillable: false,
        });
        setTimeout(() => {
          waited = true;
          competitor?.close();
        }, 300);
      }
      return grow(grant, bytes);
    };
    const rowGrants: GrantEvent[] = [];
    const observe = governor.observeGrants((event) => {
      if (event.label.endsWith("/row")) rowGrants.push(event);
    });
    const value = "p".repeat(MiB);
    await core.execute("INSERT INTO t VALUES (1, $1 || $1 || $1 || $1 || $1 || $1 || $1 || $1)", [
      value,
    ]);
    observe();
    expect(competitor).not.toBeNull();
    // It waited once, as holder growth for its row grant. Each later measured retry learned a
    // larger need and was admitted at once.
    expect(rowGrants[0]?.source).toBe("holder");
    expect(rowGrants.slice(1).every((event) => event.source === "tryAdmit")).toBe(true);
    // Served only after the competitor finished, and correctly.
    expect(waited).toBe(true);
    const rows = await core.execute("SELECT v FROM t");
    if (!("batches" in rows)) throw new Error("rows");
    const stored = oracleRows(rows).map((row) => row.v);
    expect(stored.length).toBe(1);
    expect(stored[0] === value.repeat(8)).toBe(true);
  } finally {
    Governor.stallAfterMs = stall;
    await core.close();
  }
});

it("ends a row-room wait on the statement's signal, leaking no grant", async () => {
  const core = await smallCore();
  const governor = governorOf(core);
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    const baseline = governor.usedBytes;
    // Another holder takes all but 2 MiB of what is free, so a 4 MiB row grant has to wait.
    const blocker = await governor.admit({
      label: "test blocker",
      minimumBytes: USABLE - governor.usedBytes - 2 * MiB,
      spillable: false,
    });
    const controller = new AbortController();
    let pending = true;
    const statement = core
      .execute("INSERT INTO t VALUES (1, $1 || '!')", ["q".repeat(4 * MiB)], controller.signal)
      .finally(() => {
        pending = false;
      });
    statement.catch(() => undefined);
    // The row grant cannot be admitted beside the blocker, so the statement waits.
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(pending).toBe(true);
    const aborted = performance.now();
    controller.abort(new Error("cancelled by test"));
    await expect(statement).rejects.toMatchObject({ sqlState: "57014" });
    expect(performance.now() - aborted).toBeLessThan(1000);
    blocker.close();
    // Every byte the statement held came back: no row grant outlived the cancellation.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(governor.usedBytes).toBe(baseline);
    const rows = await core.execute("SELECT count(*) AS n FROM t");
    if (!("batches" in rows)) throw new Error("rows");
    expect(oracleRows(rows).map((row) => Number(row.n))).toEqual([0]);
  } finally {
    await core.close();
  }
});

it("admits no more grants for a 1,000-row INSERT of small computed cells than of literals", async () => {
  const counts: number[] = [];
  for (const value of [
    (i: number) => `'v${String(i)}'`,
    (i: number) => `'v' || ${String(i)}::text`,
  ]) {
    // A fresh database each time: a second INSERT into a warmer store would grant differently.
    const core = await smallCore();
    try {
      await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
      let grants = 0;
      const stop = governorOf(core).observeGrants(() => grants++);
      const tuples = Array.from({ length: 1000 }, (_, i) => `(${String(i)}, ${value(i)})`);
      await core.execute(`INSERT INTO t VALUES ${tuples.join(", ")}`);
      stop();
      counts.push(grants);
      const rows = await core.execute("SELECT count(*) AS n FROM t WHERE v = 'v' || id::text");
      if (!("batches" in rows)) throw new Error("rows");
      expect(oracleRows(rows).map((row) => Number(row.n))).toEqual([1000]);
    } finally {
      await core.close();
    }
  }
  expect(counts[1]).toBeLessThanOrEqual(counts[0] ?? 0);
});

it("refuses to grow row room while the previous row is still borrowed", async () => {
  const host = new ValuesRowHost();
  host.bind({
    grant: {
      allocate: () => true,
      free: () => undefined,
    } as never,
    governor: { capBytes: CAP, reserveBytes: CAP / 8 } as never,
    signal: new AbortController().signal,
    label: "values#test",
    admitRow: () => Promise.reject(new Error("no growth in this test")),
    ownGrants: () => [],
  });
  const batch = { length: 1, columns: [], selection: null };
  await host.row([], batch, new AbortController().signal);
  await expect(host.row([], batch, new AbortController().signal)).rejects.toBeInstanceOf(
    RowBorrowInvariantError,
  );
  host.endBorrow();
  await expect(host.row([], batch, new AbortController().signal)).resolves.toEqual([]);
});
