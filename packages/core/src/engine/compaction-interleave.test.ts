/**
 * Stepped compaction under interference. A fold advanced one block at a time (`maxBlocks: 1`)
 * is interleaved with inserts, keyed and predicate updates, deletes, rekeys and upserts from the
 * same connection and a peer, with DROP/ADD COLUMN and DROP/CREATE INDEX mid-job, with the peer
 * folding the whole table (retiring this job's sources) or stepping the same job, and with this
 * connection closing and reopening mid-job to resume through `listCompactionJobs`. After every
 * round the table must equal the shadow model on both connections by scan, by index read, by
 * UNIQUE lookup and through retired keys' forwards. A job that lost its sources or its schema
 * may refuse to resume, but never publish a wrong table.
 *
 * `MINNOW_SEED=<n>` replays one seed; `MINNOW_ORACLE_ROUNDS` widens a run.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";
import { MinnowDatabase, type CompactionJobProgress } from "./database.js";
import { UniqueConstraintError } from "./errors.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/** A map entry the model guarantees is present. */
function must<T>(value: T | undefined, what: string): T {
  if (value === undefined) throw new Error(`${what} is missing`);
  return value;
}

const int = (r: () => number, n: number): number => Math.floor(r() * n);
const pick = <T>(r: () => number, xs: readonly T[]): T => xs[int(r, xs.length)] as T;
interface Row {
  id: number;
  a: number;
  b: string | null;
  d: string;
}

/** Refusals a job may legitimately meet when its sources, schema or ownership moved under it. */
const TOLERATED =
  /Schema changed|SchemaConflict|CompactionJobConflict|TransactionRecordConflict|CompactionJobCancelled|Compaction job not found|TableRecordConflict|WriteConflict|catalog epoch|retired|superseded|outside the transaction snapshot|UniqueIndexCoverage/i;

interface Outcome {
  readonly problems: string[];
  readonly published: number;
  readonly steps: number;
}

async function runRounds(seed: number, rounds: number): Promise<Outcome> {
  const r = mulberry32(seed);
  const store: BlockStore = new MemoryBlockStore();
  const opts = { rowsPerBlock: 4, autoCompact: false, compaction: { partitionRows: 64 } } as const;
  let db = new MinnowDatabase(store, opts);
  const peer = new MinnowDatabase(store, opts);
  const problems: string[] = [];
  const model = new Map<number, Row>();
  /** Retired key -> the key it forwards to. */
  const forwards = new Map<number, number>();
  let next = 1;
  let hasB = true;
  let hasIndexA = true;
  await db.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT, d TEXT NOT NULL)",
  );
  await db.execute("CREATE INDEX ia ON t(a)");
  await db.execute("CREATE UNIQUE INDEX ud ON t(d)");
  const cols = (): string[] => ["id", "a", ...(hasB ? ["b"] : []), "d"];
  const lit = (v: number | string | null | undefined): string =>
    v === null || v === undefined ? "NULL" : typeof v === "number" ? String(v) : `'${v}'`;
  const insertRows = async (conn: MinnowDatabase, n: number): Promise<void> => {
    const rows: Row[] = [];
    for (let i = 0; i < n; i += 1) {
      const id = next++;
      rows.push({
        id,
        a: int(r, 10),
        b: r() < 0.2 ? null : `b${String(int(r, 50))}`,
        d: `d${String(id)}`,
      });
    }
    await conn.execute(
      `INSERT INTO t (${cols().join(",")}) VALUES ${rows
        .map(
          (x) =>
            `(${cols()
              .map((c) => lit((x as unknown as Record<string, number | string | null>)[c]))
              .join(",")})`,
        )
        .join(",")}`,
    );
    for (const x of rows) model.set(x.id, hasB ? x : { ...x, b: null });
  };
  for (let i = 0; i < 30; i += 1) await insertRows(db, 10);
  const current = (id: number): number => {
    let c = id;
    while (forwards.has(c)) c = must(forwards.get(c), "forward");
    return c;
  };

  const mutate = async (conn: MinnowDatabase, label: string[]): Promise<void> => {
    const roll = r();
    const ids = [...model.keys()];
    try {
      if (roll < 0.25) {
        label.push("insert");
        await insertRows(conn, 1 + int(r, 4));
      } else if (roll < 0.5 && ids.length > 0) {
        const id = pick(r, ids);
        const a = int(r, 10);
        const viaRetired =
          r() < 0.2 ? [...forwards.keys()].find((k) => current(k) === id) : undefined;
        label.push(`update ${String(viaRetired ?? id)}`);
        await conn.execute(
          `UPDATE t SET a = ${String(a)}${hasB ? `, b = 'u${String(a)}'` : ""} WHERE id = ${String(viaRetired ?? id)}`,
        );
        const row = must(model.get(id), "row");
        model.set(id, { ...row, a, b: hasB ? `u${String(a)}` : row.b });
      } else if (roll < 0.65 && ids.length > 0) {
        const id = pick(r, ids);
        label.push(`delete ${String(id)}`);
        await conn.execute(`DELETE FROM t WHERE id = ${String(id)}`);
        model.delete(id);
      } else if (roll < 0.75 && ids.length > 0) {
        const id = pick(r, ids);
        const to = next++;
        label.push(`rekey ${String(id)}->${String(to)}`);
        await conn.execute(`UPDATE t SET id = ${String(to)} WHERE id = ${String(id)}`);
        const row = must(model.get(id), "row");
        model.delete(id);
        model.set(to, { ...row, id: to });
        forwards.set(id, to);
      } else if (roll < 0.85) {
        const id = r() < 0.5 && ids.length > 0 ? pick(r, ids) : next++;
        const a = int(r, 10);
        label.push(`upsert ${String(id)}`);
        await conn.execute(
          `INSERT INTO t (${cols().join(",")}) VALUES (${[id, a, ...(hasB ? ["x"] : []), `d${String(id)}`].map(lit).join(",")}) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, d = EXCLUDED.d`,
        );
        const prev = model.get(id);
        model.set(id, {
          id,
          a,
          b: prev === undefined ? (hasB ? "x" : null) : prev.b,
          d: `d${String(id)}`,
        });
        if (id >= next) next = id + 1;
      } else if (roll < 0.9) {
        label.push("UPDATE a=a+1 WHERE a=3");
        await conn.execute("UPDATE t SET a = a + 1 WHERE a = 3");
        for (const row of model.values()) if (row.a === 3) row.a = 4;
      } else if (roll < 0.95) {
        if (hasB) {
          label.push("DROP COLUMN b");
          await conn.execute("ALTER TABLE t DROP COLUMN b");
          hasB = false;
          for (const row of model.values()) row.b = null;
        } else {
          label.push("ADD COLUMN b");
          await conn.execute("ALTER TABLE t ADD COLUMN b TEXT");
          hasB = true;
        }
      } else if (hasIndexA) {
        label.push("DROP INDEX ia");
        await conn.execute("DROP INDEX ia");
        hasIndexA = false;
      } else {
        label.push("CREATE INDEX ia");
        await conn.execute("CREATE INDEX ia ON t(a)");
        hasIndexA = true;
      }
    } catch (error) {
      if (error instanceof UniqueConstraintError) {
        label.push(`(unique refused: ${String(error).slice(0, 60)})`);
        return;
      }
      throw error;
    }
  };

  const verify = async (where: string): Promise<void> => {
    const want = [...model.values()]
      .map((x) => (hasB ? x : { id: x.id, a: x.a, d: x.d }))
      .sort((l, rr) => l.id - rr.id);
    for (const conn of [db, peer]) {
      const who = conn === db ? "db" : "peer";
      const got = (
        await conn.query(`SELECT ${cols().join(",")} FROM t ORDER BY id`, { memoize: false })
      ).rows;
      if (JSON.stringify(got) !== JSON.stringify(want)) {
        problems.push(
          `seed ${String(seed)} ${where} ${who}: scan mismatch\n got=${JSON.stringify(got)}\n want=${JSON.stringify(want)}`,
        );
        return;
      }
      for (const a of [0, 3, 4, 7]) {
        const viaIndex = (
          await conn.query(`SELECT id FROM t WHERE a = ${String(a)} ORDER BY id`, {
            memoize: false,
          })
        ).rows.map((x) => x.id);
        const exp = want.filter((x) => x.a === a).map((x) => x.id);
        if (JSON.stringify(viaIndex) !== JSON.stringify(exp)) {
          problems.push(
            `seed ${String(seed)} ${where} ${who}: index a=${String(a)} got ${JSON.stringify(viaIndex)} want ${JSON.stringify(exp)}`,
          );
        }
      }
      for (const row of want.slice(0, 3)) {
        const viaUnique = (
          await conn.query(`SELECT id FROM t WHERE d = '${row.d}'`, { memoize: false })
        ).rows.map((x) => x.id);
        if (JSON.stringify(viaUnique) !== JSON.stringify([row.id])) {
          problems.push(
            `seed ${String(seed)} ${where} ${who}: unique d=${row.d} got ${JSON.stringify(viaUnique)}`,
          );
        }
      }
      for (const [retiredKey] of [...forwards].slice(-3)) {
        const cur = current(retiredKey);
        const viaForward = (
          await conn.query(`SELECT id FROM t WHERE id = ${String(retiredKey)}`, { memoize: false })
        ).rows.map((x) => x.id);
        const exp = model.has(cur) ? [cur] : [];
        if (JSON.stringify(viaForward) !== JSON.stringify(exp)) {
          problems.push(
            `seed ${String(seed)} ${where} ${who}: forward ${String(retiredKey)}->${String(cur)} got ${JSON.stringify(viaForward)} want ${JSON.stringify(exp)}`,
          );
        }
      }
    }
  };

  let published = 0;
  let steps = 0;
  for (let round = 0; round < rounds && problems.length === 0; round += 1) {
    const label: string[] = [];
    for (let i = 0; i < 12; i += 1) await mutate(r() < 0.5 ? db : peer, []);
    let progress: CompactionJobProgress | undefined;
    try {
      progress = await db.compactTableStep("t", { maxBlocks: 1 });
    } catch (error) {
      if (!TOLERATED.test(String(error))) {
        problems.push(
          `seed ${String(seed)} round ${String(round)}: plan failed: ${String(error).slice(0, 200)}`,
        );
      }
      continue;
    }
    while (progress?.result === null && progress.jobId !== null) {
      steps += 1;
      const inter = r();
      if (inter < 0.6) await mutate(r() < 0.5 ? db : peer, label);
      else if (inter < 0.7) {
        label.push("peer full compact");
        try {
          await peer.compactTable("t");
        } catch (error) {
          if (!TOLERATED.test(String(error))) {
            problems.push(`seed ${String(seed)} peer compactTable: ${String(error).slice(0, 200)}`);
          }
        }
      } else if (inter < 0.78) {
        label.push("reopen");
        await db.close();
        db = new MinnowDatabase(store, opts);
        const jobs = await db.listCompactionJobs("t");
        const active = jobs.find((j) => j.state !== "cancelled" && j.state !== "published");
        if (active === undefined) {
          label.push("(job gone after reopen)");
          progress = undefined;
          break;
        }
        progress = { ...progress, jobId: active.id };
      } else if (inter < 0.85) {
        label.push("peer step");
        try {
          await peer.compactTableStep("t", { maxBlocks: 1 });
        } catch (error) {
          if (!TOLERATED.test(String(error))) {
            problems.push(`seed ${String(seed)} peer step: ${String(error).slice(0, 200)}`);
          }
        }
      }
      const jobId = progress.jobId;
      if (jobId === null) break;
      try {
        progress = await db.resumeCompactionJob(jobId, { maxBlocks: 1 });
      } catch (error) {
        if (TOLERATED.test(String(error))) {
          label.push(`(resume conflict: ${String(error).slice(0, 70)})`);
        } else {
          problems.push(
            `seed ${String(seed)} round ${String(round)} [${label.join("; ")}]: resume failed: ${String(error).slice(0, 300)}`,
          );
        }
        progress = undefined;
      }
    }
    if (
      progress?.state !== "skipped" &&
      progress?.result !== null &&
      progress?.result !== undefined
    ) {
      published += 1;
    }
    await verify(`round ${String(round)} [${label.join("; ")}]`);
  }
  await peer.close();
  await db.close();
  return { problems, published, steps };
}

const ROUNDS = Number(process.env.MINNOW_ORACLE_ROUNDS ?? 12);

describe("stepped compaction interleaved with writes, DDL, a peer and reopen", () => {
  it.each(seedsFor("compaction-interleave", [0x3c0a, 0x4d1b, 0x5e2c]))(
    "publishes only tables equal to the model (seed %s)",
    async (seed) => {
      const outcome = await runRounds(seed, ROUNDS);
      expect(outcome.problems).toEqual([]);
      expect(outcome.steps).toBeGreaterThan(0);
    },
  );
});
