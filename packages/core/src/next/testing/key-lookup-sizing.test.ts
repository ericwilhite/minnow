/**
 * Point and range lookups (keyLookup, indexRange): PostgreSQL owns the rows, and the fetch's read
 * bound is sized over only the groups the lookup's keys name (exec/index-access.ts
 * lookupSegments). Above the locator bound the whole source is sized; nothing is refused below
 * the cap, and sizing never waits while it holds the segment list.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, KEY_LOOKUP_SIZING_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { LOOKUP_SIZING_LABEL } from "../exec/index-access.js";
import { MetadataReader } from "../format/metadata-reader.js";
import { Governor, type Grant } from "../memory/governor.js";
import { ViewIndex } from "../write/view.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

/** 16 groups of 256 consecutive ids, written in shuffled group order with merges off. */
const GROUP_ROWS = 256,
  GROUPS = 16,
  ROWS = GROUP_ROWS * GROUPS;
let core: StorageCore;
let pg: PGlite;

beforeAll(async () => {
  core = await new StorageCore(GROUP_ROWS, { merges: { mode: "off" } }).open();
  pg = new PGlite();
  await pg.waitReady;
  const ddl = "CREATE TABLE t (id BIGINT PRIMARY KEY, k INTEGER, v DOUBLE PRECISION, s TEXT)";
  await core.execute(ddl);
  await pg.exec(ddl);
  const random = mulberry32(0x10c5);
  const order = Array.from({ length: GROUPS }, (_, g) => g);
  for (let i = order.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [order[i], order[j]] = [order[j] ?? 0, order[i] ?? 0];
  }
  for (const group of order) {
    const rows = Array.from({ length: GROUP_ROWS }, (_, r) => {
      const id = group * GROUP_ROWS + r + 1;
      return { id, k: id % 7, v: id % 11 === 0 ? null : id / 4, s: `s${String(id % 13)}` };
    });
    await core.writeBatch("insertBatch", "t", rows);
    await core.harness.path.quiesce();
    await pg.exec(
      `INSERT INTO t VALUES ${rows
        .map(
          (r) => `(${String(r.id)},${String(r.k)},${r.v === null ? "NULL" : String(r.v)},'${r.s}')`,
        )
        .join(",")}`,
    );
  }
});
afterAll(async () => {
  await core.close();
  await pg.close();
});

const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((r) => [Number(r.id), r.v === null ? null : Number(r.v), r.s]);
const groupOf = (id: number) => Math.floor((id - 1) / GROUP_ROWS);

/** Groups holding the given ids (ids outside 1..ROWS name no group). */
function groupsOf(ids: Iterable<number>): number {
  const groups = new Set<number>();
  for (const id of ids) if (id >= 1 && id <= ROWS) groups.add(groupOf(id));
  return groups.size;
}

/**
 * Runs `sql` on both engines (rows sorted by id for comparison); returns the distinct group
 * roots the next/ query touched, whether each was read by both sizing and fetch readers, and the
 * root operator. (The fetch may read a root more than once; the law counts groups.)
 */
async function run(
  sql: string,
  params: readonly number[] = [],
): Promise<{ roots: number; sizedWhatItRead: boolean; op: string | undefined }> {
  const pgSql = sql.replaceAll(/\$(\d)/gu, (_, n: string) => String(params[Number(n) - 1]));
  const expected = await pg.query<Record<string, unknown>>(pgSql);
  await core.execute(sql, params); // warm the shared metadata caches
  const readers = new Map<string, Set<MetadataReader>>();
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrapper preserves receiver
  const groupRoot = MetadataReader.prototype.groupRoot;
  const roots = vi.spyOn(MetadataReader.prototype, "groupRoot").mockImplementation(function (
    this: MetadataReader,
    ref,
    budget,
  ) {
    const key = JSON.stringify(ref);
    readers.set(key, (readers.get(key) ?? new Set()).add(this));
    return groupRoot.call(this, ref, budget);
  });
  try {
    const result = await core.execute(sql, params);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error(`No rows: ${sql}`);
    const byId = (a: unknown[], b: unknown[]) => Number(a[0]) - Number(b[0]);
    expect(canonical(oracleRows(result)).sort(byId), sql).toEqual(
      canonical(expected.rows).sort(byId),
    );
    return {
      roots: readers.size,
      sizedWhatItRead: [...readers.values()].every((set) => set.size >= 2),
      op: core.lastProfile?.operators.find(
        (o) => o.op === "keyLookup" || o.op === "indexRange" || o.op === "scan",
      )?.op,
    };
  } finally {
    roots.mockRestore();
  }
}

it("reads only the group a point lookup names (pinned)", async () => {
  const { roots, sizedWhatItRead, op } = await run("SELECT id, v, s FROM t WHERE id = 777");
  expect(op).toBe("keyLookup");
  // Sizing reads the key's one group root and the fetch reads it again: never the other 15.
  expect(roots).toBeLessThanOrEqual(2 * groupsOf([777]));
  expect(sizedWhatItRead, "every fetched group was sized").toBe(true);
});

type Shape = "point" | "missing key" | "duplicate IN probes" | "range" | "range over the bound";
for (const seed of seedsFor("next-key-lookup-sizing", [0x10c51, 0x10c52]))
  it(`sizes only the groups each lookup fetches, with PostgreSQL's rows (seed ${String(seed)})`, async () => {
    const reach = createReach("key lookup sizing", KEY_LOOKUP_SIZING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom<Shape>(
          "point",
          "missing key",
          "duplicate IN probes",
          "range",
          "range over the bound",
        ),
        fc.integer({ min: 1, max: ROWS }),
        fc.integer({ min: 0, max: 300 }),
        fc.boolean(),
        async (shape, at, width, parameter) => {
          reach.beginHistory();
          let sql: string,
            params: number[] = [],
            ids: number[];
          if (shape === "point" || shape === "missing key") {
            const id = shape === "point" ? at : ROWS + at;
            sql = `SELECT id, v, s FROM t WHERE id = ${parameter ? "$1" : String(id)}`;
            if (parameter) params = [id];
            ids = [id];
            if (parameter) reach.reach("parameter");
          } else if (shape === "duplicate IN probes") {
            ids = [at, at, (at + 1000) % ROWS || 1, (at + 1000) % ROWS || 1, ROWS + 5];
            sql = `SELECT id, v, s FROM t WHERE id IN (${ids.join(", ")})`;
          } else {
            const span = shape === "range" ? width : 2049 + width;
            const low = Math.max(1, Math.min(at, ROWS - span));
            sql = `SELECT id, v, s FROM t WHERE id BETWEEN ${String(low)} AND ${String(low + span)}`;
            ids = Array.from({ length: span + 1 }, (_, k) => low + k);
          }
          const { roots, sizedWhatItRead, op } = await run(sql, params);
          expect(op, sql).toBe(shape.startsWith("range") ? "indexRange" : "keyLookup");
          const matched = ids.filter((id) => id >= 1 && id <= ROWS).length;
          if (shape === "range over the bound" || matched > 2048) {
            // Over the locator bound: the whole source is sized (more groups than it fetches), never
            // refused.
            expect(roots, sql).toBeGreaterThan(groupsOf(ids));
            reach.reach("fallback");
          } else {
            expect(roots, sql).toBeLessThanOrEqual(2 * groupsOf(ids));
            expect(sizedWhatItRead, `${sql}: every fetched group was sized`).toBe(true);
            reach.reach("work bound");
          }
          reach.reach(shape);
          reach.reach("PG exact");
        },
      ),
      {
        seed,
        numRuns: 30,
        examples: [
          ["point", 777, 0, false],
          ["point", 3000, 0, true],
          ["missing key", 5, 0, false],
          ["duplicate IN probes", 100, 0, false],
          ["range", 250, 40, false],
          ["range", 1, 300, false],
          ["range over the bound", 1, 0, false],
        ],
      },
    );
    reach.assertReached();
  }, 180_000);

it.each(["sizing", "execution"] as const)(
  "closes the %s cursor at once on cancel and settles before release",
  async (gatedRole) => {
    const counts = { sizing: 0, execution: 0, closes: 0 };
    const baseline = core.harness.governor.usedBytes;
    let release: (() => void) | undefined;
    let wake: (() => void) | undefined;
    const entered = new Promise<void>((resolve) => {
      wake = resolve;
    });
    // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrappers preserve receivers
    const planProbe = ViewIndex.prototype.planProbe;
    const probes = vi.spyOn(ViewIndex.prototype, "planProbe").mockImplementation(function (
      this: ViewIndex,
      key,
    ) {
      const plan = planProbe.call(this, key);
      return {
        minimumBytes: plan.minimumBytes,
        open(options) {
          const role = options.budget.label === LOOKUP_SIZING_LABEL ? "sizing" : "execution";
          counts[role]++;
          const cursor = plan.open(options);
          let gated = false,
            closed = false;
          return {
            skip: (count) => cursor.skip(count),
            async next(out) {
              const n = await cursor.next(out);
              if (role === gatedRole && !gated) {
                gated = true;
                await new Promise<void>((resolve) => {
                  release = resolve;
                  wake?.();
                });
              }
              return n;
            },
            close() {
              // Close is idempotent; count each cursor once.
              if (!closed) counts.closes++;
              closed = true;
              cursor.close();
            },
          };
        },
      };
    });
    const controller = new AbortController();
    try {
      const query = core.execute("SELECT id, v, s FROM t WHERE id = 777", [], controller.signal);
      await Promise.race([
        entered,
        query.then(() => {
          throw new Error("the gate was not reached");
        }),
      ]);
      controller.abort(new Error("cancelled"));
      const opens = counts.sizing + counts.execution;
      expect(counts.closes, "every opened cursor closes on cancel").toBe(opens);
      let settled = false;
      void query.catch(() => {
        settled = true;
      });
      await Promise.resolve();
      expect(settled, "the pending index call settles before release").toBe(false);
      release?.();
      await expect(query).rejects.toThrow();
      expect([counts.sizing, counts.execution, counts.closes]).toEqual(
        gatedRole === "sizing" ? [1, 0, 1] : [1, 1, 2],
      );
      expect(core.harness.governor.usedBytes).toBe(baseline);
    } finally {
      release?.();
      probes.mockRestore();
    }
  },
);

it("reads the pinned snapshot when a writer commits between sizing and execution", async () => {
  // id ROWS + 1 does not exist when the lookup is sized; a writer adds it in between.
  const sql = `SELECT id, v, s FROM t WHERE id IN (10, ${String(ROWS + 1)})`;
  const before = await pg.query<Record<string, unknown>>(sql);
  let committed = false;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrappers preserve receivers
  const planProbe = ViewIndex.prototype.planProbe;
  const probes = vi.spyOn(ViewIndex.prototype, "planProbe").mockImplementation(function (
    this: ViewIndex,
    key,
  ) {
    const plan = planProbe.call(this, key);
    return {
      minimumBytes: plan.minimumBytes,
      open(options) {
        const cursor = plan.open(options);
        if (options.budget.label !== LOOKUP_SIZING_LABEL) return cursor;
        return {
          skip: (count) => cursor.skip(count),
          async next(out) {
            const n = await cursor.next(out);
            if (!committed) {
              committed = true;
              await core.writeBatch("insertBatch", "t", [{ id: ROWS + 1, k: 0, v: 1, s: "new" }]);
              await core.harness.path.quiesce();
            }
            return n;
          },
          close: () => cursor.close(),
        };
      },
    };
  });
  try {
    const result = await core.execute(sql);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error("rows");
    expect(committed).toBe(true);
    expect(canonical(oracleRows(result))).toEqual(canonical(before.rows));
  } finally {
    probes.mockRestore();
  }
  await pg.exec(`INSERT INTO t VALUES (${String(ROWS + 1)}, 0, 1, 'new')`);
  // A later lookup sees the commit, still exactly PostgreSQL's rows and within the work bound.
  const { roots, sizedWhatItRead } = await run(sql);
  expect(roots).toBeLessThanOrEqual(2 * 2);
  expect(sizedWhatItRead).toBe(true);
});

it("never waits for memory while holding the lookup's segment list (FIFO repro)", async () => {
  const governor = new Governor(16 << 20);
  const sql = "SELECT id, v, s FROM t WHERE id = 777";
  const want = await pg.query<Record<string, unknown>>(sql);
  const nativePrepare = core.executor.prepare.bind(core.executor);
  const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
    const prepared = nativePrepare(plan);
    return { ...prepared, execute: (request) => prepared.execute({ ...request, governor }) };
  });
  const queued: { competitor: Promise<Grant> | null } = { competitor: null };
  const tryAdmit = governor.tryAdmit.bind(governor);
  const spy = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
    if (queued.competitor === null && options.label.startsWith("read-step/"))
      queued.competitor = governor.admit({
        label: "list-blocked competitor",
        // Fits only once the query holds nothing (its list and any source recipe).
        minimumBytes: governor.capBytes - governor.reserveBytes,
        spillable: false,
      });
    return tryAdmit(options);
  });
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const query = core.execute(sql, [], controller.signal);
    const competed = (async () => {
      while (queued.competitor === null) await new Promise((resolve) => setTimeout(resolve, 1));
      (await queued.competitor).close();
    })();
    const outcome = await Promise.race([
      query.then(() => "done" as const),
      new Promise<"deadlock">((resolve) => {
        timer = setTimeout(() => resolve("deadlock"), 10_000);
      }),
    ]);
    if (outcome === "deadlock") controller.abort(new Error("deadlocked"));
    expect(outcome, "sizing never waits while holding the list").toBe("done");
    await competed;
    const result = await query;
    if (result.kind !== "rows" || !("batches" in result)) throw new Error("rows");
    expect(canonical(oracleRows(result))).toEqual(canonical(want.rows));
    expect(queued.competitor, "the queued list-blocked admission happened").not.toBeNull();
    expect(governor.usedBytes).toBe(0);
  } finally {
    clearTimeout(timer);
    spy.mockRestore();
    prepare.mockRestore();
  }
}, 30_000);
