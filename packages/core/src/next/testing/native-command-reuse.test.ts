/** Reuse and concurrency of metered native trigger preparation. One cached plan runs several
 * times with NUMERIC parameters whose sizing changes between runs; a learned entry is the plan's
 * measured need and reuse admits exactly that, in one attempt. A statement and a foreign governor
 * client grow their reservations at the same time on one governor (one write engine per
 * governor, write admission #9; the client is any other lawful user of it). At every reservation a
 * root makes while preparing, it holds nothing of its preparation; total reserved bytes across
 * a statement's attempts stay within twice its final reservation; refusals only above the cap.
 * Rows are compared with PostgreSQL. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_COMMAND_CONTENTION_REACH,
  NATIVE_COMMAND_REUSE_REACH,
} from "../../testing/reach.js";
import {
  executeNativeCommand,
  learnedEntry,
  learnedNativePreparation,
} from "../owner/native-command.js";
import { NativeCommandRoot } from "../owner/command-root.js";
import { NativeCommandReach } from "../owner/command-reach.js";
import { NativeRootCredits, NATIVE_ROOT_CREDIT_BYTES } from "../owner/native-root-credits.js";
import { Governor, MemoryAdmissionError, type Grant } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import type { PhysicalPlan } from "../ir/ir.js";
import { StorageCore } from "./storage-core.js";
import { yieldTask } from "../task.js";
import { oracleRows } from "./differential-adapter.js";

/** NUMERIC parameters whose binding sizes differ: 1 and 2^64 (the review's repro). */
const SMALL = "1",
  LARGE = "18446744073709551616";
const native = (value: string): bigint => BigInt(value);

function schema(terms: number, arms: number): { native: string[]; postgres: string[] } {
  // A simple CASE copies its operand once per WHEN, so its copies outgrow the base estimate
  // and the reservation grows by doubling, which can overshoot the final need.
  // With arms, the copied operand stays modest so each history remains quick.
  const id = `OLD.id${" + 1".repeat(arms === 0 ? terms : Math.min(terms, 150))}`,
    value =
      arms === 0
        ? id
        : `CASE ${id} ${Array.from({ length: arms }, (_, i) => `WHEN ${String(i)} THEN ${String(i)}`).join(" ")} ELSE ${id} END`;
  const body = `INSERT INTO log(id, v) VALUES (${value}, NEW.n);`;
  const tables = [
    "CREATE TABLE p(id INT PRIMARY KEY, n NUMERIC)",
    "CREATE TABLE log(id INT, v NUMERIC)",
    "INSERT INTO p VALUES (1, 0), (2, 0), (3, 0)",
  ];
  return {
    native: [...tables, `CREATE TRIGGER r AFTER UPDATE ON p BEGIN ${body} END`],
    postgres: [
      ...tables,
      `CREATE FUNCTION r() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body} RETURN NEW; END $$`,
      "CREATE TRIGGER r AFTER UPDATE ON p FOR EACH ROW EXECUTE FUNCTION r()",
    ],
  };
}
const MUTATION = "UPDATE p SET n = $1 WHERE id <= 2";

interface Root {
  readonly credits: NativeRootCredits;
  preparing: boolean;
  /** Metadata reservations while preparing (one per attempt; growth adds more). */
  reserved: number[];
  /** Preparation attempts: reach builds charged to this root. */
  attempts: number;
}

/** Every reservation a preparing root makes must start with no preparation held. */
/** Competing pressure: once armed, a foreign grant takes all free memory right after a root
 * opens and again after its first completed reservation; the grant is released only when some
 * admission starts waiting, so a root that needs more must ask while holding nothing. */
interface Pressure {
  armed: number;
  applied: number;
  /** Contention: fill once, after the root's first preparation reservation. */
  barrier?: number;
  /** Release the foreign grant only once a root-side admission and a client admission are
   * waiting together. */
  overlap?: boolean;
  overlapped?: boolean;
}

const CLIENT = "foreign client";

/** A lawful foreign governor client on the same governor: it grows its request by doubling (to a
 * quarter of the cap, then from 64 KiB again) and closes each grant before asking again, so it
 * holds nothing while it waits. Stopped by `stop`, which waits for its last admission to end. */
function foreignClient(governor: Governor): {
  readonly admissions: number;
  stop(): Promise<void>;
} {
  const stop = new AbortController(),
    stopped = () => stop.signal.aborted;
  let admissions = 0;
  const done = (async () => {
    let size = 64 << 10;
    while (!stopped()) {
      let grants: readonly Grant[] = [];
      try {
        grants = await governor.admitGroup({
          label: CLIENT,
          grants: [{ label: CLIENT, minimumBytes: size, spillable: false }],
          signal: stop.signal,
        });
        admissions++;
        if (!grants[0]?.allocate(size)) throw new Error("Missing foreign client credit");
        await yieldTask();
      } catch (error) {
        if (!stopped()) throw error;
      } finally {
        for (const grant of grants) {
          if (grant.allocated) grant.free(grant.allocated);
          grant.close();
        }
      }
      size = size * 2 > governor.capBytes / 4 ? 64 << 10 : size * 2;
    }
  })();
  return {
    get admissions() {
      return admissions;
    },
    stop: async () => {
      stop.abort();
      await done;
    },
  };
}
function watchRoots(
  violations: string[],
  governor: Governor,
  pressure: Pressure,
): { roots: Root[]; restore: () => void } {
  let foreign: Grant | null = null,
    waiting = 0,
    clientWaiting = 0,
    opened = 0,
    arrived: () => void = () => undefined;
  const barrier = new Promise<void>((resolve) => {
    arrived = resolve;
  });
  const admit = governor.admitGroup.bind(governor);
  const admitSpy = vi.spyOn(governor, "admitGroup").mockImplementation((request) => {
    const client = request.label === CLIENT,
      root = request.label !== "foreign" && !client;
    const pending = admit(request);
    if (root) {
      waiting++;
      void pending.then(
        () => waiting--,
        () => waiting--,
      );
    }
    if (client) {
      clientWaiting++;
      void pending.then(
        () => clientWaiting--,
        () => clientWaiting--,
      );
    }
    if (foreign && (root || client) && (!pressure.overlap || (waiting > 0 && clientWaiting > 0))) {
      if (pressure.overlap) pressure.overlapped = true;
      const grant = foreign;
      foreign = null;
      queueMicrotask(() => {
        grant.close();
      });
    }
    return pending;
  });
  const fill = async (): Promise<void> => {
    const free = governor.capBytes - governor.usedBytes;
    if (free <= 0 || foreign) return;
    const [grant] = await admit({
      label: "foreign",
      includesProgress: true,
      grants: [{ label: "foreign", minimumBytes: free, spillable: false }],
      signal: new AbortController().signal,
    });
    if (!grant?.allocate(grant.held)) throw new Error("Missing foreign grant");
    foreign = grant;
    pressure.applied++;
  };
  const roots: Root[] = [];
  const open = NativeRootCredits.open.bind(NativeRootCredits);
  const openSpy = vi.spyOn(NativeRootCredits, "open").mockImplementation(async (scope) => {
    const credits = await open(scope);
    roots.push({ credits, preparing: true, reserved: [], attempts: 0 });
    if (pressure.barrier === undefined && pressure.armed > 0) {
      pressure.armed--;
      await fill();
    }
    return credits;
  });
  // Deliberately captured: the spy calls it with reserve.call(this, ...) below.
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const reserve = NativeRootCredits.prototype.reserve;
  const reserveSpy = vi.spyOn(NativeRootCredits.prototype, "reserve").mockImplementation(function (
    this: NativeRootCredits,
    minimum,
    signal,
  ) {
    const root = roots.find((candidate) => candidate.credits === this);
    if (root?.preparing) {
      // Held credit, not just allocations: idle credit kept across a retry is unavailable to
      // every other governor user while this root waits (hold-and-wait).
      const held =
        this.metadata.held -
        NATIVE_ROOT_CREDIT_BYTES +
        this.rules.held +
        this.parameters.held +
        this.source.held;
      if (held !== 0) violations.push(`reservation began holding ${String(held)}B of credit`);
    }
    // Count what was actually reserved: a reservation refused above the cap holds nothing.
    return reserve.call(this, minimum, signal).then(async () => {
      // Contention: once every root holds its first preparation reservation, take the rest of
      // memory, so each must need more while the others hold theirs.
      if (pressure.barrier !== undefined && root?.preparing && minimum.metadata !== undefined) {
        if (root.reserved.length === 0 && ++opened === pressure.barrier) {
          await fill();
          arrived();
        }
        await barrier;
      }
      if (pressure.armed > 0 && root?.preparing) {
        pressure.armed--;
        await fill();
      }
      if (root?.preparing && minimum.metadata !== undefined) root.reserved.push(minimum.metadata);
    });
  });
  const prepare = NativeCommandReach.prepare.bind(NativeCommandReach);
  const reachSpy = vi
    .spyOn(NativeCommandReach, "prepare")
    .mockImplementation((catalog, root, charge, work) => {
      const owner = roots.find((candidate) => candidate.credits.metadata === charge);
      if (owner) owner.attempts++;
      return prepare(catalog, root, charge, work);
    });
  const create = NativeCommandRoot.create.bind(NativeCommandRoot);
  const createSpy = vi.spyOn(NativeCommandRoot, "create").mockImplementation((scope) => {
    for (const root of roots)
      if (root.credits.metadata === scope.funding.metadata) root.preparing = false;
    return create(scope);
  });
  return {
    roots,
    restore: () => {
      openSpy.mockRestore();
      reserveSpy.mockRestore();
      admitSpy.mockRestore();
      foreign?.close();
      foreign = null;
      reachSpy.mockRestore();
      createSpy.mockRestore();
    },
  };
}

async function run(
  core: StorageCore,
  governor: Governor,
  plan: PhysicalPlan,
  value: string,
  queryId: number,
): Promise<{ readonly error: unknown } | { readonly rows: number }> {
  const tx = await core.harness.path.begin();
  try {
    const result = await executeNativeCommand({
      transaction: tx,
      rootId: queryId,
      plan,
      parameters: [native(value)],
      planner: core.lowerer,
      context: { ...core.context(), catalog: tx.catalog },
      governor,
      signal: new AbortController().signal,
      execution: {
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        spill: core.spill,
        queryId,
        statementTime: 0n,
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      },
    });
    await (
      await tx.commit(null)
    ).durable;
    return { rows: result.rowCount };
  } catch (error) {
    tx.rollback();
    return { error };
  }
}

for (const seed of seedsFor("next-native-command-reuse", [49731, 49732]))
  it(`reuses learned preparation needs and grows concurrently (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("native command reuse", NATIVE_COMMAND_REUSE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.array(fc.constantFrom(SMALL, LARGE), { minLength: 2, maxLength: 4 }),
          fc.integer({ min: 1, max: 900 }),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 0, max: 20 }),
          async (values, terms, concurrent, squeezed, arms) => {
            reach.beginHistory();
            const governor = new Governor(128 << 20),
              core = new StorageCore(64),
              violations: string[] = [],
              s = schema(terms, arms);
            Object.defineProperty(core.harness, "governor", { value: governor });
            await core.open();
            const pressure: Pressure = { armed: 0, applied: 0 };
            const watch = watchRoots(violations, governor, pressure);
            let grew = false;
            try {
              await pg.exec("DROP TABLE IF EXISTS p, log CASCADE; DROP FUNCTION IF EXISTS r()");
              for (const sql of s.postgres) await pg.exec(sql);
              for (const sql of s.native) await core.execute(sql);
              const lowered = core.lowerer.lower(MUTATION, core.context());
              if (lowered.kind !== "modify") throw new TypeError("modify plan required");
              const plan = lowered.plan,
                plans = [plan];
              for (const [step, value] of values.entries()) {
                // Reuse under pressure: memory is full right after the first reservation.
                pressure.armed = squeezed && step > 0 ? 2 : 0;
                const before = watch.roots.length;
                // Concurrently, a foreign client grows on the same governor.
                const client = concurrent ? foreignClient(governor) : null;
                let outcomes: Array<Awaited<ReturnType<typeof run>>>;
                try {
                  outcomes = [await run(core, governor, plan, value, step + 1)];
                } finally {
                  await client?.stop();
                }
                if ((client?.admissions ?? 0) > 0) grew = true;
                await pg.query(MUTATION, [value]);
                for (const outcome of outcomes) {
                  if ("error" in outcome) {
                    // Only a need above the cap may refuse; nothing here comes close.
                    expect(outcome.error).toBeInstanceOf(MemoryAdmissionError);
                    const refusal = outcome.error as MemoryAdmissionError;
                    expect(refusal.bytes).toBeGreaterThan(refusal.capacity);
                    throw outcome.error;
                  }
                  expect(outcome.rows).toBe(2);
                }
                for (const root of watch.roots.slice(before)) {
                  const final = root.reserved.at(-1) ?? 0,
                    total = root.reserved.reduce((sum, bytes) => sum + bytes, 0);
                  // Doubling: everything reserved across attempts is within twice the last.
                  expect(total).toBeLessThanOrEqual(2 * final);
                  if (step > 0) {
                    // Reuse admits the learned need exactly, once.
                    expect(root.attempts).toBe(1);
                    reach.reach("reuse in one attempt");
                  }
                }
                for (const plan of plans) {
                  const learned = learnedNativePreparation(plan);
                  expect(learned?.metadata).toBe(learned?.need);
                }
                if (step > 0 && values[step] !== values[step - 1])
                  reach.reach("parameter sizing changed on reuse");
              }
              expect(violations).toEqual([]);
              for (const sql of [
                "SELECT * FROM p ORDER BY id",
                "SELECT * FROM log ORDER BY id, v",
              ]) {
                const actual = await core.execute(sql);
                if (!("batches" in actual)) throw new Error("rows required");
                expect(oracleRows(actual), sql).toEqual((await pg.query(sql)).rows);
              }
              reach.reach("exact PG rows");
              if (grew) reach.reach("statement and foreign client growing at once");
              if (pressure.applied > 0) reach.reach("reuse with memory full after admission");
              if (values.includes(LARGE)) reach.reach("large NUMERIC parameter");
              if (terms >= 400) reach.reach("large body");
            } finally {
              watch.restore();
              await core.close();
              expect(governor.usedBytes).toBe(0);
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [[SMALL, LARGE, SMALL], 600, false, true, 0],
            [[LARGE, SMALL], 800, true, false, 0],
            [[SMALL, LARGE], 300, true, true, 0],
            [[SMALL, SMALL], 150, false, false, 20],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 600000);

it("learns a plan's measured need, never its reservation padding", () => {
  // A 40 MiB reservation that found a 20 MiB need: reuse admits the need alone, so it is
  // neither refused nor kept waiting when later headroom is between the two.
  expect(learnedEntry({ fixedMetadata: 20 << 20, ruleBytes: 4096 })).toEqual({
    metadata: 20 << 20,
    need: 20 << 20,
    rules: 4096,
    parameters: 0,
  });
});

for (const seed of seedsFor("next-native-command-contention", [49751, 49752]))
  it(`a cold preparation and a foreign client grow while contending (${String(seed)})`, async () => {
    const reach = createReach("native command contention", NATIVE_COMMAND_CONTENTION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 50, max: 400 }),
        fc.integer({ min: 0, max: 12 }),
        async (terms, arms) => {
          reach.beginHistory();
          const governor = new Governor(128 << 20),
            core = new StorageCore(64),
            violations: string[] = [],
            s = schema(terms, arms);
          Object.defineProperty(core.harness, "governor", { value: governor });
          await core.open();
          // After the root's first preparation reservation, take all free memory: the root
          // falls short, must release and wait, and the foreign client (one lawful user of the
          // same governor) waits too; the filler is returned only once both wait at once.
          const pressure: Pressure = { armed: 0, applied: 0, barrier: 1, overlap: true };
          let settled = false;
          const watch = watchRoots(violations, governor, pressure);
          let client: ReturnType<typeof foreignClient> | null = null;
          try {
            for (const sql of s.native) await core.execute(sql);
            const lowered = core.lowerer.lower(MUTATION, core.context());
            if (lowered.kind !== "modify") throw new TypeError("modify plan required");
            client = foreignClient(governor);
            const outcome = await run(core, governor, lowered.plan, SMALL, 1);
            await client.stop();
            client = null;
            if ("error" in outcome) throw outcome.error;
            expect(outcome.rows).toBe(2);
            expect(violations).toEqual([]);
            // The root needed more credit while the filler held the rest, and the root and the
            // client waited at once.
            expect(pressure.overlapped).toBe(true);
            expect(watch.roots.every((root) => root.attempts >= 2)).toBe(true);
            reach.reach("root and foreign client waited at once holding nothing");
            settled = true;
            if (arms > 0) reach.reach("copies grew the need");
            if (terms >= 200) reach.reach("large body");
          } finally {
            await client?.stop();
            watch.restore();
            await core.close();
            // After a settled history only: an earlier failure must surface, not its leak.
            if (settled) expect(governor.usedBytes).toBe(0);
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [300, 0],
          [100, 8],
        ],
      },
    );
    reach.assertReached();
  }, 600000);
