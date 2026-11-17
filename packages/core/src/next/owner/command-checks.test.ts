/** Native bounded checks against PG and the shipped engine; no SQL trigger driver is installed. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { yieldTask } from "../task.js";
import { currentEngine } from "../../testing/differential-current.js";
import {
  createReach,
  NATIVE_COMMAND_CHECK_REACH,
  NATIVE_PARENT_KEY_REACH,
  NATIVE_RI_RESTRICTION_REACH,
  NATIVE_INFORMATIONAL_CAPTURE_REACH,
  NATIVE_COMMAND_ACTION_CHECK_REACH,
  NATIVE_COMMAND_CHECK_LIFETIME_REACH,
  NATIVE_COMMAND_CHECK_CONSTRUCTION_REACH,
  NATIVE_CHILD_CAPTURE_REACH,
  REGISTRY_FOLLOWING_REACH,
  REGISTRY_FOLLOWING_LIFETIME_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { RowTargetRange } from "../access/statement-command.js";
import { RegistryFollowing, registryFollowingBytes } from "./registry-following.js";
import { RegistryTargets } from "./registry-targets.js";
import { readRows } from "../write/rows.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import type { NativeCommandBoundary } from "../access/statement-command.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { NativeCommandExecution } from "./command-execution.js";
import { NativeReferentialChecks } from "./referential-checks.js";
import { NativeCommandChecks, commandCheckMinimumBytes } from "./command-checks.js";
import { RegistryDirectory } from "./registry-directory.js";
import { RegistryFragments } from "./registry-fragments.js";
import { RegistryImages } from "./registry-images.js";
import { RegistryCommands, type AcceptedCommandRow } from "./registry-commands.js";
import { ChunkReader } from "../page/reader.js";
import { MemoryReads } from "../write/memory-read.js";
import { ViewIndex } from "../write/view.js";
import { StatementImpl } from "../write/transaction.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  TEXT,
  texts,
  tableInput,
  rules,
  locators,
  rowsOf,
} from "../write/test-support.js";

async function code(run: () => unknown): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    if (typeof error !== "object" || error === null) throw error;
    if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
    if ("code" in error && typeof error.code === "string") return error.code;
    throw error;
  }
}
async function failure(run: () => Promise<unknown>): Promise<unknown> {
  return await run().then(
    () => null,
    (error: unknown) => error,
  );
}
async function fixture(
  published: boolean,
  count: number,
  beforeRange?: (signal: AbortSignal) => Promise<void>,
  childCount = 0,
  deleteMode: "noAction" | "restrict" = "noAction",
  enforced = true,
  payloadWidth = 0,
) {
  const h = new Harness("strict", "memory", 256),
    path = await h.open();
  let parent = 0,
    child = 0;
  await autocommit(path, async (s) => {
    parent =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput(
            "p",
            [column("id", INT, false), column("u", INT), column("payload", TEXT)],
            { unique: [{ name: "p_u_key", columns: [1] }] },
          ),
        })
      ).id ?? 0;
    const def = s.catalog.table(parent);
    if (!def?.primaryKey) throw new Error("Missing native parent index");
    child =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput(
            "c",
            [column("id", INT, false), column("pid", INT), column("payload", TEXT)],
            {
              foreignKeys: [
                {
                  name: "c_pid_fkey",
                  columns: [1],
                  parentTableId: parent,
                  parentIndexId: def.primaryKey,
                  onDelete: deleteMode,
                  enforced,
                },
              ],
            },
          ),
        })
      ).id ?? 0;
  });
  const ids = Array.from({ length: count }, (_, k) => k + 1);
  for (let from = 0; from < count; from += 200) {
    const part = ids.slice(from, from + 200);
    await autocommit(path, (s) =>
      s.table(parent).insert(batch(ints(part), ints(part), texts(part.map(() => "seed"))), null),
    );
  }
  for (let from = 0; from < childCount; from += 200) {
    const positions = Array.from({ length: Math.min(200, childCount - from) }, (_, i) => from + i);
    await autocommit(path, (statement) =>
      statement
        .table(child)
        .insert(
          batch(
            ints(positions.map((i) => i + 1)),
            ints(positions.map((i) => (i % 5 === 0 ? null : (i % Math.min(count, 2)) + 1))),
            texts(positions.map(() => "child" + "雪".repeat(payloadWidth))),
          ),
          null,
        ),
    );
  }
  if (published) {
    await path.recycleLog();
    await path.quiesce();
  }
  const tx = await path.begin(),
    statement = tx.statement({ rules });
  if (!(statement instanceof StatementImpl)) throw new Error("Expected real native statement");
  const grant = await h.governor.admit({
    label: "prepaid native registry/check workspace",
    minimumBytes: 2 * 1024 * 1024,
    spillable: false,
  });
  const range = await h.governor.admit({
    label: "prepaid native constraint cursor",
    minimumBytes: 32 * 1024 * 1024,
    spillable: false,
  });
  const stop = new AbortController(),
    spill = new MemorySpillService();
  const directory = new RegistryDirectory(grant, spill, 1, stop.signal),
    fragments = new RegistryFragments(grant, spill, 1, stop.signal, () => undefined);
  const images = new RegistryImages({
    rootId: 17,
    catalog: statement.catalog,
    directory,
    fragments,
    charge: grant,
    signal: stop.signal,
    prepare: () => undefined,
    checkpoint: () => undefined,
  });
  const commands = new RegistryCommands({
    rootId: 17,
    frames: 1,
    rowEvents: 2,
    directory,
    charge: grant,
    signal: stop.signal,
    checkpoint: () => undefined,
    referentialName: (id) => `RI_ConstraintTrigger_c_${String(id)}`,
  });
  const root = commands.beginCommand(1, 1);
  let leases = 0,
    maxRange = 0;
  const projected: Pick<NativeCommandBoundary, "readRowBindings"> = {
    readRowBindings: async (ref, projection, signal) => {
      const lease = await images.readRowBindings(ref, projection, signal);
      leases++;
      return {
        tableId: lease.tableId,
        old: lease.old,
        new: lease.new,
        release: () => {
          lease.release();
          leases--;
        },
      };
    },
  };
  const targets = new RegistryTargets({
    rootId: 17,
    directory,
    charge: grant,
    signal: stop.signal,
    checkpoint: () => undefined,
  });
  const beforeCheckBytes = grant.allocated;
  const checks = new NativeCommandChecks({
    statement,
    tables: [parent, child],
    images: projected,
    charge: grant,
    signal: stop.signal,
    scan: {
      reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
      prepare: async (_table, minimum, signal) => {
        signal.throwIfAborted();
        expect(leases).toBe(0);
        expect(range.held - range.allocated).toBeGreaterThanOrEqual(minimum);
        return range;
      },
    },
    prepareRange: async (plan, signal) => {
      expect(leases).toBe(0);
      await beforeRange?.(signal);
      signal.throwIfAborted();
      maxRange = Math.max(maxRange, plan.minimumBytes);
      expect(plan.minimumBytes).toBeLessThanOrEqual(range.held);
      return range;
    },
  });
  expect(grant.allocated - beforeCheckBytes).toBe(
    commandCheckMinimumBytes(statement.catalog, [parent, child]),
  );
  return {
    h,
    path,
    tx,
    statement,
    parent,
    child,
    grant,
    range,
    stop,
    images,
    checks,
    commands,
    targets,
    directory,
    root,
    get leases() {
      return leases;
    },
    get maxRange() {
      return maxRange;
    },
    async insert(tableId: number, id: number, value: number | null): Promise<AcceptedCommandRow> {
      const input = batch(ints([id]), ints([value]), texts(["unrelated".repeat(10000)]));
      const ref = await images.append(tableId, null, { batch: input, row: 0 });
      await statement.table(tableId).insert(input, null);
      const table = statement.current().internalTable(tableId),
        mem = table.tableView.memtables.at(-1);
      if (!mem) throw new Error("Missing accepted native row");
      const accepted: AcceptedCommandRow = {
        command: root,
        tableId,
        event: "insert",
        images: ref,
        oldRowId: null,
        newRowId: mem.firstRowId + mem.rowCount - 1,
      };
      await commands.accept(accepted);
      return accepted;
    },
    async close() {
      await statement.closeNativeCommands();
      await commands.close();
      await checks.close();
      await targets.close();
      await images.close();
      await fragments.close();
      await directory.close();
      expect(grant.allocated).toBe(0);
      expect(range.allocated).toBe(0);
      grant.close();
      range.close();
      statement.rollback();
      tx.rollback();
      await h.close();
      expect(h.governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    },
  };
}

it.each(seedsFor("next-native-command-checks", [0xb5e8]))(
  "checks real native unique/FK keys with prepaid bounded cursors seed %i",
  async (seed) => {
    const reach = createReach("native command checks", NATIVE_COMMAND_CHECK_REACH),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(
            "unique",
            "primary",
            "reference",
            "reference valid",
            "reference NULL",
            "NULL",
            "valid",
            "obsolete",
          ),
          fc.boolean(),
          fc.integer({ min: 2, max: 300 }),
          async (kind, published, count) => {
            reach.beginHistory();
            const f = await fixture(published, count);
            try {
              for (const sql of [
                "DROP TABLE IF EXISTS c",
                "DROP TABLE IF EXISTS p",
                "CREATE TABLE p(id INTEGER PRIMARY KEY,u INTEGER UNIQUE,payload TEXT)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id),payload TEXT)",
                `INSERT INTO p SELECT x,x,'seed' FROM generate_series(1,${String(count)}) x`,
              ]) {
                await pg.exec(sql);
                if (!sql.startsWith("INSERT INTO")) await shipped.run({ kind: "sql", sql });
              }
              // The shipped source has no generate_series; exact same seed rows through VALUES.
              await shipped.run({
                kind: "sql",
                sql: `INSERT INTO p VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)},'seed')`).join(",")}`,
              });
              const child = kind.startsWith("reference") || kind === "obsolete",
                table = child ? f.child : f.parent,
                id = kind === "primary" ? 1 : count + 1,
                value =
                  kind === "unique"
                    ? 1
                    : kind === "reference valid"
                      ? 1
                      : kind === "reference NULL" || kind === "NULL"
                        ? null
                        : child
                          ? count + 100
                          : count + 1;
              const row = await f.insert(table, id, value);
              if (kind === "obsolete") {
                await f.statement.table(table).applyDelete([row.newRowId ?? -1]);
                reach.reach("obsolete version skipped");
              }
              const sql = `INSERT INTO ${child ? "c" : "p"} VALUES(${String(id)},${value === null ? "NULL" : String(value)},'candidate')`;
              let expected: string | null = null,
                shippedFailed = false;
              if (kind !== "obsolete") {
                expected = await code(() => pg.exec(sql));
                try {
                  await shipped.run({ kind: "sql", sql });
                } catch {
                  shippedFailed = true;
                }
              }
              const legacy = vi.spyOn(ViewIndex.prototype, "countRange").mockImplementation(() => {
                throw new Error("native check retained legacy matching IDs");
              });
              const admit = vi.spyOn(f.h.governor, "admit").mockImplementation(() => {
                  throw new Error("native check independently admitted");
                }),
                group = vi.spyOn(f.h.governor, "admitGroup").mockImplementation(() => {
                  throw new Error("native check independently admitted");
                });
              let observed: string | null;
              try {
                observed = await code(async () => {
                  await f.commands.verify(
                    f.root,
                    "unique",
                    (accepted, signal) => f.checks.verifyUnique(accepted, signal),
                    f.stop.signal,
                  );
                  if (child) {
                    const fk = f.statement.catalog.table(table)?.foreignKeys[0];
                    if (!fk) throw new Error("Missing FK");
                    await f.checks.verifyReference(row, fk.id, f.stop.signal);
                  }
                });
              } finally {
                admit.mockRestore();
                group.mockRestore();
                legacy.mockRestore();
              }
              expect(observed).toBe(expected);
              if (kind !== "obsolete") expect(shippedFailed).toBe(expected !== null);
              expect(f.leases).toBe(0);
              expect(f.range.allocated).toBe(0);
              reach.reach(published ? "published sources" : "resident sources");
              reach.reach(
                kind === "reference"
                  ? "missing parent 23503"
                  : kind === "unique"
                    ? "unique 23505"
                    : kind === "primary"
                      ? "primary 23505"
                      : kind === "reference valid"
                        ? "valid reference"
                        : kind === "reference NULL"
                          ? "NULL reference skipped"
                          : kind === "NULL"
                            ? "NULL key skipped"
                            : kind === "valid"
                              ? "valid command"
                              : "obsolete version skipped",
              );
              if (kind !== "NULL" && kind !== "obsolete") {
                expect(f.maxRange).toBeGreaterThan(0);
                reach.reach("root prepaid cursor");
                reach.reach("image released before progress");
                reach.reach("projected unrelated payload");
              }
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["unique", false, 3],
            ["primary", true, 3],
            ["reference", false, 3],
            ["reference valid", true, 3],
            ["reference NULL", false, 3],
            ["NULL", true, 3],
            ["valid", false, 3],
            ["obsolete", true, 3],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await shipped.close();
    }
  },
  120000,
);

it.each(seedsFor("next-native-command-action-checks", [0xb5e9]))(
  "checks each real accepted action after the same token passed seed %i",
  async (seed) => {
    const pg = new PGlite(),
      reach = createReach("native incremental action checks", NATIVE_COMMAND_ACTION_CHECK_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          async (earlier, published) => {
            reach.beginHistory();
            const f = await fixture(published, 3);
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS p; CREATE TABLE p(id INTEGER PRIMARY KEY,u INTEGER UNIQUE,payload TEXT); INSERT INTO p VALUES(1,1,'seed'),(2,2,'seed'),(3,3,'seed')",
              );
              await f.commands.verify(
                f.root,
                "unique",
                (row, signal) => f.checks.verifyUnique(row, signal),
                f.stop.signal,
              );
              reach.reach("root checkpoint already passed");
              const inserted = await f.insert(f.parent, 4, 4);
              expect(inserted.newRowId).not.toBeNull();
              for (let k = 0; k <= earlier; k++) {
                if (k > 0) {
                  await f.insert(f.parent, 4 + k, 4 + k);
                  reach.reach("earlier actions passed");
                }
                await pg.exec(`INSERT INTO p VALUES(${String(4 + k)},${String(4 + k)},'action')`);
                await f.commands.verify(
                  f.root,
                  "unique",
                  (row, signal) => f.checks.verifyUnique(row, signal),
                  f.stop.signal,
                );
              }
              await f.insert(f.parent, 5 + earlier, 4);
              const expected = await code(() =>
                pg.exec(`INSERT INTO p VALUES(${String(5 + earlier)},4,'duplicate'); SELECT 1/0`),
              );
              let later = false;
              const actual = await code(async () => {
                await f.commands.verify(
                  f.root,
                  "unique",
                  (row, signal) => f.checks.verifyUnique(row, signal),
                  f.stop.signal,
                );
                later = true;
                throw Object.assign(new Error("later AFTER body"), { sqlState: "22012" });
              });
              expect(expected).toBe("23505");
              expect(actual).toBe(expected);
              expect(later).toBe(false);
              reach.reach("new action uniqueness before later error");
              reach.reach(published ? "published sources" : "resident sources");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [0, false],
            [3, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120000,
);

it.each(seedsFor("next-native-command-check-lifetime", [0xb5ea]))(
  "fences native check progress before refund on cancel seed %i",
  async (seed) => {
    const reach = createReach("native check lifetime", NATIVE_COMMAND_CHECK_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), async (close, capture) => {
        reach.beginHistory();
        let entered!: () => void,
          resume!: () => void,
          observed: AbortSignal | null = null;
        const ready = new Promise<void>((r) => {
            entered = r;
          }),
          gate = new Promise<void>((r) => {
            resume = r;
          });
        const f = await fixture(false, 2, async (signal) => {
          observed = signal;
          entered();
          await gate;
        });
        let closing: Promise<void> | null = null;
        try {
          const row = await f.insert(f.parent, 3, 3);
          const images = await f.images.append(
            f.parent,
            { batch: batch(ints([1]), ints([1]), texts(["seed"])), row: 0 },
            { batch: batch(ints([4]), ints([1]), texts(["next"])), row: 0 },
          );
          const fk = f.statement.catalog.table(f.child)?.foreignKeys[0];
          if (!fk) throw new Error("Missing capture FK");
          const work = capture
            ? f.checks.captureChildren(
                { tableId: f.parent, images },
                f.child,
                fk.id,
                f.targets,
                f.stop.signal,
              )
            : f.checks.verifyUnique(row, f.stop.signal);
          const pending = work.then(
            () => null,
            (error: unknown) => error,
          );
          await ready;
          expect(f.leases).toBe(0);
          reach.reach("image gone before wait");
          const paid = f.grant.allocated;
          let done = false;
          if (close)
            closing = f.checks.close().then(() => {
              done = true;
            });
          else f.stop.abort(new Error("caller cancelled"));
          await yieldTask();
          const stillPaid = f.grant.allocated === paid,
            aborted = (observed as AbortSignal | null)?.aborted === true,
            early = done;
          resume();
          const error = await pending;
          await closing;
          expect(error).toBeInstanceOf(Error);
          expect(stillPaid).toBe(true);
          expect(aborted, "native preparation receives cancellation").toBe(true);
          expect(early).toBe(false);
          expect(f.range.allocated).toBe(0);
          reach.reach(close ? "close cancellation" : "caller cancellation");
          if (capture) reach.reach(close ? "capture close" : "capture cancellation");
          reach.reach("pending progress remains paid");
          reach.reach("no cursor after cancellation");
        } finally {
          resume();
          await closing;
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 6,
        examples: [
          [false, false],
          [true, false],
          [false, true],
          [true, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-command-check-construction", [0xb5ec]))(
  "returns private native check account after constructor faults seed %i",
  async (seed) => {
    const reach = createReach("native check construction", NATIVE_COMMAND_CHECK_CONSTRUCTION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (nth) => {
        reach.beginHistory();
        const f = await fixture(false, 2),
          before = f.grant.allocated,
          failure = new Error("native check constructor fault");
        const native = { Uint8Array, Int32Array, Float64Array };
        let calls = 0;
        const intercept = <T extends typeof Uint8Array | typeof Int32Array | typeof Float64Array>(
          target: T,
        ) =>
          new Proxy(target, {
            construct(constructor, args, newTarget) {
              if (args[0] !== 0 && ++calls === nth) throw failure;
              return Reflect.construct(constructor, args, newTarget) as
                Uint8Array | Int32Array | Float64Array;
            },
          });
        try {
          vi.stubGlobal("Uint8Array", intercept(native.Uint8Array));
          vi.stubGlobal("Int32Array", intercept(native.Int32Array));
          vi.stubGlobal("Float64Array", intercept(native.Float64Array));
          expect(
            () =>
              new NativeCommandChecks({
                statement: f.statement,
                tables: [f.parent, f.child],
                images: f.images,
                charge: f.grant,
                signal: f.stop.signal,
                prepareRange: async () => f.range,
              }),
          ).toThrow(failure);
        } finally {
          vi.stubGlobal("Uint8Array", native.Uint8Array);
          vi.stubGlobal("Int32Array", native.Int32Array);
          vi.stubGlobal("Float64Array", native.Float64Array);
        }
        try {
          expect(calls).toBe(nth);
          expect(f.grant.allocated).toBe(before);
          reach.reach(
            nth === 1
              ? "digit scratch fault"
              : nth === 2
                ? "key buffer fault"
                : nth === 3
                  ? "offset buffer fault"
                  : "output buffer fault",
          );
          reach.reach("private account restored");
        } finally {
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 6, examples: [[1], [2], [3], [4]] },
    );
    reach.assertReached();
  },
  60000,
);

for (const seed of seedsFor("next-native-child-capture", [0xb61d, 0xb61e]))
  it(`seals complete original RI child identities through a paid cursor ${String(seed)}`, async () => {
    const reach = createReach("native RI child capture", NATIVE_CHILD_CAPTURE_REACH),
      pg = new PGlite();
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.oneof(
            { weight: 3, arbitrary: fc.integer({ min: 0, max: 128 }) },
            { weight: 1, arbitrary: fc.integer({ min: 1025, max: 2300 }) },
          ),
          fc.constantFrom(1, 2, 3),
          fc.boolean(),
          fc.boolean(),
          async (count, key, published, prefix) => {
            reach.beginHistory();
            const f = await fixture(published, 3, undefined, count);
            let restoreHolders: (() => void) | undefined;
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS c;DROP TABLE IF EXISTS p;CREATE TABLE p(id INT PRIMARY KEY);INSERT INTO p VALUES(1),(2),(3);CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id))",
              );
              if (count)
                await pg.exec(
                  `INSERT INTO c VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${i % 5 === 0 ? "NULL" : String((i % 2) + 1)})`).join(",")}`,
                );
              if (prefix && count > 1) {
                const pid = f.statement.catalog.table(f.child)?.columns[1]?.id;
                if (!pid) throw new Error("Missing reference column");
                await f.statement
                  .table(f.child, { assigned: [pid], uniqueTargets: true })
                  .update(batch(locators([1], f.child), ints([1])), null);
                await pg.exec("UPDATE c SET pid=1 WHERE id=2");
                reach.reach("current prefix");
              }
              // NEW deliberately has another key. Capture uses immutable OLD before a
              // parent continuation; an updated key must not change the input identity set.
              const images = await f.images.append(
                f.parent,
                {
                  batch: batch(ints([key]), ints([key]), texts(["unrelated".repeat(10000)])),
                  row: 0,
                },
                { batch: batch(ints([key === 1 ? 2 : 1]), ints([key]), texts(["next"])), row: 0 },
              );
              const child = f.statement.catalog.table(f.child),
                fk = child?.foreignKeys[0];
              if (!child || !fk) throw new Error("Missing native child FK");
              // A large-result trap prevents RED shrinking to an empty old-API lookup.
              // eslint-disable-next-line @typescript-eslint/unbound-method
              const holders = ViewIndex.prototype.holders;
              const trap = vi.spyOn(ViewIndex.prototype, "holders").mockImplementation(function (
                this: ViewIndex,
                ...args
              ) {
                if (count > 1024 && key !== 3)
                  throw new Error("native child capture must use a bounded cursor");
                return holders.call(this, ...args);
              });
              restoreHolders = () => trap.mockRestore();
              const before = f.grant.allocated;
              type Capture = (
                parent: { tableId: number; images: typeof images },
                child: number,
                fk: number,
                targets: RegistryTargets,
                signal: AbortSignal,
              ) => Promise<RowTargetRange>;
              const capability = f.checks as unknown as { captureChildren?: Capture };
              let range: RowTargetRange;
              if (capability.captureChildren)
                range = await capability.captureChildren(
                  { tableId: f.parent, images },
                  f.child,
                  fk.id,
                  f.targets,
                  f.stop.signal,
                );
              else {
                // Parent replay of the existing legacy RI design, rather than a
                // missing-method/setup failure: holders materializes the full target set.
                const pin = f.statement.readOwn(),
                  keys = new KeyBatch(f.grant, 4096, 4096);
                try {
                  encodeKeyBatch(
                    "equality",
                    [{ vector: ints([key]), direction: "asc", nulls: "first" }],
                    { length: 1, selection: null },
                    keys,
                  );
                  const support = child.indexes.find((index) => index.id === fk.supportIndexId);
                  if (!support) throw new Error("Missing support index");
                  const rows = await new ViewIndex(
                    { ...support, unique: false },
                    pin.internalTable(f.child),
                  ).holders(keys.key(0), false);
                  range = await f.targets.capture(async function* () {
                    for (const rowId of rows) yield { tableId: f.child, rowId };
                  });
                } finally {
                  keys.close();
                  pin.release();
                }
              }
              expect(f.grant.allocated, "row count does not grow resident capture metadata").toBe(
                before,
              );
              expect(f.range.allocated, "native cursor/page reservations end before replay").toBe(
                0,
              );
              const actual: number[] = [],
                pin = f.statement.readOwn();
              try {
                for await (const row of f.targets.rows(range, f.stop.signal)) {
                  expect(row.tableId).toBe(f.child);
                  const columns = await readRows(pin.internalTable(f.child), [row.rowId]),
                    first = columns[0];
                  if (first?.data.layout !== "i32") throw new Error("Expected integer key");
                  actual.push(first.data.values[0] ?? -1);
                }
              } finally {
                pin.release();
              }
              const expected = (
                await pg.query<{ id: number }>("SELECT id FROM c WHERE pid=$1 ORDER BY id", [key])
              ).rows.map((row) => row.id);
              expect(
                actual.sort((a, b) => a - b),
                "complete OLD-key child target set matches PostgreSQL",
              ).toEqual(expected);
              expect(range.count).toBe(expected.length);
              reach.reach(published ? "published" : "resident");
              reach.reach(key === 3 || count === 0 ? "empty" : "matching OLD");
              if (expected.length > 2) reach.reach("multiple cursor fills");
              if (count > 1024) reach.reach("spilled target directory");
              reach.reach("NEW differs");
              reach.reach("fixed metadata");
              reach.reach("released cursor");
            } finally {
              restoreHolders?.();
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [0, 1, false, false],
            [40, 1, false, true],
            [45, 2, true, true],
            [2200, 1, true, false],
            [200, 3, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

for (const seed of seedsFor("next-registry-following", [0xb623, 0xb624]))
  it(`binds original follow ranges without changing queued entries ${String(seed)}`, async () => {
    const reach = createReach("native follow bindings", REGISTRY_FOLLOWING_REACH),
      pg = new PGlite();
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 6, max: 36 }),
          fc.boolean(),
          fc.boolean(),
          async (count, published, late) => {
            reach.beginHistory();
            const f = await fixture(published, 3, undefined, count);
            const before = f.grant.allocated;
            const bindings = new RegistryFollowing({
              rootId: 17,
              catalog: f.statement.catalog,
              commands: f.commands,
              directory: f.directory,
              targets: f.targets,
              checks: f.checks,
              charge: f.grant,
              signal: f.stop.signal,
            });
            const capture = vi.spyOn(f.checks, "captureChildren");
            try {
              expect(f.grant.allocated - before).toBe(registryFollowingBytes(f.statement.catalog));
              await pg.exec(
                "DROP TABLE IF EXISTS c;DROP TABLE IF EXISTS p;CREATE TABLE p(id INT PRIMARY KEY);INSERT INTO p VALUES(1),(2),(3);CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id))",
              );
              await pg.exec(
                `INSERT INTO c VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${i % 5 === 0 ? "NULL" : String((i % 2) + 1)})`).join(",")}`,
              );
              const parent = f.statement.catalog.table(f.parent),
                child = f.statement.catalog.table(f.child),
                fk = child?.foreignKeys[0];
              const pk = parent?.columns[0]?.id,
                pid = child?.columns[1]?.id;
              if (!parent || !child || !fk || !pk || !pid)
                throw new Error("Missing native follow shape");
              // Product omission follows original parent identities across a key swap.
              // PG below is an independent reference for child membership/existence,
              // not a claim PG permits a non-deferrable primary-key swap.
              await f.statement
                .table(f.parent, { assigned: [pk], uniqueTargets: true })
                .update(batch(locators([0, 1], f.parent), ints([2, 1])), null);
              const appended = f.tx.undo
                .filter((e) => e.kind === "rows" && e.tableId === f.parent)
                .at(-1);
              if (appended?.kind !== "rows" || appended.count !== 2)
                throw new Error("Missing accepted parent rows");
              const actions: number[] = [],
                refs: AcceptedCommandRow[] = [];
              for (let i = 0; i < 2; i++) {
                const images = await f.images.append(
                  f.parent,
                  { batch: batch(ints([i + 1]), ints([i + 1]), texts(["seed"])), row: 0 },
                  { batch: batch(ints([2 - i]), ints([i + 1]), texts(["seed"])), row: 0 },
                );
                const row: AcceptedCommandRow = {
                  command: f.root,
                  tableId: f.parent,
                  event: "update",
                  images,
                  oldRowId: i,
                  newRowId: appended.memtable.firstRowId + appended.before + i,
                };
                await f.commands.accept(row);
                refs.push(row);
                actions.push(
                  await f.commands.addReferential(f.root, images, {
                    fkId: fk.id,
                    event: "parentUpdate",
                    kind: "follow",
                    targets: null,
                  }),
                );
              }
              await f.commands.verify(
                f.root,
                "unique",
                (row, stop) => f.checks.verifyUnique(row, stop),
                f.stop.signal,
              );
              await bindings.prepare(f.root, f.stop.signal);
              expect(capture).toHaveBeenCalledTimes(2);
              const first = actions[0],
                second = actions[1];
              if (!first || !second) throw new Error("Missing follow event IDs");
              const firstRange = await bindings.require(first, f.stop.signal),
                secondRange = await bindings.require(second, f.stop.signal);
              const ids = async (range: RowTargetRange) => {
                const out: number[] = [],
                  pin = f.statement.readOwn();
                try {
                  for await (const row of f.targets.rows(range, f.stop.signal)) {
                    const vectors = await readRows(pin.internalTable(f.child), [row.rowId]),
                      id = vectors[0];
                    if (id?.data.layout !== "i32") throw new Error("Expected native child key");
                    out.push(id.data.values[0] ?? -1);
                  }
                } finally {
                  pin.release();
                }
                return out.sort((a, b) => a - b);
              };
              const expected = (
                await pg.query<{ id: number }>("SELECT id FROM c WHERE pid=2 ORDER BY id")
              ).rows.map((row) => row.id);
              for await (const row of f.targets.rows(firstRange, f.stop.signal)) {
                const pin = f.statement.readOwn();
                try {
                  const locator = pin.internalTable(f.child).locator(row.rowId);
                  await f.statement
                    .table(f.child, { assigned: [pid], inputView: pin, uniqueTargets: true })
                    .update(batch(locators([locator], f.child), ints([2])), null);
                  const entry = f.tx.undo
                    .filter((e) => e.kind === "rows" && e.tableId === f.child)
                    .at(-1);
                  if (entry?.kind !== "rows") throw new Error("Missing native child version");
                  await f.targets.note(
                    f.child,
                    row.rowId,
                    entry.memtable.firstRowId + entry.before + entry.count - 1,
                  );
                } finally {
                  pin.release();
                }
              }
              await pg.exec("UPDATE c SET pid=2 WHERE pid=1");
              const fresh = (
                await pg.query<{ id: number }>("SELECT id FROM c WHERE pid=2 ORDER BY id")
              ).rows.map((row) => row.id);
              expect(fresh.length).toBeGreaterThan(expected.length);
              await bindings.prepare(f.root, f.stop.signal);
              expect(
                capture,
                "completed original rows are never recaptured after an RI rewrite",
              ).toHaveBeenCalledTimes(2);
              expect(
                await ids(await bindings.require(second, f.stop.signal)),
                "second original range excludes already rewritten first children",
              ).toEqual(expected);
              expect(
                await f.commands.resolveReferential(second),
                "queued descriptor remains immutable",
              ).toMatchObject({ targets: null });
              expect(secondRange).toEqual(await bindings.require(second, f.stop.signal));
              if (late) {
                await f.statement
                  .table(f.parent, { assigned: [pk], uniqueTargets: true })
                  .update(batch(locators([2], f.parent), ints([4])), null);
                const entry = f.tx.undo
                  .filter((e) => e.kind === "rows" && e.tableId === f.parent)
                  .at(-1);
                if (entry?.kind !== "rows") throw new Error("Missing later parent version");
                const images = await f.images.append(
                  f.parent,
                  { batch: batch(ints([3]), ints([3]), texts(["seed"])), row: 0 },
                  { batch: batch(ints([4]), ints([3]), texts(["seed"])), row: 0 },
                );
                await f.commands.accept({
                  command: f.root,
                  tableId: f.parent,
                  event: "update",
                  images,
                  oldRowId: 2,
                  newRowId: entry.memtable.firstRowId + entry.before + entry.count - 1,
                });
                const id = await f.commands.addReferential(f.root, images, {
                  fkId: fk.id,
                  event: "parentUpdate",
                  kind: "follow",
                  targets: null,
                });
                await f.commands.verify(
                  f.root,
                  "unique",
                  (row, stop) => f.checks.verifyUnique(row, stop),
                  f.stop.signal,
                );
                await bindings.prepare(f.root, f.stop.signal);
                expect(
                  capture,
                  "new work on the same token gets its own capture",
                ).toHaveBeenCalledTimes(3);
                expect((await bindings.require(id, f.stop.signal)).count).toBe(0);
                reach.reach("incremental same-token work");
              }
              const invalid = await f.insert(f.child, count + 100, 999);
              await bindings.prepare(f.root, f.stop.signal);
              await expect(
                pg.exec(`INSERT INTO c VALUES(${String(count + 100)},999)`),
              ).rejects.toMatchObject({ code: "23503" });
              expect(
                await code(() =>
                  f.commands.verify(
                    f.root,
                    "references",
                    async (row, stop) => {
                      if (row.images.row === invalid.images.row)
                        await f.checks.verifyReference(row, fk.id, stop);
                    },
                    f.stop.signal,
                  ),
                ),
                "follow preparation never consumes reference checks",
              ).toBe("23503");
              reach.reach(published ? "published" : "resident");
              reach.reach("original key swap targets");
              reach.reach("no recapture");
              reach.reach("immutable queued descriptor");
              reach.reach("independent FK reference failure");
            } finally {
              capture.mockRestore();
              await bindings.close();
              expect(f.grant.allocated).toBe(before);
              await f.close();
            }
            reach.reach("fixed metadata");
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [6, false, true],
            [8, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

for (const seed of seedsFor("next-registry-following-lifetime", [0xb625]))
  it(`fences original follow input lifetime ${String(seed)}`, async () => {
    const reach = createReach("native follow lifetime", REGISTRY_FOLLOWING_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("unprepared", "retired", "corrupt", "cancel", "close"),
        fc.boolean(),
        async (kind, published) => {
          reach.beginHistory();
          const suspended = kind === "cancel" || kind === "close";
          let resume: () => void = () => undefined;
          const gate = new Promise<void>((resolve) => {
            resume = resolve;
          });
          let entered: () => void = () => undefined;
          const ready = new Promise<void>((resolve) => {
            entered = resolve;
          });
          let observed: AbortSignal | null = null;
          const f = await fixture(
            published,
            2,
            suspended
              ? async (signal) => {
                  observed = signal;
                  entered();
                  await gate;
                }
              : undefined,
            6,
          );
          const before = f.grant.allocated,
            bindings = new RegistryFollowing({
              rootId: 17,
              catalog: f.statement.catalog,
              commands: f.commands,
              directory: f.directory,
              targets: f.targets,
              checks: f.checks,
              charge: f.grant,
              signal: f.stop.signal,
            });
          let closing: Promise<void> | undefined;
          try {
            const parent = f.statement.catalog.table(f.parent),
              fk = f.statement.catalog.table(f.child)?.foreignKeys[0],
              pk = parent?.columns[0]?.id;
            if (!pk || !fk) throw new Error("Missing native follow lifetime shape");
            await f.statement
              .table(f.parent, { assigned: [pk], uniqueTargets: true })
              .update(batch(locators([0], f.parent), ints([4])), null);
            const undo = f.tx.undo
              .filter((entry) => entry.kind === "rows" && entry.tableId === f.parent)
              .at(-1);
            if (undo?.kind !== "rows") throw new Error("Missing native follow update");
            const images = await f.images.append(
              f.parent,
              { batch: batch(ints([1]), ints([1]), texts(["seed"])), row: 0 },
              { batch: batch(ints([4]), ints([1]), texts(["seed"])), row: 0 },
            );
            await f.commands.accept({
              command: f.root,
              tableId: f.parent,
              event: "update",
              images,
              oldRowId: 0,
              newRowId: undo.memtable.firstRowId + undo.before,
            });
            const id = await f.commands.addReferential(f.root, images, {
              fkId: fk.id,
              event: "parentUpdate",
              kind: "follow",
              targets: null,
            });
            if (kind === "unprepared") {
              expect(
                await failure(() => bindings.require(id, f.stop.signal)),
                "a queued ID is not yet a sealed input",
              ).toBeInstanceOf(TypeError);
            } else if (suspended) {
              const pending = bindings.prepare(f.root, f.stop.signal).then(
                () => null,
                (error: unknown) => error,
              );
              await ready;
              expect(f.leases).toBe(0);
              let closed = false;
              if (kind === "close")
                closing = bindings.close().then(() => {
                  closed = true;
                });
              else f.stop.abort(new Error("native follow cancelled"));
              await yieldTask();
              const paid =
                  f.grant.allocated === before + registryFollowingBytes(f.statement.catalog),
                cancelled = (observed as AbortSignal | null)?.aborted === true,
                early = closed;
              resume();
              const error = await pending;
              await closing;
              expect(error, "pending capture cannot complete after cancellation").toBeInstanceOf(
                Error,
              );
              expect(cancelled, "capture receives the follow scope cancellation").toBe(true);
              expect(paid, "metadata remains paid while the capture is suspended").toBe(true);
              expect(early, "close fences pending native preparation").toBe(false);
              expect(f.range.allocated).toBe(0);
              reach.reach("paid pending capture");
              reach.reach("no retained image lease");
            } else {
              await bindings.prepare(f.root, f.stop.signal);
              if (kind === "retired") {
                await f.commands.retireReferential(id);
                expect(
                  await failure(() => bindings.require(id, f.stop.signal)),
                  "retired RI IDs cannot reuse sealed ranges",
                ).toBeInstanceOf(TypeError);
              } else {
                const record = new Uint8Array(128);
                expect(await f.directory.get(9, id, record)).toBe(true);
                new DataView(record.buffer).setFloat64(16, f.root.id + 1, true);
                await f.directory.put(9, id, record);
                expect(
                  await failure(() => bindings.require(id, f.stop.signal)),
                  "binding origin is authenticated",
                ).toMatchObject({ name: "SpillCorruptionError" });
              }
            }
            reach.reach(kind);
            reach.reach(published ? "published" : "resident");
          } finally {
            resume();
            await closing;
            await bindings.close();
            expect(f.grant.allocated).toBe(before);
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          ["unprepared", false],
          ["retired", true],
          ["corrupt", false],
          ["cancel", true],
          ["close", false],
        ],
      },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-native-parent-key-probes", [0xb633, 0xb634]))
  it(`checks RI OLD/NEW equality and final parent availability ${String(seed)}`, async () => {
    const reach = createReach("native RI parent keys", NATIVE_PARENT_KEY_REACH),
      pg = new PGlite();
    try {
      await pg.exec(
        "CREATE TABLE p(id INT PRIMARY KEY,u INT UNIQUE); INSERT INTO p SELECT i,i FROM generate_series(1,5) i",
      );
      await fc.assert(
        fc.asyncProperty(
          fc.option(fc.integer({ min: 0, max: 7 }), { nil: null }),
          fc.option(fc.integer({ min: 0, max: 7 }), { nil: null }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (before, after, secondary, published, remove) => {
            reach.beginHistory();
            const f = await fixture(published, 5);
            try {
              const table = f.statement.catalog.table(f.parent);
              if (!table) throw new Error("Missing parent");
              const index = secondary
                ? table.indexes.find((index) => index.unique && index.id !== table.primaryKey)
                : table.indexes.find((index) => index.id === table.primaryKey);
              if (!index) throw new Error("Missing parent index");
              const old = batch(ints([before ?? 0]), ints([before]), texts(["old"])),
                next = batch(ints([after ?? 0]), ints([after]), texts(["new"])),
                images = await f.images.append(
                  f.parent,
                  { batch: old, row: 0 },
                  { batch: next, row: 0 },
                ),
                row = { tableId: f.parent, images },
                oldKey = secondary ? before : (before ?? 0),
                newKey = secondary ? after : (after ?? 0),
                expected = (
                  await pg.query<{ changed: boolean }>(
                    "SELECT $1::int IS NOT NULL AND $1::int IS DISTINCT FROM $2::int AS changed",
                    [oldKey, newKey],
                  )
                ).rows[0]?.changed;
              expect(
                await f.checks.parentKeyChanged(row, index.id, f.stop.signal),
                "RI action selection uses saved OLD bytes",
              ).toBe(expected);
              await pg.exec("TRUNCATE p; INSERT INTO p SELECT i,i FROM generate_series(1,5) i");
              if (remove && oldKey !== null && oldKey >= 1 && oldKey <= 5) {
                const pin = f.statement.readOwn();
                try {
                  await f.statement
                    .table(f.parent, { inputView: pin, uniqueTargets: true })
                    .delete(batch(locators([oldKey - 1], f.parent)), null);
                } finally {
                  pin.release();
                }
                await pg.query("DELETE FROM p WHERE id=$1", [oldKey]);
                reach.reach("removed parent");
              } else reach.reach("retained parent");
              const exists = (
                await pg.query<{ found: boolean }>(
                  `SELECT EXISTS(SELECT FROM p WHERE ${secondary ? "u" : "id"}=$1) AS found`,
                  [oldKey],
                )
              ).rows[0]?.found;
              expect(
                await f.checks.oldParentKeyExists(row, index.id, f.stop.signal),
                "NO ACTION resolves the final parent prefix",
              ).toBe(exists);
              expect(f.leases).toBe(0);
              reach.reach(published ? "published" : "resident");
              reach.reach(secondary ? "secondary" : "primary");
              reach.reach(expected ? "changed" : "unchanged");
              reach.reach(exists ? "parent present" : "parent absent");
              if (secondary && before === null) reach.reach("NULL OLD");
              if (secondary && before !== null && after === null) reach.reach("NULL NEW");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [1, 1, false, false, true],
            [1, 2, true, true, false],
            [null, 1, true, false, false],
            [1, null, true, true, true],
            [0, 6, false, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

for (const seed of seedsFor("next-native-ri-restriction", [0xb635, 0xb636]))
  it(`executes registry RI restrictions at native statement end ${String(seed)}`, async () => {
    const reach = createReach("native RI restrictions", NATIVE_RI_RESTRICTION_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("noAction" as const, "restrict" as const),
          fc.integer({ min: 0, max: 9 }),
          fc.constantFrom(1, 2),
          fc.boolean(),
          fc.boolean(),
          async (mode, count, key, published, replacement) => {
            reach.beginHistory();
            const f = await fixture(published, 2, undefined, count, mode);
            try {
              await pg.exec(
                `DROP TABLE IF EXISTS c,p; CREATE TABLE p(id INT PRIMARY KEY,u INT,payload TEXT); CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p ON DELETE ${mode === "restrict" ? "RESTRICT" : "NO ACTION"},payload TEXT); INSERT INTO p VALUES(1,1,'seed'),(2,2,'seed')`,
              );
              for (let i = 0; i < count; i++)
                await pg.query("INSERT INTO c VALUES($1,$2,'child')", [
                  i + 1,
                  i % 5 === 0 ? null : (i % 2) + 1,
                ]);
              const reference = await code(() =>
                  pg.query(
                    replacement
                      ? "WITH d AS (DELETE FROM p WHERE id=$1 RETURNING *) INSERT INTO p SELECT * FROM d"
                      : "DELETE FROM p WHERE id=$1",
                    [key],
                  ),
                ),
                following = new RegistryFollowing({
                  rootId: 17,
                  catalog: f.statement.catalog,
                  commands: f.commands,
                  directory: f.directory,
                  targets: f.targets,
                  checks: f.checks,
                  charge: f.grant,
                  signal: f.stop.signal,
                }),
                actions = new NativeReferentialChecks({
                  catalog: f.statement.catalog,
                  commands: f.commands,
                  targets: f.targets,
                  checks: f.checks,
                  signal: f.stop.signal,
                  charge: f.grant,
                }),
                hooks = {
                  beforeRow: async () => {
                    throw new Error("Constraint action fired BEFORE");
                  },
                  afterRow: async () => {
                    throw new Error("Constraint action drained AFTER");
                  },
                },
                execution = new NativeCommandExecution({
                  frames: 1,
                  catalog: f.statement.catalog,
                  commands: f.commands,
                  images: f.images,
                  checks: f.checks,
                  following,
                  charge: f.grant,
                  signal: f.stop.signal,
                  abortRoot: (reason) => f.stop.abort(reason),
                  runReferential: (entry, _hooks, signal) => actions.run(entry, signal),
                }),
                primary = f.statement.openNativeCommands({
                  tables: [f.parent],
                  charge: f.grant,
                  signal: f.stop.signal,
                  commandBudget: (token) => f.commands.budget(token),
                  closeRoot: async () => {
                    try {
                      await execution.close();
                    } finally {
                      actions.close();
                    }
                  },
                  finishRoot: async (signal) => {
                    await execution.verifyCommandUnique(f.root, signal);
                    for await (const entry of execution.rowReferentialEntries(f.root, oldRef))
                      await execution.executeReferentialId(entry.id, hooks, signal);
                    await execution.verifyCommandReferences(f.root, signal);
                  },
                }),
                oldInput = batch(ints([key]), ints([key]), texts(["seed"])),
                oldRef = await f.images.append(f.parent, { batch: oldInput, row: 0 }, null),
                pin = f.statement.readOwn();
              let oldId: number;
              try {
                oldId = pin.internalTable(f.parent).rowId(key - 1);
              } finally {
                pin.release();
              }
              const observed = await code(async () => {
                const prepared = await primary.prepare(
                    f.root,
                    f.parent,
                    "delete",
                    oldId,
                    [0, 0, 0],
                  ),
                  lease = await f.images.readRowImages(oldRef, f.stop.signal);
                try {
                  await primary.apply(prepared, lease.old, null);
                } finally {
                  lease.release();
                }
                await f.commands.accept({
                  command: f.root,
                  tableId: f.parent,
                  event: "delete",
                  images: oldRef,
                  oldRowId: oldId,
                  newRowId: null,
                });
                const fk = f.statement.catalog.table(f.child)?.foreignKeys[0];
                if (!fk) throw new Error("Missing native FK");
                await f.commands.addReferential(f.root, oldRef, {
                  fkId: fk.id,
                  event: "parentDelete",
                  kind: mode,
                  targets: null,
                });
                if (replacement) {
                  const ref = await f.images.append(f.parent, null, { batch: oldInput, row: 0 }),
                    token = await primary.prepare(f.root, f.parent, "insert", null, [0, 0, 4]),
                    image = await f.images.readRowImages(ref, f.stop.signal);
                  let id: number | null;
                  try {
                    id = await primary.apply(token, null, image.new);
                  } finally {
                    image.release();
                  }
                  await f.commands.accept({
                    command: f.root,
                    tableId: f.parent,
                    event: "insert",
                    images: ref,
                    oldRowId: null,
                    newRowId: id,
                  });
                }
                await f.statement.end();
                await f.tx.commit(null);
              });
              expect(observed, "native end reports PG's precise RI action code").toBe(reference);
              await f.statement.closeNativeCommands();
              if (observed !== null) f.tx.rollback();
              const snapshot = f.path.pin();
              try {
                const actual = (await rowsOf(snapshot, f.parent)).map((row) => row[0]).sort(),
                  expected = (
                    await pg.query<{ id: number }>("SELECT id FROM p ORDER BY id")
                  ).rows.map((row) => row.id);
                expect(actual, "RI failure rolls back all native primary changes").toEqual(
                  expected,
                );
              } finally {
                snapshot.release();
              }
              reach.reach(published ? "published" : "resident");
              reach.reach(mode);
              let referenced = false;
              for (let i = 0; i < count; i++)
                if (i % 5 !== 0 && (i % 2) + 1 === key) referenced = true;
              reach.reach(referenced ? "referenced" : "empty children");
              reach.reach(replacement ? "replacement parent" : "no replacement");
              reach.reach(observed ? "end failure" : "end success");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["restrict", 4, 1, false, true],
            ["noAction", 4, 1, true, true],
            ["noAction", 4, 1, false, false],
            ["restrict", 0, 2, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

for (const seed of seedsFor("next-native-informational-capture", [0xb637, 0xb638]))
  it(`captures informational follow identities by bounded projected scan ${String(seed)}`, async () => {
    const reach = createReach("native informational follow", NATIVE_INFORMATIONAL_CAPTURE_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.integer({ min: 0, max: 40 }),
          fc.constantFrom(0, 24000),
          async (published, count, width) => {
            reach.beginHistory();
            const f = await fixture(published, 2, undefined, count, "noAction", false, width),
              hints = vi.spyOn(MemoryReads.prototype, "hint");
            try {
              await pg.exec("DROP TABLE IF EXISTS c; CREATE TABLE c(id INT,pid INT)");
              for (let i = 0; i < count; i++)
                await pg.query("INSERT INTO c VALUES($1,$2)", [
                  i + 1,
                  i % 5 === 0 ? null : (i % 2) + 1,
                ]);
              const child = f.statement.catalog.table(f.child),
                fk = child?.foreignKeys[0];
              if (!child || !fk) throw new Error("Missing informational child");
              expect(fk.enforced).toBe(false);
              expect(fk.supportIndexId).toBeNull();
              const input = batch(ints([1]), ints([1]), texts(["seed"])),
                images = await f.images.append(f.parent, { batch: input, row: 0 }, null),
                paid = f.grant.allocated,
                range = await f.checks.captureChildren(
                  { tableId: f.parent, images },
                  f.child,
                  fk.id,
                  f.targets,
                  f.stop.signal,
                ),
                pin = f.statement.readOwn(),
                actual: number[] = [];
              try {
                const table = pin.internalTable(f.child),
                  ids = Array.from({ length: count }, (_, i) => table.rowId(i));
                for await (const row of f.targets.rows(range, f.stop.signal))
                  actual.push(ids.indexOf(row.rowId) + 1);
              } finally {
                pin.release();
              }
              expect(actual.sort((a, b) => a - b)).toEqual(
                (
                  await pg.query<{ id: number }>("SELECT id FROM c WHERE pid=1 ORDER BY id")
                ).rows.map((row) => row.id),
              );
              expect(f.grant.allocated, "informational scanning retains fixed root metadata").toBe(
                paid,
              );
              expect(f.range.allocated).toBe(0);
              expect(f.range.loaned).toBe(0);
              if (hints.mock.calls.length) {
                expect(
                  hints.mock.calls.every(
                    ([request]) =>
                      request.columns.length === 1 && request.columns[0] === fk.columnIds[0],
                  ),
                  "informational scan decodes FK columns only",
                ).toBe(true);
                reach.reach("projected memory source");
              }
              reach.reach(published ? "published" : "resident");
              reach.reach(actual.length ? "matching" : "empty");
              reach.reach(width ? "large unrelated payload" : "small payload");
              reach.reach("fixed metadata");
            } finally {
              hints.mockRestore();
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [false, 40, 24000],
            [true, 40, 24000],
            [true, 0, 0],
            [false, 3, 0],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
